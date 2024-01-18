package org.gpanda70.squery.datasource

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TinyIntVector, VarCharVector, VectorSchemaRoot}
import org.gpanda70.squery.datatypes.{ArrowFieldVector, ArrowTypes, Field, RecordBatch, Schema}

import java.io.{File, FileNotFoundException}
import java.util.logging.Logger
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters.CollectionHasAsScala

class CsvDataSource(
                     val fileName: String,
                     val csvSchema: Option[Schema],
                     private val hasHeaders: Boolean,
                     private val batchSize: Int) extends DataSource {

  private val logger = Logger.getLogger(classOf[CsvDataSource].getSimpleName)
  private lazy val finalSchema: Schema = csvSchema.getOrElse(inferSchema())

  private def inferSchema(): Schema = {
    logger.fine("inferSchema()")

    val file = new File(fileName)
    if (!file.exists()) throw new FileNotFoundException(file.getAbsolutePath)

    val parser = buildParser(defaultSettings())
    val source = Source.fromFile(file)
    try {
      parser.beginParsing(source.bufferedReader())
      parser.getDetectedFormat

      parser.parseNext()
      // some delimiters cause sparse arrays, so remove null columns in the parsed header
      val headers = parser.getContext.parsedHeaders.filter(_ != null).toList
      val schema =
        if (hasHeaders) {
          Schema(headers.map(colName => Field(colName, ArrowTypes.StringType)))
        } else {
          Schema(headers.zipWithIndex.map { case (_, i) => Field(s"field_${i + 1}", ArrowTypes.StringType) })
        }
      schema
    }
    finally {
      parser.stopParsing()
      source.close()
    }

  }

  private def buildParser(settings: CsvParserSettings): CsvParser = {
    new CsvParser(settings)
  }

  private def defaultSettings(): CsvParserSettings = {
    val settings = new CsvParserSettings()
    settings.setDelimiterDetectionEnabled(true)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setSkipEmptyLines(true)
    settings.setAutoClosingEnabled(true)
    settings
    }

  override def schema(): Schema = finalSchema

  override def scan(projection: List[String]): Iterable[RecordBatch] = {
    logger.fine(s"scan() projection=$projection")

    val file = new File(fileName)
    if (!file.exists()) throw new FileNotFoundException(file.getAbsolutePath)

    val readSchema =
      if (projection.nonEmpty) finalSchema.select(projection)
      else finalSchema

    val settings = defaultSettings()
    if (projection.nonEmpty) settings.selectFields(projection: _*)
    settings.setHeaderExtractionEnabled(hasHeaders)
    if (!hasHeaders) settings.setHeaders(readSchema.fields.map(_.name): _*)

    val parser = buildParser(settings)
    val source = Source.fromFile(file)
    // parser will close  once the end of reader is reached
    parser.beginParsing(source.bufferedReader())
    parser.getDetectedFormat

    new ReaderAsSequence(readSchema, parser, batchSize)
  }
}

class ReaderAsSequence(private val schema: Schema,
                        private val parser:CsvParser,
                        private val batchSize: Int) extends Iterable[RecordBatch] {
  override def iterator: Iterator[RecordBatch] = new ReaderIterator(schema, parser, batchSize)

}

class ReaderIterator(schema: Schema,
                     parser: CsvParser,
                     batchSize: Int) extends Iterator[RecordBatch] {

  private val logger = Logger.getLogger(classOf[CsvDataSource].getSimpleName)

  private var nextRecordBatch: Option[RecordBatch] = None
  private var started: Boolean = false

  override def hasNext: Boolean = {
    if (!started) {
      started = true
      nextRecordBatch = nextBatch()
    }
    nextRecordBatch.isDefined
  }

  override def next(): RecordBatch = {
    if (!started) hasNext

    val out = nextRecordBatch

    nextRecordBatch = nextBatch()

    out match {
      case Some(out) => out
      case None => throw new NoSuchElementException(
        s"Cannot read past the end of ${classOf[ReaderIterator].getSimpleName}")
    }
  }

  private def nextBatch(): Option[RecordBatch] = {
    val rows = new ArrayBuffer[Record](batchSize)

    var line = parser.parseNextRecord()

    while (line != null && rows.size < batchSize) {
      rows += line
      line = parser.parseNextRecord()
    }
    if (rows.isEmpty) None
    else Some(createBatch(rows))
  }

  private def createBatch(rows: ArrayBuffer[Record]): RecordBatch = {
    val root = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator(Long.MaxValue))
    root.getFieldVectors.forEach(_.setInitialCapacity(rows.size))
    root.allocateNew()

    root.getFieldVectors.forEach { vector =>
      vector match {
        case varcharVector: VarCharVector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(varcharVector.getName, "").trim
            varcharVector.setSafe(index, valueStr.getBytes())
          }

        case tinyIntVector: TinyIntVector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(tinyIntVector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else tinyIntVector.set(index, valueStr.toByte)
          }

        case smallIntVector: SmallIntVector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(smallIntVector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else smallIntVector.set(index, valueStr.toShort)
          }

        case intVector: IntVector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(intVector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else intVector.set(index, valueStr.toInt)
          }

        case bigIntVector: BigIntVector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(bigIntVector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else bigIntVector.set(index, valueStr.toLong)
          }

        case float4Vector: Float4Vector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(float4Vector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else float4Vector.set(index, valueStr.toFloat)
          }

        case float8Vector: Float8Vector =>
          rows.zipWithIndex.foreach { case (row, index) =>
            val valueStr = row.getValue(float8Vector.getName, "").trim
            if (valueStr.isEmpty) vector.setNull(index)
            else float8Vector.set(index, valueStr.toDouble)
          }
        case _ => throw new IllegalStateException(s"No support for reading CSV columns with data type $vector")
      }
      vector.setValueCount(rows.size)
    }
    new RecordBatch(schema, root.getFieldVectors.asScala.toList.map(new ArrowFieldVector(_)))
  }
}