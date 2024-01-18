package org.gpanda70.squery.datasource
import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.gpanda70.squery.datatypes.{ArrowFieldVector, RecordBatch, Schema}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}


class ParquetDataSource(private val filename: String) extends DataSource {

  override def schema(): Schema = {

  }

  override def scan(projection: List[String]): Iterable[RecordBatch] = new ParquetScan(filename, projection)
}

/** Based on blog post at https://www.arm64.ca/post/reading-parquet-files-java/ */
class ParquetScan(filename: String, private val columns: List[String]) extends AutoCloseable with Iterable[RecordBatch] {

  private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filename), new Configuration()))

  val schema = reader.getFooter.getFileMetaData.getSchema
  override def close(): Unit = reader.close()

  override def iterator: Iterator[RecordBatch] = new ParquetIterator()
}

class ParquetIterator(private val reader: ParquetFileReader,
                      private val projectedColumns: List[String]) extends Iterator[RecordBatch] {

  val schema = reader.getFooter.getFileMetaData.getSchema

  val arrowSchema = new SchemaConverter().fromParquet(schema).getArrowSchema

  val projectedArrowSchema = {
    val fields = projectedColumns.flatMap { name =>
      arrowSchema.getFields.asScala.find(_.getName == name)}
    new org.apache.arrow.vector.types.pojo.Schema(fields.asJava)

  }

  var batch: Option[RecordBatch] = None
  override def hasNext: Boolean = {
    batch = nextBatch()
    batch.isDefined
  }

  override def next(): RecordBatch = {
    val next = batch
    batch = None
    next.getOrElse()
  }

  private def nextBatch(): Option[RecordBatch] = {
    val pages = reader.readNextRowGroup()
    if (pages == null) {
      return None
    }

    if (pages.getRowCount > Int.MaxValue) {
      throw new IllegalStateException()
    }

    val rows = pages.getRowCount.toInt
    println(s"Reading $rows rows")

    val root = VectorSchemaRoot.create(projectedArrowSchema, new RootAllocator(Long.MaxValue))
    root.allocateNew()
    root.setRowCount(rows)

    val ballistaSchema = org.gpanda70.squery.datatypes.SchemaConverter.fromArrow(projectedArrowSchema)
    batch = Some(new RecordBatch(ballistaSchema, root.getFieldVectors.asScala.toList.map(new ArrowFieldVector(_))))

    //TODO: We really want to read directly as columns not rows
    // val columnIO = new columnIOFactory().getColumnIO(schema)
    // val recordReader: RecordReader[Group] = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))

    for (rowIndex <- 0 until rows) {
//      val group: Group = recordReader.read()
      for (projectionIndex <- 0 until projectedColumns.size) {
        // val primitiveTypeName = projectedArrowSchema.fields(fieldIndex).getType.getPrimitiveTypeName
        // println(s"column $fieldIndex : $primitiveTypeName")

        // if (group.getFieldRepetitionCount(fieldIndex) == 1) {
        //   primitiveTypeName match {
        //     case PrimitiveType.PrimitiveTypeName.INT32 =>
        //       root.fieldVectors(projectionIndex).asInstanceOf[IntVector].set(rowIndex, group.getInteger(fieldIndex, 0))
        //     case _ => println(s"unsupported type $primitiveTypeName")
        //   }
        // }

      }
    }
    batch
  }
}


