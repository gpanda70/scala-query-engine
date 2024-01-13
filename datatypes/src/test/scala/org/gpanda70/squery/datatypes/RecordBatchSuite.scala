package org.gpanda70.squery.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType
import org.gpanda70.squery.datatypes.ArrowTypes.{Int8Type, StringType}
import org.gpanda70.squery.datatypes.RecordBatch
import org.scalatest.funsuite.AnyFunSuite

class MockColumnVector(val arrowType: ArrowType, val value: Array[Option[Any]]) extends ColumnVector {
  override def getType(): ArrowType = arrowType

  override def getValue(i: Int): Option[Any] = value(i)

  override def getSize(): Int = value.length
}
class RecordBatchSuite extends AnyFunSuite{
  test("rowCount should return the correct count") {
    val schema = Schema(
      List(Field("age", Int8Type), Field("name", StringType))
    )

    val mockColumnVectors = List(
      new MockColumnVector(schema.fields(0).dataType, Array(Some(40),Some(22),Some(33))),
      new MockColumnVector(schema.fields(1).dataType, Array(Some("John"),Some("Mary"), Some("George")))
    )
    val recordBatch = new RecordBatch(schema, mockColumnVectors)
    assert(recordBatch.rowCount() === 3)
  }

  test("columnCount should return the correct count") {
    val schema = Schema(
      List(Field("age", Int8Type), Field("name", StringType))
    )

    val mockColumnVectors = List(
      new MockColumnVector(schema.fields(0).dataType, Array(Some(40),Some(22),Some(33))),
      new MockColumnVector(schema.fields(1).dataType, Array(Some("John"),Some("Mary"), Some("George")))
    )
    val recordBatch = new RecordBatch(schema, mockColumnVectors)
    assert(recordBatch.columnCount() === 2)
  }

  test("field should return the correct field") {
    val schema = Schema(
      List(Field("age", Int8Type), Field("name", StringType))
    )

    val mockColumnVectors = List(
      new MockColumnVector(schema.fields(0).dataType, Array(Some(40),Some(22))),
      new MockColumnVector(schema.fields(1).dataType, Array(Some("John"),Some("Mary")))
    )
    val recordBatch = new RecordBatch(schema, mockColumnVectors)
    assert(recordBatch.field(1) === mockColumnVectors(1))
  }

  test("toCSV should generate a CSV output") {
    val schema = Schema(
      List(Field("age", Int8Type), Field("name", StringType))
    )

    val mockColumnVectors = List(
      new MockColumnVector(schema.fields(0).dataType, Array(Some(40),Some(22))),
      new MockColumnVector(schema.fields(1).dataType, Array(Some("John"),Some("Mary")))
    )
    val recordBatch = new RecordBatch(schema, mockColumnVectors)

    val expectedCSV = "40,John\n22,Mary\n"
    assert(recordBatch.toCSV() === expectedCSV)
  }
}
