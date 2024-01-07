package org.gpanda70.squery.datatypes

import scala.jdk.CollectionConverters._
import org.gpanda70.squery.datatypes.ArrowTypes.{DoubleType, Int8Type, StringType}
import org.scalatest.funsuite.AnyFunSuite

class SchemaSuite extends AnyFunSuite{

  test("Field.toArrow should create the correct ArrowField") {
    val fieldName = "randomField"

    val randomField = Field(fieldName, Int8Type)
    val arrowRandomField = randomField.toArrow()

    assert(arrowRandomField.getName === fieldName)
    assert(arrowRandomField.getType === Int8Type)
  }

  test("Schema.toArrow should create the correct Arrow Schema") {
    val fields = List(
      Field("test1", Int8Type),
      Field("test2", StringType),
      Field("test3", DoubleType)
    )

    val randomSchema = Schema(fields)
    val arrowRandomSchema = randomSchema.toArrow()

    assert(arrowRandomSchema.getFields.size() === fields.size)
    assert(arrowRandomSchema.getFields.get(0).getName === fields(0).name)
    assert(arrowRandomSchema.getFields.get(1).getName === fields(1).name)
    assert(arrowRandomSchema.getFields.get(2).getName === fields(2).name)
  }

  test("Schema.project should create project the correct fields based on indices") {
    val fields = List(
      Field("test1", Int8Type),
      Field("test2", StringType),
      Field("test3", DoubleType)
    )
    val indices = List(1, 0)

    val randomSchema = Schema(fields)
    val randomSchemaProject = randomSchema.project(indices)

    assert(randomSchemaProject.fields.size === indices.size)
    assert(randomSchemaProject.fields(0) === fields(indices(0)))
    assert(randomSchemaProject.fields(1) === fields(indices(1)))
  }

  test("Schema.select should select the correct fields based on name") {
    val fields = List(
      Field("test1", Int8Type),
      Field("test2", StringType),
      Field("test3", DoubleType)
    )
    val names = List("test2", "test1")

    val randomSchema = Schema(fields)
    val randomSchemaSelect = randomSchema.select(names)

    assert(randomSchemaSelect.fields.size === names.size)
    assert(randomSchemaSelect.fields(0).name === names(0))
    assert(randomSchemaSelect.fields(1).name === names(1))
  }

  test("Schema.select should throw IllegalArgumentException when name does not exist") {
    val fields = List(
      Field("test1", Int8Type),
      Field("test2", StringType),
      Field("test3", DoubleType)
    )
    val names = List("doesNotExist")

    val randomSchema = Schema(fields)

    assertThrows[IllegalArgumentException] {
      randomSchema.select(names)
    }
  }
}
