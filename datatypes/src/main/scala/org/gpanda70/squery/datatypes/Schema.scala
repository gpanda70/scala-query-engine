package org.gpanda70.squery.datatypes

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema, Field => ArrowField, FieldType => ArrowFieldType}

/**
 * Represents a schema used in a query engine
 * @param fields List of fields that make up the Schema
 */
case class Schema(fields: List[Field]) {
  def toArrow(): ArrowSchema = {
    new ArrowSchema(fields.map(_.toArrow()).asJava)
  }

  def project(indices: List[Int]): Schema = {
    Schema(indices.map(fields))
  }

  def select(names: List[String]): Schema = {
    val selectedFields = ListBuffer[Field]()
    names.foreach { name =>
      val matchedFields = fields.filter(_.name == name)
      val matchedFieldsExists = matchedFields.size == 1
      if (matchedFieldsExists) selectedFields += matchedFields.head
      else throw new IllegalArgumentException()
    }
    Schema(selectedFields.toList)
  }
}

/**
 * Represents a field in a schema, typically corresponding to a column in a database
 * @param name The name of the field
 * @param dataType The data type the field represents
 */
case class Field(name: String, dataType:ArrowType) {
  def toArrow(): ArrowField = {
    val fieldType = new ArrowFieldType(true, dataType, null)
    new ArrowField(name, fieldType, java.util.Collections.emptyList())
  }
}
