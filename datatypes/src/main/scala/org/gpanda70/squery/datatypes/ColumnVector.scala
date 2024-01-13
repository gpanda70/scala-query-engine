package org.gpanda70.squery.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * An interface to provide convenient accessor methods, avoiding the
 * need to case to a specific [[FieldVector]] implementation for each data type
 *
 */
trait ColumnVector {
  def getType(): ArrowType
  def getValue(i: Int): Option[Any]
  def getSize(): Int

}
