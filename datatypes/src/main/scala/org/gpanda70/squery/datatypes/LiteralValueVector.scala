package org.gpanda70.squery.datatypes
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * An implementation that represents scalar values, avoiding the need to create/populate a [[FieldVector]]
 * with a literal value repeated for every index on the column
 *
 * @param arrowType
 * @param value
 * @param size
 */
class LiteralValueVector(val arrowType: ArrowType, val value: Any, val size: Int) extends ColumnVector {

  override def getType(): ArrowType = arrowType

  override def getValue(i: Int): Any = {
    val isOutOfBounds = (i<0 || i>=size)
    if (isOutOfBounds) throw new IndexOutOfBoundsException
    value
  }

  override def getSize(): Int = size

}
