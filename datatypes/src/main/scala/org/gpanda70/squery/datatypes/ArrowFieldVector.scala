package org.gpanda70.squery.datatypes

import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TinyIntVector, VarCharVector}
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * A wrapper around Arrow FieldVector
 * @param field An Arrow FieldVector
 */
class ArrowFieldVector(val field: FieldVector) extends ColumnVector {

  override def getType(): ArrowType = {
    field match  {
      case _: BitVector => ArrowTypes.BooleanType
      case _: TinyIntVector => ArrowTypes.Int8Type
      case _: SmallIntVector => ArrowTypes.Int16Type
      case _: IntVector => ArrowTypes.Int32Type
      case _: BigIntVector => ArrowTypes.Int64Type
      case _: Float4Vector => ArrowTypes.FloatType
      case _: Float8Vector => ArrowTypes.DoubleType
      case _: VarCharVector => ArrowTypes.StringType
      case _ => throw new IllegalStateException()
    }
  }

  override def getValue(i: Int): Option[Any] = {
    if (field.isNull(i)) return None

    field match {
      case bitVector: BitVector => Some(bitVector.get(i) == 1)
      case tinyIntVector: TinyIntVector => Some(tinyIntVector.get(i))
      case smallIntVector: SmallIntVector => Some(smallIntVector.get(i))
      case intVector: IntVector => Some(intVector.get(i))
      case bigIntVector: BigIntVector => Some(bigIntVector.get(i))
      case float4Vector: Float4Vector => Some(float4Vector.get(i))
      case float8Vector: Float8Vector => Some(float8Vector.get(i))
      case varCharVector: VarCharVector => Some(varCharVector.get(i))
      case _ => throw new IllegalStateException()
    }

  }

  override def getSize(): Int = field.getValueCount
}
