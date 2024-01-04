import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision

/**
 * This object provides constants that reference supported Arrow data types
 */
object ArrowTypes {
  val BooleanType = new ArrowType.Bool
  val Int8Type = new ArrowType.Int(8, true)
  val Int16Type = new ArrowType.Int(16, true)
  val Int32Type = new ArrowType.Int(32, true)
  val Int64Type = new ArrowType.Int(64, true)
  val UInt8Type = new ArrowType.Int(8, false)
  val UInt16Type = new ArrowType.Int(16, false)
  val UInt32type = new ArrowType.Int(32, false)
  val UInt64Type = new ArrowType.Int(64, false)
  val FloatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
  val DoubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
  val StringType = new ArrowType.Utf8
}
