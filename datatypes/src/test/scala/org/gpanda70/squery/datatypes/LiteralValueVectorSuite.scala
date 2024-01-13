package org.gpanda70.squery.datatypes

import org.gpanda70.squery.datatypes.ArrowTypes.Int8Type
import org.scalatest.funsuite.AnyFunSuite

class LiteralValueVectorSuite extends AnyFunSuite{

  test("getValue should return scalar value") {
    val value = Some(42)
    val size = 10

    val literalValueVector = new LiteralValueVector(Int8Type, value, size)

    assert(literalValueVector.getValue(0) === value)
    assert(literalValueVector.getValue(size-1) === value)
  }

  test("getValue should throw IndexOutOfBounds exception for invalid index") {
    val value = Some(42)
    val size = 10

    val literalValueVector = new LiteralValueVector(Int8Type, value, size)

    assertThrows[IndexOutOfBoundsException] {
      literalValueVector.getValue(-1)
    }
    assertThrows[IndexOutOfBoundsException] {
      literalValueVector.getValue(size)
    }

  }

}
