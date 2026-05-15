package com.rti.teehr.aggregations

/** Enum for supported transformations. */
object Transform extends Enumeration {
  type Transform = Value
  val None = Value("none")
  val Log = Value("log")
  val Sqrt = Value("sqrt")
  val Square = Value("square")
  val Cube = Value("cube")
  val Exp = Value("exp")
  val Inv = Value("inv")
  val Abs = Value("abs")
}

/** Transform utilities for applying and validating transformations. */
object TransformUtils {
  private val EPSILON: Double = 1e-6

  /**
   * Apply a transformation to a value.
   *
   * @param value            The value to transform
   * @param transformType    The transformation type
   * @param addEpsilon       Whether to add epsilon before transform (for log, inv)
   * @return The transformed value (may be NaN or Inf)
   */
  def transform(
      value: Double,
      transformType: Transform.Transform,
      addEpsilon: Boolean = false
  ): Double = {
    val adjustedValue = if (addEpsilon && (transformType == Transform.Log || transformType == Transform.Inv)) {
      value + EPSILON
    } else {
      value
    }

    transformType match {
      case Transform.None    => adjustedValue
      case Transform.Log     => scala.math.log(adjustedValue)
      case Transform.Sqrt    => scala.math.sqrt(adjustedValue)
      case Transform.Square  => adjustedValue * adjustedValue
      case Transform.Cube    => adjustedValue * adjustedValue * adjustedValue
      case Transform.Exp     => scala.math.exp(adjustedValue)
      case Transform.Inv     => 1.0 / adjustedValue
      case Transform.Abs     => scala.math.abs(adjustedValue)
      case _                 => Double.NaN
    }
  }

  /**
   * Check if a value is valid after transformation (not NaN or Inf).
   *
   * @param value The value to check
   * @return true if the value is finite
   */
  def isValid(value: Double): Boolean = {
    !value.isNaN && !value.isInfinite
  }
}
