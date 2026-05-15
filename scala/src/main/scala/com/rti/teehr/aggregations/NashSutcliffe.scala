package com.rti.teehr.aggregations

import org.apache.spark.sql.expressions.Aggregator

/**
 * Accumulator state for Nash-Sutcliffe Efficiency (NSE) computation.
 *
 * NSE = 1 - (sum_squared_error / sum_squared_deviations)
 *
 * We track:
 *   - count: number of (primary, secondary) pairs
 *   - primarySum: sum of primary values (for computing mean)
 *   - primarySumSquares: sum of primary^2 (for computing variance)
 *   - sumSquaredError: cumulative sum((primary - secondary)^2)
 *   - isValid: whether the state is valid (no NaN/Inf encountered)
 *
 * @param count             Number of values
 * @param primarySum        Sum of primary values
 * @param primarySumSquares Sum of primary values squared
 * @param sumSquaredError   Sum of (primary - secondary)^2
 * @param isValid           Whether this state is valid
 */
case class NseState(
    count: Long = 0,
    primarySum: Double = 0.0,
    primarySumSquares: Double = 0.0,
    sumSquaredError: Double = 0.0,
    isValid: Boolean = true
) {

  /**
   * Add a (primary, secondary) pair to the state.
   *
   * @param primary   Primary (observed) value
   * @param secondary Secondary (forecast) value
   * @return Updated state
   */
  def add(primary: Double, secondary: Double): NseState = {
    // Check if both values are valid
    val newIsValid = isValid && TransformUtils.isValid(primary) && TransformUtils.isValid(secondary)

    if (!newIsValid) {
      this.copy(isValid = false)
    } else {
      val error = primary - secondary
      NseState(
        count = count + 1,
        primarySum = primarySum + primary,
        primarySumSquares = primarySumSquares + (primary * primary),
        sumSquaredError = sumSquaredError + (error * error),
        isValid = true
      )
    }
  }

  /**
   * Merge two states together.
   *
   * @param other Another state
   * @return Merged state
   */
  def merge(other: NseState): NseState = {
    NseState(
      count = count + other.count,
      primarySum = primarySum + other.primarySum,
      primarySumSquares = primarySumSquares + other.primarySumSquares,
      sumSquaredError = sumSquaredError + other.sumSquaredError,
      isValid = isValid && other.isValid
    )
  }

  /**
   * Compute the final NSE result from this state.
   *
   * NSE = 1 - (sum_squared_error / sum_squared_deviations)
   *
   * Returns NaN for:
   *   - Empty groups (count == 0)
   *   - Invalid state (NaN/Inf encountered)
   *   - Zero variance in primary values
   *
   * @param addEpsilon Whether to add epsilon to denominator
   * @return NSE value or NaN
   */
  def finish(addEpsilon: Boolean = false): Double = {
    // Edge cases
    if (!isValid || count == 0) {
      return Double.NaN
    }

    if (count == 1) {
      // Cannot compute variance with single value
      return Double.NaN
    }

    val primaryMean = primarySum / count
    val sumSquaredDeviations = primarySumSquares - (primarySum * primarySum / count)

    // Handle zero variance
    val denominator = if (addEpsilon) {
      sumSquaredDeviations + 1e-6
    } else {
      sumSquaredDeviations
    }

    if (denominator <= 0.0) {
      return Double.NaN
    }

    val nse = 1.0 - (sumSquaredError / denominator)
    nse
  }
}

/**
 * Spark Aggregator for computing Nash-Sutcliffe Efficiency.
 *
 * Accepts pairs of (primary, secondary) values and computes NSE.
 *
 * @param transformType The transformation to apply (default: None)
 * @param addEpsilon    Whether to add epsilon for numerical stability
 */
class NashSutcliffeAggregator(
    transformType: Transform.Transform = Transform.None,
    addEpsilon: Boolean = false
) extends Aggregator[(Double, Double), NseState, Double] {

  /**
   * Return the zero/initial state.
   */
  override def zero: NseState = {
    NseState()
  }

  /**
   * Add a value to the accumulator state.
   *
   * @param buffer  Current state
   * @param value   (primary, secondary) pair to add
   * @return Updated state
   */
  override def reduce(buffer: NseState, value: (Double, Double)): NseState = {
    val (primary, secondary) = value

    // Apply transformation
    val transformedPrimary = TransformUtils.transform(primary, transformType, addEpsilon)
    val transformedSecondary = TransformUtils.transform(secondary, transformType, addEpsilon)

    buffer.add(transformedPrimary, transformedSecondary)
  }

  /**
   * Merge two partial states (from different partitions).
   *
   * @param buffer1 First state
   * @param buffer2 Second state
   * @return Merged state
   */
  override def merge(buffer1: NseState, buffer2: NseState): NseState = {
    buffer1.merge(buffer2)
  }

  /**
   * Compute the final NSE result from the accumulated state.
   *
   * @param reduction Final accumulated state
   * @return NSE value (or NaN for edge cases)
   */
  override def finish(reduction: NseState): Double = {
    reduction.finish(addEpsilon)
  }

  /**
   * Return the Catalyst encoder for the buffer type.
   * Spark uses this to serialize/deserialize state across workers.
   */
  override def bufferEncoder = org.apache.spark.sql.Encoders.product[NseState]

  /**
   * Return the Catalyst encoder for the output type.
   */
  override def outputEncoder = org.apache.spark.sql.Encoders.scalaDouble
}
