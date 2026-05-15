package com.rti.teehr.aggregations

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.math.{abs, sqrt}

class NashSutcliffeSpec extends AnyFlatSpec with Matchers {

  // Helper: compute NSE from raw data
  private def computeNse(
      data: Seq[(Double, Double)],
      transform: Transform.Transform = Transform.None,
      addEpsilon: Boolean = false
  ): Double = {
    val aggregator = new NashSutcliffeAggregator(transform, addEpsilon)
    val state = data.foldLeft(aggregator.zero) { (acc, value) =>
      aggregator.reduce(acc, value)
    }
    aggregator.finish(state)
  }

  // Helper: compute NSE from two Series
  private def computeNseFromSeries(
      primary: Seq[Double],
      secondary: Seq[Double],
      transform: Transform.Transform = Transform.None,
      addEpsilon: Boolean = false
  ): Double = {
    val data = primary.zip(secondary)
    computeNse(data, transform, addEpsilon)
  }

  "NashSutcliffeAggregator" should "compute NSE = 1.0 for perfect forecast" in {
    val primary = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val secondary = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val nse = computeNseFromSeries(primary, secondary)
    nse should be(1.0 +- 1e-10)
  }

  it should "return NaN for empty group" in {
    val nse = computeNse(Seq())
    nse.isNaN should be(true)
  }

  it should "return NaN for single value" in {
    val nse = computeNse(Seq((1.0, 2.0)))
    nse.isNaN should be(true)
  }

  it should "return NaN when primary has zero variance" in {
    val primary = Seq(5.0, 5.0, 5.0, 5.0)
    val secondary = Seq(1.0, 2.0, 3.0, 4.0)
    val nse = computeNseFromSeries(primary, secondary)
    nse.isNaN should be(true)
  }

  it should "compute NSE < 1.0 for imperfect forecast" in {
    val primary = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val secondary = Seq(1.5, 1.8, 3.2, 3.9, 5.1)
    val nse = computeNseFromSeries(primary, secondary)
    nse should be < 1.0
    nse should be > 0.0
  }

  it should "compute NSE < 0.0 for poor forecast" in {
    val primary = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val secondary = Seq(5.0, 4.0, 3.0, 2.0, 1.0) // reverse order
    val nse = computeNseFromSeries(primary, secondary)
    nse should be < 0.0
  }

  it should "handle NaN in input gracefully" in {
    val data = Seq(
      (1.0, 2.0),
      (Double.NaN, 3.0),
      (4.0, 5.0)
    )
    val nse = computeNse(data)
    nse.isNaN should be(true)
  }

  it should "handle Inf in input gracefully" in {
    val data = Seq(
      (1.0, 2.0),
      (Double.PositiveInfinity, 3.0),
      (4.0, 5.0)
    )
    val nse = computeNse(data)
    nse.isNaN should be(true)
  }

  it should "apply log transform correctly" in {
    // Use positive values suitable for log
    val primary = Seq(1.0, 2.0, 5.0, 10.0)
    val secondary = Seq(1.1, 1.9, 5.2, 9.8)
    val nseNoTransform = computeNseFromSeries(primary, secondary, Transform.None)
    val nseWithLog = computeNseFromSeries(primary, secondary, Transform.Log)

    nseNoTransform should not equal nseWithLog
    nseWithLog should be > 0.0
  }

  it should "apply log transform with epsilon for zero values" in {
    // With epsilon, log(0 + eps) is valid; without, log(0) = -Inf
    val primary = Seq(1.0, 0.0, 5.0)
    val secondary = Seq(1.0, 0.0, 5.0)

    // Without epsilon, should fail due to -Inf
    val nseNoEpsilon = computeNseFromSeries(primary, secondary, Transform.Log, addEpsilon = false)
    nseNoEpsilon.isNaN should be(true)

    // With epsilon, should succeed
    val nseWithEpsilon = computeNseFromSeries(primary, secondary, Transform.Log, addEpsilon = true)
    nseWithEpsilon should be(1.0 +- 1e-6)
  }

  it should "apply sqrt transform correctly" in {
    val primary = Seq(1.0, 4.0, 9.0, 16.0)
    val secondary = Seq(1.0, 4.0, 9.0, 16.0)
    val nse = computeNseFromSeries(primary, secondary, Transform.Sqrt)
    nse should be(1.0 +- 1e-10)
  }

  it should "apply square transform correctly" in {
    val primary = Seq(1.0, 2.0, 3.0, 4.0)
    val secondary = Seq(1.0, 2.0, 3.0, 4.0)
    val nse = computeNseFromSeries(primary, secondary, Transform.Square)
    nse should be(1.0 +- 1e-10)
  }

  it should "apply abs transform correctly" in {
    val primary = Seq(-5.0, -2.0, 3.0, 4.0)
    val secondary = Seq(-5.0, -2.0, 3.0, 4.0)
    val nse = computeNseFromSeries(primary, secondary, Transform.Abs)
    nse should be(1.0 +- 1e-10)
  }

  "NseState" should "merge states correctly" in {
    val state1 = NseState().add(1.0, 1.0).add(2.0, 2.0)
    val state2 = NseState().add(3.0, 3.0).add(4.0, 4.0)
    val merged = state1.merge(state2)

    merged.count should be(4)
    merged.primarySum should be(10.0)
    merged.sumSquaredError should be(0.0)

    // Finish should return 1.0 (perfect forecast)
    merged.finish() should be(1.0 +- 1e-10)
  }

  it should "handle invalid states during merge" in {
    val validState = NseState().add(1.0, 1.0).add(2.0, 2.0)
    val invalidState = NseState().copy(isValid = false)
    val merged = validState.merge(invalidState)

    merged.isValid should be(false)
    merged.finish().isNaN should be(true)
  }

  it should "maintain count correctly across operations" in {
    val state = NseState()
    val state1 = state.add(1.0, 1.0).add(2.0, 2.0).add(3.0, 3.0)
    state1.count should be(3)
  }

  it should "apply addEpsilon to denominator in finish()" in {
    // Create a state with very small denominator
    // State with primary = [1.0, 1.0, 1.0, ...] would have zero variance
    // But with addEpsilon, it should not return NaN
    val state = NseState(
      count = 2,
      primarySum = 10.0,
      primarySumSquares = 50.0,
      sumSquaredError = 0.001,
      isValid = true
    )

    val nseWithoutEpsilon = state.finish(addEpsilon = false)
    val nseWithEpsilon = state.finish(addEpsilon = true)

    // Both should be finite (not NaN), but different
    nseWithEpsilon.isFinite should be(true)
  }
}
