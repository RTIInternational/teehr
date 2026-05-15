package com.rti.teehr.aggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Registration helper for Nash-Sutcliffe Efficiency (NSE) aggregator.
 *
 * Provides methods to register NSE as a Spark SQL function so it can be
 * called from Python/SQL via aggregate operations.
 */
object NseRegistration {

  /**
   * Register NSE as an aggregate SQL function (UDAF).
   *
   * After calling this, you can use NSE in grouped SQL:
   *   spark.sql("SELECT key, nse(primary, secondary) FROM data GROUP BY key")
   *
   * @param spark SparkSession to register the function in
   * @param transformType Transform to apply (default: "none")
   * @param addEpsilon    Whether to add epsilon for stability (default: false)
   * @param functionName  Custom name for the function (default: "nse")
   */
  def registerNse(
      spark: SparkSession,
      transformType: String = "none",
      addEpsilon: Boolean = false,
      functionName: String = "nse"
  ): Unit = {
    val transformEnum = parseTransform(transformType)
    val aggregator = new NashSutcliffeAggregator(transformEnum, addEpsilon)
    spark.udf.register(functionName, udaf(aggregator))
  }

  /**
   * Register NSE aggregator with standard variants (no transform, log, sqrt).
   *
   * @param spark SparkSession to register functions in
   */
  def registerNseStandard(spark: SparkSession): Unit = {
    registerNse(spark, "none", false, "nse")
    registerNse(spark, "log", false, "nse_log")
    registerNse(spark, "log", true, "nse_log_eps")
    registerNse(spark, "sqrt", false, "nse_sqrt")
  }

  /**
   * Parse transform string to Transform enum.
   *
   * @param transform Transform name ("none", "log", "sqrt", etc.)
   * @return Corresponding Transform enum value
   */
  private def parseTransform(transform: String): Transform.Transform = {
    transform.toLowerCase match {
      case "none"   => Transform.None
      case "log"    => Transform.Log
      case "sqrt"   => Transform.Sqrt
      case "square" => Transform.Square
      case "cube"   => Transform.Cube
      case "exp"    => Transform.Exp
      case "inv"    => Transform.Inv
      case "abs"    => Transform.Abs
      case other   => throw new IllegalArgumentException(s"Unknown transform: $other")
    }
  }

  /**
   * Compute NSE directly (not as aggregator) for a single pair.
   * Useful for testing and single-value operations.
   *
   * @param primary   Primary value
   * @param secondary Secondary value
   * @param transform Transform type
   * @param addEpsilon Add epsilon
   * @return NSE value
   */
  def computeNse(
      primary: Double,
      secondary: Double,
      transform: String = "none",
      addEpsilon: Boolean = false
  ): Double = {
    val transformEnum = parseTransform(transform)
    val aggregator = new NashSutcliffeAggregator(transformEnum, addEpsilon)
    aggregator.reduce(aggregator.zero, (primary, secondary)).finish(addEpsilon)
  }
}
