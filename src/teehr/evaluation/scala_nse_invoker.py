"""Helper module to invoke Scala NSE aggregator from Python."""
from typing import List
from pyspark.sql import SparkSession


def invoke_scala_nse_sql(
    spark: SparkSession,
    primary: List[float],
    secondary: List[float],
    transform: str = "none",
    add_epsilon: bool = False,
) -> float:
    """
    Compute NSE using Scala aggregator directly on the driver machine.

    This method calls the Scala NashSutcliffeAggregator directly without
    trying to distribute the computation via RDD.

    For testing and simple cases, this works well. For production distributed
    aggregation, would need Spark SQL registered functions.

    Args:
        spark: SparkSession with Scala JAR on classpath
        primary: List of primary (observed) values
        secondary: List of secondary (forecast) values
        transform: Transform type ("none", "log", "sqrt", etc.)
        add_epsilon: Whether to add epsilon for numerical stability

    Returns:
        NSE value as float
    """
    if not primary or len(primary) != len(secondary):
        import numpy as np
        return float('nan')

    try:
        # Access JVM
        sc = spark.sparkContext
        jvm = sc._jvm

        # Get Scala aggregator class and create instance
        transform_enum = _get_transform_enum(jvm, transform)
        aggregator_class = jvm.com.rti.teehr.aggregations.NashSutcliffeAggregator
        aggregator = aggregator_class(transform_enum, add_epsilon)

        # Accumulate values directly (no RDD, just driver-side iteration)
        state = aggregator.zero()

        for p, s in zip(primary, secondary):
            # Create a Scala Tuple2 for the aggregator
            # The aggregator's reduce method expects (Double, Double) which becomes Tuple2
            p_java = jvm.java.lang.Double(float(p))
            s_java = jvm.java.lang.Double(float(s))
            tuple_val = jvm.scala.Tuple2(p_java, s_java)

            # Call reduce to accumulate
            state = aggregator.reduce(state, tuple_val)

        # Get final NSE value
        nse_value = aggregator.finish(state)

        return float(nse_value)

    except Exception as e:
        print(f"Error with Scala aggregator: {e}")
        import traceback
        traceback.print_exc()
        raise


def _get_transform_enum(jvm, transform: str):
    """Get Scala Transform enum value from string name.

    Scala enumerations use the Transform$.MODULE$ singleton pattern
    to access the object, then access the value via its name.
    """
    enum_map = {
        "none": "None",
        "log": "Log",
        "sqrt": "Sqrt",
        "square": "Square",
        "cube": "Cube",
        "exp": "Exp",
        "inv": "Inv",
        "abs": "Abs",
    }

    enum_name = enum_map.get(transform.lower())
    if enum_name is None:
        raise ValueError(f"Unknown transform: {transform}. Valid values: {list(enum_map.keys())}")

    # Access the Transform object singleton via Scala's MODULE pattern
    # In Scala, object Foo becomes Foo$ class with MODULE$ field
    transform_obj_class = getattr(jvm.com.rti.teehr.aggregations, "Transform$")
    transform_module = getattr(transform_obj_class, "MODULE$")

    # Get the enum value (e.g., transform_module.None())
    enum_value = getattr(transform_module, enum_name)
    if callable(enum_value):
        enum_value = enum_value()

    return enum_value


if __name__ == "__main__":
    # Test the helper
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test-nse").getOrCreate()

    # Test with perfect forecast
    primary = [1.0, 2.0, 3.0, 4.0, 5.0]
    secondary = [1.0, 2.0, 3.0, 4.0, 5.0]

    try:
        nse = invoke_scala_nse_sql(spark, primary, secondary)
        print(f"NSE (perfect forecast): {nse}")
    except Exception as e:
        print(f"RDD aggregation approach failed: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()
