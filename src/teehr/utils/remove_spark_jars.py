"""Remove local AWS jars that interfere with pyspark v4.0."""
import pyspark
import os


def main():
    """Remove local AWS spark jars."""
    SPARK_HOME = pyspark.__path__[0]
    print(f"SPARK_HOME is: {SPARK_HOME}")

    SPARK_VERSION = pyspark.__version__
    print(f"SPARK_VERSION is: {SPARK_VERSION}")

    if int(SPARK_VERSION[0]) < 4:
        print("This script is intended for Spark 4.0 and above. Exiting.")
        return

    jars = [
        {
            "path": f"{SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"
        },
        {
            "path": f"{SPARK_HOME}/jars/hadoop-aws-3.3.4.jar"
        }
    ]

    for jar in jars:
        if os.path.exists(jar['path']):
            os.remove(jar['path'])
            print(f"Removed {jar['path']}")
        else:
            print(f"{jar['path']} does not exist, skipping removal.")


if __name__ == "__main__":
    main()