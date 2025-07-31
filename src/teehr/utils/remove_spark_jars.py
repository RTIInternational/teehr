"""Remove local AWS jars that interfere with pyspark v4.0."""
import pyspark
import os
import glob


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
            "path": f"{SPARK_HOME}/jars/aws-java-sdk-bundle-*.jar"
        },
        {
            "path": f"{SPARK_HOME}/jars/hadoop-aws-*.jar"
        }
    ]

    for jar in jars:
        files_to_delete = glob.glob(jar['path'])
        for file_path in files_to_delete:
            os.remove(file_path)
            print(f"Removed {file_path}")


if __name__ == "__main__":
    main()