"""Download and copy AWS jars to SPARK install."""
import pyspark
import urllib.request


def main():
    """Download and copy AWS jars to SPARK install."""
    SPARK_HOME = pyspark.__path__[0]
    print(f"SPARK_HOME is: {SPARK_HOME}")

    SPARK_VERSION = pyspark.__version__
    print(f"SPARK_VERSION is: {SPARK_VERSION}")

    jars = [
        {
            "url": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar", # noqa
            "path": f"{SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"
        },
        {
            "url": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar", # noqa
            "path": f"{SPARK_HOME}/jars/hadoop-aws-3.3.4.jar"
        }
    ]

    for jar in jars:
        print(f"Downloading {jar['url']} to {jar['path']}")
        urllib.request.urlretrieve(jar['url'], jar['path'])
        print(f"Downloaded {jar['url']} to {jar['path']}")


if __name__ == "__main__":
    main()