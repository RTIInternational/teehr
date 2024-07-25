import pyspark
import urllib.request

SPARK_HOME = pyspark.__path__[0]
print(SPARK_HOME)

SPARK_VERSION = pyspark.__version__
print(SPARK_VERSION)

jars = [
    {
        "url": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
        "path": f"{SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"
    },
    {
        "url": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
        "path": f"{SPARK_HOME}/jars/hadoop-aws-3.3.4.jar"
    }
]

for jar in jars:
    print(f"Downloading {jar['url']} to {jar['path']}")
    urllib.request.urlretrieve(jar['url'], jar['path'])
    print(f"Downloaded {jar['url']} to {jar['path']}")
