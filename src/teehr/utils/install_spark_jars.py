"""Download and copy AWS jars to SPARK install."""
import pyspark
import urllib.request
import bs4


def main():
    """Download and copy AWS jars to SPARK install."""
    # obtain jars
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

    # obtain hadoop dependencies
    #repo_url = "https://github.com/ruslanmv/How-to-install-Hadoop-on-Windows/tree/master/winutils/hadoop-3.3.0-YARN-8246/bin" # noqa
    #repo_url = "https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin" #noqa
    #repo_url = "https://github.com/ruslanmv/How-to-install-Hadoop-on-Windows/tree/master/winutils/hadoop-3.3.0/bin" # noqa
    repo_url = "https://github.com/ruslanmv/How-to-install-Hadoop-on-Windows/tree/master/winutils/hadoop-3.3.1/bin" # noqa
    response = urllib.request.urlopen(repo_url)
    html = response.read()
    data = bs4.BeautifulSoup(html, "html.parser")
    table = data.find("table", {"aria-labelledby": "folders-and-files"})
    if table:
        for tr in table.find_all("tr"):
            if tr.get("id") == "folder-row-0":
                continue
            for l in tr.find_all("a"):
                link = 'https://github.com' + l["href"]
                req = urllib.request.Request(link)
                link_parts = link.split("/")
                with urllib.request.urlopen(req) as r:
                    if r.status == 200:
                        print(f"Downloading {link} to {SPARK_HOME}/bin/{link_parts[-1]}")
                        urllib.request.urlretrieve(link, f"{SPARK_HOME}/bin/{link_parts[-1]}")
                        print(f"Downloaded {link} to {SPARK_HOME}/bin/{link_parts[-1]}")

if __name__ == "__main__":
    main()