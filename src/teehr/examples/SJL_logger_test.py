import teehr
from pathlib import Path
import shutil
import logging

# define pathing
test_eval_dir = Path(Path().home(), "temp", "logging_test", "test_eval")
shutil.rmtree(test_eval_dir, ignore_errors=True)

# create evaluation object
ev = teehr.Evaluation(dir_path=test_eval_dir,
                      create_dir=True)
ev.enable_logging()

# Test default teehr logging output
logger = logging.getLogger("teehr")
logger.info("Test log message: TEEHR logging is working!")
logger.debug("Test debug message: This is a debug level message")
logger.warning("Test warning message: This is a warning level message")

# Test PySpark logging output by performing operations that generate logs
print("Performing PySpark operations to generate logs...")

# Create a simple DataFrame to trigger PySpark/py4j activity
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["name", "age"]
df = ev.spark.createDataFrame(data, columns)

# Perform some operations that should generate logs
df.show()  # This should trigger logging
df.count()  # This should also trigger logging
df.filter(df.age > 25).collect()  # More operations

# Run a simple SQL query to generate more logging
ev.spark.sql("SELECT 1 as test_column").show()

print("PySpark operations completed. Check teehr.log for output.")

# kill spark
ev.spark.stop()
