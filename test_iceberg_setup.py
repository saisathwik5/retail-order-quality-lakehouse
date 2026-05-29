import os
import sys

# Configure JVM options for Spark to run on Java 17/21 before importing or initializing Spark
java_opens = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.security=ALL-UNNAMED"
)

# Set environment variables for the JVM before Spark starts
os.environ["_JAVA_OPTIONS"] = java_opens
os.environ["JDK_JAVA_OPTIONS"] = java_opens

# Use Iceberg runtime for Spark 3.5
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 "
    f"--conf spark.driver.extraJavaOptions=\"{java_opens}\" "
    f"--conf spark.executor.extraJavaOptions=\"{java_opens}\" "
    f"pyspark-shell"
)

from pyspark.sql import SparkSession

def main():
    print("Testing PySpark 3.5 + Iceberg Setup (JVM Options Set in Env)...")
    
    spark = SparkSession.builder \
        .appName("IcebergTest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", os.path.abspath("./test_warehouse")) \
        .getOrCreate()
    
    print("Spark Session created successfully with Iceberg!")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
        spark.sql("CREATE TABLE IF NOT EXISTS local.db.test (id bigint, data string) USING iceberg")
        
        # Clear existing data to keep tests clean
        spark.sql("DELETE FROM local.db.test")
        
        spark.sql("INSERT INTO local.db.test VALUES (1, 'hello'), (2, 'world')")
        print("\nQuerying table:")
        spark.sql("SELECT * FROM local.db.test").show()
        
        # Test time travel / table history
        print("\nTable history:")
        spark.sql("SELECT * FROM local.db.test.history").show()
        
        print("Test passed successfully!")
    except Exception as e:
        print("Test failed with error:", e, file=sys.stderr)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
