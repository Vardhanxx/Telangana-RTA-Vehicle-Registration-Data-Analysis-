import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Access pipeline parameters
source_path = spark.conf.get("source_path")
schema_name = spark.conf.get("my_target_schema")
catalog_name = spark.conf.get("my_target_catalog")

@dlt.table(
    name="test_table",
    comment="Basic DLT table for pipeline validation"
)
def test_table():
    # Reads a CSV from the source path (for validation/demo)
    return spark.read.format("csv").option("header", "true").load(source_path)