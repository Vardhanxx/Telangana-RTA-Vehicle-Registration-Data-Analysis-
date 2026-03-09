import sys
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Mock dlt before importing main
mock_dlt = MagicMock()
sys.modules["dlt"] = mock_dlt

# Import the logic to test
# We need to mock SparkSession.builder.getOrCreate() because main.py calls it at top level
with patch("pyspark.sql.SparkSession.builder.getOrCreate") as mock_get_spark:
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark
    # Also mock spark.conf.get for parameters
    mock_spark.conf.get.side_effect = lambda k: "/tmp/source" if k == "source_path" else "default"
    
    from trvrda_etl.transformations.main import rta_bronze

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("MappingTest").getOrCreate()

def test_rta_bronze_mapping_standard_names(spark):
    # Test case 1: Source data uses standard names (the bug case)
    data = [("REG123", "OFF1", "2023-01-01")]
    columns = ["tempRegistrationNumber", "OfficeCd", "fromdate"]
    df = spark.createDataFrame(data, columns)
    
    # Mock readStream.load to return our test DF
    mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = df
    
    # Execute bronze transformation
    result_df = rta_bronze()
    
    # Verify results
    result = result_df.collect()[0]
    assert result.tempRegistrationNumber == "REG123"
    assert result.OfficeCd == "OFF1"
    assert result.fromdate == "2023-01-01"

def test_rta_bronze_mapping_synonyms(spark):
    # Test case 2: Source data uses synonyms
    data = [("REG456", "OFF2", "2023-02-02")]
    columns = ["regnno", "officecode", "vehicleregndate"]
    df = spark.createDataFrame(data, columns)
    
    # Mock readStream.load
    mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = df
    
    # Execute bronze transformation
    result_df = rta_bronze()
    
    # Verify results
    result = result_df.collect()[0]
    assert result.tempRegistrationNumber == "REG456"
    assert result.OfficeCd == "OFF2"
    assert result.fromdate == "2023-02-02"

def test_rta_bronze_mapping_mixed(spark):
    # Test case 3: Mixed standard names and synonyms
    data = [("REG789", "OFF3", "2023-03-03")]
    # tempRegistrationNumber (standard), office_cd (synonym), from_date (synonym)
    columns = ["tempRegistrationNumber", "office_cd", "from_date"]
    df = spark.createDataFrame(data, columns)
    
    # Mock readStream.load
    mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = df
    
    # Execute bronze transformation
    result_df = rta_bronze()
    
    # Verify results
    result = result_df.collect()[0]
    assert result.tempRegistrationNumber == "REG789"
    assert result.OfficeCd == "OFF3"
    assert result.fromdate == "2023-03-03"
