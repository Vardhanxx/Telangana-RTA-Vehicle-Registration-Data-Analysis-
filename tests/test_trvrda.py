import sys
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================
# 1. MOCK DLT FRAMEWORK (Must happen first)
# ==========================================
mock_dlt = MagicMock()
mock_dlt._table_funcs = {}

def dlt_table_mock(name=None, **kwargs):
    def decorator(func):
        # Store the function by its DLT table name for testing access
        table_name = name if name else func.__name__
        mock_dlt._table_funcs[table_name] = func
        return func
    return decorator

mock_dlt.table.side_effect = dlt_table_mock
sys.modules["dlt"] = mock_dlt

# ==========================================
# 2. DLT PIPELINE LOGIC (Gold Layer)
# ==========================================
import dlt # This now refers to our mock_dlt

MANUFACTURER_JUNK_PATTERN = "(?i)NA|N/A|NULL|NONE|JUNK"

def normalize_text_strict(col):
    return F.trim(F.upper(col))

@dlt.table(name="dim_rta")
def dim_rta():
    df = dlt.read("transport_silver")
    rta = df.select(F.upper(F.trim(F.col("OfficeCd"))).alias("RTA_NAME")).dropDuplicates()
    rta = rta.filter(~F.col("RTA_NAME").rlike(MANUFACTURER_JUNK_PATTERN))
    w = Window.orderBy("RTA_NAME")
    return rta.withColumn("RTA_ID", F.row_number().over(w).cast("int"))

@dlt.table(name="dim_manufacturer")
def dim_manufacturer():
    df = dlt.read("transport_silver")
    maker = df.select(F.col("MAKER_NAME_CLEAN").alias("MANUFACTURER_NAME")).dropDuplicates()
    maker = maker.filter(~F.col("MANUFACTURER_NAME").rlike(MANUFACTURER_JUNK_PATTERN))
    w = Window.orderBy("MANUFACTURER_NAME")
    return maker.withColumn("MANUFACTURER_ID", F.row_number().over(w).cast("int"))

@dlt.table(name="dim_vehicle")
def dim_vehicle():
    df = dlt.read("transport_silver")
    vehicle_dim = df.select(
        F.concat_ws(" | ", normalize_text_strict(F.col("modelName_canonical")), 
                    normalize_text_strict(F.col("variant"))).alias("VEHICLE_NATURAL_KEY"),
        F.col("modelName_canonical").alias("MODEL_NAME"),
        F.col("variant").alias("VARIANT"),
        F.col("fuel_clean").alias("FUEL"),
        F.col("colour").alias("COLOUR"),
        F.col("vehicleClass").alias("VEHICLE_CLASS"),
        F.when(F.lower(F.col("fuel_clean")) == "electric", True).otherwise(False).alias("IS_ELECTRIC"),
        F.col("OWNERSHIP_TYPE"),
        F.col("TRANSPORT_TYPE"),
        F.col("REGISTRATION_YEAR").cast("int")
    ).dropDuplicates(["VEHICLE_NATURAL_KEY"])
    return vehicle_dim.withColumn("VEHICLE_KEY", F.col("VEHICLE_NATURAL_KEY"))

@dlt.table(name="dim_date")
def dim_date():
    silver = dlt.read("transport_silver")
    date_dim = silver.select(F.col("fromdate_parsed").alias("DATE")).filter(F.col("DATE").isNotNull()).dropDuplicates()
    w = Window.orderBy("DATE")
    return date_dim.withColumn("DATE_KEY", F.row_number().over(w).cast("int")) \
                   .withColumn("YEAR", F.year("DATE")) \
                   .withColumn("MONTH", F.month("DATE"))

@dlt.table(name="fact_vehicle_registration")
def fact_vehicle_registration():
    silver = dlt.read("transport_silver")
    dim_rta = dlt.read("dim_rta")
    dim_maker = dlt.read("dim_manufacturer")
    dim_vehicle = dlt.read("dim_vehicle")
    dim_date = dlt.read("dim_date")

    silver = silver.withColumn("VE_NAT_KEY", F.concat_ws(" | ", normalize_text_strict(F.col("modelName_canonical")), 
                                                        normalize_text_strict(F.col("variant"))))

    fact = silver.alias("s") \
        .join(dim_vehicle.alias("v"), F.col("s.VE_NAT_KEY") == F.col("v.VEHICLE_NATURAL_KEY"), "left") \
        .join(dim_rta.alias("r"), F.upper(F.trim(F.col("s.OfficeCd"))) == F.col("r.RTA_NAME"), "left") \
        .join(dim_maker.alias("m"), F.col("s.MAKER_NAME_CLEAN") == F.col("m.MANUFACTURER_NAME"), "left") \
        .join(dim_date.alias("d"), F.col("s.fromdate_parsed") == F.col("d.DATE"), "left")

    return fact.select(
        F.col("s.tempRegistrationNumber").alias("TEMP_REGISTRATION_NUMBER"),
        F.col("v.VEHICLE_KEY"),
        F.col("r.RTA_ID"),
        F.col("m.MANUFACTURER_ID").alias("MANUFACTURER_KEY"),
        F.col("d.DATE_KEY"),
        F.col("s.makeYear").cast("int").alias("MAKE_YEAR"),
        F.col("s.category").alias("CATEGORY")
    )

# ==========================================
# 3. TEST SUITE: tevrda
# ==========================================

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[*]").appName("UnitTesting").getOrCreate()

def test_tevrda(spark_session):
    # 1. Prepare Mock Data
    data = [
        ("TS09", "MARUTI", "SWIFT", "VXI", "PETROL", "2023-01-01", "TMP001", "2022", "Private"),
        ("TS10", "HYUNDAI", "VERNA", "SX", "DIESEL", "2023-01-02", "TMP002", "2021", "Private"),
        ("NA", "JUNK", "OLD", "BASE", "PETROL", "2023-01-03", "TMP003", "2020", "Private")
    ]
    schema = ["OfficeCd", "MAKER_NAME_CLEAN", "modelName_canonical", "variant", 
              "fuel_clean", "fromdate_parsed", "tempRegistrationNumber", "makeYear", "category"]
    
    # Cast necessary columns
    df_silver = spark_session.createDataFrame(data, schema) \
        .withColumn("fromdate_parsed", F.col("fromdate_parsed").cast("date")) \
        .withColumn("colour", F.lit("RED")).withColumn("vehicleClass", F.lit("LMV")) \
        .withColumn("OWNERSHIP_TYPE", F.lit("IND")).withColumn("TRANSPORT_TYPE", F.lit("NT")) \
        .withColumn("REGISTRATION_YEAR", F.lit(2023))

    # Internal state for the mock_read
    tables = {"transport_silver": df_silver}

    def mock_read(name):
        return tables.get(name)

    mock_dlt.read.side_effect = mock_read

    # 2. Execute Dimensions and store them for the Fact Join
    tables["dim_rta"] = mock_dlt._table_funcs["dim_rta"]()
    tables["dim_manufacturer"] = mock_dlt._table_funcs["dim_manufacturer"]()
    tables["dim_vehicle"] = mock_dlt._table_funcs["dim_vehicle"]()
    tables["dim_date"] = mock_dlt._table_funcs["dim_date"]()

    # 3. Execute Fact Table
    df_fact = mock_dlt._table_funcs["fact_vehicle_registration"]()
    
    # 4. Assertions
    results = df_fact.collect()
    assert len(results) == 3
    
    # Check that the Junk record (TMP003) has a NULL Manufacturer Key due to the dimension filter
    row_junk = next(r for r in results if r.TEMP_REGISTRATION_NUMBER == "TMP003")
    assert row_junk.MANUFACTURER_KEY is None
    
    # Check successful join for TMP001
    row_valid = next(r for r in results if r.TEMP_REGISTRATION_NUMBER == "TMP001")
    assert row_valid.RTA_ID is not None
    assert row_valid.VEHICLE_KEY == "SWIFT | VXI"

    print("\n✅ tevrda: All Gold Layer transformations validated!")

if __name__ == "__main__":
    # If running directly, trigger pytest
    pytest.main([__file__])