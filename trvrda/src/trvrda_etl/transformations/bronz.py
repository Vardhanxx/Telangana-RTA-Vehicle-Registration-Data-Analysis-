import dlt
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

# Fetch the value assigned to the key in YAML
spark = SparkSession.builder.getOrCreate()
source_path = spark.conf.get("source_path")
schema_name = spark.conf.get("my_target_schema")
catalog_name = spark.conf.get("my_target_catalog")

@dlt.table(
    name="rta_bronze",
    comment="Raw ingestion from CSV with dynamic synonym mapping to prevent schema collapse."
)
def rta_bronze():
    # Canonical Mapping to prevent schema collapse
    mapping = {
        "tempRegistrationNumber": ["regnno", "regno", "tempregnno", "registration_no", "reg_id"],
        "OfficeCd": ["officecode", "office_cd", "office.cd", "office.code", "office"],
        "fromdate": ["vehicleregndate", "regn_date", "regn.date", "from_date"],
        "todate": ["vehregnvaliddate", "to_date", "regn_to_date"],
        "seatCapacity": ["seatingcapacity", "seat_capacity", "seating.capacity", "seat_cap", "seatcapacity"],
        "vehicleClass": ["vehicleclass", "classofveh", "vehicle.class"],
        "modelDesc": ["modeldesc", "model.description", "model_desc"],
        "makerName": ["maker", "makername", "manufacturer", "manufacturername", "manufacturer_name"],
        "fuel": ["fueltype", "fuel.type", "fuel_used"],
        "secondVehicle": ["secondvehicle", "issecondvehicle", "second.vehicle"],
        "category": ["category", "veh.category"],
        "colour": ["colour", "color"],
        "makeYear": ["makeyear", "make_year", "mfg_year"],
        "transportType": ["transporttype", "transport_type"],
        "OWNERSHIP_TYPE": ["ownershiptype", "ownership_type"]
    }
    
    # Auto Loader for robust file streaming
    raw_df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("cloudFiles.schemaLocation", f"{source_path}/_checkpoints")
                .load(source_path))

    # Identify which columns actually exist in the currently processed file
    file_columns = {c.lower().strip(): c for c in raw_df.columns}
    final_selections = []

    for target, synonyms in mapping.items():
        match = next((s for s in synonyms if s in file_columns), None)
        if match:
            actual_name = file_columns[match]
            # Backticks protect against dots or spaces in raw column names
            final_selections.append(col(f"`{actual_name}`").alias(target))
        else:
            final_selections.append(lit(None).cast(StringType()).alias(target))

    return raw_df.select(*final_selections)
