import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

UNKNOWN_STR = "UNKNOWN"
DATE_LIKE = r"^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[12]\d{3}[01]\d{1}[0-3]\d{1})\s*$"
OFFICE_CODE_LIKE = r"^(TS|TG)?\d{1,4}$"

# Helper: Robust date parsing
def parse_to_date_expr(colname):
    trimmed = trim(col(colname).cast("string"))
    norm = regexp_replace(trimmed, r"[-.\\]", "/")
    return coalesce(
        to_date(norm, "dd/MM/yyyy"), to_date(norm, "d/M/yyyy"),
        to_date(norm, "yyyy/MM/dd"), to_date(norm, "MM/dd/yyyy"),
        to_date(norm, "M/d/yyyy"), to_date(norm, "yyyy-MM-dd"),
        to_date(norm, "yyyyMMdd"), to_date(norm, "dd/MM/yy"),
        to_date(norm, "MM/dd/yy"), lit(None).cast("date")
    )

# Helper: RTA reverse lookup
def apply_reverse_rta_lookup(df, col_name):
    c = upper(trim(col(col_name)))
    return df.withColumn(col_name,
        when(c.contains("ADILABAD"), "TS01")
        .when(c.contains("NIRMAL"), "TS02")
        .when(c.contains("KARIMNAGAR"), "TS04")
        .when(c.contains("HYDERABAD-CZ"), "TS19")
        .otherwise(c)
    )

# Helper: RTA code normalization
def apply_rta_standardization(df, col_name):
    c = upper(trim(col(col_name)))
    return df.withColumn(col_name + "_STD",
        when(c.rlike("^(TS|TG)00[1-8]$"), regexp_replace(c, "00", "0")) 
        .when(c.rlike("^(TS|TG)0[1-9][0-9]$"), regexp_replace(c, "(TS|TG)0", "$1"))
        .otherwise(regexp_replace(c, "^TG", "TS"))
    )

@dlt.table(name="rta_silver")
@dlt.expect_or_drop("has_id", "tempRegistrationNumber IS NOT NULL")
def rta_silver():
    df = dlt.read("rta_bronze")
    
    # 1. Shift/Swap Logic
    df = df.withColumn("is_swapped", trim(col("OfficeCd")).rlike(DATE_LIKE) & upper(trim(col("fromdate"))).rlike(OFFICE_CODE_LIKE)) \
           .withColumn("is_shifted", (trim(col("OfficeCd")).rlike(OFFICE_CODE_LIKE) | col("OfficeCd").isNull() | (trim(col("OfficeCd")) == "")) & trim(col("fromdate")).rlike(r"(?i)[A-Z]{3,}") & trim(col("todate")).rlike(DATE_LIKE))
    
    df = df.withColumn("OfficeCd_temp", when(col("is_shifted") | col("is_swapped"), trim(col("fromdate"))).otherwise(col("OfficeCd"))) \
           .withColumn("fromdate_temp", when(col("is_shifted"), col("todate")).when(col("is_swapped"), col("OfficeCd")).otherwise(col("fromdate"))) \
           .withColumn("todate_temp", when(col("is_shifted"), col("todate")).when(col("is_swapped"), lit(None).cast("string")).otherwise(col("todate")))
    
    df = df.withColumn("OfficeCd_final", when(col("OfficeCd_temp").isNull() | (trim(col("OfficeCd_temp")) == ""), regexp_extract(col("tempRegistrationNumber"), r'^(TS|TG)\d{1,3}', 0)).otherwise(col("OfficeCd_temp")))
    df = df.drop("OfficeCd","fromdate","todate", "is_swapped", "is_shifted", "OfficeCd_temp") \
             .withColumnRenamed("OfficeCd_final","OfficeCd") \
             .withColumnRenamed("fromdate_temp","fromdate") \
             .withColumnRenamed("todate_temp","todate")
    
    # 2. Manufacturer & Fuel Cleaning
    df = df.withColumn("makerName", when(col("makerName").isNull() | (col("makerName") == ""), lit(f"{UNKNOWN_STR}_MANUFACTURER")).otherwise(upper(regexp_replace(col("makerName"), r"\s+", " "))))
    df = df.withColumn("fuel_clean", upper(
        when(col("fuel").rlike(r"(?i)HYBRID|AHYBRID|((PETROL|CNG|LPG|DIESEL).*(ELECTRIC|BATTERY))|((ELECTRIC|BATTERY).*(PETROL|CNG|LPG|DIESEL))"), "HYBRID")
        .when(col("fuel").rlike(r"(?i)BATTERY|ELECTRIC"), "ELECTRIC")
        .when(col("fuel").rlike(r"(?i)PETROL|GASOLINE"), "PETROL")
        .when(col("fuel").rlike(r"(?i)DIESEL"), "DIESEL")
        .otherwise("UNKNOWN")
    ))
    
    # 3. Ownership & Transport Types
    df = df.withColumn("OWNERSHIP_TYPE", when(lower(trim(coalesce(col("secondVehicle"), lit("")))).isin("y","yes","1"), lit("PRE_OWNED")).otherwise(lit("NEW")))
    df = df.withColumn("TRANSPORT_TYPE", when(col("vehicleClass").rlike(r"(?i)CONTRACT|STAGE|GOODS|BUS|TAXI|AUTORICKSHAW|TRANSPORT"), lit("TRANSPORT")).otherwise(lit("NON_TRANSPORT")))
    
    # 4. Dates & RTA
    df = df.withColumn("fromdate_parsed", parse_to_date_expr("fromdate"))
    df = df.withColumn("todate_parsed", parse_to_date_expr("todate"))
    df = apply_reverse_rta_lookup(df, "OfficeCd")
    df = apply_rta_standardization(df, "OfficeCd")
    
    # 5. Deduplication
    w = Window.partitionBy("tempRegistrationNumber").orderBy(coalesce(col("fromdate_parsed"), col("todate_parsed")).desc_nulls_last())
    return df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

@dlt.table(name="metrics_bronze")
def metrics_bronze():
    df = dlt.read("rta_bronze")
    null_counts = {c: df.filter(col(c).isNull()).count() for c in df.columns}
    unknown_counts = {c: df.filter(col(c) == UNKNOWN_STR).count() for c in df.columns}
    if "makerName" in df.columns:
        unknown_counts["makerName"] = df.filter(col("makerName") == UNKNOWN_STR + "_MANUFACTURER").count()
    metrics = {f"null_{c}": v for c, v in null_counts.items()}
    metrics.update({f"unknown_{c}": v for c, v in unknown_counts.items()})
    return spark.createDataFrame([metrics])

@dlt.table(name="metrics_silver")
def metrics_silver():
    df = dlt.read("rta_silver")
    null_counts = {c: df.filter(col(c).isNull()).count() for c in df.columns}
    unknown_counts = {c: df.filter(col(c) == UNKNOWN_STR).count() for c in df.columns}
    if "makerName" in df.columns:
        unknown_counts["makerName"] = df.filter(col("makerName") == UNKNOWN_STR + "_MANUFACTURER").count()
    metrics = {f"null_{c}": v for c, v in null_counts.items()}
    metrics.update({f"unknown_{c}": v for c, v in unknown_counts.items()})
    return spark.createDataFrame([metrics])

@dlt.materialized_view(name="bronze_row_count")
def bronze_row_count():
    df = dlt.read("rta_bronze")
    return df.agg(count("*").alias("bronze_count"))

@dlt.materialized_view(name="silver_row_count")
def silver_row_count():
    df = dlt.read("rta_silver")
    return df.agg(count("*").alias("silver_count"))

@dlt.materialized_view(name="fact_row_count")
def fact_row_count():
    df = spark.read.table("fact_fuzzy_registrations")
    return df.agg(count("*").alias("fact_count"))
