import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, regexp_replace, coalesce, to_date, upper, when, row_number, regexp_extract, lower, concat, explode, date_format, substring, levenshtein, length, abs
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import Window

spark = SparkSession.builder.getOrCreate()

# Access pipeline parameters
source_path = spark.conf.get("source_path")
schema_name = spark.conf.get("my_target_schema")
catalog_name = spark.conf.get("my_target_catalog")

# --- Bronze Table ---
@dlt.table(
    name="rta_bronze",
    comment="Raw ingestion from CSV with dynamic synonym mapping to prevent schema collapse."
)
def rta_bronze():
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
    raw_df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("cloudFiles.schemaLocation", f"{source_path}/_checkpoints")
                .load(source_path))
    file_columns = {c.lower().strip(): c for c in raw_df.columns}
    final_selections = []
    for target, synonyms in mapping.items():
        match = next((s for s in synonyms if s in file_columns), None)
        if match:
            actual_name = file_columns[match]
            final_selections.append(col(f"`{actual_name}`").alias(target))
        else:
            final_selections.append(lit(None).cast(StringType()).alias(target))
    return raw_df.select(*final_selections)

# --- Silver Table ---
UNKNOWN_STR = "UNKNOWN"
DATE_LIKE = r"^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[12]\d{3}[01]\d{1}[0-3]\d{1})\s*$"
OFFICE_CODE_LIKE = r"^(TS|TG)?\d{1,4}$"

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

def apply_reverse_rta_lookup(df, col_name):
    c = upper(trim(col(col_name)))
    return df.withColumn(col_name,
        when(c.contains("ADILABAD"), "TS01")
        .when(c.contains("NIRMAL"), "TS02")
        .when(c.contains("KARIMNAGAR"), "TS04")
        .when(c.contains("HYDERABAD-CZ"), "TS19")
        .otherwise(c)
    )

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
    df = df.withColumn("makerName", when(col("makerName").isNull() | (col("makerName") == ""), lit(f"{UNKNOWN_STR}_MANUFACTURER")).otherwise(upper(regexp_replace(col("makerName"), r"\s+", " "))))
    df = df.withColumn("fuel_clean", upper(
        when(col("fuel").rlike(r"(?i)HYBRID|AHYBRID|((PETROL|CNG|LPG|DIESEL).*(ELECTRIC|BATTERY))|((ELECTRIC|BATTERY).*(PETROL|CNG|LPG|DIESEL))"), "HYBRID")
        .when(col("fuel").rlike(r"(?i)BATTERY|ELECTRIC"), "ELECTRIC")
        .when(col("fuel").rlike(r"(?i)PETROL|GASOLINE"), "PETROL")
        .when(col("fuel").rlike(r"(?i)DIESEL"), "DIESEL")
        .otherwise("UNKNOWN")
    ))
    df = df.withColumn("OWNERSHIP_TYPE", when(lower(trim(coalesce(col("secondVehicle"), lit("")))).isin("y","yes","1"), lit("PRE_OWNED")).otherwise(lit("NEW")))
    df = df.withColumn("TRANSPORT_TYPE", when(col("vehicleClass").rlike(r"(?i)CONTRACT|STAGE|GOODS|BUS|TAXI|AUTORICKSHAW|TRANSPORT"), lit("TRANSPORT")).otherwise(lit("NON_TRANSPORT")))
    df = df.withColumn("fromdate_parsed", parse_to_date_expr("fromdate"))
    df = df.withColumn("todate_parsed", parse_to_date_expr("todate"))
    df = apply_reverse_rta_lookup(df, "OfficeCd")
    df = apply_rta_standardization(df, "OfficeCd")
    w = Window.partitionBy("tempRegistrationNumber").orderBy(coalesce(col("fromdate_parsed"), col("todate_parsed")).desc_nulls_last())
    return df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

# --- Gold Table ---
LEV_THRESHOLD = 3

def normalize_text_strict(c):
    return regexp_replace(lower(trim(c)), r"[^a-z0-9]", "")

@dlt.table(name="dim_date_gold")
def dim_date_gold():
    return spark.sql("SELECT sequence(to_date('2000-01-01'), to_date('2030-12-31'), interval 1 day) as d") \
                .select(explode(col("d")).alias("full_date")) \
                .unionByName(spark.createDataFrame([("1900-01-01",)], ["d"]).select(to_date(col("d")).alias("full_date"))) \
                .withColumn("DATE_ID", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))

@dlt.table(name="dim_manufacturer")
def dim_manufacturer():
    df = dlt.read("rta_silver").select(col("makerName").alias("MANUFACTURER_NAME")).dropDuplicates()
    return df.withColumn("MANUFACTURER_KEY", concat(lit("M"), row_number().over(Window.orderBy("MANUFACTURER_NAME"))).cast(StringType()))

@dlt.table(name="dim_vehicle")
def dim_vehicle():
    df = dlt.read("rta_silver").select("modelDesc", "vehicleClass", "fuel_clean").dropDuplicates()
    return df.withColumn("VEHICLE_KEY", row_number().over(Window.orderBy("modelDesc")).cast(IntegerType()))

@dlt.table(name="dim_rta")
def dim_rta():
    rta_master_data = [
        ("TS01", "Adilabad"), ("TS02", "Nirmal"), ("TS03", "Mancherial"),
        ("TS04", "Karimnagar"), ("TS05", "Jagtial"), ("TS06", "Peddapalli"),
        ("TS07", "Ramagundam (Godavarikhani)"), ("TS08", "Warangal Urban (Hanamkonda)"),
        ("TS09", "Warangal Rural (Parkal)"), ("TS10", "Mahabubabad"), ("TS11", "Jangaon"),
        ("TS12", "Kothagudem"), ("TS13", "Khammam"), ("TS14", "Khammam (Wyra)"),
        ("TS15", "Nalgonda"), ("TS16", "Suryapet"), ("TS17", "Bhongir / Yadadri Bhuvanagiri"),
        ("TS18", "Huzurnagar"), ("TS19", "Hyderabad Central"), ("TS20", "Hyderabad North"),
        ("TS21", "Hyderabad East"), ("TS22", "Hyderabad West"), ("TS23", "Hyderabad South"),
        ("TS24", "Hyderabad Secunderabad"), ("TS25", "Ranga Reddy (Vikarabad)"),
        ("TS26", "Ranga Reddy (Sanga Reddy"), ("TS27", "Ranga Reddy (Medchal)"),
        ("TS28", "Ranga Reddy (Balanagar)"), ("TS29", "Mahbubnagar"), ("TS30", "Wanaparthy"),
        ("TS31", "Nagarkurnool"), ("TS32", "Gadwal"), ("TS33", "Narayanpet"),
        ("TS34", "Medak"), ("TS35", "Siddipet"), ("TS36", "Zaheerabad"),
        ("TS37", "Nizamabad"), ("TS38", "Kamareddy"), ("TS39", "Komaram Bheem (Asifabad)"),
        ("TS40", "Jayashankar Bhupalpally"), ("TS41", "Mulugu"), ("TS42", "Jogulamba Gadwal"),
        ("TS43", "Rajanna Sircilla"), ("TS44", "Vikarabad"), ("TS45", "Sanga Reddy (Patancheru)"),
        ("TS46", "Shameerpet (Medchal-Malkajgiri)"), ("TS47", "Cherial (Siddipet)"),
        ("TS48", "Bhadradri Kothagudem"), ("TS49", "Warangal (Parvathagiri)"),
        ("TS50", "Peddapalli (Sulthanabad)"), ("TS51", "Nizamabad (Bodhan)"),
        ("TS102", "Unit Office Huzurabad"), ("TS107", "RTA Ibrahimpatnam"),
        ("TS108", "RTA Uppal"), ("TS116", "Unit Office Armoor"),
        ("TS121", "Unit Office Korutla"), ("TS122", "Unit Office Ramagundam"),
        ("TS128", "Unit Office Bhadrachalam"), ("TS129", "Unit Office Kodad"),
        ("TS131", "Unit Office Kalwakurthy"), ("TS132", "Unit Office Pebbair"),
        ("TS134", "Unit Office Pargi"), ("TS207", "Unit Office Shadnagar"),
        ("TS208", "Unit Office Kukatpally"), ("TS215", "Unit Office Zahirabad"),
        ("TS216", "Unit Office Bhodan"), ("TS304", "Unit Office Sattupalli"),
        ("TS305", "Unit Office Miryalaguda"), ("TS404", "Unit Office Wyra"),
        ("TS415", "Unit Office Patancheruvu")
    ]
    rta_master_map = spark.createDataFrame(rta_master_data, ["MAP_CODE", "DISTRICT_NAME"])
    unique_rta_codes = dlt.read("rta_silver").select(col("OfficeCd_STD").alias("RTA_CODE")).dropDuplicates()
    rta_dim_df = unique_rta_codes.join(rta_master_map, unique_rta_codes.RTA_CODE == rta_master_map.MAP_CODE, "left") \
        .select(col("RTA_CODE").alias("RTA_OFFICE_CODE"), coalesce(col("DISTRICT_NAME"), lit("UNKNOWN")).alias("RTA_NAME")) \
        .filter(col("RTA_OFFICE_CODE").isNotNull())
    rta_dim = rta_dim_df.withColumn("RTA_ID", concat(lit("R"), row_number().over(Window.orderBy("RTA_OFFICE_CODE"))).cast(StringType()))
    unknown_rta_df = spark.createDataFrame([(UNKNOWN_STR, "UNK", "Unknown / Unmapped RTA")], ["RTA_ID", "RTA_OFFICE_CODE", "RTA_NAME"])
    return rta_dim.unionByName(unknown_rta_df)

@dlt.table(name="fact_fuzzy_registrations", comment="Fact table with optimized Levenshtein fuzzy matching applied.")
def fact_fuzzy_registrations():
    silver = dlt.read("rta_silver")
    dv = dlt.read("dim_vehicle")
    stg_keys = silver.select("tempRegistrationNumber", "modelDesc").dropDuplicates(["tempRegistrationNumber"]) \
        .withColumn("MN", normalize_text_strict(col("modelDesc"))) \
        .withColumn("BLOCK", substring(col("MN"), 1, 4))
    dv_keys = dv.select("VEHICLE_KEY", "modelDesc") \
        .withColumn("MN2", normalize_text_strict(col("modelDesc"))) \
        .withColumn("BLOCK2", substring(col("MN2"), 1, 4))
    exact = stg_keys.join(dv_keys, stg_keys.MN == dv_keys.MN2, "inner") \
        .select("tempRegistrationNumber", "VEHICLE_KEY") \
        .withColumn("MATCH_STATUS", lit("EXACT"))
    unresolved = stg_keys.join(exact.select("tempRegistrationNumber"), "tempRegistrationNumber", "left_anti")
    u_alias = unresolved.alias("u")
    d_alias = dv_keys.alias("d")
    candidates = u_alias.join(
        d_alias,
        (col("u.BLOCK") == col("d.BLOCK2")) & (abs(length(col("u.MN") ) - length(col("d.MN2"))) <= LEV_THRESHOLD),
        "inner"
    )
    matches = candidates.withColumn("LEV", levenshtein(col("u.MN"), col("d.MN2"))).filter(col("LEV") <= LEV_THRESHOLD)
    w = Window.partitionBy("u.tempRegistrationNumber").orderBy(col("LEV").asc(), col("d.VEHICLE_KEY").asc())
    best_matches = matches.withColumn("rn", row_number().over(w)).filter(col("rn") == 1) \
        .select(col("u.tempRegistrationNumber"), col("d.VEHICLE_KEY")) \
        .withColumn("MATCH_STATUS", lit("FUZZY"))
    resolved_keys = exact.unionByName(best_matches)
    fact_df = silver.join(resolved_keys, "tempRegistrationNumber", "left")
    fact_df = fact_df.withColumn("VEHICLE_KEY", coalesce(col("VEHICLE_KEY"), lit(-1).cast(IntegerType()))) \
                     .withColumn("MATCH_STATUS", coalesce(col("MATCH_STATUS"), lit("UNMATCHED")))
    fact_df = fact_df.withColumn("DATE_ID", date_format(col("fromdate_parsed"), "yyyyMMdd").cast(IntegerType()))
    dim_date = dlt.read("dim_date_gold")
    fact_df = fact_df.join(dim_date, ["DATE_ID"], "left")
    dim_rta_df = dlt.read("dim_rta")
    fact_df = fact_df.join(dim_rta_df, fact_df["OfficeCd_STD"] == dim_rta_df["RTA_OFFICE_CODE"], "left")
    return fact_df
