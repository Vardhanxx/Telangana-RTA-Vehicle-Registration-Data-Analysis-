import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

UNKNOWN_STR = "UNKNOWN"
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
        ("TS26", "Ranga Reddy (Sanga Reddy)"), ("TS27", "Ranga Reddy (Medchal)"),
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
    # Extract keys for matching
    stg_keys = silver.select("tempRegistrationNumber", "modelDesc").dropDuplicates(["tempRegistrationNumber"]) \
        .withColumn("MN", normalize_text_strict(col("modelDesc"))) \
        .withColumn("BLOCK", substring(col("MN"), 1, 4))
    dv_keys = dv.select("VEHICLE_KEY", "modelDesc") \
        .withColumn("MN2", normalize_text_strict(col("modelDesc"))) \
        .withColumn("BLOCK2", substring(col("MN2"), 1, 4))
    # Exact Matches
    exact = stg_keys.join(dv_keys, stg_keys.MN == dv_keys.MN2, "inner") \
        .select("tempRegistrationNumber", "VEHICLE_KEY") \
        .withColumn("MATCH_STATUS", lit("EXACT"))
    unresolved = stg_keys.join(exact.select("tempRegistrationNumber"), "tempRegistrationNumber", "left_anti")
    # Fuzzy Matches
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
    # LEFT JOIN back to silver for 100% record retention
    fact_df = silver.join(resolved_keys, "tempRegistrationNumber", "left")
    fact_df = fact_df.withColumn("VEHICLE_KEY", coalesce(col("VEHICLE_KEY"), lit(-1).cast(IntegerType()))) \
                     .withColumn("MATCH_STATUS", coalesce(col("MATCH_STATUS"), lit("UNMATCHED")))
    # Integrate dim_date_gold
    fact_df = fact_df.withColumn("DATE_ID", date_format(col("fromdate_parsed"), "yyyyMMdd").cast(IntegerType()))
    dim_date = spark.read.table("dim_date_gold")
    fact_df = fact_df.join(dim_date, ["DATE_ID"], "left")
    # Integrate dim_rta
    dim_rta_df = spark.read.table("dim_rta")
    fact_df = fact_df.join(dim_rta_df, fact_df["OfficeCd_STD"] == dim_rta_df["RTA_OFFICE_CODE"], "left")
    # Select desired columns
    return fact_df
