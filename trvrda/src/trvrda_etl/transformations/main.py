import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, regexp_replace, coalesce, to_date, upper, when, row_number,
    regexp_extract, lower, concat, explode, date_format, substring, levenshtein,
    length, abs as spark_abs, concat_ws, array_join, expr, split, year, month, 
    dayofmonth, quarter, broadcast, floor
)
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import Window

spark = SparkSession.builder.getOrCreate()

# Access pipeline parameters
source_path = spark.conf.get("source_path", "/path/to/default/bronze")
schema_name = spark.conf.get("my_target_schema", "default_schema")
catalog_name = spark.conf.get("my_target_catalog", "default_catalog")

# --- Constants & Patterns ---
UNKNOWN_STR = "UNKNOWN"
LEV_THRESHOLD = 3

PATTERNS = {
    "manufacturer_junk": r"(?i)\b(TEST|DUMMY|SAMPLE|JUNK|ABCD|XXXX|NA|N/A|NULL|UNKNOWN|NONE)\b",
    "manufacturer_in_rta": r"(?i)(MOTORS|LTD|PVT|AGENCY|MARUTI|HONDA|HERO|TOYOTA|MAHINDRA|TATA|HYUNDAI|KIA|SUZUKI|FORD|YAMAHA|BAJAJ|VOLKSWAGEN|NISSAN|RENAULT|BMW|MERCEDES|AUDI|SKODA|JEEP|MG|ISUZU|FORCE|ASHOK|EICHER|PIAGGIO)",
    "office_name_junk": r"(?i)\b(ADILABAD|NIRMAL|MANCHERIAL|KARIMNAGAR|JAGTIAL|WARANGAL|NALGONDA|HYDERABAD|SECUNDERABAD|MEDAK|NIZAMABAD|MAHBUBNAGAR|KHAMMAM|SURYAPET|SIDDIPET|MEDCHAL|RANGAREDDY|SANGAREDDY|KAMAREDDY|NAGARKURNOOL|GADWAL|WANAPARTHY|NARAYANPET|VIKARABAD|RTA|OFFICE|TRANSPORT)\b",
    "remove_words": ["LIMITED", "LTD", "PVT", "PRIVATE", "MOTORS", "INDIA", "AUTO", "INTERNATIONAL", "ENTERPRISES", "AGENCY"]
}

remove_words_regex = r'(?i)\b(' + '|'.join(PATTERNS['remove_words']) + r')\b'
business_junk = r"(?i)\b(M/S\.?|M\sS|MS|MOTORS?|AGENCIES|SHOWROOM|LTD|LIMITED|PVT|PRIVATE|LLP|WORKS|SERVICE|ENGG|AGRO|INDUSTRIES|TRACTORS|ENGINEERING|FRANKLIN\sEV|ESCORTS|PROMOTIONS)\b"
office_junk   = r"(?i)^(ONLINE|NA|OTHERS|N/A|NULL)$"
numeric_name  = r"^\d+$"
special_char  = r"^[^a-zA-Z0-9]+$"
date_like     = r"^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[12]\d{3}[01]\d{1}[0-3]\d{1})\s*$"
office_code_like = r"^(TS|TG)?\d{1,4}$"


# --- Helper Functions ---
def parse_to_date_expr(colname):
    trimmed = trim(col(colname).cast("string"))
    norm = regexp_replace(trimmed, r"[-.\\]", "/")
    return coalesce(
        to_date(norm, "dd/MM/yyyy"), to_date(norm, "d/M/yyyy"),
        to_date(norm, "yyyy/MM/dd"), to_date(norm, "MM/dd/yyyy"),
        to_date(norm, "M/d/yyyy"),   to_date(norm, "yyyy-MM-dd"),
        to_date(norm, "yyyyMMdd"),   to_date(norm, "dd/MM/yy"),
        to_date(norm, "MM/dd/yy"),   lit(None).cast("date")
    )

def preclean_date(colname):
    return when(
        lower(trim(col(colname))).rlike(PATTERNS['office_name_junk']), lit(None).cast("string")
    ).otherwise(col(colname))

def normalize_text_strict(c):
    return regexp_replace(lower(trim(c)), r"[^a-z0-9]", "")

def apply_reverse_rta_lookup(df, col_name):
    """Convert full office names like 'RTA WARANGAL' → 'TS08'"""
    c = upper(trim(col(col_name)))
    return df.withColumn(col_name,
        when(c.contains("ADILAB"), "TS01").when(c.contains("NIRMAL"), "TS02").when(c.contains("MANCHERIAL"), "TS03")
        .when(c.contains("KARIMNAGAR"), "TS04").when(c == "RTA JAGITYAL", "TS05").when(c.contains("JAGTIAL"), "TS05")
        .when(c == "RTA PEDDAPALLI", "TS06").when((c.contains("RAMAGUNDAM")) & (~c.contains("UNIT")), "TS07")
        .when(c == "RTA HANUMAKONDA", "TS08").when(c == "RTA WARANGAL", "TS08").when(c == "RTA WARANGAL URBAN", "TS08")
        .when(c == "RTA WARANGAL RURAL", "TS09").when(c.contains("MAHABUBABAD"), "TS10").when(c.contains("JANGOAN"), "TS11")
        .when(c == "RTA KHAMMAM", "TS13").when(c.contains("NALGONDA"), "TS15").when(c.contains("SURYAPET"), "TS16")
        .when(c.contains("YADADRI"), "TS17").when(c.contains("BHONGIR"), "TS17").when(c == "RTA HUZURNAGAR", "TS18")
        .when(c.contains("MAHABOOBNAGAR"), "TS29").when(c.contains("MAHBUBNAGAR"), "TS29").when(c.contains("WANAPARTHY"), "TS30")
        .when(c.contains("NAGARKURNOOL"), "TS31").when(c.contains("GADWAL"), "TS32").when(c == "RTA JOGULAMBA", "TS42")
        .when(c.contains("NARAYANPET"), "TS33").when(c == "RTA MEDAK", "TS34").when(c.contains("SIDDIPET"), "TS35")
        .when(c == "RTA ZAHEERABAD", "TS36").when(c == "RTA NIZAMABAD", "TS37").when(c.contains("KAMAREDDY"), "TS38")
        .when(c.contains("KOMRAMBHEEM"), "TS39").when(c.contains("JAYASHANKAR"), "TS40").when(c.contains("BHUPALPALLY"), "TS40")
        .when(c.contains("MULUG"), "TS41").when(c.contains("SIRCILLA"), "TS43").when(c == "RTA RAJANNA", "TS43")
        .when(c == "RTA VIKARABAD", "TS44").when(c == "RTA SANGAREDDY", "TS26").when(c == "RTA MEDCHAL", "TS27")
        .when(c == "RTA BHADRADRI", "TS48")
        .when(c.contains("HYDERABAD-CZ"), "TS19").when(c.contains("HYDERABAD-NZ"), "TS20")
        .when(c.contains("HYDERABAD-EZ"), "TS21").when(c.contains("HYDERABAD-SZ"), "TS22")
        .when(c.contains("HYDERABAD-WZ"), "TS23").when(c.contains("TRANSPORT OFFICER, HYDERABAD"), "TS19")
        .when(c == "RTA RANGAREDDY", "TS28")
        .when(c.contains("KODAD"), "TS129")
        .otherwise(col(col_name))
    )

def apply_rta_standardization(df, col_name):
    """Normalize RTA codes: TG009→TS19, TS001→TS01, TG→TS etc. Creates OfficeCd_STD"""
    c = upper(trim(col(col_name)))
    return df.withColumn(col_name + "_STD",
        when(c.rlike("^(TS|TG)009$"), "TS19").when(c.rlike("^(TS|TG)010$"), "TS20")
        .when(c.rlike("^(TS|TG)011$"), "TS21").when(c.rlike("^(TS|TG)012$"), "TS22")
        .when(c.rlike("^(TS|TG)013$"), "TS23").when(c.rlike("^(TS|TG)020$"), "TS20")
        .when(c.rlike("^(TS|TG)021$"), "TS21").when(c.rlike("^(TS|TG)022$"), "TS22")
        .when(c.rlike("^(TS|TG)023$"), "TS23").when(c.rlike("^(TS|TG)024$"), "TS24")
        .when(c.rlike("^(TS|TG)025$"), "TS25").when(c.rlike("^(TS|TG)026$"), "TS26")
        .when(c.rlike("^(TS|TG)027$"), "TS27").when(c.rlike("^(TS|TG)028$"), "TS28")
        .when(c.rlike("^(TS|TG)029$"), "TS29").when(c.rlike("^(TS|TG)038$"), "TS38")
        .when(c.rlike("^(TS|TG)00[1-8]$"), regexp_replace(c, "00", "0"))
        .when(c.rlike("^(TS|TG)0[1-9][0-9]$"), regexp_replace(c, "(TS|TG)0", "$1"))
        .otherwise(regexp_replace(c, "^TG", "TS"))
    )

# --- Bronze Table ---
# Flat source→target mapping (identical to standard-rta.py normalize_colname_map)
# Key = lowercased source column name, Value = target column name
COLUMN_RENAME_MAP = {
    "slno": "slno", "sl_no": "slno",
    "modeldesc": "modelDesc", "model.description": "modelDesc", "model_desc": "modelDesc",
    "maker": "makerName", "makername": "makerName", "manufacturername": "makerName",
    "manufacturer_name": "makerName", "manufacturer": "makerName",
    "tempregnno": "tempRegistrationNumber", "regnno": "tempRegistrationNumber",
    "regno": "tempRegistrationNumber", "tempregistrationnumber": "tempRegistrationNumber",
    "registration_no": "tempRegistrationNumber", "reg_id": "tempRegistrationNumber",
    "officecode": "OfficeCd", "office_cd": "OfficeCd", "office": "OfficeCd",
    "officecd": "OfficeCd", "officecd.1": "OfficeCd", "office.cd": "OfficeCd",
    "office.code": "OfficeCd",
    "vehicleregndate": "fromdate", "vehregnvaliddate": "todate",
    "from_date": "fromdate", "to_date": "todate", "fromdate": "fromdate", "todate": "todate",
    "regn_date": "fromdate", "regn.date": "fromdate", "regn_to_date": "todate",
    "colour": "colour", "color": "colour",
    "fueltype": "fuel", "fuel_used": "fuel", "fuel": "fuel", "fuel.type": "fuel",
    "vehicleclass": "vehicleClass", "classofveh": "vehicleClass", "vehicle.class": "vehicleClass",
    "seatingcapacity": "seatCapacity", "seat_cap": "seatCapacity",
    "seatcapacity": "seatCapacity", "seat_capacity": "seatCapacity", "seating.capacity": "seatCapacity",
    "makeyear": "makeYear", "make_year": "makeYear", "mfg_year": "makeYear",
    "secondvehicle": "secondVehicle", "issecondvehicle": "secondVehicle", "second.vehicle": "secondVehicle",
    "category": "category", "veh.category": "category",
}

# Golden schema: all columns the pipeline expects after bronze
GOLDEN_SCHEMA = [
    "slno", "modelDesc", "makerName", "OfficeCd", "tempRegistrationNumber",
    "fromdate", "todate", "fuel", "colour", "vehicleClass",
    "makeYear", "seatCapacity", "category", "secondVehicle"
]

@dlt.table(
    name="rta_bronze",
    comment="Raw ingestion from CSV with auto-column-normalization (same logic as standard-rta.py)."
)
def rta_bronze():
    raw_df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.schemaLocation", f"{source_path}/_checkpoints_v3")
                .load(source_path))

    # Step 1: Group columns by target name and coalesce duplicates (fixes ambiguous reference errors)
    target_groups = {}
    for c in raw_df.columns:
        norm_c = c.lower().strip()
        if norm_c in COLUMN_RENAME_MAP:
            target_name = COLUMN_RENAME_MAP[norm_c]
            if target_name not in target_groups:
                target_groups[target_name] = []
            target_groups[target_name].append(c)
    
    df = raw_df
    for target_name, source_cols in target_groups.items():
        # Use coalesce to merge data from multiple potential source columns (e.g. modelDesc and model_desc)
        # Use col(f"`{sc}`") to prevent dots in column names from being interpreted as struct fields
        df = df.withColumn(target_name, coalesce(*[col(f"`{sc}`") for sc in source_cols]))
        
        # Drop source columns if they have a different name than the target to avoid duplicates
        for sc in source_cols:
            if sc != target_name and sc in df.columns:
                df = df.drop(col(f"`{sc}`"))

    # Step 3: Fill missing golden schema columns with NULL
    for c in GOLDEN_SCHEMA:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))

    return df.select(*GOLDEN_SCHEMA)


# --- Silver Table (Robust Clean) ---
@dlt.table(name="rta_silver", comment="Highly robust cleaning table equivalent to standard-rta.py.")
@dlt.expect_or_drop("valid_registration", "tempRegistrationNumber IS NOT NULL AND length(trim(tempRegistrationNumber)) > 3")
def rta_silver():
    df = dlt.read("rta_bronze")
    
    # 0. Edge cases text cleaning from AdvancedCleaner
    df = df.withColumn("makerName", trim(regexp_replace(col("makerName"), r"\s+", " "))) \
           .withColumn("modelDesc",  trim(regexp_replace(col("modelDesc"),  r"\s+", " ")))
           
    df = df.withColumn("makerName",
        when(col("makerName").rlike(business_junk), trim(regexp_replace(col("makerName"), business_junk, "")))
        .when(col("makerName").rlike(numeric_name) | col("makerName").rlike(special_char), lit(UNKNOWN_STR + "_MANUFACTURER"))
        .otherwise(col("makerName"))
    )
    df = df.withColumn("OfficeCd",
        when(col("OfficeCd").rlike(office_junk), lit(None).cast(StringType()))
        .otherwise(col("OfficeCd"))
    )

    # Date max length logic
    for c in ["fromdate", "todate"]:
        df = df.withColumn(c, when(length(col(c)) > 10, substring(col(c), 1, 10)).otherwise(col(c)))

    # Keys prep
    df = df.withColumn("tempRegistrationNumber", regexp_replace(upper(trim(col("tempRegistrationNumber"))), r"^(TS|TG|AP)-?", "$1"))
    
    # Rare cases - handle numeric extraction robustly (e.g. "2.0" -> 2)
    df = df.withColumn("seatCapacity", floor(col("seatCapacity").cast("double")).cast(StringType()))

    # 1. Standard text trimming
    text_cols = ["modelDesc", "makerName", "OfficeCd", "tempRegistrationNumber",
                 "fuel", "colour", "vehicleClass", "category", "fromdate", "todate", "makeYear", "secondVehicle"]
    for c in text_cols:
        df = df.withColumn(c, trim(col(c).cast(StringType())))

    # 2. Manufacturer cleaning
    df = df.withColumn("makerName",
        when(col("makerName").rlike(PATTERNS['manufacturer_junk']) | (col("makerName") == "") | col("makerName").isNull(),
             lit(f"{UNKNOWN_STR}_MANUFACTURER"))
        .otherwise(upper(regexp_replace(regexp_replace(col("makerName"), r"\s+", " "), r"[.,]+$", "")))
    )

    # 3. Column shift/swap detection & correction
    df = df.withColumn("is_swapped", trim(col("OfficeCd")).rlike(date_like) & upper(trim(col("fromdate"))).rlike(office_code_like)) \
           .withColumn("is_shifted",
        (trim(col("OfficeCd")).rlike(office_code_like) | col("OfficeCd").isNull() | (trim(col("OfficeCd")) == "")) &
        trim(col("fromdate")).rlike(r"(?i)[A-Z]{3,}") & trim(col("todate")).rlike(date_like)
    )
    df = df.withColumn("OfficeCd_temp",
        when(col("is_shifted") | col("is_swapped"), trim(col("fromdate"))).otherwise(col("OfficeCd"))
    ).withColumn("fromdate_temp",
        when(col("is_shifted"), col("todate")).when(col("is_swapped"), col("OfficeCd")).otherwise(col("fromdate"))
    ).withColumn("todate_temp",
        when(col("is_shifted"), col("todate")).when(col("is_swapped"), lit(None).cast("string")).otherwise(col("todate"))
    )
    df = df.withColumn("OfficeCd_final",
        when(col("OfficeCd_temp").isNull() | (trim(col("OfficeCd_temp")) == "") | (col("OfficeCd_temp") == "NULL"),
             regexp_extract(col("tempRegistrationNumber"), r'^(TS|TG)\d{1,3}', 0))
        .otherwise(col("OfficeCd_temp"))
    )
    df = df.drop("OfficeCd", "fromdate", "todate") \
           .withColumnRenamed("OfficeCd_final", "OfficeCd") \
           .withColumnRenamed("fromdate_temp", "fromdate") \
           .withColumnRenamed("todate_temp", "todate") \
           .drop("OfficeCd_temp", "is_swapped", "is_shifted")

    # 4. Filter invalid RTA in OfficeCd
    df = df.withColumn("OfficeCd",
        when(col("OfficeCd").rlike(PATTERNS['manufacturer_in_rta']), lit(None).cast(StringType()))
        .otherwise(col("OfficeCd"))
    )

    # 5. Preclean Dates
    df = df.withColumn("fromdate_preclean", preclean_date("fromdate")) \
           .withColumn("todate_preclean", preclean_date("todate"))

    # 7. Model cleaning + variant
    df = df.withColumn("modelDescClean", trim(regexp_replace(coalesce(col("modelDesc"), lit("")), r"[^A-Za-z0-9\s\+\-\(\)\./]", " "))) \
           .withColumn("isTrailer", lower(col("modelDescClean")).rlike(r"trailer|trailor|tipper|tractor|tanker")) \
           .withColumn("isElectric", lower(col("modelDescClean")).rlike(r"\b(ev|bov|electric|hybrid)\b"))
    
    df = df.withColumn("MODEL_NAME_CLEAN_1", regexp_replace(upper(col("modelDescClean")), remove_words_regex, "")) \
           .withColumn("MODEL_NAME_TOKENS", split(regexp_replace(trim(col("MODEL_NAME_CLEAN_1")), r'\s+', ' '), " ")) \
           .withColumn("MODEL_NAME_TOKENS_CLEAN", expr("filter(MODEL_NAME_TOKENS, x -> x != '')"))
           
    df = df.withColumn("modelName_canonical", upper(array_join(expr("slice(MODEL_NAME_TOKENS_CLEAN, 1, 2)"), " "))) \
           .withColumn("modelName_canonical", trim(regexp_replace(col("modelName_canonical"), r"[^A-Z0-9\s]", "")))
           
    df = df.withColumn("variant", expr("trim(regexp_replace(upper(modelDescClean), modelName_canonical, ''))"))
    df = df.withColumn("variant", when(col("isTrailer"), lit("TRAILER/TIPPER/TRACTOR/TANKER")).otherwise(col("variant")))
    df = df.withColumn("variant", when((col("variant") == "") | col("variant").isNull(), lit("UNKNOWN")).otherwise(col("variant")))
    df = df.drop("MODEL_NAME_CLEAN_1", "MODEL_NAME_TOKENS", "MODEL_NAME_TOKENS_CLEAN", "modelDescClean")

    # 8. Date Parsing
    df = df.withColumn("fromdate_parsed", parse_to_date_expr("fromdate_preclean")) \
           .withColumn("fromdate_parsed", when(year(col("fromdate_parsed")) < 100, expr("add_months(fromdate_parsed, 2000 * 12)")).otherwise(col("fromdate_parsed")))
    df = df.withColumn("todate_parsed", parse_to_date_expr("todate_preclean")) \
           .withColumn("todate_parsed", when(year(col("todate_parsed")) < 100, expr("add_months(todate_parsed, 2000 * 12)")).otherwise(col("todate_parsed")))
    
    eff_date = coalesce(col("fromdate_parsed"), col("todate_parsed"))
    # Null-out dates outside the valid 2019-2030 window so they don't corrupt the dashboard
    eff_date_valid = when(
        (eff_date.isNotNull()) & (year(eff_date) >= 2019) & (year(eff_date) <= 2030),
        eff_date
    ).otherwise(lit(None).cast("date"))
    df = df.withColumn("REGISTRATION_YEAR", year(eff_date_valid)) \
           .withColumn("REGISTRATION_MONTH", month(eff_date_valid)) \
           .withColumn("REGISTRATION_DATE_ID", date_format(eff_date_valid, "yyyyMMdd").cast(IntegerType()))

    # 10. Fuel Normalization
    df = df.withColumn("fuel_clean", upper(
        when(col("fuel").rlike(r"(?i)HYBRID|AHYBRID|((PETROL|CNG|LPG|DIESEL).*(ELECTRIC|BATTERY))|((ELECTRIC|BATTERY).*(PETROL|CNG|LPG|DIESEL))"), "HYBRID")
        .when(col("fuel").rlike(r"(?i)BATTERY|ELECTRIC"), "ELECTRIC")
        .when(col("fuel").rlike(r"(?i)PETROL|GASOLINE"), "PETROL")
        .when(col("fuel").rlike(r"(?i)DIESEL"), "DIESEL")
        .when(col("fuel").rlike(r"(?i)CNG"), "CNG")
        .when(col("fuel").rlike(r"(?i)LPG"), "LPG")
        .when(col("fuel").isNull() & lower(col("modelDesc")).rlike(r"trailer|tipper|tanker|tractor"), "DIESEL")
        .otherwise("UNKNOWN")
    ))

    # 11. MakeYear inference
    df = df \
        .withColumn("makeYear_inferred", regexp_extract(col("modelDesc"), r"(19\d{2}|20\d{2})", 0)) \
        .withColumn("makeYear_raw",
            when(col("makeYear").isNull() | (trim(col("makeYear")) == "") | (upper(trim(col("makeYear"))) == "UNKNOWN"),
                 when(col("makeYear_inferred") != "", col("makeYear_inferred")).otherwise(lit(None).cast(StringType())))
            .otherwise(col("makeYear"))
        )
    df = df \
        .withColumn("makeYear_4digit", regexp_extract(col("makeYear_raw"), r"(19\d{2}|20\d{2})", 0)) \
        .withColumn("makeYear_clean_digits", regexp_replace(col("makeYear_raw"), r"[^0-9]", ""))
    df = df.withColumn("makeYear",
        when((col("makeYear_4digit").isNotNull()) & (length(col("makeYear_4digit")) == 4), col("makeYear_4digit"))
        .when(length(col("makeYear_clean_digits")) == 2, concat(lit("20"), col("makeYear_clean_digits")))
        .otherwise(lit(None).cast(StringType()))
    ).drop("makeYear_inferred", "makeYear_raw", "makeYear_4digit", "makeYear_clean_digits")

    # 12. Ownership & Transport Type
    df = df \
        .withColumn("OWNERSHIP_TYPE", when(lower(trim(coalesce(col("secondVehicle"), lit("")))).isin("y", "yes", "1"), lit("PRE_OWNED")).otherwise(lit("NEW"))) \
        .withColumn("TRANSPORT_TYPE",
            when(lower(trim(col("category"))).rlike(r"(?i)non.*transport"), lit("NON_TRANSPORT"))
            .when(lower(trim(col("category"))).rlike(r"(?i)transport"), lit("TRANSPORT"))
            .when(upper(trim(col("vehicleClass"))).rlike(
                # Core commercial transport
                r"CONTRACT|STAGE CARRIAGE|GOODS\s*CARRIAGE|GOODS\s*VEHICLE|TAXI|TRANSPORT|"
                # Buses & minibuses
                r"OMNI\s*BUS|OMINIBUS|OMNIBUS|MOTOR\s*BUS|SCHOOL\s*BUS|MAXI\s*CAB|"
                r"EDUCATIONAL\s*INSTITUTION\s*BUS|STAFF\s*BUS|PRIVATE\s*SERVICE\s*VEHICLE|"
                # Cabs & rickshaws
                r"MOTOR\s*CAB|AUTO\s*RICKSHAW|AUTORICKSHAW|E\s*RICKSHAW|ERICKSHAW|"
                r"E\s*CART|ECART|ELECTRIC\s*RICKSHAW|"
                # Three/four wheeled goods
                r"THREE\s*WHEELED\s*GOODS|3\s*WHEELED\s*GOODS|QUADRACYCLE\s*TRANSPORT|"
                # Heavy goods / articulated
                r"ARTICULATED|LOADER|TIPPER|TANKER|"
                # Trailers & tractors for commercial use
                r"TRAILER\s*FOR\s*COMMERCIAL|TRACTOR\s*FOR\s*COMMERCIAL|SEMI\s*TRAILER|"
                # Construction & special equipment (commercial)
                r"ROAD\s*ROLLER|MOTOR\s*GRADER|FORK\s*LIFT|FORKLIFT|CRANE|"
                r"SELF\s*LOADING\s*CONCRETE\s*MIXER|CONCRETE\s*MIXER|"
                r"VEHICLE\s*FITTED\s*WITH\s*CONSTRUCTION|"
                # Chassis supplied for conversion
                r"CHASSIS"
            ), lit("TRANSPORT"))
            .otherwise(lit("NON_TRANSPORT")))

    # 13. RTA lookup and standardization
    df = apply_reverse_rta_lookup(df, "OfficeCd")
    df = apply_rta_standardization(df, "OfficeCd")

    # 9. Deduplication
    w = Window.partitionBy("tempRegistrationNumber").orderBy(coalesce(col("fromdate_parsed"), col("todate_parsed")).desc_nulls_last())
    return df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")


# --- Gold Tables ---
@dlt.table(name="dim_date_gold")
def dim_date_gold():
    # Date range: 2019-01-01 to 2030-12-31 (matches Telangana RTA data window)
    return spark.sql("SELECT sequence(to_date('2019-01-01'), to_date('2030-12-31'), interval 1 day) as d") \
                .select(explode(col("d")).alias("full_date")) \
                .withColumn("DATE_ID", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType())) \
                .withColumn("YEAR", year(col("full_date"))) \
                .withColumn("MONTH", month(col("full_date"))) \
                .withColumn("DAY", dayofmonth(col("full_date"))) \
                .withColumn("QUARTER", quarter(col("full_date")))

@dlt.table(name="dim_manufacturer")
def dim_manufacturer():
    df = dlt.read("rta_silver").select(col("makerName").alias("MANUFACTURER_NAME")).dropDuplicates() \
        .filter((col("MANUFACTURER_NAME").isNotNull()) & (col("MANUFACTURER_NAME") != f"{UNKNOWN_STR}_MANUFACTURER")) \
        .withColumn("MANUFACTURER_KEY", concat(lit("M"), row_number().over(Window.orderBy("MANUFACTURER_NAME"))).cast(StringType()))
    unknown_manuf = spark.createDataFrame([(f"{UNKNOWN_STR}_MANUFACTURER", "-1")], ["MANUFACTURER_NAME", "MANUFACTURER_KEY"])
    return df.unionByName(unknown_manuf)

@dlt.table(name="dim_vehicle")
def dim_vehicle():
    df = dlt.read("rta_silver")
    df_veh = df \
        .withColumn("MODEL_NAME_NORM", regexp_replace(upper(trim(col("modelName_canonical"))), r"[^A-Z0-9]", " ")) \
        .withColumn("MODEL_NAME_NORM", regexp_replace(col("MODEL_NAME_NORM"), r"\s+", " ")) \
        .withColumn("VARIANT_NORM", regexp_replace(upper(trim(col("variant"))), r"[^A-Z0-9]", "")) \
        .withColumn("VARIANT_NORM", regexp_replace(col("VARIANT_NORM"), r"(?:PLUS|\+|O|OPT|OPTIONAL)$", "")) \
        .withColumn("FUEL_NORM", when(col("fuel").isin("", "NA", "N/A", None), UNKNOWN_STR).otherwise(upper(trim(col("fuel"))))) \
        .withColumn("COLOUR_NORM", when(col("colour").isin("", "NA", None), UNKNOWN_STR).otherwise(upper(trim(col("colour"))))) \
        .withColumn("VEHICLECLASS_NORM", upper(trim(col("vehicleClass"))))

    vehicle_clean = df_veh \
        .filter((col("MODEL_NAME_NORM").isNotNull()) & (col("VARIANT_NORM").isNotNull()) & (trim(col("MODEL_NAME_NORM")) != "")) \
        .dropDuplicates(["MODEL_NAME_NORM", "VARIANT_NORM", "FUEL_NORM"]) \
        .select(
            col("MODEL_NAME_NORM").alias("MODEL_NAME"),
            col("VARIANT_NORM").alias("VARIANT"),
            col("FUEL_NORM").alias("FUEL"),
            col("COLOUR_NORM").alias("COLOUR"),
            col("VEHICLECLASS_NORM").alias("VEHICLE_CLASS")
        ) \
        .withColumn("VEHICLE_KEY", row_number().over(Window.orderBy("MODEL_NAME", "VARIANT", "FUEL")).cast(IntegerType()))

    unknown_veh_df = spark.createDataFrame(
        [(-1, "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")],
        ["VEHICLE_KEY", "MODEL_NAME", "VARIANT", "FUEL", "COLOUR", "VEHICLE_CLASS"]
    ).withColumn("VEHICLE_KEY", col("VEHICLE_KEY").cast(IntegerType()))
    
    return vehicle_clean.unionByName(unknown_veh_df)

@dlt.table(name="dim_rta")
def dim_rta():
    rta_master_data = [
        ("TS01","Adilabad"),("TS02","Nirmal"),("TS03","Mancherial"),("TS04","Karimnagar"),
        ("TS05","Jagtial"),("TS06","Peddapalli"),("TS07","Ramagundam (Godavarikhani)"),
        ("TS08","Warangal Urban (Hanamkonda)"),("TS09","Warangal Rural (Parkal)"),
        ("TS10","Mahabubabad"),("TS11","Jangaon"),("TS12","Kothagudem"),("TS13","Khammam"),
        ("TS14","Khammam (Wyra)"),("TS15","Nalgonda"),("TS16","Suryapet"),
        ("TS17","Bhongir / Yadadri Bhuvanagiri"),("TS18","Huzurnagar"),
        ("TS19","Hyderabad Central"),("TS20","Hyderabad North"),("TS21","Hyderabad East"),
        ("TS22","Hyderabad West"),("TS23","Hyderabad South"),("TS24","Hyderabad Secunderabad"),
        ("TS25","Ranga Reddy (Vikarabad)"),("TS26","Ranga Reddy (Sanga Reddy)"),
        ("TS27","Ranga Reddy (Medchal)"),("TS28","Ranga Reddy (Balanagar)"),
        ("TS29","Mahbubnagar"),("TS30","Wanaparthy"),("TS31","Nagarkurnool"),
        ("TS32","Gadwal"),("TS33","Narayanpet"),("TS34","Medak"),("TS35","Siddipet"),
        ("TS36","Zaheerabad"),("TS37","Nizamabad"),("TS38","Kamareddy"),
        ("TS39","Komaram Bheem (Asifabad)"),("TS40","Jayashankar Bhupalpally"),
        ("TS41","Mulugu"),("TS42","Jogulamba Gadwal"),("TS43","Rajanna Sircilla"),
        ("TS44","Vikarabad"),("TS45","Sanga Reddy (Patancheru)"),
        ("TS46","Shameerpet (Medchal-Malkajgiri)"),("TS47","Cherial (Siddipet)"),
        ("TS48","Bhadradri Kothagudem"),("TS49","Warangal (Parvathagiri)"),
        ("TS50","Peddapalli (Sulthanabad)"),("TS51","Nizamabad (Bodhan)"),
        ("TS102","Unit Office Huzurabad"),("TS107","RTA Ibrahimpatnam"),
        ("TS108","RTA Uppal"),("TS116","Unit Office Armoor"),
        ("TS121","Unit Office Korutla"),("TS122","Unit Office Ramagundam"),
        ("TS128","Unit Office Bhadrachalam"),("TS129","Unit Office Kodad"),
        ("TS131","Unit Office Kalwakurthy"),("TS132","Unit Office Pebbair"),
        ("TS134","Unit Office Pargi"),("TS207","Unit Office Shadnagar"),
        ("TS208","Unit Office Kukatpally"),("TS215","Unit Office Zahirabad"),
        ("TS216","Unit Office Bhodan"),("TS304","Unit Office Sattupalli"),
        ("TS305","Unit Office Miryalaguda"),("TS404","Unit Office Wyra"),
        ("TS415","Unit Office Patancheruvu")
    ]
    rta_master_map = spark.createDataFrame(rta_master_data, ["MAP_CODE", "DISTRICT_NAME"])
    
    unique_rta = dlt.read("rta_silver").select(col("OfficeCd_STD").alias("RTA_CODE")).dropDuplicates()
    
    rta_known = unique_rta.join(rta_master_map, unique_rta.RTA_CODE == rta_master_map.MAP_CODE, "inner") \
        .select(col("RTA_CODE").alias("RTA_OFFICE_CODE"), col("DISTRICT_NAME").alias("RTA_NAME")) \
        .dropDuplicates(["RTA_NAME"]) \
        .withColumn("RTA_ID", concat(lit("R"), row_number().over(Window.orderBy("RTA_NAME"))).cast(StringType()))
        
    unknown_rta = spark.createDataFrame([(UNKNOWN_STR, "UNK", "Unknown / Unmapped RTA")], ["RTA_ID", "RTA_OFFICE_CODE", "RTA_NAME"])
    return rta_known.unionByName(unknown_rta)

@dlt.table(name="fact_fuzzy_registrations", comment="Robust Fact table with advanced Levy fuzzy matching.")
def fact_fuzzy_registrations():
    silver = dlt.read("rta_silver")
    dv = dlt.read("dim_vehicle")
    
    stg = silver.select("tempRegistrationNumber", "modelName_canonical", "variant") \
        .dropDuplicates(["tempRegistrationNumber"]) \
        .withColumn("MN", normalize_text_strict(col("modelName_canonical"))) \
        .withColumn("VN", normalize_text_strict(col("variant"))) \
        .withColumn("KEY", concat_ws(" ", col("MN"), col("VN"))) \
        .withColumn("BLOCK", substring(col("MN"), 1, 4))
        
    dv_mod = dv.select("VEHICLE_KEY", "MODEL_NAME", "VARIANT") \
        .withColumn("MN2", normalize_text_strict(col("MODEL_NAME"))) \
        .withColumn("VN2", normalize_text_strict(col("VARIANT"))) \
        .withColumn("KEY2", concat_ws(" ", col("MN2"), col("VN2"))) \
        .withColumn("BLOCK2", substring(col("MN2"), 1, 4))

    exact = stg.join(dv_mod, stg.KEY == dv_mod.KEY2, "left").filter(col("VEHICLE_KEY").isNotNull()) \
        .select(col("tempRegistrationNumber"), col("VEHICLE_KEY").cast(IntegerType())) \
        .withColumn("MATCH_STATUS", lit("EXACT"))

    unresolved = stg.join(exact.select("tempRegistrationNumber"), on="tempRegistrationNumber", how="left_anti")

    candidates = unresolved.alias("u").join(
        broadcast(dv_mod).alias("d"),
        (col("u.BLOCK") == col("d.BLOCK2")) &
        (spark_abs(length(col("u.KEY")) - length(col("d.KEY2"))) <= LEV_THRESHOLD),
        "inner"
    )
    matches = candidates.withColumn("LEV", levenshtein(col("u.KEY"), col("d.KEY2"))).filter(col("LEV") <= LEV_THRESHOLD)
    
    # FIX: Safer window partitioning - use col() object correctly without string interpolation
    w = Window.partitionBy(col("u.tempRegistrationNumber")).orderBy(col("LEV").asc(), col("d.VEHICLE_KEY").asc())
    
    best_matches = matches.withColumn("rn", row_number().over(w)).filter(col("rn") == 1) \
        .select(col("u.tempRegistrationNumber"), col("d.VEHICLE_KEY").cast(IntegerType())) \
        .withColumn("MATCH_STATUS", lit("FUZZY"))

    matched = exact.select("tempRegistrationNumber").unionByName(best_matches.select("tempRegistrationNumber"))
    unmatched = unresolved.join(matched, on="tempRegistrationNumber", how="left_anti") \
        .select("tempRegistrationNumber") \
        .withColumn("VEHICLE_KEY", lit(-1).cast(IntegerType())) \
        .withColumn("MATCH_STATUS", lit("UNMATCHED"))

    resolved_keys = exact.unionByName(best_matches).unionByName(unmatched)
    
    # Build Fact
    dim_rta_df = dlt.read("dim_rta")
    dim_manuf = dlt.read("dim_manufacturer")
    dim_date = dlt.read("dim_date_gold")

    fact_df = silver.alias("s") \
        .join(resolved_keys.alias("r"), on="tempRegistrationNumber", how="left") \
        .join(broadcast(dim_rta_df).alias("rta"), col("s.OfficeCd_STD") == col("rta.RTA_OFFICE_CODE"), "left") \
        .join(broadcast(dim_manuf).alias("m"), col("s.makerName") == col("m.MANUFACTURER_NAME"), "left") \
        .join(broadcast(dim_date).alias("d"), col("s.REGISTRATION_DATE_ID") == col("d.DATE_ID"), "left")

    return fact_df.select(
        col("s.tempRegistrationNumber").alias("TEMP_REGISTRATION_NUMBER"),
        coalesce(col("r.VEHICLE_KEY"), lit(-1).cast(IntegerType())).alias("VEHICLE_KEY"),
        coalesce(col("rta.RTA_ID"), lit(UNKNOWN_STR)).alias("RTA_ID"),
        coalesce(col("m.MANUFACTURER_KEY"), lit("-1")).alias("MANUFACTURER_KEY"),
        coalesce(col("d.DATE_ID"), col("s.REGISTRATION_DATE_ID")).alias("REGISTRATION_DATE_ID"),
        col("s.REGISTRATION_YEAR"),
        col("s.REGISTRATION_MONTH"),
        col("s.makeYear"),
        col("s.seatCapacity").alias("SEAT_CAPACITY"),
        col("s.category").alias("CATEGORY"),
        col("s.fuel_clean").alias("FUEL"),
        col("s.OWNERSHIP_TYPE"),
        col("s.TRANSPORT_TYPE"),
        col("s.OfficeCd_STD").alias("RTA_OFFICE_CODE"),
        coalesce(col("rta.RTA_NAME"), lit(UNKNOWN_STR)).alias("RTA_NAME"),
        coalesce(col("r.MATCH_STATUS"), lit("UNMATCHED")).alias("MATCH_STATUS")
    ).dropDuplicates(["TEMP_REGISTRATION_NUMBER"])
