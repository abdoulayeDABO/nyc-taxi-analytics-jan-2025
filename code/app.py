"""
NYC Yellow Taxi 2025 : nettoyage, feature engineering, agrégation
et chargement d'un schéma en étoile dans PostgreSQL (schéma dwh).

Lancement :
docker exec -it spark /bin/bash -c "cd /code && /opt/spark/bin/spark-submit --master local[*] app.py"
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# ============================================
# 1. CONFIGURATION
# ============================================
JDBC_URL = "jdbc:postgresql://postgres:5432/nyc_taxi_dwh"
JDBC_PROPS = {
    "user": "analyst",
    "password": "analyst_pwd",
    "driver": "org.postgresql.Driver",
}

ZONES_PATH = "data/taxi_zone_lookup.csv"
YELLOW_PATH = "data/yellow_tripdata_2025-01.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def write_pg(df: DataFrame, table: str) -> None:
    """Vide la table (TRUNCATE, sans DROP) puis insère : schéma et FK restent intacts."""
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .options(**JDBC_PROPS)
        .option("dbtable", table)
        .option("truncate", "true")
        .option("cascadeTruncate", "true")
        .mode("overwrite")
        .save()
    )
    logger.info("%s chargée", table)


# ============================================
# 2. SPARK
# ============================================
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("NYC_Yellow_Taxi_Analysis")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
logger.info("Spark prêt, version %s", spark.version)

try:
    # ============================================
    # 3. CHARGEMENT DES DONNÉES
    # ============================================
    taxi_zones_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("nullValue", "N/A")
        .option("inferSchema", "true")
        .load(ZONES_PATH)
        .cache()
    )
    yellow_df = spark.read.parquet(YELLOW_PATH).cache()

    logger.info("Yellow Taxi Records: %d", yellow_df.count())
    logger.info("Taxi Zones Records: %d", taxi_zones_df.count())

    # ============================================
    # 4. NETTOYAGE DE BASE
    # ============================================
    yellow_df_clean = yellow_df.filter(
        col("tpep_pickup_datetime").isNotNull()
        & col("tpep_dropoff_datetime").isNotNull()
        & col("passenger_count").isNotNull()
        & (col("passenger_count") > 0)
        & (col("trip_distance") > 0)
        & (col("fare_amount") > 0)
    )
    logger.info("Cleaned Yellow Taxi Records: %d", yellow_df_clean.count())

    # ============================================
    # 5. JOINTURE AVEC LES ZONES (pickup et dropoff)
    # ============================================
    pu_zones = taxi_zones_df.select(
        col("LocationID").alias("pu_zone_id"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
    )
    do_zones = taxi_zones_df.select(
        col("LocationID").alias("do_zone_id"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
    )

    yellow_df_clean = (
        yellow_df_clean
        .join(pu_zones, col("PULocationID") == col("pu_zone_id"), "left")
        .join(do_zones, col("DOLocationID") == col("do_zone_id"), "left")
        .drop("pu_zone_id", "do_zone_id")
    )

    # Zones inconnues ou nulles écartées (nullValue="N/A" met déjà ces valeurs à NULL)
    yellow_df_clean = yellow_df_clean.filter(
        col("pickup_zone").isNotNull() & (col("pickup_zone") != "Unknown")
        & col("dropoff_zone").isNotNull() & (col("dropoff_zone") != "Unknown")
        & col("pickup_borough").isNotNull() & (col("pickup_borough") != "Unknown")
        & col("dropoff_borough").isNotNull() & (col("dropoff_borough") != "Unknown")
    )
    logger.info("Records après filtre des zones: %d", yellow_df_clean.count())
    yellow_df_clean.select(
        "PULocationID", "pickup_borough", "pickup_zone",
        "DOLocationID", "dropoff_borough", "dropoff_zone",
    ).show(5)

    # ============================================
    # 6. FEATURE ENGINEERING
    # ============================================
    zero = F.lit(0.0)
    yellow_df_clean = (
        yellow_df_clean
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn(
            "trip_duration",
            F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"),
        )
        .withColumn(
            "total_revenue",
            F.coalesce(col("fare_amount"), zero)
            + F.coalesce(col("tip_amount"), zero)
            + F.coalesce(col("congestion_surcharge"), zero)
            + F.coalesce(col("Airport_fee"), zero),
        )
    )

    yellow_df_clean.select("trip_duration").summary("count", "min", "max", "mean").show()
    logger.info("Trips > 1h: %d", yellow_df_clean.filter(col("trip_duration") > 3600).count())
    logger.info("Trips > 50 miles: %d", yellow_df_clean.filter(col("trip_distance") > 50).count())

    # Filtres de validité
    yellow_df_clean = yellow_df_clean.filter(
        (col("trip_duration") >= 60)      # au moins 1 minute
        & (col("trip_distance") >= 0.1)   # au moins 0.1 mile
        & col("pickup_hour").isNotNull()
    )

    # Plafonnement des valeurs extrêmes
    yellow_df_clean = (
        yellow_df_clean
        .withColumn("trip_duration", F.when(col("trip_duration") > 3600, 3600).otherwise(col("trip_duration")))
        .withColumn("trip_distance", F.when(col("trip_distance") > 50, 50.0).otherwise(col("trip_distance")))
    )
    logger.info("Records après filtres et plafonnement: %d", yellow_df_clean.count())
    yellow_df_clean.select("trip_duration").summary("count", "min", "max", "mean").show()

    # ============================================
    # 7. DIMENSIONS
    # ============================================
    dim_zone = taxi_zones_df.select(
        col("LocationID").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone"),
    )

    dim_vendor = spark.createDataFrame(
        [(1, "Creative Mobile Technologies"), (2, "VeriFone"), (6, "Autre"), (7, "Autre")],
        ["vendor_id", "vendor_name"],
    )

    dim_ratecode = spark.createDataFrame(
        [(1, "Standard"), (2, "JFK"), (3, "Newark"), (4, "Nassau/Westchester"),
         (5, "Négocié"), (6, "Groupe"), (99, "Inconnu")],
        ["ratecode_id", "ratecode_desc"],
    )

    dim_payment_type = spark.createDataFrame(
        [(0, "Flex fare"), (1, "Carte de crédit"), (2, "Espèces"), (3, "Gratuit"),
         (4, "Litige"), (5, "Inconnu"), (6, "Annulé")],
        ["payment_type_id", "payment_desc"],
    )

    calendar = spark.sql(
        "SELECT explode(sequence(to_date('2024-12-31'), to_date('2026-12-31'), interval 1 day)) AS d"
    )
    dim_date = calendar.select(
        F.date_format("d", "yyyyMMdd").cast("int").alias("date_id"),
        col("d").alias("full_date"),
        F.dayofmonth("d").alias("day"),
        F.month("d").alias("month"),
        F.year("d").alias("year"),
        F.quarter("d").alias("quarter"),
        (((F.dayofweek("d") + 5) % 7) + 1).cast("int").alias("day_of_week"),
        F.date_format("d", "EEEE").alias("day_name"),
        F.dayofweek("d").isin(1, 7).alias("is_weekend"),
        F.dayofyear("d").alias("day_of_year"),
        F.weekofyear("d").alias("week_of_year"),
        F.date_format("d", "MMMM").alias("month_name"),
        F.date_format("d", "yyyy-MM").alias("year_month"),
        F.concat(F.year("d").cast("string"), F.lit("-Q"), F.quarter("d").cast("string")).alias("year_quarter"),
        F.when(F.month("d").isin(12, 1, 2), "Hiver")
        .when(F.month("d").isin(3, 4, 5), "Printemps")
        .when(F.month("d").isin(6, 7, 8), "Été")
        .otherwise("Automne").alias("season"),
        (F.dayofmonth("d") == 1).alias("is_month_start"),
        (col("d") == F.last_day("d")).alias("is_month_end"),
    )

    dim_time = spark.range(24).select(
        col("id").cast("int").alias("hour_id"),
        F.when(col("id") < 6, "Nuit")
        .when(col("id") < 12, "Matin")
        .when(col("id") < 18, "Après-midi")
        .otherwise("Soir").alias("period"),
        F.concat(
            F.lpad(col("id").cast("string"), 2, "0"), F.lit(":00-"),
            F.lpad(col("id").cast("string"), 2, "0"), F.lit(":59"),
        ).alias("hour_label"),
        F.when(col("id") % 12 == 0, 12).otherwise(col("id") % 12).cast("int").alias("hour_12"),
        F.when(col("id") < 12, "AM").otherwise("PM").alias("am_pm"),
        ((col("id") >= 22) | (col("id") < 5)).alias("is_night"),
        (col("id").between(7, 9) | col("id").between(16, 19)).alias("is_rush_hour"),
        F.when(col("id").between(7, 9), "Pointe matin")
        .when(col("id").between(16, 19), "Pointe soir")
        .otherwise("Hors pointe").alias("rush_type"),
    )

    # ============================================
    # 8. TABLE DE FAITS AGRÉGÉE
    # ============================================
    # On ne garde que les lignes qui respectent les clés étrangères
    base = (
        yellow_df_clean
        .withColumn("ratecode_id", F.coalesce(col("RatecodeID").cast("int"), F.lit(99)))
        .withColumn("payment_type_id", F.coalesce(col("payment_type").cast("int"), F.lit(5)))
        .filter(col("VendorID").isin(1, 2, 6, 7))
        .filter(col("ratecode_id").isin(1, 2, 3, 4, 5, 6, 99))
        .filter(col("payment_type_id").between(0, 6))
        .filter(
            (col("tpep_pickup_datetime") >= "2025-01-01")
            & (col("tpep_pickup_datetime") < "2026-01-01")
        )
    )

    fact_trips_hourly = (
        base.groupBy(
            F.date_format("tpep_pickup_datetime", "yyyyMMdd").cast("int").alias("date_id"),
            col("pickup_hour").alias("hour_id"),
            col("PULocationID").alias("pu_location_id"),
            col("DOLocationID").alias("do_location_id"),
            col("VendorID").alias("vendor_id"),
            "payment_type_id",
            "ratecode_id",
        )
        .agg(
            F.count("*").alias("nb_trips"),
            F.sum("passenger_count").alias("total_passengers"),
            F.sum("trip_distance").alias("total_distance"),
            F.sum("trip_duration").alias("total_duration_sec"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tip"),
            F.sum("total_revenue").alias("total_revenue"),
        )
    )
    fact_trips_hourly.show(5)

    # ============================================
    # 9. ÉCRITURE DANS POSTGRES (dimensions d'abord, fait ensuite)
    # ============================================
    for df, table in [
        (dim_zone, "dwh.dim_zone"),
        (dim_vendor, "dwh.dim_vendor"),
        (dim_ratecode, "dwh.dim_ratecode"),
        (dim_payment_type, "dwh.dim_payment_type"),
        (dim_time, "dwh.dim_time"),
        (dim_date, "dwh.dim_date"),
    ]:
        write_pg(df, table)

    write_pg(fact_trips_hourly, "dwh.fact_trips")
    logger.info("Chargement du DWH terminé")

except Exception:
    logger.exception("Échec du traitement")
    raise
finally:
    spark.stop()