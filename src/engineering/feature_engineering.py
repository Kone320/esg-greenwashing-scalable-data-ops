"""
Feature Engineering ESG — Features 1 à 4.

Usage :
    python src/engineering/feature_engineering.py
"""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

BUCKET     = os.environ.get("PROJECT_BUCKET", "sarangan")
SILVER_IN  = f"s3a://{BUCKET}/silver/esg_clean/"
SILVER_OUT = f"s3a://{BUCKET}/silver/esg_features_step1/"

spark = (
    SparkSession.builder
    .appName("feature-engineering-thu")
    .config("spark.hadoop.fs.s3a.endpoint",          "https://minio.lab.sspcloud.fr")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    # ← CORRECTION : accès anonyme bucket public sarangan
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── LECTURE SILVER (produite par Amadou) ────────────────────────────
df = spark.read.parquet(SILVER_IN)
print(f"[Silver] {df.count()} lignes | {len(df.columns)} colonnes")

COLS_AMADOU = ["cdp_score_encoded", "net_zero_target_set_enc",
               "sbti_committed_enc", "emissions_disclosed_enc",
               "third_party_verified_enc"]
manquantes = [c for c in COLS_AMADOU if c not in df.columns]
if manquantes:
    print(f"[ERREUR] Colonnes manquantes : {manquantes}")
    spark.stop()
    exit(1)
print("[Check] Colonnes Amadou présentes ✅")

# ── FEATURE 1 : commitment_score ────────────────────────────────────
df = df.withColumn(
    "commitment_score",
    (F.col("net_zero_target_set_enc") +
     F.col("sbti_committed_enc") +
     F.col("emissions_disclosed_enc") +
     F.col("third_party_verified_enc")).cast(DoubleType())
)
r1 = df.stat.corr("commitment_score", "greenwashing_flag")
print(f"[Feature 1] commitment_score     | corr = {r1:+.4f}")

# ── FEATURE 2 : esg_cdp_gap ─────────────────────────────────────────
df = df.withColumn(
    "esg_cdp_gap",
    (F.col("esg_score_0_100") / 100.0) - (F.col("cdp_score_encoded") / 8.0)
)
r2 = df.stat.corr("esg_cdp_gap", "greenwashing_flag")
print(f"[Feature 2] esg_cdp_gap          | corr = {r2:+.4f}")

# ── FEATURE 3 : scope3_share ─────────────────────────────────────────
total_em = (F.col("scope1_emissions_mt_co2e") +
            F.col("scope2_emissions_mt_co2e") +
            F.col("scope3_emissions_mt_co2e"))
df = df.withColumn(
    "scope3_share",
    F.when(total_em > 0, F.col("scope3_emissions_mt_co2e") / total_em).otherwise(0.0)
)
r3 = df.stat.corr("scope3_share", "greenwashing_flag")
print(f"[Feature 3] scope3_share         | corr = {r3:+.4f}")

# ── FEATURE 4 : log_carbon_intensity ────────────────────────────────
df = df.withColumn(
    "log_carbon_intensity",
    F.log1p(F.col("carbon_intensity_tco2e_per_musd"))
)
r4 = df.stat.corr("log_carbon_intensity", "greenwashing_flag")
print(f"[Feature 4] log_carbon_intensity | corr = {r4:+.4f}")

print("\n[Aperçu features Thu]")
df.select("company", "year", "commitment_score",
          "esg_cdp_gap", "scope3_share", "log_carbon_intensity").show(5)

# ── ÉCRITURE ────────────────────────────────────────────────────────
(
    df.write
      .mode("overwrite")
      .partitionBy("sector", "year")
      .parquet(SILVER_OUT)
)
print(f"[Silver Features Step1] Écrit → {SILVER_OUT}")
spark.stop()