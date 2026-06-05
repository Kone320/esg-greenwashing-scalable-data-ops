"""
Pipeline Bronze → Silver — version Engineering.

Améliorations vs version Architecte :
- CDP ordinal encodé (A=8 → F=1) au lieu de laisser en string
- Binaires encodés en int (0/1) au lieu de boolean
- Imputation yoy par médiane au lieu de 0

Usage :
    python src/ingestion/bronze_to_silver.py
"""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

BUCKET = os.environ.get("PROJECT_BUCKET", "sarangan")
BRONZE = f"s3a://{BUCKET}/bronze/kaggle_esg/esg_greenwashing.csv"
SILVER = f"s3a://{BUCKET}/silver/esg_clean/"

spark = (
    SparkSession.builder
    .appName("bronze-to-silver-v2")
    # Endpoint MinIO Onyxia
    .config("spark.hadoop.fs.s3a.endpoint",          "https://minio.lab.sspcloud.fr")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    # ← CORRECTION : accès anonyme pour bucket public sarangan
    # équivalent du --no-sign-request qui fonctionnait en AWS CLI
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── LECTURE BRONZE ──────────────────────────────────────────────────
df = spark.read.option("header", True).option("inferSchema", True).csv(BRONZE)
print(f"[Bronze] {df.count()} lignes lues | {len(df.columns)} colonnes")

# ── AUDIT RAPIDE ────────────────────────────────────────────────────
print("[Audit] Valeurs manquantes :")
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# ── IMPUTATION yoy_scope1_change_pct ────────────────────────────────
median_yoy = df.approxQuantile("yoy_scope1_change_pct", [0.5], 0.01)[0]
df = df.fillna({"yoy_scope1_change_pct": median_yoy})
print(f"[Nettoyage] Médiane yoy = {median_yoy:.4f} | nulls restants : "
      f"{df.filter(F.col('yoy_scope1_change_pct').isNull()).count()}")

# ── ENCODAGE CDP ORDINAL ─────────────────────────────────────────────
CDP_MAP = {"A": 8, "A-": 7, "B": 6, "B-": 5, "C": 4, "C-": 3, "D": 2, "F": 1}
cdp_expr = F.lit(None).cast(IntegerType())
for label, val in CDP_MAP.items():
    cdp_expr = F.when(F.col("cdp_climate_score") == label, val).otherwise(cdp_expr)
df = df.withColumn("cdp_score_encoded", cdp_expr)

print("[Encodage CDP]")
df.groupBy("cdp_climate_score", "cdp_score_encoded") \
  .count().orderBy("cdp_score_encoded", ascending=False).show()

# ── ENCODAGE BINAIRE Yes/No → 1/0 ───────────────────────────────────
COLS_BINAIRES = [
    "net_zero_target_set", "sbti_committed",
    "emissions_disclosed", "third_party_verified"
]
for c in COLS_BINAIRES:
    df = df.withColumn(
        f"{c}_enc",
        F.when(F.col(c) == "Yes", 1).otherwise(0).cast(IntegerType())
    )
df = df.drop("cdp_climate_score", *COLS_BINAIRES)

# ── ÉCRITURE SILVER ──────────────────────────────────────────────────
(
    df.write
      .mode("overwrite")
      .partitionBy("sector", "year")
      .parquet(SILVER)
)
print(f"[Silver] Écrit → {SILVER}")
print(f"[Silver] {df.count()} lignes | {len(df.columns)} colonnes")
df.printSchema()
spark.stop()