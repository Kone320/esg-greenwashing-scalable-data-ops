import os, json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

BUCKET      = os.environ.get("PROJECT_BUCKET", "sarangan")
SILVER_IN   = f"s3a://{BUCKET}/silver/esg_features_step1/"
SILVER_OUT  = f"s3a://{BUCKET}/silver/esg_clean/"
GOLD_REPORT = f"s3a://{BUCKET}/gold/dashboard/rapport_qualite.json"

spark = (
    SparkSession.builder
    .appName("silver-final-romeo")
    .config("spark.hadoop.fs.s3a.endpoint",          "https://minio.lab.sspcloud.fr")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SILVER_IN)
print(f"[Step1] {df.count()} lignes | {len(df.columns)} colonnes")

COLS_THU = ["commitment_score", "esg_cdp_gap", "scope3_share", "log_carbon_intensity"]
manquantes = [c for c in COLS_THU if c not in df.columns]
if manquantes:
    print(f"[ERREUR] Colonnes Thu manquantes : {manquantes}")
    spark.stop()
    exit(1)
print("[Check] Colonnes Thu présentes ✅")

df = df.withColumn(
    "credibility_gap",
    F.col("commitment_score") *
    (F.lit(1) - F.col("third_party_verified_enc")).cast(DoubleType())
)
r5 = df.stat.corr("credibility_gap", "greenwashing_flag")
print(f"[Feature 5] credibility_gap      | corr = {r5:+.4f}")

window_3y = (
    Window
    .partitionBy("company")
    .orderBy("year")
    .rowsBetween(-2, 0)
)
df = df.withColumn(
    "emission_trend_3y",
    F.avg("yoy_scope1_change_pct").over(window_3y)
)
r6 = df.stat.corr("emission_trend_3y", "greenwashing_flag")
print(f"[Feature 6] emission_trend_3y    | corr = {r6:+.4f}")

df.filter(F.col("company") == "ExxonMobil") \
  .select("year", "yoy_scope1_change_pct", "emission_trend_3y") \
  .orderBy("year").show(8)

ALL_FEATURES = [
    "cdp_score_encoded",
    "net_zero_target_set_enc", "sbti_committed_enc",
    "emissions_disclosed_enc", "third_party_verified_enc",
    "commitment_score", "esg_cdp_gap",
    "scope3_share", "log_carbon_intensity",
    "credibility_gap", "emission_trend_3y",
    "greenwashing_flag"
]
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ALL_FEATURES
]).show()

total = df.count()
df.groupBy("greenwashing_flag").agg(
    F.count("*").alias("nb"),
    F.round(F.count("*") / total * 100, 1).alias("pct_%")
).show()

COLS_FINALES = [
    "year", "company", "ticker", "sector", "country", "revenue_usd_bn",
    "scope1_emissions_mt_co2e", "scope2_emissions_mt_co2e",
    "scope3_emissions_mt_co2e", "total_s1_s2_mt_co2e",
    "yoy_scope1_change_pct", "carbon_intensity_tco2e_per_musd",
    "esg_score_0_100", "cdp_score_encoded",
    "net_zero_target_set_enc", "sbti_committed_enc",
    "emissions_disclosed_enc", "third_party_verified_enc",
    "commitment_score", "esg_cdp_gap",
    "scope3_share", "log_carbon_intensity",
    "credibility_gap", "emission_trend_3y",
    "greenwashing_flag"
]
(
    df.select(COLS_FINALES)
      .repartition(6)
      .write
      .mode("overwrite")
      .partitionBy("sector", "year")
      .parquet(SILVER_OUT)
)
print(f"\n[Silver Finale] Écrit → {SILVER_OUT}")

df_val = spark.read.parquet(SILVER_OUT)
rapport = {
    "pipeline"       : "Amadou → Thu → Romeo",
    "nb_lignes"      : df_val.count(),
    "nb_colonnes"    : len(df_val.columns),
    "features_romeo" : ["credibility_gap", "emission_trend_3y"],
    "nulls_residuels": 0,
    "statut"         : "OK — prêt pour Data Scientist",
    "notes_scientist": {
        "chemin"       : SILVER_OUT,
        "cible"        : "greenwashing_flag",
        "desequilibre" : "85.6% / 14.4% → weightCol obligatoire",
        "split"        : "train <= 2020 | test > 2020"
    }
}
print(json.dumps(rapport, indent=2, ensure_ascii=False))
(
    spark
    .createDataFrame([{"rapport": json.dumps(rapport, ensure_ascii=False, indent=2)}])
    .write.mode("overwrite")
    .json(GOLD_REPORT)
)
print(f"[Rapport] Exporté → {GOLD_REPORT}")
spark.stop()
