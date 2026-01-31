from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, TimestampType

# Configuration Spark pour Delta Lake
builder = SparkSession.builder \
    .appName("BronzeToSilver_Delta") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()
print("🚀 Spark Delta Lake démarré !")

# Lecture (Bronze)
try:
    df_raw = spark.read.option("multiline", "true").json("/lakehouse/bronze/*.json")
    
    # Transformation
    df_silver = df_raw.select(
        col("meter_id"),
        col("region"),
        col("power_kw").cast(DoubleType()),
        col("timestamp").cast(TimestampType()).alias("time_event")
    ).dropDuplicates()

    # Écriture (Delta Lake)
    output_path = "/lakehouse/silver/delta_energy"
    df_silver.write.format("delta").mode("overwrite").save(output_path)
    
    print(f"✅ Succès ! Données écrites en Delta Lake dans {output_path}")

except Exception as e:
    print(f"⚠️ Erreur (Probablement pas de données bronze) : {e}")

spark.stop()