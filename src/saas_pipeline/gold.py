from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_daily_metrics_gold(spark: SparkSession, silver_deliveries_path: str, gold_metrics_path: str):

    # 1. Lectura de Silver
    df_silver = spark.read.format("delta").load(silver_deliveries_path)

    # 2. Agregación según requerimientos del negocio
    # Se agrupa por tenant, fecha y tipo de entrega
    df_gold = df_silver.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega").agg(
        F.sum("cantidad_normalizada_st").alias("total_units"),
        F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports")
    )

    # 3. Idempotencia en Gold
    df_gold.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("fecha_proceso") \
        .save(gold_metrics_path)