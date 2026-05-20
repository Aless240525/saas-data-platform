from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import uuid

def ingest_deliveries_to_bronze(spark: SparkSession, raw_path: str, bronze_path: str, batch_id: str = None):
    """
    Ingesta el archivo transaccional a Bronze.
    Aplica particionado dinámico por fecha_proceso y _tenant_id.
    """
    if not batch_id:
        batch_id = str(uuid.uuid4())

    df_raw = spark.read.csv(raw_path, header=True, inferSchema=True)

    df_bronze = df_raw.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name()) \
                      .withColumn("_batch_id", F.lit(batch_id).cast(StringType())) \
                      .withColumn("_tenant_id", F.lower(F.col("pais")))

    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("fecha_proceso", "_tenant_id")
        .option("partitionOverwriteMode", "dynamic")
        .save(bronze_path))
    
    return df_bronze


def ingest_catalog_to_bronze(spark: SparkSession, raw_path: str, bronze_path: str, batch_id: str = None):
    """
    Ingesta el catálogo de materiales a Bronze.
    No se particiona por tenant ni fecha de proceso porque es una dimensión global.
    """
    if not batch_id:
        batch_id = str(uuid.uuid4())

    df_raw = spark.read.csv(raw_path, header=True, inferSchema=True)

    df_bronze = df_raw.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name()) \
                      .withColumn("_batch_id", F.lit(batch_id).cast(StringType()))
                      # No se agrega _tenant_id porque el catálogo no tiene país.

    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_path))
    
    return df_bronze