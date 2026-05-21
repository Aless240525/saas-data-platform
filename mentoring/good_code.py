import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_deliveries(spark: SparkSession, file_path: str, output_path: str, tenant: str):
    try:
        logger.info(f"Iniciando procesamiento para tenant: {tenant}")
        df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
        
        df_filtered = df_raw.filter(F.col("pais") == tenant)
        valid_types = ["ZPRE", "ZVE1"]
        
        df_result = df_filtered.filter(F.col("tipo_entrega").isin(valid_types)) \
            .select(
                F.col("pais"),
                F.col("fecha_proceso").alias("fecha"),
                F.col("material"),
                F.when(F.col("unidad") == "CS", F.col("cantidad") * 20)
                 .otherwise(F.col("cantidad")).alias("cantidad_st"),
                F.col("precio")
            ).withColumn("total", F.col("cantidad_st") * F.col("precio")) \
             .drop("precio")

        df_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{output_path}/{tenant}")
        logger.info("Procesamiento exitoso.")
        return df_result

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise