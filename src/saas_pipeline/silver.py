from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType
from delta.tables import DeltaTable
import os

def process_dim_materials_silver(spark: SparkSession, raw_path: str, silver_path: str):

    # 1. Leer de Raw
    df_bronze = spark.read.format("delta").load(raw_path)

    # Castear fechas
    df_silver = df_bronze.withColumn("valid_from", F.col("valid_from").cast(DateType())) \
                         .withColumn("valid_to", F.col("valid_to").cast(DateType())) \
                         .withColumn("precio_base", F.col("precio_base").cast(DecimalType(10, 2)))

    # 2. Idempotencia: MERGE INTO
    if DeltaTable.isDeltaTable(spark, silver_path):
        target_table = DeltaTable.forPath(spark, silver_path)
        target_table.alias("target").merge(
            df_silver.alias("source"),
            "target.material = source.material AND target.valid_from = source.valid_from"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # Carga inicial
        df_silver.write.format("delta").mode("overwrite").save(silver_path)

def process_fact_deliveries_silver(spark: SparkSession, bronze_deliveries_path: str, silver_dim_materials_path: str, silver_deliveries_path: str, quarantine_path: str, tenant: str):

    # 1. Lectura
    df_fact = spark.read.format("delta").load(bronze_deliveries_path)
    df_dim = spark.read.format("delta").load(silver_dim_materials_path)

    # --- LA MAGIA ESTÁ AQUÍ ---
    # Seleccionamos SOLO lo útil de la dimensión
    dim_cols = ["material", "descripcion", "categoria", "precio_base", "valid_from", "valid_to"]
    df_dim_clean = df_dim.select(*dim_cols).withColumnRenamed("material", "dim_material")

    # 2. Deduplicación
    df_fact = df_fact.dropDuplicates()

    # 3. Reglas de Negocio
    valid_tipos = ["ZPRE", "ZVE1", "Z04", "Z05"]
    df_fact = df_fact.filter(F.col("tipo_entrega").isin(valid_tipos))

    df_fact = df_fact.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(["ZPRE", "ZVE1"])) \
                     .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(["Z04", "Z05"]))

    df_fact = df_fact.withColumn(
        "cantidad_normalizada_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * 20).otherwise(F.col("cantidad"))
    ).withColumn("cantidad_normalizada_st", F.col("cantidad_normalizada_st").cast(DecimalType(10, 2)))

    df_fact = df_fact.withColumn("fecha_date", F.to_date(F.col("fecha_proceso"), "yyyyMMdd"))

    # 4. Join Temporal con el catálogo limpio
    join_cond = [
        df_fact["material"] == df_dim_clean["dim_material"],
        df_fact["fecha_date"] >= df_dim_clean["valid_from"],
        df_fact["fecha_date"] <= df_dim_clean["valid_to"]
    ]
    
    df_joined = df_fact.join(df_dim_clean, join_cond, "left")

    # 5. Detección de Anomalías (Cuarentena)
    df_validated = df_joined.withColumn(
        "_quarantine_reason",
        F.when(F.col("fecha_proceso").isNull() | F.col("fecha_date").isNull(), "Fecha nula o invalida")
         .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "Cantidad nula o menor/igual a cero")
         .when(F.col("precio").isNull(), "Precio transaccional nulo")
         .when(F.col("dim_material").isNull(), "Material no existe en catalogo para esta fecha")
         .otherwise(None)
    )

    # 6. Registros Válidos y Cuarentena
    
    cols_to_drop = ["fecha_date", "dim_material", "valid_from", "valid_to", "_quarantine_reason"]
    df_valid = df_validated.filter(F.col("_quarantine_reason").isNull()).drop(*cols_to_drop)


    cols_to_keep_quarantine = [col for col in df_fact.columns if col != "fecha_date"] + ["_quarantine_reason"]
    df_quarantine = df_validated.filter(F.col("_quarantine_reason").isNotNull()).select(*cols_to_keep_quarantine)

    # 7. carga a Cuarentena 
    if not df_quarantine.isEmpty():
        df_quarantine.write.format("delta").mode("append").save(quarantine_path)

    # 8. Idempotencia en Silver
    merge_condition = """
        target._tenant_id = source._tenant_id AND 
        target.fecha_proceso = source.fecha_proceso AND 
        target.transporte = source.transporte AND 
        target.ruta = source.ruta AND 
        target.material = source.material AND 
        target.tipo_entrega = source.tipo_entrega
    """
    
    if not df_valid.isEmpty():
        if DeltaTable.isDeltaTable(spark, silver_deliveries_path):
            target_table = DeltaTable.forPath(spark, silver_deliveries_path)
            target_table.alias("target").merge(
                df_valid.alias("source"), merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Carga inicial particionada por fecha_proceso
            df_valid.write.format("delta").mode("overwrite").partitionBy("fecha_proceso").save(silver_deliveries_path)