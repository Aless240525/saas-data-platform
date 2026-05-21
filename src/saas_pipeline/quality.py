from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType
import uuid
from datetime import datetime

def run_silver_quality_checks(spark: SparkSession, silver_path: str, tenant: str, batch_id: str):

    df_silver = spark.read.format("delta").load(silver_path)
    
    run_id = str(uuid.uuid4())
    executed_at = datetime.utcnow()
    total_records = df_silver.count()
    
    logs = []

    # 1. Unicidad de Clave de Negocio
    distinct_records = df_silver.dropDuplicates(["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]).count()
    failed_uniqueness = total_records - distinct_records
    
    logs.append({
        "_run_id": run_id,
        "_batch_id": batch_id,
        "tenant_id": tenant,
        "layer": "silver",
        "table_name": "fact_deliveries",
        "check_name": "unicidad_clave_transaccional",
        "check_severity": "critical",
        "records_checked": total_records,
        "records_failed": failed_uniqueness,
        "check_passed": failed_uniqueness == 0,
        "executed_at": executed_at
    })

    # 2. Cantidad Positiva 
    failed_qty = df_silver.filter(F.col("cantidad_normalizada_st") <= 0).count()
    
    logs.append({
        "_run_id": run_id,
        "_batch_id": batch_id,
        "tenant_id": tenant,
        "layer": "silver",
        "table_name": "fact_deliveries",
        "check_name": "cantidad_normalizada_positiva",
        "check_severity": "critical",
        "records_checked": total_records,
        "records_failed": failed_qty,
        "check_passed": failed_qty == 0,
        "executed_at": executed_at
    })

    # 3. Consistencia de Precios
    failed_price = df_silver.filter((F.col("is_routine_delivery") == True) & (F.col("precio") <= 0)).count()
    
    logs.append({
        "_run_id": run_id,
        "_batch_id": batch_id,
        "tenant_id": tenant,
        "layer": "silver",
        "table_name": "fact_deliveries",
        "check_name": "precio_valido_en_rutina",
        "check_severity": "warning",
        "records_checked": total_records,
        "records_failed": failed_price,
        "check_passed": failed_price == 0,
        "executed_at": executed_at
    })

    return logs

def log_quality_results(spark: SparkSession, logs: list, quality_logs_path: str, fail_on_critical: bool):

    if not logs:
        return


    schema = StructType([
        StructField("_run_id", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("tenant_id", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("check_severity", StringType(), True),
        StructField("records_checked", LongType(), True),
        StructField("records_failed", LongType(), True),
        StructField("check_passed", BooleanType(), True),
        StructField("executed_at", TimestampType(), True)
    ])

    logs_df = spark.createDataFrame(logs, schema)
    

    logs_df.write.format("delta").mode("append").save(quality_logs_path)

    # El freno de seguridad: abortar antes de Gold si hay un falllo critico
    if fail_on_critical:
        critical_failures = [log for log in logs if log["check_severity"] == "critical" and not log["check_passed"]]
        if critical_failures:
            nombres_fallos = ", ".join([c["check_name"] for c in critical_failures])
            raise RuntimeError(f"DATA QUALITY ERROR: Validacion critica fallida ({nombres_fallos}). Abortando pipeline.")