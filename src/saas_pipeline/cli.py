import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from saas_pipeline.config import load_config
from saas_pipeline.bronze import ingest_deliveries_to_bronze, ingest_catalog_to_bronze

def main():
    # 1. Captura de argumentos
    parser = argparse.ArgumentParser(description="Orquestador Pipeline SAAS")
    parser.add_argument("--tenant", type=str, required=True, help="Target tenant (ej: sv, hn) o 'all'")
    parser.add_argument("--start-date", type=str, required=False, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, required=False, help="Fecha fin YYYY-MM-DD")
    args = parser.parse_args()

    # 2. Inicializar Spark con soporte Delta
    spark = SparkSession.builder \
        .appName("SAAS_Data_Platform") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # 3. Cargar configuraciones
    cfg = load_config("config/base.yaml")
    
    # 4. Determinar la lista de tenants a procesar dinámicamente
    target_tenant = args.tenant.lower()
    
    if target_tenant == "all":
        # Extrae la lista de tenants leyendo los archivos .yaml en config/tenants/
        tenant_dir = Path("config/tenants")
        tenants_to_process = [file.stem for file in tenant_dir.glob("*.yaml")]
        
        # Freno de seguridad si la carpeta está vacía
        if not tenants_to_process:
            print("ERROR: No se encontraron archivos de configuración en config/tenants/")
            sys.exit(1)
    else:
        # Si pasan un país específico, solo procesa ese
        tenants_to_process = [target_tenant]

    print(f"-> Iniciando pipeline. Tenants a procesar: {tenants_to_process}")

    # Rutas de origen (Leídas 100% del YAML, cero hardcodeo)
    deliveries_raw = f"{cfg.paths.raw}/{cfg.files.deliveries_raw}"
    catalog_raw = f"{cfg.paths.raw}/{cfg.files.catalog_raw}"

    # 5. EL BUCLE PRINCIPAL (Itera por cada país)
    for tenant in tenants_to_process:
        print(f"\n=========================================")
        print(f"=== PROCESANDO TENANT: {tenant.upper()} ===")
        print(f"=========================================")
        
        # --- CAPA BRONZE ---
        print(f"[{tenant.upper()}] -> Iniciando Capa BRONZE")
        
        # Las rutas destino se arman dinámicamente para el tenant actual del bucle
        deliveries_bronze = f"{cfg.paths.bronze}/{tenant}/deliveries"
        catalog_bronze = f"{cfg.paths.bronze}/{tenant}/dim_materials"

        # Ingestar
        ingest_deliveries_to_bronze(spark, deliveries_raw, deliveries_bronze)
        ingest_catalog_to_bronze(spark, catalog_raw, catalog_bronze)
        
        print(f"[{tenant.upper()}] -> Capa BRONZE completada.")

    # Cierre limpio
    spark.stop()

if __name__ == "__main__":
    main()