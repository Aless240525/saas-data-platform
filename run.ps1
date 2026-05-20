# 1. Aislar el entorno (Desactivar variables globales del OS que causan conflictos)
$env:SPARK_HOME=""
$env:JAVA_HOME=""

# 2. Configurar dependencias locales de Hadoop requeridas por Delta Lake en Windows
$env:HADOOP_HOME="C:\hadoop"

# 3. Forzar a PySpark a usar estrictamente el Python del entorno virtual local
$env:PYSPARK_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\.venv\Scripts\python.exe"

# 4. Validar que se haya pasado el parámetro del tenant
$tenant = $args[0]
if ([string]::IsNullOrWhiteSpace($tenant)) {
    Write-Host "ERROR: Debes especificar un tenant. Ejemplo: .\run.ps1 sv" -ForegroundColor Red
    exit 1
}

Write-Host "Lanzando pipeline para tenant: $tenant..." -ForegroundColor Green

# 5. Ejecutar el orquestador
& "$PWD\.venv\Scripts\python.exe" src/saas_pipeline/cli.py --tenant $tenant