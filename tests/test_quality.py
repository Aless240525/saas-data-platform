import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-quality-pipeline") \
        .master("local[2]") \
        .getOrCreate()

def test_quality_null_price(spark):
    """
    Prueba la regla de calidad que identifica precios nulos.
    Regla: Si el precio es nulo, el registro es anómalo y va a cuarentena.
    """
    data = [Row(id=1, precio=15.50), Row(id=2, precio=None)]
    df = spark.createDataFrame(data)
    
    # Simulación del filtro de calidad aplicado en el pipeline
    df_valid = df.filter(F.col("precio").isNotNull())
    df_invalid = df.filter(F.col("precio").isNull())
    
    # Validaciones
    assert df_valid.count() == 1
    assert df_valid.first().id == 1
    assert df_invalid.count() == 1
    assert df_invalid.first().id == 2