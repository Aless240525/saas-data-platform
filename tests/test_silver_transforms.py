import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

# Fixture para inicializar Spark una sola vez por sesión de pruebas
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-saas-pipeline") \
        .master("local[2]") \
        .getOrCreate()

def test_unit_conversion_cs_to_st(spark):
    """
    Prueba la conversión de cajas (CS) a unidades (ST).
    Regla: 1 CS = 20 ST.
    """
    data = [Row(cantidad=2, unidad="CS"), Row(cantidad=5, unidad="ST")]
    df = spark.createDataFrame(data)
    
    # Lógica de Silver
    df_res = df.withColumn(
        "cantidad_normalizada_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * 20).otherwise(F.col("cantidad"))
    )
    
    res = df_res.collect()
    assert res[0].cantidad_normalizada_st == 40  # 2 * 20
    assert res[1].cantidad_normalizada_st == 5   # Ya estaba en ST, no se multiplica

def test_delivery_type_filtering(spark):
    """
    Prueba el filtrado de tipos de entrega válidos.
    Regla: Solo ZPRE, ZVE1, Z04, Z05 ingresan a Silver. COBR se descarta.
    """
    data = [Row(tipo_entrega="ZPRE"), Row(tipo_entrega="COBR"), Row(tipo_entrega="Z04")]
    df = spark.createDataFrame(data)
    
    valid_types = ["ZPRE", "ZVE1", "Z04", "Z05"]
    df_res = df.filter(F.col("tipo_entrega").isin(valid_types))
    
    res = [row.tipo_entrega for row in df_res.collect()]
    assert "ZPRE" in res
    assert "Z04" in res
    assert "COBR" not in res
    assert len(res) == 2

def test_anomaly_handling_negative_quantity(spark):
    """
    Prueba la segregación de anomalías por cantidad.
    Regla: Cantidad <= 0 se aísla en cuarentena.
    """
    data = [Row(id=1, cantidad_normalizada_st=-5), Row(id=2, cantidad_normalizada_st=10)]
    df = spark.createDataFrame(data)
    
    # Lógica de segregación
    df_valid = df.filter(F.col("cantidad_normalizada_st") > 0)
    df_quarantine = df.filter(F.col("cantidad_normalizada_st") <= 0)
    
    assert df_valid.count() == 1
    assert df_valid.first().id == 2
    assert df_quarantine.count() == 1
    assert df_quarantine.first().id == 1