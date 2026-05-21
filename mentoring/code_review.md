# Revisión de Código

## Observaciones Accionables

### 1. Uso de Pandas e iteración manual (`iterrows`) en un entorno Spark
* **Qué está mal:** El código lee con Pandas y usa un bucle `for` con `iterrows()` antes de convertir a Spark.
* **Por qué importa:** Destruye el propósito del procesamiento distribuido. Causará fallos por OutOfMemory (OOM) con volúmenes grandes.
* **Cómo se corrige:** Usar `spark.read.csv` y aplicar transformaciones nativas de PySpark con `withColumn`.

### 2. Escritura no idempotente y rutas en crudo (Hardcoding)
* **Qué está mal:** Guarda en una ruta hardcodeada temporal usando Parquet clásico con un `overwrite` general.
* **Por qué importa:** Si falla a la mitad, deja datos corruptos. Impide la promoción entre ambientes y rompe el estándar Medallion.
* **Cómo se corrige:** Parametrizar las rutas, usar formato `delta` y aplicar particionado dinámico.

### 3. Falta de tipado y manejo estricto de esquemas
* **Qué está mal:** Pandas infiere tipos al vuelo y luego Spark vuelve a inferirlos en `createDataFrame`. 
* **Por qué importa:** La inferencia doble es riesgosa y costosa. Un cambio de formato en origen romperá las capas posteriores sin avisar.
* **Cómo se corrige:** Definir un esquema explícito o castear los tipos al leer.

### 4. Ausencia de manejo de excepciones y logging
* **Qué está mal:** Flujo "happy path" que solo hace `print("done")`.
* **Por qué importa:** En Databricks, un error detendrá el pipeline abruptamente. Sin logs, debuggear es una pesadilla.
* **Cómo se corrige:** Usar bloques `try-except` y la librería `logging`.

---

## Cómo se lo explicaría al junior
Abordaría el feedback desde una perspectiva de escalabilidad. Le diría: *"El resultado funcional es correcto. Sin embargo, estamos en un clúster distribuido, y al usar Pandas y bucles `for`, estamos obligando a un Ferrari a ir a la velocidad de una bicicleta."* Le pediría que investigue para la próxima sesión:
1. Diferencias entre procesamiento en un solo nodo y procesamiento distribuido.
2. El uso de `when().otherwise()` como reemplazo a los condicionales iterativos.
3. El concepto de Idempotencia y por qué preferimos Delta sobre Parquet.