### 1. Ambigüedad y redundancia en el particionado de la Capa Bronze
* [cite_start]**Observación:** La sección 5.4 de la arquitectura indica que la capa Bronze debe ser particionada por `fecha_proceso` y `tenant_id`.
Sin embargo, la sección 5.2 establece una estrategia de aislamiento físico donde cada tenant posee su propia ruta base (`data/<layer>/<tenant>/...`) o schema en Unity Catalog.

* **Resolución en la implementación:** Particionar físicamente por `_tenant_id` dentro de un directorio que ya es exclusivo de ese tenant genera una subcarpeta redundante 
(ej. `.../hn/deliveries/fecha_proceso=20250128/_tenant_id=hn`). Decidí omitir la columna `_tenant_id` de la instrucción `partitionBy` en la escritura Delta de Bronze. 
Esto elimina metadatos innecesarios y alinea el almacenamiento local exactamente con el diseño lógico requerido para un futuro despliegue sobre Unity Catalog. 
Se conservó la columna `_tenant_id` dentro del DataFrame únicamente con fines de trazabilidad de gobierno de datos.

### 2. Ambigüedad en tipos de datos para el Join Temporal SCD2
* **Observación:** La sección 5.7 requiere enriquecer `fact_deliveries` con `dim_materials` usando un join temporal basado en la condición `fecha_proceso BETWEEN valid_from AND valid_to`. 
Sin embargo, la sección 4.1 define `fecha_proceso` como un string en formato `YYYYMMDD`, mientras que el catálogo define `valid_from` y `valid_to` como tipo `date` (YYYY-MM-DD).
* **Resolución en la implementación:** Realizar un `BETWEEN` directo entre un string `YYYYMMDD` y campos `date` puede generar comportamientos impredecibles o fallos de validación en el
 motor de Spark. En la capa Silver, implementé un casteo explícito transformando `fecha_proceso` a `DateType` antes de ejecutar el join temporal para garantizar la precisión de la evaluación 
 y evitar errores de compatibilidad de tipos.

### 3. Mejora tecnológica: Fragilidad del formato CSV en la ingesta RAW
* **Observación:** La arquitectura actual define la capa RAW basada en archivos CSV, un formato propenso a errores de parseo (por ejemplo, comas incrustadas o saltos de línea dentro del 
campo `descripcion` del catálogo de materiales).
* **Mejora tecnológica (Horizonte 2):** Para futuras iteraciones, propongo reemplazar la lectura estática de CSV por **Databricks Auto Loader (cloudFiles)** en la capa Bronze. 
Esto permitiría inferir y evolucionar esquemas automáticamente (schema drift), manejar archivos corruptos mediante `rescuedDataColumn`, y procesar ingestas incrementales de forma 
mucho más robusta y económica que leer lotes completos de CSV repetidamente.