# Proyecto SAAS — Plataforma de Datos Multi-Tenant

Este repositorio contiene la implementación de una plataforma de datos empresarial basada en arquitectura Medallion (Bronze, Silver, Gold) utilizando PySpark y 
Delta Lake. 

El pipeline está diseñado bajo un enfoque multi-tenant dinámico, asegurando el aislamiento lógico de datos y facilitando la escalabilidad a nuevos países (tenants) 
mediante configuración jerárquica con OmegaConf.

## 1. Estructura del Repositorio

La estructura del proyecto respeta la separación entre configuración, código fuente y datos locales:

```text
saas-data-platform/
├── .github/                   # Workflows de CI/CD (GitHub Actions)
├── config/                    # Configuración jerárquica con OmegaConf
│   ├── base.yaml              
│   ├── env/                   
│   └── tenants/               # Ficheros YAML para descubrimiento de tenants
├── data/                      # Directorio local (Ignorado en control de versiones)
│   ├── raw/                   # CSVs de origen
│   └── bronze/                # Tablas Delta particionadas por tenant
├── docs/                      # Documentación (Observaciones e Infraestructura)
├── mentoring/                 # Ejercicio de revisión de código (bad_code/good_code)
├── src/
│   └── saas_pipeline/         # Código fuente del pipeline PySpark
├── tests/                     # Pruebas unitarias con pytest
├── .gitignore                 # Exclusión de entornos virtuales y datos locales
├── Makefile                   # Comandos rápidos para ejecución y validación
├── pyproject.toml             # Gestión de dependencias
└── README.md                  # Documentación principal
```

## 2. Instrucciones de Configuración (Setup)
El proyecto requiere Python 3.11+, PySpark 3.5.x y Delta Lake 3.x.
Para inicializar el entorno y garantizar la reproducibilidad de las dependencias, ejecuta los siguientes comandos en tu terminal:

Crear y activar el entorno virtual:

	#2.1 En Windows
	python -m venv .venv
	.venv\Scripts\activate
	
	#2.2 Instalar dependencias del proyecto:
	pip install -e .
	(Nota: Esto instalará automáticamente pyspark, delta-spark, omegaconf y herramientas de desarrollo según lo declarado en el archivo de configuración).

	#2.3 Ubicación de los datos iniciales:
	Asegúrate de colocar los archivos global_mobility_data_entrega_productos.csv y materials_catalog.csv dentro de la carpeta data/raw/ antes de la primera 
	ejecución.
	
## 3. Ejecución del Pipeline
El pipeline se ejecuta a través del CLI centralizado. El motor de Spark se inicializa de forma local con soporte nativo para formato Delta.

	# 3.1 Procesar un tenant específico (Ej: El Salvador):

	python src/saas_pipeline/cli.py --tenant sv
	Procesar todos los tenants activos:

	# 3.2 Procesar todos los tenants activos:
	python src/saas_pipeline/cli.py --tenant all

FinOps Note: El pipeline garantiza un cierre limpio de la sesión (spark.stop()) al finalizar, liberando recursos inmediatamente para evitar consumo innecesario 
de créditos en despliegues sobre clústeres cloud.

## 4. Onboarding de un Nuevo Tenant
La plataforma cumple estrictamente con el requerimiento de "mínima fricción" para adherir nuevas unidades de negocio. El proceso de onboarding es 100% dinámico 
y no requiere modificar el código fuente en Python.

Para agregar un nuevo país (ej: Colombia, con código co):

	1. Navega a la carpeta config/tenants/.

	2. Crea un archivo vacío llamado co.yaml.

	3. Ejecuta el pipeline con --tenant all.

El orquestador (cli.py) detectará automáticamente el archivo, agregará a Colombia a la lista de iteración y compondrá dinámicamente sus rutas de almacenamiento 
(ej: data/bronze/co/deliveries).