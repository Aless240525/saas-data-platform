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
El proyecto requiere **Python 3.11+**, **Java 11**, PySpark 3.5.x y Delta Lake 3.x.

### 2.1 Entorno Virtual y Dependencias (General)
Para inicializar el entorno y garantizar la reproducibilidad, ejecuta en tu terminal:

```bash
# 1. Crear y activar entorno virtual
python -m venv .venv
.venv\Scripts\activate      # En Windows
# source .venv/bin/activate # En Mac/Linux

# 2. Instalar dependencias del proyecto
pip install -e .
```

**Datos iniciales:** Asegúrate de colocar los archivos `global_mobility_data_entrega_productos.csv` y `materials_catalog.csv` dentro de la carpeta `data/raw/` antes de la primera ejecución.

### 2.2 Configuración Local (Exclusivo para evaluadores en Windows)
*(Nota: Si evalúas este repositorio en Mac, Linux o clústeres nativos de Databricks, omite este paso).*

PySpark y Delta Lake en Windows requieren dependencias nativas de Hadoop (`winutils`). En una terminal **PowerShell**, ejecuta este bloque por única vez para descargar los binarios. 
Las variables de entorno problemáticas se manejarán automáticamente en tiempo de ejecución mediante el script `run.ps1`:

``` Terminal
New-Item -Path "C:\hadoop\bin" -ItemType Directory -Force
Invoke-WebRequest -Uri "[https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.1/bin/winutils.exe](https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.1/bin/winutils.exe)" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "[https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.1/bin/hadoop.dll](https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.1/bin/hadoop.dll)" -OutFile "C:\hadoop\bin\hadoop.dll"
```
## 3. Ejecución del Pipeline
El pipeline se ejecuta a través de un CLI centralizado. El motor de Spark se inicializa de forma local con soporte nativo para formato Delta.

**Para usuarios en Windows:**
Se provee un script envoltorio (`run.ps1`) que inyecta las variables de entorno de Java, Spark y Hadoop de forma efímera para evitar conflictos con las configuraciones 
globales del sistema operativo.
``` Terminal
# Procesar un tenant específico (Ej: El Salvador)
.\run.ps1 sv

# Procesar todos los tenants activos
.\run.ps1 all
```

## 4. Onboarding de un Nuevo Tenant
La plataforma cumple estrictamente con el requerimiento de "mínima fricción" para adherir nuevas unidades de negocio. El proceso de onboarding es 100% dinámico 
y no requiere modificar el código fuente en Python.

Para agregar un nuevo país (ej: Colombia, con código co):

	1. Navega a la carpeta config/tenants/.

	2. Crea un archivo vacío llamado co.yaml.

	3. Ejecuta el pipeline con --tenant all.

El orquestador (cli.py) detectará automáticamente el archivo, agregará a Colombia a la lista de iteración y compondrá dinámicamente sus rutas de almacenamiento 
(ej: data/bronze/co/deliveries).