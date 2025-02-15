# ETL - Gestión de Residuos

Este proyecto implementa un proceso ETL en Python para consolidar y transformar datos de residuos provenientes de dos fuentes:  
- **dataset_disposicion_residuos.csv**  
- **dataset_generacion_residuos.csv**

La información se estructura en un modelo estrella (star schema) mediante la creación de tablas de dimensiones y una tabla de hechos, lo que facilita el análisis y la creación de dashboards. Además, se genera un diagrama ER utilizando ERAlchemy y SQLite.

---

## Tabla de Contenidos

- [Descripción](#descripción)
- [Características](#características)
- [Prerequisitos](#prerequisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Proceso ETL](#proceso-etl)
- [Generación del Diagrama ER](#generación-del-diagrama-er)
- [Licencia](#licencia)

---

## Descripción

El objetivo de este proyecto es transformar datos de residuos en un modelo de data warehouse que permita analizar la generación y disposición de residuos desde múltiples dimensiones (tiempo, ubicación, municipalidad, demografía y sitio de disposición). El proceso incluye:

- **Extracción:** Lectura de archivos CSV.
- **Transformación:** Limpieza, deduplicación, conversión de fechas y unión de datasets.
- **Carga:** Creación de tablas de dimensiones y una tabla de hechos, y posterior inserción en una base de datos SQLite.
- **Visualización del Modelo:** Generación de un diagrama ER para visualizar la estructura del data warehouse.

---

## Características

- **Limpieza y Transformación:** Se eliminan duplicados, se convierten fechas y se extraen atributos temporales.
- **Modelado Dimensional:** Se crean dimensiones para tiempo, ubicación, municipalidad, demografía y sitio de disposición.
- **Tabla de Hechos:** Se consolidan las métricas de generación y disposición en una única tabla.
- **Integración con SQLite:** Los datos transformados se cargan en una base de datos SQLite.
- **Diagrama ER:** Se genera un diagrama de relaciones de entidades que refleja el modelo estrella.

---

## Prerequisitos

- **Python 3.x**
- **Pandas**
- **SQLite3** (incluido en la biblioteca estándar de Python)
- **ERAlchemy**
- **Graphviz** o **PyGraphviz** (instalado a nivel de sistema para generar el diagrama ER)

---

## Instalación

1. **Clonar el repositorio:**

   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd <NOMBRE_DEL_DIRECTORIO>
