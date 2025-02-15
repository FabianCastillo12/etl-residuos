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

En el contexto actual, la gestión de residuos es uno de los desafíos ambientales y económicos más apremiantes para las administraciones locales y regionales. El incremento en la generación de residuos, impulsado por el crecimiento poblacional y el aumento del consumo, pone de manifiesto la necesidad de contar con sistemas eficientes y sostenibles para la recolección, tratamiento y disposición final. Sin embargo, uno de los problemas críticos que enfrentan las autoridades es la falta de información consolidada y de calidad que permita tomar decisiones informadas.

Muchas veces, los datos relacionados con la generación y disposición de residuos provienen de fuentes dispersas y en formatos inconsistentes, lo que dificulta su análisis y el seguimiento de indicadores clave. La fragmentación y falta de estandarización de estos datos impide identificar, de forma oportuna, las áreas críticas y evaluar el impacto real de las políticas y acciones implementadas. Esto genera incertidumbre y limita la capacidad de diseñar estrategias que optimicen el uso de recursos y minimicen el impacto ambiental.

Además, es importante destacar que los datasets utilizados en este proyecto fueron encontrados en la página de datos libres del Ministerio de Ambiente del Perú, lo que garantiza que la información es oficial y relevante para el contexto nacional. Esto aporta una base sólida y confiable para el análisis y la toma de decisiones en la gestión de residuos.

El proyecto ETL desarrollado se justifica en este escenario por las siguientes razones:

- **Integración de Datos:** Al consolidar la información de diferentes fuentes en un único modelo dimensional, se facilita la generación de una visión integral de la situación, permitiendo comparar datos a lo largo del tiempo y entre distintas áreas geográficas.
- **Modelado Dimensional Eficiente:** La estructuración en tablas de dimensiones y una tabla de hechos (modelo estrella) permite identificar y analizar indicadores clave, como la generación per cápita, la eficiencia en la disposición de residuos o la cobertura de los sistemas de recolección y tratamiento.
- **Soporte para la Toma de Decisiones:** Con datos consolidados y consistentes, las autoridades y responsables pueden diseñar políticas públicas más efectivas, optimizar recursos, priorizar inversiones en infraestructura y establecer programas de reciclaje y reducción de residuos basados en evidencia.
- **Sostenibilidad Ambiental y Económica:** Un sistema de gestión de residuos que se base en datos confiables contribuye a reducir el impacto ambiental, mejora la salud pública y optimiza los costos operativos, generando beneficios a largo plazo para la comunidad.

En resumen, este proyecto ETL no solo responde a la necesidad de contar con información integrada y estandarizada en el área de la gestión de residuos, sino que también sienta las bases para la creación de dashboards y análisis estratégicos. Estos permiten transformar datos dispersos en conocimiento útil, apoyando la toma de decisiones y promoviendo un manejo más eficiente y responsable de los residuos. La utilización de datos oficiales del Ministerio de Ambiente del Perú refuerza la relevancia y confiabilidad del análisis, posicionando al proyecto como una herramienta valiosa para enfrentar los desafíos actuales en la gestión de residuos.

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
    ```

2. **Instalar los paquetes de Python necesarios:**

    ```bash
    pip install pandas eralchemy
    ```

3. **Instalar Graphviz en tu sistema:**

    En Ubuntu/Debian:

    ```bash
    sudo apt-get update
    sudo apt-get install graphviz
    ```

    En macOS (usando Homebrew):

    ```bash
    brew install graphviz
    ```

    En Windows:

    Descarga e instala Graphviz desde [Graphviz Download](https://graphviz.org/download/), y asegúrate de agregarlo a la variable de entorno PATH.

---

## Uso

Coloca los datasets en el directorio raíz del proyecto:

- **dataset_disposicion_residuos.csv**
- **dataset_generacion_residuos.csv**

Ejecuta el script ETL:

```bash
python etl.py
```

Esto realizará lo siguiente:

- Carga y transformación de los datos.
- Creación de las dimensiones y la tabla de hechos final.
- Inserción de las tablas en una base de datos SQLite (temp.db).
- Generación de un diagrama ER guardado en er_diagram.png.

---

## Estructura del Proyecto

```bash
├── dataset_disposicion_residuos.csv
├── dataset_generacion_residuos.csv
├── etl.py
├── temp.db                 # Base de datos generada (SQLite)
├── er_diagram.png          # Diagrama ER generado
└── README.md
```

---

## Proceso ETL

### Carga de Datos:

Se leen los archivos CSV y se muestra información inicial de cada uno.

### Transformación:

- Se eliminan duplicados.
- Se convierten las columnas de fecha a formato datetime.
- Se extraen los componentes de la fecha (AÑO_CORTE, MES_CORTE, DIA_CORTE, TRIMESTRE).
- Se renombra la columna ANIO a AÑO para mayor consistencia.

### Unión de Datasets:

Se realiza un merge (outer join) utilizando claves definidas para combinar la información de ambos archivos. Se utiliza un indicador para identificar registros exclusivos en cada fuente.

### Creación de Dimensiones:

Se generan las siguientes dimensiones:

- **dim_tiempo:** Con FECHA_CORTE y sus componentes.
- **dim_ubicacion:** Con UBIGEO, DEPARTAMENTO, PROVINCIA, DISTRITO y REGION_NATURAL.
- **dim_municipalidad:** Con TIPO_MUNICIPALIDAD y CLASIFICACION_MUNICIPAL_MEF (se elimina UBIGEO para evitar redundancia).
- **dim_demografia:** Con POB_TOTAL_INEI, POB_URBANA_INEI y POB_RURAL_INEI (sin UBIGEO).
- **dim_sitio:** Con TIPO_SITIO_DISPOSICION_FINAL_ADECUADA, NOMBRE_SITIO_DISPOSICION_FINAL_ADECUADA y TIPO_ADMINISTRADOR_SITIO_DISPOSICION_FINAL_ADECUADA.

### Tabla de Hechos

La tabla de hechos, denominada `fact_final`, es el núcleo del modelo dimensional. En ella se almacenan las métricas cuantitativas derivadas de la consolidación de los datos de generación y disposición de residuos, junto con las claves foráneas que vinculan cada registro a las diferentes dimensiones. A continuación, se describe su estructura:

- **Claves Foráneas:**
  - **Tiempo_ID:** Relaciona cada registro con la dimensión de tiempo (`dim_tiempo`), que contiene la fecha de corte y sus componentes (año, mes, día, trimestre).
  - **Ubicacion_ID:** Conecta el registro con la dimensión de ubicación (`dim_ubicacion`), que incluye el UBIGEO y datos geográficos (departamento, provincia, distrito y región natural).
  - **Municipalidad_ID:** Vincula el registro con la dimensión de municipalidad (`dim_municipalidad`), que abarca información sobre el tipo y la clasificación de la entidad municipal.
  - **Demografia_ID:** Relaciona el registro con la dimensión de demografía (`dim_demografia`), que contiene los datos poblacionales (población total, urbana y rural).
  - **SitioDisposicion_ID:** Asocia el registro con la dimensión de sitio de disposición (`dim_sitio`), que incluye información sobre el tipo, nombre y administrador del sitio de disposición.

- **Métricas:**
  - **AÑO:** Representa el año en que se realiza la medición, facilitando análisis temporales (aunque la dimensión de tiempo contiene una información más detallada).
  - **Generación de Residuos:**
    - `GENERACION_PER_CAPITA_DOM`: Generación de residuos per cápita a nivel domiciliario.
    - `GENERACION_DOM_URBANA_TDIA`: Generación diaria de residuos en zona urbana (domiciliaria).
    - `GENERACION_DOM_URBANA_TANIO`: Generación anual de residuos en zona urbana (domiciliaria).
    - `GENERACION_MUN_TDIA`: Generación diaria de residuos a nivel municipal.
    - `GENERACION_MUN_TANIO`: Generación anual de residuos a nivel municipal.
    - `GENERACION_PER_CAPITA_MUNICIPAL`: Generación de residuos per cápita a nivel municipal.
  - **Disposición de Residuos:**
    - `DISPOSICION_FINAL_ADECUADA`: Indica si la disposición final de los residuos es considerada adecuada.
    - `COBERTURA_DISPOSICION_FINAL_ADECUADA`: Mide el porcentaje o indicador de cobertura de la disposición final adecuada.
    - `RESIDUOS_MUNICIPALES_DISPUESTOS`: Cantidad total de residuos municipales dispuestos.

Para asegurar la integridad de los análisis, se imputan valores nulos en las métricas con 0, evitando que la falta de datos afecte los resultados.

Esta estructura permite realizar análisis complejos, como calcular tasas de generación per cápita, evaluar la eficiencia de la disposición final y comparar la generación y disposición de residuos a través de diferentes dimensiones (temporal, geográfica, institucional y demográfica), facilitando así la toma de decisiones basada en datos.


### Carga en SQLite:

Se escriben las tablas resultantes en una base de datos SQLite (temp.db).

---

## Generación del Diagrama ER

El script utiliza ERAlchemy para generar un diagrama ER a partir de la base de datos SQLite. El diagrama resultante (er_diagram.png) muestra las relaciones entre la tabla de hechos y las dimensiones.

---

## Licencia

Este proyecto se distribuye bajo la licencia MIT. Consulta el archivo LICENSE para más detalles.