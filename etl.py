import pandas as pd
import sqlite3
from eralchemy import render_er

def load_data():
    """
    Carga los datasets de generación y disposición desde archivos CSV.
    """
    df_generacion = pd.read_csv("dataset_disposicion_residuos.csv", sep=";", encoding="latin1")
    df_disposicion = pd.read_csv("dataset_generacion_residuos.csv", sep=";", encoding="latin1")
    return df_generacion, df_disposicion

def display_initial_data(df_generacion, df_disposicion):
    """
    Muestra las primeras filas e información básica de los datasets.
    """
    print("Generación de residuos inicial")
    print(df_generacion.head())
    print(df_generacion.info())

    print("Disposición de residuos inicial")
    print(df_disposicion.head())
    print(df_disposicion.info())

def transform_data(df_generacion, df_disposicion):
    """
    Realiza la transformación de datos:
      - Elimina duplicados.
      - Revisa valores nulos en las columnas críticas.
      - Convierte 'FECHA_CORTE' a datetime.
      - Extrae componentes de la fecha.
      - Renombra 'ANIO' a 'AÑO'.
    """
    # Eliminar duplicados
    df_generacion = df_generacion.drop_duplicates()
    df_disposicion = df_disposicion.drop_duplicates()

    # Revisar valores nulos en columnas críticas
    print("Valores nulos en df_generacion:")
    print(df_generacion[['UBIGEO', 'FECHA_CORTE', 'ANIO']].isnull().sum())
    print("Valores nulos en df_disposicion:")
    print(df_disposicion[['UBIGEO', 'FECHA_CORTE', 'ANIO']].isnull().sum())

    # Convertir 'FECHA_CORTE' a formato datetime
    df_generacion['FECHA_CORTE'] = pd.to_datetime(df_generacion['FECHA_CORTE'].astype(str), format='%Y%m%d')
    df_disposicion['FECHA_CORTE'] = pd.to_datetime(df_disposicion['FECHA_CORTE'].astype(str), format='%Y%m%d')

    # Extraer componentes de la fecha para la dimensión de tiempo
    for df in [df_generacion, df_disposicion]:
        df['AÑO_CORTE'] = df['FECHA_CORTE'].dt.year
        df['MES_CORTE'] = df['FECHA_CORTE'].dt.month
        df['DIA_CORTE'] = df['FECHA_CORTE'].dt.day
        df['TRIMESTRE'] = df['FECHA_CORTE'].dt.quarter

    # Renombrar 'ANIO' a 'AÑO'
    df_generacion.rename(columns={'ANIO': 'AÑO'}, inplace=True)
    df_disposicion.rename(columns={'ANIO': 'AÑO'}, inplace=True)

    return df_generacion, df_disposicion

def merge_datasets(df_generacion, df_disposicion, keys):
    """
    Realiza un merge (outer join) de los dos datasets utilizando las claves definidas.
    Se incluye el indicador para verificar el origen de cada registro.
    """
    df_merged = pd.merge(df_generacion, df_disposicion, on=keys, how='outer', indicator=True)
    print("Conteo del indicador _merge:")
    print(df_merged['_merge'].value_counts())
    print(f"Registros en df_merged: {len(df_merged)}")
    print("Datos unidos:")
    print(df_merged.info())
    return df_merged

def create_dimensions(df_merged):
    """
    Crea las dimensiones a partir de df_merged:
      - dim_tiempo: Basada en FECHA_CORTE y sus componentes.
      - dim_ubicacion: Contiene UBIGEO, DEPARTAMENTO, PROVINCIA, DISTRITO y REGION_NATURAL.
      - dim_municipalidad: Con TIPO_MUNICIPALIDAD y CLASIFICACION_MUNICIPAL_MEF (sin UBIGEO).
      - dim_demografia: Con datos poblacionales (sin UBIGEO).
      - dim_sitio: Con información del sitio de disposición.
    """
    # Dimensión de Tiempo
    dim_tiempo = df_merged[['FECHA_CORTE', 'AÑO_CORTE', 'MES_CORTE', 'DIA_CORTE', 'TRIMESTRE']].drop_duplicates().reset_index(drop=True)
    dim_tiempo['TIEMPO_ID'] = dim_tiempo.index + 1

    # Dimensión de Ubicación
    dim_ubicacion = df_merged[['UBIGEO', 'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'REGION_NATURAL']].drop_duplicates().reset_index(drop=True)
    dim_ubicacion['UBICACION_ID'] = dim_ubicacion.index + 1

    # Dimensión de Municipalidad 
    dim_municipalidad = df_merged[['TIPO_MUNICIPALIDAD', 'CLASIFICACION_MUNICIPAL_MEF']].drop_duplicates().reset_index(drop=True)
    dim_municipalidad['MUNICIPALIDAD_ID'] = dim_municipalidad.index + 1

    # Dimensión de Demografía 
    dim_demografia = df_merged[['POB_TOTAL_INEI', 'POB_URBANA_INEI', 'POB_RURAL_INEI']].drop_duplicates().reset_index(drop=True)
    dim_demografia['DEMOGRAFIA_ID'] = dim_demografia.index + 1

    # Dimensión de Sitio de Disposición
    dim_sitio = df_merged[['TIPO_SITIO_DISPOSICION_FINAL_ADECUADA',
                           'NOMBRE_SITIO_DISPOSICION_FINAL_ADECUADA',
                           'TIPO_ADMINISTRADOR_SITIO_DISPOSICION_FINAL_ADECUADA']].drop_duplicates().reset_index(drop=True)
    dim_sitio['SITIO_DISPOSICION_ID'] = dim_sitio.index + 1

    # Mostrar dimensiones
    print("Dimensión de tiempo:")
    print(dim_tiempo)
    print("Dimensión de ubicación:")
    print(dim_ubicacion)
    print("Dimensión de municipalidad:")
    print(dim_municipalidad)
    print("Dimensión de demografía:")
    print(dim_demografia)
    print("Dimensión de sitio de disposición:")
    print(dim_sitio)

    return dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio

def create_fact_table(df_merged, dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio):
    """
    Mapea las dimensiones a la tabla de hechos.
    Se realiza el merge con cada dimensión y se extraen las columnas finales.
    También se imputan valores nulos en las métricas.
    """
    fact = df_merged.copy()

    # Mapear la dimensión Tiempo
    fact = fact.merge(dim_tiempo, on=['FECHA_CORTE', 'AÑO_CORTE', 'MES_CORTE', 'DIA_CORTE', 'TRIMESTRE'], how='left')
    # Mapear la dimensión Ubicación
    fact = fact.merge(dim_ubicacion, on=['UBIGEO', 'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'REGION_NATURAL'], how='left')
    # Mapear la dimensión Municipalidad
    fact = fact.merge(dim_municipalidad, on=['TIPO_MUNICIPALIDAD', 'CLASIFICACION_MUNICIPAL_MEF'], how='left')
    # Mapear la dimensión Demográfica
    fact = fact.merge(dim_demografia, on=['POB_TOTAL_INEI', 'POB_URBANA_INEI', 'POB_RURAL_INEI'], how='left')
    # Mapear la dimensión Sitio de Disposición
    fact = fact.merge(dim_sitio, on=['TIPO_SITIO_DISPOSICION_FINAL_ADECUADA',
                                     'NOMBRE_SITIO_DISPOSICION_FINAL_ADECUADA',
                                     'TIPO_ADMINISTRADOR_SITIO_DISPOSICION_FINAL_ADECUADA'], how='left')

    print("Tabla de hechos (fact) tras mapear dimensiones:")
    print(fact)
    print("Columnas de la tabla de hechos:")
    print(fact.columns)

    # Extraer la tabla de hechos final con las métricas y claves foráneas
    fact_final = fact[[
        'TIEMPO_ID', 'UBICACION_ID', 'MUNICIPALIDAD_ID', 'DEMOGRAFIA_ID', 'SITIO_DISPOSICION_ID', 'AÑO',
        'GENERACION_PER_CAPITA_DOM', 'GENERACION_DOM_URBANA_TDIA', 'GENERACION_DOM_URBANA_TANIO',
        'GENERACION_MUN_TDIA', 'GENERACION_MUN_TANIO', 'GENERACION_PER_CAPITA_MUNICIPAL',
        'DISPOSICION_FINAL_ADECUADA', 'COBERTURA_DISPOSICION_FINAL_ADECUADA', 'RESIDUOS_MUNICIPALES_DISPUESTOS'
    ]]

    # Imputar valores nulos en las métricas con 0
    metricas = [
        'GENERACION_PER_CAPITA_DOM', 'GENERACION_DOM_URBANA_TDIA', 'GENERACION_DOM_URBANA_TANIO',
        'GENERACION_MUN_TDIA', 'GENERACION_MUN_TANIO', 'GENERACION_PER_CAPITA_MUNICIPAL',
        'DISPOSICION_FINAL_ADECUADA', 'COBERTURA_DISPOSICION_FINAL_ADECUADA', 'RESIDUOS_MUNICIPALES_DISPUESTOS'
    ]
    for metrica in metricas:
        fact_final[metrica] = fact_final[metrica].fillna(0)

    print("Columnas de la tabla de hechos final:")
    print(fact_final.columns)
    print("Tabla de hechos final:")
    print(fact_final)

    return fact_final

def write_to_database(fact_final, dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio):
    """
    Escribe la tabla de hechos y las dimensiones en una base de datos SQLite.
    """
    conn = sqlite3.connect("temp.db")
    fact_final.to_sql("fact_final", conn, if_exists="replace", index=False)
    dim_tiempo.to_sql("dim_tiempo", conn, if_exists="replace", index=False)
    dim_ubicacion.to_sql("dim_ubicacion", conn, if_exists="replace", index=False)
    dim_municipalidad.to_sql("dim_municipalidad", conn, if_exists="replace", index=False)
    dim_demografia.to_sql("dim_demografia", conn, if_exists="replace", index=False)
    dim_sitio.to_sql("dim_sitio", conn, if_exists="replace", index=False)
    conn.close()

def generate_er_diagram():
    """
    Genera un diagrama ER (Entity-Relationship) a partir de la base de datos SQLite.
    """
    render_er("sqlite:///temp.db", "er_diagram.png")

def main():
    # Cargar los datos
    df_generacion, df_disposicion = load_data()

    # Mostrar datos iniciales
    display_initial_data(df_generacion, df_disposicion)

    # Transformar los datos
    df_generacion, df_disposicion = transform_data(df_generacion, df_disposicion)

    # Definir las columnas clave para el merge
    keys = [
        "UBIGEO", "FECHA_CORTE", "AÑO", "AÑO_CORTE", "MES_CORTE", "DIA_CORTE", "TRIMESTRE",
        "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "REGION_NATURAL", "TIPO_MUNICIPALIDAD",
        "POB_TOTAL_INEI", "POB_URBANA_INEI", "POB_RURAL_INEI", "CLASIFICACION_MUNICIPAL_MEF"
    ]

    # Realizar el merge de los datasets
    df_merged = merge_datasets(df_generacion, df_disposicion, keys)

    # Crear las dimensiones
    dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio = create_dimensions(df_merged)

    # Crear la tabla de hechos final
    fact_final = create_fact_table(df_merged, dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio)

    # Escribir las tablas en la base de datos SQLite
    write_to_database(fact_final, dim_tiempo, dim_ubicacion, dim_municipalidad, dim_demografia, dim_sitio)

    # Generar el diagrama ER
    generate_er_diagram()

    fact_final.to_csv("fact_final.csv", index=False)
    dim_tiempo.to_csv("dim_tiempo.csv", index=False)
    dim_ubicacion.to_csv("dim_ubicacion.csv", index=False)
    dim_municipalidad.to_csv("dim_municipalidad.csv", index=False)
    dim_demografia.to_csv("dim_demografia.csv", index=False)
    dim_sitio.to_csv("dim_sitio.csv", index=False)

if __name__ == "__main__":
    main()
