import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

"""
--------------------------------------------------------------------------------------
                        1. EXTRACT (Cargamos los datos crudos)
--------------------------------------------------------------------------------------
"""

print("Cargando los datos...")
try:
    df_clientes =  pd.read_csv('crm_clientes_raw.csv')
    df_ventas = pd.read_csv('crm_ventas_raw.csv')
    print("Carga de arhivos correcta...")
except Exception as e:
    print("Error durante el proceso de carga de los archivos: ", e)

"""
--------------------------------------------------------------------------------------
                    2. TRANSFORM - TABLA VENTAS (FACT TABLE)
--------------------------------------------------------------------------------------
"""

print("Transformando los datos de ventas...")

# A. Transformamos a datetime las fechas de nuestra columna.
cols_fecha = ['Fecha_Creacion_Oportunidad', 'Fecha_Cierre_Real']
for col in cols_fecha:
    df_ventas[col] = pd.to_datetime(df_ventas[col], errors='coerce')
print("Fechas corregidas: ")
print(df_ventas.info())


# --- BLOQUE DE REPARACIÓN DE DATOS (Insertar en Transformación) ---

print("Reparando fechas de cierre faltantes...")

# 1. Identificar ventas que dicen estar 'Cerradas' pero no tienen fecha
# Filtramos por las etapas que indican fin del proceso
filtro_cerradas = df_ventas['Etapa'].isin(['Cerrado Ganado', 'Cerrado Perdido']) & df_ventas['Fecha_Cierre_Real'].isna()

num_a_reparar = filtro_cerradas.sum()
print(f"Detectadas {num_a_reparar} ventas cerradas sin fecha. Generando fechas...")

if num_a_reparar > 0:
    # 2. Generar días aleatorios de duración (entre 5 y 120 días)
    # Creamos un array de números aleatorios del tamaño exacto que necesitamos
    dias_simulados = np.random.randint(5, 120, size=num_a_reparar)
    
    # 3. Calcular la fecha de cierre sumando esos días a la fecha de creación
    # Usamos .loc para asignar solo a las filas filtradas
    df_ventas.loc[filtro_cerradas, 'Fecha_Cierre_Real'] = \
        df_ventas.loc[filtro_cerradas, 'Fecha_Creacion_Oportunidad'] + \
        pd.to_timedelta(dias_simulados, unit='D')

# --- FIN DEL BLOQUE DE REPARACIÓN ---

# AHORA SÍ, recalculamos el ciclo de venta
df_ventas['Dias_Ciclo_Venta'] = (df_ventas['Fecha_Cierre_Real'] - df_ventas['Fecha_Creacion_Oportunidad']).dt.days

# Verificación rápida en consola
print("\nVerificación post-reparación:")
print(df_ventas[['Etapa', 'Fecha_Creacion_Oportunidad', 'Fecha_Cierre_Real', 'Dias_Ciclo_Venta']].dropna().head())

# B. Lógica detrás del negocio: Calcular "Días de ciclo de Venta" (Lead Time)
#Solo nos interesa calcular esto si la venta ya se cerró.
# Si la venta sigue abierta entonces el resultado será NaT o NaN.
df_ventas['Dias_Ciclo_Venta'] = (df_ventas['Fecha_Cierre_Real'] - df_ventas['Fecha_Creacion_Oportunidad']).dt.days

#C. Enriquecimiento: Categorizar la venta (Buckets)
# ¿Cuantas ventas enterprise tenemos?

def categorizar_monto(monto):
    if monto < 5000:
        return 'Small Business'
    elif monto < 20000:
        return 'Mid-Market'
    else:
        return 'Enterprise'
    
df_ventas['Categoria_Deal'] = df_ventas['Monto'].apply(categorizar_monto)

#D. Extraemos las fechas para un filtrado rápido
# A veces PoweBI lo hace de manera autómatica, pero es mejor tenerlo por seguridad
df_ventas['Mes_Creación'] = df_ventas['Fecha_Creacion_Oportunidad'].dt.month
df_ventas['Anio_Creacion'] = df_ventas['Fecha_Creacion_Oportunidad'].dt.year

#E. Manejo de Nulos (Limpieza)
# Si no hay fecha de cierre (Venta Abierta), rellenamos para evitar errores en ciertos gráficos
# Ojo, no rellenamos la fecha, pero podemos crear una bandera.
df_ventas['Es_Venta_Cerrada'] = df_ventas['Fecha_Cierre_Real'].notnull().astype(int)
print("Ventas transformadas correctamente")
print(df_ventas.head(5))
print(df_ventas.info())

"""
--------------------------------------------------------------------------------------
                3. TRANSFORM - TABLA CLIENTES (DIM TABLE)
--------------------------------------------------------------------------------------
"""
print("Transformando Clientes...")
#A. Convertimos a fecha.
df_clientes['Fecha_Registro'] = pd.to_datetime(df_clientes['Fecha_Registro'])

# B. Enriquecimiento: Antiguedad del Cliente (en días de hoy)
#Usamos pd.Timestamp.now() normalizando para quitar la hora
df_clientes['Antiguedad_Dias'] = (pd.Timestamp.now().normalize() - df_clientes['Fecha_Registro']).dt.days

# C. Normalizacion de texto (Ejemplo: Ciudad en Mayusculas para evitar problemas)
df_clientes['Ciudad'] = df_clientes['Ciudad'].str.upper().str.strip()

print("Datos de clientes transformados correctamente")
print(df_clientes.head(5))
print(df_clientes.info())
"""
--------------------------------------------------------------------------------------
                4. LOAD (EXPORTAMOS LOS DATOS LIMPIOS PARA POWER BI)
--------------------------------------------------------------------------------------
"""
try:
    DB_USER = "postgres"
    DB_PASS = "admin"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "crm_warehouse"

    connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    #Creamos el motor de la conexión.
    engine = create_engine(connection_str)

    print("Cargando Tablas SQL")
    df_clientes.to_sql('dim_clientes', engine, if_exists='replace', index=False)
    print("Tabla de dim_clientes creada correctamente")
    df_ventas.to_sql("fact_ventas", engine, if_exists="replace", index=False)
    print("Tabla de fact_ventas cargada correctamente")

    print("ETL completado exitosamente!")
except Exception as e:
    print("Error durante la conexión la base de datos.")