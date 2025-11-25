import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

#Inicializar Faker, lo configuramos en español
fake = Faker('es_MX')
Faker.seed(42) #Inicializamos la semilla para que nos genere los mismos datos "aleatorios"

#Configuramos
NUM_CLIENTES = 100
NUM_VENTAS = 500 #Generamos 500 oportunidades historicas.


"""
-------------------------------------------------------------------------------------------
                    1. GENERAMOS TABLA DE DIMENSIÓN: CLIENTES.
-------------------------------------------------------------------------------------------
"""
print("1) Generando clientes...")
industrias = ['Tecnologias', 'Manofactura', 'Retail', 'Salud', 'Finanzas', 'Consultoria']
origenes = ['Website', 'Linkedin', 'Referido', 'Email Cold', 'Evento']

data_clientes = []

for _ in range(NUM_CLIENTES):
    cliente = {
        'ID_Cliente': fake.unique.uuid4()[:8], #ID corto y único
        'Nombre_Empresa': fake.company(),
        'Contacto_Principal': fake.name(),
        'Email': fake.company_email(),
        'Ciudad': fake.city(),
        'Industria': random.choice(industrias),
        'Fuente_Lead': random.choice(origenes),
        'Fecha_Registro': fake.date_between(start_date='-2y', end_date='today')
    }
    data_clientes.append(cliente)

df_clientes = pd.DataFrame(data_clientes)

"""
-------------------------------------------------------------------------------------------
                    2. GENERAMOS LA TABLA DE HECHOS: OPORTUNIDADES (VENTA)
-------------------------------------------------------------------------------------------
"""
print("Generando oportunidades de venta...")

etapas = ['Prospecto', 'Análisis', 'Propuesta', 'Negociación', 'Cerrado Ganado', 'Cerrado Perdido']
vendedores = ['Ana P.', 'Carlos M.', 'Salvador T.', 'Matias A.', 'Sofial L.']

data_ventas = []

for _ in range(NUM_VENTAS):
    #Seleccionamos un cliente random generado de la anterior iteracion.
    cliente_random = df_clientes.sample(1).iloc[0]

    #Lógica de las fechas.
    fecha_creación = fake.date_between(start_date=cliente_random['Fecha_Registro'], end_date='today')

    #Dias que duró la negociación (Lead Time simulado).
    dias_proceso = random.randint(5,120)
    fecha_cierre =  fecha_creación + timedelta(days=dias_proceso)

    #Determinar estado y etapa.
    #Simulamos que el 60% de las ventas ya se cerraron (Ganadas o Perdidas)
    estado_final = random.choices(['Abierto', 'Cerrado'], weights=[0.3, 0.7])[0]

    if estado_final == 'Cerrado':
        etapa = random.choice(['Cerrado Ganado','Cerrado Perdido'])
        probabilidad = 100 if etapa == 'Cerrado Ganado' else 0
    
    else:
        etapa =  random.choice(etapas[:4]) #Solo etapas abiertas
        probabilidad = (etapas.index(etapa) + 1) * 20 #Probabilidad según avance
        fecha_cierre = None #Si está abierta, aún no tiene fecha real de cierre



    venta = {
        'ID_Oportunidad': fake.unique.uuid4()[:8],
        'ID_Cliente': cliente_random['ID_Cliente'], #Foreign Key
        'Vendedor': random.choice(vendedores),
        'Producto': random.choice(['Maquinado', 'Terminado', 'Re-Trabajo','Placas']),
        'Monto': round(random.uniform(1000, 50000),2),
        'Etapa': etapa,
        'Probabilidad': probabilidad,
        'Fecha_Creacion_Oportunidad': fecha_creación,
        'Fecha_Cierre_Real': fecha_cierre 
    }
    
    data_ventas.append(venta)

df_ventas = pd.DataFrame(data_ventas)
print(df_ventas.info())

"""
-------------------------------------------------------------------------------------------
                    3. EXPORTAR A CSV (RAW DATA)
-------------------------------------------------------------------------------------------
"""

df_clientes.to_csv('crm_clientes_raw.csv', index=False)
df_ventas.to_csv('crm_ventas_raw.csv', index=False)

print("Archivos generados correctamente: ")
print(f"1. crm_clientes_raw.csv ({len(df_clientes)} registros)")
print(f"2. crm_ventas_raw.csv ({len(df_ventas)} registros)")
