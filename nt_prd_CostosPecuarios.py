# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_CostosPecuarios"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp,date_format,col
from dateutil.relativedelta import relativedelta
from datetime import datetime

print("inicia spark")
# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")
 
## Parametros Globales 
amb             = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
db_sf_costo_tmp = ssm.get_parameter(Name='p_db_prd_sf_costo_tmp', WithDecryption=True)['Parameter']['Value']
db_sf_costo_gl  = ssm.get_parameter(Name='p_db_prd_sf_costo_gl', WithDecryption=True)['Parameter']['Value']
db_sf_costo_si  = ssm.get_parameter(Name='p_db_prd_sf_costo_si', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_br  = ssm.get_parameter(Name='p_db_prd_sf_costo_br', WithDecryption=True)['Parameter']['Value']

#db_sf_pec_tmp = ssm.get_parameter(Name='p_db_prd_sf_pec_tmp', WithDecryption=True)['Parameter']['Value']
#db_sf_pec_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
db_sf_pec_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']

db_sf_fin_gold = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"

bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_source = f"ue1stg{ambiente}as3dtl005-silver"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"

database_name_costos_tmp = db_sf_costo_tmp
database_name_costos_gl  = db_sf_costo_gl
database_name_costos_si  = db_sf_costo_si
#database_name_costos_br   = db_sf_costo_br

#database_name_tmp = db_sf_pec_tmp
#database_name_gl  = db_sf_pec_gl
database_name_si  = db_sf_pec_si

file_name_target1 = f"{bucket_name_prdmtech}ft_sf_co_mtech_HuevoComercial/"
file_name_target2 = f"{bucket_name_prdmtech}ft_sf_co_mtech_Ponedoras/"

path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

#database_name = "default"
print('cargando ruta')
df_ft_sf_co_mtech_HuevoComercial=spark.sql(f"""
select 
 date_format(cast(xDate as timestamp),'yyyyMM') Mes
,xDate 
,DivisionName 
,CompanyNo 
,ProductNo 
,ProductName 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
,CostCenterNo 
,ComplexEntityNo 
,FarmNo 
,EntityNo 
,SystemLocationGroupNo 
,SystemStageNo 
,SystemCostObjectNo 
,SystemCostElementNo 
,SystemElementUserNo 
,SystemComplexAccountNo 
,AccountName 
,SourceCode 
,Description 
,RelativeAmount 
,RelativeUnits 
FROM {database_name_costos_si}.si_mvproteinjournaltrans 
where SystemLocationGroupNo='LAYER' and SystemStageNo='LAY' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
""")
print('carga df_ft_sf_co_mtech_HuevoComercial', df_ft_sf_co_mtech_HuevoComercial.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
table_name = 'ft_sf_co_mtech_HuevoComercial'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((col("Mes")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_HuevoComercial
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_sf_co_mtech_Ponedoras=spark.sql(f"""
select 
 date_format(cast(DateCap as timestamp),'yyyyMM') Mes
,DateCap 
,DivisionName 
,CompanyNo 
,ProductNo 
,ProductName 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
,CostCenterNo 
,ComplexEntityNo 
,FarmNo 
,EntityNo 
,SystemLocationGroupNo 
,SystemStageNo 
,SystemCostObjectNo 
,SystemCostElementNo 
,SystemElementUserNo 
,SystemComplexAccountNo 
,AccountName 
,SourceCode 
,Description 
,RelativeAmount 
,RelativeUnits 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
left join {database_name_si}.si_bimcapitalizationtrans B on A.proteinentitiesirn = B.proteinentitiesirn 
where date_format(cast(DateCap as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and SystemStageNo = 'BROOD' and SystemLocationGroupNo = 'LAYER'
""")
print('carga df_ft_sf_co_mtech_Ponedoras', df_ft_sf_co_mtech_Ponedoras.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
table_name = 'ft_sf_co_mtech_Ponedoras'
try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((col("Mes")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_Ponedoras
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()