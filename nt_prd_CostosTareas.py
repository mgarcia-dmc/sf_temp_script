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
JOB_NAME = "nt_prd_tbl_CostosTareas"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_date, current_timestamp,date_format,col
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
db_sf_fin_silver = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"

bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_source = f"ue1stg{ambiente}as3dtl005-silver"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"
#database_name = "default"

database_name_costos_tmp = db_sf_costo_tmp
database_name_costos_gl  = db_sf_costo_gl
database_name_costos_si  = db_sf_costo_si
#database_name_costos_br   = db_sf_costo_br

#database_name_tmp = db_sf_pec_tmp
#database_name_gl  = db_sf_pec_gl
database_name_si  = db_sf_pec_si

db_sap_fin_gl = db_sf_fin_gold
db_sap_fin_si = db_sf_fin_silver

table_name1 = "ft_sf_co_mtech_tarea13800"
table_name2 = "ft_sf_co_mtech_tarea30800"
table_name3 = "ft_sf_co_mtech_tarea22600"
table_name4 = "ft_sf_co_mtech_tarea6000"
table_name5 = "ft_sf_co_mtech_tarea6100"
table_name6 = "ft_sf_co_mtech_tarea6200"
table_name7 = "ft_sf_co_mtech_tarea6300"
table_name8 = "ft_sf_co_mtech_KardexInterfaceData"
table_name9 = "ft_sf_co_mtech_AsientosContables"
table_name10 ="ft_sf_co_mtech_tarea20900"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
file_name_target2 = f"{bucket_name_prdmtech}{table_name2}/"
file_name_target3 = f"{bucket_name_prdmtech}{table_name3}/"
file_name_target4 = f"{bucket_name_prdmtech}{table_name4}/"
file_name_target5 = f"{bucket_name_prdmtech}{table_name5}/"
file_name_target6 = f"{bucket_name_prdmtech}{table_name6}/"
file_name_target7 = f"{bucket_name_prdmtech}{table_name7}/"
file_name_target8 = f"{bucket_name_prdmtech}{table_name8}/"
file_name_target9 = f"{bucket_name_prdmtech}{table_name9}/"
file_name_target10 = f"{bucket_name_prdmtech}{table_name10}/"
path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
path_target3 = f"s3://{bucket_name_target}/{file_name_target3}"
path_target4 = f"s3://{bucket_name_target}/{file_name_target4}"
path_target5 = f"s3://{bucket_name_target}/{file_name_target5}"
path_target6 = f"s3://{bucket_name_target}/{file_name_target6}"
path_target7 = f"s3://{bucket_name_target}/{file_name_target7}"
path_target8 = f"s3://{bucket_name_target}/{file_name_target8}"
path_target9 = f"s3://{bucket_name_target}/{file_name_target9}"
path_target10 = f"s3://{bucket_name_target}/{file_name_target10}"
print('cargando ruta')
df_ft_sf_co_mtech_tarea13800 = spark.sql(f"""
select
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo as Lote 
,SystemLocationGroupNo as LocationGroupNo 
,SystemStageNo as Etapa 
,SystemCostObjectNo as ObjetoContable 
,SystemCostElementNo as ElementoContable 
,SystemElementUserNo as ElementoUsuario 
,AccountName 
,SourceCode as Origen 
,Description as Descripcion 
,RelativeAmount as MontoRelativo 
,RelativeUnits as UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName as DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName as ProductNombre 
,GrowoutNo as SubZonaNo 
,GrowoutName as SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP BRIM - Farm>Plant' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
""")
print('df_ft_sf_co_mtech_tarea13800', df_ft_sf_co_mtech_tarea13800.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name1} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name1}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea13800
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name1}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name1}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name1}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
            
    print(f"agrega registros nuevos a la tabla {table_name1} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name1} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name1}Temporal')
    print(f"Tabla {table_name1}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
    }
    df_ft_sf_co_mtech_tarea13800.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_ft_sf_co_mtech_tarea30800 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo as Lote 
,SystemLocationGroupNo as LocationGroupNo 
,SystemStageNo as Etapa 
,SystemCostObjectNo as ObjetoContable 
,SystemCostElementNo as ElementoContable 
,SystemElementUserNo as ElementoUsuario 
,AccountName 
,SourceCode as Origen 
,Description as Descripcion 
,RelativeAmount as MontoRelativo 
,RelativeUnits as UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName as DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName as ProductNombre 
,GrowoutNo as SubZonaNo 
,GrowoutName as SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP TIM - Farm>Plant' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
union all 
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP TIM - Farm>Farm' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
""")
print("carga df_ft_sf_co_mtech_tarea30800", df_ft_sf_co_mtech_tarea30800.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name2} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name2}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea30800
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name2}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name2}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name2}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
            
    print(f"agrega registros nuevos a la tabla {table_name2} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name2} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name2}Temporal')
    print(f"Tabla {table_name2}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
    }
    df_ft_sf_co_mtech_tarea30800.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
df_ft_sf_co_mtech_tarea22600 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP EPS - Egg Receivings' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
""")
print('carga df_ft_sf_co_mtech_tarea22600', df_ft_sf_co_mtech_tarea22600.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target3)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name3} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name3}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target3)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea22600
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name3}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name3}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name3}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
            
    print(f"agrega registros nuevos a la tabla {table_name3} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name3} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name3}Temporal')
    print(f"Tabla {table_name3}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
    }
    df_ft_sf_co_mtech_tarea22600.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
df_ft_sf_co_mtech_tarea6000 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvProteinJournalTrans 
where SourceCode IN ('EOP - P1>P1', 'EOP - P1>P2') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
AND CostCenterNo IN ('1409003M','1409003E')
""")
print('carga df_ft_sf_co_mtech_tarea6000',df_ft_sf_co_mtech_tarea6000.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target4)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name4} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name4}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target4)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea6000
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name4}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name4}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name4}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name4}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name4}")
            
    print(f"agrega registros nuevos a la tabla {table_name4} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name4} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name4}Temporal')
    print(f"Tabla {table_name4}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name4}"
    }
    df_ft_sf_co_mtech_tarea6000.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name4}")
df_ft_sf_co_mtech_tarea6100 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - GIM Farm>Farm') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
AND CostCenterNo IN ('1409003E')
union all 
select 
xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - GIM Farm>Plant') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
AND CostCenterNo IN ('1411402','1409003E') AND ProductNo = '49226'
""")
print('carga df_ft_sf_co_mtech_tarea6100', df_ft_sf_co_mtech_tarea6100.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target5)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name5} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name5}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target5)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea6100
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name5}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name5}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name5}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name5}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name5}")
            
    print(f"agrega registros nuevos a la tabla {table_name5} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name5} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name5}Temporal')
    print(f"Tabla {table_name5}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name5}"
    }
    df_ft_sf_co_mtech_tarea6100.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name5}")
df_ft_sf_co_mtech_tarea6200 = spark.sql(f"""
select 
xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - P1>P1') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
AND CostCenterNo IN ('1409007M','1409005M')
union all 
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - P1>P2') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
AND CostCenterNo IN ('1409005M','1409005E','1409007E','1409007M')
""")
print('carga df_ft_sf_co_mtech_tarea6200', df_ft_sf_co_mtech_tarea6200.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target6)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name6} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name6}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target6)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea6200
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name6}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name6}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name6}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name6}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name6}")
            
    print(f"agrega registros nuevos a la tabla {table_name6} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name6} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name6}Temporal')
    print(f"Tabla {table_name6}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name6}"
    }
    df_ft_sf_co_mtech_tarea6200.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name6}")
df_ft_sf_co_mtech_tarea6300 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - GIM Farm>Farm') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
AND CostCenterNo IN ('1409005E','1409007E')
union all
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - GIM Farm>Plant') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
AND CostCenterNo IN ('1411402','1409005E','1409007E') 
AND ProductNo = '0811'
""")
print('carga df_ft_sf_co_mtech_tarea6300', df_ft_sf_co_mtech_tarea6300.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target7)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name7} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name7}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target7)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea6300
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name7}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name7}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name7}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name7}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name7}")
            
    print(f"agrega registros nuevos a la tabla {table_name7} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name7} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name7}Temporal')
    print(f"Tabla {table_name7}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name7}"
    }
    df_ft_sf_co_mtech_tarea6300.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name7}")
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
year_str = int(fecha_str[:4]) 
month_str = int(fecha_str[4:])

df_ft_sf_co_mtech_KardexInterfaceData = spark.sql(f"""
select * 
from {db_sap_fin_si}.si_ZCstKardex
where FiscalYear = {year_str} AND xMonth = {month_str}
""")
print('carga df_ft_sf_co_mtech_KardexInterfaceData',df_ft_sf_co_mtech_KardexInterfaceData.count())
try:
    df_existentes = spark.read.format("parquet").load(path_target8)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name8} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name8}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target8)
    data_after_delete = existing_data.filter(~((col("FiscalYear") == year_str) & (col("xMonth") == month_str)))
    filtered_new_data = df_ft_sf_co_mtech_KardexInterfaceData
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name8}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name8}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name8}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name8}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name8}")
            
    print(f"agrega registros nuevos a la tabla {table_name8} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name8} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name8}Temporal')
    print(f"Tabla {table_name8}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name8}"
    }
    df_ft_sf_co_mtech_KardexInterfaceData.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name8}")
df_ft_sf_co_mtech_AsientosContables = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo 
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""") 
print('carga df_ft_sf_co_mtech_AsientosContables', df_ft_sf_co_mtech_AsientosContables.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target9)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name9} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name9}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target9)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_AsientosContables
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name9}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name9}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name9}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name9}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name9}")
            
    print(f"agrega registros nuevos a la tabla {table_name9} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name9} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name9}Temporal')
    print(f"Tabla {table_name9}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name9}"
    }
    df_ft_sf_co_mtech_AsientosContables.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name9}")
df_ft_sf_co_mtech_tarea20900 = spark.sql(f"""
select 
 xDate 
,CostCenterNo 
,FarmNo 
,EntityNo Lote 
,SystemLocationGroupNo LocationGroupNo 
,SystemStageNo Etapa 
,SystemCostObjectNo ObjetoContable 
,SystemCostElementNo ElementoContable 
,SystemElementUserNo ElementoUsuario 
,AccountName 
,SourceCode Origen 
,Description Descripcion 
,RelativeAmount MontoRelativo 
,RelativeUnits UnidadesRelativas 
,ComplexEntityNo 
,DivisionNo 
,DivisionName DivisionNombre 
,CompanyNo 
,ProductNo
,ProductName ProductNombre 
,GrowoutNo SubZonaNo 
,GrowoutName SubZonaNombre 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode IN ('EOP - LIM Capitalizations') and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga df_ft_sf_co_mtech_tarea20900', df_ft_sf_co_mtech_tarea20900.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target10)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name10} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name10}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target10)
    data_after_delete = existing_data.filter(~((date_format(col("xDate"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_sf_co_mtech_tarea20900
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name10}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name10}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name10}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name10}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name10}")
            
    print(f"agrega registros nuevos a la tabla {table_name10} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name10} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name10}Temporal')
    print(f"Tabla {table_name10}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name10}"
    }
    df_ft_sf_co_mtech_tarea20900.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name10}")
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()