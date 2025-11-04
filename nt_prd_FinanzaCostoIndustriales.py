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
JOB_NAME = "nt_prd_FinanzaCostoIndustriales"

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
#db_sf_costo_si  = ssm.get_parameter(Name='p_db_prd_sf_costo_si', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_br  = ssm.get_parameter(Name='p_db_prd_sf_costo_br', WithDecryption=True)['Parameter']['Value']

#db_sf_pec_tmp = ssm.get_parameter(Name='p_db_prd_sf_pec_tmp', WithDecryption=True)['Parameter']['Value']
#db_sf_pec_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
#db_sf_pec_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']

db_sf_fin_gold = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"

bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_source = f"ue1stg{ambiente}as3dtl005-silver"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"

database_name_costos_tmp = db_sf_costo_tmp
database_name_costos_gl  = db_sf_costo_gl
#database_name_costos_si  = db_sf_costo_si
#database_name_costos_br   = db_sf_costo_br

#database_name_tmp = db_sf_pec_tmp
#database_name_gl  = db_sf_pec_gl
#database_name_si  = db_sf_pec_si

table_name1 = "ft_SAP_Pollo"
table_name2 = "ft_SAP_Pavo"
table_name3 = "ft_SAP_Procesados"
table_name4 = "ft_SAP_Cerdo"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
file_name_target2 = f"{bucket_name_prdmtech}{table_name2}/"
file_name_target3 = f"{bucket_name_prdmtech}{table_name3}/"
file_name_target4 = f"{bucket_name_prdmtech}{table_name4}/"
path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
path_target3 = f"s3://{bucket_name_target}/{file_name_target3}"
path_target4 = f"s3://{bucket_name_target}/{file_name_target4}"
#database_name = "default"
db_sap_fin_gl = db_sf_fin_gold
print('cargando ruta')
df_ft_SAP_Pollo = spark.sql(f"""
WITH SapPolloAgg AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,ARBPL PuestoTrabajo,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATGEN = 'RENDIMIENTO' AND (JER_N1_T = 'POLLO' OR JER_N1_T IS NULL)
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2)),ARBPL
)
select 
 concat(GJAHR,SUBSTR(PERIO, -2)) AS Mes
,WERKS CentroOrden
,ARBPL Recurso
,GJAHR Anho
,SUBSTR(PERIO, -2) Periodo
,CATEG Categoria
,CATEG_TXT DescripcionCategoria
,KSTAR ClasedeCoste1
,KSTAR DescripcionClaseCoste1
,CANT CantidadTotal
,MEINH UnidadMedida
,VAL_MON ValorTotal
,VAL_REC ValorRecalculado1
,WAERS Moneda
,ORIGEN Origen
,ORIGEN_TXT DescripcionOrigen
,MATNR Material1
,COS_PIP_01 CostoPIP1
,VAL_REC_02 ValorRecalculado2
,COS_PIP_02 CostoPIP2
,V_S VS
,AUFNR Orden
,ARBPL PuestoTrabajo
,MACAOR MaterialCabeceraOrden
,MACAOR_TXT DescripcionMaterialCabeceraOrden
,CANT_KG CantidadKG
,MTART TipoMaterial
,'' MaterialLargo
,'' OrdenLargo
,CATGEN DescripcionCategoriaGeneral
,JER_N1 JerarqNivel1
,JER_N2 JerarqNivel2
,JER_N3 JerarqNivel3
,JER_N4 JerarqNivel4
,JER_N1_T DescripcionJerarquiaNivel1
,JER_N2_T DescripcionJerarquiaNivel2
,JER_N3_T DescripcionJerarquiaNivel3
,JER_N4_T DescripcionJerarquiaNivel4
,COALESCE(SapAgg.SumCantidadKg, 0) CantidadKgXRecurso
,CONCAT('C',DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(concat(GJAHR,SUBSTR(PERIO, -2),'01'), 'yyyyMMdd'),1)),'MM')) Ciclo
,'SAN FERNANDO' Empresa
from {db_sap_fin_gl}.costos_reales_produccion M
LEFT JOIN SapPolloAgg SapAgg ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg.Mes AND M.ARBPL = SapAgg.PuestoTrabajo
where concat(GJAHR,SUBSTR(PERIO, -2))  = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and WERKS ='5002' and BUKRS='SFER'
""")
print('carga df_ft_SAP_Pollo', df_ft_SAP_Pollo.count())
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
    data_after_delete = existing_data.filter(~((date_format(col("Mes"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_SAP_Pollo
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
    df_ft_SAP_Pollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_MesCicloSap_pollo = spark.sql(f"""
select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_SAP_Pollo 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloSap_pollo"
}
df_MesCicloSap_pollo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloSap_pollo")
print('carga tabla temporal MesCicloSap_pollo', df_MesCicloSap_pollo.count())
df_ft_sap_polloTemp = spark.sql(f"""
select A.* 
from {database_name_costos_gl}.ft_SAP_Pollo A 
left join {database_name_costos_tmp}.MesCicloSap_pollo B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and Empresa = 'SAN FERNANDO'
order by 1
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_sap_polloTemp"
}
df_ft_sap_polloTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_sap_polloTemp")
print('carga tabla temporal ft_sap_polloTemp', df_ft_sap_polloTemp.count())
df_ft_SAP_PolloINS =spark.sql(f"""select 
 B.Mes 
,CentroOrden
,Recurso
,Anho
,Periodo
,Categoria
,DescripcionCategoria
,ClasedeCoste1
,DescripcionClaseCoste1
,CantidadTotal
,UnidadMedida
,ValorTotal
,ValorRecalculado1
,Moneda
,Origen
,DescripcionOrigen
,Material1
,CostoPIP1
,ValorRecalculado2
,CostoPIP2
,VS
,Orden
,PuestoTrabajo
,MaterialCabeceraOrden
,DescripcionMaterialCabeceraOrden
,CantidadKG
,TipoMaterial
,MaterialLargo
,OrdenLargo
,DescripcionCategoriaGeneral
,JerarqNivel1
,JerarqNivel2
,JerarqNivel3
,JerarqNivel4
,DescripcionJerarquiaNivel1
,DescripcionJerarquiaNivel2
,DescripcionJerarquiaNivel3
,DescripcionJerarquiaNivel4
,CantidadKgXRecurso
,A.Ciclo
,B.Empresa
from {database_name_costos_tmp}.MesCicloSap_pollo A 
left join {database_name_costos_tmp}.ft_sap_polloTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null and Empresa = 'SAN FERNANDO'
except 
select * from {database_name_costos_gl}.ft_SAP_Pollo 
where Empresa = 'SAN FERNANDO'
""")
print('carga temporal df_ft_SAP_PolloINS', df_ft_SAP_PolloINS.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
}
df_ft_SAP_PolloINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
print('carga tabla INS ft_SAP_Pollo', df_ft_SAP_PolloINS.count())
df_ft_SAP_Pavo = spark.sql(f"""
WITH SapPavoAgg AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,JER_N3_T DescripcionJerarquiaNivel3,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATGEN = 'RENDIMIENTO' AND JER_N1_T = 'PAVO'
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2)),JER_N3_T
)
select 
 concat(GJAHR,SUBSTR(PERIO, -2)) AS Mes
,WERKS CentroOrden
,ARBPL Recurso
,GJAHR Anho
,SUBSTR(PERIO, -2) Periodo
,CATEG Categoria
,CATEG_TXT DescripcionCategoria
,KSTAR ClasedeCoste1
,KSTAR DescripcionClaseCoste1
,CANT CantidadTotal
,MEINH UnidadMedida
,VAL_MON ValorTotal
,VAL_REC ValorRecalculado1
,WAERS Moneda
,ORIGEN Origen
,ORIGEN_TXT DescripcionOrigen
,MATNR Material1
,COS_PIP_01 CostoPIP1
,VAL_REC_02 ValorRecalculado2
,COS_PIP_02 CostoPIP2
,V_S VS
,AUFNR Orden
,ARBPL PuestoTrabajo
,MACAOR MaterialCabeceraOrden
,MACAOR_TXT DescripcionMaterialCabeceraOrden
,CANT_KG CantidadKG
,MTART TipoMaterial
,'' MaterialLargo
,'' OrdenLargo
,CATGEN DescripcionCategoriaGeneral
,JER_N1 JerarqNivel1
,JER_N2 JerarqNivel2
,JER_N3 JerarqNivel3
,JER_N4 JerarqNivel4
,JER_N1_T DescripcionJerarquiaNivel1
,JER_N2_T DescripcionJerarquiaNivel2
,JER_N3_T DescripcionJerarquiaNivel3
,JER_N4_T DescripcionJerarquiaNivel4
,COALESCE(SapAgg.SumCantidadKg, 0) CantidadKgXRecurso
,CONCAT('C',DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(concat(GJAHR,SUBSTR(PERIO, -2),'01'), 'yyyyMMdd'),1)),'MM')) Ciclo
,'SAN FERNANDO' Empresa
from {db_sap_fin_gl}.costos_reales_produccion M
LEFT JOIN SapPavoAgg SapAgg ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg.Mes AND M.JER_N3_T = SapAgg.DescripcionJerarquiaNivel3
where concat(GJAHR,SUBSTR(PERIO, -2))  = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and WERKS ='5006' and BUKRS='SFER'
""")
print('carga df_ft_SAP_Pavo', df_ft_SAP_Pavo.count())
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
    data_after_delete = existing_data.filter(~((date_format(col("Mes"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_SAP_Pavo
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
    df_ft_SAP_Pavo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
df_MesCicloSap_pavo = spark.sql(f"""
select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_SAP_Pavo
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloSap_pavo"
}
df_MesCicloSap_pavo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloSap_pavo")
print('carga tabla temporal MesCicloSap_pavo', df_MesCicloSap_pavo.count())
df_ft_sap_pavoTemp = spark.sql(f"""
select A.* 
from {database_name_costos_gl}.ft_SAP_Pavo A 
left join {database_name_costos_tmp}.MesCicloSap_pavo B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and Empresa = 'SAN FERNANDO'
order by 1
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_sap_pavoTemp"
}
df_ft_sap_pavoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_sap_pavoTemp")
print('carga tabla temporal ft_sap_pavoTemp', df_ft_sap_pavoTemp.count())
df_ft_SAP_PavoINS =spark.sql(f"""select 
 B.Mes 
,CentroOrden
,Recurso
,Anho
,Periodo
,Categoria
,DescripcionCategoria
,ClasedeCoste1
,DescripcionClaseCoste1
,CantidadTotal
,UnidadMedida
,ValorTotal
,ValorRecalculado1
,Moneda
,Origen
,DescripcionOrigen
,Material1
,CostoPIP1
,ValorRecalculado2
,CostoPIP2
,VS
,Orden
,PuestoTrabajo
,MaterialCabeceraOrden
,DescripcionMaterialCabeceraOrden
,CantidadKG
,TipoMaterial
,MaterialLargo
,OrdenLargo
,DescripcionCategoriaGeneral
,JerarqNivel1
,JerarqNivel2
,JerarqNivel3
,JerarqNivel4
,DescripcionJerarquiaNivel1
,DescripcionJerarquiaNivel2
,DescripcionJerarquiaNivel3
,DescripcionJerarquiaNivel4
,CantidadKgXRecurso
,A.Ciclo
,B.Empresa
from {database_name_costos_tmp}.MesCicloSap_pavo A 
left join {database_name_costos_tmp}.ft_sap_pavoTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null and Empresa = 'SAN FERNANDO'
except 
select * from {database_name_costos_gl}.ft_SAP_Pavo
where Empresa = 'SAN FERNANDO'
""")
print('carga temporal ft_SAP_PavoINS', df_ft_SAP_PavoINS.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
}
df_ft_SAP_PavoINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
print('carga tabla INS ft_SAP_Pavo', df_ft_SAP_PavoINS.count())
df_ft_SAP_Procesados = spark.sql(f"""
WITH SapProcesadosAgg AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,JER_N3_T DescripcionJerarquiaNivel3,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATEG_TXT = 'ENTREGAS' AND MTART = 'FERT'
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2)),JER_N3_T
)
select 
 concat(GJAHR,SUBSTR(PERIO, -2)) AS Mes
,WERKS CentroOrden
,ARBPL Recurso
,GJAHR Anho
,SUBSTR(PERIO, -2) Periodo
,CATEG Categoria
,CATEG_TXT DescripcionCategoria
,KSTAR ClasedeCoste1
,KSTAR DescripcionClaseCoste1
,CANT CantidadTotal
,MEINH UnidadMedida
,VAL_MON ValorTotal
,VAL_REC ValorRecalculado1
,WAERS Moneda
,ORIGEN Origen
,ORIGEN_TXT DescripcionOrigen
,MATNR Material1
,COS_PIP_01 CostoPIP1
,VAL_REC_02 ValorRecalculado2
,COS_PIP_02 CostoPIP2
,V_S VS
,AUFNR Orden
,ARBPL PuestoTrabajo
,MACAOR MaterialCabeceraOrden
,MACAOR_TXT DescripcionMaterialCabeceraOrden
,CANT_KG CantidadKG
,MTART TipoMaterial
,'' MaterialLargo
,'' OrdenLargo
,CATGEN DescripcionCategoriaGeneral
,JER_N1 JerarqNivel1
,JER_N2 JerarqNivel2
,JER_N3 JerarqNivel3
,JER_N4 JerarqNivel4
,JER_N1_T DescripcionJerarquiaNivel1
,JER_N2_T DescripcionJerarquiaNivel2
,JER_N3_T DescripcionJerarquiaNivel3
,JER_N4_T DescripcionJerarquiaNivel4
,COALESCE(SapAgg.SumCantidadKg, 0) CantidadKgXRecurso
,CONCAT('C',DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(concat(GJAHR,SUBSTR(PERIO, -2),'01'), 'yyyyMMdd'),1)),'MM')) Ciclo
,'SAN FERNANDO' Empresa
from {db_sap_fin_gl}.costos_reales_produccion M
LEFT JOIN SapProcesadosAgg SapAgg ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg.Mes AND M.JER_N3_T = SapAgg.DescripcionJerarquiaNivel3
where concat(GJAHR,SUBSTR(PERIO, -2)) = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and WERKS in ('5007','5008') and BUKRS='SFER'
""")
print('carga ft_SAP_Procesados', df_ft_SAP_Procesados.count())
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
    data_after_delete = existing_data.filter(~((date_format(col("Mes"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_SAP_Procesados
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
    df_ft_SAP_Procesados.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
df_MesCicloSap_Procesados = spark.sql(f"""
select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_SAP_Procesados
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloSap_Procesados"
}
df_MesCicloSap_Procesados.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloSap_Procesados")
print('carga tabla temporal MesCicloSap_Procesados', df_MesCicloSap_Procesados.count())
df_ft_sap_ProcesadosTemp = spark.sql(f"""
select A.* 
from {database_name_costos_gl}.ft_SAP_Procesados A 
left join {database_name_costos_tmp}.MesCicloSap_Procesados B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and Empresa = 'SAN FERNANDO'
order by 1
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_sap_ProcesadosTemp"
}
df_ft_sap_ProcesadosTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_sap_ProcesadosTemp")
print('carga tabla temporal ft_sap_ProcesadosTemp', df_ft_sap_ProcesadosTemp.count())
df_ft_SAP_ProcesadosINS =spark.sql(f"""select 
 B.Mes 
,CentroOrden
,Recurso
,Anho
,Periodo
,Categoria
,DescripcionCategoria
,ClasedeCoste1
,DescripcionClaseCoste1
,CantidadTotal
,UnidadMedida
,ValorTotal
,ValorRecalculado1
,Moneda
,Origen
,DescripcionOrigen
,Material1
,CostoPIP1
,ValorRecalculado2
,CostoPIP2
,VS
,Orden
,PuestoTrabajo
,MaterialCabeceraOrden
,DescripcionMaterialCabeceraOrden
,CantidadKG
,TipoMaterial
,MaterialLargo
,OrdenLargo
,DescripcionCategoriaGeneral
,JerarqNivel1
,JerarqNivel2
,JerarqNivel3
,JerarqNivel4
,DescripcionJerarquiaNivel1
,DescripcionJerarquiaNivel2
,DescripcionJerarquiaNivel3
,DescripcionJerarquiaNivel4
,CantidadKgXRecurso
,A.Ciclo
,B.Empresa
from {database_name_costos_tmp}.MesCicloSap_Procesados A 
left join {database_name_costos_tmp}.ft_sap_ProcesadosTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null and Empresa = 'SAN FERNANDO'
except 
select * from {database_name_costos_gl}.ft_SAP_Procesados
where Empresa = 'SAN FERNANDO'
""")
print('carga df_ft_SAP_ProcesadosINS', df_ft_SAP_ProcesadosINS.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
}
df_ft_SAP_ProcesadosINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
print('carga tabla INS ft_SAP_Procesados', df_ft_SAP_ProcesadosINS.count())
df_ft_SAP_Cerdo = spark.sql(f"""
WITH SapCerdoAgg AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,ARBPL PuestoTrabajo,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATGEN = 'RENDIMIENTO'
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2)),ARBPL
)
,SapCerdoAgg2 AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATGEN = 'RENDIMIENTO' and ARBPL = 'cer_ben'
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2))
)
,SapCerdoAgg3 AS (
    SELECT concat(GJAHR,SUBSTR(PERIO, -2)) Mes,COALESCE(SUM(CANT_KG), 0) AS SumCantidadKg
    FROM {db_sap_fin_gl}.costos_reales_produccion
    WHERE CATGEN = 'RENDIMIENTO' and ARBPL in ('cer_aca', 'cer_cco', 'cer_con', 'cer_cor', 'cer_des', 'cer_emp')
    GROUP BY concat(GJAHR,SUBSTR(PERIO, -2))
)
select 
 concat(GJAHR,SUBSTR(PERIO, -2)) AS Mes
,WERKS CentroOrden
,ARBPL Recurso
,GJAHR Anho
,SUBSTR(PERIO, -2) Periodo
,CATEG Categoria
,CATEG_TXT DescripcionCategoria
,KSTAR ClasedeCoste1
,KSTAR DescripcionClaseCoste1
,CANT CantidadTotal
,MEINH UnidadMedida
,VAL_MON ValorTotal
,VAL_REC ValorRecalculado1
,WAERS Moneda
,ORIGEN Origen
,ORIGEN_TXT DescripcionOrigen
,MATNR Material1
,COS_PIP_01 CostoPIP1
,VAL_REC_02 ValorRecalculado2
,COS_PIP_02 CostoPIP2
,V_S VS
,AUFNR Orden
,ARBPL PuestoTrabajo
,MACAOR MaterialCabeceraOrden
,MACAOR_TXT DescripcionMaterialCabeceraOrden
,CANT_KG CantidadKG
,MTART TipoMaterial
,'' MaterialLargo
,'' OrdenLargo
,CATGEN DescripcionCategoriaGeneral
,JER_N1 JerarqNivel1
,JER_N2 JerarqNivel2
,JER_N3 JerarqNivel3
,JER_N4 JerarqNivel4
,JER_N1_T DescripcionJerarquiaNivel1
,JER_N2_T DescripcionJerarquiaNivel2
,JER_N3_T DescripcionJerarquiaNivel3
,JER_N4_T DescripcionJerarquiaNivel4
,COALESCE(SapAgg.SumCantidadKg, 0) CantidadKgXRecurso
,case when PuestoTrabajo = 'cer_ben' then COALESCE(SapAgg2.SumCantidadKg, 0) when PuestoTrabajo in ('cer_aca', 'cer_cco', 'cer_con', 'cer_cor', 'cer_des', 'cer_emp') then COALESCE(SapAgg3.SumCantidadKg, 0) end CantidadKgXCategoria
,CONCAT('C',DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(concat(GJAHR,SUBSTR(PERIO, -2),'01'), 'yyyyMMdd'),1)),'MM')) Ciclo
,'SAN FERNANDO' Empresa
from {db_sap_fin_gl}.costos_reales_produccion M
LEFT JOIN SapCerdoAgg SapAgg ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg.Mes AND M.ARBPL = SapAgg.PuestoTrabajo
LEFT JOIN SapCerdoAgg2 SapAgg2 ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg2.Mes
LEFT JOIN SapCerdoAgg3 SapAgg3 ON CONCAT(M.GJAHR, SUBSTR(M.PERIO, -2)) = SapAgg3.Mes
where concat(GJAHR,SUBSTR(PERIO, -2))  = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and WERKS ='5028' and BUKRS='SFER'
""")
print('carga df_ft_SAP_Cerdo', df_ft_SAP_Cerdo.count())
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
    data_after_delete = existing_data.filter(~((date_format(col("Mes"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_SAP_Cerdo
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
    df_ft_SAP_Cerdo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name4}")
df_MesCicloSap_Cerdo = spark.sql(f"""
select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_SAP_Cerdo
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloSap_cerdo"
}
df_MesCicloSap_Cerdo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloSap_cerdo")
print('carga tabla temporal MesCicloSap_cerdo', df_MesCicloSap_Cerdo.count())
df_ft_sap_cerdoTemp = spark.sql(f"""
select A.* 
from {database_name_costos_gl}.ft_SAP_Cerdo A 
left join {database_name_costos_tmp}.MesCicloSap_cerdo B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and Empresa = 'SAN FERNANDO'
order by 1
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_sap_cerdoTemp"
}
df_ft_sap_cerdoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_sap_cerdoTemp")
print('carga tabla temporal ft_sap_cerdoTemp', df_ft_sap_cerdoTemp.count())
df_ft_SAP_CerdoINS =spark.sql(f"""select 
 B.Mes 
,CentroOrden
,Recurso
,Anho
,Periodo
,Categoria
,DescripcionCategoria
,ClasedeCoste1
,DescripcionClaseCoste1
,CantidadTotal
,UnidadMedida
,ValorTotal
,ValorRecalculado1
,Moneda
,Origen
,DescripcionOrigen
,Material1
,CostoPIP1
,ValorRecalculado2
,CostoPIP2
,VS
,Orden
,PuestoTrabajo
,MaterialCabeceraOrden
,DescripcionMaterialCabeceraOrden
,CantidadKG
,TipoMaterial
,MaterialLargo
,OrdenLargo
,DescripcionCategoriaGeneral
,JerarqNivel1
,JerarqNivel2
,JerarqNivel3
,JerarqNivel4
,DescripcionJerarquiaNivel1
,DescripcionJerarquiaNivel2
,DescripcionJerarquiaNivel3
,DescripcionJerarquiaNivel4
,CantidadKgXRecurso
,CantidadKgXCategoria
,A.Ciclo
,B.Empresa
from {database_name_costos_tmp}.MesCicloSap_cerdo A 
left join {database_name_costos_tmp}.ft_sap_cerdoTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null and Empresa = 'SAN FERNANDO'
except 
select * from {database_name_costos_gl}.ft_SAP_Cerdo
where Empresa = 'SAN FERNANDO'
""")
print('carga temporal df_ft_SAP_CerdoINS', df_ft_SAP_CerdoINS.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name4}"
}
df_ft_SAP_CerdoINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name4}")
print('carga tabla INS ft_SAP_Cerdo', df_ft_SAP_CerdoINS.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()