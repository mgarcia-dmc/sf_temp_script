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
JOB_NAME = "nt_prd_AdicionaGasto"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *

print("inicia spark")
# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")
 
## Parametros Globales 
amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
db_sf_costo_tmp = ssm.get_parameter(Name='p_db_prd_sf_costo_tmp', WithDecryption=True)['Parameter']['Value']
db_sf_costo_gl  = ssm.get_parameter(Name='p_db_prd_sf_costo_gl', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_si  = ssm.get_parameter(Name='p_db_prd_sf_costo_si', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_br  = ssm.get_parameter(Name='p_db_prd_sf_costo_br', WithDecryption=True)['Parameter']['Value']

db_sf_fin_gold   = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
db_sf_fin_silver = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']

db_sf_pec_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"
bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"
bucket_name_prdmtech2 = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/MAESTROS_COSTOS/"

database_name_costos_tmp = f"{db_sf_costo_tmp}"
database_name_costos_gl  = f"{db_sf_costo_gl}"
#database_name_costos_si  = f"{db_sf_costo_si}"
#database_name_costos_br  = f"{db_sf_costo_br}"

db_sap_fin_gl = db_sf_fin_gold
db_sap_fin_si = db_sf_fin_silver

database_name_si  = db_sf_pec_si
#database_name = "default"

#por prueba de qas 
aniomes = '202412'
print('cargando ruta')
## KE24_Vinculo
df_KE24_Vinculo= spark.sql(f"""
select DISTINCT a.BZIRK as ZONAVTA, '' DESCZONADEVENTA, '' CODZONA, '' DESCZONA, '' CODDPTO, '' DESCRIPDPTO, '' CODNEGOC, '' DESCRIPCIONNEGOC, B.ort01 PROVINCIA
from {db_sap_fin_si}.si_knvv a 
--from database_name_si.si_knvv a 
--inner join database_name_si.si_kna1 b on a.MANDT = b.MANDT and a.KUNNR = b.KUNNR
inner join {db_sap_fin_si}.si_kna1 b on a.MANDT = b.MANDT and a.KUNNR = b.KUNNR
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech2}KE24_Vinculo"
}
df_KE24_Vinculo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24_Vinculo")
print('carga df_KE24_Vinculo',df_KE24_Vinculo.count())
## KE24_Vinculo01
df_KE24_Vinculo01= spark.sql(f"""
select DISTINCT PRCTR CEBE, MCTXT DESCEBE, '' JERAQ, CASE WHEN MCTXT NOT LIKE '%POLLO%VIVO%' AND MCTXT LIKE '%POLLO%' THEN 'POLLO BENEFICIADO' WHEN MCTXT LIKE '%POLLO%VIVO%'THEN 'POLLO VIVO' ELSE '' END DESCPJERARQUIA 
--from database_name_si.si_M_PRCTN
from {db_sap_fin_si}.si_M_PRCTN
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech2}KE24_Vinculo01"
}
df_KE24_Vinculo01.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24_Vinculo01")
print('carga df_KE24_Vinculo01',df_KE24_Vinculo01.count())
df_KE24 = spark.sql(f"""
SELECT
 WAERK Mon
,year(CAST(FKDAT AS timestamp)) Periodo
,month(CAST(FKDAT AS timestamp)) Per
,PRCTR CeBe
,MATNR Articulo
,WERKS Ce
,VBELN Ndocref
,FKSTO Docanul
,FKDAT Fefactura
,FKDAT Fecontab
,ERDAT Creadoel
,'' Pedclte
,POSNR NposR
,'' Cloper
,CASE WHEN FKART = 'ZCD' THEN NETWR*-1 ELSE NETWR END  IngresoVta
,0 Ingresos
,0 Otrosingr
,0 CVtasreal
,WAVWR CVtasRefer
,0 OtrosDesc
,0 Descuentos
,0 AjCVOtro
,0 Otrosegre
,0 AjusteMTECH
,0 CstVtas
,CASE WHEN FKART = 'ZCD' THEN FKLMG * -1 ELSE FKLMG END CantFactUB
,MEINS UMB
,CASE WHEN FKART = 'ZCD' THEN NTGEW * -1 ELSE NTGEW END PesoFactUM
,'' UMB2
,'' MONEX
,0  CANTFactUM
,'' UMB3
,0  CantEntCWM
,0  MermaCant
,'' Ndocum
,'' Creadopor
,'SFER' Soc
,'' ObjetoPA
,'' Operref
,FKART ClFac
,VKBUR OfVta
,'' Ramo
,BZIRK ZV
,VKORG OrgVt
,VTWEG CDis
,SPART Se
,'' Vendedor
,KUNAG Cliente
,VKGRP GVen
,'' LLAVEOV_CD
,'' DEPARTAMENTO
,'' DECCEBE
,PLTYP Lprecio
,KDGRP GrClientePedido
,PRODH JerarquiaProducto
,MATKL GRupoArticulo
,VBTYP TipoDocComercial
,FKSTO Anulada
,ARKTX Denominacion
,0 PrecioBase
,'' EsContabilizado
,0 year
,0 Precio_interno_periodico
,0 CvtasRefer_real
,0 MesCostoR
,0 AnioCostoR
FROM {db_sap_fin_gl}.FACTURACION
WHERE PRCTR <> '0001501102'
and date_format(TRY_CAST(FKDAT AS date),'yyyyMM') = {aniomes}
--and TRY_CAST(FKDAT AS date) = (current_date-1)
""")
print("carga KE24 --> Registros procesados:", df_KE24.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}KE24"
}
df_KE24.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24")
print("carga KE24 --> Registros procesados:", df_KE24.count())
#table_name = 'KE24'
#fecha_menos = datetime.today()+timedelta(days=-1)
#fecha_str = fecha_menos.strftime("%Y-%m-%d")
#file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
#path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"
#
#try:
#    df_existentes = spark.read.format("parquet").load(path_target31)
#    datos_existentes = True
#    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
#except:
#    datos_existentes = False
#    logger.info(f"No se encontraron datos existentes en {table_name}")
#
#if datos_existentes:
#    existing_data = spark.read.format("parquet").load(path_target31)
#    data_after_delete = existing_data.filter(~((date_format(col("Fefactura"),"yyyy-MM-dd") >= fecha_str)))
#    filtered_new_data = df_KE24.filter((date_format(col("Fefactura"),"yyyy-MM-dd") >= fecha_str))
#    final_data = filtered_new_data.union(data_after_delete)                             
#   
#    cant_ingresonuevo = filtered_new_data.count()
#    cant_total = final_data.count()
#    
#    # Escribir los resultados en ruta temporal
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
#    }
#    final_data.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}Temporal")
#
#    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    final_data2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
#            
#    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
#    print(f"Total de registros en la tabla {table_name} : {cant_total}")
#     #Limpia la ubicación temporal
#    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
#    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
#    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
#else:
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    df_KE24.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
#actualizacion
df_UPD_KE24 = spark.sql(f"""SELECT 
Mon 
,Periodo 
,Per 
,a.CeBe 
,Articulo 
,Ce 
,Ndocref 
,Docanul 
,Fefactura 
,Fecontab 
,Creadoel 
,Pedclte 
,NposR 
,Cloper 
,IngresoVta 
,Ingresos 
,Otrosingr 
,CVtasreal 
,CVtasRefer 
,OtrosDesc 
,Descuentos 
,AjCVOtro 
,Otrosegre 
,AjusteMTECH 
,CstVtas 
,CantFactUB 
,UMB 
,PesoFactUM 
,UMB2 
,MONEX 
,CANTFactUM 
,UMB3 
,CantEntCWM 
,MermaCant 
,Ndocum 
,Creadopor 
,Soc 
,ObjetoPA 
,Operref 
,ClFac 
,OfVta 
,Ramo 
,ZV 
,CASE WHEN a.cebe = b.cebe THEN '1002' ELSE a.OrgVt END OrgVt 
,CDis 
,Se 
,Vendedor 
,Cliente 
,GVen 
,LLAVEOV_CD 
,DEPARTAMENTO 
,DECCEBE 
,Lprecio 
,GrClientePedido 
,JerarquiaProducto 
,GRupoArticulo 
,TipoDocComercial 
,Anulada 
,Denominacion 
,PrecioBase 
,EsContabilizado 
,year 
,Precio_interno_periodico 
,CvtasRefer_real 
,MesCostoR 
,AnioCostoR 
from {database_name_costos_gl}.KE24 a left join (SELECT cebe FROM  {database_name_costos_gl}.KE24_Vinculo01 WHERE DESCPJERARQUIA = 'POLLO BENEFICIADO') b on a.cebe = b.cebe""")
print("carga df_UPD_KE24 --> Registros procesados:", df_UPD_KE24.count())
table_name = 'KE24'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_UPD_KE24                         
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
            
    #print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_KE24_Gasto = spark.sql(f"""SELECT 
 VKORG Org_Venta
,B.PROVINCIA 
,SUM(VVVEN) gastoC 
,SUM(VVTRT) gastoL 
,SUM(VVADM) gastoA 
,SUM(VVVEN + VVTRT + VVADM) gasto 
,SUM(VVCEW - VV936) kilos 
,SUM((VVVEN + VVTRT + VVADM)) / SUM(NULLIF((VVCEW - VV936),0)) gunit 
,GJAHR Ejercicio 
,substring(perde,2,2) Per 
FROM {db_sap_fin_gl}.copa_real A 
LEFT JOIN {database_name_costos_gl}.KE24_Vinculo B ON A.BZIRK = B.ZONAVTA 
WHERE PRCTR LIKE '000150%' 
AND VKORG IN ('1001','1002') 
AND B.PROVINCIA IS NOT NULL 
--AND CONCAT(GJAHR,substring(perde,2,2)) = date_format(add_months(trunc(current_date, 'month'),-1),'yyyyMM')
AND CONCAT(GJAHR,substring(perde,2,2)) = {aniomes}
GROUP BY VKORG,B.PROVINCIA,GJAHR,PERDE""")
print("carga KE24_Gasto --> Registros procesados:", df_KE24_Gasto.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}KE24_Gasto"
}
df_KE24_Gasto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24_Gasto")
print("carga KE24_Gasto --> Registros procesados:", df_KE24_Gasto.count())
#table_name = 'KE24_Gasto'
#fecha_menos = datetime.today()+ relativedelta(months=1)
#fecha_str = fecha_menos.strftime("%Y%m")
#file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
#path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"
#
#try:
#    df_existentes = spark.read.format("parquet").load(path_target31)
#    datos_existentes = True
#    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
#except:
#    datos_existentes = False
#    logger.info(f"No se encontraron datos existentes en {table_name}")
#
#if datos_existentes:
#    existing_data = spark.read.format("parquet").load(path_target31)
#    data_after_delete = existing_data.filter(~(concat(col("Ejercicio"),col("Per")) >= fecha_str))
#    filtered_new_data = df_KE24_Gasto.filter(concat(col("Ejercicio"),col("Per")) >= fecha_str)
#    final_data = filtered_new_data.union(data_after_delete)                             
#   
#    cant_ingresonuevo = filtered_new_data.count()
#    cant_total = final_data.count()
#    
#    # Escribir los resultados en ruta temporal
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
#    }
#    final_data.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}Temporal")
#
#    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    final_data2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
#            
#    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
#    print(f"Total de registros en la tabla {table_name} : {cant_total}")
#     #Limpia la ubicación temporal
#    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
#    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
#    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
#else:
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    df_KE24_Gasto.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
df_total01 =spark.sql(f"""
WITH SumaTablaCopa
(SELECT GJAHR Ejercicio,substring(PERDE,2,2) Per, ARTNR, SUM(VVPFN) VVPFN FROM {db_sap_fin_gl}.copa_real GROUP BY GJAHR,PERDE,ARTNR)
SELECT 
 A.ARTNR Articulo 
,GJAHR ejercicio 
,substring(PERDE,2,2) Per 
,'' ce --,WERKS ce -- este campo no se tiene en copa 
,'P' controlprecios 
,CASE WHEN ((VVCVR + VVDOT + RABAT+ VV991 + VVOEG + KWSOHD + VVCVM)) > 0 THEN (VVCVR + VVDOT + RABAT+ VV991 + VVOEG + KWSOHD + VVCVM) ELSE 0 END PrecioInterno 
,VVCVE/NULLIF((VVCEW-VV936),0) preciostandar 
,A.VVPFN cantidadcw 
,MT.VVPFN cantidadfabub 
FROM {db_sap_fin_gl}.copa_real A 
LEFT JOIN {database_name_gl}.KE24_Vinculo B ON A.BZIRK = B.ZONAVTA 
LEFT JOIN SumaTablaCopa MT ON MT.Ejercicio =A.GJAHR  AND MT.Per = substring(PERDE,2,2) and MT.ARTNR = A.ARTNR 
WHERE PRCTR LIKE '000150%' 
AND VKORG IN ('1001','1002') 
--AND WERKS IS NOT NULL -- este campo no se tiene en copa 
--AND CONCAT(GJAHR,substring(PERDE,2,2)) = date_format(add_months(trunc(current_date, 'month'),-1),'yyyyMM')
AND CONCAT(GJAHR,substring(PERDE,2,2)) = {aniomes}
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Total01"
}
df_total01.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Total01")
print('carga temporal Total01', df_total01.count())
#INSERT INTO [STGCOSTOS].[KE24_CostosVenta]
df_KE24_CostosVenta = spark.sql(f"""select 
 articulo 
,ejercicio 
,per 
,ce 
,controlprecios 
,sum(PrecioInterno)/nullif(sum(cantidadcw),0) Precio_interno_periodico
,sum(preciostandar) Precio_Standar
,sum(cantidadcw) Cant_CW
,max(cantidadfabub) CantFactUB
from {database_name_costos_tmp}.Total01 
group by articulo, ejercicio,per,ce,controlprecios""")
print('carga temporal KE24_CostosVenta', df_KE24_CostosVenta.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}KE24_CostosVenta"
}
df_KE24_CostosVenta.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24_CostosVenta")
print('carga KE24_CostosVenta', df_KE24_CostosVenta.count())
#table_name = 'KE24_CostosVenta'
#fecha_menos = datetime.today()+ relativedelta(months=1)
#fecha_str = fecha_menos.strftime("%Y%m")
#file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
#path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"
#
#try:
#    df_existentes = spark.read.format("parquet").load(path_target31)
#    datos_existentes = True
#    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
#except:
#    datos_existentes = False
#    logger.info(f"No se encontraron datos existentes en {table_name}")
#
#if datos_existentes:
#    existing_data = spark.read.format("parquet").load(path_target31)
#    data_after_delete = existing_data.filter(~(concat(col("Ejercicio"),col("Per")) >= fecha_str))
#    filtered_new_data = df_KE24_Gasto.filter(concat(col("Ejercicio"),col("Per")) >= fecha_str)
#    final_data = filtered_new_data.union(data_after_delete)                             
#   
#    cant_ingresonuevo = filtered_new_data.count()
#    cant_total = final_data.count()
#    
#    # Escribir los resultados en ruta temporal
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
#    }
#    final_data.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}Temporal")
#
#    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    final_data2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
#            
#    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
#    print(f"Total de registros en la tabla {table_name} : {cant_total}")
#     #Limpia la ubicación temporal
#    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
#    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
#    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
#else:
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    df_KE24_Gasto.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
##PROCESO PARA INSERTAR LOS DATOS EN LA TABLA KE24_GASTOS01
#df_KE24_Gasto01 = spark.sql(f"""
#WITH A1 AS (
#SELECT 
#'1-POLLO VIVO' Tipo 
#,Provincia 
#,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
#,GastoA 
#,GastoC
#,GastoL 
#,Gasto 
#,Kilos 
#,Gunit 
#,0 EsGerencia 
#FROM {database_name}.KE24_Gasto 
#where CAST(Per AS INT)=month(current_date)-1
#and Ejercicio=year(current_date) and month(current_date) <> 1 
#and Org_Venta='1001'),
#A2 AS (
#SELECT 
#'1-POLLO VIVO' Tipo 
#,Provincia 
#,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
#,GastoA 
#,GastoC 
#,GastoL 
#,Gasto 
#,Kilos 
#,Gunit 
#,0 EsGerencia 
#FROM {database_name}.KE24_Gasto 
#where cast(Per as int)=12 
#and Ejercicio=year(current_date)-1 and month(current_date) =1 
#and Org_Venta='1001' ),
#A3 AS (
#SELECT 
#'2-POLLO BENEFICIADO' Tipo 
#,Provincia 
#,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
#,sum(GastoA) GastoA 
#,sum(GastoC) GastoC 
#,sum(GastoL) GastoL 
#,sum(Gasto) Gasto 
#,sum(Kilos) Kilos 
#,Round((sum(Gasto)/nullif(sum(Kilos),0)),3) Gunit 
#,0 EsGerencia 
#FROM {database_name}.KE24_Gasto 
#where cast(Per as int)=month(current_date)-1
#and Ejercicio=year(current_date) and month(current_date) <> 1 
#and Org_Venta='1002'
#group by Ejercicio,Per,Provincia
#),
#A4 AS (
#SELECT 
#'2-POLLO BENEFICIADO' Tipo 
#,Provincia 
#,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
#,sum(GastoA) GastoA 
#,sum(GastoC) GastoC 
#,sum(GastoL) GastoL 
#,sum(Gasto) Gasto 
#,sum(Kilos) Kilos 
#,Round((sum(Gasto)/sum(Kilos)),3) Gunit 
#,0 EsGerencia 
#FROM {database_name}.KE24_Gasto 
#where cast(Per as int)=12 
#and Ejercicio= year(current_date)-1 and month(current_date)=1 
#and Org_Venta='1002'
#group by Ejercicio,Per,Provincia
#)
#SELECT * FROM A1
#UNION ALL
#SELECT * FROM A2
#UNION ALL
#SELECT * FROM A3
#UNION ALL
#SELECT * FROM A4
#""")
#print('carga temporal KE24_Gasto01', df_KE24_Gasto01.count())
df_KE24_Gasto01 = spark.sql(f"""
WITH A1 AS (
SELECT 
'1-POLLO VIVO' Tipo 
,Provincia 
,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
,GastoA 
,GastoC
,GastoL 
,Gasto 
,Kilos 
,Gunit 
,0 EsGerencia 
FROM {database_name_costos_gl}.KE24_Gasto 
where Org_Venta='1001'),
A3 AS (
SELECT 
'2-POLLO BENEFICIADO' Tipo 
,Provincia 
,CONCAT(Ejercicio,'-',Per,'-','01') Fecha 
,sum(GastoA) GastoA 
,sum(GastoC) GastoC 
,sum(GastoL) GastoL 
,sum(Gasto) Gasto 
,sum(Kilos) Kilos 
,Round((sum(Gasto)/nullif(sum(Kilos),0)),3) Gunit 
,0 EsGerencia 
FROM {database_name_costos_gl}.KE24_Gasto 
where Org_Venta='1002'
group by Ejercicio,Per,Provincia
)
SELECT * FROM A1
UNION ALL
SELECT * FROM A3
""")
print('carga temporal KE24_Gasto01', df_KE24_Gasto01.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}KE24_Gasto01"
}
df_KE24_Gasto01.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.KE24_Gasto01")
print('carga KE24_Gasto01', df_KE24_Gasto01.count())
#table_name = 'KE24_Gasto01'
#file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
#path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"
#
#today = datetime.today()
#current_year = today.year
#current_month = today.month
#previous_month_date = today + relativedelta(months=-1)
#prev_year = previous_month_date.year
#prev_month = previous_month_date.month
#
#try:
#    df_existentes = spark.read.format("parquet").load(path_target31)
#    datos_existentes = True
#    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
#except:
#    datos_existentes = False
#    logger.info(f"No se encontraron datos existentes en {table_name}")
#
#if datos_existentes:
#    existing_data = spark.read.format("parquet").load(path_target31)
#    
#    existing_data = existing_data.withColumn("fecha_date", to_date(col("fecha"), "yyyy-MM-dd"))
#    df_KE24_Gasto01 = df_KE24_Gasto01.withColumn("fecha_date", to_date(col("fecha"), "yyyy-MM-dd"))
#
#    condition1 = ((month(col("fecha_date")) == prev_month) & (year(col("fecha_date")) == current_year) & (lit(current_month) != 1))
#    condition2 = ((month(col("fecha_date")) == 12) & (year(col("fecha_date")) == current_year - 1) & (lit(current_month) == 1))
#    delete_condition = condition1 | condition2
#    data_after_delete = existing_data.filter(~delete_condition)
#    filtered_new_data = df_KE24_Gasto01.filter(delete_condition)
#    filtered_new_data = filtered_new_data.withColumn("EsGerencia", lit(1)) #set EsGerencia=1
#    final_data = filtered_new_data.union(data_after_delete)
#    final_data = final_data.drop("fecha_date")    
#    
#    cant_ingresonuevo = filtered_new_data.count()
#    cant_total = final_data.count()
#    
#    # Escribir los resultados en ruta temporal
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
#    }
#    final_data.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}Temporal")
#
#    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    final_data2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
#            
#    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
#    print(f"Total de registros en la tabla {table_name} : {cant_total}")
#     #Limpia la ubicación temporal
#    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
#    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
#    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
#else:
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
#    }
#    df_KE24_Gasto.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.{table_name}")
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()