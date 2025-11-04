# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
#sc._conf.set("spark.executor.memory", "16g")
#sc._conf.set("spark.driver.memory", "4g")
#sc._conf.set("spark.dynamicAllocation.enabled", "true")
#sc._conf.set("spark.executor.memoryOverhead", "2g")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_ConsumoGasCamaAgua_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")

import sys
from pyspark.sql import SparkSession
#from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime

print("inicia spark")
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, current_timestamp
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

#file_name_target2 = f"{bucket_name_prdmtech}ft_ConsumoGasCamaAgua/"
file_name_target2 = f"{bucket_name_prdmtech}stg_GasAguaCama/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
print('cargando rutas')
from pyspark.sql.functions import *

db_bi_sf_tmp = "bi_sf_tmp"
db_mtech_prd_sf_br = "mtech_prd_sf_br"
db_mtech_prd_sf_si = "mtech_prd_sf_si"
db_mtech_prd_sf_gl = "mtech_prd_sf_gl"

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')

df_mvHimChickTrans= spark.sql(f"select CONCAT(rtrim(upper(farmno)),'-',rtrim(upper(entityno))) as ComplexEntityNo, sum(RelativeHeadPlaced) as Total \
                              from {database_name}.si_mvhimchicktrans \
                              where PostStatus=2 and VoidFlag = false \
                              group by upper(farmno),upper(entityno)")

print('carga df_mvHimChickTrans')
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvHimChickTrans"
}
df_mvHimChickTrans.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mvHimChickTrans")

print('carga temporal mvHimChickTrans')

#df_mvHimChickTrans1 = spark.sql(f"select substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNo, sum(Inventario) Total, 'Pavo' Division \
#from {database_name}.ft_ingresocons \
#where pk_empresa = 1 and pk_division = 2 \
#group by substring(complexentityno,1,(length(complexentityno)-6))")
##iddivision = 4
#
#print('carga df_mvHimChickTrans1')

#df_mvHimChickTrans2= spark.sql(f"select CONCAT(rtrim(farmno),'-',rtrim(entityno)) as ComplexEntityNo, sum(RelativeHeadPlaced) as Total, 'Pollo' Division \
#                              from {database_name}.si_mvhimchicktrans \
#                              where PostStatus=2 and VoidFlag = false \
#                              and FarmType = 1 and SpeciesType = 1\
#                              group by farmno,entityno")
#
#print('carga df_mvHimChickTrans2')
#df_mvHimChickTrans = df_mvHimChickTrans1.union(df_mvHimChickTrans2)
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvHimChickTrans"
#}
#df_mvHimChickTrans.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.mvHimChickTrans")
#print('carga temporal mvHimChickTrans')
df_mvHimChickTransGalpon= spark.sql(f"select CONCAT(rtrim(upper(farmno)),'-',rtrim(upper(entityno)),'-',rtrim(upper(houseno))) as ComplexEntityNo, sum(RelativeHeadPlaced) as Total \
                                     from {database_name}.si_mvhimchicktrans \
                                     where PostStatus=2 and VoidFlag = false \
                                     group by upper(farmno),upper(entityno),upper(houseno) ")

print('carga df_mvHimChickTransGalpon')
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvHimChickTransGalpon"
}
df_mvHimChickTransGalpon.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mvHimChickTransGalpon")
print('carga mvHimChickTransGalpon')
#df_mvHimChickTransGalpon1= spark.sql(f"select substring(complexentityno,1,(length(complexentityno)-3)) ComplexEntityNo, sum(Inventario) Total, 'Pavo' Division \
#from {database_name}.ft_ingresocons \
#where pk_empresa = 1 and pk_division = 2 \
#group by substring(complexentityno,1,(length(complexentityno)-3))")
##iddivision = 4
#
#print('carga df_mvHimChickTransGalpon1')
#df_mvHimChickTransGalpon2= spark.sql(f"select CONCAT(rtrim(farmno),'-',rtrim(entityno),'-',rtrim(houseno)) as ComplexEntityNo, sum(RelativeHeadPlaced) as Total, 'Pollo' Division \
#                                     from {database_name}.si_mvhimchicktrans \
#                                     where PostStatus=2 and VoidFlag = false \
#                                     and FarmType = 1 and SpeciesType = 1 \
#                                     group by farmno,entityno,houseno ")
#
#print('carga df_mvHimChickTransGalpon2')
##df_mvHimChickTransGalpon.createOrReplaceTempView("mvHimChickTransGalpon")
#df_mvHimChickTransGalpon =df_mvHimChickTransGalpon1.union(df_mvHimChickTransGalpon2)
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvHimChickTransGalpon"
#}
#df_mvHimChickTransGalpon.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.mvHimChickTransGalpon")
#print('carga mvHimChickTransGalpon')
#la tabla si_mvproteinproductwhusage tiene sql:358,148 y aws:1,138,217
#Realiza el cálculo de GasPropano
df_GasPropano1= spark.sql(f"select upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage \
where ProductNo='10354' and PostStatus=2 AND VoidFlag = false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID), upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \
#sql=348 / aws:1,201

df_GasPropano2 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) ProductName \
from {database_name}.si_mvproteinproductwhusage \
where ProductNo='10354' and PostStatus=2 AND VoidFlag = false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))), upper(ProductName)")
#sql:82 / aws:159

df_GasPropano = df_GasPropano1.union(df_GasPropano2)
#df_GasPropano.createOrReplaceTempView("GasPropano")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/GasPropano"
}
df_GasPropano.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.GasPropano")
print('carga GasPropano')
#df_GasPropano1.count()
#df_Gasx= spark.sql(f"select date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')")
#df_Gasx= spark.sql(f"select MAKE_DATE(YEAR(current_date()) - 1, 1, 1) AS first_day_last_year")
#df_Gasx= spark.sql(f"select date_format(cast(xdate as timestamp),'yyyy-MM-dd'), MAKE_DATE(YEAR(current_date()) - 1, 1, 1) from {database_name}.si_mvproteinproductwhusage limit 100")
#df_Gasx = spark.sql(f"select TransactionEntityID, ProductName, count(1) from {database_name}.si_mvproteinproductwhusage \
#                    where date_format(cast(xdate as timestamp),'yyyy-MM-dd') >= date_format(MAKE_DATE(YEAR(current_date()) - 1, 1, 1), 'yyyy-MM-dd') \
#                    And ProductNo='10354' and PostStatus=2 and VoidFlag = false and FarmType = 1 and SpeciesType = 1 group by TransactionEntityID, ProductName")
#df_Gasx = spark.sql(f"select count(1) from {database_name}.si_mvproteinproductwhusage \
#                    where date_format(cast(xdate as timestamp),'yyyy-MM-dd') >= date_format(MAKE_DATE(YEAR(current_date()) - 1, 1, 1), 'yyyy-MM-dd') ")
#df_Gasx = spark.sql(f"select count(1) from {database_name}.si_mvproteinproductwhusage \
#                    where date_format(cast(xdate as timestamp),'yyyy-MM-dd') >= date_format(MAKE_DATE(YEAR(current_date()) - 1, 1, 1), 'yyyy-MM-dd') \
#                    and ProductNo='10354'  \
#                    and PostStatus=2 \
#                    and VoidFlag=0 \
#                    and FarmType = 1 and SpeciesType = 1 \
#                    group by TransactionEntityID, ProductName ")

#df_Gasx.show()
#df_Gasx.count()
#Realiza el cálculo de GasGranel
df_GasGranel1 = spark.sql(f"select upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='10355' and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \

df_GasGranel2 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) ProductName \
from {database_name}.si_mvproteinproductwhusage \
where ProductNo='10355' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))), upper(ProductName)")

df_GasGranel = df_GasGranel1.union(df_GasGranel2)
#df_GasGranel.createOrReplaceTempView("GasGranel")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/GasGranel"
}
df_GasGranel.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.GasGranel")
print('carga GasGranel')
#Realiza el cálculo de Cama1
df_Cama01 = spark.sql(f"select upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='51877' and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \

df_Cama02 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='51877' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")
        
df_Cama1 = df_Cama01.union(df_Cama02)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama1"
}
df_Cama1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama1")
print('carga Cama1')
#Realiza el cálculo de Cama2
df_Cama03 = spark.sql(f"select upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='11763' and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \

df_Cama04 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity,  upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='11763' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")

df_Cama2= df_Cama03.union(df_Cama04)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama2"
}
df_Cama2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama2")
print('carga Cama2')
#Realiza el cálculo de Cama3
df_Cama3 = spark.sql(f"select  upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='90464' and PostStatus=2 AND VoidFlag= false \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID), upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama3"
}
df_Cama3.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama3")
print('carga Cama3')
#Realiza el cálculo de Cama4
df_Cama05 = spark.sql(f"select  upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='90465' and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \


df_Cama06 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='90465' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")
                      
df_Cama4= df_Cama05.union(df_Cama06)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama4"
}
df_Cama4.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama4")
print('carga Cama4')
#Realiza el cálculo de Cama5
df_Cama07 = spark.sql(f"select  upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='90466' and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID), upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \


df_Cama08 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='90466' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")

df_Cama5= df_Cama07.union(df_Cama08)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama5"
}
df_Cama5.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama5")
print('carga Cama5')
#Realiza el cálculo de Cama6
df_Cama6 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo='94145' and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 and length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Cama6"
}
df_Cama6.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Cama6")
print('carga Cama6')
#Realiza el cálculo de Agua
df_Agua01 = spark.sql(f"select upper(TransactionEntityID) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo IN ('47791') and PostStatus=2 AND VoidFlag= false and FarmType = 1 and SpeciesType = 1 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(TransactionEntityID),upper(ProductName)")
#and (date_format(cast(xDate as timestamp),'yyyyMM') >= {AnioMes} AND date_format(cast(xDate as timestamp),'yyyyMM') <= {AnioMesFin}) \


df_Agua02 = spark.sql(f"select upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))) as TransactionEntityID, SUM(Quantity) as Quantity, upper(ProductName) as ProductName \
from {database_name}.si_mvproteinproductwhusage x \
where ProductNo IN ('47788','1001442') and PostStatus=2 AND VoidFlag= false and FarmType = 7 and SpeciesType = 2 AND length(TransactionEntityID) >= 5 \
and date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
group by upper(substring(TransactionEntityID,1,(length(TransactionEntityID)-3))),upper(ProductName)")

df_Agua = df_Agua01.union(df_Agua02)
#df_Agua.createOrReplaceTempView("Agua")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Agua"
}
df_Agua.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Agua")
print('carga Agua')
df_mvPmtsProcRecvTrans =spark.sql(f"select CONCAT(upper(rtrim(FarmNo)),'-',upper(rtrim(EntityNo))) as ComplexEntityNo, \
Sum(PlantWtNet) as PlantWtNet \
from {database_name}.si_mvpmtsprocrecvtranshousedetail \
where isnotnull(PostTransactionId) = true \
group by  CONCAT(upper(rtrim(FarmNo)),'-',upper(rtrim(EntityNo)))")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvPmtsProcRecvTrans"
}
df_mvPmtsProcRecvTrans.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mvPmtsProcRecvTrans")
print('carga mvPmtsProcRecvTrans')
df_mvPmtsProcRecvTransGalpon = spark.sql(f"select CONCAT(upper(rtrim(FarmNo)),'-',upper(rtrim(EntityNo)),'-',upper(rtrim(HouseNo))) as ComplexEntityNo, \
Sum(PlantWtNet) as PlantWtNet \
from {database_name}.si_mvpmtsprocrecvtranshousedetail \
where isnotnull(PostTransactionId) = true \
group by CONCAT(upper(rtrim(FarmNo)),'-',upper(rtrim(EntityNo)),'-',upper(rtrim(HouseNo)))")


# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvPmtsProcRecvTransGalpon"
}
df_mvPmtsProcRecvTransGalpon.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mvPmtsProcRecvTransGalpon")
print('carga mvPmtsProcRecvTransGalpon')
#Se muestra el consumo a nivel de Galpon y Lote
df_ConsumoGL = spark.sql(f"select * \
from ( \
select \
A.ComplexEntityNo AS ComplexEntityNoLT, \
A.ComplexEntityNo, \
A.Total, \
B.Quantity AS QuantityGP, \
B.ProductName AS ProductNameGP, \
C.Quantity AS QuantityGG, \
C.ProductName AS ProductNameGG, \
D.Quantity AS QuantityCA, \
D.ProductName AS ProductNameCA, \
E.Quantity AS QuantityCB, \
E.ProductName AS ProductNameCB, \
F.Quantity AS QuantityCC, \
F.ProductName AS ProductNameCC, \
G.Quantity AS QuantityCD, \
G.ProductName AS ProductNameCD, \
H.Quantity AS QuantityCE, \
H.ProductName AS ProductNameCE, \
K.Quantity AS QuantityCF, \
K.ProductName AS ProductNameCF, \
I.PlantWtNet, \
J.Quantity AS QuantityA, \
J.ProductName AS ProductNameA \
from \
{database_name}.mvHimChickTrans A \
left join {database_name}.GasPropano B on upper(A.ComplexEntityNo) = upper(B.TransactionEntityID) \
left join {database_name}.GasGranel C on upper(A.ComplexEntityNo) = upper(c.TransactionEntityID) \
left join {database_name}.Cama1 D on upper(A.ComplexEntityNo) = upper(D.TransactionEntityID) \
left join {database_name}.Cama2 E on upper(A.ComplexEntityNo) = upper(E.TransactionEntityID) \
left join {database_name}.Cama3 F on upper(A.ComplexEntityNo) = upper(F.TransactionEntityID) \
left join {database_name}.Cama4 G on upper(A.ComplexEntityNo) = upper(G.TransactionEntityID) \
left join {database_name}.Cama5 H on upper(A.ComplexEntityNo) = upper(H.TransactionEntityID) \
left join {database_name}.Cama6 k on upper(A.ComplexEntityNo) = upper(K.TransactionEntityID) \
left join {database_name}.mvPmtsProcRecvTrans I on upper(A.ComplexEntityNo) = upper(i.ComplexEntityNo) \
left join {database_name}.Agua J on upper(A.ComplexEntityNo) = upper(J.TransactionEntityID) \
union \
select \
substring(A.ComplexEntityNo,1,length(A.ComplexEntityNo)-3) AS ComplexEntityNoLT, \
A.ComplexEntityNo, \
A.Total, \
B.Quantity AS QuantityGP, \
B.ProductName AS ProductNameGP, \
C.Quantity AS QuantityGG, \
C.ProductName AS ProductNameGG, \
D.Quantity AS QuantityCA, \
D.ProductName AS ProductNameCA, \
E.Quantity AS QuantityCB, \
E.ProductName AS ProductNameCB, \
F.Quantity AS QuantityCC, \
F.ProductName AS ProductNameCC, \
G.Quantity AS QuantityCD, \
G.ProductName AS ProductNameCD, \
H.Quantity AS QuantityCE, \
H.ProductName AS ProductNameCE, \
K.Quantity AS QuantityCF, \
K.ProductName AS ProductNameCF, \
I.PlantWtNet, \
J.Quantity AS QuantityA, \
J.ProductName AS ProductNameA \
from \
{database_name}.mvHimChickTransGalpon A \
left join {database_name}.GasPropano B on upper(A.ComplexEntityNo) = upper(B.TransactionEntityID) \
left join {database_name}.GasGranel C on upper(A.ComplexEntityNo) = upper(c.TransactionEntityID) \
left join {database_name}.Cama1 D on upper(A.ComplexEntityNo) = upper(D.TransactionEntityID) \
left join {database_name}.Cama2 E on upper(A.ComplexEntityNo) = upper(E.TransactionEntityID) \
left join {database_name}.Cama3 F on upper(A.ComplexEntityNo) = upper(F.TransactionEntityID) \
left join {database_name}.Cama4 G on upper(A.ComplexEntityNo) = upper(G.TransactionEntityID) \
left join {database_name}.Cama5 H on upper(A.ComplexEntityNo) = upper(H.TransactionEntityID) \
left join {database_name}.Cama6 k on upper(A.ComplexEntityNo) = upper(K.TransactionEntityID) \
left join {database_name}.mvPmtsProcRecvTransGalpon I on upper(A.ComplexEntityNo) = upper(i.ComplexEntityNo) \
left join {database_name}.Agua J on upper(A.ComplexEntityNo) = upper(J.TransactionEntityID))A ")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsumoGL"
}
df_ConsumoGL.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsumoGL")
print('carga ConsumoGL')
#Inserta los datos en la tabla STGPECUARIO.GasAguaCama
df_GasAguaCama01 = spark.sql(f"select A.ComplexEntityNo \
,nvl(total,0) as Total \
,nvl(b.Quantity,0) as GasPropano \
,nvl(c.Quantity,0) as GasGranel \
,nvl(d.Quantity,0) as CamaA \
,nvl(e.Quantity,0) as CamaB \
,nvl(f.Quantity,0) as CamaC \
,nvl(g.Quantity,0) as CamaD \
,nvl(h.Quantity,0) as CamaE \
,nvl(K.Quantity,0) as CamaF \
,nvl(j.Quantity,0) as AguaA \
,(CASE WHEN nvl(Total,0)>0 THEN cast((nvl(b.Quantity,0) * 21.6 + nvl(c.Quantity,0)) * 1000 as decimal(30,8)) / nvl(total,0) ELSE 0 END) as Gas \
,(CASE WHEN nvl(Total,0)>0 THEN cast((nvl(D.Quantity,0) * 8.45 + nvl(E.Quantity,0) + nvl(F.Quantity,0) * 8.45 + nvl(G.Quantity,0)* 8.45 + \
nvl(H.Quantity,0)* 8.45 + nvl(K.Quantity,0)* 8.45) * 1000 as decimal(30,8)) / nvl(total,0) ELSE 0 END) as Cama \
,CASE WHEN A.ComplexEntityNo LIKE 'V%' THEN cast(nvl(J.Quantity,0) * 1000 as long) / NULLIF(nvl(A.total,0),0) ELSE \
cast(nvl(J.Quantity,0) * 1000 as long) / NULLIF(nvl(PlantWtNet,0),0) END AS Agua \
from {database_name}.mvHimChickTransGalpon A \
left join {database_name}.GasPropano B on upper(A.ComplexEntityNo) = upper(B.TransactionEntityID) \
left join {database_name}.GasGranel C on upper(A.ComplexEntityNo) = upper(c.TransactionEntityID) \
left join {database_name}.Cama1 D on upper(A.ComplexEntityNo) = upper(D.TransactionEntityID) \
left join {database_name}.Cama2 E on upper(A.ComplexEntityNo) = upper(E.TransactionEntityID) \
left join {database_name}.Cama3 F on upper(A.ComplexEntityNo) = upper(F.TransactionEntityID) \
left join {database_name}.Cama4 G on upper(A.ComplexEntityNo) = upper(G.TransactionEntityID) \
left join {database_name}.Cama5 H on upper(A.ComplexEntityNo) = upper(H.TransactionEntityID) \
left join {database_name}.Cama6 K on upper(A.ComplexEntityNo) = upper(K.TransactionEntityID) \
left join {database_name}.mvPmtsProcRecvTransGalpon I on upper(A.ComplexEntityNo) = upper(i.ComplexEntityNo) \
left join {database_name}.Agua J on upper(A.ComplexEntityNo) = upper(J.TransactionEntityID)")

##,(CASE WHEN nvl(Total,0)>0 THEN (nvl(b.Quantity,0) * 21.6 + nvl(c.Quantity,0)) * 1000 / nvl(total,0) ELSE 0 END) as Gas \
##,(CASE WHEN nvl(Total,0)>0 THEN (nvl(D.Quantity,0) * 8.45 + nvl(E.Quantity,0) + nvl(F.Quantity,0) * 8.45 + nvl(G.Quantity,0)* 8.45 + \
##nvl(H.Quantity,0)* 8.45 + nvl(K.Quantity,0)* 8.45) * 1000 / nvl(total,0) ELSE 0 END) as Cama \
##,CASE WHEN A.ComplexEntityNo LIKE 'V%' THEN (nvl(J.Quantity,0) * 1000) / NULLIF(nvl(A.total,0),0) ELSE \
##(nvl(J.Quantity,0) * 1000) / NULLIF(nvl(PlantWtNet,0),0) END AS Agua \

df_GasAguaCama02 = spark.sql(f"select A.ComplexEntityNo \
,MAX(nvl(A.Total,0)) as Total \
,SUM(nvl(K.QuantityGP,0)) as GasPropano \
,SUM(nvl(K.QuantityGG,0)) as GasGranel \
,SUM(nvl(K.QuantityCA,0)) as CamaA \
,SUM(nvl(K.QuantityCB,0)) as CamaB \
,SUM(nvl(K.QuantityCC,0)) as CamaC \
,SUM(nvl(K.QuantityCD,0)) as CamaD \
,SUM(nvl(K.QuantityCE,0)) as CamaE \
,SUM(nvl(K.QuantityCF,0)) as CamaF \
,SUM(nvl(K.QuantityA,0)) as AguaA \
,(CASE WHEN MAX(nvl(A.Total,0))>0 THEN cast((SUM(nvl(K.QuantityGP,0)) * 21.6 + SUM(nvl(K.QuantityGG,0))) * 1000 as decimal(30,8)) / MAX(nvl(A.total,0)) ELSE 0 \
  END) as Gas \
,(CASE WHEN MAX(nvl(A.Total,0))>0 THEN cast((SUM(nvl(K.QuantityCA,0)) * 8.45 + SUM(nvl(K.QuantityCB,0)) + SUM(nvl(K.QuantityCC,0)) * 8.45 + \
  SUM(nvl(K.QuantityCD,0))* 8.45 + SUM(nvl(K.QuantityCE,0)* 8.45) + SUM(nvl(K.QuantityCF,0)* 8.45)) * 1000 as decimal(30,8)) / MAX(nvl(A.total,0)) ELSE 0 END) as Cama \
,CASE WHEN A.ComplexEntityNo LIKE 'V%' THEN cast(SUM(nvl(K.QuantityA,0)) * 1000 as long) / NULLIF(MAX(nvl(A.total,0)),0) ELSE cast(SUM(nvl(K.QuantityA,0)) * 1000 as long) \
/ NULLIF(MAX(nvl(I.PlantWtNet,0)),0) END AS Agua \
from {database_name}.mvHimChickTrans A \
left join {database_name}.GasPropano B on upper(A.ComplexEntityNo) = upper(B.TransactionEntityID) \
left join {database_name}.GasGranel C on upper(A.ComplexEntityNo) = upper(c.TransactionEntityID) \
left join {database_name}.Cama1 D on upper(A.ComplexEntityNo) = upper(D.TransactionEntityID) \
left join {database_name}.Cama2 E on upper(A.ComplexEntityNo) = upper(E.TransactionEntityID) \
left join {database_name}.Cama3 F on upper(A.ComplexEntityNo) = upper(F.TransactionEntityID) \
left join {database_name}.Cama4 G on upper(A.ComplexEntityNo) = upper(G.TransactionEntityID) \
left join {database_name}.Cama5 H on upper(A.ComplexEntityNo) = upper(H.TransactionEntityID) \
left join {database_name}.Cama6 L on upper(A.ComplexEntityNo) = upper(L.TransactionEntityID) \
left join {database_name}.mvPmtsProcRecvTrans I on upper(A.ComplexEntityNo) = upper(i.ComplexEntityNo) \
left join {database_name}.Agua J on upper(A.ComplexEntityNo) = upper(J.TransactionEntityID) \
left join {database_name}.ConsumoGL K ON upper(K.ComplexEntityNoLT) = upper(A.ComplexEntityNo) \
GROUP BY A.ComplexEntityNo")


#df_GasAguaCama02 = df_GasAguaCama02.withColumn('Gas', col('Gas').cast(DecimalType(38,10)))
#df_GasAguaCama01.unionByName(df_GasAguaCama02, allowMissingColumns=True)

df_GasAguaCama = df_GasAguaCama01.union(df_GasAguaCama02)


from pyspark.sql.types import DecimalType
df_GasAguaCama = df_GasAguaCama.withColumn('Gas', col('Gas').cast(DecimalType(38,8)))\
    .withColumn('Cama', col('Cama').cast(DecimalType(38,8)))\
    .withColumn('Agua', col('Agua').cast(DecimalType(38,8)))

#df_ft_consumogascamaagua = df_GasAguaCama

df_stg_GasAguaCama = df_GasAguaCama

#df_ft_consumogascamaagua.show()
#df_stg_GasAguaCama.show()

#print('inicia carga tabla df_ft_consumogascamaagua')
print('inicia carga tabla df_stg_GasAguaCama')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target2)     
try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    #logger.info(f"Datos existentes de ft_ConsumoGasCamaAgua cargados: {df_existentes.count()} registros")
    logger.info(f"Datos existentes de stg_GasAguaCama cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    #logger.info("No se encontraron datos existentes en ft_ConsumoGasCamaAgua")
    logger.info("No se encontraron datos existentes en stg_GasAguaCama")


if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    #data_after_delete = existing_data.filter(~((date_format(col("fecha"),"yyyyMM") >= AnioMes) & (date_format(col("fecha"),"yyyyMM") <= AnioMesFin)))
    #filtered_new_data = df_ft_consumogascamaagua.filter((date_format(col("fecha"),"yyyyMM") >= AnioMes) & (date_format(col("fecha"),"yyyyMM") <= AnioMesFin))
    #final_data = filtered_new_data.union(data_after_delete)                             
    #
    cant_dataanterior= existing_data.count()
    #cant_ingresonuevo = df_ft_consumogascamaagua.count()
    cant_ingresonuevo = df_stg_GasAguaCama.count()
    #filtered_new_data.count()
    #cant_total = df_ft_consumogascamaagua.count()
    cant_total = df_stg_GasAguaCama.count()
    #
    ## Escribir los resultados en ruta temporal
    #additional_options = {
    #    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ConsumoGasCamaAguaTemporal"
    #}
    #final_data.write \
    #    .format("parquet") \
    #    .options(**additional_options) \
    #    .mode("overwrite") \
    #    .saveAsTable(f"{database_name}.ft_ConsumoGasCamaAguaTemporal")
    
    
    #schema = existing_data.schema
    #final_data2 = df_ft_consumogascamaagua
    final_data2 = df_stg_GasAguaCama
    #spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ConsumoGasCamaAguaTemporal")
            
    #"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ConsumoGasCamaAgua"
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}stg_GasAguaCama"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.stg_gasaguacama")
    
    #.saveAsTable(f"{database_name}.ft_ConsumoGasCamaAgua")
    
    print(f"Cantidad de registros anteriores de stg_GasAguaCama : {cant_dataanterior}")
    print(f"agrega registros nuevos a la tabla stg_GasAguaCama : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla stg_GasAguaCama : {cant_total}")
     #Limpia la ubicación temporal
    #glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    #glue_client.delete_table(DatabaseName=database_name, Name='ft_ConsumoGasCamaAguaTemporal')
    #print(f"Tabla ft_ConsumoGasCamaAguaTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}stg_GasAguaCama"
    }
    df_stg_GasAguaCama.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.stg_gasaguacama")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()