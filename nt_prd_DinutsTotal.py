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
JOB_NAME = "nt_prd_DinutsTotal"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime
from pyspark.sql.functions import *

print("inicia spark")
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

print('cargando ruta')
database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
df_std1 = spark.sql(f""" 
select 
 pk_standard
,cstandard
,nstandard
,idempresa pk_empresa
,lstandard
,Age pk_diasvida
,COALESCE(BSD.U_MortPercent, 0) as STDMortDia
,COALESCE(BSD.U_MortPorcAcm,0) as STDMortDiaAcum
,COALESCE(BSD.U_MortPorcSem,0) as STDMortSem
,COALESCE(BSD.U_PesoVivo,0) as STDPeso
,COALESCE(BSD.U_FeedConsumed,0) as STDConsDia
,COALESCE(BSD.U_ConsAlimAcum,0) as STDConsAcum
,COALESCE(BSD.U_IEP,0) as STD_IEP
,COALESCE(BSD.U_FeedConversionBC,0) as STDICA
,COALESCE(BSD.U_WeightGainDay,0) as STDGanancia
,BSD.U_MortPercent+BSD.U_MortPorcAcm+BSD.U_MortPorcSem+BSD.U_PesoVivo+BSD.U_FeedConsumed Total
from default.si_brimstandardsdata BSD
LEFT JOIN default.si_ProteinStandardVersions PSV ON BSD.ProteinStandardVersionsIRN = PSV.IRN
LEFT JOIN default.lk_standard LST ON LST.IRN = PSV.ProteinStandardsIRN
where age <> 0
order by pk_standard,age
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/std1"
}
df_std1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.std1")

print("carga std1 --> Registros procesados:", df_std1.count())
df_UPDstd1 = spark.sql(f"""
WITH stdMax AS (
select pk_standard,cstandard,nstandard,pk_empresa,lstandard, MAX(STDMortDia) STDMortDia, MAX(STDMortDiaAcum) STDMortDiaAcum
,MAX(STDMortSem) STDMortSem,MAX(STDPeso) STDPeso,MAX(STDConsDia) STDConsDia, MAX(STDConsAcum) STDConsAcum,MAX(STD_IEP) STD_IEP,MAX(STDICA) STDICA,MAX(STDGanancia) STDGanancia
from DEFAULT.std1
group by pk_standard,cstandard,nstandard,pk_empresa,lstandard)

select 
 A.pk_standard
,A.cstandard
,A.nstandard
,A.pk_empresa
,A.lstandard
,A.pk_diasvida
,B.STDMortDia
,B.STDMortDiaAcum
,B.STDMortSem
,B.STDPeso
,B.STDConsDia
,B.STDConsAcum
,B.STD_IEP
,B.STDICA
,B.STDGanancia
,A.Total
from DEFAULT.std1 A
left join stdMax B on A.pk_standard = B.pk_standard
where Total = 0
union all 
select A.* 
FROM DEFAULT.std1 A
WHERE Total <> 0
""")
 
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/std1_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDstd1.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable("default.std1_new")

df_std1_nueva = spark.sql("""SELECT * from default.std1_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql("DROP TABLE IF EXISTS default.std1")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/std1"
}
df_std1_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"default.std1")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql("DROP TABLE IF EXISTS default.std1_new")

print("carga UPD std1 --> Registros procesados:", df_std1_nueva.count())
df_ft_dinuts_total = spark.sql(f""" 
select 
pk_standard, cstandard, nstandard, pk_empresa, lstandard, pk_diasvida,
STDMortDia,
STDMortDiaAcum,
STDMortSem,
STDPeso,
STDConsDia,
STDConsAcum,
STD_IEP,
STDICA,
STDGanancia
from default.std1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_dinuts_total"
}
df_ft_dinuts_total.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_dinuts_total")

print("carga ft_dinuts_total --> Registros procesados:", df_ft_dinuts_total.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()