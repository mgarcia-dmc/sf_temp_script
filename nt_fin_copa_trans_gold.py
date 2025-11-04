from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_copa_trans_gold"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
from pyspark.sql.functions import col
import boto3
ssm = boto3.client("ssm")

## Parametros Globales 
db_name = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"
gold_bucket_name = f"ue1stg{p_amb}as3dtl005-gold"

table_name_source_k81 = "si_k810001"
table_name_source_k82 = "si_k810002"
path_source_copa1 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_source_k81}"
path_source_copa2 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_source_k82}"

table_name_target_copa_real = f"COPA_REAL"
path_target_copa_real = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_target_copa_real}"

table_name_target_copa_plan = f"COPA_PLAN"
path_target_copa_plan = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_target_copa_plan}"

# Leer archivos de origen
df_copa1 = spark.read.parquet(path_source_copa1)
df_copa2 = spark.read.parquet(path_source_copa2)

# Aplicar filtros y seleccionar columnas
df_copa_real = df_copa1.join(df_copa2, df_copa1["TRKEYNR"] == df_copa2["TRKEYNR"], "inner").filter(df_copa1.PLIKZ == "0").select(df_copa1.TRKEYNR, df_copa1.ARTNR, df_copa1.KNDNR, df_copa1.BRSCH, df_copa1.BZIRK, df_copa1.COPA_KOSTL, df_copa1.KDGRP, df_copa1.KMVKBU, df_copa1.MATKL, df_copa1.WWCIC, df_copa1.WWGCB, df_copa1.WWCLC, df_copa1.GJAHR, df_copa1.PERDE, df_copa1.BUKRS, df_copa1.FKART, df_copa1.KOKRS, df_copa1.PRCTR, df_copa1.VERSI, df_copa1.VKORG, df_copa1.VRGAR, df_copa1.VTWEG, df_copa1.PALEDGER, df_copa2.ERLOS, df_copa2.KWSOHD, df_copa2.RABAT, df_copa2.VV990, df_copa2.VVADM, df_copa2.VVALM, df_copa2.VVALT, df_copa2.VVCDS, df_copa2.VVCFB, df_copa2.VVCIR, df_copa2.VVCL1, df_copa2.VVCL2, df_copa2.VVCL3, df_copa2.VVCL4, df_copa2.VVCL5, df_copa2.VVCOI, df_copa2.VVCTC, df_copa2.VVCVE, df_copa2.VVCVM, df_copa2.VVCVR, df_copa2.VVD50, df_copa2.VVDBO, df_copa2.VVDME, df_copa2.VVDOT, df_copa2.VVDRA, df_copa2.VVEPG, df_copa2.VVGFI, df_copa2.VVGNF, df_copa2.VVGUN, df_copa2.VVGVD, df_copa2.VVIMT, df_copa2.VVIPV, df_copa2.VVK10, df_copa2.VVK12, df_copa2.VVK40, df_copa2.VVK42, df_copa2.VVK44, df_copa2.VVK49, df_copa2.VVLOI, df_copa2.VVMKT, df_copa2.VVOEG, df_copa2.VVOIN, df_copa2.VVPAR, df_copa2.VVPFN, df_copa2.VVR11, df_copa2.VVR12, df_copa2.VVR13, df_copa2.VVRSV, df_copa2.VVTD2, df_copa2.VVTRP, df_copa2.VVTRT, df_copa2.VVVEN, df_copa2.VVVPR,df_copa2.VV936,df_copa2.VVCEW,df_copa2.VV991)
    
# Guardar datos en formato Parquet y registrar en Data Catalog
df_copa_real.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", path_target_copa_real) \
    .saveAsTable(f"{database_name}.{table_name_target_copa_real}")
# Leer archivos de origen
df_copa_plan = df_copa1.join(df_copa2, df_copa1["TRKEYNR"] == df_copa2["TRKEYNR"], "inner").filter(df_copa1.PLIKZ == "1").select(df_copa1.TRKEYNR, df_copa1.ARTNR, df_copa1.KNDNR, df_copa1.BRSCH, df_copa1.BZIRK, df_copa1.COPA_KOSTL, df_copa1.KDGRP, df_copa1.KMVKBU, df_copa1.MATKL, df_copa1.WWCIC, df_copa1.WWGCB, df_copa1.WWCLC, df_copa1.GJAHR, df_copa1.PERDE, df_copa1.BUKRS, df_copa1.FKART, df_copa1.KOKRS, df_copa1.PRCTR, df_copa1.VERSI, df_copa1.VKORG, df_copa1.VRGAR, df_copa1.VTWEG, df_copa1.PALEDGER, df_copa2.ERLOS, df_copa2.KWSOHD, df_copa2.RABAT, df_copa2.VV990, df_copa2.VVADM, df_copa2.VVALM, df_copa2.VVALT, df_copa2.VVCDS, df_copa2.VVCFB, df_copa2.VVCIR, df_copa2.VVCL1, df_copa2.VVCL2, df_copa2.VVCL3, df_copa2.VVCL4, df_copa2.VVCL5, df_copa2.VVCOI, df_copa2.VVCTC, df_copa2.VVCVE, df_copa2.VVCVM, df_copa2.VVCVR, df_copa2.VVD50, df_copa2.VVDBO, df_copa2.VVDME, df_copa2.VVDOT, df_copa2.VVDRA, df_copa2.VVEPG, df_copa2.VVGFI, df_copa2.VVGNF, df_copa2.VVGUN, df_copa2.VVGVD, df_copa2.VVIMT, df_copa2.VVIPV, df_copa2.VVK10, df_copa2.VVK12, df_copa2.VVK40, df_copa2.VVK42, df_copa2.VVK44, df_copa2.VVK49, df_copa2.VVLOI, df_copa2.VVMKT, df_copa2.VVOEG, df_copa2.VVOIN, df_copa2.VVPAR, df_copa2.VVPFN, df_copa2.VVR11, df_copa2.VVR12, df_copa2.VVR13, df_copa2.VVRSV, df_copa2.VVTD2, df_copa2.VVTRP, df_copa2.VVTRT, df_copa2.VVVEN, df_copa2.VVVPR,df_copa2.VV936,df_copa2.VVCEW,df_copa2.VV991)
df_copa_plan.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", path_target_copa_plan) \
    .saveAsTable(f"{database_name}.{table_name_target_copa_plan}")

# Contar registros y finalizar
record_count_copa_real = df_copa_real.count()
record_count_copa_plan = df_copa_plan.count()
print(f"Registros procesados COPA REAL: {record_count_copa_real}")
print(f"Registros procesados COPA PLAN: {record_count_copa_plan}")
# print(f"Archivo copiado: {path_source_copa1} -> {path_target_copa_real}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()