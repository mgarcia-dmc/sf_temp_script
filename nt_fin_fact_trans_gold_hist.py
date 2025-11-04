from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_fact_trans_gold_hist"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
from pyspark.sql.functions import col

## Parametros Globales 
import boto3
ssm = boto3.client("ssm")
db_name = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"
gold_bucket_name = f"ue1stg{p_amb}as3dtl005-gold"

table_name_source_1 = "si_vbrk"
table_name_source_2 = "si_vbrp"
path_source_1 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_source_1}"
path_source_2 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_source_2}"

table_name_target = f"facturacion"
path_target = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target}"

# Leer archivos de origen
df_1 = spark.read.parquet(path_source_1)
df_2 = spark.read.parquet(path_source_2)

# Aplicar filtros y seleccionar columnas
df_facturacion = df_1.join(df_2,(df_1["MANDT"] == df_2["MANDT"]) & (df_1["VBELN"] == df_2["VBELN"]), "inner").select(df_1.VBELN,df_1.WAERK,df_1.ERDAT,df_1.BUKRS,df_1.FKART,df_1.BZIRK,df_1.VKORG,df_1.VTWEG,df_1.SPART,df_1.KUNAG,df_1.FKSTO,df_1.PLTYP,df_1.KDGRP,df_1.VBTYP,df_1.FKDAT,df_2.POSNR,df_2.PRCTR,df_2.MATNR,df_2.WERKS,df_2.NETWR,df_2.WAVWR,df_2.SKFBP,df_2.FKLMG,df_2.MEINS,df_2.NTGEW,df_2.VKBUR,df_2.VKGRP,df_2.PRODH,df_2.MATKL,df_2.ARKTX)

# Aplicar los filtros
df_facturacion_sf = df_facturacion.filter(
    (col("VKORG").isin("1001", "1002")) & 
    (~col("FKART").isin("ZFTC")) & 
    (col("BUKRS") == "SFER") & 
    (col("PRCTR").startswith("000150"))
)
df_facturacion_ch = df_facturacion.filter(
    (col("VKORG").isin("2001", "2002", "2007")) & 
    (~col("FKART").isin("ZFTC")) & 
    (col("BUKRS") == "CHIM") & 
    (col("PRCTR").startswith("00046"))
)
df_facturacion_f = df_facturacion_sf.union(df_facturacion_ch)

# Guardar datos en formato Parquet y registrar en Data Catalog
df_facturacion_f.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", path_target) \
    .saveAsTable(f"{database_name}.{table_name_target}")
# Contar registros y finalizar
record_count = df_facturacion_f.count()
print(f"Registros procesados FACTURACION: {record_count}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()