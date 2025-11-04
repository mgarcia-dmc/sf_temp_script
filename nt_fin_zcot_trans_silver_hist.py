from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_zcot_trans_silver_hist"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import boto3
ssm = boto3.client("ssm")
## Parametros Globales 
db_name = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"
silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"

# Variables ZCOT_0026
table_name_source_zcot = "br_zcot_0026"
table_name_target_zcot = "si_zcot_0026"
path_source_zcot = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/{table_name_source_zcot}"
path_target_zcot = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/{table_name_target_zcot}"
    
# Leer archivos de origen
try:
    df_source_zcot = spark.read.parquet(path_source_zcot)
    # Contar registros y finalizar
    record_count_zcot = df_source_zcot.count()
    print(f"Registros procesados: {record_count_zcot}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_zcot.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_zcot) \
        .saveAsTable(f"{database_name}.{table_name_target_zcot}")

    print(f"Archivo copiado: {path_source_zcot} -> {path_target_zcot}")

except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_zcot}. Error: {e}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")    
job.commit()