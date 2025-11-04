from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_zcot_trans_bronze_hist"

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
# Variables Generales
db_name = ssm.get_parameter(Name='db_fin_bronce', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

landing_bucket_name = f"ue1stg{p_amb}as3dtl001-landing"
bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"

# Variables YBWT_MDOC
table_name_source_zc26 = "la_zcot_0026"
table_name_target_zc26 = "br_zcot_0026"
path_source_zc26 = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT_HISTORICO/ZCOT_0026_HIST/{table_name_source_zc26}_*.parquet"
path_target_zc26 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/{table_name_target_zc26}/"
# Leer archivos de origen
try:
    df_source_zc26 = spark.read.parquet(path_source_zc26)
    # Copiar destino final
    df_source_zc26.write.format("parquet").mode("overwrite").option("path", path_target_zc26).saveAsTable(f"{database_name}.{table_name_target_zc26}")
    # Contar registros y finalizar
    record_count_zc26 = df_source_zc26.count()
    print(f"Registros initial: {record_count_zc26}")
    print(f"Tabla {table_name_target_zc26} procesada.")
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_zc26}. Error: {e}")

job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()