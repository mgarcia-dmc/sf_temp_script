#import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from pyspark.sql.functions import col


# Definir el nombre del trabajo
JOB_NAME = "nt_fin_fact_trans_bronze_hist"

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

# Variables vbrk
table_name_source_1 = "la_vbrk"
table_name_target_1 = "br_vbrk"
path_source_1 = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION_HISTORICO/{table_name_source_1}_*.parquet"
path_target_1 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target_1}/"

# Variables vbrp
table_name_source_2 = "la_vbrp"
table_name_target_2 = "br_vbrp"
path_source_2 = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION_HISTORICO/{table_name_source_2}_*.parquet"
path_target_2 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target_2}/"
try:
    # Leer archivos de origen
    df_source_1 = spark.read.parquet(path_source_1)
    # Copiar destino final
    df_source_1.write.format("parquet").mode("overwrite").option("path", path_target_1).saveAsTable(f"{database_name}.{table_name_target_1}")
    # Contar registros y finalizar
    record_count_1 = df_source_1.count()
    print(f"Registros initial: {record_count_1}")
    print(f"Tabla {table_name_target_1} procesada.")
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_1}. Error: {e}")
#    continue

#print(path_source)
try:
    # Leer archivos de origen
    df_source_2 = spark.read.parquet(path_source_2)
    # Copiar destino final
    df_source_2.write.format("parquet").mode("overwrite").option("path", path_target_2).saveAsTable(f"{database_name}.{table_name_target_2}")
    # Contar registros y finalizar
    record_count_2 = df_source_2.count()
    print(f"Registros initial: {record_count_2}")

    print(f"Tabla {table_name_target_2} procesada.")
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_2}. Error: {e}")
#    continue
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()