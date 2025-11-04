from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_fact_trans_silver_hist"

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

table_name_source_1 = "br_vbrk"
table_name_target_1 = "si_vbrk"
path_source_1 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_source_1}"
path_target_1 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target_1}/"

table_name_source_2 = "br_vbrp"
table_name_target_2 = "si_vbrp"
path_source_2 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_source_2}"
path_target_2 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target_2}"

try:
    df_source_1 = spark.read.parquet(path_source_1)
    # Contar registros y finalizar
    record_count_1 = df_source_1.count()
    print(f"Registros procesados: {record_count_1}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_1.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_1) \
        .saveAsTable(f"{database_name}.{table_name_target_1}")

    print(f"Archivo copiado: {path_source_1} -> {path_target_1}")        
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_1}. Error: {e}")

try:
    df_source_2 = spark.read.parquet(path_source_2)
    # Contar registros y finalizar
    record_count_2 = df_source_2.count()
    print(f"Registros procesados: {record_count_2}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_2.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_2) \
        .saveAsTable(f"{database_name}.{table_name_target_2}")

    print(f"Archivo copiado: {path_source_2} -> {path_target_2}")        
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_2}. Error: {e}")

job.commit()  # Llama a commit al final del trabajo
print("termina notebook")    
job.commit()