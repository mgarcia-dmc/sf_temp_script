from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_copa_trans_silver_hist"

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

# Variables K810001
table_name_source_k81 = "br_k810001"
table_name_target_k81 = "si_k810001"
path_source_k81 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_source_k81}"
path_target_k81 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_target_k81}"


# Variables K810002
table_name_source_k82 = "br_k810002"
table_name_target_k82 = "si_k810002"
path_source_k82 = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_source_k82}"
path_target_k82 = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/COPA/{table_name_target_k82}"

    
# Leer archivos de origen
try:
    df_source_k81 = spark.read.parquet(path_source_k81)
    # Contar registros y finalizar
    record_count_k81 = df_source_k81.count()
    print(f"Registros procesados: {record_count_k81}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_k81.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_k81) \
        .saveAsTable(f"{database_name}.{table_name_target_k81}")

    print(f"Archivo copiado: {path_source_k81} -> {path_target_k81}")        

except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_k81}. Error: {e}")
## Carga archivo hacia Silver - COPA K810001 Histórico
# Leer archivos de origen
try:
    df_source_k82 = spark.read.parquet(path_source_k82)
    # Contar registros y finalizar
    record_count_k82 = df_source_k82.count()
    print(f"Registros procesados: {record_count_k82}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_k82.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_k82) \
        .saveAsTable(f"{database_name}.{table_name_target_k82}")

    print(f"Archivo copiado: {path_source_k82} -> {path_target_k82}")        

except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_k82}. Error: {e}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")    
job.commit()