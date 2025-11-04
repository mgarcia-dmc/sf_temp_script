from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_gastos_trans_silver_hist"

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
db_name = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"
silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"

table_name_source_glpca = "br_glpca"
table_name_target_glpca = "si_glpca"
path_source_glpca = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_source_glpca}"
path_target_glpca = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpca}/"

table_name_source_glpcp = "br_glpcp"
table_name_target_glpcp = "si_glpcp"
path_source_glpcp = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_source_glpcp}"
path_target_glpcp = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpcp}"

try:
    df_source_glpca = spark.read.parquet(path_source_glpca)
    # Contar registros y finalizar
    record_count_glpca = df_source_glpca.count()
    print(f"Registros procesados: {record_count_glpca}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_glpca.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_glpca) \
        .saveAsTable(f"{database_name}.{table_name_target_glpca}")

    print(f"Archivo copiado: {path_source_glpca} -> {path_target_glpca}")        
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_glpca}. Error: {e}")

try:
    df_source_glpcp = spark.read.parquet(path_source_glpcp)
    # Contar registros y finalizar
    record_count_glpcp = df_source_glpcp.count()
    print(f"Registros procesados: {record_count_glpcp}")
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source_glpcp.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_glpcp) \
        .saveAsTable(f"{database_name}.{table_name_target_glpcp}")

    print(f"Archivo copiado: {path_source_glpcp} -> {path_target_glpcp}")        
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_glpcp}. Error: {e}")

job.commit()  # Llama a commit al final del trabajo
print("termina notebook")    
job.commit()