#import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from pyspark.sql.functions import col


# Definir el nombre del trabajo
JOB_NAME = "nt_fin_gasto_trans_bronze_hist"

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
db_name = ssm.get_parameter(Name='db_fin_bronce', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

landing_bucket_name = f"ue1stg{p_amb}as3dtl001-landing"
bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"

table_name_source_glpca = "la_glpca"
table_name_target_glpca = "br_glpca"
path_source_glpca = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS_HISTORICO/GLPCA_HIST/{table_name_source_glpca}_*.parquet"
path_target_glpca = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpca}/"

table_name_source_glpcp = "la_glpcp"
table_name_target_glpcp = "br_glpcp"
path_source_glpcp = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS_HISTORICO/GLPCP_HIST/{table_name_source_glpcp}_*.parquet"
path_target_glpcp = f"s3://{bronze_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpcp}/"

try:
    # Leer archivos de origen
    df_source_glpca = spark.read.parquet(path_source_glpca)
    # Copiar destino final
    df_source_glpca.write.format("parquet").mode("overwrite").option("path", path_target_glpca).saveAsTable(f"{database_name}.{table_name_target_glpca}")
    # Contar registros y finalizar
    record_count_glpca = df_source_glpca.count()
    print(f"Registros initial: {record_count_glpca}")
    print(f"Tabla {table_name_target_glpca} procesada.")
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_glpca}. Error: {e}")
#    continue

#print(path_source)
try:
    # Leer archivos de origen
    df_source_glpcp = spark.read.parquet(path_source_glpcp)
    # Copiar destino final
    df_source_glpcp.write.format("parquet").mode("overwrite").option("path", path_target_glpcp).saveAsTable(f"{database_name}.{table_name_target_glpcp}")
    # Contar registros y finalizar
    record_count_glpcp = df_source_glpcp.count()
    print(f"Registros initial: {record_count_glpcp}")

    print(f"Tabla {table_name_target_glpcp} procesada.")
except Exception as e:
    print(f"No se encontraron archivos para la tabla {table_name_source_glpcp}. Error: {e}")
#    continue
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()