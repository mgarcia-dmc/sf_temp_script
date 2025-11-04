from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_all_ma_bronze_hist"

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
# Configuración inicial
db_name = ssm.get_parameter(Name='db_fin_bronce', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

landing_bucket_name = f"ue1stg{p_amb}as3dtl001-landing"
bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"
path_lista = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/listatablas.txt"
lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
#lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
#pk_columns = [col for col in lista_df.columns if col.startswith("PK")]

# Procesar cada tabla en la lista
for row in lista_df.collect():
    table_name_source = f"la_{row[0]}".lower()
    table_name_target = f"br_{row[0]}".lower()
    #pk_col = [row[col] for col in pk_columns if row[col] is not None]
    path_source = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS_HISTORICO/{table_name_source}_*.parquet"
    prefix_target = f"UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/{table_name_target}/"
    path_target = f"s3://{bronze_bucket_name}/{prefix_target}"
    
    # Leer archivos de origen
    try:
        df_source = spark.read.parquet(path_source)
    except Exception as e:
        print(f"No se encontraron archivos para la tabla {table_name_source}. Error: {e}")
        continue

    # Carga destino final
    df_source.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{database_name}.{table_name_target}")

    # Contar registros y finalizar
    record_count = df_source.count()
    print(f"Registros Initial: {record_count}")
        
    print(f"Tabla {table_name_target} procesada y archivos movidos a la carpeta de procesados.")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()