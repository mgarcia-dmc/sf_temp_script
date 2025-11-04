from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_all_ma_silver"

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

## Carga archivo hacia Bronze
db_name = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

path_lista = f"s3://ue1stg{p_amb}as3dtl001-landing/UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/listatablas.txt"
lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
database_name = f"{db_name}"

# Recorrer los registros obtenidos con collect()
for row in lista_df.collect():
    table_name_source = f"br_{row[0]}".lower()
    table_name_target = f"si_{row[0]}".lower()
    prefix_target = f"UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/{table_name_target}/"
    path_target = f"s3://ue1stg{p_amb}as3dtl005-silver/UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/{table_name_target}"
    path_source = f"s3://ue1stg{p_amb}as3dtl005-bronze/UE1STG{p_ambU}AS3FIN001/SAP/MAESTROS/{table_name_source}"
    
    # Leer datos de entrada y del historial
    df_source = spark.read.parquet(path_source)
    
    # Contar registros y finalizar
    record_count = df_source.count()
    print(f"Registros procesados: {record_count}")
    
    # Guardar los datos en una carpeta temporal y mover a destino final
        # Guardar datos en formato Parquet y registrar en Data Catalog
    df_source.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target) \
        .saveAsTable(f"{database_name}.{table_name_target}")
    
    print(f"Archivo copiado: {path_source} -> {path_target}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()