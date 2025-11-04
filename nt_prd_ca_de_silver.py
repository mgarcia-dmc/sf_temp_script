# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job

# Obtén el contexto de Spark activo proporcionado por Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Define el nombre del trabajo
JOB_NAME = "nt_prd_ca_de_silver"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
#from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp

# Crear una sesión de Spark
#spark = SparkSession.builder.appName("CargaMaestrosSF").getOrCreate()
print("inicia spark")
# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")

amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
database_name_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']
bucket_name_target = f"ue1stg{amb}as3dtl005-silver"
bucket_name_source = f"ue1stg{amb}as3dtl005-bronze"
bucket_name_prdmtech = f"UE1STG{amb.upper()}AS3PRD001/DE/"
path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}maestrosdeSF.txt"
listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
# Recorrer los registros obtenidos con collect()
for row in listat_df.collect():
    
    table_name = f"br_{row[0]}".lower()
    table_name2 = f"si_{row[0]}".lower()
    #awtyp_list = [row['PK1'],row['PK2'],row['PK3'],row['PK4'],row['PK5']]
    #join = " and ".join([f"target.{col} = source.{col}" for col in awtyp_list if col is not None])
    print(table_name)

    #table_name = "la_ttyp"
    #folder_name = f"{bucket_name_prdmtech}{table_name2}"
    file_name = f"{bucket_name_prdmtech}{table_name}"
    file_name2 = f"{bucket_name_prdmtech}{table_name2}"
    path_target = f"s3://{bucket_name_target}/{file_name2}"
    path_source = f"s3://{bucket_name_source}/{file_name}/"
    print(path_target)
    
    df = spark.read.parquet(path_source)
    cant = df.count()
    #Guardamos tabla Delta Bronce
    additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
    }
    df.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_si}.{table_name2}")
        
    
    print(f"extrae datos del archivo {bucket_name_source}/{bucket_name_prdmtech}{table_name}")
    print(f"Crea archivo {bucket_name_target}/{bucket_name_prdmtech}{table_name2}")
    print(f"Cantidad de registros  {cant}")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()