# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job

# Obtén el contexto de Spark activo proporcionado por Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Define el nombre del trabajo
JOB_NAME = "nt_prd_ca_bronze"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
#from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, current_timestamp

# Crear una sesión de Spark
#spark = SparkSession.builder.appName("CargaMaestrosSF").getOrCreate()
print("inicia spark")
database_name_br = "mtech_prd_sf_br"
# Parámetros de entrada global
bucket_name_target = "ue1stgprodas3dtl005-bronze"
bucket_name_source = "ue1stgprodas3dtl001-landing"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/"
path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}maestrosSF.txt"
listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
# Recorrer los registros obtenidos con collect()
for row in listat_df.collect():
    
    table_name = f"la_{row[0]}".lower()
    table_name2 = f"br_{row[0]}".lower()
    #awtyp_list = [row['PK1'],row['PK2'],row['PK3'],row['PK4'],row['PK5']]
    #join = " and ".join([f"target.{col} = source.{col}" for col in awtyp_list if col is not None])
    print(table_name2)

    #table_name = "la_ttyp"
    folder_name = f"{bucket_name_prdmtech}{table_name2}"
    file_name = f"{bucket_name_prdmtech}{table_name}.parquet"
    path_target = f"s3://{bucket_name_target}/{file_name}"
    path_source = f"s3://{bucket_name_source}/{file_name}"

    
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
        .saveAsTable(f"{database_name_br}.{table_name2}")
    
    print(f"Crea archivo {bucket_name_source}/{bucket_name_prdmtech}{table_name}")
    print(f"Crea archivo {bucket_name_target}/{bucket_name_prdmtech}{table_name2}")
    print(f"Cantidad de registros :  {cant}")
# Parámetros de entrada global - Cargando tablas SAP
bucket_name_target = "ue1stgprodas3dtl005-bronze"
bucket_name_source = "ue1stgprodas3dtl001-landing"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/SAP/MAESTROS/"
path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}maestrosSAPSF.txt"
listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
# Recorrer los registros obtenidos con collect()
for row in listat_df.collect():
    
    table_name = f"la_{row[0]}".lower()
    table_name2 = f"br_{row[0]}".lower()
    #awtyp_list = [row['PK1'],row['PK2'],row['PK3'],row['PK4'],row['PK5']]
    #join = " and ".join([f"target.{col} = source.{col}" for col in awtyp_list if col is not None])
    print(table_name2)

    #table_name = "la_ttyp"
    folder_name = f"{bucket_name_prdmtech}{table_name2}"
    file_name = f"{bucket_name_prdmtech}{table_name}.parquet"
    path_target = f"s3://{bucket_name_target}/{file_name}"
    path_source = f"s3://{bucket_name_source}/{file_name}"

    
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
        .saveAsTable(f"{database_name_br}.{table_name2}")
    
    print(f"Crea archivo {bucket_name_source}/{bucket_name_prdmtech}{table_name}")
    print(f"Crea archivo {bucket_name_target}/{bucket_name_prdmtech}{table_name2}")
    print(f"Cantidad de registros :  {cant}")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
#%stop_session
print("termina notebook")
job.commit()