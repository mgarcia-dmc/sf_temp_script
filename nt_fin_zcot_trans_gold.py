from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_zcot_trans_gold"

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
## Parametros Globales 
db_name = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

path_lista = f"s3://ue1stg{p_amb}as3dtl001-landing/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/listatablas_zcot.txt"
lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
database_name = f"{db_name}"

# Recorrer los registros obtenidos con collect()
for row in lista_df.collect():
    table_name_source = f"si_{row[0]}".lower()
    table_name_target = f"{row[1]}".lower()
    path_source = f"s3://ue1stg{p_amb}as3dtl005-silver/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/{table_name_source}"
    path_target = f"s3://ue1stg{p_amb}as3dtl005-gold/UE1STG{p_ambU}AS3FIN001/SAP/ZCOT/{table_name_target}"
    
    # Leer archivos de origen
    try:
        df_source = spark.read.parquet(path_source)
    except Exception as e:
        print(f"No se encontraron archivos para la tabla {table_name_source}. Error: {e}")
        continue
    
    # Definir filtros
    #filtersZCOT = (
    #((col("BWART") == "101") | (col("BWART") == "102")) 
    #(col("SOCIEDAD") == "SFER") &  ##FALTA AGREGAR SOCIEDAD
    #)   
    
    df_filtered = df_source #.filter(filtersZCOT)
        
    # Contar registros y finalizar
    record_count = df_filtered.count()
    print(f"Registros procesados: {record_count}")
    
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_filtered.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target) \
        .saveAsTable(f"{database_name}.{table_name_target}")
    
    print(f"Archivo copiado: {path_source} -> {path_target}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()