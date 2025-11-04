from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_huevo_trans_gold"

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
db_name = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"
gold_bucket_name = f"ue1stg{p_amb}as3dtl005-gold"

table_name_source_huevo = "si_ybwt_mdoc"
table_name_target_huevo = "huevo"
path_source_huevo = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/HUEVO/{table_name_source_huevo}"
path_target_huevo = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/HUEVO/{table_name_target_huevo}"
# Leer archivos de origen
try:
    df_source_huevo = spark.read.parquet(path_source_huevo)

    # Definir filtros
    filtersHUEVO = (
    ((col("BWART") == "101") | (col("BWART") == "102")) 
    #(col("SOCIEDAD") == "SFER") &  ##FALTA AGREGAR SOCIEDAD
    )

    # Aplicar filtros y seleccionar columnas
    df_filtered_huevo = df_source_huevo.filter(filtersHUEVO)

    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_filtered_huevo.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_huevo) \
        .saveAsTable(f"{database_name}.{table_name_target_huevo}")

    # Contar registros y finalizar
    record_count_huevo = df_filtered_huevo.count()
    print(f"Registros procesados: {record_count_huevo}")

    print(f"Archivo copiado: {path_source_huevo} -> {path_target_huevo}")

except Exception as e:
    print(f"No se encontraron archivos para la tabla {path_source_huevo}. Error: {e}")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()