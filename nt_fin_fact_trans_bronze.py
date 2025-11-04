from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_fact_trans_bronze"

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

# Configuración inicial
db_name = ssm.get_parameter(Name='db_fin_bronce', WithDecryption=True)['Parameter']['Value']
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

# Configuración inicial
path_lista = f"s3://ue1stg{p_amb}as3dtl001-landing/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/listatablas_facturacion.txt"
lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
database_name = f"{db_name}"
pk_columns = [col for col in lista_df.columns if col.startswith("PK")]

# Boto3 setup
s3 = boto3.resource('s3')
landing_bucket_name = f"ue1stg{p_amb}as3dtl001-landing"
processed_prefix = f"UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION_PROCESADOS/"
bronze_bucket_name = f"ue1stg{p_amb}as3dtl005-bronze"

# Procesar cada tabla en la lista
for row in lista_df.collect():
    table_name_source = f"la_{row[0]}".lower()
    table_name_target = f"br_{row[0]}".lower()
    pk_col = [row[col] for col in pk_columns if row[col] is not None]
    prefix_source = f"UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/"
    path_source = f"s3://{landing_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_source}_*.parquet"
    prefix_target = f"UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target}/"
    prefix_target_temp = f"UE1STG{p_ambU}AS3FIN001/SAP/FACTURACION/{table_name_target}_temp/"
    path_target = f"s3://{bronze_bucket_name}/{prefix_target}"
    temp_target_path = f"s3://{bronze_bucket_name}/{prefix_target_temp}"
    
    # Leer archivos de origen
    try:
        df_source = spark.read.parquet(path_source)
    except Exception as e:
        print(f"No se encontraron archivos para la tabla {table_name_source}. Error: {e}")
        continue

    # Leer historial o inicializar si no existe
    try:
        df_target = spark.read.parquet(path_target)
    except Exception:
        df_target = None  # Si no existe, inicializar como None

    # Filtrar registros según OPFLAG
    new_records = df_source.filter(col("OPFLAG") == "I")
    updated_records = df_source.filter(col("OPFLAG") == "U")
    deleted_records = df_source.filter(col("OPFLAG") == "D")
    initial_load_records = df_source.filter((col("OPFLAG") == "") | col("OPFLAG").isNull())

    if df_target is not None:
        # Procesar eliminaciones y actualizaciones
        df_target_after_deletions = df_target.join(deleted_records.union(updated_records), on=pk_col, how="left_anti")
        new_records = new_records.drop("OPFLAG")  # No es necesario mantener OPFLAG para la historia
        updated_records = updated_records.drop("OPFLAG")  # No es necesario mantener OPFLAG para la historia
        df_final = df_target_after_deletions.union(new_records).union(updated_records)
    else:
        # Primera carga
        df_final = initial_load_records

    df_final = df_final.drop("OPFLAG")

    # Guardar en carpeta temporal
    df_final.write.mode("overwrite").parquet(temp_target_path)
    
    print(path_source)
    
    print("Registros initial:", initial_load_records.count())
    print("Registros new:", new_records.count())
    print("Registros update:", updated_records.count())
    print("Registros delete:", deleted_records.count())

    # Eliminar carpeta destino en S3
    bronze_bucket = s3.Bucket(bronze_bucket_name)
    bronze_bucket.objects.filter(Prefix=prefix_target).delete()

    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(temp_target_path)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{database_name}.{table_name_target}")

    # Contar registros y finalizar
    record_count = df_temp.count()
    print(f"Registros procesados: {record_count}")
    
    # Mover archivos procesados
    landing_bucket = s3.Bucket(landing_bucket_name)
    for obj in landing_bucket.objects.filter(Prefix=prefix_source):
        if obj.key.startswith(prefix_source + f"{table_name_source}_"):
            new_key = processed_prefix + obj.key.split("/")[-1]
            # Copiar el archivo
            copy_source = {'Bucket': landing_bucket_name, 'Key': obj.key}
            landing_bucket.copy(copy_source, new_key)
            print(f"Archivo copiado: {obj.key} -> {new_key}")
            # Elimina archivo procesado
            obj.delete()
            print(f"Archivo eliminado: {obj.key}")
        
    # Eliminar carpeta temporal
    bronze_bucket.objects.filter(Prefix=prefix_target_temp).delete()

    print(f"Tabla {table_name_target} procesada y archivos movidos a la carpeta de procesados.")
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()