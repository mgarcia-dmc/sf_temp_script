# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_bronce"

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
#spark = SparkSession.builder.appName("CargaTransaccionalesSF").getOrCreate()

print("inicia spark")
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
#import delta
import pyspark.sql.functions as F
from pyspark.sql.functions import *
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-bronze"
bucket_name_source = "ue1stgtestas3dtl001-landing"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}transaccionalesSF.txt"
listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)

# Recorrer los registros obtenidos con collect()
for row in listat_df.collect():
    
    table_name  = f"la_{row[0]}".lower()
    table_name2 = f"br_{row[0]}".lower()
    table_name3 = f"br_{row[0]}temporal".lower()
    campo_fecha = f"{row[1]}"
    print(table_name)
    print(campo_fecha)
    
    folder_name = f"{bucket_name_prdmtech}{table_name2}"
    file_name = f"{bucket_name_prdmtech}{table_name}.parquet"
    path_target = f"s3://{bucket_name_target}/{folder_name}"
    path_source = f"s3://{bucket_name_source}/{file_name}"
    
    print(f"Procesando landing : {table_name}")
    print(f"Procesando bronce : {table_name2}")
    print(f"Procesando tabla temporal : {table_name3}")
    
    
    try:
        # Verificar si la tabla gold ya existe
        #df_nuevos = spark.read.format("parquet").load(path_source)
        #gold_table = spark.read.format("parquet").load(path_target)     
        try:
            df_existentes = spark.read.format("parquet").load(path_target)
            datos_existentes = True
            logger.info(f"Datos existentes de {table_name2} cargados: {df_existentes.count()} registros")
        except:
            datos_existentes = False
            logger.info(f"No se encontraron datos existentes en {table_name2}")
            
            
        
        if datos_existentes:
            df_nuevos = spark.read.format("parquet").load(path_source)
            
            if (campo_fecha != "None"):
                
                existing_data = spark.read.format("parquet").load(path_target)
                data_after_delete = existing_data.filter(~((date_format(F.col(f"{campo_fecha}"),"yyyyMM") >= AnioMes) & (date_format(F.col(f"{campo_fecha}"),"yyyyMM") <= AnioMesFin)))
                filtered_new_data = df_nuevos.filter((date_format(F.col(f"{campo_fecha}"),"yyyyMM") >= AnioMes) & (date_format(F.col(f"{campo_fecha}"),"yyyyMM") <= AnioMesFin))
                final_data = filtered_new_data.union(data_after_delete)                             
               
                cant_ingresonuevo = filtered_new_data.count()
                cant_total = final_data.count()
                cant_inicio = existing_data.count()
                # Escribir los resultados en ruta temporal
                additional_options = {
                    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}"
                }
                final_data.write \
                    .format("parquet") \
                    .options(**additional_options) \
                    .mode("overwrite") \
                    .saveAsTable(f"{database_name}.{table_name3}")
                
                
                #schema = existing_data.schema
                final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}")
                        
                        
                additional_options = {
                    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
                }
                final_data2.write \
                    .format("parquet") \
                    .options(**additional_options) \
                    .mode("overwrite") \
                    .saveAsTable(f"{database_name}.{table_name2}")
                        
                print(f"agrega registros nuevos a la tabla {table_name2} : {cant_ingresonuevo}")
                print(f"Total de registros en la tabla {table_name2} : {cant_total}")
                print(f"Total de registros en un inicio en la tabla {table_name2} : {cant_inicio}")
                 #Limpia la ubicación temporal
                glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}", {"retentionPeriod": 0})
                #glueContext.purge_table("default", "ft_mortalidadtemporal", {"retentionPeriod": 0})
                glue_client.delete_table(DatabaseName=database_name, Name=table_name3)
                print(f"Tabla {table_name3} eliminada correctamente de la base de datos '{database_name}'.")
            
            else:
                final_data = df_nuevos
                
                #existing_data = spark.read.format("parquet").load(path_target)
                
                # Escribir los resultados en ruta temporal
                additional_options = {
                    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}"
                }
                final_data.write \
                    .format("parquet") \
                    .options(**additional_options) \
                    .mode("overwrite") \
                    .saveAsTable(f"{database_name}.{table_name3}")
                
                
                #schema = existing_data.schema
                final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}")
                
                additional_options = {
                    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
                }
                final_data2.write \
                    .format("parquet") \
                    .options(**additional_options) \
                    .mode("overwrite") \
                    .saveAsTable(f"{database_name}.{table_name2}")
                
                
                
                cant_ingresonuevo = df_nuevos.count()
                cant_total = df_nuevos.count()
                #cant_inicio = existing_data.count()
                
                print(f"agrega registros nuevos a la tabla {table_name2} : {cant_ingresonuevo}")
                print(f"Total de registros en la tabla {table_name2} : {cant_total}")
                #print(f"Total de registros en un inicio en la tabla {table_name2} : {cant_inicio}")
                 #Limpia la ubicación temporal
                glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/{table_name3}", {"retentionPeriod": 0})
                #glueContext.purge_table("default", "ft_mortalidadtemporal", {"retentionPeriod": 0})
                glue_client.delete_table(DatabaseName=database_name, Name=table_name3)
                print(f"Tabla {table_name3} eliminada correctamente de la base de datos '{database_name}'.")
                
        else:
            df_nuevos = spark.read.format("parquet").load(path_source)
            
            additional_options = {
                "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
            }
            df_nuevos.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{database_name}.{table_name2}")
            
    except Exception as e:
        error_msg = f"Error en el proceso: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
#import delta
#import pyspark.sql.functions as F
#from pyspark.sql.functions import *
## Parámetros de entrada global
#bucket_name_target = "ue1stgdesaas3dtl005-bronze"
#bucket_name_source = "ue1stgdesaas3dtl001-landing"
#bucket_name_prdmtech = "UE1STGDESAAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
#path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}transaccionalesSF.txt"
#listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
## Recorrer los registros obtenidos con collect()
#for row in listat_df.collect():
#    
#    table_name  = f"la_{row[0]}".lower()
#    table_name2 = f"br_{row[0]}".lower()
#    campo_fecha = f"{row[1]}"
#    print(table_name)
#    print(campo_fecha)
#    
#    folder_name = f"{bucket_name_prdmtech}{table_name2}"
#    file_name = f"{bucket_name_prdmtech}{table_name}.parquet"
#    path_target = f"s3://{bucket_name_target}/{folder_name}"
#    path_source = f"s3://{bucket_name_source}/{file_name}"
#    
#    
#    try:
#        # Cargar los nuevos datos
#        df_nuevos = spark.read.format("parquet").load(path_source)
#        logger.info(f"Datos nuevos cargados: {df_nuevos.count()} registros")
#
#       
#        df_final = df_nuevos
#    
#    
#            
#        additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
#        }
#        df_final.write \
#            .format("parquet") \
#            .options(**additional_options) \
#            .mode("overwrite") \
#            .saveAsTable(f"default.{table_name2}")
#        
#            #Limpia la ubicación temporal
#            #glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/", {"retentionPeriod": 0})
#        
#             # Validaciones finales
#        total_registros = df_final.count()
#        logger.info(f"Proceso completado. Total de registros finales: {total_registros}")
#        
#             # Mostrar algunos ejemplos del resultado
#        print(f"Muestra de los datos procesados: {total_registros}")
#           
#
#    except Exception as e:
#        error_msg = f"Error en el proceso: {str(e)}"
#        logger.error(error_msg)
#        raise Exception(error_msg)
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()