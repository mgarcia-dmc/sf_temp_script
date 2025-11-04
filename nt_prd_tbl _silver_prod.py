# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job

# Obtén el contexto de Spark activo proporcionado por Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_silver"

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
#AnioMes="202501"
#AnioMesFin=AnioMes
database_name_br ="mtech_prd_sf_br"
database_name_si ="mtech_prd_sf_si"
database_name_tmp ="bi_sf_tmp"

print('carga de parámetros')
#import delta
#import pyspark.sql.functions as F
#from pyspark.sql.functions import *
## Parámetros de entrada global
#bucket_name_target = "ue1stgdesaas3dtl005-silver"
#bucket_name_source = "ue1stgdesaas3dtl005-bronze"
#bucket_name_prdmtech = "UE1STGDESAAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
#path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}transaccionalesSF.txt"
#listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
## Recorrer los registros obtenidos con collect()
#for row in listat_df.collect():
#    
#    table_name = f"br_{row[0]}".lower()
#    table_name2 = f"si_{row[0]}".lower()
#    campo_fecha = f"{row[1]}"
#    print(table_name)
#    print(campo_fecha)
#
#    #table_name = "la_ttyp"
#    folder_name = f"{bucket_name_prdmtech}{table_name2}"
#    file_name = f"{bucket_name_prdmtech}{table_name}"
#    path_target = f"s3://{bucket_name_target}/{folder_name}"
#    path_source = f"s3://{bucket_name_source}/{file_name}"
#    
#    try:
#        # Cargar los nuevos datos
#        #df_nuevos = spark.read.format("parquet").load(path_source)
#        #logger.info(f"Datos nuevos cargados: {df_nuevos.count()} registros")
#
#        # Cargar datos existentes si existen
#        try:
#            df_existentes = spark.read.format("parquet").load(path_target)
#            datos_existentes = True
#            logger.info(f"Datos existentes cargados: {df_existentes.count()} registros")
#        except:
#            datos_existentes = False
#            logger.info("No se encontraron datos existentes")
#
#            
#        if datos_existentes:
#            
#            if campo_fecha is None
#                df_final = spark.read.format("parquet").load(path_source)
#            else
#                # Eliminar registros de la fecha específica
#                df_filtrado = df_existentes.filter(date_format(F.col(campo_fecha),"yyyy-MM") != "2024-12")
#                logger.info(f"Registros después de filtrar fecha {campo_fecha}: {df_filtrado.count()}")
#                print(f"Registros de filtrado {df_filtrado.count()}")
#
#                # Unir los datos filtrados con los nuevos
#                df_final = df_filtrado.union(df_nuevos.filter(date_format(F.col(campo_fecha),"yyyy-MM") == "2024-12"))
#               
#            
#            print(f"Registros de final {df_final.count()}")
#        else:
#            # Si no hay datos existentes, usar solo los nuevos
#            df_final = df_nuevos
#    
#    
#            # Escribir los resultados en ruta temporal
#            additional_options = {
#            "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/"
#            }
#            df_final.write \
#                .format("parquet") \
#                .options(**additional_options) \
#                .mode("overwrite")
#               
#            df_final2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/")
#            #spark.read.parquet(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/")
#            
#            # Escribir los resultados
#            additional_options = {
#            "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
#            }
#            df_final2.write \
#                .format("parquet") \
#                .options(**additional_options) \
#                .mode("overwrite") \
#                .saveAsTable(f"default.{table_name2}")
#        
#            #Limpia la ubicación temporal
#            glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/", {"retentionPeriod": 0})
#        
#             # Validaciones finales
#            total_registros = df_final.count()
#            logger.info(f"Proceso completado. Total de registros finales: {total_registros}")
#        
#             # Mostrar algunos ejemplos del resultado
#            print(f"Muestra de los datos procesados: {total_registros}")
#           
#
#    except Exception as e:
#        error_msg = f"Error en el proceso: {str(e)}"
#        logger.error(error_msg)
#        raise Exception(error_msg)
#spark.stop() 
#job.commit()  # Llama a commit al final del trabajo
#print("termina notebook")
#import delta
import pyspark.sql.functions as F
from pyspark.sql.functions import *
# Parámetros de entrada global
bucket_name_target = "ue1stgprodas3dtl005-silver"
bucket_name_source = "ue1stgprodas3dtl005-bronze"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
path_listat = f"s3://{bucket_name_source}/{bucket_name_prdmtech}transaccionalesSF.txt"
listat_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_listat)
# Recorrer los registros obtenidos con collect()
for row in listat_df.collect():
    
    table_name = f"br_{row[0]}".lower()
    table_name2 = f"si_{row[0]}".lower()
    #campo_fecha = f"{row[1]}"
    print(table_name2)
    #print(campo_fecha)

    #table_name = "la_ttyp"
    folder_name = f"{bucket_name_prdmtech}{table_name2}"
    file_name = f"{bucket_name_prdmtech}{table_name}"
    path_target = f"s3://{bucket_name_target}/{folder_name}"
    path_source = f"s3://{bucket_name_source}/{file_name}"
    
    try:
        # Cargar los nuevos datos
        df_nuevos = spark.read.format("parquet").load(path_source)
        logger.info(f"Datos nuevos cargados: {df_nuevos.count()} registros")

     
        df_final = df_nuevos
    
        # Escribir los resultados
        additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
        }
        df_final.write \
            .format("parquet") \
            .options(**additional_options) \
            .mode("overwrite") \
            .saveAsTable(f"{database_name_si}.{table_name2}")
        
            #Limpia la ubicación temporal
            #glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/", {"retentionPeriod": 0})
        
        # Validaciones finales
        total_registros = df_final.count()
        logger.info(f"Proceso completado. Total de registros finales: {total_registros}")
        
        # Mostrar algunos ejemplos del resultado
        print(f"Tabla {table_name2} de los datos procesados: {total_registros}")
           

    except Exception as e:
        error_msg = f"Error en el proceso: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()