# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_upd_ca_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/"
database_name = "default"
print('cargando rutas')
table_name = "lk_producto"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    logger.info(f"Datos existentes de {table_name} cargados: {existing_data.count()} registros")
    
    # Definir las listas de valores para las condiciones WHERE
    empaquetados = ['47665', '47677', '47690', '47704', '58089', '47719', '47724', 'PVHE (CD)']
    trozados = ['47738', '47753', '57571', '57587', '58052', '58070', '121774', '121775', '121776', '121777', '121778', '121779', '121780', '121781', '58105', '121773', 'PVMA (GH1H2)', 'PVMA (DEF)']

    # Realizar la primera actualización (EMPACADO)
    updated_data_empaquetado = existing_data.withColumn(
        "grupoproducto",
        when(col("cproducto").isin(empaquetados), "EMPACADO").otherwise(col("grupoproducto"))
    ).withColumn(
        "grupoproductoSalida",
        when(col("cproducto").isin(empaquetados), "EMPACADO").otherwise(col("grupoproductoSalida"))
    )

    # Realizar la segunda actualización (TROZADO) sobre el DataFrame ya modificado
    updated_data_final = updated_data_empaquetado.withColumn(
        "grupoproducto",
        when(col("cproducto").isin(trozados), "TROZADO").otherwise(col("grupoproducto"))
    ).withColumn(
        "grupoproductoSalida",
        when(col("cproducto").isin(trozados), "TROZADO").otherwise(col("grupoproductoSalida"))
    )
    
    updated_data_final = updated_data_final.withColumnRenamed("producttype", "pk_grupoconsumo")
    
    df_lk= spark.sql(f"""SELECT
                PP.ProductNo cproducto_B,
                Description nproducto_B,
                CASE PP.ProductNo WHEN 'PO V H' THEN 'VIVO' WHEN 'PO V M'  THEN 'VIVO'ELSE 'BENEFICIO' end grupoproducto,
                COALESCE(PPA.IRN,'NONE') IRNProteinProductsAnimals,
                PP.IRN,
                1 activeflag_B,
                COALESCE(GC.pk_grupoconsumo,12) AS ProductType_B,
                CASE Description WHEN '%BENEF%' THEN 'BENEFICIO' ELSE 'VIVO' END grupoproductosalida,
                1 AS idempresa
            FROM {database_name}.si_ProteinProducts PP
            LEFT JOIN {database_name}.si_ProteinProductsAnimals PPA ON PPA.ProteinProductsIRN = PP.IRN
            LEFT JOIN {database_name}.lk_grupoconsumo GC ON PP.ProductType = GC.cgrupoconsumo and idempresa =1""")
    
    updated_data = updated_data_final.join(
    df_lk,
    updated_data_final["IRN"] == df_lk["IRN"],
    "left"
    ).withColumn(
        "cproducto",
        when((df_lk["cproducto_B"].isNotNull()) & (updated_data_final["idempresa"] == 1), col("cproducto_B")).otherwise(col("cproducto"))
    ).withColumn(
        "nproducto",
        when((df_lk["cproducto_B"].isNotNull()) & (updated_data_final["idempresa"] == 1), col("nproducto_B")).otherwise(col("nproducto"))
    ).withColumn(
        "activeflag",
        when((df_lk["cproducto_B"].isNotNull()) & (updated_data_final["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).withColumn(
        "pk_grupoconsumo",
        when((df_lk["cproducto_B"].isNotNull()) & (updated_data_final["idempresa"] == 1), col("ProductType_B")).otherwise(col("pk_grupoconsumo"))
    ).select(
        col("cproducto"),
        col("nproducto"),
        updated_data_final["grupoproducto"],
        updated_data_final["irnproteinproductsanimals"],
        updated_data_final["irn"],
        col("activeflag"),
        col("pk_grupoconsumo"),
        updated_data_final["grupoproductoSalida"],
        updated_data_final["idempresa"],
        updated_data_final["pk_producto"]
    )
    cant_total = updated_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    updated_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_administrador"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk_administrador= spark.sql(f"""SELECT TechSupervisorNo cadministrador, TechSupervisorname nadministrador_B, IRN AS IRN_B, U_Administrador abrevadministrador_B, FarmType AS tipo_B FROM {database_name}.si_ProteinTechSupervisors""")
    
    updated_data = existing_data.join(
        df_lk_administrador,
        existing_data["cadministrador"] == df_lk_administrador["cadministrador"],
        "left"
    ).withColumn(
        "nadministrador",
        when((df_lk_administrador["cadministrador"].isNotNull()) & (existing_data["idempresa"] == 1), col("nadministrador_B")).otherwise(col("nadministrador"))
    ).withColumn(
        "abrevadministrador",
        when((df_lk_administrador["cadministrador"].isNotNull()) & (existing_data["idempresa"] == 1), col("abrevadministrador_B")).otherwise(col("abrevadministrador"))
    ).withColumn(
        "status",
        when((df_lk_administrador["cadministrador"].isNotNull()) & (existing_data["idempresa"] == 1), col("tipo_B")).otherwise(col("tipo"))
    ).withColumn(
        "IRN",
        when((df_lk_administrador["cadministrador"].isNotNull()) & (existing_data["idempresa"] == 1), col("IRN_B")).otherwise(col("IRN"))
    ).select(
        existing_data["cadministrador"],
        col("nadministrador"),
        col("abrevadministrador"),
        col("tipo"),
        col("IRN"),
        existing_data["idempresa"],
        existing_data["pk_administrador"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal') 
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
except:
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_alimento"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT FeedTypeNo,FeedTypeName,cast(IRN as varchar(50)) IRN,CreationDate FROM {database_name}.si_FmimFeedTypes""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "nalimento",
        when(df_lk["IRN"].isNotNull(), df_lk["FeedTypeName"]).otherwise(existing_data["nalimento"])
    ).select(
        existing_data["calimento"],
        col("nalimento"),
        existing_data["IRN"],
        existing_data["CreationDate"],
        existing_data["idempresa"],
        existing_data["pk_alimento"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_conductor"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT DriverNo cconductor, DriverName nconductor_B, IRN, activeflag activeflag_B FROM {database_name}.si_ProteinDrivers""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cconductor"] == df_lk["cconductor"],
        "left"
    ).withColumn(
        "nconductor",
        when((df_lk["cconductor"].isNotNull()) & (existing_data["idempresa"] == 1), col("nconductor_B")).otherwise(col("nconductor"))
    ).withColumn(
        "activeflag",
        when((df_lk["cconductor"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["cconductor"],
        col("nconductor"),
        existing_data["IRN"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_conductor"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_especie"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT BreedNo,BreedName,SpeciesType,IRN,ProteinBreedCodeTypesIRN,ActiveFlag activeflag_B FROM {database_name}.si_ProteinBreedCodes""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cespecie"] == df_lk["BreedNo"],
        "left"
    ).withColumn(
        "nespecie",
        when((df_lk["BreedNo"].isNotNull()) & (existing_data["idempresa"] == 1), col("BreedName")).otherwise(col("nespecie"))
    ).withColumn(
        "tipo",
        when((df_lk["BreedNo"].isNotNull()) & (existing_data["idempresa"] == 1), col("SpeciesType")).otherwise(col("tipo"))
    ).withColumn(
        "activeflag",
        when((df_lk["BreedNo"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["cespecie"],
        col("nespecie"),
        col("tipo"),
        existing_data["IRN"],
        existing_data["proteinbreedcodetypesirn"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_especie"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_formula"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT MFF.FormulaNo,MFF.FormulaName,MFF.IRN, LA.pk_alimento,MFF.ProteinProductsIRN,ActiveFlag activeflag_B FROM {database_name}.si_mvFmimFeedFormulas MFF LEFT JOIN {database_name}.lk_alimento LA ON LA.IRN = MFF.FmimFeedTypesIRN""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cformula"] == df_lk["FormulaNo"],
        "left"
    ).withColumn(
        "nformula",
        when((df_lk["FormulaNo"].isNotNull()) & (existing_data["idempresa"] == 1), col("FormulaName")).otherwise(col("nformula"))
    ).withColumn(
        "activeflag",
        when((df_lk["FormulaNo"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["cformula"],
        col("nformula"),
        col("IRN"),
        existing_data["pk_alimento"],
        existing_data["proteinproductsirn"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_formula"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_incubadora"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT HatcheryNo cincubadora,HatcheryName,HatcheryNo,IRN,Activeflag activeflag_B,ProteinCostCentersIRN FROM {database_name}.si_ProteinFacilityHatcheries""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "nincubadora",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("HatcheryName")).otherwise(col("nincubadora"))
    ).withColumn(
        "activeflag",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["cincubadora"],
        col("nincubadora"),
        existing_data["noincubadora"],
        existing_data["irn"],
        col("activeflag"),
        existing_data["proteincostcentersirn"],
        existing_data["idempresa"],
        existing_data["pk_incubadora"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_productoconsumo"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT PP.productNo cproductoconsumo_B,SUBSTRING(PP.Description,1,50) nproductoconsumo_B,PP.IRN,PP.ActiveFlag activeflag_B,GC.pk_grupoconsumo pk_grupoconsumo_B,pk_subgrupoconsumo pk_subgrupoconsumo_B
                        FROM (select *,
                                CASE WHEN ProductType = 6 AND Description like '%GAS%' THEN 'Gas'
                                     WHEN ProductType = 6 AND Description like '%CASC%' THEN 'Cama' 
                                     WHEN ProductType = 6 AND Description like '%AGUA%' THEN 'Agua'
                                     WHEN ProductType = 6 AND Description like '%KRAFT%' THEN 'Kraft'
                                     WHEN ProductType = 12 THEN 'Medicina'
                                     ELSE 'Sin Sub Grupo Consumo'
                                END SubGrupoConsumo from {database_name}.si_ProteinProducts) PP
                        LEFT OUTER JOIN {database_name}.si_ProteinProductsAnimals PPA ON PPA.ProteinProductsIRN =PP.IRN
                        LEFT JOIN {database_name}.lk_grupoconsumo GC ON PP.ProductType = GC.cgrupoconsumo AND GC.idempresa = 1
                        LEFT JOIN {database_name}.lk_subgrupoconsumo SGC ON PP.SubGrupoConsumo = SGC.nsubgrupoconsumo
                        WHERE ProteinProductsIRN IS NULL""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "cproductoconsumo",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("cproductoconsumo_B")).otherwise(col("cproductoconsumo"))
    ).withColumn(
        "nproductoconsumo",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("nproductoconsumo_B")).otherwise(col("nproductoconsumo"))
    ).withColumn(
        "activeflag",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).withColumn(
        "pk_grupoconsumo",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("pk_grupoconsumo_B")).otherwise(col("pk_grupoconsumo"))
    ).withColumn(
        "pk_subgrupoconsumo",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("pk_subgrupoconsumo_B")).otherwise(col("pk_subgrupoconsumo"))
    ).select(
        col("cproductoconsumo"),
        col("nproductoconsumo"),
        existing_data["IRN"],
        col("activeflag"),
        col("pk_grupoconsumo"),
        existing_data["idempresa"],
        col("pk_subgrupoconsumo"),
        existing_data["pk_productoconsumo"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_standard"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT StandardNo cstandard_B, StandardName nstandard_B, IRN,'1' activeflag_B, SpeciesType SpeciesType_B,CASE WHEN StandardNo like '%AD%' THEN 'Adulto' WHEN StandardNo like '%JO%' THEN 'Joven' ELSE '-' END AS lstandard_B FROM {database_name}.si_ProteinStandards""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "cstandard",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("cstandard_B")).otherwise(col("cstandard"))
    ).withColumn(
        "nstandard",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("nstandard_B")).otherwise(col("nstandard"))
    ).withColumn(
        "activeflag",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).withColumn(
        "SpeciesType",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("SpeciesType_B")).otherwise(col("SpeciesType"))
    ).withColumn(
        "lstandard",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("lstandard_B")).otherwise(col("lstandard"))
    ).select(
        col("cstandard"),
        col("nstandard"),
        existing_data["IRN"],
        col("SpeciesType"),
        col("activeflag"),
        existing_data["idempresa"],
        col("lstandard"),
        existing_data["pk_standard"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_subzona"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT GrowoutNo csubzona_B, GrowoutName nzubzona_B, IRN, '1' Activeflag_B FROM {database_name}.si_ProteinGrowoutCodes""")
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "csubzona",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("csubzona_B")).otherwise(col("csubzona"))
    ).withColumn(
        "nsubzona",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("nzubzona_B")).otherwise(col("nsubzona"))
    ).withColumn(
        "activeflag",
        when((df_lk["IRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        col("csubzona"),
        col("nsubzona"),
        existing_data["IRN"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_subzona"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_zona"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT CountyNo czona, CountyName nzona_B, IRN,'1' activeflag_B FROM {database_name}.si_ProteinCounties """)
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["czona"] == df_lk["czona"],
        "left"
    ).withColumn(
        "nzona",
        when((df_lk["czona"].isNotNull()) & (existing_data["idempresa"] == 1), col("nzona_B")).otherwise(col("nzona"))
    ).withColumn(
        "activeflag",
        when((df_lk["czona"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["czona"],
        col("nzona"),
        existing_data["IRN"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_zona"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_vehiculo"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT rtrim(VehicleNo) cvehiculo, rtrim(VehicleName) nvehiculo_B,rtrim(VehicleName) marca_B,IRN, activeflag activeflag_B FROM {database_name}.si_ProteinVehicles """)
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cvehiculo"] == df_lk["cvehiculo"],
        "left"
    ).withColumn(
        "nvehiculo",
        when((df_lk["cvehiculo"].isNotNull()) & (existing_data["idempresa"] == 1), col("nvehiculo_B")).otherwise(col("nvehiculo"))
    ).withColumn(
        "marca",
        when((df_lk["cvehiculo"].isNotNull()) & (existing_data["idempresa"] == 1), col("marca_B")).otherwise(col("marca"))
    ).withColumn(
        "activeflag",
        when((df_lk["cvehiculo"].isNotNull()) & (existing_data["idempresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).select(
        existing_data["cvehiculo"],
        col("nvehiculo"),
        col("marca"),
        existing_data["IRN"],
        col("activeflag"),
        existing_data["idempresa"],
        existing_data["pk_vehiculo"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_plantel"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT  PF.FarmNo cplantel_B, PF.FarmName nplantel_B, PF.FarmNo noplantel_B, PF.IRN, PF.Address1 direccion1_B, PF.Address2 direccion2_B,COALESCE(LZ.pk_zona,1) pk_zona_B, pf.ActiveFlag, PF.ProteinCostCentersIRN, pk_subzona pk_subzona_B, UFC.Name Cluster_B, PV.VendorShortName duenho_B
                            FROM {database_name}.si_ProteinFarms PF
                            LEFT JOIN {database_name}.Lk_zona LZ ON LZ.IRN = PF.ProteinCountiesIRN
                            LEFT JOIN {database_name}.lk_subzona LS ON ls.IRN = pf.ProteinGrowoutCodesIRN
                            LEFT JOIN {database_name}.si_BrimFarms BF ON PF.IRN = BF.proteinfarmsirn
                            LEFT JOIN {database_name}.si_MtSysUserFieldCodes UFC ON UFC.IRN = BF.U_Cluster
                            LEFT JOIN {database_name}.si_mvProteinVendors PV ON PV.IRN = PF.ProteinVendorsIRN """)
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["IRN"] == df_lk["IRN"],
        "left"
    ).withColumn(
        "cplantel",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("cplantel_B")).otherwise(col("cplantel"))
    ).withColumn(
        "nplantel",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("nplantel_B")).otherwise(col("nplantel"))
    ).withColumn(
        "noplantel",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("noplantel_B")).otherwise(col("noplantel"))
    ).withColumn(
        "direccion1",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("direccion1_B")).otherwise(col("direccion1"))
    ).withColumn(
        "direccion2",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("direccion2_B")).otherwise(col("direccion2"))
    ).withColumn(
        "pk_zona",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("pk_zona_B")).otherwise(col("pk_zona"))
    ).withColumn(
        "pk_subzona",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("pk_subzona_B")).otherwise(col("pk_subzona"))
    ).withColumn(
        "cluster",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("cluster_B")).otherwise(col("cluster"))
    ).withColumn(
        "duenho",
        when((df_lk["cplantel_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("duenho_B")).otherwise(col("duenho"))
    ).select(
        col("cplantel"),
        col("nplantel"),
        col("noplantel"),
        existing_data["IRN"],
        col("direccion1"),
        col("direccion2"),
        col("pk_zona"),
        existing_data["activeflag"],
        existing_data["proteincostcentersirn"],
        col("pk_subzona"),
        existing_data["idempresa"],
        col("cluster"),
        col("duenho"),
        existing_data["pk_plantel"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_lote"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""WITH 
    Lote as (SELECT distinct CONCAT(rtrim(BE.Farmno),'-',rtrim(BE.EntityNo)) clote, rtrim(BE.entityNo) nlote, rtrim(BE.farmno) noplantel,''AS IRN, '1' activeflag , COALESCE((SELECT pk_plantel FROM  lk_plantel LP WHERE LP.IRN like BE.ProteinFarmsIRN), 1) idplantel, 1 AS idempresa FROM {database_name}.si_mvBrimEntities BE ),
    ProteinEntities as (
    select *,EntityNo EntityNoCat from {database_name}.si_ProteinEntities where substring(notes,1,5) not in ('c1','c2','c3') or notes is null
    union
    select *, concat(substring(EntityNo,1,2) , substring(Notes,2,1) , substring(EntityNo,3,3)) EntityNoCat from {database_name}.si_ProteinEntities where substring(notes,1,5) in ('c1','c2','c3') and length(EntityNo) <> 7
    union
    select *, EntityNo EntityNoCat from {database_name}.si_ProteinEntities where substring(notes,1,5) in ('c1','c2','c3') and length(EntityNo) = 7 ) ,
    LotePavo AS (SELECT	DISTINCT substring(VB.ComplexEntityNo,1,length(VB.ComplexEntityNo)-6) clote
        ,VB.EntityNo nlote
        ,VB.FarmNo noplantel
        ,substring(PE.Notes,1,2) Notes
        ,concat(substring(VB.EntityNo,1,2) , '0' , substring(PE.Notes,2,1) , substring(VB.EntityNo,3,3)) EntityNoNotes
        ,concat(substring(VB.EntityNo,1,2) , '0' , substring(PE.Notes,2,1)) AhnoCampana
        FROM {database_name}.si_BrimFieldTrans AS BFT INNER JOIN
        {database_name}.si_mvBrimEntities AS VB ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN INNER JOIN
        {database_name}.si_ProteinFarms AS PF ON VB.ProteinFarmsIRN = PF.IRN inner join
        ProteinEntities PE ON PE.IRN = BFT.ProteinEntitiesIRN
        WHERE PF.FarmType = 7 and VB.SpeciesType = 2 and EntityHistoryFlag = False and PE.Notes is not null and length(VB.EntityNo) <> 7
        union 
        SELECT DISTINCT substring(VB.ComplexEntityNo,1,length(VB.ComplexEntityNo)-6) clote
        ,VB.EntityNo nlote
        ,VB.FarmNo noplantel
        ,substring(PE.Notes,1,2) Notes
        ,VB.EntityNo EntityNoNotes
        ,substring(VB.EntityNo,1,length(VB.EntityNo)-3) AhnoCampana
        FROM {database_name}.si_BrimFieldTrans AS BFT INNER JOIN
        {database_name}.si_mvBrimEntities AS VB ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN INNER JOIN
        {database_name}.si_ProteinFarms AS PF ON VB.ProteinFarmsIRN = PF.IRN inner join
        ProteinEntities PE ON PE.IRN = BFT.ProteinEntitiesIRN
        WHERE PF.FarmType = 7 and VB.SpeciesType = 2 and EntityHistoryFlag = False and PE.Notes is not null and length(VB.EntityNo) = 7)
    select
    a.clote clote_B
    ,a.nlote
    ,a.noplantel noplantel_B
    ,IRN
    ,activeflag
    ,idplantel pk_plantel
    ,idempresa
    ,b.notes
    ,case when b.clote is null then rtrim(a.clote) else concat(rtrim(a.noplantel),'-',rtrim(b.EntityNoNotes)) end clote2_B
    ,case when b.clote is null then rtrim(a.nlote) else rtrim(b.EntityNoNotes) end nlote2_B
    ,case when b.clote is null then rtrim(a.clote) else concat(rtrim(a.noplantel),'-',rtrim(b.AhnoCampana)) end PlantelCampana_B
    from Lote a
    left join LotePavo b on a.clote = b.clote""")
    
    updated_data = existing_data.join(
        df_lk,
        (existing_data["pk_plantel"] == df_lk["pk_plantel"]) & (existing_data["nlote"] == df_lk["nlote"]),
        "left"
    ).withColumn(
        "clote",
        when((df_lk["clote_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("clote_B")).otherwise(col("clote"))
    ).withColumn(
        "noplantel",
        when((df_lk["clote_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("noplantel_B")).otherwise(col("noplantel"))
    ).withColumn(
        "clote2",
        when((df_lk["clote_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("clote2_B")).otherwise(col("clote2"))
    ).withColumn(
        "nlote2",
        when((df_lk["clote_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("nlote2_B")).otherwise(col("nlote2"))
    ).withColumn(
        "PlantelCampana",
        when((df_lk["clote_B"].isNotNull()) & (existing_data["idempresa"] == 1), col("PlantelCampana_B")).otherwise(col("PlantelCampana"))
    ).select(
        col("clote"),
        existing_data["nlote"],
        col("noplantel"),
        existing_data["IRN"],
        existing_data["activeflag"],
        existing_data["pk_plantel"],
        existing_data["idempresa"],
        existing_data["notes"],
        col("clote2"),
        col("nlote2"),
        col("PlantelCampana"),
        existing_data["pk_lote"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_galpon"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT concat(rtrim(LP.cplantel),'-',rtrim(PH.HouseNo)) cgalpo,concat(rtrim(LP.cplantel),'-',rtrim(PH.HouseNo)) ngalpo, rtrim(PH.HouseNo) nogalpon, rtrim(LP.cplantel) noplantel_B, PH.HeadCapacity capacidad_B,PH.IRN IRN_B, PH.activeflag, LP.pk_plantel, PH.length length_B, PH.Width width_B, PH.area area_B 
                        FROM {database_name}.si_ProteinHouses PH LEFT JOIN {database_name}.lk_plantel LP ON PH.ProteinFarmsIRN = LP.IRN """)
    
    updated_data = existing_data.join(
        df_lk,
        (existing_data["pk_plantel"] == df_lk["pk_plantel"]) & (existing_data["nogalpon"] == df_lk["nogalpon"]),
        "left"
    ).withColumn(
        "cgalpon",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("cgalpo")).otherwise(col("cgalpon"))
    ).withColumn(
        "ngalpon",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("ngalpo")).otherwise(col("ngalpon"))
    ).withColumn(
        "noplantel",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("noplantel_B")).otherwise(col("noplantel"))
    ).withColumn(
        "capacidad",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("capacidad_B")).otherwise(col("capacidad"))
    ).withColumn(
        "length",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("length_B")).otherwise(col("length"))
    ).withColumn(
        "width",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("width_B")).otherwise(col("width"))
    ).withColumn(
        "area",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("area_B")).otherwise(col("area"))
    ).withColumn(
        "IRN",
        when((df_lk["cgalpo"].isNotNull()) & (existing_data["idempresa"] == 1), col("IRN_B")).otherwise(col("IRN"))
    ).select(
        col("cgalpon"),
        col("ngalpon"),
        existing_data["nogalpon"],
        col("noplantel"),
        col("capacidad"),
        col("IRN"),
        existing_data["activeflag"],
        existing_data["pk_plantel"],
        col("length"),
        col("width"),
        col("area"),
        existing_data["idempresa"],
        existing_data["pk_galpon"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_proveedor"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT distinct COALESCE(VendorNo,'0') cproveedor, COALESCE(VendorName,'Sin Proveedor') nproveedor_B FROM {database_name}.si_mvBrimFarms """)
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cproveedor"] == df_lk["cproveedor"],
        "left"
    ).withColumn(
        "nproveedor",
        when((df_lk["cproveedor"].isNotNull()) & (existing_data["idempresa"] == 1), col("nproveedor_B")).otherwise(col("nproveedor"))
    ).select(
        existing_data["cproveedor"],
        col("nproveedor"),
        existing_data["idempresa"],
        existing_data["pk_proveedor"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_planta"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    df_lk= spark.sql(f"""SELECT PlantNo cplanta, PlantName nplanta_B,ProteinCostCentersIRN ProteinCostCentersIRN_B, activeflag activeflag_B,IRN IRN_B FROM {database_name}.si_ProteinFacilityPlants """)
    
    updated_data = existing_data.join(
        df_lk,
        existing_data["cplanta"] == df_lk["cplanta"],
        "left"
    ).withColumn(
        "nplanta",
        when((df_lk["cplanta"].isNotNull()) & (existing_data["pk_empresa"] == 1), col("nplanta_B")).otherwise(col("nplanta"))
    ).withColumn(
        "ProteinCostCentersIRN",
        when((df_lk["cplanta"].isNotNull()) & (existing_data["pk_empresa"] == 1), col("ProteinCostCentersIRN_B")).otherwise(col("ProteinCostCentersIRN"))
    ).withColumn(
        "activeflag",
        when((df_lk["cplanta"].isNotNull()) & (existing_data["pk_empresa"] == 1), col("activeflag_B")).otherwise(col("activeflag"))
    ).withColumn(
        "IRN",
        when((df_lk["cplanta"].isNotNull()) & (existing_data["pk_empresa"] == 1), col("IRN_B")).otherwise(col("IRN"))
    ).select(
        existing_data["cplanta"],
        col("nplanta"),
        col("ProteinCostCentersIRN"),
        col("activeflag"),
        existing_data["pk_empresa"],
        col("IRN"),
        existing_data["pk_planta"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_maquinaincubadora"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    existing_data = existing_data.withColumnRenamed("setterno", "nmaquinaincubadora")
    df_lk= spark.sql(f"""SELECT DISTINCT CONCAT(rtrim(hatcheryno),'-',rtrim(setterno)) cmaquinaincubadora_B ,setterno, settername settername_B,himsettersirn,rtrim(hatcheryno) hatcheryno_B,pk_incubadora
                            FROM {database_name}.si_mvHimHatchTrans A
                            LEFT JOIN {database_name}.si_HimHatchTrans B on A.IRN = B.IRN 
                            LEFT JOIN {database_name}.lk_incubadora C on rtrim(C.cincubadora) = rtrim(A.hatcheryno)
                            WHERE setterno IS NOT NULL """)
    
    updated_data = existing_data.join(
        df_lk,
        (existing_data["pk_incubadora"] == df_lk["pk_incubadora"]) & (existing_data["HimSettersIRN"] == df_lk["HimSettersIRN"]),
        "left"
    ).withColumn(
        "cmaquinaincubadora",
        when((df_lk["HimSettersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("cmaquinaincubadora_B")).otherwise(col("cmaquinaincubadora"))
    ).withColumn(
        "nomaquinaincubadora",
        when((df_lk["HimSettersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("settername_B")).otherwise(col("settername"))
    ).withColumn(
        "cincubadora",
        when((df_lk["HimSettersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("hatcheryno_B")).otherwise(col("hatcheryno"))
    ).select(
        col("cmaquinaincubadora"),
        col("setterno"), 
        col("nomaquinaincubadora"),
        existing_data["HimSettersIRN"],
        col("cincubadora"),
        existing_data["pk_incubadora"],
        existing_data["idempresa"],
        existing_data["pk_maquinaincubadora"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
table_name = "lk_maquinanacedora"
path_source = f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}/"

try:
    existing_data = spark.read.format("parquet").load(path_source)
    existing_data = existing_data.withColumnRenamed("hatchername", "nomaquinanacedora")
    existing_data = existing_data.withColumnRenamed("hatcheryno", "cincubadora")
    existing_data = existing_data.withColumnRenamed("hatcherno", "nmaquinanacedora")
    df_lk= spark.sql(f"""SELECT DISTINCT CONCAT(rtrim(hatcheryno),'-',rtrim(hatcherno)) cmaquinanacedora_B ,hatcherno, hatchername,himhatchersirn,rtrim(hatcheryno) hatcheryno,pk_incubadora
                        FROM {database_name}.si_mvHimHatchTrans A
                        LEFT JOIN {database_name}.si_HimHatchTrans B on A.IRN = B.IRN 
                        LEFT JOIN {database_name}.lk_incubadora C on rtrim(C.cincubadora) = rtrim(A.hatcheryno)
                        WHERE hatcherno is not null """)
    
    updated_data = existing_data.join(
        df_lk,
        (existing_data["pk_incubadora"] == df_lk["pk_incubadora"]) & (existing_data["HimHatchersIRN"] == df_lk["HimHatchersIRN"]),
        "left"
    ).withColumn(
        "cmaquinanacedora",
        when((df_lk["HimHatchersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("cmaquinanacedora_B")).otherwise(col("cmaquinanacedora"))
    ).withColumn(
        "nomaquinanacedora",
        when((df_lk["HimHatchersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("hatchername")).otherwise(col("nomaquinanacedora"))
    ).withColumn(
        "cincubadora",
        when((df_lk["HimHatchersIRN"].isNotNull()) & (existing_data["idempresa"] == 1), col("hatcheryno")).otherwise(col("cincubadora"))
    ).select(
        col("cmaquinanacedora"),
        existing_data["nmaquinanacedora"],
        col("nomaquinanacedora"),
        existing_data["HimHatchersIRN"],
        col("cincubadora"),
        existing_data["pk_incubadora"],
        existing_data["idempresa"],
        existing_data["pk_maquinanacedora"]
    )
    cant_total = updated_data.count()

    # Escribir los resultados en ruta temporal
    additional_options = {
       "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
   }
    updated_data.write \
       .format("parquet") \
       .options(**additional_options) \
       .mode("overwrite") \
       .saveAsTable(f"{database_name}.{table_name}Temporal")
    
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.{table_name}")
            
    print(f"Total de registros actualizados en la tabla {table_name} : {cant_total}")
    #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")  
except Exception as e:
    print(f"Ocurrió un error: {e}")
    logger.info(f"No se encontraron datos existentes en {table_name}")
#no se actualiza
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()