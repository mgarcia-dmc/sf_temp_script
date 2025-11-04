from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Definir el nombre del trabajo
JOB_NAME = "nt_fin_gastos_trans_gold_hist"

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
db_name = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
database_name = f"{db_name}"
p_amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
p_ambU = p_amb.upper()

silver_bucket_name = f"ue1stg{p_amb}as3dtl005-silver"
gold_bucket_name = f"ue1stg{p_amb}as3dtl005-gold"

table_name_source_glpca = "si_glpca"
table_name_target_glpca = "gastos_real"
path_source_glpca = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_source_glpca}"
path_target_glpca = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpca}"

table_name_source_glpcp = "si_glpcp"
table_name_target_glpcp = "gastos_plan"
path_source_glpcp = f"s3://{silver_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_source_glpcp}"
path_target_glpcp = f"s3://{gold_bucket_name}/UE1STG{p_ambU}AS3FIN001/SAP/GASTOS/{table_name_target_glpcp}"
# Leer archivos de origen
try:
    df_source_glpca = spark.read.parquet(path_source_glpca)

    # Definir filtros
    filtersGLPCA = (
    ((col("RHOART") == "02") | (col("RHOART") == "05")) &  # Filtro RRCTY 2 o 5
    (col("RBUKRS").isin("SFER", "CHIM")) & 
    (col("RVERS") == "000") & 
    (col("KOKRS") == "GSFE") & 
    (col("RACCT").between("0060000000", "0069999999")) & 
    (col("ACTIV").between("0000", "ZZZZ")))

    # Seleccionar columnas necesarias
    columns_to_selectGLPCA = ["RRCTY","RVERS","RYEAR","POPER","DOCCT","RBUKRS","RPRCTR","KOKRS","RACCT","HRKFT","RASSC","SPRCTR","SHOART","HSL","MSL","CPUDT","USNAM","DOCTY","BLDAT","BUDAT","REFRYEAR","REFDOCCT","WERKS","KOSTL","LSTAR","AUFNR","MATNR","KUNNR","EBELN","KSTRG","PS_PSP_PNR","KDAUF","FKART","CO_PRZNR","RSCOPE","BLART"]

    # Aplicar filtros y seleccionar columnas
    df_filtered_glpca = df_source_glpca.filter(filtersGLPCA).select(columns_to_selectGLPCA)

    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_filtered_glpca.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_glpca) \
        .saveAsTable(f"{database_name}.{table_name_target_glpca}")

    # Contar registros y finalizar
    record_count_glpca = df_filtered_glpca.count()
    print(f"Registros procesados: {record_count_glpca}")

    print(f"Archivo copiado: {path_source_glpca} -> {path_target_glpca}")

except Exception as e:
    print(f"No se encontraron archivos para la tabla {path_source_glpca}. Error: {e}")

# Leer archivos de origen
try:
    df_source_glpcp = spark.read.parquet(path_source_glpcp)

    # Definir filtros
    filtersGLPCP = (
    (col("RVERS") == "10A") & 
    (col("KOKRS") == "GSFE") & 
    (col("RACCT").between("0060000000", "0069999999")) & 
    (col("ACTIV").between("0000", "ZZZZ")))

    # Seleccionar columnas necesarias
    columns_to_selectGLPCP = ["RRCTY","RVERS","RYEAR","RPMAX","DOCCT","RBUKRS","RPRCTR","KOKRS","RACCT","HRKFT","RASSC","SPRCTR","SHOART","CPUDT","USNAM","DOCTY","BLDAT","BUDAT","REFRYEAR","REFDOCCT","WERKS","KOSTL","LSTAR","AUFNR","MATNR","KUNNR","KSTRG","PS_PSP_PNR","KDAUF","CO_PRZNR","RSCOPE","DOCNR","DOCLN","CPUTM","ANLN1","ANLN2","RTCUR","REFDOCLN","TSLVT","TSL01","TSL02","TSL03","TSL04","TSL05","TSL06","TSL07","TSL08","TSL09","TSL10","TSL11","TSL12","TSL13","TSL14","TSL15","TSL16","HSLVT","HSL01","HSL02","HSL03","HSL04","HSL05","HSL06","HSL07","HSL08","HSL09","HSL10","HSL11","HSL12","HSL13","HSL14","HSL15","HSL16","KSLVT","KSL01","KSL02","KSL03","KSL04","KSL05","KSL06","KSL07","KSL08","KSL09","KSL10","KSL11","KSL12","KSL13","KSL14","KSL15","KSL16","MSLVT","MSL01","MSL02","MSL03","MSL04","MSL05","MSL06","MSL07","MSL08","MSL09","MSL10","MSL11","MSL12","MSL13","MSL14","MSL15","MSL16"]
    # Aplicar filtros y seleccionar columnas
    df_filtered_glpcp = df_source_glpcp.filter(filtersGLPCP).select(columns_to_selectGLPCP)
    # Guardar datos en formato Parquet y registrar en Data Catalog
    df_filtered_glpcp.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", path_target_glpcp) \
        .saveAsTable(f"{database_name}.{table_name_target_glpcp}")

    # Contar registros y finalizar
    record_count_glpcp = df_filtered_glpcp.count()
    print(f"Registros procesados: {record_count_glpcp}")

    print(f"Archivo copiado: {path_source_glpcp} -> {path_target_glpcp}")

except Exception as e:
    print(f"No se encontraron archivos para la tabla {path_source_glpcp}. Error: {e}")
        
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()