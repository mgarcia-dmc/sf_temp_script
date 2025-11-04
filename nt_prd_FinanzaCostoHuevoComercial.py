# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_FinanzaCostoHuevoComercial"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp,date_format,date_add,col,to_date
from dateutil.relativedelta import relativedelta
from datetime import datetime
print("inicia spark")
# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")
 
## Parametros Globales 
amb             = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
db_sf_costo_tmp = ssm.get_parameter(Name='p_db_prd_sf_costo_tmp', WithDecryption=True)['Parameter']['Value']
db_sf_costo_gl  = ssm.get_parameter(Name='p_db_prd_sf_costo_gl', WithDecryption=True)['Parameter']['Value']
db_sf_costo_si  = ssm.get_parameter(Name='p_db_prd_sf_costo_si', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_br  = ssm.get_parameter(Name='p_db_prd_sf_costo_br', WithDecryption=True)['Parameter']['Value']

#db_sf_pec_tmp = ssm.get_parameter(Name='p_db_prd_sf_pec_tmp', WithDecryption=True)['Parameter']['Value']
#db_sf_pec_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
db_sf_pec_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']

db_sf_fin_gold = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
db_sf_fin_silver = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"

bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_source = f"ue1stg{ambiente}as3dtl005-silver"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"

database_name_costos_tmp = db_sf_costo_tmp
database_name_costos_gl  = db_sf_costo_gl
database_name_costos_si  = db_sf_costo_si
#database_name_costos_br   = db_sf_costo_br

#database_name_tmp = db_sf_pec_tmp
#database_name_gl  = db_sf_pec_gl
database_name_si  = db_sf_pec_si

db_sap_fin_gl = db_sf_fin_gold
db_sap_fin_si = db_sf_fin_silver

table_name1= "ft_HuevoComercial"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
#database_name = "default"
print('cargando ruta')
df_HuevoComercial = spark.sql(f"""
select date_format(cast(xdate as timestamp) ,'yyyyMM') Mes,* 
FROM {database_name_costos_si}.si_mvproteinjournaltrans 
where SystemLocationGroupNo='LAYER' and SystemStageNo='LAY' and date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
                              
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/HuevoComercial"
}
df_HuevoComercial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.HuevoComercial")
print('carga temporal HuevoComercial',df_HuevoComercial.count())
df_ft_HuevoComercial = spark.sql(f"""select * 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
from {database_name_costos_tmp}.HuevoComercial""")
print('carga temporal df_ft_HuevoComercial', df_ft_HuevoComercial.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name1} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name1}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(col("Mes"),"yyyyMM")== fecha_str)))
    filtered_new_data = df_ft_HuevoComercial
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name1}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name1}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name1}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
            
    print(f"agrega registros nuevos a la tabla {table_name1} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name1} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name1}Temporal')
    print(f"Tabla {table_name1}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
    }
    df_ft_HuevoComercial.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_MesCicloHuevoComercial = spark.sql(f"""
select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_HuevoComercial 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes  >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloHuevoComercial"
}
df_MesCicloHuevoComercial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloHuevoComercial")
print('carga temporal MesCicloHuevoComercial', df_MesCicloHuevoComercial.count())
df_ft_HuevoComercialTemp = spark.sql(f"""select A.* 
from {database_name_costos_gl}.ft_HuevoComercial A 
left join {database_name_costos_tmp}.MesCicloHuevoComercial B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_HuevoComercialTemp"
}
df_ft_HuevoComercialTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_HuevoComercialTemp")
print('carga temporal ft_HuevoComercialTemp', df_ft_HuevoComercialTemp.count())
df_ft_HuevoComercial = spark.sql(f"""
select 
B.Mes 
,CreationDate 
,CreationUserId 
,ReplicaSourceId 
,ReplicationDateTime 
,LastModDate 
,IRN 
,UserId 
,PostDate 
,PostStatus 
,JournalType 
,ProteinFarmsIRN 
,ProteinEntitiesIRN 
,ProteinVendorsIRN 
,xDate 
,ProteinChartOfAccountsIRN 
,RefNo 
,TransCode 
,Amount 
,Units 
,ProteinProductsIRN 
,Description 
,SourceCode 
,SourceTransCode 
,StandardCostFlag 
,TransactionId 
,TransactionEntityId 
,TransactionEntityName 
,ComplexAccountNo 
,FarmType 
,ProteinCostCentersIRN 
,VoidFlag 
,EventDate 
,ProteinGrowoutCodesIRN 
,RelativeAmount 
,RelativeUnits 
,JournalId 
,BaseUnits 
,SourceRefNo 
,AccrualType 
,TransferMode 
,SystemLocationGroupNo 
,SystemStageNo 
,SystemCostObjectNo 
,SystemCostElementNo 
,SystemElementUserNo 
,ProteinProductsIRN_SubProduct 
,ProteinTaxCodesIRN 
,OMCustomersIRN 
,PostReversedFlag 
,ExternalId 
,CustomerNo 
,CustomerName 
,ProductNo 
,SubProductNo 
,VendorNo 
,VendorName 
,CostCenterNo 
,ComplexEntityNo 
,EntityNo 
,HouseNo 
,PenNo 
,FarmNo 
,SourceRecordId 
,CurrencyExchangeRates
,ProductType 
,TaxNo 
,AccountName 
,SpeciesType
,GrowoutNo 
,GrowoutName 
,SystemLocationGroupName 
,SystemStageName 
,SystemCostObjectName 
,SystemCostElementName 
,SystemElementUserName 
,SystemComplexAccountNo 
,SystemDescription 
,AccountType 
,CostCenterName 
,StandardCostType 
,VarianceType 
,DivisionNo 
,DivisionName 
,CompanyNo 
,CompanyName 
,ProteinCurrenciesIRN 
,TaxName 
,CurrencyNo 
,CurrencyName 
,BICategory 
,SubProductName 
,ProductName 
,RelativeTransactionAmount 
,TransactionCurrencyDebit 
,TransactionCurrencyCredit 
,TransactionCurrencyNo 
,TransactionCurrencyName 
,A.Ciclo 
from {database_name_costos_tmp}.MesCicloHuevoComercial A 
left join {database_name_costos_tmp}.ft_HuevoComercialTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null and A.Mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
except 
select * from {database_name_costos_gl}.ft_HuevoComercial 
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
print('carga temporal ft_HuevoComercial', df_ft_HuevoComercial.count())
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_HuevoComercial"
}
df_ft_HuevoComercial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.ft_HuevoComercial")
print('carga ft_HuevoComercial', df_ft_HuevoComercial.count())
df_ft_HuevoComercial_Actual = spark.sql(f"""select * from {database_name_costos_gl}.ft_HuevoComercial where Mes >= '202401' """)
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_HuevoComercial_Actual"
}
df_ft_HuevoComercial_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_HuevoComercial_Actual")
print('carga ft_HuevoComercial_Actual', df_ft_HuevoComercial_Actual.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()