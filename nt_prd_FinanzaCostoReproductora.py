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
JOB_NAME = "nt_prd_FinanzaCostoReproductora"

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
db_sf_pec_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
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
database_name_gl  = db_sf_pec_gl
database_name_si  = db_sf_pec_si

db_sap_fin_gl = db_sf_fin_gold
db_sap_fin_si = db_sf_fin_silver

table_name1="ft_Reprod_Huevo_Incubable"
table_name2="ft_Reprod_Levante"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
file_name_target2 = f"{bucket_name_prdmtech}{table_name2}/"

path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
#database_name = "default"
print('cargando ruta')
df_KilosRendidosHuevoIncubable = spark.sql(f"""
select Mes, DivisionName, SUM(RelativeUnits) RelativeUnits 
from ( 
  select date_format(cast(xDate as timestamp),'yyyyMM') Mes,RelativeUnits*-1 RelativeUnits, DivisionName 
  from {database_name_costos_si}.si_mvproteinjournaltrans A 
  where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
  and SystemStageNo = 'LAY' 
  and SystemElementUserNo = 'OUT' 
  and SystemLocationGroupNo = 'BRDR' 
  and sourcecode = 'EOP T-HIM - Egg Receivings' 
  and DivisionName = 'Pavo' 
) A 
group by Mes, DivisionName
union all 
select Mes, DivisionName, SUM(RelativeUnits) RelativeUnits
from ( 
  select date_format(cast(xDate as timestamp),'yyyyMM') Mes,RelativeUnits*-1 RelativeUnits, DivisionName 
  from {database_name_costos_si}.si_mvproteinjournaltrans A 
  where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
  and SystemStageNo = 'LAY' 
  and SystemElementUserNo = 'OUT' 
  and SystemLocationGroupNo = 'BRDR' 
  and sourcecode = 'EOP B-HIM - Egg Receivings' 
  and DivisionName = 'Pollo' 
) A 
group by Mes, DivisionName
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/KilosRendidosHuevoIncubable"
}
df_KilosRendidosHuevoIncubable.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.KilosRendidosHuevoIncubable")
print("carga KilosRendidosHuevoIncubable --> Registros procesados:", df_KilosRendidosHuevoIncubable.count())
df_Reprod_Huevo_Incubable = spark.sql(f"""
select date_format(cast(xDate as timestamp),'yyyyMM') Mes,* 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemStageNo = 'LAY' and SystemElementUserNo <> 'OUT' and SystemLocationGroupNo = 'BRDR' 
union all 
select date_format(cast(xDate as timestamp),'yyyyMM') Mes,* 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemStageNo = 'LAY' and SystemElementUserNo = 'OUT' and SystemLocationGroupNo = 'BRDR' 
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Huevo_Incubable"
}
df_Reprod_Huevo_Incubable.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Reprod_Huevo_Incubable")
print("carga Reprod_Huevo_Incubable --> Registros procesados:", df_Reprod_Huevo_Incubable.count())
df_ft_Reprod_Huevo_Incubable = spark.sql(f"""
select A.*,B.RelativeUnits KilosRendidos 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(A.Mes,1,4),'-',substring(A.Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,'SAN FERNANDO' Empresa 
from {database_name_costos_tmp}.Reprod_Huevo_Incubable A 
left join {database_name_costos_tmp}.KilosRendidosHuevoIncubable B on A.Mes = B.Mes and A.DivisionName = B.DivisionName 
where A.Mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_ft_Reprod_Huevo_Incubable', df_ft_Reprod_Huevo_Incubable.count())
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
    data_after_delete = existing_data.filter(~(col("Mes")== fecha_str))
    filtered_new_data = df_ft_Reprod_Huevo_Incubable
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
    df_ft_Reprod_Huevo_Incubable.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_MesCicloReprod_Huevo_Incubable = spark.sql(f"""
select distinct mes, 
CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_Reprod_Huevo_Incubable 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloReprod_Huevo_Incubable"
}
df_MesCicloReprod_Huevo_Incubable.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloReprod_Huevo_Incubable")
print('carga temporal MesCicloReprod_Huevo_Incubable', df_MesCicloReprod_Huevo_Incubable.count())
df_ft_Reprod_Huevo_IncubableTemp = spark.sql(f"""
select A.* 
from {database_name_costos_gl}.ft_Reprod_Huevo_Incubable A 
left join {database_name_costos_tmp}.MesCicloReprod_Huevo_Incubable B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and A.Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Reprod_Huevo_IncubableTemp"
}
df_ft_Reprod_Huevo_IncubableTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_Reprod_Huevo_IncubableTemp")
print('carga temporal ft_Reprod_Huevo_IncubableTemp', df_ft_Reprod_Huevo_IncubableTemp.count())
df_ft_Reprod_Huevo_IncubableINS = spark.sql(f"""
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
,KilosRendidos 
,A.Ciclo 
,Empresa 
from {database_name_costos_tmp}.MesCicloReprod_Huevo_Incubable A 
left join {database_name_costos_tmp}.ft_Reprod_Huevo_IncubableTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null 
except 
select * from {database_name_costos_gl}.ft_Reprod_Huevo_Incubable 
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Huevo_Incubable"
}
df_ft_Reprod_Huevo_IncubableINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Reprod_Huevo_Incubable")
print('carga INS ft_Reprod_Huevo_Incubable', df_ft_Reprod_Huevo_IncubableINS.count())
df_Reprod_Levante = spark.sql(f"""
select date_format(cast(DateCap as timestamp),'yyyyMM') Mes,A.* 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
left join {database_name_si}.si_bimcapitalizationtrans B on A.proteinentitiesirn = B.proteinentitiesirn 
where date_format(cast(DateCap as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
and SystemStageNo = 'BROOD' and SystemElementUserNo = 'OUT' and SystemLocationGroupNo = 'BRDR'
union all 
select date_format(cast(DateCap as timestamp),'yyyyMM') Mes,A.* 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
left join {database_name_si}.si_bimcapitalizationtrans  B on A.proteinentitiesirn = B.proteinentitiesirn 
where date_format(cast(DateCap as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
and SystemStageNo = 'BROOD' and SystemElementUserNo <> 'OUT' and SystemLocationGroupNo = 'BRDR'
union all 
select date_format(cast(DateCap as timestamp),'yyyyMM') Mes,A.* 
from {database_name_costos_si}.si_mvproteinjournaltrans A 
left join {database_name_si}.si_bimcapitalizationtrans  B on A.proteinentitiesirn = B.proteinentitiesirn 
where date_format(cast(DateCap as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
and SystemStageNo = 'BROOD' and SystemElementUserNo = 'OUT' and SystemLocationGroupNo = 'LAYER'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Levante"
}
df_Reprod_Levante.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Reprod_Levante")
print('carga temporal Reprod_Levante', df_Reprod_Levante.count())
df_AvesCapitalizadas = spark.sql(f"""
select * 
from (select rtrim(FarmNo) FarmNo,rtrim(EntityNo)EntityNo, concat(rtrim(FarmNo),'-',rtrim(EntityNo)) ComplexEntityNo,sum(HensCap+MalesCap) AvesCapitalizadas 
      from {database_name_si}.si_mvbimcapitalizationtrans 
      group by FarmNo, EntityNo) A 
where AvesCapitalizadas <> 0
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AvesCapitalizadas"
}
df_AvesCapitalizadas.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.AvesCapitalizadas")
print('carga temporal AvesCapitalizadas', df_AvesCapitalizadas.count())
df_ft_Reprod_Levante = spark.sql(f"""
select A.*,B.AvesCapitalizadas 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,'SAN FERNANDO' Empresa 
from {database_name_costos_tmp}.Reprod_Levante A 
left join {database_name_costos_tmp}.AvesCapitalizadas B on A.FarmNo = B.FarmNo and A.EntityNo = B.EntityNo 
where A.mes =DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_ft_Reprod_Levante' , df_ft_Reprod_Levante.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name2} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name2}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~(col("Mes")== fecha_str))
    filtered_new_data = df_ft_Reprod_Levante
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name2}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name2}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name2}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
            
    print(f"agrega registros nuevos a la tabla {table_name2} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name2} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name2}Temporal')
    print(f"Tabla {table_name2}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
    }
    df_ft_Reprod_Levante.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
df_MesCicloReprod_Levante = spark.sql(f"""
select distinct mes, 
CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_Reprod_Levante 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloReprod_Levante"
}
df_MesCicloReprod_Levante.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloReprod_Levante")
print('carga temporal MesCicloReprod_Levante', df_MesCicloReprod_Levante.count())
df_ft_Reprod_LevanteTemp = spark.sql(f"""select A.* 
from {database_name_costos_gl}.ft_Reprod_Levante A 
left join {database_name_costos_tmp}.MesCicloReprod_Levante B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Reprod_LevanteTemp"
}
df_ft_Reprod_LevanteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_Reprod_LevanteTemp")
print('carga temporal ft_Reprod_LevanteTemp', df_ft_Reprod_LevanteTemp.count())
df_ft_Reprod_LevanteINS = spark.sql(f"""
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
,AvesCapitalizadas 
,A.Ciclo 
,Empresa 
from {database_name_costos_tmp}.MesCicloReprod_Levante A 
left join {database_name_costos_tmp}.ft_Reprod_LevanteTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null 
except 
select * from {database_name_costos_gl}.ft_Reprod_Levante 
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and Empresa = 'SAN FERNANDO'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Levante"
}
df_ft_Reprod_LevanteINS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Reprod_Levante")
print('carga INS ft_Reprod_Levante', df_ft_Reprod_LevanteINS.count())
df_ft_Reprod_Huevo_Incubable_Actual = spark.sql(f"""select * from {database_name_costos_gl}.ft_Reprod_Huevo_Incubable where Mes >= '202401'""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Huevo_Incubable_Actual"
}
df_ft_Reprod_Huevo_Incubable_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Reprod_Huevo_Incubable_Actual")
print('carga temporal ft_Reprod_Levante', df_ft_Reprod_Huevo_Incubable_Actual.count())
df_ft_Reprod_Levante_Actual = spark.sql(f"""select * from {database_name_costos_gl}.ft_Reprod_Levante where Mes >= '202401'""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Levante_Actual"
}
df_ft_Reprod_Levante_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Reprod_Levante_Actual")
print('carga temporal ft_Reprod_Levante_Actual', df_ft_Reprod_Levante_Actual.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()