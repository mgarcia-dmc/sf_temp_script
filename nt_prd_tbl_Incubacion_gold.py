# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
#sc._conf.set("spark.executor.memory", "16g")
#sc._conf.set("spark.driver.memory", "4g")
#sc._conf.set("spark.dynamicAllocation.enabled", "true")
#sc._conf.set("spark.executor.memoryOverhead", "2g")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_Incubacion_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
#from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime

print("inicia spark")
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, current_timestamp
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

file_name_target20 = f"{bucket_name_prdmtech}ft_Incub_PollitosNacidos/"
file_name_target21 = f"{bucket_name_prdmtech}ft_Incub_Ovoscopia/"
file_name_target22 = f"{bucket_name_prdmtech}ft_Incub_Tabla_NacDiario/"
file_name_target23 = f"{bucket_name_prdmtech}ft_Incub_Tabla_NacSemanal/"

path_target20 = f"s3://{bucket_name_target}/{file_name_target20}"
path_target21 = f"s3://{bucket_name_target}/{file_name_target21}"
path_target22 = f"s3://{bucket_name_target}/{file_name_target22}"
path_target23 = f"s3://{bucket_name_target}/{file_name_target23}"
print('cargando rutas')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
##Pollitos Nacidos
#df_IncubacionTemp =spark.sql(f"""SELECT A.xDate,A.ProdDate,A.TransferDate,A.ReceivedDate,A.SetDate 
#,RTRIM(A.ProteinFacilityHatcheriesIRN) ProteinFacilityHatcheriesIRN 
#,RTRIM(A.HatcheryNo) HatcheryNo 
#,RTRIM(A.HatcheryName) HatcheryName 
#,RTRIM(B.HimHatchersIRN) HimHatchersIRN 
#,RTRIM(A.HatcherNo) HatcherNo 
#,RTRIM(A.HatcherName) HatcherName 
#,RTRIM(B.HimSettersIRN) HimSettersIRN 
#,RTRIM(A.SetterNo) SetterNo 
#,RTRIM(A.SetterName) SetterName 
#,RTRIM(A.SetterHatcheryName) SetterHatcheryName 
#,RTRIM(A.ProteinEntitiesIRN) ProteinEntitiesIRN 
#,RTRIM(A.ComplexEntityNo) ComplexEntityNo 
#,RTRIM(A.FarmNo) FarmNo 
#,RTRIM(A.EntityNo) EntityNo 
#,RTRIM(A.HouseNo) HouseNo 
#,RTRIM(A.BreedNo) BreedNo 
#,RTRIM(A.BreedName) BreedName 
#,RTRIM(A.ProteinCostCentersIRN) ProteinCostCentersIRN 
#,RTRIM(A.SpeciesType) SpeciesType 
#,RTRIM(A.ProteinBreedCodesIRN_Progeny) ProteinBreedCodesIRN_Progeny 
#,RTRIM(A.ProgenyBreedNo) ProgenyBreedNo 
#,RTRIM(A.ProgenyBreedName) ProgenyBreedName 
#,RTRIM(A.ProductNo) ProductNo 
#,RTRIM(A.ProductName) ProductName 
#,RTRIM(A.GenerationCode) GenerationCode 
#,RTRIM(A.TrackingNo) TrackingNo 
#,A.EggsSet,A.AdjustedEggsSet,B.U_SaleableFemales,B.U_SaleableMales,B.U_SexorCulls,A.TotalChicksHatched,B.FemaleWeight,B.MaleWeight,1 pk_empresa 
#FROM (select * 
#      from {database_name}.si_mvhimhatchtrans 
#      where (date_format(cast(xDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-12),'yyyyMM'))
#      ) A 
#LEFT JOIN (select * 
#           from {database_name}.si_himhatchtrans 
#           where (date_format(cast(xDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-12),'yyyyMM'))
#           ) B on A.IRN = B.IRN 
#WHERE 
#(date_format(cast(A.xDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-12),'yyyyMM'))""")
##df_IncubacionTemp.createOrReplaceTempView("Incubacion")
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Incubacion"
#}
#df_IncubacionTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.Incubacion")
#print('carga Incubacion',df_IncubacionTemp.count())
#Pollitos Nacidos
df_IncubacionTemp =spark.sql(f"""SELECT A.xDate,A.ProdDate,A.TransferDate,A.ReceivedDate,A.SetDate 
,RTRIM(A.ProteinFacilityHatcheriesIRN) ProteinFacilityHatcheriesIRN 
,RTRIM(A.HatcheryNo) HatcheryNo 
,RTRIM(A.HatcheryName) HatcheryName 
,RTRIM(B.HimHatchersIRN) HimHatchersIRN 
,RTRIM(A.HatcherNo) HatcherNo 
,RTRIM(A.HatcherName) HatcherName 
,RTRIM(B.HimSettersIRN) HimSettersIRN 
,RTRIM(A.SetterNo) SetterNo 
,RTRIM(A.SetterName) SetterName 
,RTRIM(A.SetterHatcheryName) SetterHatcheryName 
,RTRIM(A.ProteinEntitiesIRN) ProteinEntitiesIRN 
,RTRIM(A.ComplexEntityNo) ComplexEntityNo 
,RTRIM(A.FarmNo) FarmNo 
,RTRIM(A.EntityNo) EntityNo 
,RTRIM(A.HouseNo) HouseNo 
,RTRIM(A.BreedNo) BreedNo 
,RTRIM(A.BreedName) BreedName 
,RTRIM(A.ProteinCostCentersIRN) ProteinCostCentersIRN 
,RTRIM(A.SpeciesType) SpeciesType 
,RTRIM(A.ProteinBreedCodesIRN_Progeny) ProteinBreedCodesIRN_Progeny 
,RTRIM(A.ProgenyBreedNo) ProgenyBreedNo 
,RTRIM(A.ProgenyBreedName) ProgenyBreedName 
,RTRIM(A.ProductNo) ProductNo 
,RTRIM(A.ProductName) ProductName 
,RTRIM(A.GenerationCode) GenerationCode 
,RTRIM(A.TrackingNo) TrackingNo 
,A.EggsSet,A.AdjustedEggsSet,B.U_SaleableFemales,B.U_SaleableMales,B.U_SexorCulls,A.TotalChicksHatched,B.FemaleWeight,B.MaleWeight,1 pk_empresa 
FROM (select * 
      from {database_name}.si_mvhimhatchtrans 
      where (date_format(cast(xDate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))
      ) A 
LEFT JOIN (select * 
           from {database_name}.si_himhatchtrans 
           where (date_format(cast(xDate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))
           ) B on A.IRN = B.IRN 
WHERE 
(date_format(cast(A.xDate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))""")
#df_IncubacionTemp.createOrReplaceTempView("Incubacion")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Incubacion"
}
df_IncubacionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Incubacion")
print('carga Incubacion',df_IncubacionTemp.count())
#DELETE FROM DMPECUARIO.ft_Incub_PollitosNacidos 
#WHERE idfecha >= CONVERT(VARCHAR(8),DATEADD(mm,-3,DATEADD(mm,DATEDIFF(mm,0,GETDATE()),0)),112)

df_ft_Incub_PollitosNacidos = spark.sql(f"SELECT \
LT.pk_tiempo \
,nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
,nvl(RGD.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
,nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
,nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
,nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
,nvl(LI.pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) pk_incubadora \
,nvl(LE.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(LGE.pk_generacion,(select pk_generacion from {database_name}.lk_generacion where cgeneracion=0)) pk_generacion \
,nvl(LPRO.pk_producto,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_producto \
,nvl(LM.pk_maquinaincubadora,1) pk_maquinaincubadora \
,nvl(LN.pk_maquinanacedora,1) pk_maquinanacedora \
,A.ComplexEntityNo \
,RTRIM(LM.setterno) MaquinaIncubadora \
,RTRIM(LN.nmaquinanacedora) MaquinaNacedora \
,RGD.Edad EdadFechaProd \
,RGDC.Edad EdadFechaCarga \
,A.xDate FechaNacimiento \
,A.ProdDate FechaProduccion \
,A.TransferDate FechaTransferencia \
,A.ReceivedDate FechaRecepcion \
,A.SetDate FechaCarga \
,A.TrackingNo CodigoRastreo \
,A.EggsSet HvosCargados \
,A.AdjustedEggsSet HvosCargadosAju \
,A.U_SaleableFemales ProductoH \
,A.U_SaleableMales ProductoM \
,A.U_SexorCulls DescarteSinSexar \
,A.TotalChicksHatched TotPollitosNac \
,ROUND(A.FemaleWeight,3) PesoH \
,ROUND(A.MaleWeight,3) PesoM \
,A.pk_empresa \
,nvl(cast(cast(A.xDate as timestamp) as date),cast('1899-11-30' as date)) fecha \
FROM {database_name}.Incubacion A \
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha = nvl(cast(cast(A.xDate as timestamp) as date),cast('1899-11-30' as date)) \
LEFT JOIN {database_name}.lk_plantel LP ON LP.cplantel = A.FarmNo AND LP.idempresa = 1 \
LEFT JOIN {database_name}.lk_lote LL ON LL.nlote = A.EntityNo and LL.noplantel = A.FarmNo \
LEFT JOIN {database_name}.lk_galpon LG ON LG.nogalpon = A.HouseNo and LG.noplantel = A.FarmNo AND LG.idempresa = 1 \
LEFT JOIN {database_name}.lk_incubadora LI ON LI.IRN = A.ProteinFacilityHatcheriesIRN \
LEFT JOIN {database_name}.lk_especie LE ON LE.cespecie = BreedNo \
LEFT JOIN {database_name}.lk_generacion LGE ON LGE.cgeneracion = A.GenerationCode \
LEFT JOIN {database_name}.lk_producto LPRO ON LPRO.cproducto = A.ProductNo AND LPRO.idempresa = 1 \
LEFT JOIN {database_name}.ft_Reprod_Galpon_Diario RGD ON RGD.pk_plantel = LP.pk_plantel and RGD.pk_lote = LL.pk_lote and RGD.pk_galpon = LG.pk_galpon and \
RGD.descripFecha = cast(cast(A.ProdDate as timestamp) as date) \
LEFT JOIN {database_name}.ft_Reprod_Galpon_Diario RGDC ON RGDC.pk_plantel = LP.pk_plantel and RGDC.pk_lote = LL.pk_lote and RGDC.pk_galpon = LG.pk_galpon and \
RGDC.descripFecha = cast(cast(A.SetDate as timestamp) as date) \
LEFT JOIN {database_name}.si_proteincostcenters PC ON PC.IRN = A.ProteinCostCentersIRN \
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = PC.ProteinDivisionsIRN \
LEFT JOIN {database_name}.lk_maquinaincubadora LM ON LM.himsettersirn = A.himsettersirn \
LEFT JOIN {database_name}.lk_maquinanacedora LN ON LN.HimHatchersIRN = A.HimHatchersIRN \
where (date_format(cast(A.xDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM'))")
                                        
print('df_ft_Incub_PollitosNacidos')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target20)    
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes00 = spark.read.format("parquet").load(path_target20)
    datos_existentes00 = True
    logger.info(f"Datos existentes de ft_Incub_PollitosNacidos cargados: {df_existentes00.count()} registros")
except:
    datos_existentes00 = False
    logger.info("No se encontraron datos existentes en ft_Incub_PollitosNacidos")



if datos_existentes00:
    existing_data00 = spark.read.format("parquet").load(path_target20)
    data_after_delete00 = existing_data00.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data00 = df_ft_Incub_PollitosNacidos.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data00 = filtered_new_data00.union(data_after_delete00)                             
   
    cant_ingresonuevo00 = filtered_new_data00.count()
    cant_total00 = final_data00.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_PollitosNacidosTemporal"
    }
    final_data00.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_PollitosNacidosTemporal")
    
    
    #schema = existing_data.schema
    final_data00_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_PollitosNacidosTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_PollitosNacidos"
    }
    final_data00_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_PollitosNacidos")
            
    print(f"agrega registros nuevos a la tabla ft_Incub_PollitosNacidos : {cant_ingresonuevo00}")
    print(f"Total de registros en la tabla ft_Incub_PollitosNacidos : {cant_total00}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Incub_PollitosNacidosTemporal')
    print(f"Tabla ft_Incub_PollitosNacidosTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_PollitosNacidos"
    }
    df_ft_Incub_PollitosNacidos.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_PollitosNacidos")
#Ovoscopia
df_OvoscopiaTemp = spark.sql(f"""SELECT A.TransDate 
,RTRIM(A.ProteinFacilityHatcheriesIRN) ProteinFacilityHatcheriesIRN 
,RTRIM(A.HatcheryNo) HatcheryNo 
,RTRIM(A.HatcheryName) HatcheryName 
,RTRIM(A.HimHatchersIRN) HimHatchersIRN 
,RTRIM(A.HimSettersIRN) HimSettersIRN 
,RTRIM(A.ProteinEntitiesIRN) ProteinEntitiesIRN 
,RTRIM(A.ComplexEntityNo) ComplexEntityNo 
,RTRIM(A.FarmNo) FarmNo 
,RTRIM(A.EntityNo) EntityNo 
,RTRIM(A.HouseNo) HouseNo 
,RTRIM(A.ProgenyBreedNo) ProgenyBreedNo 
,RTRIM(A.ProgenyBreedName) ProgenyBreedName 
,RTRIM(A.ProteinCostCentersIRN) ProteinCostCentersIRN 
,RTRIM(A.ProgenyProductNo) ProgenyProductNo 
,RTRIM(A.ProgenyProductName) ProgenyProductName 
,A.EventDate 
,A.HatchDate 
,A.ProdDate 
,A.SetDate 
,A.ReceivedDate 
,A.CreationDate 
,A.LastModDate 
,A.EggsSet 
,A.NumberOfSamples 
,B.U_Infertil 
,B.U_Mort1era 
,B.U_Mort2da 
,B.U_Mort3era 
,B.U_PipsV 
,B.U_PipsM 
,B.U_Hongos 
,B.U_Bomba 
,B.U_Cont 
,B.U_Quebr 
,B.U_HuevosExplotados 
,A.ExpectedHatch 
,A.ProjectedHatch 
,RTRIM(A.TrackingNo) TrackingNo 
,A.SelectedEggs 
,RTRIM(A.RefNo) RefNo 
,1 pk_empresa 
FROM (select * 
      from {database_name}.si_mvhimbreakouttrans 
      WHERE (date_format(cast(transdate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))
      ) A 
LEFT JOIN (select * 
           from {database_name}.si_himbreakouttrans 
           WHERE (date_format(cast(transdate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))
           ) B on A.IRN = B.IRN 
WHERE (date_format(cast(A.transdate as timestamp),'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM'))""")

#df_OvoscopiaTemp.createOrReplaceTempView("Ovoscopia")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Ovoscopia"
}
df_OvoscopiaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Ovoscopia")
print('carga Ovoscopia')
#DELETE FROM DMPECUARIO.ft_Incub_Ovoscopia 
#WHERE idfecha >= CONVERT(VARCHAR(8),DATEADD(mm,-3,DATEADD(mm,DATEDIFF(mm,0,GETDATE()),0)),112)

df_ft_Incub_Ovoscopia = spark.sql(f"SELECT \
LT.pk_tiempo \
,nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
,nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
,nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
,nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
,nvl(LI.pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) pk_incubadora \
,nvl(LE.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(LPRO.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(LM.pk_maquinaincubadora,1) pk_maquinaincubadora \
,nvl(LN.pk_maquinanacedora,1) pk_maquinanacedora \
,A.ComplexEntityNo \
,RTRIM(setterno) MaquinaIncubadora \
,RTRIM(LN.nmaquinanacedora) MaquinaNacedora \
,RGD.Edad EdadFechaProd \
,RGDC.Edad EdadFechaCarga \
,A.CreationDate FechaCreacion \
,A.SetDate FechaCarga \
,A.EventDate FechaEvento \
,A.HatchDate FechaNacimiento \
,A.ProdDate FechaProduccion \
,A.ReceivedDate FechaRecepcion \
,A.TransDate FechaTransaccion \
,A.TrackingNo CodigoRastreo \
,A.RefNo \
,A.NumberOfSamples NumMuestras \
,ROUND(A.ExpectedHatch,3) NacimientoEsperado \
,ROUND(A.ProjectedHatch,3) NacimientoProyectado \
,A.EggsSet HvosCargados \
,A.SelectedEggs HvosSeleccionados \
,A.U_Infertil HvosInfertil \
,A.U_Mort1era Mort1Sem \
,A.U_Mort2da Mort2Sem \
,A.U_Mort3era Mort3Sem \
,A.U_PipsV HvosPipsVivos \
,A.U_PipsM HvosPipsMuertos \
,A.U_Hongos HvosHongos \
,A.U_Bomba HvosBomba \
,A.U_Cont HvosContaminados \
,A.U_Quebr HvosQuebrados \
,A.U_HuevosExplotados HvosExplotados \
,A.pk_empresa \
,nvl(cast(cast(A.transdate as timestamp) as date),cast('1899-11-30' as date)) fecha \
FROM {database_name}.Ovoscopia A \
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha = nvl(cast(cast(A.transdate as timestamp) as date),cast('1899-11-30' as date)) \
LEFT JOIN {database_name}.lk_plantel LP ON LP.cplantel = A.FarmNo AND LP.idempresa = 1 \
LEFT JOIN {database_name}.lk_lote LL ON LL.nlote = A.EntityNo and LL.noplantel = A.FarmNo \
LEFT JOIN {database_name}.lk_galpon LG ON LG.nogalpon = A.HouseNo and LG.noplantel = A.FarmNo AND LG.idempresa = 1 \
LEFT JOIN {database_name}.lk_incubadora LI ON LI.IRN = A.ProteinFacilityHatcheriesIRN \
LEFT JOIN {database_name}.lk_especie LE ON LE.cespecie = ProgenyBreedNo \
LEFT JOIN {database_name}.lk_producto LPRO ON LPRO.cproducto = A.ProgenyProductNo AND LPRO.idempresa = 1 \
LEFT JOIN {database_name}.ft_Reprod_Galpon_Diario RGD ON RGD.pk_plantel = LP.pk_plantel and RGD.pk_lote = LL.pk_lote and RGD.pk_galpon = LG.pk_galpon and RGD.descripFecha = cast(cast(A.ProdDate as timestamp) as date) \
LEFT JOIN {database_name}.ft_Reprod_Galpon_Diario RGDC ON RGDC.pk_plantel = LP.pk_plantel and RGDC.pk_lote = LL.pk_lote and RGDC.pk_galpon = LG.pk_galpon and RGDC.descripFecha = cast(cast(A.SetDate as timestamp) as date) \
LEFT JOIN {database_name}.si_proteincostcenters PC ON PC.IRN = A.ProteinCostCentersIRN \
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = PC.ProteinDivisionsIRN \
LEFT JOIN {database_name}.lk_maquinaincubadora LM ON LM.himsettersirn = A.himsettersirn \
LEFT JOIN {database_name}.lk_maquinanacedora LN ON LN.HimHatchersIRN = A.HimHatchersIRN \
WHERE (date_format(cast(A.transdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM'))")
print('df_ft_Incub_Ovoscopia')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target21)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")


try:
    df_existentes = spark.read.format("parquet").load(path_target21)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_Incub_Ovoscopia cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_Incub_Ovoscopia")


if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target21)
    data_after_delete = existing_data.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_Incub_Ovoscopia.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_OvoscopiaTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_OvoscopiaTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_OvoscopiaTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Ovoscopia"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Ovoscopia")
            
    print(f"agrega registros nuevos a la tabla ft_Incub_Ovoscopia : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_Incub_Ovoscopia : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Incub_OvoscopiaTemporal')
    print(f"Tabla ft_Incub_OvoscopiaTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Ovoscopia"
    }
    df_ft_Incub_Ovoscopia.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Ovoscopia")
#Nacimiento Diario
df_TablaNacDiarioTemp = spark.sql(f"""SELECT
A.pk_tiempo 
,A.fecha 
,anio 
,mes 
,semanaIncub semana 
,A.pk_division 
,case when A.pk_zona = 34 then 1 else A.pk_zona end pk_zona
,A.pk_plantel 
,A.pk_lote 
,A.pk_galpon 
,A.pk_incubadora 
,A.pk_especie 
,A.pk_generacion 
,A.pk_producto 
,A.pk_maquinaincubadora 
,A.pk_maquinanacedora 
,A.ComplexEntityNo 
,A.MaquinaNacedora 
,A.MaquinaIncubadora 
,Ceiling(A.EdadFechaCarga) Edad 
,A.FechaNacimiento 
,min(A.FechaProduccion) FechaProduccionMin 
,max(A.FechaProduccion) FechaProduccionMax 
,A.FechaTransferencia 
,min(A.FechaRecepcion) FechaRecepcionMin 
,max(A.FechaRecepcion) FechaRecepcionMax 
,A.FechaCarga 
,sum(A.HvosCargados) HvosCargados 
,sum(A.ProductoH) ProductoH 
,sum(A.ProductoM) ProductoM 
,sum(A.DescarteSinSexar) DescarteSinSexar 
,sum(A.TotPollitosNac) TotPollitosNac 
,max(B.NumMuestras ) NumMuestras 
,max(B.HvosInfertil) HvosInfertil 
,max(B.Mort1Sem) Mort1Sem 
,max(B.Mort2Sem) Mort2Sem 
,max(B.Mort3Sem) Mort3Sem 
,max(B.HvosPipsVivos) HvosPipsVivos 
,max(B.HvosPipsMuertos) HvosPipsMuertos 
,max(B.HvosHongos) HvosHongos 
,max(B.HvosBomba) HvosBomba 
,max(B.HvosContaminados) HvosContaminados 
,max(B.HvosQuebrados) HvosQuebrados 
,max(B.HvosExplotados) HvosExplotados 
,substring(C.cespecie,1,4) Raza 
,max(D.hvofert) HvosFertSTD 
,max(D.hvoinfert) HvosInfertSTD 
,max(D.MortEmbrio) MortEmbrioSTD 
,max(D.NacPbb1ra) NacPbb1raSTD 
,max(D.Descarte) DescarteSTD 
,max(D.NacTotal) NacPbbTotalSTD 
,max(D.incubabilidad) IncubabilidadSTD 
,A.pk_empresa 
FROM {database_name}.ft_incub_pollitosnacidos A 
left join {database_name}.ft_Incub_Ovoscopia B on A.ComplexEntityNo = B.ComplexEntityNo and A.MaquinaNacedora = B.MaquinaNacedora and A.MaquinaIncubadora = B.MaquinaIncubadora and A.pk_tiempo = B.pk_tiempo 
and A.pk_incubadora = B.pk_incubadora and B.NumMuestras <> 0 
left join {database_name}.lk_especie C on A.pk_especie = C.pk_especie 
left join {database_name}.lk_ft_excel_incub_std_raza D on substring(C.cespecie,1,4) = D.raza and Ceiling(A.EdadFechaCarga) = D.edad 
left join {database_name}.lk_tiempo T on T.pk_tiempo = A.pk_tiempo 
WHERE (date_format(A.fecha,'yyyyMM') >= date_format(DATE_TRUNC('year', ADD_MONTHS(CURRENT_DATE(), -12)),'yyyyMM')) 
GROUP BY A.pk_tiempo,A.fecha,case when A.pk_zona = 34 then 1 else A.pk_zona end,A.pk_division,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_incubadora,A.pk_especie,A.pk_generacion 
,A.pk_producto,A.pk_maquinaincubadora,A.pk_maquinanacedora,A.MaquinaNacedora,A.MaquinaIncubadora,A.ComplexEntityNo,Ceiling(A.EdadFechaCarga) 
,A.FechaNacimiento,A.FechaTransferencia ,A.FechaCarga,substring(C.cespecie,1,4),anio,mes,semanaIncub,A.pk_empresa""")
#df_TablaNacDiarioTemp.createOrReplaceTempView("TablaNacDiario")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaNacDiario"
}
df_TablaNacDiarioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaNacDiario")
print('carga TablaNacDiario')
#DELETE FROM DMPECUARIO.ft_Incub_Tabla_NacDiario
#WHERE idfecha >= CONVERT(VARCHAR(8),DATEADD(mm,-3,DATEADD(mm,DATEDIFF(mm,0,GETDATE()),0)),112)


df_ft_Incub_Tabla_NacDiario =spark.sql(f"SELECT pk_tiempo,anio,mes,semana,pk_division,pk_zona,pk_plantel,pk_lote \
,pk_galpon,pk_incubadora,pk_especie,pk_generacion,pk_producto,pk_maquinaincubadora,pk_maquinanacedora,ComplexEntityNo \
,MaquinaNacedora,MaquinaIncubadora,Edad,FechaNacimiento,FechaProduccionMin,FechaProduccionMax,FechaCarga \
,concat(datediff(cast(cast(FechaCarga as timestamp) as date),cast(cast(FechaProduccionMax as timestamp) as date)),' - ',datediff(cast(cast(FechaCarga as timestamp) as date),cast(cast(FechaProduccionMin as timestamp) as date))) DiasAlmacenamiento\
,HvosCargados,(ProductoH + ProductoM) NacPBB1ra,DescarteSinSexar Descarte,TotPollitosNac NacPBBTotal \
,round(((ProductoH + ProductoM) / nullif((HvosCargados*1.0),0))*100,2) as PorcNacPBB1ra \
,NacPBB1raSTD as PorcNacPBB1raSTD \
,((round(((ProductoH + ProductoM) / nullif((HvosCargados*1.0),0))*100,2)) - (NacPBB1raSTD)) as PorcNacPBB1ra_R_S \
,round((DescarteSinSexar / nullif((HvosCargados*1.0),0))*100,2) PorcDescarte \
,DescarteSTD PorcDescarteSTD \
,(round((DescarteSinSexar / nullif((HvosCargados*1.0),0))*100,2) - DescarteSTD) PorcDescarte_R_S \
,round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2) PorcNacPBBTotal \
,NacPbbTotalSTD PorcNacPBBTotalSTD \
,(round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2) - NacPbbTotalSTD) PorcNacPBBTotal_R_S \
,round(((round(((ProductoH + ProductoM) / nullif((HvosCargados*1.0),0))*100,3)) / (nullif((round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,3)),0)*100)),2) PorcAprov \
,NumMuestras \
,HvosInfertil Infertil \
,Mort1Sem \
,Mort2Sem \
,Mort3Sem \
,HvosPipsVivos PipsVivos \
,HvosPipsMuertos PipsMuertos \
,HvosQuebrados Quebrados \
,(HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados) MortTotal \
,HvosHongos Hongos \
,HvosBomba Bomba \
,HvosExplotados Explotados \
,HvosContaminados Otros \
,round((((Mort1Sem) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcMort1Sem \
,round((((Mort2Sem) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcMort2Sem \
,round((((Mort3Sem) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcMort3Sem \
,round((((HvosPipsVivos) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcPipVivos \
,round((((HvosPipsMuertos) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcPipMuertos \
,round((((HvosQuebrados) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcQuebrados \
,round((((HvosHongos) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcHongos \
,round(((HvosBomba + HvosExplotados) / (nullif((HvosCargados*1.0),0)))*100,2) PorcBomba \
,round((((HvosContaminados) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcOtros \
,(round((((HvosHongos) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2)) + \
 (round(((HvosBomba + HvosExplotados) / (nullif((HvosCargados*1.0),0)))*100,2)) + \
 (round((((HvosContaminados) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2)) PorcHvosContaminados \
,round((((Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos \
+ HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcMortEmbTotal \
,MortEmbrioSTD PorcMortEmbTotalSTD \
,(round((((Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos \
+ HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) - MortEmbrioSTD) PorcMortEmbTotal_R_S \
,100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) PorcFertil \
,HvosFertSTD PorcFertilSTD \
,((100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2)) - \
(HvosFertSTD)) PorcFertil_R_S \
,round(((round(((round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2))/(100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem \
+ HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2))*100),2)) * HvosCargados)/100,0) CantIncub \
,round(((round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2))/(100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/ \
nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2))*100),2) PorcIncub \
,round(((IncubabilidadSTD * HvosCargados)/100),0) CantIncubSTD \
,IncubabilidadSTD PorcIncubSTD \
,((round(((round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2))/(100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/ \
nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2))*100),2)) - IncubabilidadSTD) PorcIncub_R_S \
,(round((TotPollitosNac / nullif((HvosCargados*1.0),0))*100,2)) + (100 - (100-round((((HvosInfertil) * ((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/ \
nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) )) + round((((Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados) * \
((HvosCargados-TotPollitosNac)/nullif((HvosCargados*1.0),0)))/nullif(((HvosInfertil + Mort1Sem + Mort2Sem + Mort3Sem + HvosPipsVivos + HvosPipsMuertos + HvosQuebrados)*1.0),0))*100,2) Real \
,(NacPbbTotalSTD + (100-HvosFertSTD) + MortEmbrioSTD) STD \
,(Edad * HvosCargados) EdadPonderada \
,CASE WHEN Edad >= 24 and Edad <= 35 THEN 'Joven' WHEN Edad >= 36 and Edad <= 50 THEN 'Adulto' WHEN Edad >= 51 THEN 'Viejo' END EdadAgrupada \
,pk_empresa \
,fecha \
FROM {database_name}.TablaNacDiario \
WHERE (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')) ")

print('df_ft_Incub_Tabla_NacDiario')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target22) 
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")


try:
    df_existentes0 = spark.read.format("parquet").load(path_target22)
    datos_existentes0 = True
    logger.info(f"Datos existentes de ft_Incub_Tabla_NacDiario cargados: {df_existentes0.count()} registros")
except:
    datos_existentes0 = False
    logger.info("No se encontraron datos existentes en ft_Incub_Tabla_NacDiario")


if datos_existentes0:
    existing_data0 = spark.read.format("parquet").load(path_target22)
    data_after_delete0 = existing_data0.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data0 = df_ft_Incub_Tabla_NacDiario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data0 = filtered_new_data0.union(data_after_delete0)                             
   
    cant_ingresonuevo0 = filtered_new_data0.count()
    cant_total0 = final_data0.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_Tabla_NacDiarioTemporal"
    }
    final_data0.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacDiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data0_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_Tabla_NacDiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Tabla_NacDiario"
    }
    final_data0_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacDiario")
            
    print(f"agrega registros nuevos a la tabla ft_Incub_Tabla_NacDiario : {cant_ingresonuevo0}")
    print(f"Total de registros en la tabla ft_Incub_Tabla_NacDiario : {cant_total0}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Incub_Tabla_NacDiarioTemporal')
    print(f"Tabla ft_Incub_Tabla_NacDiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Tabla_NacDiario"
    }
    df_ft_Incub_Tabla_NacDiario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacDiario")
#Nacimiento Semanal
df_SemanalTemp = spark.sql(f"SELECT \
 max(pk_tiempo) pk_tiempo \
,max(fecha) fecha \
,anio \
,mes \
,semana \
,pk_division \
,case when pk_zona = 34 then 1 else pk_zona end pk_zona \
,pk_plantel \
,pk_lote \
,pk_galpon \
,pk_incubadora \
,pk_especie \
,pk_generacion \
,pk_producto \
,pk_maquinaincubadora \
,pk_maquinanacedora \
,ComplexEntityNo \
,min(Edad) Edad \
,max(FechaNacimiento) FechaNacimiento \
,max(FechaCarga) FechaCarga \
,sum(HvosCargados) HvoCargados \
,sum(NacPBB1ra) NacPBB1ra \
,sum(Descarte) Descarte \
,sum(NacPBBTotal) NacPBBTotal \
,sum(NumMuestras) NumMuestras \
,sum(Infertil) Infertil \
,sum(Mort1Sem) Mort1Sem \
,sum(Mort2Sem) Mort2Sem \
,sum(Mort3Sem) Mort3Sem \
,sum(PipsVivos) PipsVivos \
,sum(PipsMuertos) PipsMuertos \
,sum(Quebrados) Quebrados \
,sum(MortTotal) MortTotal \
,sum(Hongos) Hongos \
,sum(Bomba) Bomba \
,sum(Explotados) Explotados \
,sum(Otros) Otros \
,pk_empresa \
FROM {database_name}.ft_Incub_Tabla_NacDiario \
GROUP BY anio,mes,semana,pk_division,case when pk_zona = 34 then 1 else pk_zona end,pk_plantel,pk_lote,pk_galpon,pk_incubadora,pk_especie,pk_generacion \
,pk_producto,pk_maquinaincubadora,pk_maquinanacedora,ComplexEntityNo,pk_empresa")

#df_SemanalTemp.createOrReplaceTempView("Semanal")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Semanal"
}
df_SemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Semanal")
print('carga Semanal')
#DELETE FROM DMPECUARIO.ft_Incub_Tabla_NacSemanal
#WHERE idfecha >= CONVERT(VARCHAR(8),DATEADD(mm,-3,DATEADD(mm,DATEDIFF(mm,0,GETDATE()),0)),112)

df_ft_Incub_Tabla_NacSemanal = spark.sql(f"SELECT \
A.pk_tiempo \
,A.anio \
,A.mes \
,A.semana \
,A.pk_division \
,A.pk_zona \
,A.pk_plantel \
,A.pk_lote \
,A.pk_galpon \
,A.pk_incubadora \
,A.pk_especie \
,A.pk_generacion \
,A.pk_producto \
,A.pk_maquinaincubadora \
,A.pk_maquinanacedora \
,A.ComplexEntityNo \
,A.Edad \
,A.FechaNacimiento \
,A.FechaCarga \
,A.HvoCargados \
,A.NacPBB1ra \
,A.Descarte \
,A.NacPBBTotal \
,ROUND((A.NacPBB1ra/nullif(A.HvoCargados*1.0,0))*100,2) PorcNacPBB1ra \
,ROUND(D.NacPBB1ra * ((A.HvoCargados*1.0)/100),0) NacPBB1raSTD \
,D.NacPBB1ra PorcNacPBB1raSTD \
,(ROUND((A.NacPBB1ra/nullif(A.HvoCargados*1.0,0))*100,2) - D.NacPBB1ra) PorcNacPBB1ra_R_S \
,ROUND((A.Descarte/nullif(A.HvoCargados*1.0,0))*100,2) PorcDescarte \
,ROUND(D.Descarte * ((A.HvoCargados*1.0)/100),0) DescarteSTD \
,D.Descarte PorcDescarteSTD \
,(ROUND((A.Descarte/nullif(A.HvoCargados*1.0,0))*100,2) - D.Descarte) PorcDescarte_R_S \
,ROUND((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100,2) PorcNacPBBTotal \
,ROUND(D.NacTotal * ((A.HvoCargados*1.0)/100),0) NacPBBTotalSTD \
,D.NacTotal PorcNacPBBTotalSTD \
,(ROUND((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100,2) - D.NacTotal) PorcNacPBBTotal_R_S \
,ROUND((ROUND((A.NacPBB1ra/nullif(A.HvoCargados*1.0,0))*100,2)/nullif(ROUND((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100,2),0))*100,2) PorcAprov \
,A.NumMuestras \
,A.Infertil \
,A.Mort1Sem \
,A.Mort2Sem \
,A.Mort3Sem \
,A.PipsVivos \
,A.PipsMuertos \
,A.Quebrados \
,A.MortTotal \
,A.Hongos \
,A.Bomba \
,A.Explotados \
,A.Otros \
,ROUND((A.Mort1Sem * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcMort1Sem \
,ROUND((A.Mort2Sem * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcMort2Sem \
,ROUND((A.Mort3Sem * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcMort3Sem \
,ROUND((A.PipsVivos * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcPipsVivos \
,ROUND((A.PipsMuertos * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcPipsMuertos \
,ROUND((A.Quebrados * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcQuebrados \
,ROUND((A.Hongos * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcHongos \
,ROUND(((A.Bomba + A.Explotados) / (nullif((A.HvoCargados*1.0),0)))*100,2) PorcBomba \
,ROUND((A.Otros * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcOtros \
,ROUND((A.Hongos * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) + \
 ROUND(((A.Bomba + A.Explotados) / (nullif((A.HvoCargados*1.0),0)))*100,2) + \
 ROUND((A.Otros * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)*100,2) PorcHvosContaminados \
,ROUND(((A.Mort1Sem + A.Mort2Sem + A.Mort3Sem + A.PipsVivos + A.PipsMuertos+A.Quebrados) * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0) * 100,2) PorcMortEmbTotal \
,ROUND((((A.Mort1Sem + A.Mort2Sem + A.Mort3Sem + A.PipsVivos + A.PipsMuertos+A.Quebrados) * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0)) * A.HvoCargados,0) MortEmbTotal \
,D.MortEmbrio PorcMortEmbTotalSTD \
,ROUND((D.MortEmbrio * A.HvoCargados/100),0) MortEmbTotalSTD \
,(ROUND(((A.Mort1Sem + A.Mort2Sem + A.Mort3Sem + A.PipsVivos + A.PipsMuertos+A.Quebrados) * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0) * 100,2) - D.MortEmbrio) PorcMortEmbTotal_R_S \
,100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2) PorcFertil \
,ROUND(((100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2))* A.HvoCargados)/100,0) Fertil \
,D.hvofert PorcFertilSTD \
,ROUND((D.hvofert * A.HvoCargados/100),0) FertilSTD \
,((100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2)) - D.hvofert) PorcFertil_R_S \
,ROUND((((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100) / (100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2))*100),2) PorcIncub \
,ROUND((ROUND((((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100) / (100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2))*100),2) * A.HvoCargados) / 100,0) CantIncub \
,D.incubabilidad PorcIncubSTD \
,ROUND((D.incubabilidad * A.HvoCargados/100),0) CantIncubSTD \
,((ROUND((((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100) / (100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2))*100),2)) - D.incubabilidad) PorcIncub_R_S \
,(ROUND((A.NacPBBTotal/nullif(A.HvoCargados*1.0,0))*100,2)) + (100-(100-ROUND((((A.Infertil) * ((A.HvoCargados-A.NacPBBTotal)/nullif((A.HvoCargados*1.0),0)))/nullif(A.MortTotal,0))*100,2))) + \
(ROUND(((A.Mort1Sem + A.Mort2Sem + A.Mort3Sem + A.PipsVivos + A.PipsMuertos+A.Quebrados) * ((A.HvoCargados-A.NacPBBTotal)/nullif(A.HvoCargados*1.0,0))) / nullif(A.MortTotal,0) * 100,2)) Real \
,D.NacTotal + (100-(D.hvofert)) + D.MortEmbrio STD \
,CASE WHEN A.Edad >= 24 and A.Edad <= 35 THEN 'Joven' WHEN A.Edad >= 36 and A.Edad <= 50 THEN 'Adulto' WHEN A.Edad >= 51 THEN 'Viejo' END EdadAgrupada \
,substring(ComplexEntityNo,1,1) CodigoGranja \
,CASE WHEN A.pk_especie = 16 THEN A.HvoCargados ELSE 0 END HvoCargadosCOBBMX \
,CASE WHEN A.pk_especie = 16 THEN A.NacPBB1ra ELSE 0 END NacPBB1raCOBBMX \
,CASE WHEN A.pk_especie = 15 THEN A.HvoCargados ELSE 0 END HvoCargadosCOBBMV \
,CASE WHEN A.pk_especie = 15 THEN A.NacPBB1ra ELSE 0 END NacPBB1raCOBBMV \
,CASE WHEN A.pk_especie = 40 THEN A.HvoCargados ELSE 0 END HvoCargadosROSS \
,CASE WHEN A.pk_especie = 40 THEN A.NacPBB1ra ELSE 0 END NacPBB1raROSS \
,CASE WHEN A.pk_especie =  6 THEN A.HvoCargados ELSE 0 END HvoCargadosCOBB500 \
,CASE WHEN A.pk_especie =  6 THEN A.NacPBB1ra ELSE 0 END NacPBB1raROSSCOBB500 \
,cast(semana as varchar(10)) + '-' + nzona SemGranja \
,CASE WHEN A.Edad >= 24 and A.Edad <= 35 THEN A.HvoCargados ELSE 0 END HvoCargadosJoven \
,CASE WHEN A.Edad >= 24 and A.Edad <= 35 THEN A.HvoCargados ELSE 0 END * A.Edad HvoCargadosJovenXEdad \
,CASE WHEN A.Edad >= 36 and A.Edad <= 50 THEN A.NacPBB1ra ELSE 0 END HvoCargadosAdulto \
,CASE WHEN A.Edad >= 36 and A.Edad <= 50 THEN A.NacPBB1ra ELSE 0 END * A.Edad HvoCargadosAdultoXEdad \
,CASE WHEN A.Edad >= 51 THEN A.NacPBB1ra ELSE 0 END HvoCargadosViejo \
,CASE WHEN A.Edad >= 51 THEN A.NacPBB1ra ELSE 0 END * A.Edad HvoCargadosViejoXEdad \
,right(rtrim(E.cespecie),2) CodRaza \
,cast(semana as varchar(10)) + '_' + right(rtrim(E.cespecie),2) SemRaza \
,A.pk_empresa \
,0 hvospips \
,0 hvosroturatemprana \
,0 hvosroturatransfer \
,0 hvosinvertido \
,0.0 porchvospips \
,0.0 porchvosroturatemprana \
,0.0 porchvosroturatransfer \
,0.0 porchvosinvertido \
,0 hvoscontaminado \
,A.fecha \
FROM {database_name}.Semanal A \
LEFT JOIN {database_name}.lk_especie C on A.pk_especie = C.pk_especie \
LEFT JOIN {database_name}.lk_ft_Excel_Incub_STD_Raza D on substring(C.cespecie,1,4) = D.raza and Ceiling(A.Edad) = D.edad \
LEFT JOIN {database_name}.lk_zona Z on A.pk_zona = Z.pk_zona \
LEFT JOIN {database_name}.lk_especie E ON A.pk_especie = E.pk_especie \
WHERE (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM'))")
print('df_ft_Incub_Tabla_NacSemanal')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target23)     
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")



try:
    df_existentes01 = spark.read.format("parquet").load(path_target23)
    datos_existentes01 = True
    logger.info(f"Datos existentes de ft_Incub_Tabla_NacSemanal cargados: {df_existentes01.count()} registros")
except:
    datos_existentes01 = False
    logger.info("No se encontraron datos existentes en ft_Incub_Tabla_NacSemanal")



if datos_existentes01:
    existing_data01 = spark.read.format("parquet").load(path_target23)
    data_after_delete01 = existing_data01.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data01 = df_ft_Incub_Tabla_NacSemanal.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data01 = filtered_new_data01.union(data_after_delete01)                             
   
    cant_ingresonuevo01 = filtered_new_data01.count()
    cant_total01 = final_data01.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_Tabla_NacSemanalTemporal"
    }
    final_data01.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacSemanalTemporal")
    
    
    #schema = existing_data.schema
    final_data01_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Incub_Tabla_NacSemanalTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Tabla_NacSemanal"
    }
    final_data01_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacSemanal")
            
    print(f"agrega registros nuevos a la tabla ft_Incub_Tabla_NacSemanal : {cant_ingresonuevo01}")
    print(f"Total de registros en la tabla ft_Incub_Tabla_NacSemanal : {cant_total01}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Incub_Tabla_NacSemanalTemporal')
    print(f"Tabla ft_Incub_Tabla_NacSemanalTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Incub_Tabla_NacSemanal"
    }
    df_ft_Incub_Tabla_NacSemanal.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Incub_Tabla_NacSemanal")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()