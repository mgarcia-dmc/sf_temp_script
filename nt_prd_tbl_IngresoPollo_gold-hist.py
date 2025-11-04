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
JOB_NAME = "nt_prd_tbl_IngresoPollo_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime

print("inicia spark")
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, current_timestamp
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

file_name_target4 = f"{bucket_name_prdmtech}ft_Ingreso/"
file_name_target5 = f"{bucket_name_prdmtech}ft_Ingresocons/"

path_target4 = f"s3://{bucket_name_target}/{file_name_target4}"
path_target5 = f"s3://{bucket_name_target}/{file_name_target5}"
print('cargando rutas')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
#1 Inserta los datos de ingreso de pollo en la tabla temporal #Ingresobb
df_Ingresobb1 = spark.sql(f"SELECT \
                          PE.ComplexEntityNo \
                          ,(select distinct pk_empresa from {database_name}.lk_empresa where cempresa=1) as pk_empresa  \
                          ,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador \
                          ,nvl(LDV.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
                          ,nvl(LPL.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
                          ,nvl(LLT.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
                          ,nvl(LGP.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
                          ,nvl(LSX.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0))pk_sexo \
                          ,nvl(LZN.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
                          ,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona \
                          ,nvl(LES.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
                          ,nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
                          ,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha=Cast('1899-11-30' as date))) pk_tiempo \
                          ,nvl(LT.fecha,CAST('1899-11-30' AS date)) fecha \
                          ,BIN.CreationUserId AS usuario_creacion \
                          ,BIN.Quantity AS Inventario \
                          ,nvl(pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) pk_incubadora \
                          ,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
                          ,CAT.categoria \
                          ,nvl(AT.FlagAtipico,1) AS FlagAtipico \
                          ,0 FlagTransPavos \
                          ,PE.ComplexEntityNo SourceComplexEntityNo \
                          FROM {database_name}.si_brimentityinventory BIN \
                          LEFT JOIN {database_name}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
                          LEFT JOIN {database_name}.si_brimentities BE ON BE.ProteinEntitiesIRN = PE.IRN \
                          LEFT JOIN {database_name}.lk_plantel LPL ON LPL.IRN = CAST(PE.ProteinFarmsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LPL.ProteinCostCentersIRN \
                          LEFT JOIN {database_name}.lk_division LDV ON LDV.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR (50)) \
                          LEFT JOIN {database_name}.lk_zona LZN ON LZN.pk_zona = LPL.pk_zona \
                          LEFT JOIN {database_name}.lk_subzona LSZ ON LSZ.pk_subzona = LPL.pk_subzona \
                          LEFT JOIN {database_name}.lk_lote LLT ON LLT.pk_plantel=LPL.pk_plantel AND LLT.nlote = PE.EntityNo AND LLT.activeflag IN (0,1) \
                          LEFT JOIN {database_name}.lk_galpon LGP ON LGP.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) AND LGP.activeflag IN (false,true) \
                          LEFT JOIN {database_name}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRN = CAST(PE.ProteinProductsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_sexo LSX ON LSX.csexo = Pe.PenNo \
                          LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyy-MM-dd') =date_format(cast(BIN.xDate as timestamp),'yyyy-MM-dd') \
                          LEFT JOIN {database_name}.si_mvhimchicktrans MHT ON MHT.ProteinEntitiesIRN=PE.IRN AND BIN.Quantity=MHT.HeadPlaced AND MHT.RefNo = BIN.RefNo \
                          LEFT JOIN {database_name}.lk_incubadora LIC ON LIC.IRN=CAST(MHT.ProteinFacilityHatcheriesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.si_mvbrimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN  \
                          LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
                          LEFT JOIN {database_name}.categoria CAT ON LPL.pk_plantel = CAT.pk_plantel and LLT.pk_lote = CAT.pk_lote and LGP.pk_galpon = CAT.pk_galpon \
                          LEFT JOIN {database_name}.atipicos AT ON PE.ComplexEntityNo = AT.ComplexEntityNo \
                          WHERE BIN.Quantity > 0 \
                          AND BIN.EntityHistoryFlag = false \
                          AND SourceCode = 'CHICK' AND LDV.cdivision = 1") 
                         #3
print('carga df_Ingresobb1')
df_Ingresobb2 = spark.sql(f"SELECT \
                          PE.ComplexEntityNo \
                          ,(select distinct pk_empresa from {database_name}.lk_empresa where cempresa=1) as pk_empresa  \
                          ,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador \
                          ,nvl(LDV.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
                          ,nvl(LPL.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
                          ,nvl(LLT.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
                          ,nvl(LGP.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
                          ,nvl(LSX.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0))pk_sexo \
                          ,nvl(LZN.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
                          ,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona \
                          ,nvl(LES.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
                          ,nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
                          ,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo \
                          ,nvl(LT.fecha,cast('1899-11-30' as date)) fecha \
                          ,BIN.CreationUserId AS usuario_creacion \
                          ,BIN.Quantity AS Inventario \
                          ,nvl(pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) pk_incubadora \
                          ,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
                          ,CAT.categoria \
                          ,nvl(AT.FlagAtipico,1) AS FlagAtipico \
                          ,0 FlagTransPavos \
                          ,PE.ComplexEntityNo SourceComplexEntityNo \
                          FROM {database_name}.si_brimentityinventory BIN \
                          LEFT JOIN {database_name}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
                          LEFT JOIN {database_name}.si_brimentities BE ON BE.ProteinEntitiesIRN = PE.IRN \
                          LEFT JOIN {database_name}.lk_plantel LPL ON LPL.IRN = CAST(PE.ProteinFarmsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LPL.ProteinCostCentersIRN \
                          LEFT JOIN {database_name}.lk_division LDV ON LDV.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR (50)) \
                          LEFT JOIN {database_name}.lk_zona LZN ON LZN.pk_zona = LPL.pk_zona \
                          LEFT JOIN {database_name}.lk_subzona LSZ ON LSZ.pk_subzona = LPL.pk_subzona \
                          LEFT JOIN {database_name}.lk_lote LLT ON LLT.pk_plantel=LPL.pk_plantel AND LLT.nlote = PE.EntityNo AND LLT.activeflag IN (0,1) \
                          LEFT JOIN {database_name}.lk_galpon LGP ON LGP.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) AND LGP.activeflag IN (false,true) \
                          LEFT JOIN {database_name}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRN = CAST(PE.ProteinProductsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_sexo LSX ON LSX.csexo = Pe.PenNo \
                          LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyy-MM-dd') =date_format(cast(BIN.xDate as timestamp),'yyyy-MM-dd') \
                          LEFT JOIN {database_name}.si_mvhimchicktrans MHT ON MHT.ProteinEntitiesIRN=PE.IRN AND BIN.Quantity=MHT.HeadPlaced AND MHT.RefNo = BIN.RefNo \
                          LEFT JOIN {database_name}.lk_incubadora LIC ON LIC.IRN=CAST(MHT.ProteinFacilityHatcheriesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name}.si_mvbrimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN  \
                          LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
                          LEFT JOIN {database_name}.categoria CAT ON LPL.pk_plantel = CAT.pk_plantel and LLT.pk_lote = CAT.pk_lote and LGP.pk_galpon = CAT.pk_galpon \
                          LEFT JOIN {database_name}.atipicos AT ON PE.ComplexEntityNo = AT.ComplexEntityNo \
                          WHERE BIN.Quantity > 0 AND BIN.EntityHistoryFlag = false \
                          AND SourceCode = 'CHICK' AND LDV.cdivision = 2")
                                                        # 4
print('carga df_Ingresobb2')
df_Ingresobb = df_Ingresobb1.union(df_Ingresobb2)

df_ft_ingreso = df_Ingresobb

print('inicia carga de datos df_ft_ingreso ')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target4)  
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target4)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ingreso cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ingreso")



if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target4)
    data_after_delete = existing_data.filter(~((date_format(F.col("fecha"),"yyyy-MM-dd") >= fecha_str) ))
    filtered_new_data = df_ft_ingreso.filter((date_format(F.col("fecha"),"yyyy-MM-dd") >= fecha_str) )
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ingresoTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ingreso"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ingreso")
            
    print(f"agrega registros nuevos a la tabla ft_ingreso : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ingreso : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ingresoTemporal')
    print(f"Tabla ft_ingresoTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ingreso"
    }
    df_ft_ingreso.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ingreso")
##Actualizar las categorias que se registran en excel a nivel de Lote
#df_ft_ingreso_upd1 = spark.sql(f"SELECT \
#                          a.ComplexEntityNo \
#                          ,a.pk_empresa  \
#                          ,a.pk_administrador \
#                          ,a.pk_division \
#                          ,a.pk_plantel \
#                          ,a.pk_lote \
#                          ,a.pk_galpon \
#                          ,a.pk_sexo \
#                          ,a.pk_zona \
#                          ,a.pk_subzona \
#                          ,a.pk_especie \
#                          ,a.pk_producto \
#                          ,a.pk_tiempo \
#                          ,a.fecha \
#                          ,a.usuario_creacion \
#                          ,a.Inventario \
#                          ,a.pk_incubadora \
#                          ,a.pk_proveedor \
#                          ,CASE WHEN (a.categoria IS NULL OR a.categoria = '-') THEN nvl(c.categoria,'-') ELSE a.categoria END categoria \
#                          ,a.FlagAtipico \
#                          ,a.FlagTransPavos \
#                          ,a.SourceComplexEntityNo \
#                          from {database_name}.ft_ingreso a \
#                          left join {database_name}.lk_lote b on a.pk_lote = b.pk_lote \
#                          left join {database_name}.lk_ft_Excel_categoria c on c.lote = b.clote ")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoTemporal"
#}
#df_ft_ingreso_upd1.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.ft_ingresoTemporal")
#   
#   
#   #schema = existing_data.schema
#final_dataup1 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoTemporal")
#           
#           
#additional_options = {
#    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ingreso"
#}
#final_dataup1.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.ft_ingreso")
#
#print('actualizacion de ft_ingreso')
##update dmpecuario.ft_ingreso 
##set categoria = isnull(B.categoria,'-')
##from dmpecuario.ft_ingreso A
##left join [DMPECUARIO].[ft_Excel_categoria] B on A.idlote = B.idlote
##where idempresa =1 and (a.categoria is null or a.categoria ='-')
#
#
#tabla ft_ingresocons
df_ft_ingresocons = spark.sql(f"SELECT \
ComplexEntityNo \
,pk_empresa \
,pk_administrador \
,pk_division \
,pk_plantel \
,pk_lote \
,pk_galpon \
,pk_sexo \
,pk_zona \
,pk_subzona \
,pk_especie \
,pk_producto \
,MIN(fecha) fecha \
,MIN(pk_tiempo) pk_tiempo \
,pk_proveedor \
,SUM(Inventario)Inventario \
,categoria \
,FlagAtipico \
,FlagTransPavos \
,SourceComplexEntityNo \
FROM {database_name}.ft_Ingreso \
GROUP BY ComplexEntityNo,pk_empresa,pk_administrador,pk_division,pk_plantel,pk_lote,pk_galpon, \
pk_sexo,pk_zona, pk_subzona,pk_especie,pk_producto,pk_proveedor,categoria,FlagAtipico, \
FlagTransPavos ,SourceComplexEntityNo")

#date_format(fecha,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 15 MONTH), 'yyyyMM')
print(f"carga datos para ft_ingresocons")
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target5)   
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes5 = spark.read.format("parquet").load(path_target5)
    datos_existentes5 = True
    logger.info(f"Datos existentes de ft_Ingresocons cargados: {df_existentes5.count()} registros")
except:
    datos_existentes5 = False
    logger.info("No se encontraron datos existentes en ft_Ingresocons")



if datos_existentes5:
    existing_data5 = spark.read.format("parquet").load(path_target5)
    data_after_delete5 = existing_data5.filter(~((date_format(F.col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data5 = df_ft_ingresocons.filter((date_format(F.col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data5 = filtered_new_data5.union(data_after_delete5)                             
   
    cant_ingresonuevo5 = filtered_new_data5.count()
    cant_total5 = final_data5.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoconsTemporal"
    }
    final_data5.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_IngresoconsTemporal")
    
    
    #schema = existing_data.schema
    final_data5_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoconsTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Ingresocons"
    }
    final_data5_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Ingresocons")
            
    print(f"agrega registros nuevos a la tabla ft_Ingresocons : {cant_ingresonuevo5}")
    print(f"Total de registros en la tabla ft_Ingresocons : {cant_total5}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_IngresoconsTemporal')
    print(f"Tabla ft_IngresoconsTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Ingresocons"
    }
    df_ft_ingresocons.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Ingresocons")
##Actualizar las categorias que se registran en excel a nivel de Lote
#df_ft_ingresocons_upd1 = spark.sql(f"SELECT \
#a.ComplexEntityNo \
#,a.pk_empresa \
#,a.pk_administrador \
#,a.pk_division \
#,a.pk_plantel \
#,a.pk_lote \
#,a.pk_galpon \
#,a.pk_sexo \
#,a.pk_zona \
#,a.pk_subzona \
#,a.pk_especie \
#,a.pk_producto \
#,a.fecha \
#,a.pk_tiempo \
#,a.pk_proveedor \
#,a.Inventario \
#,CASE WHEN (a.categoria IS NULL OR a.categoria = '-') THEN nvl(c.categoria,'-') ELSE a.categoria END categoria \
#,a.FlagAtipico \
#,a.FlagTransPavos \
#,a.SourceComplexEntityNo \
#FROM {database_name}.ft_Ingreso a \
#left join {database_name}.lk_lote b on a.pk_lote = b.pk_lote \
#left join {database_name}.lk_ft_Excel_categoria c on c.lote = b.clote ")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoconsTemporal"
#}
#df_ft_ingresocons_upd1.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.ft_ingresoTemporal")
#   
#   
#   #schema = existing_data.schema
#final_dataup1 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_IngresoconsTemporal")
#           
#           
#additional_options = {
#    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ingresocons"
#}
#final_dataup1.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.ft_ingresocons")
#
#print('actualizacion de ft_ingresocons')
##date_format(fecha,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 15 MONTH), 'yyyyMM')
##print(f"carga datos para ft_ingresocons")
##update dmpecuario.ft_ingresocons 
##set categoria = isnull(B.categoria,'-')
##from dmpecuario.ft_ingresocons A
##left join [DMPECUARIO].[ft_Excel_categoria] B on A.idlote = B.idlote
##where idempresa =1 and (a.categoria is null or a.categoria ='-')
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()