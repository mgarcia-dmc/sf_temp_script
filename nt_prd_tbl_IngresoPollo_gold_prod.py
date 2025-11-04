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
JOB_NAME = "nt_prd_tbl_IngresoPollo_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from datetime import datetime
from pyspark.sql.functions import *
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
print("inicia spark")
# Parámetros de entrada global
database_name_tmp = "bi_sf_tmp"
database_name_br ="mtech_prd_sf_br"
database_name_si ="mtech_prd_sf_si"
database_name_gl ="mtech_prd_sf_gl"
bucket_name_target = "ue1stgprodas3dtl005-gold"
bucket_name_source = "ue1stgprodas3dtl005-silver"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

table_name4 = "ft_ingreso"
table_name5 = "ft_ingresocons"
file_name_target4 = f"{bucket_name_prdmtech}{table_name4}/"
file_name_target5 = f"{bucket_name_prdmtech}{table_name5}/"

path_target4 = f"s3://{bucket_name_target}/{file_name_target4}"
path_target5 = f"s3://{bucket_name_target}/{file_name_target5}"
print('cargando rutas')
#1 Inserta los datos de ingreso de pollo en la tabla temporal #Ingresobb
df_Ingresobb1 = spark.sql(f"SELECT \
                          PE.ComplexEntityNo \
                          ,(select pk_empresa from {database_name_gl}.lk_empresa where cempresa=1) as pk_empresa  \
                          ,nvl(LAD.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0')) pk_administrador \
                          ,nvl(LDV.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) pk_division \
                          ,nvl(LPL.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) pk_plantel \
                          ,nvl(LLT.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) pk_lote \
                          ,nvl(LGP.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) pk_galpon \
                          ,nvl(LSX.pk_sexo,(select pk_sexo from {database_name_gl}.lk_sexo where csexo=0))pk_sexo \
                          ,nvl(LZN.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) pk_zona \
                          ,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) pk_subzona \
                          ,nvl(LES.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie \
                          ,nvl(LPR.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto \
                          ,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha=Cast('1899-11-30' as date))) pk_tiempo \
                          ,nvl(pk_incubadora,(select pk_incubadora from {database_name_gl}.lk_incubadora where cincubadora='0')) pk_incubadora \
                          ,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
                          ,BIN.CreationUserId AS usuario_creacion \
                          ,BIN.Quantity AS Inventario \
                          ,CAT.categoria \
                          ,nvl(AT.FlagAtipico,1) AS FlagAtipico \
                          ,0 FlagTransPavos \
                          ,PE.ComplexEntityNo SourceComplexEntityNo \
                          ,LT.fecha descripfecha \
                          ,(select nempresa from {database_name_gl}.lk_empresa where cempresa=1) descripempresa \
                          ,LDV.ndivision descripdivision \
                          ,LZN.nzona descripzona \
                          ,LSZ.nsubzona descripsubzona \
                          ,LPL.cplantel plantel \
                          ,LLT.clote lote \
                          ,LGP.nogalpon galpon \
                          ,LSX.nsexo descripsexo \
                          ,LPR.nproducto descripproducto \
                          ,LES.cespecie descripespecie \
                          ,LAD.nadministrador descripadministrador \
                          ,PRO.nproveedor descripproveedor \
                          FROM {database_name_si}.si_brimentityinventory BIN \
                          LEFT JOIN {database_name_si}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
                          LEFT JOIN {database_name_si}.si_brimentities BE ON BE.ProteinEntitiesIRN = PE.IRN \
                          LEFT JOIN {database_name_gl}.lk_plantel LPL ON LPL.IRN = CAST(PE.ProteinFarmsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_si}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LPL.ProteinCostCentersIRN \
                          LEFT JOIN {database_name_gl}.lk_division LDV ON LDV.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR (50)) \
                          LEFT JOIN {database_name_gl}.lk_zona LZN ON LZN.pk_zona = LPL.pk_zona \
                          LEFT JOIN {database_name_gl}.lk_subzona LSZ ON LSZ.pk_subzona = LPL.pk_subzona \
                          LEFT JOIN {database_name_gl}.lk_lote LLT ON LLT.pk_plantel=LPL.pk_plantel AND LLT.nlote = PE.EntityNo AND LLT.activeflag IN (0,1) \
                          LEFT JOIN {database_name_gl}.lk_galpon LGP ON LGP.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) AND LGP.activeflag IN (false,true) \
                          LEFT JOIN {database_name_gl}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_producto LPR ON LPR.IRN = CAST(PE.ProteinProductsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_sexo LSX ON LSX.csexo = Pe.PenNo \
                          LEFT JOIN {database_name_gl}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_tiempo LT ON date_format(LT.fecha,'yyyy-MM-dd') =date_format(cast(BIN.xDate as timestamp),'yyyy-MM-dd') \
                          LEFT JOIN {database_name_si}.si_mvhimchicktrans MHT ON MHT.ProteinEntitiesIRN=PE.IRN AND BIN.Quantity=MHT.HeadPlaced AND MHT.RefNo = BIN.RefNo \
                          LEFT JOIN {database_name_gl}.lk_incubadora LIC ON LIC.IRN=CAST(MHT.ProteinFacilityHatcheriesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_si}.si_mvbrimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN  \
                          LEFT JOIN {database_name_gl}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
                          LEFT JOIN {database_name_tmp}.categoria CAT ON LPL.pk_plantel = CAT.pk_plantel and LLT.pk_lote = CAT.pk_lote and LGP.pk_galpon = CAT.pk_galpon \
                          LEFT JOIN {database_name_tmp}.atipicos AT ON PE.ComplexEntityNo = AT.ComplexEntityNo \
                          WHERE BIN.Quantity > 0 \
                          AND BIN.EntityHistoryFlag = false \
                          AND (date_format(cast(BIN.EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                          AND SourceCode = 'CHICK' AND LDV.cdivision = '1'") 
                         #3
print('carga df_Ingresobb1', df_Ingresobb1.count())
df_Ingresobb2 = spark.sql(f"SELECT \
                          PE.ComplexEntityNo \
                          ,(select distinct pk_empresa from {database_name_gl}.lk_empresa where cempresa=1) as pk_empresa  \
                          ,nvl(LAD.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0')) pk_administrador \
                          ,nvl(LDV.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) pk_division \
                          ,nvl(LPL.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) pk_plantel \
                          ,nvl(LLT.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) pk_lote \
                          ,nvl(LGP.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) pk_galpon \
                          ,nvl(LSX.pk_sexo,(select pk_sexo from {database_name_gl}.lk_sexo where csexo=0))pk_sexo \
                          ,nvl(LZN.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) pk_zona \
                          ,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) pk_subzona \
                          ,nvl(LES.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie \
                          ,nvl(LPR.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto \
                          ,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo \
                          ,nvl(pk_incubadora,(select pk_incubadora from {database_name_gl}.lk_incubadora where cincubadora='0')) pk_incubadora \
                          ,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
                          ,BIN.CreationUserId AS usuario_creacion \
                          ,BIN.Quantity AS Inventario \
                          ,CAT.categoria \
                          ,nvl(AT.FlagAtipico,1) AS FlagAtipico \
                          ,0 FlagTransPavos \
                          ,PE.ComplexEntityNo SourceComplexEntityNo \
                          ,LT.fecha descripfecha \
                          ,(select nempresa from {database_name_gl}.lk_empresa where cempresa=1) descripempresa \
                          ,LDV.ndivision descripdivision \
                          ,LZN.nzona descripzona \
                          ,LSZ.nsubzona descripsubzona \
                          ,LPL.cplantel plantel \
                          ,LLT.clote lote \
                          ,LGP.nogalpon galpon \
                          ,LSX.nsexo descripsexo \
                          ,LPR.nproducto descripproducto \
                          ,LES.cespecie descripespecie \
                          ,LAD.nadministrador descripadministrador \
                          ,PRO.nproveedor descripproveedor \
                          FROM {database_name_si}.si_brimentityinventory BIN \
                          LEFT JOIN {database_name_si}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
                          LEFT JOIN {database_name_si}.si_brimentities BE ON BE.ProteinEntitiesIRN = PE.IRN \
                          LEFT JOIN {database_name_gl}.lk_plantel LPL ON LPL.IRN = CAST(PE.ProteinFarmsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_si}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LPL.ProteinCostCentersIRN \
                          LEFT JOIN {database_name_gl}.lk_division LDV ON LDV.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR (50)) \
                          LEFT JOIN {database_name_gl}.lk_zona LZN ON LZN.pk_zona = LPL.pk_zona \
                          LEFT JOIN {database_name_gl}.lk_subzona LSZ ON LSZ.pk_subzona = LPL.pk_subzona \
                          LEFT JOIN {database_name_gl}.lk_lote LLT ON LLT.pk_plantel=LPL.pk_plantel AND LLT.nlote = PE.EntityNo AND LLT.activeflag IN (0,1) \
                          LEFT JOIN {database_name_gl}.lk_galpon LGP ON LGP.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) AND LGP.activeflag IN (false,true) \
                          LEFT JOIN {database_name_gl}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_producto LPR ON LPR.IRN = CAST(PE.ProteinProductsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_sexo LSX ON LSX.csexo = Pe.PenNo \
                          LEFT JOIN {database_name_gl}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_gl}.lk_tiempo LT ON date_format(LT.fecha,'yyyy-MM-dd') =date_format(cast(BIN.xDate as timestamp),'yyyy-MM-dd') \
                          LEFT JOIN {database_name_si}.si_mvhimchicktrans MHT ON MHT.ProteinEntitiesIRN=PE.IRN AND BIN.Quantity=MHT.HeadPlaced AND MHT.RefNo = BIN.RefNo \
                          LEFT JOIN {database_name_gl}.lk_incubadora LIC ON LIC.IRN=CAST(MHT.ProteinFacilityHatcheriesIRN AS VARCHAR(50)) \
                          LEFT JOIN {database_name_si}.si_mvbrimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN  \
                          LEFT JOIN {database_name_gl}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
                          LEFT JOIN {database_name_tmp}.categoria CAT ON LPL.pk_plantel = CAT.pk_plantel and LLT.pk_lote = CAT.pk_lote and LGP.pk_galpon = CAT.pk_galpon \
                          LEFT JOIN {database_name_tmp}.atipicos AT ON PE.ComplexEntityNo = AT.ComplexEntityNo \
                          WHERE BIN.Quantity > 0 AND BIN.EntityHistoryFlag = false \
                          AND (date_format(cast(BIN.EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                          AND SourceCode = 'CHICK' AND LDV.cdivision = '2'")
                                                        # 4
print('carga df_Ingresobb2', df_Ingresobb2.count())
df_Ingresobb = df_Ingresobb1.union(df_Ingresobb2)
df_ft_ingreso = df_Ingresobb
print('inicia carga de datos df_ft_ingreso ',df_ft_ingreso.count())
# Verificar si la tabla gold ya existe
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target4)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ingreso cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name4}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target4)
    data_after_delete = existing_data.filter(~((date_format(F.col("descripfecha"),"yyyy-MM-dd") >= fecha_str) ))
    filtered_new_data = df_ft_ingreso.filter((date_format(F.col("descripfecha"),"yyyy-MM-dd") >= fecha_str) )
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name4}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_ingresoTemporal")
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name4}Temporal")
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name4}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.{table_name4}")
            
    print(f"agrega registros nuevos a la tabla {table_name4} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name4} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_ingresoTemporal')
    print(f"Tabla {table_name4}Temporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name4}"
    }
    df_ft_ingreso.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.{table_name4}")
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
,MIN(pk_tiempo) pk_tiempo \
,pk_proveedor \
,SUM(Inventario)Inventario \
,categoria \
,FlagAtipico \
,FlagTransPavos \
,SourceComplexEntityNo \
,min(descripfecha) descripfecha \
,descripempresa \
,descripdivision \
,descripzona \
,descripsubzona \
,plantel \
,lote \
,galpon \
,descripsexo \
,descripproducto \
,descripespecie \
,descripadministrador \
,descripproveedor \
FROM {database_name_gl}.ft_ingreso \
WHERE (date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
GROUP BY ComplexEntityNo,pk_empresa,pk_administrador,pk_division,pk_plantel,pk_lote,pk_galpon, \
pk_sexo,pk_zona, pk_subzona,pk_especie,pk_producto,pk_proveedor,categoria,FlagAtipico, \
FlagTransPavos ,SourceComplexEntityNo,descripempresa,descripdivision,descripzona,descripsubzona,plantel,lote,galpon,descripsexo,descripproducto,descripespecie,descripadministrador,descripproveedor")
print(f"carga datos para ft_ingresocons", df_ft_ingresocons.count())
# Verificar si la tabla gold ya existe 
fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes5 = spark.read.format("parquet").load(path_target5)
    datos_existentes5 = True
    logger.info(f"Datos existentes de {table_name5} cargados: {df_existentes5.count()} registros")
except:
    datos_existentes5 = False
    logger.info(f"No se encontraron datos existentes en {table_name5}")

if datos_existentes5:
    existing_data5 = spark.read.format("parquet").load(path_target5)
    data_after_delete5 = existing_data5.filter(~((date_format(F.col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data5 = df_ft_ingresocons.filter((date_format(F.col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data5 = filtered_new_data5.union(data_after_delete5)                             
   
    cant_ingresonuevo5 = filtered_new_data5.count()
    cant_total5 = final_data5.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name5}Temporal"
    }
    final_data5.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.{table_name5}Temporal")
    
    #schema = existing_data.schema
    final_data5_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name5}Temporal")

    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name5}"
    }
    final_data5_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.{table_name5}")
            
    print(f"agrega registros nuevos a la tabla {table_name5} : {cant_ingresonuevo5}")
    print(f"Total de registros en la tabla {table_name5} : {cant_total5}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_IngresoconsTemporal')
    print(f"Tabla {table_name5}Temporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name5}"
    }
    df_ft_ingresocons.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.{table_name5}")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()