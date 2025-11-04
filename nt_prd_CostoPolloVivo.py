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
JOB_NAME = "nt_prd_CostoPolloVivo"

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

file_name_target1 = f"{bucket_name_prdmtech}ft_costoPollo/"
path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
#database_name = "default"
print('cargando ruta')
df_Costo = spark.sql(f"""select PF.FarmName, 
PF.FarmNo, 
PE.EntityNo, 
A.AccountNo as Location, 
S.AccountNo as Estado, 
U.AccountNo as Usuario, 
CO.AccountNo as CostObj, 
E.AccountNo as Element, 
MVPJ.SourceCode, 
(MVPJ.RelativeAmount * -1) as Amount, 
month(MVPJ.xDate) as Mes, 
year(MVPJ.xDate) as Anio, 
MVPJ.TransCode, 
MVPJ.xDate, 
1 as Empresa, 
'' categoria, 
0 pk_estado, 
'' zona, 
'' subzona, 
'' administrador, 
'' division, 
'' tipoproducto, 
'' especie, 
'' proveedor, 
0 pk_diasvida, 
MVPJ.CompanyNo, 
MVPJ.CostCenterNo, 
'' Seccion,
'' Tipo,
0.0 KilosRendidos,
0.0 MontoEliminados,
0.0 Variacion,
'' status,
'' Standard,
0.0 MontoSinEliminacion,
'' Clase,
0 PolloNacidos,
0 ProdHuevoInc
from {database_name_costos_si}.si_mvproteinjournaltrans MVPJ 
inner join {database_name_si}.si_proteinchartofaccounts PCA on MVPJ.ProteinChartOfAccountsIRN = PCA.IRN 
left JOIN {database_name_si}.si_proteinaccounts A ON A.IRN = PCA.ProteinAccountsIRN_Location 
left JOIN {database_name_si}.si_proteinaccounts S ON S.IRN = PCA.ProteinAccountsIRN_Stages 
left JOIN {database_name_si}.si_proteinaccounts U ON U.IRN = PCA.ProteinAccountsIRN_User 
left join {database_name_si}.si_proteinaccounts CO ON CO.IRN = PCA.ProteinAccountsIRN_CostObjects 
left join {database_name_si}.si_proteinaccounts E ON E.IRN = PCA.ProteinAccountsIRN_Elements 
inner join {database_name_si}.si_proteinentities PE on PE.IRN = MVPJ.ProteinEntitiesIRN 
inner join {database_name_si}.si_proteinfarms PF on PF.IRN = MVPJ.ProteinFarmsIRN 
WHERE date_format(cast(MVPJ.xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') 
and MVPJ.SourceCode in ('EOP BRIM - Farm>Farm','EOP BRIM - Farm>Plant') 
and A.AccountNo='BRLR' AND S.AccountNo='GROW' AND U.AccountNo='OUT' and MVPJ.CompanyNo = 'SAN FERNANDO' AND MVPJ.DivisionNo = 1""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costo"
}
df_Costo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costo")
print('carga temporal costo', df_Costo.count())
# Tabla temporal para anexar las dimensiones de los costos
df_ft_consolidado_lote = spark.sql(f"""select 
CC.pk_empresa, 
rtrim(ltrim(substring(CC.ComplexEntityNo,1,4))) FarmNo, 
rtrim(ltrim(SubString(CC.ComplexEntityNo,6,4))) Entity, 
CC.categoria, 
CC.pk_estado, 
LZ.czona, 
LSZ.nsubzona, 
LA.nadministrador, 
LD.ndivision, 
LTP.ntipoproducto, 
LE.nespecie, 
LPV.nproveedor, 
LDV.pk_diasvida 
from {database_name_gl}.ft_consolidado_Lote CC 
left join {database_name_gl}.lk_zona LZ on CC.pk_zona = LZ.pk_zona 
left join {database_name_gl}.lk_subzona LSZ on CC.pk_subzona = LSZ.pk_subzona 
left join {database_name_gl}.lk_administrador LA on CC.pk_administrador = LA.pk_administrador 
left join {database_name_gl}.lk_division LD on CC.pk_division = LD.pk_division 
left join {database_name_gl}.lk_tipoproducto LTP on CC.pk_tipoproducto = LTP.pk_tipoproducto 
left join {database_name_gl}.lk_especie LE on CC.pk_especie = LE.pk_especie 
left join {database_name_gl}.lk_proveedor LPV on CC.pk_proveedor = LPV.pk_proveedor 
left join {database_name_gl}.lk_diasvida LDV on CC.pk_diasvida = LDV.pk_diasvida 
where date_format(cast(CC.descripfecha as date),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -2)), 'yyyyMM')
and CC.pk_empresa = 1 
group by CC.pk_empresa,rtrim(ltrim(substring(CC.ComplexEntityNo,1,4))),rtrim(ltrim(SubString(CC.ComplexEntityNo,6,4))), 
CC.categoria,CC.pk_estado,LZ.czona,LSZ.nsubzona,LA.nadministrador,LD.ndivision,LTP.ntipoproducto,LE.nespecie,LPV.nproveedor,LDV.pk_diasvida""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_lote_temp"
}
df_ft_consolidado_lote.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_consolidado_lote_temp")
print('carga temporal ft_consolidado_lote_temp', df_ft_consolidado_lote.count())
df_ft_costoPollo =spark.sql(f"""select 
 A.FarmName 
,A.FarmNo 
,A.EntityNo Entity 
,A.Location 
,A.Estado 
,A.Usuario 
,A.CostObj
,A.Element 
,A.SourceCode 
,A.Amount 
,A.Mes 
,A.Anio 
,A.TransCode 
,A.xDate Fecha 
,A.Empresa
,'' Tipo
,'' Variacion
,B.categoria 
,B.pk_estado Status
,B.czona zona 
,B.nsubzona subzona 
,B.nadministrador administrador 
,B.ndivision division 
,'' Producto
,B.ntipoproducto tipoproducto 
,'' Standar
,B.nespecie especie 
,B.nproveedor proveedor 
,B.pk_diasvida
,0.0 KilosRendidos
,'' Seccion
,0.0 MontoEliminados
,0.0 MontoSinEliminacion
,'' Clase
,0 PolloNacidos
,0 ProdHuevoInc
,A.CostCenterNo CentroCosto 
,A.CompanyNo DescripcionEmpresa 
from {database_name_costos_tmp}.costo A 
left join {database_name_costos_tmp}.ft_consolidado_lote_temp B on A.FarmNo = B.FarmNo and A.EntityNo = B.Entity""")
print('carga df_ft_costoPollo', df_ft_costoPollo.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(col("Fecha"),"yyyyMM")== fecha_str) & (col("Seccion") == "ENGORDE") & (col("Empresa") == "1") ))
    filtered_new_data = df_ft_costoPollo #.filter((date_format(col("Fecha"),"yyyyMM")== fecha_str) & (col("Seccion") == "ENGORDE") & (col("Empresa") == "1"))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
fecha_menos = datetime.now() - relativedelta(months=1)
aniomes_ant = fecha_menos.strftime("%Y%m")
print(aniomes_ant)
#actualizacion
df_ft_costoPollo_upd1 =spark.sql(f"""select 
 FarmName 
,FarmNo 
,Entity 
,Location 
,Estado 
,Usuario 
,CostObj 
,Element 
,SourceCode 
,Amount 
,Mes 
,Anio 
,TransCode 
,Fecha 
,Empresa 
,CASE WHEN Location='BRLR' AND Estado='GROW' and CostObj='FDCONS' and (Element='MAN' OR Element='ING') AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'AABB' WHEN Location='BRLR' AND Estado='GROW' and CostObj='INV' and (Element='BRLRFEMALE' OR Element='BRLRMALE' ) AND (Usuario='OUT' OR Usuario='INV') 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'POLLOBB' WHEN Location='BRLR' AND Estado='GROW' and CostObj='GAS' and Element='GAS' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'GAS' WHEN Location='BRLR' AND Estado='GROW' and CostObj='AGUA' and Element='AGUA' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'AGUA' WHEN Location='BRLR' AND Estado='GROW' and CostObj='CAMA' and Element='CAMA' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'PAJILLA' WHEN Location='BRLR' AND Estado='GROW' and CostObj='MEDVITVAC' and Element='VACC' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'VACUNAS' WHEN Location='BRLR' AND Estado='GROW' and CostObj='MEDVITVAC' and Element='MED' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'MEDICINAS' WHEN Location='BRLR' AND Estado='GROW' and CostObj='TRANS' and Element='TRANS' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Transporte' WHEN Location='BRLR' AND Estado='GROW' and CostObj='SECR' and Element='SECR' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'ServiciosCrianza' WHEN Location='BRLR' AND Estado='GROW' and CostObj='ALQU' and Element='ALQU' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Alquileres' WHEN Location='BRLR' AND Estado='GROW' and CostObj='ELEC' and Element='ELEC' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Electricidad' WHEN Location='BRLR' AND Estado='GROW' and CostObj='MEDVITVAC' and Element='VIT' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'VITAMINAS' WHEN Location='BRLR' AND Estado='GROW' and CostObj='REPM' and Element='REPM' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Mantenimiento' WHEN Location='BRLR' AND Estado='GROW' and CostObj='SUMI' and Element='SUMI' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'SuminitrosDiversos' WHEN Location='BRLR' AND Estado='GROW' and CostObj='GPAY' and Element='GPAYBAS' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'SuminitrosDiversos' WHEN Location='BRLR' AND Estado='GROW' and CostObj='SUMI' and Element='SUMI1' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'SuminitrosDiversos' WHEN Location='BRLR' AND Estado='GROW' and CostObj='SUPPLIES' and Element='MISC' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'SuminitrosDiversos' WHEN Location='BRLR' AND Estado='GROW' and (CostObj='SERV' or CostObj='GPAY') and Element='SERV' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Servicios' WHEN Location='BRLR' AND Estado='GROW' and CostObj='DEPA' and Element='DEPA' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'Depreciación' WHEN Location='BRLR' AND Estado='GROW' and CostObj='MAOB' and Element='MAOB' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'MANOOBRA' WHEN Location='BRLR' AND Estado='GROW' and CostObj='INDR' and Element='INDR' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'INDIRECTOS' WHEN Location='BRLR' AND Estado='GROW' and CostObj='INDREC' and Element='INDREC' AND Usuario='OUT' 
and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'INDIRECTOSREC' ELSE tipo END tipo 
,Variacion
,categoria 
,Status 
,zona 
,subzona 
,administrador 
,division 
,Producto
,tipoproducto 
,Standar
,especie 
,proveedor 
,pk_diasvida 
,KilosRendidos
,CASE WHEN Location='BRLR' and Empresa = 1 and date_format(cast(Fecha as timestamp),'yyyyMM') = {aniomes_ant} THEN 'ENGORDE' ELSE Seccion END Seccion 
,MontoEliminados 
,MontoSinEliminacion 
,Clase 
,PolloNacidos 
,ProdHuevoInc
,CentroCosto
,DescripcionEmpresa
from {database_name_costos_gl}.ft_costoPollo """)
print('carga df_ft_costoPollo_upd1', df_ft_costoPollo_upd1.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd1
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_costoPollo_upd2 =spark.sql(f"""
WITH kilos as (select date_format(cast(xdate as timestamp),'yyyyMM') pk_mes,SUM(FarmWtNet) as KilosRendidos 
from {database_name_costos_si}.si_mvpmtsprocrecvtranshousedetail 
where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') and PlantNo not in ('LABORATORIO','PLANTAE') AND VoidFlag=0 and FarmType=1 
group by date_format(cast(xdate as timestamp),'yyyyMM'))
select 
 FarmName 
,FarmNo 
,Entity 
,Location 
,Estado 
,Usuario 
,CostObj 
,Element 
,SourceCode 
,Amount 
,Mes 
,Anio 
,TransCode 
,Fecha 
,Empresa 
,tipo 
,Variacion
,categoria 
,Status 
,zona 
,subzona 
,administrador 
,division 
,Producto
,tipoproducto 
,Standar
,especie 
,proveedor 
,pk_diasvida 
,CASE WHEN Empresa = 1 and seccion = 'ENGORDE' and date_format(cast(Fecha as timestamp),'yyyyMM') = pk_mes THEN b.KilosRendidos ELSE a.KilosRendidos END KilosRendidos
,Seccion 
,MontoEliminados
,MontoSinEliminacion 
,Clase 
,PolloNacidos 
,ProdHuevoInc
,CentroCosto
,DescripcionEmpresa
from {database_name_costos_gl}.ft_costoPollo a 
left join Kilos b on date_format(cast(a.fecha as timestamp),'yyyyMM') = b.pk_mes
""")
print('carga df_ft_costoPollo_upd2', df_ft_costoPollo_upd2.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd2
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_Eliminados = spark.sql(f"""select 
FarmNo, 
EntityNo, 
xDate, 
SUM(FarmWtNet) as KilosRendidos 
from {database_name_costos_si}.si_mvpmtsprocrecvtranshousedetail 
where date_format(cast(xdate as timestamp),'yyyyMM') = date_format(add_months(trunc(current_date, 'month'),-1),'yyyyMM') 
and PlantNo in ('PLANTAE') and VoidFlag=0 and FarmType=1 and PostStatus=2 
group by FarmNo,EntityNo,xDate""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Eliminados"
}
df_Eliminados.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Eliminados")
print('carga temporal Eliminados', df_Eliminados.count())
df_EliminadosTipo = spark.sql(f"""select 
C.FarmNo, 
C.EntityNo, 
C.xDate, 
C.Amount, 
C.CostObj, 
Element, 
Mes, 
Anio, 
E.KilosRendidos 
from {database_name_costos_tmp}.Eliminados E 
inner join {database_name_costos_tmp}.costo C on E.FarmNo =  C.FarmNo and E.EntityNo = C.EntityNo and E.xDate = C.xDate""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EliminadosTipo"
}
df_EliminadosTipo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.EliminadosTipo")
print('carga temporal EliminadosTipo', df_EliminadosTipo.count())
df_EliminadosMonto = spark.sql(f"""select 
sum(ET.Amount) as Monto, 
ET.CostObj, 
Mes, 
Anio, 
Max(KilosRendidos) as kilosRendidos 
from {database_name_costos_tmp}.EliminadosTipo ET 
group by ET.CostObj, Mes,Anio""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EliminadosMonto"
}
df_EliminadosMonto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.EliminadosMonto")
print('carga temporal EliminadosMonto', df_EliminadosMonto.count())
df_EliminadosMonto1 = spark.sql(f"""select sum(Amount) as Monto, 
CostObj, 
Element,
Mes, 
Anio, 
Max(KilosRendidos) as kilosRendidos 
from {database_name_costos_tmp}.EliminadosTipo 
group by CostObj,Element,Mes,Anio""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EliminadosMonto1"
}
df_EliminadosMonto1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.EliminadosMonto1")
print('carga temporal EliminadosMonto1', df_EliminadosMonto1.count())
df_ft_costoPollo_upd3 =spark.sql(f"""
SELECT
 CP.FarmName
,CP.FarmNo
,CP.Entity
,CP.Location
,CP.Estado
,CP.Usuario
,CP.CostObj
,CP.Element
,CP.SourceCode
,CP.Amount
,CP.mes
,CP.anio
,CP.TransCode
,CP.Fecha
,CP.Empresa
,CP.Tipo
,CP.Variacion
,CP.Categoria
,CP.Status
,CP.Zona
,CP.SubZona
,CP.Administrador
,CP.Division
,CP.Producto
,CP.TipoProducto
,CP.Standar
,CP.Especie
,CP.Proveedor
,CP.pk_diasvida
,CP.KilosRendidos
,CP.Seccion
,CASE WHEN trim(CP.CostObj) = 'MEDVITVAC' AND trim(CP.Element) = trim(EM1.Element) AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' THEN EM1.Monto
    WHEN trim(CP.CostObj) <> 'MEDVITVAC' AND trim(CP.CostObj) = trim(EM.CostObj) AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' AND CP.Empresa = 1 AND trim(CP.Seccion) = 'ENGORDE' THEN EM.Monto
    WHEN (CP.MontoEliminados IS NULL or CP.MontoEliminados ='') AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = {aniomes_ant} AND CP.Empresa = 1 AND trim(CP.Seccion) = 'ENGORDE' THEN 0.0
    ELSE CP.MontoEliminados END AS MontoEliminados
,CP.MontoSinEliminacion
,CP.Clase
,CP.PolloNacidos
,CP.ProdHuevoInc
,CP.CentroCosto
,CP.DescripcionEmpresa
FROM {database_name_costos_gl}.ft_costoPollo AS CP 
LEFT JOIN {database_name_costos_tmp}.EliminadosMonto AS EM ON trim(CP.CostObj) = trim(EM.CostObj)
LEFT JOIN {database_name_costos_tmp}.EliminadosMonto1 AS EM1 ON trim(CP.CostObj) = trim(EM1.CostObj)
""")
print('carga df_ft_costoPollo_upd3' ,df_ft_costoPollo_upd3.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd3
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_costoRepro = spark.sql(f"""select 
PF.FarmName, 
PF.FarmNo, 
PE.EntityNo Entity, 
A.AccountNo as Location, 
S.AccountNo as Estado, 
U.AccountNo as Usuario, 
CO.AccountNo as CostObj, 
E.AccountNo as Element, 
MVPJ.SourceCode, 
(MVPJ.RelativeAmount * -1) as Amount, 
month(MVPJ.xDate) as Mes, 
year(MVPJ.xDate) as Anio, 
MVPJ.TransCode, 
MVPJ.xDate Fecha, 
1 as Empresa,  
MVPJ.CompanyNo DescripcionEmpresa, 
MVPJ.CostCenterNo CentroCosto
from {database_name_costos_si}.si_mvproteinjournaltrans MVPJ inner join 
{database_name_si}.si_proteinchartofaccounts PCOA on MVPJ.ProteinChartOfAccountsIRN = PCOA.IRN 
left JOIN {database_name_si}.si_proteinaccounts A ON A.IRN=PCOA.ProteinAccountsIRN_Location 
left JOIN {database_name_si}.si_proteinaccounts S ON S.IRN=PCOA.ProteinAccountsIRN_Stages 
left JOIN {database_name_si}.si_proteinaccounts U ON U.IRN=PCOA.ProteinAccountsIRN_User 
left join {database_name_si}.si_proteinaccounts CO ON CO.IRN=PCOA.ProteinAccountsIRN_CostObjects 
left join {database_name_si}.si_proteinaccounts E ON E.IRN=PCOA.ProteinAccountsIRN_Elements 
inner join {database_name_si}.si_proteinentities PE on PE.IRN=MVPJ.ProteinEntitiesIRN 
inner join {database_name_si}.si_proteinfarms PF on PF.IRN=MVPJ.ProteinFarmsIRN 
where date_format(cast(xdate as timestamp),'yyyyMM') = date_format(add_months(trunc(current_date, 'month'),-1),'yyyyMM') 
and A.AccountNo='BRDR' AND S.AccountNo='LAY' AND E.AccountNo<>'EGGS' 
and MVPJ.CompanyNo = 'SAN FERNANDO' 
AND MVPJ.DivisionNo = 1""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costoRepro"
}
df_costoRepro.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costoRepro")
print('carga temporal costoRepro', df_costoRepro.count())
df_insert_costopollo = spark.sql(f"""select 
 FarmName
,FarmNo
,Entity
,Location
,Estado
,Usuario
,CostObj
,Element
,SourceCode
,Amount
,mes
,anio
,TransCode
,Fecha
,Empresa
,'' Tipo
,'' Variacion
,'' Categoria
,'' Status
,'' Zona
,'' SubZona
,'' Administrador
,'' Division
,'' Producto
,'' TipoProducto
,'' Standar
,'' Especie
,'' Proveedor
,'' Idvida
,0.0 KilosRendidos
,'' Seccion
,0.0 MontoEliminados
,0.0 MontoSinEliminacion
,'' Clase
,0 PolloNacidos
,0 ProdHuevoInc
,CentroCosto
,DescripcionEmpresa
from {database_name_costos_tmp}.costoRepro """)
print('carga df_insert_costopollo' ,df_insert_costopollo.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(col("Fecha"),"yyyyMM")== fecha_str) & (col("Seccion") == "REPRODUCTORAS") & (col("Empresa") == "1") ))
    filtered_new_data = df_insert_costopollo #.filter((date_format(col("Fecha"),"yyyyMM")== fecha_str) & (col("Seccion") == "ENGORDE") & (col("Empresa") == "1"))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_costoPollo_upd4 =spark.sql(f"""
SELECT
 CP.FarmName
,CP.FarmNo
,CP.Entity
,CP.Location
,CP.Estado
,CP.Usuario
,CP.CostObj
,CP.Element
,CP.SourceCode
,CP.Amount
,CP.mes
,CP.anio
,CP.TransCode
,CP.Fecha
,CP.Empresa
,CASE WHEN trim(CP.Location) = 'BRDR' AND CP.Estado = 'LAY' AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' AND CP.Empresa = 1
	AND (CASE WHEN trim(CP.Location) = 'BRDR' AND CP.Empresa = 1 AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' THEN 'REPRODUCTORAS' ELSE CP.Seccion END) = 'REPRODUCTORAS'
	THEN CASE WHEN trim(CP.CostObj) = 'FDCONS' AND (trim(CP.Element) = 'MAN' OR trim(CP.Element) = 'ING') THEN 'AABB'
			WHEN trim(CP.CostObj) = 'GAS' AND trim(CP.Element) = 'GAS' THEN 'GAS'
			WHEN trim(CP.CostObj) = 'AGUA' AND trim(CP.Element) = 'AGUA' THEN 'AGUA'
			WHEN trim(CP.CostObj) = 'CAMA' AND trim(CP.Element) = 'CAMA' THEN 'PAJILLA'
			WHEN trim(CP.CostObj) = 'MEDVITVAC' THEN 'MEDICINAS'
			WHEN trim(CP.CostObj) = 'TRANS' THEN 'Transporte'
			WHEN trim(CP.CostObj) = 'ALQU' THEN 'Alquileres'
			WHEN trim(CP.CostObj) = 'REPM' THEN 'Mantenimiento'
			WHEN trim(CP.CostObj) = 'SUMI' THEN 'Suministros'
			WHEN (trim(CP.CostObj) = 'SERV' OR trim(CP.CostObj) = 'GPAY') THEN 'Servicios'
			WHEN trim(CP.CostObj) = 'DEPR' THEN 'Depreciación'
			WHEN trim(CP.CostObj) = 'DEPA' THEN 'DEPA'
			WHEN trim(CP.CostObj) = 'MAOB' THEN 'MANOOBRA'
			WHEN trim(CP.CostObj) = 'INDR' THEN 'INDIRECTOS'
			WHEN trim(CP.CostObj) = 'INDREC' THEN 'INDIRECTOSREC'
			ELSE CP.Tipo END ELSE CP.Tipo END AS Tipo
,CP.Variacion
,CP.Categoria
,CP.Status
,CP.Zona
,CP.SubZona
,CP.Administrador
,CP.Division
,CP.Producto
,CP.TipoProducto
,CP.Standar
,CP.Especie
,CP.Proveedor
,CP.Idvida
,CP.KilosRendidos
,CASE WHEN trim(CP.Location) = 'BRDR' AND CP.Empresa = 1 AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' THEN 'REPRODUCTORAS' ELSE CP.Seccion END AS Seccion
,CP.MontoEliminados
,CP.MontoSinEliminacion
,CP.Clase
,CP.PolloNacidos
,CP.ProdHuevoInc
,CP.CentroCosto
,CP.DescripcionEmpresa
FROM {database_name_costos_gl}.ft_costoPollo AS CP
""")
print('carga df_ft_costoPollo_upd4', df_ft_costoPollo_upd4.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd4
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_costoIncuba = spark.sql(f"""select 
'' as FarmName, 
'' as FarmNo, 
'' as Entity, 
A.AccountNo as Location, 
S.AccountNo as Estado, 
U.AccountNo as Usuario, 
CO.AccountNo as CostObj, 
E.AccountNo as Element, 
MVPJ.SourceCode, 
(MVPJ.RelativeAmount * -1) as Amount, 
month(MVPJ.xDate) as Mes, 
year(MVPJ.xDate) as Anio, 
MVPJ.TransCode, 
MVPJ.xDate Fecha, 
1 as Empresa, 
MVPJ.CompanyNo DescripcionEmpresa, 
MVPJ.CostCenterNo CentroCosto
from {database_name_costos_si}.si_mvproteinjournaltrans MVPJ 
inner join {database_name_si}.si_proteinchartofaccounts PCOA on MVPJ.ProteinChartOfAccountsIRN = PCOA.IRN 
left JOIN {database_name_si}.si_proteinaccounts A ON A.IRN=PCOA.ProteinAccountsIRN_Location 
left JOIN {database_name_si}.si_proteinaccounts S ON S.IRN=PCOA.ProteinAccountsIRN_Stages 
left JOIN {database_name_si}.si_proteinaccounts U ON U.IRN=PCOA.ProteinAccountsIRN_User 
left join {database_name_si}.si_proteinaccounts CO ON CO.IRN=PCOA.ProteinAccountsIRN_CostObjects 
left join {database_name_si}.si_proteinaccounts E ON E.IRN=PCOA.ProteinAccountsIRN_Elements 
where date_format(cast(xdate as timestamp),'yyyyMM') = date_format(add_months(trunc(current_date, 'month'),-1),'yyyyMM') 
and A.AccountNo='HAT' AND S.AccountNo='CHXPLT' AND U.AccountNo in('OUT','SLS') 
and MVPJ.CompanyNo = 'SAN FERNANDO' AND MVPJ.DivisionNo = 1 """)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costoIncuba"
}
df_costoIncuba.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costoIncuba")
print('carga temporal costoIncuba', df_costoIncuba.count())
df_insert_costopollo2 = spark.sql(f"""select 
 FarmName
,FarmNo
,Entity
,Location
,Estado
,Usuario
,CostObj
,Element
,SourceCode
,Amount
,mes
,anio
,TransCode
,Fecha
,Empresa
,'' Tipo
,'' Variacion
,'' Categoria
,'' Status
,'' Zona
,'' SubZona
,'' Administrador
,'' Division
,'' Producto
,'' TipoProducto
,'' Standar
,'' Especie
,'' Proveedor
,'' Idvida
,0.0 KilosRendidos
,'' Seccion
,0.0 MontoEliminados
,0.0 MontoSinEliminacion
,'' Clase
,0 PolloNacidos
,0 ProdHuevoInc
,CentroCosto
,DescripcionEmpresa
from {database_name_costos_tmp}.costoIncuba """)
print('carga df_insert_costopollo2' ,df_insert_costopollo2.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(col("Fecha"),"yyyyMM")== fecha_str) & (col("Seccion") == "INCUBACION") & (col("Empresa") == "1") ))
    filtered_new_data = df_insert_costopollo2 
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_costoPollo_upd5 =spark.sql(f"""
SELECT
 CP.FarmName
,CP.FarmNo
,CP.Entity
,CP.Location
,CP.Estado
,CP.Usuario
,CP.CostObj
,CP.Element
,CP.SourceCode
,CP.Amount
,CP.mes
,CP.anio
,CP.TransCode
,CP.Fecha
,CP.Empresa
,CASE WHEN trim(CP.Location) = 'HAT' AND DATE_FORMAT(cast(Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' AND CP.Empresa = 1
     AND (CASE WHEN trim(CP.Location) = 'HAT' AND CP.Empresa = 1 AND DATE_FORMAT(cast(Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' THEN 'INCUBACION' ELSE CP.Seccion END) = 'INCUBACION'
     THEN CASE WHEN CP.Estado = 'OPS' AND (trim(CP.Usuario) = 'IN' OR trim(CP.Usuario) = 'USAGE')
	 THEN CASE WHEN trim(CP.CostObj) = 'AGUA' THEN 'AGUA'
			WHEN trim(CP.CostObj) = 'ALQU' THEN 'ALQU'
			WHEN trim(CP.CostObj) = 'DEPA' THEN 'DEPA'
			WHEN trim(CP.CostObj) = 'ELEC' THEN 'ELEC'
			WHEN trim(CP.CostObj) = 'GAS' THEN 'GAS'
			WHEN trim(CP.CostObj) = 'INDR' THEN 'INDR'
			WHEN trim(CP.CostObj) = 'INDREC' THEN 'INDREC'
			WHEN trim(CP.CostObj) = 'MAOB' THEN 'MAOB'
			WHEN trim(CP.CostObj) = 'REPM' THEN 'MANTENIMIENTO'
			WHEN trim(CP.CostObj) = 'SERV' THEN 'SERVICIO'
			WHEN trim(CP.CostObj) = 'SUMI' THEN 'SUMINISTRO'
			WHEN trim(CP.CostObj) = 'TRANS' THEN 'TRANSPORTE'
			WHEN trim(CP.CostObj) = 'MEDVITVAC' AND trim(CP.Element) = 'MED' THEN 'MEDICINA'
			WHEN trim(CP.CostObj) = 'MEDVITVAC' AND trim(CP.Element) = 'VACC' THEN 'VACUNA' ELSE CP.Tipo END
			WHEN CP.Estado = 'CHXPLT' THEN
			CASE WHEN trim(CP.CostObj) = 'INV' AND trim(CP.Usuario) = 'SLS' THEN 'INV'
				WHEN trim(CP.CostObj) = 'OVHD' AND trim(CP.Usuario) = 'SLS' THEN 'OVHD'
				WHEN trim(CP.CostObj) = 'OVHD' AND trim(CP.Usuario) = 'OUT' THEN 'OVHD'
				WHEN trim(CP.CostObj) = 'INV' AND trim(CP.Usuario) = 'OUT' THEN 'INV'
				ELSE CP.Tipo END ELSE CP.Tipo END ELSE CP.Tipo END AS Tipo
,CP.Variacion
,CP.Categoria
,CP.Status
,CP.Zona
,CP.SubZona
,CP.Administrador
,CP.Division
,CP.Producto
,CP.TipoProducto
,CP.Standar
,CP.Especie
,CP.Proveedor
,CP.Idvida
,CP.KilosRendidos
,CASE WHEN trim(CP.Location) = 'HAT' AND CP.Empresa = 1 AND DATE_FORMAT(cast(Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' THEN 'INCUBACION' ELSE CP.Seccion END AS Seccion
,CP.MontoEliminados
,CP.MontoSinEliminacion
,CP.Clase
,CP.PolloNacidos
,CP.ProdHuevoInc
,CP.CentroCosto
,CP.DescripcionEmpresa
FROM {database_name_costos_gl}.ft_costoPollo AS CP
""")
print('carga df_ft_costoPollo_upd5', df_ft_costoPollo_upd5.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd5
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_costoPollo_upd6 =spark.sql(f"""
WITH total_hens_placed AS (SELECT SUM(hensPlaced) AS SumHensPlaced FROM {database_name_si}.si_HimChickTrans WHERE DATE_FORMAT(cast(transdate as timestamp), 'yyyyMM') = '{aniomes_ant}' AND transcode = 1)
SELECT
 CP.FarmName
,CP.FarmNo
,CP.Entity
,CP.Location
,CP.Estado
,CP.Usuario
,CP.CostObj
,CP.Element
,CP.SourceCode
,CP.Amount
,CP.mes
,CP.anio
,CP.TransCode
,CP.Fecha
,CP.Empresa
,CP.Tipo
,CP.Variacion
,CP.Categoria
,CP.Status
,CP.Zona
,CP.SubZona
,CP.Administrador
,CP.Division
,CP.Producto
,CP.TipoProducto
,CP.Standar
,CP.Especie
,CP.Proveedor
,CP.Idvida
,CP.KilosRendidos
,CP.Seccion
,CP.MontoEliminados
,CP.MontoSinEliminacion
,CP.Clase
,CASE WHEN CP.Seccion = 'INCUBACION' AND DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' AND CP.Empresa = 1 THEN thp.SumHensPlaced ELSE CP.PolloNacidos END AS PolloNacidos
,CP.ProdHuevoInc
,CP.CentroCosto
,CP.DescripcionEmpresa
FROM {database_name_costos_gl}.ft_costoPollo AS CP
LEFT JOIN total_hens_placed AS thp ON 1=1 
""")
print('carga df_ft_costoPollo_upd6', df_ft_costoPollo_upd6.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd6
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
df_ft_costoPollo_upd7 =spark.sql(f"""
SELECT
 CP.FarmName
,CP.FarmNo
,CP.Entity
,CP.Location
,CP.Estado
,CP.Usuario
,CP.CostObj
,CP.Element
,CP.SourceCode
,CP.Amount
,CP.mes
,CP.anio
,CP.TransCode
,CP.Fecha
,CP.Empresa
,CP.Tipo
,CP.Variacion
,CP.Categoria
,CP.Status
,CP.Zona
,CP.SubZona
,CP.Administrador
,CP.Division
,CP.Producto
,CP.TipoProducto
,CP.Standar
,CP.Especie
,CP.Proveedor
,CP.Idvida
,CP.KilosRendidos
,CP.Seccion
,CP.MontoEliminados
,CP.MontoSinEliminacion
,CASE WHEN DATE_FORMAT(cast(CP.Fecha as timestamp), 'yyyyMM') = '{aniomes_ant}' AND CP.Empresa = 1 AND CC.Clase IS NOT NULL THEN CC.Clase ELSE CP.CLASE END AS Clase
,CP.PolloNacidos
,CP.ProdHuevoInc
,CP.CentroCosto
,CP.DescripcionEmpresa
FROM {database_name_costos_gl}.ft_costoPollo AS CP
LEFT JOIN {database_name_gl}.ft_costoxClase AS CC ON trim(CP.Seccion) = trim(CC.Seccion) AND trim(CP.Tipo) = trim(CC.Tipo)
""")
print('carga df_ft_costoPollo_upd7', df_ft_costoPollo_upd7.count())
table_name = 'ft_costoPollo'
try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    final_data = df_ft_costoPollo_upd7
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    df_ft_costoPollo.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()