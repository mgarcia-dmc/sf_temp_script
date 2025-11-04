from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import date_format, current_date, add_months,expr, col, lit
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Definir el nombre del trabajo
JOB_NAME = "nt_prd_tbl_Mortalidad_gold"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
# Parámetros de entrada global

##bucket_name_target = "ue1stgtestas3dtl005-gold"
##bucket_name_source = "ue1stgtestas3dtl005-silver"
##bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
bucket_name_target = "ue1stgprodas3dtl005-gold"
bucket_name_source = "ue1stgprodas3dtl005-silver"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

##database_name = "default"
database_name_tmp = "bi_sf_tmp"
database_name_br ="mtech_prd_sf_br"
database_name_si ="mtech_prd_sf_si"
database_name_gl ="mtech_prd_sf_gl"


file_name_target24 = f"{bucket_name_prdmtech}ft_mortalidad/"
file_name_target25 = f"{bucket_name_prdmtech}ft_mortalidad_diario/"
file_name_target26 = f"{bucket_name_prdmtech}ft_mortalidad_corral/"
file_name_target27 = f"{bucket_name_prdmtech}ft_mortalidad_galpon/"
file_name_target28 = f"{bucket_name_prdmtech}ft_mortalidad_lote/"
file_name_target29 = f"{bucket_name_prdmtech}ft_mortalidad_mensual/"
file_name_target30 = f"{bucket_name_prdmtech}ft_mortalidad_lote_semanal/"

path_target24 = f"s3://{bucket_name_target}/{file_name_target24}"
path_target25 = f"s3://{bucket_name_target}/{file_name_target25}"
path_target26 = f"s3://{bucket_name_target}/{file_name_target26}"
path_target27 = f"s3://{bucket_name_target}/{file_name_target27}"
path_target28 = f"s3://{bucket_name_target}/{file_name_target28}"
path_target29 = f"s3://{bucket_name_target}/{file_name_target29}"
path_target30 = f"s3://{bucket_name_target}/{file_name_target30}"
print("actualiza rutas")
df_detmortTemp = spark.sql(f"""SELECT IRN,cmortalidad,causa FROM (SELECT 
  IRN,EventDate,
  stack(
    40,
    'U_PEAccidentados', U_PEAccidentados,
    'U_PEAscitis', U_PEAscitis,
    'U_PEBazoMoteado', U_PEBazoMoteado,
    'U_PEEnteritis', U_PEEnteritis,
    'U_PEErosionDeMolleja', U_PEErosionDeMolleja,
    'U_PEEstresPorCalor', U_PEEstresPorCalor,
    'U_PEHemopericardio', U_PEHemopericardio,
    'U_PEHemorragiaMusculos', U_PEHemorragiaMusculos,
    'U_PEHepatomegalia', U_PEHepatomegalia,
    'U_PEHidropericardio', U_PEHidropericardio,
    'U_PEHigadoGraso', U_PEHigadoGraso,
    'U_PEHigadoHemorragico', U_PEHigadoHemorragico,
    'U_PEInanicion', U_PEInanicion,
    'U_PEMaterialCaseoso', U_PEMaterialCaseoso,
    'U_PEMuerteSubita', U_PEMuerteSubita,
    'U_PENoViable', U_PENoViable,
    'U_PEOnfalitis', U_PEOnfalitis,
    'U_PEPericarditis', U_PEPericarditis,
    'U_PEPeritonitis', U_PEPeritonitis,
    'U_PEPicaje', U_PEPicaje,
    'U_PEProblemaRespiratorio', U_PEProblemaRespiratorio,
    'U_PEProlapso', U_PEProlapso,
    'U_PERetencionDeYema', U_PERetencionDeYema,
    'U_PERupturaAortica', U_PERupturaAortica,
    'U_PESangreEnCieGO', U_PESangreEnCieGO,
    'U_PESCH', U_PESCH,
    'U_PEUratosis', U_PEUratosis,
    'U_PEAerosaculitisG2', U_PEAerosaculitisG2,
    'U_PECojera', U_PECojera,
    'U_PEHigadoIcterico', U_PEHigadoIcterico,
    'U_PEMaterialCaseoso_po1ra', U_PEMaterialCaseoso_po1ra,
    'U_PEMaterialCaseosoMedRetr', U_PEMaterialCaseosoMedRetr,
    'U_PENecrosisHepatica', U_PENecrosisHepatica,
    'U_PENeumonia', U_PENeumonia,
    'U_PESepticemia', U_PESepticemia,
    'U_PEVomitoNegro', U_PEVomitoNegro,
    'U_PEAsperguillius', U_PEAsperguillius,
    'U_PEBazoGrandeMot', U_PEBazoGrandeMot,
    'U_PECorazonGrande', U_PECorazonGrande,
    'U_PECuadroToxico', U_PECuadroToxico
  ) AS (causa, cmortalidad)
from {database_name_tmp}.mortdia
)
WHERE 
  cmortalidad > 0
  AND date_format(cast(EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date-1, 'month'),-9),'yyyyMM')""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/detmort"
}
df_detmortTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.detmort")
print('carga detmort', df_detmortTemp.count())
df_ft_mortalidadTemp = spark.sql(f"""
select MO.pk_empresa 
,nvl(MO.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) pk_division 
,nvl(MO.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) as pk_zona 
,nvl(MO.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) pk_subzona 
,nvl(LCM.pk_causamortalidad,(select pk_causamortalidad from {database_name_gl}.lk_causamortalidad where irn='Sin Causa Mortalidad')) pk_causamortalidad 
,nvl(MO.pk_diasvida,(select pk_diasvida from {database_name_gl}.lk_diasvida where cdiavida='D00')) pk_diasvida 
,nvl(MO.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0'))pk_administrador 
,nvl(MO.pk_semanavida,(select pk_semanavida from {database_name_gl}.lk_semanavida where csemanavida='S0'))pk_semanavida 
,nvl(MO.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) pk_plantel 
,nvl(MO.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) pk_lote 
,nvl(MO.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) pk_galpon 
,nvl(MO.pk_sexo,(select pk_sexo from {database_name_gl}.lk_sexo where csexo=0)) pk_sexo 
,nvl(MO.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo 
,nvl(MO.pk_standard,(select pk_standard from {database_name_gl}.lk_standard where cstandard='0')) pk_standard 
,nvl(MO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) pk_proveedor 
,MO.ComplexEntityNo 
,nvl(MO.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto 
,nvl(MO.pk_estado,(select pk_estado from {database_name_gl}.lk_estado where cestado=0)) pk_estado 
,MO.FechaNacimiento Nacimiento 
,MO.Edad as Edad 
,nvl(DM.cmortalidad,0)cmortalidad 
,FIN.Inventario Cingreso 
,MO.U_categoria categoria 
,nvl(AT.FlagAtipico,1) FlagAtipico 
,MO.descripfecha
FROM {database_name_gl}.stg_ProduccionDetalle MO 
LEFT JOIN {database_name_tmp}.detmort DM ON UPPER(DM.IRN)= UPPER(MO.BrimFieldTransIRN )
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.IRN = case when DM.CAUSA = 'U_PESangreEnCieGO' then 'U_PESangreEnCiego' else CAST(DM.CAUSA AS VARCHAR(50)) end
LEFT JOIN {database_name_gl}.ft_ingresocons FIN ON FIN.ComplexEntityNo = MO.ComplexEntityNo 
LEFT JOIN {database_name_tmp}.atipicos AT ON MO.ComplexEntityNo = AT.ComplexEntityNo  
WHERE MO.Edad >= 0 
AND (date_format(MO.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))""") 

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidadTemp"
}
df_ft_mortalidadTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidadTemp")
print('carga ft_mortalidadTemp', df_ft_mortalidadTemp.count())
df_scmortalidadTemp = spark.sql(f"select complexentityno,pk_semanavida,sum(nvl(cmortalidad,0)) as cmortalidad \
from {database_name_tmp}.ft_mortalidadTemp \
group by complexentityno,pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/scmortalidad"
}
df_scmortalidadTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.scmortalidad")
print('carga scmortalidad' , df_scmortalidadTemp.count())
df_scmortalidadxlesionTemp = spark.sql(f"select complexentityno,pk_semanavida,pk_causamortalidad,sum(nvl(cmortalidad, 0)) as cmortalidad \
from {database_name_tmp}.ft_mortalidadTemp \
group by complexentityno,pk_semanavida,pk_causamortalidad")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/scmortalidadxlesion"
}
df_scmortalidadxlesionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.scmortalidadxlesion")
print('carga scmortalidadxlesion', df_scmortalidadxlesionTemp.count())
df_scmortalidadxlesionxloteTemp = spark.sql(f"select substring(complexentityno, 1, (length(complexentityno)-6)) complexentityno,pk_semanavida,pk_causamortalidad,sum(nvl(cmortalidad, 0)) as cmortalidad \
from {database_name_tmp}.ft_mortalidadTemp \
group by substring(complexentityno, 1, (length(complexentityno)-6)),pk_semanavida,pk_causamortalidad")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/scmortalidadxlesionxlote"
}
df_scmortalidadxlesionxloteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.scmortalidadxlesionxlote")
print('carga scmortalidadxlesionxlote', df_scmortalidadxlesionxloteTemp.count())
df_scingresoxlesionxloteTemp = spark.sql(f"select substring(complexentityno,1,(length(complexentityno)-6)) complexentityno,pk_semanavida, pk_causamortalidad,sum(cingreso) cingreso \
from (select complexentityno, pk_semanavida, pk_causamortalidad, max(cingreso) cingreso \
from {database_name_tmp}.ft_mortalidadTemp \
group by complexentityno, pk_semanavida, pk_causamortalidad \
) A \
group by substring(complexentityno,1,(length(complexentityno)-6)), pk_semanavida, pk_causamortalidad")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/scingresoxlesionxlote"
}
df_scingresoxlesionxloteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.scingresoxlesionxlote")
print('carga scingresoxlesionxlote' , df_scingresoxlesionxloteTemp.count())
df_ft_mortalidadTemp1 = spark.sql(f"""
SELECT mo.pk_empresa 
,mo.pk_division 
,mo.pk_zona 
,mo.pk_subzona 
,mo.pk_causamortalidad 
,mo.pk_diasvida 
,mo.pk_administrador 
,mo.pk_semanavida 
,mo.pk_plantel 
,mo.pk_lote 
,mo.pk_galpon 
,mo.pk_sexo 
,max(mo.pk_tiempo) pk_tiempo
,max(descripfecha) descripfecha 
,mo.pk_standard 
,mo.pk_proveedor 
,mo.complexentityno 
,substring(mo.complexentityno,1,(length(complexentityno)-6)) complexentitynolote 
,mo.pk_producto 
,mo.pk_estado
,mo.nacimiento 
,max(nvl(mo.edad,0)) edad 
,sum(nvl(cmortalidad,0)) as cmortalidad 
,avg(nvl(cingreso,0)) cingreso 
,0 AS cmortalidadacum 
,0 AS pmortalidadacum 
,max(nvl(cingreso,0)) as maxingreso 
,mo.categoria 
,mo.FlagAtipico
FROM {database_name_tmp}.ft_mortalidadTemp MO 
GROUP BY mo.pk_empresa,mo.pk_division, mo.pk_zona, mo.pk_subzona, mo.pk_causamortalidad, mo.pk_diasvida, mo.pk_administrador, mo.pk_semanavida, 
mo.pk_plantel,mo.pk_lote,mo.pk_galpon, mo.pk_sexo,mo.pk_standard,mo.pk_proveedor, mo.complexentityno, mo.pk_producto, mo.pk_estado, 
mo.nacimiento,mo.categoria,mo.FlagAtipico
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidadTemp1"
}
df_ft_mortalidadTemp1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidadTemp1")
print('carga ft_mortalidadTemp1', df_ft_mortalidadTemp1.count())
df_ft_mortalidadT = spark.sql(f"""SELECT 
a.pk_empresa 
,a.pk_division 
,a.pk_zona 
,a.pk_subzona 
,a.pk_causamortalidad pk_mortalidad
,a.pk_diasvida 
,a.pk_administrador 
,a.pk_semanavida 
,a.pk_plantel 
,a.pk_lote 
,a.pk_galpon 
,a.pk_sexo 
,a.pk_tiempo 
,a.pk_standard 
,a.pk_producto 
,a.pk_estado 
,a.pk_proveedor 
,a.complexentityno 
,a.nacimiento 
,a.edad 
,a.cmortalidad cmoratalidad
,nvl(round((a.cmortalidad/nullif((a.cingreso*1.0),0))*100,3),0) pmortalidad 
,0 AS cmortalidadacum 
,0 AS pmortalidadacum 
,a.cingreso 
,CASE WHEN a.pk_causamortalidad = 1 THEN 0 ELSE row_number() OVER (PARTITION BY a.complexentityno,a.pk_diasvida ORDER BY a.cmortalidad DESC, a.pk_causamortalidad ASC) END OrderLesion 
,a.categoria 
,a.FlagAtipico 
,b.cmortalidad scmortalidad 
,round(((b.cmortalidad)/nullif(a.cingreso*1.0,0))*100,2) spmortalidad 
,c.cmortalidad scmortalidadxlesion 
,CASE WHEN a.pk_causamortalidad = 1 THEN 0 ELSE dense_rank() OVER (PARTITION BY a.complexentityno,a.pk_semanavida ORDER BY c.cmortalidad DESC, a.pk_causamortalidad ASC) END sOrdenLesion 
,round(((c.cmortalidad)/nullif(a.cingreso*1.0,0))*100,2) spmortalidadxlesion 
,d.cmortalidad scmortalidadxlesionxlote 
,round(((d.cmortalidad)/nullif(e.cingreso*1.0,0))*100,2) spmortalidadxlesionxlote 
,CASE WHEN a.pk_causamortalidad = 1 THEN 0 ELSE dense_rank() OVER (PARTITION BY a.complexentitynoLote,a.pk_semanavida  ORDER BY d.cmortalidad DESC, a.pk_causamortalidad ASC) END sOrderLesionLote 
,a.descripfecha DescripFecha 
,'' DescripEmpresa
,'' DescripDivision
,'' DescripZona
,'' DescripSubzona
,'' DescripMortalidad
,'' DescripDiaVida
,'' DescripAdministrador
,'' DescripSemanaVida
,'' Plantel
,'' Lote
,'' Galpon
,'' DescripSexo
,'' DescripStandard
,'' DescripProveedor
,'' DescripProducto
,'' DescripEstado
FROM {database_name_tmp}.ft_mortalidadTemp1 a left join {database_name_tmp}.scmortalidad B on A.complexentityno = B.complexentityno AND A.pk_semanavida = B.pk_semanavida 
left join {database_name_tmp}.scmortalidadxlesion C on A.complexentityno = C.complexentityno AND A.pk_semanavida =  C.pk_semanavida AND A.pk_causamortalidad = C.pk_causamortalidad 
left join {database_name_tmp}.scmortalidadxlesionxlote D on A.complexentitynoLote = D.complexentityno AND A.pk_semanavida = D.pk_semanavida AND A.pk_causamortalidad = D.pk_causamortalidad 
left join {database_name_tmp}.scingresoxlesionxlote E on A.complexentitynoLote = E.complexentityno AND A.pk_semanavida = E.pk_semanavida AND A.pk_causamortalidad = E.pk_causamortalidad
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidadT"
}
df_ft_mortalidadT.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidadT")
print('carga df_ft_mortalidadT', df_ft_mortalidadT.count())
df_ft_mortalidad = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidadT
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_mortalidad', df_ft_mortalidad.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes24 = spark.read.format("parquet").load(path_target24)
    datos_existentes24 = True
    logger.info(f"Datos existentes de ft_mortalidad cargados: {df_existentes24.count()} registros")
except:
    datos_existentes24 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad")

if datos_existentes24:
    existing_data24 = spark.read.format("parquet").load(path_target24)
    data_after_delete24 = existing_data24.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data24 = df_ft_mortalidad.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data24 = filtered_new_data24.union(data_after_delete24)
   
    cant_ingresonuevo24 = filtered_new_data24.count()
    cant_total24 = final_data24.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidadTemporal"
    }
    final_data24.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidadTemporal")

    final_data24_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidadTemporal")  
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad"
    }
    final_data24_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad : {cant_ingresonuevo24}")
    print(f"Total de registros en la tabla ft_mortalidad : {cant_total24}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidadTemporal')
    print(f"Tabla ft_mortalidadTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad"
    }
    df_ft_mortalidad.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad")
print('carga df_ft_mortalidad', df_ft_mortalidad.count())
df_AcumuladosTemp =spark.sql(f"\
SELECT LL.noplantel FarmNo,LL.nlote EntityNo,LG.nogalpon HouseNo,LS.csexo PenNo,M.pk_lote,M.ComplexEntityNo,M.Edad FirstHatchDateAge,M.xDate \
,M.descripfecha eventdate \
,nvl(M.pk_semanavida,1) AS pk_semanavida \
,nvl(M.MortDia,0) AS Mortality \
,nvl(IC.Inventario,0) AS Ingreso \
FROM {database_name_gl}.stg_ProduccionDetalle M \
LEFT JOIN {database_name_gl}.lk_lote LL on M.pk_lote = LL.pk_lote \
LEFT JOIN {database_name_gl}.lk_galpon LG on M.pk_galpon = LG.pk_galpon \
LEFT JOIN {database_name_gl}.lk_sexo LS on M.pk_sexo = LS.pk_sexo \
LEFT JOIN (SELECT pk_lote,ComplexEntityNo, SUM(Inventario) AS Inventario FROM {database_name_gl}.ft_ingresocons GROUP BY pk_lote,ComplexEntityNo) IC ON IC.ComplexEntityNo = M.ComplexEntityNo \
WHERE (date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosTemp"
}
df_AcumuladosTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AcumuladosTemp")
print('carga AcumuladosTemp',df_AcumuladosTemp.count())
df_AcumuladorsMortalidad= spark.sql(f"""
WITH FilteredData AS (
    SELECT
        pk_lote,
        ComplexEntityNo,
        xDate,
        FirstHatchDateAge,
        pk_semanavida,
        Mortality
    FROM
        {database_name_tmp}.AcumuladosTemp
    WHERE
        DATE_FORMAT(EventDate, 'yyyyMMdd') >=  date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
),
GroupedData AS (
    SELECT
        pk_lote,
        ComplexEntityNo,
        xDate,
        FirstHatchDateAge,
        pk_semanavida,
        SUM(Mortality) AS Mortality
    FROM
        FilteredData
    GROUP BY
        pk_lote, ComplexEntityNo, xDate, FirstHatchDateAge, pk_semanavida
),
WindowedData AS (
    SELECT
        *,
        COALESCE(SUM(Mortality) OVER (PARTITION BY ComplexEntityNo ORDER BY FirstHatchDateAge), 0) AS MortAcum,
        COALESCE(SUM(Mortality) OVER (PARTITION BY ComplexEntityNo, pk_semanavida), 0) AS MortSem,
        COALESCE(SUM(Mortality) OVER (PARTITION BY ComplexEntityNo ORDER BY pk_semanavida), 0) AS MortSemAcum
    FROM
        GroupedData
)
SELECT
    pk_lote,
    ComplexEntityNo,
    xDate,
    b.pk_diasvida FirstHatchDateAge,
    a.pk_semanavida,
    Mortality,
    MortAcum,
    MortSem,
    MortSemAcum,
    1 AS flagartificio
FROM
    WindowedData a
left join {database_name_gl}.lk_diasvida b on a.FirstHatchDateAge = b.FirstHatchDateAge
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladorsMortalidad"
}
df_AcumuladorsMortalidad.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AcumuladorsMortalidad")
print("carga AcumuladorsMortalidad --> Registros procesados:", df_AcumuladorsMortalidad.count())
df_UPDAcumuladorsMortalidad = spark.sql(f"""
SELECT 
pk_lote             
,complexentityno     
,xdate               
,2 firsthatchdateage   
,2 pk_semanavida       
,mortality           
,mortacum            
,mortsem             
,mortsemacum         
,flagartificio        
FROM {database_name_tmp}.AcumuladorsMortalidad
WHERE FirstHatchDateAge IN (0, -1)
union all 
select * 
FROM {database_name_tmp}.AcumuladorsMortalidad
WHERE FirstHatchDateAge not IN (0, -1)
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladorsMortalidad_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDAcumuladorsMortalidad.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.AcumuladorsMortalidad_new")

df_AcumuladorsMortalidad_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.AcumuladorsMortalidad_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.AcumuladorsMortalidad")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladorsMortalidad"
}
df_AcumuladorsMortalidad_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AcumuladorsMortalidad")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.AcumuladorsMortalidad_new")

print("carga AcumuladorsMortalidad --> Registros procesados:", df_AcumuladorsMortalidad_nueva.count())
df_artificioacum = spark.sql(f"""
WITH artificio_acum AS (SELECT ComplexEntityNo,pk_lote,MAX(FirstHatchDateAge) AS FirstHatchDateAge FROM {database_name_tmp}.AcumuladorsMortalidad GROUP BY ComplexEntityNo, pk_lote)
, artificio_lote as ((SELECT pk_lote, MAX(FirstHatchDateAge) AS FirstHatchDateAgelote FROM {database_name_tmp}.AcumuladorsMortalidad GROUP BY pk_lote))
SELECT 
    a.ComplexEntityNo,
    a.FirstHatchDateAge,
    b.FirstHatchDateAgelote
FROM artificio_acum a
LEFT JOIN artificio_lote b ON a.pk_lote = b.pk_lote
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/artificioacum"
}
df_artificioacum.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.artificioacum")
print("carga artificioacum --> Registros procesados:", df_artificioacum.count())
df_INSAcumuladorsMortalidad = spark.sql(f"""
SELECT
B.pk_lote,
B.ComplexEntityNo,
B.xdate,
C.pk_diasvida,
C.pk_semanavida,
0 AS Mortality,
0 AS MortAcum,
0 AS MortSem,
B.MortSemAcum,
2 AS flagartificio
FROM {database_name_tmp}.artificioacum A
LEFT JOIN {database_name_tmp}.AcumuladorsMortalidad B ON A.ComplexEntityNo = B.ComplexEntityNo AND A.FirstHatchDateAge = B.FirstHatchDateAge
CROSS JOIN {database_name_gl}.lk_diasvida C
WHERE c.pk_diasvida > A.FirstHatchDateAge AND c.pk_diasvida <= A.FirstHatchDateAgelote
ORDER BY 1
""")

try:
    df = spark.table(f"{database_name_tmp}.AcumuladorsMortalidad")
  
    df_AcumuladorsMortalidad_new = df_INSAcumuladorsMortalidad.union(df)
    #print("✅ Tabla cargada correctamente")
except Exception as e:
    df_AcumuladorsMortalidad_new = df_INSAcumuladorsMortalidad

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladorsMortalidad_new"
}
# 1️⃣ Crear DataFrame intermedio
df_AcumuladorsMortalidad_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.AcumuladorsMortalidad_new")

df_AcumuladorsMortalidad_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.AcumuladorsMortalidad_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.AcumuladorsMortalidad")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}AcumuladorsMortalidad"
}
df_AcumuladorsMortalidad_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AcumuladorsMortalidad")

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.AcumuladorsMortalidad_new")

print("carga INS AcumuladorsMortalidad --> Registros procesados:", df_AcumuladorsMortalidad_nueva.count())
df_lesionesTemp = spark.sql(f"SELECT Mt.ComplexEntityNo,MAX(Mt.xDate) as xDate ,Mt.pk_diasvida,Mt.pk_semanavida,MAX(mt.pk_tiempo) pk_tiempo,MAX(mt.DescripFecha) fecha, \
SUM(MT.U_PEAccidentados) AcumPEAccidentados, \
SUM(MT.U_PEAscitis) AcumPEAscitis, \
SUM(MT.U_PEBazoMoteado) AcumPEBazoMoteado, \
SUM(MT.U_PEEnteritis) AcumPEEnteritis, \
SUM(MT.U_PEErosionDeMolleja) AcumPEErosionDeMolleja, \
SUM(MT.U_PEEstresPorCalor) AcumPEEstresPorCalor, \
SUM(MT.U_PEHemopericardio) AcumPEHemopericardio, \
SUM(MT.U_PEHemorragiaMusculos) AcumPEHemorragiaMusculos, \
SUM(MT.U_PEHepatomegalia) AcumPEHepatomegalia, \
SUM(MT.U_PEHidropericardio) AcumPEHidropericardio, \
SUM(MT.U_PEHigadoGraso) AcumPEHigadoGraso, \
SUM(MT.U_PEHigadoHemorragico) AcumPEHigadoHemorragico, \
SUM(MT.U_PEInanicion) AcumPEInanicion, \
SUM(MT.U_PEMaterialCaseoso) AcumPEMaterialCaseoso, \
SUM(MT.U_PEMuerteSubita) AcumPEMuerteSubita, \
SUM(MT.U_PENoViable) AcumPENoViable, \
SUM(MT.U_PEOnfalitis) AcumPEOnfalitis, \
SUM(MT.U_PEPericarditis) AcumPEPericarditis, \
SUM(MT.U_PEPeritonitis) AcumPEPeritonitis, \
SUM(MT.U_PEPicaje) AcumPEPicaje, \
SUM(MT.U_PEProblemaRespiratorio) AcumPEProblemaRespiratorio, \
SUM(MT.U_PEProlapso) AcumPEProlapso, \
SUM(MT.U_PERetencionDeYema) AcumPERetencionDeYema, \
SUM(MT.U_PERupturaAortica) AcumPERupturaAortica, \
SUM(MT.U_PESangreEnCiego) AcumPESangreEnCiego, \
SUM(MT.U_PESCH) AcumPESCH, \
SUM(MT.U_PEUratosis) AcumPEUratosis, \
SUM(MT.U_PEAerosaculitisG2) AcumPEAerosaculitisG2, \
SUM(MT.U_PECojera) AcumPECojera, \
SUM(MT.U_PEHigadoIcterico) AcumPEHigadoIcterico, \
SUM(MT.U_PEMaterialCaseoso_po1ra) AcumPEMaterialCaseoso_po1ra, \
SUM(MT.U_PEMaterialCaseosoMedRetr) AcumPEMaterialCaseosoMedRetr, \
SUM(MT.U_PENecrosisHepatica) AcumPENecrosisHepatica, \
SUM(MT.U_PENeumonia) AcumPENeumonia, \
SUM(MT.U_PESepticemia) AcumPESepticemia, \
SUM(MT.U_PEVomitoNegro) AcumPEVomitoNegro, \
SUM(MT.U_PEAsperguillius) AcumPEAsperguillius, \
SUM(MT.U_PEBazoGrandeMot) AcumPEBazoGrandeMot, \
SUM(MT.U_PECorazonGrande) AcumPECorazonGrande, \
SUM(MT.U_PECuadroToxico) AcumPECuadroToxico \
FROM {database_name_gl}.stg_ProduccionDetalle mt \
WHERE (date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
GROUP BY Mt.ComplexEntityNo,Mt.pk_diasvida,Mt.pk_semanavida \
ORDER BY pk_diasvida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/lesiones"
}
df_lesionesTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.lesiones")
print("carga lesiones --> Registros procesados:", df_lesionesTemp.count())
df_pruebaTemp = spark.sql(f"SELECT M.ComplexEntityNo,MAX(M.xDate) AS xDate,M.pk_diasvida,M.pk_semanavida, \
SUM(MT.AcumPEAccidentados) AcumPEAccidentados, \
SUM(MT.AcumPEAscitis) AcumPEAscitis, \
SUM(MT.AcumPEBazoMoteado) AcumPEBazoMoteado, \
SUM(MT.AcumPEEnteritis) AcumPEEnteritis, \
SUM(MT.AcumPEErosionDeMolleja) AcumPEErosionDeMolleja, \
SUM(MT.AcumPEEstresPorCalor) AcumPEEstresPorCalor, \
SUM(MT.AcumPEHemopericardio) AcumPEHemopericardio, \
SUM(MT.AcumPEHemorragiaMusculos) AcumPEHemorragiaMusculos, \
SUM(MT.AcumPEHepatomegalia) AcumPEHepatomegalia, \
SUM(MT.AcumPEHidropericardio) AcumPEHidropericardio, \
SUM(MT.AcumPEHigadoGraso) AcumPEHigadoGraso, \
SUM(MT.AcumPEHigadoHemorragico) AcumPEHigadoHemorragico, \
SUM(MT.AcumPEInanicion) AcumPEInanicion, \
SUM(MT.AcumPEMaterialCaseoso) AcumPEMaterialCaseoso, \
SUM(MT.AcumPEMuerteSubita) AcumPEMuerteSubita, \
SUM(MT.AcumPENoViable) AcumPENoViable, \
SUM(MT.AcumPEOnfalitis) AcumPEOnfalitis, \
SUM(MT.AcumPEPericarditis) AcumPEPericarditis, \
SUM(MT.AcumPEPeritonitis) AcumPEPeritonitis, \
SUM(MT.AcumPEPicaje) AcumPEPicaje, \
SUM(MT.AcumPEProblemaRespiratorio) AcumPEProblemaRespiratorio, \
SUM(MT.AcumPEProlapso) AcumPEProlapso, \
SUM(MT.AcumPERetencionDeYema) AcumPERetencionDeYema, \
SUM(MT.AcumPERupturaAortica) AcumPERupturaAortica, \
SUM(MT.AcumPESangreEnCiego) AcumPESangreEnCiego, \
SUM(MT.AcumPESCH) AcumPESCH, \
SUM(MT.AcumPEUratosis) AcumPEUratosis, \
SUM(MT.AcumPEAerosaculitisG2) AcumPEAerosaculitisG2, \
SUM(MT.AcumPECojera) AcumPECojera, \
SUM(MT.AcumPEHigadoIcterico) AcumPEHigadoIcterico, \
SUM(MT.AcumPEMaterialCaseoso_po1ra) AcumPEMaterialCaseoso_po1ra, \
SUM(MT.AcumPEMaterialCaseosoMedRetr) AcumPEMaterialCaseosoMedRetr, \
SUM(MT.AcumPENecrosisHepatica) AcumPENecrosisHepatica, \
SUM(MT.AcumPENeumonia) AcumPENeumonia, \
SUM(MT.AcumPESepticemia) AcumPESepticemia, \
SUM(MT.AcumPEVomitoNegro) AcumPEVomitoNegro, \
SUM(MT.AcumPEAsperguillius) AcumPEAsperguillius, \
SUM(MT.AcumPEBazoGrandeMot) AcumPEBazoGrandeMot, \
SUM(MT.AcumPECorazonGrande) AcumPECorazonGrande, \
SUM(MT.AcumPECuadroToxico) AcumPECuadroToxico \
FROM {database_name_tmp}.lesiones M \
LEFT JOIN {database_name_tmp}.lesiones MT ON MT.ComplexEntityNo= M.ComplexEntityNo AND MT.pk_diasvida <= M.pk_diasvida \
WHERE (date_format(M.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
GROUP BY M.ComplexEntityNo,M.pk_diasvida,M.pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/prueba"
}
df_pruebaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.prueba")
print("carga prueba --> Registros procesados:", df_pruebaTemp.count())
df_lesionesSemanalTemp=spark.sql(f"SELECT \
Mt.ComplexEntityNo,MAX(Mt.xDate) as xDate,Mt.pk_semanavida,MAX(mt.pk_tiempo) pk_tiempo,MAX(mt.DescripFecha) fecha, \
SUM(MT.U_PEAccidentados) SemAccidentados, \
SUM(MT.U_PEAscitis) SemAscitis, \
SUM(MT.U_PEBazoMoteado) SemBazoMoteado, \
SUM(MT.U_PEEnteritis) SemEnteritis, \
SUM(MT.U_PEErosionDeMolleja) SemErosionDeMolleja, \
SUM(MT.U_PEEstresPorCalor) SemEstresPorCalor, \
SUM(MT.U_PEHemopericardio) SemHemopericardio, \
SUM(MT.U_PEHemorragiaMusculos) SemHemorragiaMusculos, \
SUM(MT.U_PEHepatomegalia) SemHepatomegalia, \
SUM(MT.U_PEHidropericardio) SemHidropericardio, \
SUM(MT.U_PEHigadoGraso) SemHigadoGraso, \
SUM(MT.U_PEHigadoHemorragico) SemHigadoHemorragico, \
SUM(MT.U_PEInanicion) SemInanicion, \
SUM(MT.U_PEMaterialCaseoso) SemMaterialCaseoso, \
SUM(MT.U_PEMuerteSubita) SemMuerteSubita, \
SUM(MT.U_PENoViable) SemNoViable, \
SUM(MT.U_PEOnfalitis) SemOnfalitis, \
SUM(MT.U_PEPericarditis) SemPericarditis, \
SUM(MT.U_PEPeritonitis) SemPeritonitis, \
SUM(MT.U_PEPicaje) SemPicaje, \
SUM(MT.U_PEProblemaRespiratorio) SemProblemaRespiratorio, \
SUM(MT.U_PEProlapso) SemProlapso, \
SUM(MT.U_PERetencionDeYema) SemRetencionDeYema, \
SUM(MT.U_PERupturaAortica) SemRupturaAortica, \
SUM(MT.U_PESangreEnCiego) SemSangreEnCiego, \
SUM(MT.U_PESCH) SemSCH, \
SUM(MT.U_PEUratosis) SemUratosis, \
SUM(MT.U_PEAerosaculitisG2) SemAerosaculitisG2, \
SUM(MT.U_PECojera) SemCojera, \
SUM(MT.U_PEHigadoIcterico) SemHigadoIcterico, \
SUM(MT.U_PEMaterialCaseoso_po1ra) SemMaterialCaseoso_po1ra, \
SUM(MT.U_PEMaterialCaseosoMedRetr) SemMaterialCaseosoMedRetr, \
SUM(MT.U_PENecrosisHepatica) SemNecrosisHepatica, \
SUM(MT.U_PENeumonia) SemNeumonia, \
SUM(MT.U_PESepticemia) SemSepticemia, \
SUM(MT.U_PEVomitoNegro) SemVomitoNegro, \
SUM(MT.U_PEAsperguillius) SemAsperguillius, \
SUM(MT.U_PEBazoGrandeMot) SemBazoGrandeMot, \
SUM(MT.U_PECorazonGrande) SemCorazonGrande, \
SUM(MT.U_PECuadroToxico) SemCuadroToxico \
FROM {database_name_gl}.stg_ProduccionDetalle mt \
WHERE (date_format(Mt.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
GROUP BY Mt.ComplexEntityNo,Mt.pk_semanavida \
ORDER BY pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/lesionesSemanal"
}
df_lesionesSemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.lesionesSemanal")
print("carga lesionesSemanal --> Registros procesados:", df_lesionesSemanalTemp.count())
df_alojamiento = spark.sql(f"""
select 
ComplexEntityNo,
RazaMayor,
ListaPadre,
ListaIncubadora,
PadreMayor,
IncubadoraMayor,
EdadPadreCorralDescrip,
MAX(EdadPadreCorral) EdadPadreCorral,
MAX(PorcAlojPadreMayor) PorcAlojPadreMayor,
MAX(PorcRazaMayor) PorcRazaMayor,
MAX(PorcIncMayor) PorcIncMayor,
MAX(DiasAloj) DiasAloj,
AVG(TotalAloj)TotalAloj,
AVG(PesoHvoPond) PesoHvoPond,
TipoOrigen
from {database_name_gl}.ft_alojamiento
where pk_empresa = 1
and ComplexEntityNo not in ('P252-2003-06-01','P244-2003-02-02','P244-2003-03-01')
group by ComplexEntityNo,ListaPadre,ListaIncubadora,PadreMayor,RazaMayor,IncubadoraMayor,EdadPadreCorralDescrip,TipoOrigen
order by 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamiento"
}
df_alojamiento.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.alojamiento")
print("carga alojamiento --> Registros procesados:", df_alojamiento.count())
df_lesionTemp=spark.sql(f"SELECT A.complexEntityNo,A.pk_diasvida,A.OrderLesion AS OrderLesion1,A.cmoratalidad AS cmoratalidad1,A.nmortalidad AS nmortalidad1, \
B.OrderLesion AS OrderLesion2,B.cmoratalidad AS cmoratalidad2, \
B.nmortalidad AS nmortalidad2,C.OrderLesion AS OrderLesion3,C.cmoratalidad AS cmoratalidad3,C.nmortalidad AS nmortalidad3 \
FROM \
(SELECT MO1.*,LCM.nmortalidad FROM {database_name_gl}.ft_mortalidad MO1 \
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.pk_causamortalidad = MO1.pk_mortalidad \
WHERE OrderLesion =1) A \
LEFT JOIN \
(SELECT MO1.*,LCM.nmortalidad  FROM {database_name_gl}.ft_mortalidad MO1 \
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.pk_causamortalidad = MO1.pk_mortalidad \
WHERE OrderLesion =2) B ON A.complexEntityNo = B.complexEntityNo AND A.pk_diasvida = B.pk_diasvida \
LEFT JOIN \
(SELECT MO1.*,LCM.nmortalidad  FROM {database_name_gl}.ft_mortalidad MO1 \
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.pk_causamortalidad = MO1.pk_mortalidad \
WHERE OrderLesion =3) C ON A.complexEntityNo = C.complexEntityNo AND A.pk_diasvida = C.pk_diasvida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/lesion"
}
df_lesionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.lesion")
print("carga lesion --> Registros procesados:", df_lesionTemp.count())
df_SemanaOrdenLesionTemp = spark.sql(f"SELECT MO.complexEntityNo, MO.pk_semanavida, MAX(MO.sOrdenLesion) sOrdenLesion, MAX(MO.scmortalidadxlesion) scmortalidadxlesion, LCM.nmortalidad \
FROM {database_name_gl}.ft_mortalidad MO \
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.pk_causamortalidad = MO.pk_mortalidad \
where MO.sOrdenLesion IN (1,2,3) \
group by MO.complexEntityNo, MO.pk_semanavida, LCM.nmortalidad \
UNION \
SELECT A.complexEntityNo, A.pk_semanavida, 4 sOrdenLesion, SUM(A.scmortalidadxlesion) scmortalidadxlesion, 'Otros' nmortalidad \
FROM \
( \
SELECT MO.complexEntityNo, MO.pk_semanavida, MAX(MO.sOrdenLesion) sOrdenLesion, MAX(MO.scmortalidadxlesion) scmortalidadxlesion, LCM.nmortalidad \
FROM {database_name_gl}.ft_mortalidad MO \
LEFT JOIN {database_name_gl}.lk_causamortalidad LCM ON LCM.pk_causamortalidad = MO.pk_mortalidad \
where MO.sOrdenLesion >= 4 \
group by MO.complexEntityNo, MO.pk_semanavida, LCM.nmortalidad \
) A \
GROUP BY A.complexEntityNo, A.pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SemanaOrdenLesion"
}
df_SemanaOrdenLesionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.SemanaOrdenLesion")
print("carga SemanaOrdenLesion --> Registros procesados:", df_SemanaOrdenLesionTemp.count())
df_OrdenlesionSemanaTemp=spark.sql(f"SELECT A.complexEntityNo,A.pk_semanavida,A.sOrdenLesion AS sOrdenLesion1,A.scmortalidadxlesion AS scmortalidadxlesion1,A.nmortalidad AS snmortalidad1 \
,B.sOrdenLesion AS sOrdenLesion2,B.scmortalidadxlesion AS scmortalidadxlesion2,B.nmortalidad AS snmortalidad2,C.sOrdenLesion AS sOrdenLesion3, \
C.scmortalidadxlesion AS scmortalidadxlesion3,C.nmortalidad AS snmortalidad3 \
,D.sOrdenLesion AS sOrdenLesion4,D.scmortalidadxlesion AS scmortalidadxlesion4,D.nmortalidad AS snmortalidad4 \
FROM \
(SELECT * FROM {database_name_tmp}.SemanaOrdenLesion MO1 \
WHERE sOrdenLesion =1) A \
LEFT JOIN \
(SELECT * FROM {database_name_tmp}.SemanaOrdenLesion MO1 \
WHERE sOrdenLesion =2) B ON A.complexEntityNo = B.complexEntityNo AND A.pk_semanavida = B.pk_semanavida \
LEFT JOIN \
(SELECT * FROM {database_name_tmp}.SemanaOrdenLesion MO1 \
WHERE sOrdenLesion =3) C ON A.complexEntityNo = C.complexEntityNo AND A.pk_semanavida = C.pk_semanavida \
LEFT JOIN \
(SELECT * FROM {database_name_tmp}.SemanaOrdenLesion MO1 \
WHERE sOrdenLesion =4) D ON A.complexEntityNo = D.complexEntityNo AND A.pk_semanavida = D.pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenlesionSemana"
}
df_OrdenlesionSemanaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenlesionSemana")

print("carga OrdenlesionSemana --> Registros procesados:", df_OrdenlesionSemanaTemp.count())
df_TablaPivotMortSemTemp = spark.sql(f""" 
SELECT
    pk_lote,
    ComplexEntityNo,
    flagartificio,
    MAX(CASE WHEN pk_semanavida = 2  THEN MortSem ELSE NULL END) AS MortSem1,
    MAX(CASE WHEN pk_semanavida = 3  THEN MortSem ELSE NULL END) AS MortSem2,
    MAX(CASE WHEN pk_semanavida = 4  THEN MortSem ELSE NULL END) AS MortSem3,
    MAX(CASE WHEN pk_semanavida = 5  THEN MortSem ELSE NULL END) AS MortSem4,
    MAX(CASE WHEN pk_semanavida = 6  THEN MortSem ELSE NULL END) AS MortSem5,
    MAX(CASE WHEN pk_semanavida = 7  THEN MortSem ELSE NULL END) AS MortSem6,
    MAX(CASE WHEN pk_semanavida = 8  THEN MortSem ELSE NULL END) AS MortSem7,
    MAX(CASE WHEN pk_semanavida = 9  THEN MortSem ELSE NULL END) AS MortSem8,
    MAX(CASE WHEN pk_semanavida = 10 THEN MortSem ELSE NULL END) AS MortSem9,
    MAX(CASE WHEN pk_semanavida = 11 THEN MortSem ELSE NULL END) AS MortSem10,
    MAX(CASE WHEN pk_semanavida = 12 THEN MortSem ELSE NULL END) AS MortSem11,
    MAX(CASE WHEN pk_semanavida = 13 THEN MortSem ELSE NULL END) AS MortSem12,
    MAX(CASE WHEN pk_semanavida = 14 THEN MortSem ELSE NULL END) AS MortSem13,
    MAX(CASE WHEN pk_semanavida = 15 THEN MortSem ELSE NULL END) AS MortSem14,
    MAX(CASE WHEN pk_semanavida = 16 THEN MortSem ELSE NULL END) AS MortSem15,
    MAX(CASE WHEN pk_semanavida = 17 THEN MortSem ELSE NULL END) AS MortSem16,
    MAX(CASE WHEN pk_semanavida = 18 THEN MortSem ELSE NULL END) AS MortSem17,
    MAX(CASE WHEN pk_semanavida = 19 THEN MortSem ELSE NULL END) AS MortSem18,
    MAX(CASE WHEN pk_semanavida = 20 THEN MortSem ELSE NULL END) AS MortSem19,
    MAX(CASE WHEN pk_semanavida = 21 THEN MortSem ELSE NULL END) AS MortSem20
FROM
    {database_name_tmp}.AcumuladorsMortalidad
WHERE
    flagartificio = 1
GROUP BY
    pk_lote, ComplexEntityNo, flagartificio
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotMortSem"
}
df_TablaPivotMortSemTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaPivotMortSem")

print("carga TablaPivotMortSem --> Registros procesados:", df_TablaPivotMortSemTemp.count())
df_TablaPivotMortSemAcumTemp = spark.sql(f"""
SELECT
    pk_lote,
    ComplexEntityNo,
    flagartificio,
    MAX(CASE WHEN pk_semanavida = 2  THEN MortSemAcum ELSE NULL END) AS MortSemAcum1,
    MAX(CASE WHEN pk_semanavida = 3  THEN MortSemAcum ELSE NULL END) AS MortSemAcum2,
    MAX(CASE WHEN pk_semanavida = 4  THEN MortSemAcum ELSE NULL END) AS MortSemAcum3,
    MAX(CASE WHEN pk_semanavida = 5  THEN MortSemAcum ELSE NULL END) AS MortSemAcum4,
    MAX(CASE WHEN pk_semanavida = 6  THEN MortSemAcum ELSE NULL END) AS MortSemAcum5,
    MAX(CASE WHEN pk_semanavida = 7  THEN MortSemAcum ELSE NULL END) AS MortSemAcum6,
    MAX(CASE WHEN pk_semanavida = 8  THEN MortSemAcum ELSE NULL END) AS MortSemAcum7,
    MAX(CASE WHEN pk_semanavida = 9  THEN MortSemAcum ELSE NULL END) AS MortSemAcum8,
    MAX(CASE WHEN pk_semanavida = 10 THEN MortSemAcum ELSE NULL END) AS MortSemAcum9,
    MAX(CASE WHEN pk_semanavida = 11 THEN MortSemAcum ELSE NULL END) AS MortSemAcum10,
    MAX(CASE WHEN pk_semanavida = 12 THEN MortSemAcum ELSE NULL END) AS MortSemAcum11,
    MAX(CASE WHEN pk_semanavida = 13 THEN MortSemAcum ELSE NULL END) AS MortSemAcum12,
    MAX(CASE WHEN pk_semanavida = 14 THEN MortSemAcum ELSE NULL END) AS MortSemAcum13,
    MAX(CASE WHEN pk_semanavida = 15 THEN MortSemAcum ELSE NULL END) AS MortSemAcum14,
    MAX(CASE WHEN pk_semanavida = 16 THEN MortSemAcum ELSE NULL END) AS MortSemAcum15,
    MAX(CASE WHEN pk_semanavida = 17 THEN MortSemAcum ELSE NULL END) AS MortSemAcum16,
    MAX(CASE WHEN pk_semanavida = 18 THEN MortSemAcum ELSE NULL END) AS MortSemAcum17,
    MAX(CASE WHEN pk_semanavida = 19 THEN MortSemAcum ELSE NULL END) AS MortSemAcum18,
    MAX(CASE WHEN pk_semanavida = 20 THEN MortSemAcum ELSE NULL END) AS MortSemAcum19,
    MAX(CASE WHEN pk_semanavida = 21 THEN MortSemAcum ELSE NULL END) AS MortSemAcum20
FROM
    {database_name_tmp}.AcumuladorsMortalidad
WHERE
    flagartificio = 1
GROUP BY
    pk_lote, ComplexEntityNo, flagartificio
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotMortSemAcum"
}
df_TablaPivotMortSemAcumTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaPivotMortSemAcum")

print("carga TablaPivotMortSemAcum --> Registros procesados:", df_TablaPivotMortSemAcumTemp.count())
df_NoViableXSemTemp = spark.sql(f"select ComplexEntityNo, sum(U_PENoViable) U_PENoViable, pk_semanavida \
from {database_name_gl}.stg_ProduccionDetalle \
where pk_empresa = 1 and pk_division = 4 \
group by ComplexEntityNo,pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/NoViableXSem"
}
df_NoViableXSemTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.NoViableXSem")

print("carga NoViableXSem --> Registros procesados:", df_NoViableXSemTemp.count())
df_TablaPivotNoViableSemTemp = spark.sql(f"""
SELECT
    ComplexEntityNo,
    MAX(CASE WHEN pk_semanavida = 2 THEN U_PENoViable ELSE NULL END) AS NoViableSem1,
    MAX(CASE WHEN pk_semanavida = 3 THEN U_PENoViable ELSE NULL END) AS NoViableSem2,
    MAX(CASE WHEN pk_semanavida = 4 THEN U_PENoViable ELSE NULL END) AS NoViableSem3,
    MAX(CASE WHEN pk_semanavida = 5 THEN U_PENoViable ELSE NULL END) AS NoViableSem4,
    MAX(CASE WHEN pk_semanavida = 6 THEN U_PENoViable ELSE NULL END) AS NoViableSem5,
    MAX(CASE WHEN pk_semanavida = 7 THEN U_PENoViable ELSE NULL END) AS NoViableSem6,
    MAX(CASE WHEN pk_semanavida = 8 THEN U_PENoViable ELSE NULL END) AS NoViableSem7,
    MAX(CASE WHEN pk_semanavida = 9 THEN U_PENoViable ELSE NULL END) AS NoViableSem8
FROM
    {database_name_tmp}.NoViableXSem
GROUP BY
    ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotNoViableSem"
}
df_TablaPivotNoViableSemTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaPivotNoViableSem")

print("carga TablaPivotNoViableSem --> Registros procesados:", df_TablaPivotNoViableSemTemp.count())
df_MortalidadDetalle1Temp1 = spark.sql(f"SELECT \
nvl(MO.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha = cast('1899-11-30' as date))) pk_tiempo \
,nvl(MO.DescripFecha,cast('1899-11-30' as date)) fecha \
,nvl(MO.pk_empresa,(select pk_empresa from {database_name_gl}.lk_empresa where cempresa=4)) pk_empresa \
,nvl(MO.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) AS pk_division \
,nvl(MO.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) AS pk_zona \
,nvl(MO.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) AS pk_subzona \
,nvl(MO.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) AS pk_plantel \
,nvl(MO.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) AS pk_lote \
,nvl(MO.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) AS pk_galpon \
,nvl(MO.pk_sexo, (select pk_sexo from {database_name_gl}.lk_sexo where csexo=0)) pk_sexo \
,nvl(MO.pk_standard,(select pk_standard from {database_name_gl}.lk_standard where cstandard='0')) pk_standard \
,nvl(MO.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto \
,nvl(MO.pk_tipoproducto,(select pk_tipoproducto from {database_name_gl}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(LEP.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie \
,nvl(MO.pk_estado,(select pk_estado from {database_name_gl}.lk_estado where cestado=0)) pk_estado \
,nvl(MO.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0')) pk_administrador \
,nvl(MO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) pk_proveedor \
,MO.pk_semanavida,MO.pk_diasvida,MO.ComplexEntityNo,MO.FechaNacimiento,MO.FechaCierre \
,nvl(FIN.Inventario,0) AS PobInicial \
,nvl(MO.MortDia,0) AS MortDia \
,nvl(MORT.MortAcum,0) AS MortAcum \
,nvl(MORT.MortSem,0) AS MortSem \
,nvl(MORT.MortSemAcum,0) AS MortSemAcum \
,nvl(MO.MortDia,0)  / nvl((FIN.Inventario*1.0),1) AS PorcMortDia \
,nvl(MORT.MortAcum,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortDiaAcum \
,nvl(MORT.MortSem,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortSem \
,nvl(MORT.MortSemAcum,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortSemAcum \
,nvl(TPM.MortSem1,0) MortSem1,nvl(TPM.MortSem2,0) MortSem2,nvl(TPM.MortSem3,0) MortSem3,nvl(TPM.MortSem4,0) MortSem4,nvl(TPM.MortSem5,0) MortSem5 \
,nvl(TPM.MortSem6,0) MortSem6,nvl(TPM.MortSem7,0) MortSem7,nvl(TPM.MortSem8,0) MortSem8,nvl(TPM.MortSem9,0) MortSem9,nvl(TPM.MortSem10,0) MortSem10 \
,nvl(TPM.MortSem11,0) MortSem11,nvl(TPM.MortSem12,0) MortSem12,nvl(TPM.MortSem13,0) MortSem13,nvl(TPM.MortSem14,0) MortSem14,nvl(TPM.MortSem15,0) MortSem15 \
,nvl(TPM.MortSem16,0) MortSem16,nvl(TPM.MortSem17,0) MortSem17,nvl(TPM.MortSem18,0) MortSem18,nvl(TPM.MortSem19,0) MortSem19,nvl(TPM.MortSem20,0) MortSem20 \
,nvl(MortSemAcum1,0) MortSemAcum1,nvl(MortSemAcum2,0) MortSemAcum2,nvl(MortSemAcum3,0) MortSemAcum3,nvl(MortSemAcum4,0) MortSemAcum4,nvl(MortSemAcum5,0) MortSemAcum5 \
,nvl(MortSemAcum6,0) MortSemAcum6,nvl(MortSemAcum7,0) MortSemAcum7,nvl(MortSemAcum8,0) MortSemAcum8,nvl(MortSemAcum9,0) MortSemAcum9,nvl(MortSemAcum10,0) MortSemAcum10 \
,nvl(MortSemAcum11,0) MortSemAcum11,nvl(MortSemAcum12,0) MortSemAcum12,nvl(MortSemAcum13,0) MortSemAcum13,nvl(MortSemAcum14,0) MortSemAcum14,nvl(MortSemAcum15,0) MortSemAcum15 \
,nvl(MortSemAcum16,0) MortSemAcum16,nvl(MortSemAcum17,0) MortSemAcum17,nvl(MortSemAcum18,0) MortSemAcum18,nvl(MortSemAcum19,0) MortSemAcum19,nvl(MortSemAcum20,0) MortSemAcum20 \
,nvl(MO.STDMortDia, 0) AS STDMortDia \
,nvl(MO.STDMortDiaAcum,0) AS STDMortAcum \
,nvl(MO.U_PEAccidentados,0) AS U_PEAccidentados \
,nvl(MO.U_PEHigadoGraso,0) AS U_PEHigadoGraso \
,nvl(MO.U_PEHepatomegalia,0) AS U_PEHepatomegalia \
,nvl(MO.U_PEHigadoHemorragico,0) AS U_PEHigadoHemorragico \
,nvl(MO.U_PEInanicion,0) AS U_PEInanicion \
,nvl(MO.U_PEProblemaRespiratorio,0) AS U_PEProblemaRespiratorio \
,nvl(MO.U_PESCH,0) AS U_PESCH \
,nvl(MO.U_PEEnteritis,0) AS U_PEEnteritis \
,nvl(MO.U_PEAscitis,0) AS U_PEAscitis \
,nvl(MO.U_PEMuerteSubita,0) AS U_PEMuerteSubita \
,nvl(MO.U_PEEstresPorCalor,0) AS U_PEEstresPorCalor \
,nvl(MO.U_PEHidropericardio,0) AS U_PEHidropericardio \
,nvl(MO.U_PEHemopericardio,0) AS U_PEHemopericardio \
,nvl(MO.U_PEUratosis,0) AS U_PEUratosis \
,nvl(MO.U_PEMaterialCaseoso,0) AS U_PEMaterialCaseoso \
,nvl(MO.U_PEOnfalitis,0) AS U_PEOnfalitis \
,nvl(MO.U_PERetencionDeYema,0) AS U_PERetencionDeYema \
,nvl(MO.U_PEErosionDeMolleja,0) AS U_PEErosionDeMolleja \
,nvl(MO.U_PEHemorragiaMusculos,0) AS U_PEHemorragiaMusculos \
,nvl(MO.U_PESangreEnCiego,0) AS U_PESangreEnCiego \
,nvl(MO.U_PEPericarditis,0) AS U_PEPericarditis \
,nvl(MO.U_PEPeritonitis,0) AS U_PEPeritonitis \
,nvl(MO.U_PEProlapso,0) AS U_PEProlapso \
,nvl(MO.U_PEPicaje,0) AS U_PEPicaje \
,nvl(MO.U_PERupturaAortica,0) AS U_PERupturaAortica \
,nvl(MO.U_PEBazoMoteado,0) AS U_PEBazoMoteado \
,nvl(MO.U_PENoViable,0) AS U_PENoViable \
,nvl(MO.U_PEAerosaculitisG2,0) AS U_PEAerosaculitisG2 \
,nvl(MO.U_PECojera,0) AS U_PECojera \
,nvl(MO.U_PEHigadoIcterico,0) AS U_PEHigadoIcterico \
,nvl(MO.U_PEMaterialCaseoso_po1ra,0) AS U_PEMaterialCaseoso_po1ra \
,nvl(MO.U_PEMaterialCaseosoMedRetr,0) AS U_PEMaterialCaseosoMedRetr \
,nvl(MO.U_PENecrosisHepatica,0) AS U_PENecrosisHepatica \
,nvl(MO.U_PENeumonia,0) AS U_PENeumonia \
,nvl(MO.U_PESepticemia,0) AS U_PESepticemia \
,nvl(MO.U_PEVomitoNegro,0) AS U_PEVomitoNegro \
,nvl(MO.U_PEAsperguillius,0) AS U_PEAsperguillius \
,nvl(MO.U_PEBazoGrandeMot,0) AS U_PEBazoGrandeMot \
,nvl(MO.U_PECorazonGrande,0) AS U_PECorazonGrande \
,nvl(MO.U_PECuadroToxico,0) AS U_PECuadroToxico \
,nvl(ACM.AcumPEAccidentados,0) AS AcumPEAccidentados \
,nvl(ACM.AcumPEHigadoGraso,0) AS AcumPEHigadoGraso \
,nvl(ACM.AcumPEHepatomegalia,0) AS AcumPEHepatomegalia \
,nvl(ACM.AcumPEHigadoHemorragico,0) AS AcumPEHigadoHemorragico \
,nvl(ACM.AcumPEInanicion,0) AS AcumPEInanicion \
,nvl(ACM.AcumPEProblemaRespiratorio,0) AS AcumPEProblemaRespiratorio \
,nvl(ACM.AcumPESCH,0) AS AcumPESCH \
,nvl(ACM.AcumPEEnteritis,0) AS AcumPEEnteritis \
,nvl(ACM.AcumPEAscitis,0) AS AcumPEAscitis \
,nvl(ACM.AcumPEMuerteSubita,0) AS AcumPEMuerteSubita \
,nvl(ACM.AcumPEEstresPorCalor,0) AS AcumPEEstresPorCalor \
,nvl(ACM.AcumPEHidropericardio,0) AS AcumPEHidropericardio \
,nvl(ACM.AcumPEHemopericardio,0) AS AcumPEHemopericardio \
,nvl(ACM.AcumPEUratosis,0) AS AcumPEUratosis \
,nvl(ACM.AcumPEMaterialCaseoso,0) AS AcumPEMaterialCaseoso \
,nvl(ACM.AcumPEOnfalitis,0) AS AcumPEOnfalitis \
,nvl(ACM.AcumPERetencionDeYema,0) AS AcumPERetencionDeYema \
,nvl(ACM.AcumPEErosionDeMolleja,0) AS AcumPEErosionDeMolleja \
,nvl(ACM.AcumPEHemorragiaMusculos,0) AS AcumPEHemorragiaMusculos \
,nvl(ACM.AcumPESangreEnCiego,0) AS AcumPESangreEnCiego \
,nvl(ACM.AcumPEPericarditis,0) AS AcumPEPericarditis \
,nvl(ACM.AcumPEPeritonitis,0) AS AcumPEPeritonitis \
,nvl(ACM.AcumPEProlapso,0) AS AcumPEProlapso \
,nvl(ACM.AcumPEPicaje,0) AS AcumPEPicaje \
,nvl(ACM.AcumPERupturaAortica,0) AS AcumPERupturaAortica \
,nvl(ACM.AcumPEBazoMoteado,0) AS AcumPEBazoMoteado \
,nvl(ACM.AcumPENoViable,0) AS AcumPENoViable \
,nvl(ACM.AcumPEAerosaculitisG2,0) AS AcumPEAerosaculitisG2 \
,nvl(ACM.AcumPECojera,0) AS AcumPECojera \
,nvl(ACM.AcumPEHigadoIcterico,0) AS AcumPEHigadoIcterico \
,nvl(ACM.AcumPEMaterialCaseoso_po1ra,0) AS AcumPEMaterialCaseoso_po1ra \
,nvl(ACM.AcumPEMaterialCaseosoMedRetr,0) AS AcumPEMaterialCaseosoMedRetr \
,nvl(ACM.AcumPENecrosisHepatica,0) AS AcumPENecrosisHepatica \
,nvl(ACM.AcumPENeumonia,0) AS AcumPENeumonia \
,nvl(ACM.AcumPESepticemia,0) AS AcumPESepticemia \
,nvl(ACM.AcumPEVomitoNegro,0) AS AcumPEVomitoNegro \
,nvl(ACM.AcumPEAsperguillius,0) AS AcumPEAsperguillius \
,nvl(ACM.AcumPEBazoGrandeMot,0) AS AcumPEBazoGrandeMot \
,nvl(ACM.AcumPECorazonGrande,0) AS AcumPECorazonGrande \
,nvl(ACM.AcumPECuadroToxico,0) AS AcumPECuadroToxico \
,OL1.cmoratalidad1 AS PrimeraLesion,ol1.nmortalidad1 AS PrimeraLesionNom,OL1.cmoratalidad2 AS SegundaLesion,OL1.nmortalidad2 AS SegundaLesionNom,OL1.cmoratalidad3 AS TerceraLesion,OL1.nmortalidad3 AS TerceraLesionNom \
,ALO.ListaPadre \
,ALO.IncubadoraMayor \
,ALO.ListaIncubadora \
,ALO.PadreMayor \
,ALO.RazaMayor \
,ALO.EdadPadreCorralDescrip \
,ALO.EdadPadreCorral \
,ALO.PorcAlojPadreMayor \
,ALO.PorcRazaMayor \
,ALO.PorcIncMayor \
,MO.U_categoria categoria \
,nvl(MO.FlagAtipico,1) FlagAtipico \
,nvl(MO.U_PavosBB,0) AS PavosBBMortIncub \
,MO.FlagTransfPavos \
,MO.SourceComplexEntityNo \
,MO.DestinationComplexEntityNo \
,LS.SemAccidentados \
,LS.SemAscitis \
,LS.SemBazoMoteado \
,LS.SemEnteritis \
,LS.SemErosionDeMolleja \
,LS.SemEstresPorCalor \
,LS.SemHemopericardio \
,LS.SemHemorragiaMusculos \
,LS.SemHepatomegalia \
,LS.SemHidropericardio \
,LS.SemHigadoGraso \
,LS.SemHigadoHemorragico \
,LS.SemInanicion \
,LS.SemMaterialCaseoso \
,LS.SemMuerteSubita \
,LS.SemNoViable \
,LS.SemOnfalitis \
,LS.SemPericarditis \
,LS.SemPeritonitis \
,LS.SemPicaje \
,LS.SemProblemaRespiratorio \
,LS.SemProlapso \
,LS.SemRetencionDeYema \
,LS.SemRupturaAortica \
,LS.SemSangreEnCiego \
,LS.SemSCH \
,LS.SemUratosis \
,LS.SemAerosaculitisG2 \
,LS.SemCojera \
,LS.SemHigadoIcterico \
,LS.SemMaterialCaseoso_po1ra \
,LS.SemMaterialCaseosoMedRetr \
,LS.SemNecrosisHepatica \
,LS.SemNeumonia \
,LS.SemSepticemia \
,LS.SemVomitoNegro \
,LS.SemAsperguillius \
,LS.SemBazoGrandeMot \
,LS.SemCorazonGrande \
,LS.SemCuadroToxico \
,OLS1.scmortalidadxlesion1 AS SemPrimeraLesion \
,OLS1.snmortalidad1 AS SemPrimeraLesionNom \
,OLS1.scmortalidadxlesion2 AS SemSegundaLesion \
,OLS1.snmortalidad2 AS SemSegundaLesionNom \
,OLS1.scmortalidadxlesion3 AS SemTerceraLesion \
,OLS1.snmortalidad3 AS SemTerceraLesionNom \
,OLS1.scmortalidadxlesion4 AS SemOtrosLesion \
,OLS1.snmortalidad4 AS SemOtrosLesionNom \
,nvl(MO.U_RuidosTotales,0) U_RuidosTotales \
,TCAF.ListaFormulaNo \
,TCAF.ListaFormulaName \
,ALO.TipoOrigen \
,TPNV.NoViableSem1 \
,TPNV.NoViableSem2 \
,TPNV.NoViableSem3 \
,TPNV.NoViableSem4 \
,TPNV.NoViableSem5 \
,TPNV.NoViableSem6 \
,TPNV.NoViableSem7 \
,TPNV.NoViableSem8 \
FROM {database_name_gl}.stg_ProduccionDetalle MO \
LEFT JOIN {database_name_tmp}.AcumuladorsMortalidad MORT ON MORT.ComplexEntityNo = MO.ComplexEntityNo AND MORT.pk_diasvida = MO.pk_diasvida and mo.xdate = mort.xdate \
LEFT JOIN {database_name_tmp}.prueba ACM ON ACM.ComplexEntityNo=MO.ComplexEntityNo AND MO.pk_diasvida= ACM.pk_diasvida \
LEFT JOIN {database_name_gl}.ft_ingresocons FIN ON FIN.ComplexEntityNo = MO.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.lesion OL1 ON CAST(OL1.complexEntityNo AS VARCHAR(50)) = MO.ComplexEntityNo AND OL1.pk_diasvida = MO.pk_diasvida \
LEFT JOIN {database_name_tmp}.OrdenlesionSemana OLS1 ON CAST(OLS1.complexEntityNo AS VARCHAR(50)) = MO.ComplexEntityNo AND OLS1.pk_semanavida = MO.pk_semanavida \
LEFT JOIN {database_name_tmp}.lesionesSemanal LS ON LS.ComplexEntityNo = MO.ComplexEntityNo AND LS.pk_semanavida = MO.pk_semanavida \
LEFT JOIN {database_name_tmp}.alojamiento ALO ON MO.ComplexEntityNo = ALO.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.TablaPivotMortSem TPM ON MO.ComplexEntityNo = TPM.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.TablaPivotMortSemAcum TPMA ON MO.ComplexEntityNo = TPMA.ComplexEntityNo \
LEFT JOIN {database_name_gl}.lk_especie LEP ON LEP.cespecie = ALO.RazaMayor \
LEFT JOIN {database_name_tmp}.TablaConsumoAlimentoXFormula TCAF ON MO.ComplexEntityNo = TCAF.ComplexEntityNo and MO.pk_diasvida = TCAF.pk_diasvida \
LEFT JOIN {database_name_tmp}.TablaPivotNoViableSem TPNV on MO.ComplexEntityNo = TPNV.ComplexEntityNo \
WHERE MO.pk_diasvida > 0 AND \
(date_format(MO.DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
and MO.pk_lote not in (select pk_lote from {database_name_gl}.lk_lote where (Upper(clote) like 'P186%' AND SUBSTRING(clote,8,4) >= '11')) \
AND MO.pk_plantel NOT IN (21,180) \
AND MO.GRN = 'P' \
AND MO.pk_division = 4") 

print("carga df_MortalidadDetalle1Temp1 --> Registros procesados:", df_MortalidadDetalle1Temp1.count())
df_MortalidadDetalle1Temp2 = spark.sql(f"SELECT \
nvl(MO.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha = cast('1899-11-30' as date))) pk_tiempo \
,nvl(MO.DescripFecha,cast('1899-11-30' as date)) fecha \
,nvl(MO.pk_empresa,(select pk_empresa from {database_name_gl}.lk_empresa where cempresa=4)) pk_empresa \
,nvl(MO.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) AS pk_division \
,nvl(MO.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) AS pk_zona \
,nvl(MO.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) AS pk_subzona \
,nvl(MO.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) AS pk_plantel \
,nvl(MO.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) AS pk_lote \
,nvl(MO.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) AS pk_galpon \
,nvl(MO.pk_sexo, (select pk_sexo from {database_name_gl}.lk_sexo where csexo=0)) pk_sexo \
,nvl(MO.pk_standard,(select pk_standard from {database_name_gl}.lk_standard where cstandard='0')) pk_standard \
,nvl(MO.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto \
,nvl(MO.pk_tipoproducto,(select pk_tipoproducto from {database_name_gl}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(LEP.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie \
,nvl(MO.pk_estado,(select pk_estado from {database_name_gl}.lk_estado where cestado=0)) pk_estado \
,nvl(MO.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0')) pk_administrador \
,nvl(MO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) pk_proveedor \
,MO.pk_semanavida,MO.pk_diasvida,MO.ComplexEntityNo,MO.FechaNacimiento,MO.FechaCierre \
,nvl(FIN.Inventario,0) AS PobInicial \
,nvl(MO.MortDia,0) AS MortDia \
,nvl(MORT.MortAcum,0) AS MortAcum \
,nvl(MORT.MortSem,0) AS MortSem \
,nvl(MORT.MortSemAcum,0) AS MortSemAcum \
,nvl(MO.MortDia,0)  / nvl((FIN.Inventario*1.0),1) AS PorcMortDia \
,nvl(MORT.MortAcum,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortDiaAcum \
,nvl(MORT.MortSem,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortSem \
,nvl(MORT.MortSemAcum,0) / nvl((FIN.Inventario*1.0),1) AS PorcMortSemAcum \
,nvl(TPM.MortSem1,0) MortSem1,nvl(TPM.MortSem2,0) MortSem2,nvl(TPM.MortSem3,0) MortSem3,nvl(TPM.MortSem4,0) MortSem4,nvl(TPM.MortSem5,0) MortSem5 \
,nvl(TPM.MortSem6,0) MortSem6,nvl(TPM.MortSem7,0) MortSem7,nvl(TPM.MortSem8,0) MortSem8,nvl(TPM.MortSem9,0) MortSem9,nvl(TPM.MortSem10,0) MortSem10 \
,nvl(TPM.MortSem11,0) MortSem11,nvl(TPM.MortSem12,0) MortSem12,nvl(TPM.MortSem13,0) MortSem13,nvl(TPM.MortSem14,0) MortSem14,nvl(TPM.MortSem15,0) MortSem15 \
,nvl(TPM.MortSem16,0) MortSem16,nvl(TPM.MortSem17,0) MortSem17,nvl(TPM.MortSem18,0) MortSem18,nvl(TPM.MortSem19,0) MortSem19,nvl(TPM.MortSem20,0) MortSem20 \
,nvl(MortSemAcum1,0) MortSemAcum1,nvl(MortSemAcum2,0) MortSemAcum2,nvl(MortSemAcum3,0) MortSemAcum3,nvl(MortSemAcum4,0) MortSemAcum4,nvl(MortSemAcum5,0) MortSemAcum5 \
,nvl(MortSemAcum6,0) MortSemAcum6,nvl(MortSemAcum7,0) MortSemAcum7,nvl(MortSemAcum8,0) MortSemAcum8,nvl(MortSemAcum9,0) MortSemAcum9,nvl(MortSemAcum10,0) MortSemAcum10 \
,nvl(MortSemAcum11,0) MortSemAcum11,nvl(MortSemAcum12,0) MortSemAcum12,nvl(MortSemAcum13,0) MortSemAcum13,nvl(MortSemAcum14,0) MortSemAcum14,nvl(MortSemAcum15,0) MortSemAcum15 \
,nvl(MortSemAcum16,0) MortSemAcum16,nvl(MortSemAcum17,0) MortSemAcum17,nvl(MortSemAcum18,0) MortSemAcum18,nvl(MortSemAcum19,0) MortSemAcum19,nvl(MortSemAcum20,0) MortSemAcum20 \
,nvl(MO.STDMortDia, 0) AS STDMortDia \
,nvl(MO.STDMortDiaAcum,0) AS STDMortAcum \
,nvl(MO.U_PEAccidentados,0) AS U_PEAccidentados \
,nvl(MO.U_PEHigadoGraso,0) AS U_PEHigadoGraso \
,nvl(MO.U_PEHepatomegalia,0) AS U_PEHepatomegalia \
,nvl(MO.U_PEHigadoHemorragico,0) AS U_PEHigadoHemorragico \
,nvl(MO.U_PEInanicion,0) AS U_PEInanicion \
,nvl(MO.U_PEProblemaRespiratorio,0) AS U_PEProblemaRespiratorio \
,nvl(MO.U_PESCH,0) AS U_PESCH \
,nvl(MO.U_PEEnteritis,0) AS U_PEEnteritis \
,nvl(MO.U_PEAscitis,0) AS U_PEAscitis \
,nvl(MO.U_PEMuerteSubita,0) AS U_PEMuerteSubita \
,nvl(MO.U_PEEstresPorCalor,0) AS U_PEEstresPorCalor \
,nvl(MO.U_PEHidropericardio,0) AS U_PEHidropericardio \
,nvl(MO.U_PEHemopericardio,0) AS U_PEHemopericardio \
,nvl(MO.U_PEUratosis,0) AS U_PEUratosis \
,nvl(MO.U_PEMaterialCaseoso,0) AS U_PEMaterialCaseoso \
,nvl(MO.U_PEOnfalitis,0) AS U_PEOnfalitis \
,nvl(MO.U_PERetencionDeYema,0) AS U_PERetencionDeYema \
,nvl(MO.U_PEErosionDeMolleja,0) AS U_PEErosionDeMolleja \
,nvl(MO.U_PEHemorragiaMusculos,0) AS U_PEHemorragiaMusculos \
,nvl(MO.U_PESangreEnCiego,0) AS U_PESangreEnCiego \
,nvl(MO.U_PEPericarditis,0) AS U_PEPericarditis \
,nvl(MO.U_PEPeritonitis,0) AS U_PEPeritonitis \
,nvl(MO.U_PEProlapso,0) AS U_PEProlapso \
,nvl(MO.U_PEPicaje,0) AS U_PEPicaje \
,nvl(MO.U_PERupturaAortica,0) AS U_PERupturaAortica \
,nvl(MO.U_PEBazoMoteado,0) AS U_PEBazoMoteado \
,nvl(MO.U_PENoViable,0) AS U_PENoViable \
,nvl(MO.U_PEAerosaculitisG2,0) AS U_PEAerosaculitisG2 \
,nvl(MO.U_PECojera,0) AS U_PECojera \
,nvl(MO.U_PEHigadoIcterico,0) AS U_PEHigadoIcterico \
,nvl(MO.U_PEMaterialCaseoso_po1ra,0) AS U_PEMaterialCaseoso_po1ra \
,nvl(MO.U_PEMaterialCaseosoMedRetr,0) AS U_PEMaterialCaseosoMedRetr \
,nvl(MO.U_PENecrosisHepatica,0) AS U_PENecrosisHepatica \
,nvl(MO.U_PENeumonia,0) AS U_PENeumonia \
,nvl(MO.U_PESepticemia,0) AS U_PESepticemia \
,nvl(MO.U_PEVomitoNegro,0) AS U_PEVomitoNegro \
,nvl(MO.U_PEAsperguillius,0) AS U_PEAsperguillius \
,nvl(MO.U_PEBazoGrandeMot,0) AS U_PEBazoGrandeMot \
,nvl(MO.U_PECorazonGrande,0) AS U_PECorazonGrande \
,nvl(MO.U_PECuadroToxico,0) AS U_PECuadroToxico \
,nvl(ACM.AcumPEAccidentados,0) AS AcumPEAccidentados \
,nvl(ACM.AcumPEHigadoGraso,0) AS AcumPEHigadoGraso \
,nvl(ACM.AcumPEHepatomegalia,0) AS AcumPEHepatomegalia \
,nvl(ACM.AcumPEHigadoHemorragico,0) AS AcumPEHigadoHemorragico \
,nvl(ACM.AcumPEInanicion,0) AS AcumPEInanicion \
,nvl(ACM.AcumPEProblemaRespiratorio,0) AS AcumPEProblemaRespiratorio \
,nvl(ACM.AcumPESCH,0) AS AcumPESCH \
,nvl(ACM.AcumPEEnteritis,0) AS AcumPEEnteritis \
,nvl(ACM.AcumPEAscitis,0) AS AcumPEAscitis \
,nvl(ACM.AcumPEMuerteSubita,0) AS AcumPEMuerteSubita \
,nvl(ACM.AcumPEEstresPorCalor,0) AS AcumPEEstresPorCalor \
,nvl(ACM.AcumPEHidropericardio,0) AS AcumPEHidropericardio \
,nvl(ACM.AcumPEHemopericardio,0) AS AcumPEHemopericardio \
,nvl(ACM.AcumPEUratosis,0) AS AcumPEUratosis \
,nvl(ACM.AcumPEMaterialCaseoso,0) AS AcumPEMaterialCaseoso \
,nvl(ACM.AcumPEOnfalitis,0) AS AcumPEOnfalitis \
,nvl(ACM.AcumPERetencionDeYema,0) AS AcumPERetencionDeYema \
,nvl(ACM.AcumPEErosionDeMolleja,0) AS AcumPEErosionDeMolleja \
,nvl(ACM.AcumPEHemorragiaMusculos,0) AS AcumPEHemorragiaMusculos \
,nvl(ACM.AcumPESangreEnCiego,0) AS AcumPESangreEnCiego \
,nvl(ACM.AcumPEPericarditis,0) AS AcumPEPericarditis \
,nvl(ACM.AcumPEPeritonitis,0) AS AcumPEPeritonitis \
,nvl(ACM.AcumPEProlapso,0) AS AcumPEProlapso \
,nvl(ACM.AcumPEPicaje,0) AS AcumPEPicaje \
,nvl(ACM.AcumPERupturaAortica,0) AS AcumPERupturaAortica \
,nvl(ACM.AcumPEBazoMoteado,0) AS AcumPEBazoMoteado \
,nvl(ACM.AcumPENoViable,0) AS AcumPENoViable \
,nvl(ACM.AcumPEAerosaculitisG2,0) AS AcumPEAerosaculitisG2 \
,nvl(ACM.AcumPECojera,0) AS AcumPECojera \
,nvl(ACM.AcumPEHigadoIcterico,0) AS AcumPEHigadoIcterico \
,nvl(ACM.AcumPEMaterialCaseoso_po1ra,0) AS AcumPEMaterialCaseoso_po1ra \
,nvl(ACM.AcumPEMaterialCaseosoMedRetr,0) AS AcumPEMaterialCaseosoMedRetr \
,nvl(ACM.AcumPENecrosisHepatica,0) AS AcumPENecrosisHepatica \
,nvl(ACM.AcumPENeumonia,0) AS AcumPENeumonia \
,nvl(ACM.AcumPESepticemia,0) AS AcumPESepticemia \
,nvl(ACM.AcumPEVomitoNegro,0) AS AcumPEVomitoNegro \
,nvl(ACM.AcumPEAsperguillius,0) AS AcumPEAsperguillius \
,nvl(ACM.AcumPEBazoGrandeMot,0) AS AcumPEBazoGrandeMot \
,nvl(ACM.AcumPECorazonGrande,0) AS AcumPECorazonGrande \
,nvl(ACM.AcumPECuadroToxico,0) AS AcumPECuadroToxico \
,OL1.cmoratalidad1 AS PrimeraLesion,ol1.nmortalidad1 AS PrimeraLesionNom,OL1.cmoratalidad2 AS SegundaLesion,OL1.nmortalidad2 AS SegundaLesionNom,OL1.cmoratalidad3 AS TerceraLesion,OL1.nmortalidad3 AS TerceraLesionNom \
,ALO.ListaPadre \
,ALO.IncubadoraMayor \
,ALO.ListaIncubadora \
,ALO.PadreMayor \
,ALO.RazaMayor \
,ALO.EdadPadreCorralDescrip \
,ALO.EdadPadreCorral \
,ALO.PorcAlojPadreMayor \
,ALO.PorcRazaMayor \
,ALO.PorcIncMayor \
,MO.U_categoria categoria \
,nvl(MO.FlagAtipico,1) FlagAtipico \
,nvl(MO.U_PavosBB,0) AS PavosBBMortIncub \
,MO.FlagTransfPavos \
,MO.SourceComplexEntityNo \
,MO.DestinationComplexEntityNo \
,LS.SemAccidentados \
,LS.SemAscitis \
,LS.SemBazoMoteado \
,LS.SemEnteritis \
,LS.SemErosionDeMolleja \
,LS.SemEstresPorCalor \
,LS.SemHemopericardio \
,LS.SemHemorragiaMusculos \
,LS.SemHepatomegalia \
,LS.SemHidropericardio \
,LS.SemHigadoGraso \
,LS.SemHigadoHemorragico \
,LS.SemInanicion \
,LS.SemMaterialCaseoso \
,LS.SemMuerteSubita \
,LS.SemNoViable \
,LS.SemOnfalitis \
,LS.SemPericarditis \
,LS.SemPeritonitis \
,LS.SemPicaje \
,LS.SemProblemaRespiratorio \
,LS.SemProlapso \
,LS.SemRetencionDeYema \
,LS.SemRupturaAortica \
,LS.SemSangreEnCiego \
,LS.SemSCH \
,LS.SemUratosis \
,LS.SemAerosaculitisG2 \
,LS.SemCojera \
,LS.SemHigadoIcterico \
,LS.SemMaterialCaseoso_po1ra \
,LS.SemMaterialCaseosoMedRetr \
,LS.SemNecrosisHepatica \
,LS.SemNeumonia \
,LS.SemSepticemia \
,LS.SemVomitoNegro \
,LS.SemAsperguillius \
,LS.SemBazoGrandeMot \
,LS.SemCorazonGrande \
,LS.SemCuadroToxico \
,OLS1.scmortalidadxlesion1 AS SemPrimeraLesion \
,OLS1.snmortalidad1 AS SemPrimeraLesionNom \
,OLS1.scmortalidadxlesion2 AS SemSegundaLesion \
,OLS1.snmortalidad2 AS SemSegundaLesionNom \
,OLS1.scmortalidadxlesion3 AS SemTerceraLesion \
,OLS1.snmortalidad3 AS SemTerceraLesionNom \
,OLS1.scmortalidadxlesion4 AS SemOtrosLesion \
,OLS1.snmortalidad4 AS SemOtrosLesionNom \
,nvl(MO.U_RuidosTotales,0) U_RuidosTotales \
,TCAF.ListaFormulaNo \
,TCAF.ListaFormulaName \
,ALO.TipoOrigen \
,TPNV.NoViableSem1 \
,TPNV.NoViableSem2 \
,TPNV.NoViableSem3 \
,TPNV.NoViableSem4 \
,TPNV.NoViableSem5 \
,TPNV.NoViableSem6 \
,TPNV.NoViableSem7 \
,TPNV.NoViableSem8 \
FROM {database_name_gl}.stg_ProduccionDetalle MO \
LEFT JOIN {database_name_tmp}.AcumuladorsMortalidad MORT ON MORT.ComplexEntityNo = MO.ComplexEntityNo AND MORT.pk_diasvida = MO.pk_diasvida and mo.xdate = mort.xdate \
LEFT JOIN {database_name_tmp}.prueba ACM ON ACM.ComplexEntityNo=MO.ComplexEntityNo AND MO.pk_diasvida= ACM.pk_diasvida \
LEFT JOIN {database_name_gl}.ft_ingresocons FIN ON FIN.ComplexEntityNo = MO.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.lesion OL1 ON CAST(OL1.complexEntityNo AS VARCHAR(50)) = MO.ComplexEntityNo AND OL1.pk_diasvida = MO.pk_diasvida \
LEFT JOIN {database_name_tmp}.OrdenlesionSemana OLS1 ON CAST(OLS1.complexEntityNo AS VARCHAR(50)) = MO.ComplexEntityNo AND OLS1.pk_semanavida = MO.pk_semanavida \
LEFT JOIN {database_name_tmp}.lesionesSemanal LS ON LS.ComplexEntityNo = MO.ComplexEntityNo AND LS.pk_semanavida = MO.pk_semanavida \
LEFT JOIN {database_name_tmp}.alojamiento ALO ON MO.ComplexEntityNo = ALO.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.TablaPivotMortSem TPM ON MO.ComplexEntityNo = TPM.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.TablaPivotMortSemAcum TPMA ON MO.ComplexEntityNo = TPMA.ComplexEntityNo \
LEFT JOIN {database_name_gl}.lk_especie LEP ON LEP.cespecie = ALO.RazaMayor \
LEFT JOIN {database_name_tmp}.TablaConsumoAlimentoXFormula TCAF ON MO.ComplexEntityNo = TCAF.ComplexEntityNo and MO.pk_diasvida = TCAF.pk_diasvida \
LEFT JOIN {database_name_tmp}.TablaPivotNoViableSem TPNV on MO.ComplexEntityNo = TPNV.ComplexEntityNo \
WHERE MO.pk_diasvida > 0 AND \
(date_format(MO.DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
AND MO.GRN = 'P' \
AND MO.pk_division = 2") 

print("carga df_MortalidadDetalle1Temp2 --> Registros procesados:", df_MortalidadDetalle1Temp2.count())
df_MortalidadDetalle1 = df_MortalidadDetalle1Temp1.union(df_MortalidadDetalle1Temp2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadDetalle1"
}
df_MortalidadDetalle1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadDetalle1")

print("carga MortalidadDetalle1 --> Registros procesados:", df_MortalidadDetalle1.count())
unpivot_cols = [
    "AcumPEAccidentados", "AcumPEAscitis", "AcumPEBazoMoteado", "AcumPEEnteritis", "AcumPEErosionDeMolleja",
    "AcumPEEstresPorCalor", "AcumPEHemopericardio", "AcumPEHemorragiaMusculos", "AcumPEHepatomegalia",
    "AcumPEHidropericardio", "AcumPEHigadoGraso", "AcumPEHigadoHemorragico", "AcumPEInanicion", "AcumPEMaterialCaseoso",
    "AcumPEMuerteSubita", "AcumPENoViable", "AcumPEOnfalitis", "AcumPEPericarditis", "AcumPEPeritonitis", "AcumPEPicaje",
    "AcumPEProblemaRespiratorio", "AcumPEProlapso", "AcumPERetencionDeYema", "AcumPERupturaAortica", "AcumPESangreEnCieGO",
    "AcumPESCH", "AcumPEUratosis", "AcumPEAerosaculitisG2", "AcumPECojera", "AcumPEHigadoIcterico", "AcumPEMaterialCaseoso_po1ra",
    "AcumPEMaterialCaseosoMedRetr", "AcumPENecrosisHepatica", "AcumPENeumonia", "AcumPESepticemia", "AcumPEVomitoNegro",
    "AcumPEAsperguillius", "AcumPEBazoGrandeMot", "AcumPECorazonGrande", "AcumPECuadroToxico"
]

stack_args = ", ".join(f"'{col}', `{col}`" for col in unpivot_cols)
num_cols = len(unpivot_cols)

print(num_cols)
print(stack_args)

df_detmortacumTemp = spark.sql(f"""
SELECT DISTINCT
    ComplexEntityNo,
    pk_diasvida,
    cmortalidadacum,
    causaacum
FROM (
    SELECT
        ComplexEntityNo,
        pk_diasvida,
        fecha,
        stack(
            {num_cols},
            {stack_args}
        ) AS (causaacum, cmortalidadacum)
    FROM
        {database_name_tmp}.MortalidadDetalle1
) AS unpivoted_data
WHERE
    cmortalidadacum > 0
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/detmortacum"
}
df_detmortacumTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.detmortacum")

print("carga detmortacum --> Registros procesados:", df_detmortacumTemp.count())
df_ft_mortalidad_upd = spark.sql(f"""SELECT 
 a.pk_empresa 
,a.pk_division 
,a.pk_zona 
,a.pk_subzona 
,a.pk_mortalidad 
,a.pk_diasvida 
,a.pk_administrador 
,a.pk_semanavida 
,a.pk_plantel 
,a.pk_lote 
,a.pk_galpon 
,a.pk_sexo 
,a.pk_tiempo 
,a.pk_standard 
,a.pk_producto
,a.pk_estado 
,a.pk_proveedor 
,a.complexentityno 
,a.nacimiento 
,a.edad 
,a.cmoratalidad 
,a.pmortalidad 
,nvl(c.cmortalidadacum,0) cmortalidadacum 
,CASE WHEN coalesce(cingreso,0.0) = 0 THEN 0 ELSE ROUND((coalesce(c.cmortalidadacum,0) / (coalesce(cingreso,0)*1.0))*100,3) end pmortalidadacum 
,a.cingreso 
,a.OrderLesion 
,a.categoria 
,a.FlagAtipico 
,a.scmortalidad 
,a.spmortalidad 
,a.scmortalidadxlesion 
,a.sOrdenLesion 
,a.spmortalidadxlesion 
,a.scmortalidadxlesionxlote 
,a.spmortalidadxlesionxlote 
,a.sOrderLesionLote 
,a.DescripFecha 
,a.DescripEmpresa
,a.DescripDivision
,a.DescripZona
,a.DescripSubzona
,a.DescripMortalidad
,a.DescripDiaVida
,a.DescripAdministrador
,a.DescripSemanaVida
,a.Plantel
,a.Lote
,a.Galpon
,a.DescripSexo
,a.DescripStandard
,a.DescripProveedor
,a.DescripProducto
,a.DescripEstado
FROM {database_name_gl}.ft_mortalidad a \
LEFT JOIN {database_name_gl}.lk_causamortalidad b ON a.pk_mortalidad = b.pk_causamortalidad \
LEFT JOIN {database_name_tmp}.detmortacum c ON a.complexentityno = c.complexentityno AND a.pk_diasvida = c.pk_diasvida and SUBSTRING(UPPER(irn),5,30) = SUBSTRING(UPPER(causaacum),7,30)
where date_format(descripfecha, 'yyyyMM') >= date_format(add_months(trunc(current_date(), 'month'), -4), 'yyyyMM')
""")

print('carga df_ft_mortalidad_upd' , df_ft_mortalidad_upd.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes24 = spark.read.format("parquet").load(path_target24)
    datos_existentes24 = True
    logger.info(f"Datos existentes de ft_mortalidad cargados: {df_existentes24.count()} registros")
except:
    datos_existentes24 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad")

if datos_existentes24:
    existing_data24 = spark.read.format("parquet").load(path_target24)
    data_after_delete24 = existing_data24.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data24 = df_ft_mortalidad_upd.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data24 = filtered_new_data24.union(data_after_delete24)                             
   
    cant_ingresonuevo24 = filtered_new_data24.count()
    cant_total24 = final_data24.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidadTemporal"
    }
    final_data24.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidadTemporal")

    final_data24_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidadTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad"
    }
    final_data24_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad : {cant_ingresonuevo24}")
    print(f"Total de registros en la tabla ft_mortalidad : {cant_total24}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidadTemporal')
    print(f"Tabla ft_mortalidadTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad"
    }
    df_ft_mortalidad_upd.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad")

#print("carga UPD ft_mortalidad --> Registros procesados:", df_ft_mortalidad_upd.count())   
df_MortAcumMayorTemp = spark.sql(f"SELECT * \
,ROW_NUMBER() OVER (PARTITION BY MO.complexentityno,MO.pk_diasvida  ORDER BY MO.cmortalidadacum DESC) OrderLesionAcum \
FROM {database_name_tmp}.detmortacum mo \
ORDER BY 2,3")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortAcumMayor"
}
df_MortAcumMayorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortAcumMayor")

print("carga MortAcumMayor --> Registros procesados:", df_MortAcumMayorTemp.count())
df_MortalidadTemp =spark.sql(f"SELECT \
MAX(MO.pk_tiempo) pk_tiempo,max(MO.fecha) fecha,MO.pk_empresa,MO.pk_division,MO.pk_zona,MO.pk_subzona,MO.pk_plantel,MO.pk_lote,MO.pk_galpon,MO.pk_sexo,MO.pk_standard,MO.pk_producto, \
MO.pk_tipoproducto,MO.pk_especie,MO.pk_estado,MO.pk_administrador,MO.pk_proveedor,MO.pk_semanavida,MO.pk_diasvida,MO.ComplexEntityNo, \
MO.FechaNacimiento,MO.FechaCierre,AVG(MO.PobInicial) PobInicial, SUM(MO.MortDia) MortDia, MAX(MO.MortAcum)MortAcum, AVG(MO.MortSem)MortSem, \
AVG(MO.MortSemAcum) MortSemAcum, SUM(MO.PorcMortDia) PorcMortDia,AVG(MO.PorcMortDiaAcum) PorcMortDiaAcum,AVG(MO.PorcMortSem) PorcMortSem, \
AVG(MO.PorcMortSemAcum) PorcMortSemAcum,AVG(MO.MortSem1)MortSem1,AVG(MO.MortSem2) MortSem2,AVG(MO.MortSem3) MortSem3,AVG(MO.MortSem4) MortSem4, \
AVG(MO.MortSem5) MortSem5,AVG(MO.MortSem6) MortSem6,AVG(MO.MortSem7) MortSem7,AVG(MO.MortSem8) MortSem8,AVG(MO.MortSem9) MortSem9,AVG(MO.MortSem10) MortSem10, \
AVG(MO.MortSem11) MortSem11,AVG(MO.MortSem12) MortSem12,AVG(MO.MortSem13) MortSem13,AVG(MO.MortSem14) MortSem14,AVG(MO.MortSem15) MortSem15,AVG(MO.MortSem16) MortSem16, \
AVG(MO.MortSem17) MortSem17,AVG(MO.MortSem18) MortSem18,AVG(MO.MortSem19) MortSem19,AVG(MO.MortSem20) MortSem20, \
AVG(MO.MortSemAcum1) MortSemAcum1, \
AVG(MO.MortSemAcum2) MortSemAcum2,AVG(MO.MortSemAcum3) MortSemAcum3,AVG(MO.MortSemAcum4) MortSemAcum4,AVG(MO.MortSemAcum5) MortSemAcum5, \
AVG(MO.MortSemAcum6) MortSemAcum6,AVG(MO.MortSemAcum7) MortSemAcum7,AVG(MO.MortSemAcum8) MortSemAcum8,AVG(MO.MortSemAcum9) MortSemAcum9 \
,AVG(MO.MortSemAcum10) MortSemAcum10,AVG(MO.MortSemAcum11) MortSemAcum11,AVG(MO.MortSemAcum12) MortSemAcum12,AVG(MO.MortSemAcum13) MortSemAcum13 \
,AVG(MO.MortSemAcum14) MortSemAcum14,AVG(MO.MortSemAcum15) MortSemAcum15,AVG(MO.MortSemAcum16) MortSemAcum16,AVG(MO.MortSemAcum17) MortSemAcum17 \
,AVG(MO.MortSemAcum18) MortSemAcum18,AVG(MO.MortSemAcum19) MortSemAcum19,AVG(MO.MortSemAcum20) MortSemAcum20 \
,MAX(MO.STDMortDia) + nvl(AVG(STD.STDMortDia),0) STDMortDia \
,MAX(MO.STDMortAcum) +  nvl(AVG(STD.STDMortAcum),0) STDMortAcum \
,SUM(MO.U_PEAccidentados) U_PEAccidentados,SUM(MO.U_PEHigadoGraso) U_PEHigadoGraso,SUM(MO.U_PEHepatomegalia) U_PEHepatomegalia, \
SUM(MO.U_PEHigadoHemorragico) U_PEHigadoHemorragico,SUM(MO.U_PEInanicion) U_PEInanicion,SUM(MO.U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
SUM(MO.U_PESCH) U_PESCH,SUM(MO.U_PEEnteritis) U_PEEnteritis,SUM(MO.U_PEAscitis) U_PEAscitis,SUM(MO.U_PEMuerteSubita) U_PEMuerteSubita, \
SUM(MO.U_PEEstresPorCalor) U_PEEstresPorCalor,SUM(MO.U_PEHidropericardio) U_PEHidropericardio,SUM(MO.U_PEHemopericardio) U_PEHemopericardio, \
SUM(MO.U_PEUratosis) U_PEUratosis,SUM(MO.U_PEMaterialCaseoso) U_PEMaterialCaseoso,SUM(MO.U_PEOnfalitis) U_PEOnfalitis, \
SUM(MO.U_PERetencionDeYema) U_PERetencionDeYema,SUM(MO.U_PEErosionDeMolleja) U_PEErosionDeMolleja,SUM(MO.U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
SUM(MO.U_PESangreEnCiego) U_PESangreEnCiego,SUM(MO.U_PEPericarditis) U_PEPericarditis,SUM(MO.U_PEPeritonitis)U_PEPeritonitis, \
SUM(MO.U_PEProlapso)U_PEProlapso,SUM(MO.U_PEPicaje)U_PEPicaje,SUM(MO.U_PERupturaAortica) U_PERupturaAortica,SUM(MO.U_PEBazoMoteado)U_PEBazoMoteado, \
SUM(MO.U_PENoViable) U_PENoViable, \
SUM(MO.U_PEAerosaculitisG2) U_PEAerosaculitisG2, \
SUM(MO.U_PECojera) U_PECojera, \
SUM(MO.U_PEHigadoIcterico) U_PEHigadoIcterico, \
SUM(MO.U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra, \
SUM(MO.U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr, \
SUM(MO.U_PENecrosisHepatica) U_PENecrosisHepatica, \
SUM(MO.U_PENeumonia) U_PENeumonia, \
SUM(MO.U_PESepticemia) U_PESepticemia, \
SUM(MO.U_PEVomitoNegro) U_PEVomitoNegro, \
SUM(MO.U_PEAsperguillius) U_PEAsperguillius, \
SUM(MO.U_PEBazoGrandeMot) U_PEBazoGrandeMot, \
SUM(MO.U_PECorazonGrande) U_PECorazonGrande, \
SUM(MO.U_PECuadroToxico) U_PECuadroToxico, \
AVG(MO.AcumPEAccidentados) AcumPEAccidentados,AVG(MO.AcumPEHigadoGraso) AcumPEHigadoGraso,AVG(MO.AcumPEHepatomegalia) AcumPEHepatomegalia, \
AVG(MO.AcumPEHigadoHemorragico) AcumPEHigadoHemorragico,AVG(MO.AcumPEInanicion) AcumPEInanicion,AVG(MO.AcumPEProblemaRespiratorio) AcumPEProblemaRespiratorio, \
AVG(MO.AcumPESCH) AcumPESCH,AVG(MO.AcumPEEnteritis) AcumPEEnteritis,AVG(MO.AcumPEAscitis) AcumPEAscitis,AVG(MO.AcumPEMuerteSubita) AcumPEMuerteSubita, \
AVG(MO.AcumPEEstresPorCalor) AcumPEEstresPorCalor,AVG(MO.AcumPEHidropericardio) AcumPEHidropericardio,AVG(MO.AcumPEHemopericardio) AcumPEHemopericardio, \
AVG(MO.AcumPEUratosis) AcumPEUratosis,AVG(MO.AcumPEMaterialCaseoso) AcumPEMaterialCaseoso,AVG(MO.AcumPEOnfalitis) AcumPEOnfalitis, \
AVG(MO.AcumPERetencionDeYema) AcumPERetencionDeYema,AVG(MO.AcumPEErosionDeMolleja) AcumPEErosionDeMolleja,AVG(MO.AcumPEHemorragiaMusculos) AcumPEHemorragiaMusculos, \
AVG(MO.AcumPESangreEnCiego) AcumPESangreEnCiego,AVG(MO.AcumPEPericarditis) AcumPEPericarditis,AVG(MO.AcumPEPeritonitis)AcumPEPeritonitis, \
AVG(MO.AcumPEProlapso)AcumPEProlapso,AVG(MO.AcumPEPicaje)AcumPEPicaje,AVG(MO.AcumPERupturaAortica) AcumPERupturaAortica,AVG(MO.AcumPEBazoMoteado)AcumPEBazoMoteado, \
AVG(MO.AcumPENoViable) AcumPENoViable, \
AVG(MO.AcumPEAerosaculitisG2) AcumPEAerosaculitisG2, \
AVG(MO.AcumPECojera) AcumPECojera, \
AVG(MO.AcumPEHigadoIcterico) AcumPEHigadoIcterico, \
AVG(MO.AcumPEMaterialCaseoso_po1ra) AcumPEMaterialCaseoso_po1ra, \
AVG(MO.AcumPEMaterialCaseosoMedRetr) AcumPEMaterialCaseosoMedRetr, \
AVG(MO.AcumPENecrosisHepatica) AcumPENecrosisHepatica, \
AVG(MO.AcumPENeumonia) AcumPENeumonia, \
AVG(MO.AcumPESepticemia) AcumPESepticemia, \
AVG(MO.AcumPEVomitoNegro) AcumPEVomitoNegro, \
SUM(MO.AcumPEAsperguillius) AcumPEAsperguillius, \
SUM(MO.AcumPEBazoGrandeMot) AcumPEBazoGrandeMot, \
SUM(MO.AcumPECorazonGrande) AcumPECorazonGrande, \
SUM(MO.AcumPECuadroToxico) AcumPECuadroToxico,  \
AVG(MO.PrimeraLesion)PrimeraLesion,MO.PrimeraLesionNom,AVG(MO.SegundaLesion) SegundaLesion,MO.SegundaLesionNom, \
AVG(MO.TerceraLesion)TerceraLesion,MO.TerceraLesionNom, \
AVG(DM1.cmortalidadacum)PrimeraLesionAcum, DM1.causaacum PrimeraLesionAcumNom, \
AVG(DM2.cmortalidadacum)SegundaLesionAcum, DM2.causaacum AS SegundaLesionAcumNom,AVG(DM3.cmortalidadacum)TerceraLesionAcum, DM3.causaacum AS TerceraLesionAcumNom \
,nvl(AVG(STD.STDMortDia),0) as STDMortDia2, nvl(AVG(STD.STDMortAcum),0) as STDMortAcum2 \
,IncubadoraMayor,ListaIncubadora,ListaPadre,PadreMayor,RazaMayor,EdadPadreCorralDescrip  \
,MAX(EdadPadreCorral) EdadPadreCorral  \
,MAX(PorcAlojPadreMayor) PorcAlojPadreMayor  \
,MAX(PorcRazaMayor) PorcRazaMayor \
,MAX(PorcIncMayor) PorcIncMayor \
,categoria,FlagAtipico, MAX(MO.PavosBBMortIncub) PavosBBMortIncub, \
FlagTransfPavos,SourceComplexEntityNo,DestinationComplexEntityNo \
,MAX(MO.SemAccidentados) SemAccidentados  \
,MAX(MO.SemAscitis) SemAscitis \
,MAX(MO.SemBazoMoteado) SemBazoMoteado \
,MAX(MO.SemEnteritis) SemEnteritis \
,MAX(MO.SemErosionDeMolleja) SemErosionDeMolleja \
,MAX(MO.SemEstresPorCalor) SemEstresPorCalor \
,MAX(MO.SemHemopericardio) SemHemopericardio \
,MAX(MO.SemHemorragiaMusculos) SemHemorragiaMusculos \
,MAX(MO.SemHepatomegalia) SemHepatomegalia \
,MAX(MO.SemHidropericardio) SemHidropericardio \
,MAX(MO.SemHigadoGraso) SemHigadoGraso \
,MAX(MO.SemHigadoHemorragico) SemHigadoHemorragico \
,MAX(MO.SemInanicion) SemInanicion \
,MAX(MO.SemMaterialCaseoso) SemMaterialCaseoso \
,MAX(MO.SemMuerteSubita) SemMuerteSubita \
,MAX(MO.SemNoViable) SemNoViable \
,MAX(MO.SemOnfalitis) SemOnfalitis \
,MAX(MO.SemPericarditis) SemPericarditis \
,MAX(MO.SemPeritonitis) SemPeritonitis \
,MAX(MO.SemPicaje) SemPicaje \
,MAX(MO.SemProblemaRespiratorio) SemProblemaRespiratorio \
,MAX(MO.SemProlapso) SemProlapso \
,MAX(MO.SemRetencionDeYema) SemRetencionDeYema \
,MAX(MO.SemRupturaAortica) SemRupturaAortica \
,MAX(MO.SemSangreEnCiego) SemSangreEnCiego \
,MAX(MO.SemSCH) SemSCH \
,MAX(MO.SemUratosis) SemUratosis \
,MAX(MO.SemAerosaculitisG2) SemAerosaculitisG2 \
,MAX(MO.SemCojera) SemCojera \
,MAX(MO.SemHigadoIcterico) SemHigadoIcterico \
,MAX(MO.SemMaterialCaseoso_po1ra) SemMaterialCaseoso_po1ra \
,MAX(MO.SemMaterialCaseosoMedRetr) SemMaterialCaseosoMedRetr \
,MAX(MO.SemNecrosisHepatica) SemNecrosisHepatica \
,MAX(MO.SemNeumonia) SemNeumonia \
,MAX(MO.SemSepticemia) SemSepticemia \
,MAX(MO.SemVomitoNegro) SemVomitoNegro \
,MAX(MO.SemAsperguillius) SemAsperguillius \
,MAX(MO.SemBazoGrandeMot) SemBazoGrandeMot \
,MAX(MO.SemCorazonGrande) SemCorazonGrande \
,MAX(MO.SemCuadroToxico) SemCuadroToxico \
,AVG(MO.SemPrimeraLesion)SemPrimeraLesion,MO.SemPrimeraLesionNom \
,AVG(MO.SemSegundaLesion)SemSegundaLesion,MO.SemSegundaLesionNom \
,AVG(MO.SemTerceraLesion)SemTerceraLesion,MO.SemTerceraLesionNom \
,AVG(MO.SemOtrosLesion)SemOtrosLesion,MO.SemOtrosLesionNom \
,MAX(MO.U_RuidosTotales) U_RuidosTotales \
,MO.ListaFormulaNo \
,MO.ListaFormulaName \
,MO.TipoOrigen \
,MAX(MO.NoViableSem1) NoViableSem1 \
,MAX(MO.NoViableSem2) NoViableSem2 \
,MAX(MO.NoViableSem3) NoViableSem3 \
,MAX(MO.NoViableSem4) NoViableSem4 \
,MAX(MO.NoViableSem5) NoViableSem5 \
,MAX(MO.NoViableSem6) NoViableSem6 \
,MAX(MO.NoViableSem7) NoViableSem7 \
,MAX(MO.NoViableSem8) NoViableSem8 \
FROM {database_name_tmp}.MortalidadDetalle1 MO \
LEFT JOIN {database_name_tmp}.MortAcumMayor DM1 ON mo.ComplexEntityNo = DM1.ComplexEntityNo AND mo.pk_diasvida = DM1.pk_diasvida AND DM1.OrderLesionAcum = 1 \
LEFT JOIN {database_name_tmp}.MortAcumMayor DM2 ON mo.ComplexEntityNo = DM2.ComplexEntityNo AND mo.pk_diasvida = DM2.pk_diasvida AND DM2.OrderLesionAcum = 2 \
LEFT JOIN {database_name_tmp}.MortAcumMayor DM3 ON mo.ComplexEntityNo = DM3.ComplexEntityNo AND mo.pk_diasvida = DM3.pk_diasvida AND DM3.OrderLesionAcum = 3 \
LEFT JOIN ( \
SELECT pk_tiempo,pk_diasvida,ComplexEntityNo,STDMortDia,STDMortAcum FROM {database_name_tmp}.MortalidadDetalle1 MD \
WHERE pk_diasvida = ( \
SELECT MAX(pk_diasvida) FROM {database_name_tmp}.MortalidadDetalle1 MD1 \
WHERE md.ComplexEntityNo = MD1.ComplexEntityNo \
AND pk_diasvida >= 43 AND MortDia <>0 AND STDMortDia <> 0) \
GROUP BY pk_tiempo,pk_diasvida,ComplexEntityNo,STDMortDia,STDMortAcum \
) STD ON MO.ComplexEntityNo=STD.ComplexEntityNo and mo.pk_diasvida > std.pk_diasvida \
GROUP BY MO.pk_empresa,MO.pk_division,MO.pk_zona,MO.pk_subzona,MO.pk_plantel,MO.pk_lote,MO.pk_galpon,MO.pk_sexo,MO.pk_standard,MO.pk_producto, \
MO.pk_tipoproducto,MO.pk_especie,MO.pk_estado,MO.pk_administrador,MO.pk_proveedor,MO.pk_semanavida,MO.pk_diasvida,MO.ComplexEntityNo, \
MO.FechaNacimiento,MO.FechaCierre,MO.PrimeraLesionNom,MO.SegundaLesionNom,MO.TerceraLesionNom,DM1.causaacum,DM2.causaacum,DM3.causaacum \
,IncubadoraMayor,ListaIncubadora,ListaPadre,categoria,FlagAtipico,FlagTransfPavos,SourceComplexEntityNo,DestinationComplexEntityNo, \
MO.SemPrimeraLesionNom,MO.SemSegundaLesionNom,MO.SemTerceraLesionNom,MO.SemOtrosLesionNom,PadreMayor,RazaMayor,EdadPadreCorralDescrip \
,MO.ListaFormulaNo,MO.ListaFormulaName,MO.TipoOrigen")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Mortalidad"
}
df_MortalidadTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Mortalidad")

print("carga Mortalidad --> Registros procesados:", df_MortalidadTemp.count())
df_ConcatCorralTemp = spark.sql(f"select A.ComplexEntityNo,A.pk_tiempo,A.fecha,A.pk_semanavida,B.semanaIncub pk_semanaCalenIncub, \
CONCAT(substring(A.complexentityno,1,(length(A.complexentityno)-6)),' ',RTRIM(B.semanaIncub), '-' ,A.pk_semanavida) ConcatCorral, \
CONCAT(rtrim(B.semanaIncub),'-',B.anio,'-',A.pk_semanavida) ConcatSemAnioCorral \
from (select PD.ComplexEntityNo,MAX(PD.pk_tiempo) pk_tiempo,MAX(PD.fecha) fecha, PD.pk_empresa, PD.pk_division, MAX(pk_semanavida) pk_semanavida \
from {database_name_tmp}.Mortalidad PD \
where MortSem <> 0 and PD.pk_empresa = 1 and PD.pk_division = 4  \
group by PD.ComplexEntityNo, PD.pk_empresa, PD.pk_division) A \
left join {database_name_gl}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConcatCorral"
}
df_ConcatCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ConcatCorral")

print("carga ConcatCorral --> Registros procesados:", df_ConcatCorralTemp.count())
df_TotalGeneralCorralTemp = spark.sql(f"select SUM(X.PobInicial) PobInicial, SUM(X.PobInicialXMortSem) PobInicialXMortSem,SUM(X.PobInicialXMortSem)/SUM(X.PobInicial) MortSemAntCorral, X.ConcatCorral, X.pk_semanavida \
from \
(select A.pk_tiempo,A.ComplexEntityNo,A.PobInicial,A.MortSem,A.PobInicial * A.MortSem PobInicialXMortSem, ConcatCorral, ConcatSemAnioCorral,A.pk_semanavida \
from \
(select max(PD.pk_tiempo) pk_tiempo, PD.ComplexEntityNo,MAX(PD.PobInicial) PobInicial, MAX(PD.MortSem) MortSem, PD.pk_semanavida \
from {database_name_tmp}.Mortalidad PD \
where PD.pk_diasvida >= 8 and PD.pk_empresa = 1 and PD.pk_division = 4 \
group by PD.ComplexEntityNo, PD.pk_semanavida) A \
left join {database_name_tmp}.ConcatCorral B on A.ComplexEntityNo = B.ComplexEntityNo \
where A.MortSem <> 0 \
) X \
group by X.ConcatCorral, X.pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TotalGeneralCorral"
}
df_TotalGeneralCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TotalGeneralCorral")

print("carga TotalGeneralCorral --> Registros procesados:", df_TotalGeneralCorralTemp.count())
df_ConcatLoteTemp = spark.sql(f"select A.ComplexEntityNoLote ComplexEntityNo,A.pk_tiempo,A.fecha, A.pk_semanavida,B.semanaIncub pk_semanaCalenIncub, \
CONCAT(A.ComplexEntityNoLote,' ',RTRIM(B.semanaIncub), '-' ,A.pk_semanavida) ConcatLote, \
CONCAT(rtrim(B.semanaIncub),'-',B.anio,'-',A.pk_semanavida) ConcatSemAnioLote \
from (select substring(PD.complexentityno,1,(length(PD.complexentityno)-6)) ComplexEntityNoLote,MAX(PD.pk_tiempo) pk_tiempo,MAX(PD.fecha) fecha, PD.pk_empresa, PD.pk_division, \
MAX(pk_semanavida) pk_semanavida \
from {database_name_tmp}.Mortalidad PD \
where MortSem <> 0 \
group by substring(PD.complexentityno,1,(length(PD.complexentityno)-6)), PD.pk_empresa, PD.pk_division) A \
left join {database_name_gl}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo \
where A.pk_empresa = 1 and A.pk_division = 4")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConcatLote"
}
df_ConcatLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ConcatLote")

print("carga ConcatLote --> Registros procesados:", df_ConcatLoteTemp.count())
df_TotalGeneralLoteTemp = spark.sql(f"select SUM(cast(X.PobInicial as bigint)) PobInicial, SUM(cast(X.PobInicialXMortSem as bigint)) PobInicialXMortSem, \
SUM(cast(X.PobInicialXMortSem as numeric))/SUM(cast(X.PobInicial as numeric)) MortSemAntLote, \
X.ConcatSemAnioLote, X.pk_semanavida \
from \
(select PDR.pk_tiempo,substring(PDR.complexentityno,1,(length(PDR.complexentityno)-6)) complexentityno,PDR.complexentityno complexentityno2,PDR.PobInicial, \
PDR.MortSem,PDR.PobInicial * PDR.MortSem PobInicialXMortSem,PDR.pk_semanavida,ConcatLote, ConcatSemAnioLote \
from \
(select max(PD.pk_tiempo) pk_tiempo, PD.ComplexEntityNo,MAX(PD.PobInicial) PobInicial, MAX(PD.MortSem) MortSem, PD.pk_semanavida \
from {database_name_tmp}.Mortalidad PD \
where PD.pk_diasvida >= 8 and PD.MortSem <> 0 and PD.pk_empresa = 1 and PD.pk_division = 4 \
group by PD.ComplexEntityNo, PD.pk_semanavida \
)PDR \
left join {database_name_tmp}.ConcatLote B on substring(PDR.complexentityno,1,(length(PDR.complexentityno)-6)) = B.ComplexEntityNo \
) X \
group by X.ConcatSemAnioLote, X.pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TotalGeneralLote"
}
df_TotalGeneralLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TotalGeneralLote")

print("carga TotalGeneralLote --> Registros procesados:", df_TotalGeneralLoteTemp.count())
df_SumaSTDMortDiaTemp = spark.sql(f"SELECT ComplexEntityNo , pk_semanavida ,SUM(STDMortDia) STDMortDia, MAX(STDMortAcum) STDMortAcum FROM {database_name_tmp}.Mortalidad \
group by ComplexEntityNo , pk_semanavida ")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumaSTDMortDiaTemp"
}
df_SumaSTDMortDiaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.SumaSTDMortDiaTemp")

print("carga SumaSTDMortDiaTemp --> Registros procesados:", df_SumaSTDMortDiaTemp.count())
df_ft_mortalidad_Diario = spark.sql(f"SELECT \
 B.pk_tiempo \
,pk_empresa \
,pk_division \
,pk_zona \
,pk_subzona \
,pk_plantel \
,pk_lote \
,pk_galpon \
,pk_sexo \
,pk_standard \
,pk_producto \
,pk_tipoproducto \
,pk_especie \
,pk_estado \
,pk_administrador \
,pk_proveedor \
,B.pk_semanavida \
,pk_diasvida \
,B.ComplexEntityNo \
,FechaNacimiento \
,IncubadoraMayor \
,ListaIncubadora \
,ListaPadre \
,AVG(B.PobInicial) AS PobInicial \
,SUM(MortDia) AS MortDia \
,MAX(MortAcum) AS MortDiaAcum \
,MAX(MortSem) AS MortSem \
,MAX(MortSemAcum) AS MortSemAcum \
,ROUND(((AVG(B.STDMortDia)/100)*AVG(B.PobInicial)),0) AS STDMortDia \
,ROUND(((AVG(B.STDMortAcum)/100)*AVG(B.PobInicial)),0) AS STDMortDiaAcum \
,ROUND(((SUM(A.STDMortDia)/100)*AVG(B.PobInicial)),0)  AS STDMortSem \
,ROUND(((MAX(A.STDMortAcum)/100)*AVG(B.PobInicial)),0) AS STDMortSemAcum \
,ROUND((MAX(PorcMortDia*100)),3) AS PorcMortDia \
,ROUND((MAX(PorcMortDiaAcum*100)),3) AS PorcMortDiaAcum \
,ROUND((MAX(PorcMortSem*100)),3) AS PorcMortSem \
,ROUND((MAX(PorcMortSemAcum*100)),3) AS PorcMortSemAcum \
,ROUND((AVG(B.STDMortDia)),2) AS STDPorcMortDia \
,ROUND((AVG(B.STDMortAcum)),2) AS STDPorcMortDiaAcum \
,ROUND((SUM(A.STDMortDia)),2) STDPorcMortSem \
,ROUND((MAX(A.STDMortAcum)),2) STDPorcMortSemAcum \
,ROUND((ROUND((MAX(PorcMortDia*100)),2) - AVG(B.STDMortDia)),2) AS DifPorcMortDia_STDDia \
,ROUND((ROUND((MAX(PorcMortDiaAcum*100)),2) - AVG(B.STDMortAcum)),2) AS DifPorcMortDiaAcum_STDDiaAcum \
,ROUND((ROUND((MAX(PorcMortSem*100)),2) - SUM(A.STDMortDia) ),2) AS DifPorcMortSem_STDSem \
,ROUND((ROUND((MAX(PorcMortSemAcum*100)),2) - MAX(A.STDMortAcum) ),2) AS DifPorcMortSemAcum_STDSemAcum \
,AVG(MortSem1) AS MortSem1,AVG(MortSem2) AS MortSem2,AVG(MortSem3) AS MortSem3,AVG(MortSem4) AS MortSem4 \
,AVG(MortSem5) AS MortSem5,AVG(MortSem6) AS MortSem6,AVG(MortSem7) AS MortSem7 \
,AVG(MortSemAcum1) AS MortSemAcum1,AVG(MortSemAcum2) AS MortSemAcum2,AVG(MortSemAcum3) AS MortSemAcum3,AVG(MortSemAcum4) AS MortSemAcum4 \
,AVG(MortSemAcum5) AS MortSemAcum5,AVG(MortSemAcum6) AS MortSemAcum6,AVG(MortSemAcum7) AS MortSemAcum7 \
,CASE WHEN MAX(MortAcum) = 0 THEN 0.0 ELSE SUM(AcumPENoViable) / MAX(MortAcum*1.0) END AS TasaNoViable \
,SUM(U_PEAccidentados) AS U_PEAccidentados \
,SUM(U_PEHigadoGraso) AS U_PEHigadoGraso \
,SUM(U_PEHepatomegalia) AS U_PEHepatomegalia \
,SUM(U_PEHigadoHemorragico) AS U_PEHigadoHemorragico \
,SUM(U_PEInanicion) AS U_PEInanicion \
,SUM(U_PEProblemaRespiratorio) AS U_PEProblemaRespiratorio \
,SUM(U_PESCH) AS U_PESCH \
,SUM(U_PEEnteritis) AS U_PEEnteritis \
,SUM(U_PEAscitis) AS U_PEAscitis \
,SUM(U_PEMuerteSubita) AS U_PEMuerteSubita \
,SUM(U_PEEstresPorCalor) AS U_PEEstresPorCalor \
,SUM(U_PEHidropericardio) AS U_PEHidropericardio \
,SUM(U_PEHemopericardio) AS U_PEHemopericardio \
,SUM(U_PEUratosis) AS U_PEUratosis \
,SUM(U_PEMaterialCaseoso) AS U_PEMaterialCaseoso \
,SUM(U_PEOnfalitis) AS U_PEOnfalitis \
,SUM(U_PERetencionDeYema) AS U_PERetencionDeYema \
,SUM(U_PEErosionDeMolleja) AS U_PEErosionDeMolleja \
,SUM(U_PEHemorragiaMusculos) AS U_PEHemorragiaMusculos \
,SUM(U_PESangreEnCiego) AS U_PESangreEnCiego \
,SUM(U_PEPericarditis) AS U_PEPericarditis \
,SUM(U_PEPeritonitis) AS U_PEPeritonitis \
,SUM(U_PEProlapso) AS U_PEProlapso \
,SUM(U_PEPicaje) AS U_PEPicaje \
,SUM(U_PERupturaAortica) AS U_PERupturaAortica \
,SUM(U_PEBazoMoteado) AS U_PEBazoMoteado \
,SUM(U_PENoViable) AS U_PENoViable \
,'' AS U_PECaja,'' AS U_PEGota,'' AS U_PEIntoxicacion,'' AS U_PERetrazos,'' AS U_PEEliminados,'' AS U_PEAhogados,'' AS U_PEEColi,'' AS U_PEDescarte \
,'' AS U_PEOtros,'' AS U_PECoccidia,'' AS U_PEDeshidratados,'' AS U_PEHepatitis,'' AS U_PETraumatismo \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEAccidentados)/AVG(B.PobInicial*1.0))*100 END AS PorcAccidentados \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHigadoGraso)/AVG(B.PobInicial*1.0))*100 END AS PorcHigadoGraso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHepatomegalia)/AVG(B.PobInicial*1.0))*100 END AS PorcHepatomegalia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHigadoHemorragico)/AVG(B.PobInicial*1.0))*100 END AS PorcHigadoHemorragico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEInanicion)/AVG(B.PobInicial*1.0))*100 END AS PorcInanicion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEProblemaRespiratorio)/AVG(B.PobInicial*1.0))*100 END AS PorcProblemaRespiratorio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PESCH)/AVG(B.PobInicial*1.0))*100 END AS PorcSCH \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEEnteritis)/AVG(B.PobInicial*1.0))*100 END AS PorcEnteritis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEAscitis)/AVG(B.PobInicial*1.0))*100 END AS PorcAscitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEMuerteSubita)/AVG(B.PobInicial*1.0))*100 END AS PorcMuerteSubita \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEEstresPorCalor)/AVG(B.PobInicial*1.0))*100 END AS PorcEstresPorCalor \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHidropericardio)/AVG(B.PobInicial*1.0))*100 END AS PorcHidropericardio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHemopericardio)/AVG(B.PobInicial*1.0))*100 END AS PorcHemopericardio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEUratosis)/AVG(B.PobInicial*1.0))*100 END AS PorcUratosis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEMaterialCaseoso)/AVG(B.PobInicial*1.0))*100 END AS PorcMaterialCaseoso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEOnfalitis)/AVG(B.PobInicial*1.0))*100 END AS PorcOnfalitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PERetencionDeYema)/AVG(B.PobInicial*1.0))*100 END AS PorcRetencionDeYema \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEErosionDeMolleja)/AVG(B.PobInicial*1.0))*100 END AS PorcErosionDeMolleja \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHemorragiaMusculos)/AVG(B.PobInicial*1.0))*100 END AS PorcHemorragiaMusculos \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PESangreEnCiego)/AVG(B.PobInicial*1.0))*100 END AS PorcSangreEnCiego \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEPericarditis)/AVG(B.PobInicial*1.0))*100 END AS PorcPericarditis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEPeritonitis)/AVG(B.PobInicial*1.0))*100 END AS PorcPeritonitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEProlapso)/AVG(B.PobInicial*1.0))*100 END AS PorcProlapso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEPicaje)/AVG(B.PobInicial*1.0))*100 END AS PorcPicaje \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PERupturaAortica)/AVG(B.PobInicial*1.0))*100 END AS PorcRupturaAortica \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEBazoMoteado)/AVG(B.PobInicial*1.0))*100 END AS PorcBazoMoteado \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PENoViable)/AVG(B.PobInicial*1.0))*100 END AS PorcNoViable \
,'' AS PorcCaja,'' AS PorcGota,'' AS PorcIntoxicacion \
,'' AS PorcRetrazos,'' AS PorcEliminados,'' AS PorcAhogados,'' AS PorcEcoli,'' AS PorcDescarte,'' AS PorcOtros,'' AS PorcCoccidia,'' AS PorcDeshidratados \
,'' AS PorcHepatitis,'' AS PorcTraumatismo \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEAccidentados)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumAccidentados \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHigadoGraso)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHigadoGraso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHepatomegalia)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHepatomegalia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHigadoHemorragico)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHigadoHemorragico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEInanicion)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumInanicion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEProblemaRespiratorio)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumProblemaRespiratorio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPESCH)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumSCH \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEEnteritis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumEnteritis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEAscitis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumAscitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEMuerteSubita)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumMuerteSubita \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEEstresPorCalor)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumEstresPorCalor \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHidropericardio)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHidropericardio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHemopericardio)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHemopericardio \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEUratosis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumUratosis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEMaterialCaseoso)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumMaterialCaseoso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEOnfalitis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumOnfalitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPERetencionDeYema)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumRetencionDeYema \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEErosionDeMolleja)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumErosionDeMolleja \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHemorragiaMusculos)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHemorragiaMusculos \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPESangreEnCiego)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumSangreEnCiego \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEPericarditis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumPericarditis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEPeritonitis)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumPeritonitis \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEProlapso)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumProlapso \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEPicaje)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumPicaje \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPERupturaAortica)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumRupturaAortica \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEBazoMoteado)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumBazoMoteado \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPENoViable)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumNoViable \
,'' PorcAcumCaja \
,'' PorcAcumGota            \
,'' PorcAcumIntoxicacion    \
,'' PorcAcumRetrazos        \
,'' PorcAcumEliminados      \
,'' PorcAcumAhogados        \
,'' PorcAcumEColi           \
,'' PorcAcumDescarte        \
,'' PorcAcumOtros           \
,'' PorcAcumCoccidia        \
,'' PorcAcumDeshidratados   \
,'' PorcAcumHepatitis       \
,'' PorcAcumTraumatismo     \
,nvl(AVG(PrimeraLesion),0) AS PriLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(PrimeraLesion)/AVG(B.PobInicial*1.0))*100,3) END AS PorcPriLesion \
,nvl(PrimeraLesionNom,'-') AS PriLesionNom \
,nvl(AVG(SegundaLesion),0) AS SegLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SegundaLesion)/AVG(B.PobInicial*1.0))*100,3) END AS PorcSegLesion \
,nvl(SegundaLesionNom,'-') AS SegLesionNom \
,nvl(AVG(TerceraLesion),0) AS TerLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(TerceraLesion)/AVG(B.PobInicial*1.0))*100,3) END AS PorcTerLesion \
,nvl(TerceraLesionNom,'-') AS TerLesionNom \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(PrimeraLesionAcum)/AVG(B.PobInicial*1.0))*100,3) END AS PorcPriLesionAcum \
,nvl(PrimeraLesionAcumNom,'-') AS PriLesionAcumNom \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SegundaLesionAcum)/AVG(B.PobInicial*1.0))*100,3) END AS PorcSegLesionAcum \
,nvl(SegundaLesionAcumNom,'-') AS SegLesionAcumNom \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(TerceraLesionAcum)/AVG(B.PobInicial*1.0))*100,3) END AS PorcTerLesionAcum \
,nvl(TerceraLesionAcumNom,'-') AS TerLesionAcumNom \
,concat(CAST((CASE WHEN AVG(B.PobInicial) = 0 then 0 else CAST((ROUND((nvl((AVG(PrimeraLesion)/AVG(B.PobInicial*1.0))*100,0)),3)) AS DECIMAL(12,3)) END) AS VARCHAR(50)) ,'%', ' ' , nvl(PrimeraLesionNom,'-')) AS TopLesion \
,1 AS FlagArtificio  \
,categoria \
,FlagAtipico \
,SUM(U_PEAerosaculitisG2) AS U_PEAerosaculitisG2 \
,SUM(U_PECojera) AS U_PECojera \
,SUM(U_PEHigadoIcterico) AS U_PEHigadoIcterico \
,SUM(U_PEMaterialCaseoso_po1ra) AS U_PEMaterialCaseoso_po1ra \
,SUM(U_PEMaterialCaseosoMedRetr) AS U_PEMaterialCaseosoMedRetr \
,SUM(U_PENecrosisHepatica) AS U_PENecrosisHepatica \
,SUM(U_PENeumonia) AS U_PENeumonia \
,SUM(U_PESepticemia) AS U_PESepticemia \
,SUM(U_PEVomitoNegro) AS U_PEVomitoNegro \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEAerosaculitisG2)/AVG(B.PobInicial*1.0))*100 END AS PorcAerosaculitisG2 \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PECojera)/AVG(B.PobInicial*1.0))*100 END AS PorcCojera \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEHigadoIcterico)/AVG(B.PobInicial*1.0))*100 END AS PorcHigadoIcterico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEMaterialCaseoso_po1ra)/AVG(B.PobInicial*1.0))*100 END AS PorcMaterialCaseoso_po1ra \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEMaterialCaseosoMedRetr)/AVG(B.PobInicial*1.0))*100 END AS PorcMaterialCaseosoMedRetr \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PENecrosisHepatica)/AVG(B.PobInicial*1.0))*100 END AS PorcNecrosisHepatica \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PENeumonia)/AVG(B.PobInicial*1.0))*100 END AS PorcNeumonia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PESepticemia)/AVG(B.PobInicial*1.0))*100 END AS PorcSepticemia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEVomitoNegro)/AVG(B.PobInicial*1.0))*100 END AS PorcVomitoNegro \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEAerosaculitisG2)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumAerosaculitisG2 \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPECojera)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumCojera \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEHigadoIcterico)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumHigadoIcterico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEMaterialCaseoso_po1ra)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumMaterialCaseoso_po1ra \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEMaterialCaseosoMedRetr)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumMaterialCaseosoMedRetr \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPENecrosisHepatica)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumNecrosisHepatica \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPENeumonia)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumNeumonia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPESepticemia)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumSepticemia \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEVomitoNegro)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumVomitoNegro \
,SUM(U_PEAsperguillius) AS U_PEAsperguillius \
,SUM(U_PEBazoGrandeMot) AS U_PEBazoGrandeMot \
,SUM(U_PECorazonGrande) AS U_PECorazonGrande \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEAsperguillius)/AVG(B.PobInicial*1.0))*100 END AS PorcAsperguillius \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PEBazoGrandeMot)/AVG(B.PobInicial*1.0))*100 END AS PorcBazoGrandeMot \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PECorazonGrande)/AVG(B.PobInicial*1.0))*100 END AS PorcCorazonGrande \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEAsperguillius)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumAsperguillius \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPEBazoGrandeMot)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumBazoGrandeMot \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPECorazonGrande)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumCorazonGrande \
,AVG(MortSem8) AS MortSem8,AVG(MortSem9) AS MortSem9,AVG(MortSem10) AS MortSem10,AVG(MortSem11) AS MortSem11,AVG(MortSem12) AS MortSem12 \
,AVG(MortSem13) AS MortSem13,AVG(MortSem14) AS MortSem14,AVG(MortSem15) AS MortSem15,AVG(MortSem16) AS MortSem16,AVG(MortSem17) AS MortSem17 \
,AVG(MortSem18) AS MortSem18,AVG(MortSem19) AS MortSem19,AVG(MortSem20) AS MortSem20 \
,AVG(MortSemAcum8) AS MortSemAcum8,AVG(MortSemAcum9) AS MortSemAcum9,AVG(MortSemAcum10) AS MortSemAcum10,AVG(MortSemAcum11) AS MortSemAcum11 \
,AVG(MortSemAcum12) AS MortSemAcum12,AVG(MortSemAcum13) AS MortSemAcum13,AVG(MortSemAcum14) AS MortSemAcum14,AVG(MortSemAcum15) AS MortSemAcum15 \
,AVG(MortSemAcum16) AS MortSemAcum16,AVG(MortSemAcum17) AS MortSemAcum17,AVG(MortSemAcum18) AS MortSemAcum18,AVG(MortSemAcum19) AS MortSemAcum19 \
,AVG(MortSemAcum20) AS MortSemAcum20 \
,SUM(U_PECuadroToxico) AS U_PECuadroToxico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(U_PECuadroToxico)/AVG(B.PobInicial*1.0))*100 END AS PorcCuadroToxico \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE (SUM(AcumPECuadroToxico)/AVG(B.PobInicial*1.0))*100 END AS PorcAcumCuadroToxico \
,MAX(PavosBBMortIncub) AS PavosBBMortIncub \
,FlagTransfPavos \
,SourceComplexEntityNo \
,DestinationComplexEntityNo \
,MAX(B.SemAccidentados) SemAccidentados \
,MAX(B.SemAscitis) SemAscitis \
,MAX(B.SemBazoMoteado) SemBazoMoteado \
,MAX(B.SemEnteritis) SemEnteritis \
,MAX(B.SemErosionDeMolleja) SemErosionDeMolleja \
,MAX(B.SemEstresPorCalor) SemEstresPorCalor \
,MAX(B.SemHemopericardio) SemHemopericardio \
,MAX(B.SemHemorragiaMusculos) SemHemorragiaMusculos \
,MAX(B.SemHepatomegalia) SemHepatomegalia \
,MAX(B.SemHidropericardio) SemHidropericardio \
,MAX(B.SemHigadoGraso) SemHigadoGraso \
,MAX(B.SemHigadoHemorragico) SemHigadoHemorragico \
,MAX(B.SemInanicion) SemInanicion \
,MAX(B.SemMaterialCaseoso) SemMaterialCaseoso \
,MAX(B.SemMuerteSubita) SemMuerteSubita \
,MAX(B.SemNoViable) SemNoViable \
,MAX(B.SemOnfalitis) SemOnfalitis \
,MAX(B.SemPericarditis) SemPericarditis \
,MAX(B.SemPeritonitis) SemPeritonitis \
,MAX(B.SemPicaje) SemPicaje \
,MAX(B.SemProblemaRespiratorio) SemProblemaRespiratorio \
,MAX(B.SemProlapso) SemProlapso \
,MAX(B.SemRetencionDeYema) SemRetencionDeYema \
,MAX(B.SemRupturaAortica) SemRupturaAortica \
,MAX(B.SemSangreEnCiego) SemSangreEnCiego \
,MAX(B.SemSCH) SemSCH \
,MAX(B.SemUratosis) SemUratosis \
,MAX(B.SemAerosaculitisG2) SemAerosaculitisG2 \
,MAX(B.SemCojera) SemCojera \
,MAX(B.SemHigadoIcterico) SemHigadoIcterico \
,MAX(B.SemMaterialCaseoso_po1ra) SemMaterialCaseoso_po1ra \
,MAX(B.SemMaterialCaseosoMedRetr) SemMaterialCaseosoMedRetr \
,MAX(B.SemNecrosisHepatica) SemNecrosisHepatica \
,MAX(B.SemNeumonia) SemNeumonia \
,MAX(B.SemSepticemia) SemSepticemia \
,MAX(B.SemVomitoNegro) SemVomitoNegro \
,MAX(B.SemAsperguillius) SemAsperguillius \
,MAX(B.SemBazoGrandeMot) SemBazoGrandeMot \
,MAX(B.SemCorazonGrande) SemCorazonGrande \
,MAX(B.SemCuadroToxico) SemCuadroToxico \
,ROUND((MAX(B.SemAccidentados)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcAccidentados \
,ROUND((MAX(B.SemAscitis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcAscitis \
,ROUND((MAX(B.SemBazoMoteado)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcBazoMoteado \
,ROUND((MAX(B.SemEnteritis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcEnteritis \
,ROUND((MAX(B.SemErosionDeMolleja)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcErosionDeMolleja \
,ROUND((MAX(B.SemEstresPorCalor)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcEstresPorCalor \
,ROUND((MAX(B.SemHemopericardio)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHemopericardio \
,ROUND((MAX(B.SemHemorragiaMusculos)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHemorragiaMusculos \
,ROUND((MAX(B.SemHepatomegalia)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHepatomegalia \
,ROUND((MAX(B.SemHidropericardio)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHidropericardio \
,ROUND((MAX(B.SemHigadoGraso)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHigadoGraso \
,ROUND((MAX(B.SemHigadoHemorragico)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHigadoHemorragico \
,ROUND((MAX(B.SemInanicion)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcInanicion \
,ROUND((MAX(B.SemMaterialCaseoso)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcMaterialCaseoso \
,ROUND((MAX(B.SemMuerteSubita)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcMuerteSubita \
,ROUND((MAX(B.SemNoViable)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcNoViable \
,ROUND((MAX(B.SemOnfalitis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcOnfalitis \
,ROUND((MAX(B.SemPericarditis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcPericarditis \
,ROUND((MAX(B.SemPeritonitis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcPeritonitis \
,ROUND((MAX(B.SemPicaje)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcPicaje \
,ROUND((MAX(B.SemProblemaRespiratorio)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcProblemaRespiratorio \
,ROUND((MAX(B.SemProlapso)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcProlapso \
,ROUND((MAX(B.SemRetencionDeYema)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcRetencionDeYema \
,ROUND((MAX(B.SemRupturaAortica)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcRupturaAortica \
,ROUND((MAX(B.SemSangreEnCiego)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcSangreEnCiego \
,ROUND((MAX(B.SemSCH)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcSCH \
,ROUND((MAX(B.SemUratosis)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcUratosis \
,ROUND((MAX(B.SemAerosaculitisG2)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcAerosaculitisG2 \
,ROUND((MAX(B.SemCojera)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcCojera \
,ROUND((MAX(B.SemHigadoIcterico)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcHigadoIcterico \
,ROUND((MAX(B.SemMaterialCaseoso_po1ra)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcMaterialCaseoso_po1ra \
,ROUND((MAX(B.SemMaterialCaseosoMedRetr)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcMaterialCaseosoMedRetr \
,ROUND((MAX(B.SemNecrosisHepatica)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcNecrosisHepatica \
,ROUND((MAX(B.SemNeumonia)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcNeumonia \
,ROUND((MAX(B.SemSepticemia)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcSepticemia \
,ROUND((MAX(B.SemVomitoNegro)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcVomitoNegro \
,ROUND((MAX(B.SemAsperguillius)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcAsperguillius \
,ROUND((MAX(B.SemBazoGrandeMot)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcBazoGrandeMot \
,ROUND((MAX(B.SemCorazonGrande)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcCorazonGrande \
,ROUND((MAX(B.SemCuadroToxico)/NULLIF(AVG(B.PobInicial*1.0),0))*100,3) SemPorcCuadroToxico \
,nvl(AVG(SemPrimeraLesion),0) AS SemPriLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SemPrimeraLesion)/AVG(B.PobInicial*1.0))*100,3) END AS SemPorcPriLesion \
,nvl(SemPrimeraLesionNom,'-') AS SemPriLesionNom \
,nvl(AVG(SemSegundaLesion),0) AS SemSegLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SemSegundaLesion)/AVG(B.PobInicial*1.0))*100,3) END AS SemPorcSegLesion \
,nvl(SemSegundaLesionNom,'-') AS SemSegLesionNom \
,nvl(AVG(SemTerceraLesion),0) AS SemTerLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SemTerceraLesion)/AVG(B.PobInicial*1.0))*100,3) END AS SemPorcTerLesion \
,nvl(SemTerceraLesionNom,'-') AS SemTerLesionNom \
,nvl(AVG(SemOtrosLesion),0) AS SemOtrosLesion \
,CASE WHEN AVG(B.PobInicial) = 0 THEN 0 ELSE ROUND((AVG(SemOtrosLesion)/AVG(B.PobInicial*1.0))*100,3) END AS SemPorcOtrosLesion \
,nvl(SemOtrosLesionNom,'-') AS SemOtrosLesionNom \
,PadreMayor \
,MAX(U_RuidosTotales) RuidosRespiratorios \
,MAX(PorcIncMayor) PorcIncMayor \
,MAX(PorcAlojPadreMayor) PorcAlojPadreMayor \
,RazaMayor \
,MAX(PorcRazaMayor) PorcRazaMayor \
,MAX(EdadPadreCorral) EdadPadreCorral \
,EdadPadreCorralDescrip \
,'' ProductoConsumo				  \
,'' CantMortSemLesionUno          \
,'' PorcMortSemLesionUno          \
,'' DescripMortSemLesionUno       \
,'' CantMortSemLesionDos          \
,'' PorcMortSemLesionDos          \
,'' DescripMortSemLesionDos       \
,'' CantMortSemLesionTres         \
,'' PorcMortSemLesionTres         \
,'' DescripMortSemLesionTres      \
,'' CantMortSemLesionOtros        \
,'' PorcMortSemLesionOtros        \
,'' DescripMortSemLesionOtros     \
,'' CantMortSemLesionAcumUno      \
,'' PorcMortSemLesionAcumUno      \
,'' DescripMortSemLesionAcumUno   \
,'' CantMortSemLesionAcumDos      \
,'' PorcMortSemLesionAcumDos      \
,'' DescripMortSemLesionAcumDos   \
,'' CantMortSemLesionAcumTres     \
,'' PorcMortSemLesionAcumTres     \
,'' DescripMortSemLesionAcumTres  \
,'' CantMortSemLesionAcumOtros    \
,'' PorcMortSemLesionAcumOtros    \
,'' DescripMortSemLesionAcumOtros \
,'' SaldoAves                     \
,ListaFormulaNo \
,ListaFormulaName \
,TipoOrigen \
,TGC.ConcatCorral mortconcatcorral\
,MAX(TGC.MortSemAntCorral) MortSemAntCorral \
,CL.ConcatLote MortConcatLote\
,MAX(TGL.MortSemAntLote) MortSemAntLote \
,CL.ConcatSemAnioLote MortConcatSemAnioLote \
,CC.ConcatSemAnioCorral MortConcatSemAnioCorral \
,'' SemCaja \
,'' SemGota              \
,'' SemIntoxicacion      \
,'' SemRetrazos          \
,'' SemEliminados        \
,'' SemAhogados          \
,'' SemEColi             \
,'' SemOtros             \
,'' SemCoccidia          \
,'' SemDeshidratados     \
,'' SemHepatitis         \
,'' SemTraumatismo       \
,'' SemPorcCaja          \
,'' SemPorcGota          \
,'' SemPorcIntoxicacion  \
,'' SemPorcRetrazos      \
,'' SemPorcEliminados    \
,'' SemPorcAhogados      \
,'' SemPorcEColi         \
,'' SemPorcOtros         \
,'' SemPorcCoccidia      \
,'' SemPorcDeshidratados \
,'' SemPorcHepatitis     \
,'' SemPorcTraumatismo   \
,MAX(B.NoViableSem1) NoViableSem1,MAX(B.NoViableSem2) NoViableSem2,MAX(B.NoViableSem3) NoViableSem3,MAX(B.NoViableSem4) NoViableSem4,MAX(B.NoViableSem5) NoViableSem5 \
,MAX(B.NoViableSem6) NoViableSem6,MAX(B.NoViableSem7) NoViableSem7 \
,ROUND((SUM(B.NoViableSem1)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem1 \
,ROUND((SUM(B.NoViableSem2)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem2 \
,ROUND((SUM(B.NoViableSem3)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem3 \
,ROUND((SUM(B.NoViableSem4)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem4 \
,ROUND((SUM(B.NoViableSem5)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem5 \
,ROUND((SUM(B.NoViableSem6)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem6 \
,ROUND((SUM(B.NoViableSem7)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem7 \
,MAX(B.NoViableSem8) NoViableSem8 \
,ROUND((SUM(B.NoViableSem8)/NULLIF(AVG(B.PobInicial*1.0),0))*100,4) PorcNoViableSem8 \
,'' U_PENoEspecificado \
,'' U_PEAnalisis_Laboratorio    \
,'' PorcNoEspecificado          \
,'' PorcAnalisisLaboratorio     \
,'' PorcAcumNoEspecificado      \
,'' PorcAcumAnalisisLaboratorio \
,'' SemNoEspecificado           \
,'' SemAnalisisLaboratorio      \
,'' SemPorcNoEspecificado       \
,'' SemPorcAnalisisLaboratorio  \
,b.fecha DescripFecha                \
,'' DescripEmpresa              \
,'' DescripDivision             \
,'' DescripZona                 \
,'' DescripSubzona              \
,'' Plantel                     \
,'' Lote                        \
,'' Galpon                      \
,'' DescripSexo                 \
,'' DescripStandard             \
,'' DescripProducto             \
,'' DescripTipoProducto         \
,'' DescripEspecie              \
,'' DescripEstado               \
,'' DescripAdministrador        \
,'' DescripProveedor            \
,'' DescripDiaVida              \
,'' DescripSemanaVida           \
FROM {database_name_tmp}.Mortalidad B \
left join {database_name_tmp}.ConcatCorral CC on b.ComplexEntityNo = CC.ComplexEntityNo and b.fecha <= CC.fecha \
left join {database_name_tmp}.TotalGeneralCorral TGC on CC.ConcatCorral = TGC.ConcatCorral and b.pk_semanavida = TGC.pk_semanavida \
left join {database_name_tmp}.ConcatLote CL on substring(b.complexentityno,1,(length(b.complexentityno)-6)) = CL.ComplexEntityNo and b.fecha <= CL.fecha \
left join {database_name_tmp}.TotalGeneralLote TGL on CL.ConcatSemAnioLote = TGL.ConcatSemAnioLote and b.pk_semanavida = TGL.pk_semanavida \
left join {database_name_tmp}.SumaSTDMortDiaTemp A ON A.ComplexEntityNo = B.ComplexEntityNo AND A.pk_semanavida = B.pk_semanavida \
WHERE (date_format(B.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')) \
GROUP BY B.pk_tiempo,B.fecha,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_tipoproducto \
,pk_especie,pk_estado,pk_administrador,pk_proveedor,B.pk_semanavida,pk_diasvida,B.ComplexEntityNo,FechaNacimiento,PrimeraLesionNom, \
 SegundaLesionNom,TerceraLesionNom,PrimeraLesionAcumNom,SegundaLesionAcumNom,TerceraLesionAcumNom \
,IncubadoraMayor,ListaIncubadora,ListaPadre,categoria,FlagAtipico,FlagTransfPavos,SourceComplexEntityNo \
,DestinationComplexEntityNo,SemPrimeraLesionNom,SemSegundaLesionNom,SemTerceraLesionNom,SemOtrosLesionNom,PadreMayor,RazaMayor,EdadPadreCorralDescrip \
,ListaFormulaNo,ListaFormulaName,TipoOrigen,TGC.ConcatCorral,CL.ConcatLote,CL.ConcatSemAnioLote,CC.ConcatSemAnioCorral \
order by pk_tiempo")

print("carga ft_mortalidad_Diario --> Registros procesados:", df_ft_mortalidad_Diario.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes251 = spark.read.format("parquet").load(path_target25)
    datos_existentes251 = True
    logger.info(f"Datos existentes de ft_mortalidad_Diario cargados: {df_existentes251.count()} registros")
except:
    datos_existentes251 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Diario")

if datos_existentes251:
    existing_data251 = spark.read.format("parquet").load(path_target25)
    data_after_delete251 = existing_data251.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data251 = df_ft_mortalidad_Diario.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data251 = filtered_new_data251.union(data_after_delete251)                             
   
    cant_ingresonuevo251 = filtered_new_data251.count()
    cant_total251 = final_data251.count()

    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_DiarioTemporal"
    }
    final_data251.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_DiarioTemporal")
    
    final_data251_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_DiarioTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_diario"
    }
    final_data251_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_diario")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Diario : {cant_ingresonuevo251}")
    print(f"Total de registros en la tabla ft_mortalidad_Diario : {cant_total251}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_DiarioTemporal')
    print(f"Tabla ft_mortalidad_DiarioTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_diario"
    }
    df_ft_mortalidad_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_diario")
df_maxfechalotetemp =spark.sql(f"select pk_lote,max(descripfecha) fecha from {database_name_gl}.ft_mortalidad_diario  group by pk_lote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxfechaloteTemp"
}
df_maxfechalotetemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MaxfechaloteTemp")

print("carga MaxfechaloteTemp --> Registros procesados:", df_maxfechalotetemp.count())
df_artificioTemp = spark.sql(f"select ComplexEntityNo, \
max(a.pk_tiempo) pk_tiempo, \
max(a.descripfecha) fecha, \
max(c.pk_tiempo) as pk_fechalote, \
max(XA.fecha) fechalote \
from {database_name_gl}.ft_mortalidad_Diario A \
left join {database_name_tmp}.MaxfechaloteTemp XA ON XA.pk_lote = A.pk_lote \
left join {database_name_gl}.lk_tiempo c on c.fecha = XA.fecha \
where (date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
and pk_empresa = 1 \
and SUBSTRING(ComplexEntityNo,1,1) <> 'V' \
group by ComplexEntityNo, a.pk_lote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/artificio"
}
df_artificioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.artificio")

print("carga artificio --> Registros procesados:", df_artificioTemp.count())
df_ft_mortalidad_diario_ins = spark.sql(f"""select 
 b.pk_tiempo
,pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_lote
,pk_galpon
,pk_sexo
,pk_standard
,pk_producto
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,pk_semanavida
,pk_diasvida
,C.ComplexEntityNo
,FechaNacimiento
,IncubadoraMayor
,ListaIncubadora
,ListaPadre
,PobInicial
,MortDia
,MortDiaAcum
,MortSem
,MortSemAcum
,STDMortDia
,STDMortDiaAcum
,STDMortSem
,STDMortSemAcum
,PorcMortDia
,PorcMortDiaAcum
,PorcMortSem
,PorcMortSemAcum
,STDPorcMortDia
,STDPorcMortDiaAcum
,STDPorcMortSem
,STDPorcMortSemAcum
,DifPorcMortDia_STDDia
,DifPorcMortDiaAcum_STDDiaAcum
,DifPorcMortSem_STDSem
,DifPorcMortSemAcum_STDSemAcum
,MortSem1
,MortSem2
,MortSem3
,MortSem4
,MortSem5
,MortSem6
,MortSem7
,MortSemAcum1
,MortSemAcum2
,MortSemAcum3
,MortSemAcum4
,MortSemAcum5
,MortSemAcum6
,MortSemAcum7
,TasaNoViable
,U_PEAccidentados
,U_PEHigadoGraso
,U_PEHepatomegalia
,U_PEHigadoHemorragico
,U_PEInanicion
,U_PEProblemaRespiratorio
,U_PESCH
,U_PEEnteritis
,U_PEAscitis
,U_PEMuerteSubita
,U_PEEstresPorCalor
,U_PEHidropericardio
,U_PEHemopericardio
,U_PEUratosis
,U_PEMaterialCaseoso
,U_PEOnfalitis
,U_PERetencionDeYema
,U_PEErosionDeMolleja
,U_PEHemorragiaMusculos
,U_PESangreEnCiego
,U_PEPericarditis
,U_PEPeritonitis
,U_PEProlapso
,U_PEPicaje
,U_PERupturaAortica
,U_PEBazoMoteado
,U_PENoViable
,U_PECaja
,U_PEGota
,U_PEIntoxicacion
,U_PERetrazos
,U_PEEliminados
,U_PEAhogados
,U_PEEColi
,U_PEDescarte
,U_PEOtros
,U_PECoccidia
,U_PEDeshidratados
,U_PEHepatitis
,U_PETraumatismo
,PorcAccidentados
,PorcHigadoGraso
,PorcHepatomegalia
,PorcHigadoHemorragico
,PorcInanicion
,PorcProblemaRespiratorio
,PorcSCH
,PorcEnteritis
,PorcAscitis
,PorcMuerteSubita
,PorcEstresPorCalor
,PorcHidropericardio
,PorcHemopericardio
,PorcUratosis
,PorcMaterialCaseoso
,PorcOnfalitis
,PorcRetencionDeYema
,PorcErosionDeMolleja
,PorcHemorragiaMusculos
,PorcSangreEnCiego
,PorcPericarditis
,PorcPeritonitis
,PorcProlapso
,PorcPicaje
,PorcRupturaAortica
,PorcBazoMoteado
,PorcNoViable
,PorcCaja
,PorcGota
,PorcIntoxicacion
,PorcRetrazos
,PorcEliminados
,PorcAhogados
,PorcEColi
,PorcDescarte
,PorcOtros
,PorcCoccidia
,PorcDeshidratados
,PorcHepatitis
,PorcTraumatismo
,PorcAcumAccidentados
,PorcAcumHigadoGraso
,PorcAcumHepatomegalia
,PorcAcumHigadoHemorragico
,PorcAcumInanicion
,PorcAcumProblemaRespiratorio
,PorcAcumSCH
,PorcAcumEnteritis
,PorcAcumAscitis
,PorcAcumMuerteSubita
,PorcAcumEstresPorCalor
,PorcAcumHidropericardio
,PorcAcumHemopericardio
,PorcAcumUratosis
,PorcAcumMaterialCaseoso
,PorcAcumOnfalitis
,PorcAcumRetencionDeYema
,PorcAcumErosionDeMolleja
,PorcAcumHemorragiaMusculos
,PorcAcumSangreEnCiego
,PorcAcumPericarditis
,PorcAcumPeritonitis
,PorcAcumProlapso
,PorcAcumPicaje
,PorcAcumRupturaAortica
,PorcAcumBazoMoteado
,PorcAcumNoViable
,PorcAcumCaja
,PorcAcumGota
,PorcAcumIntoxicacion
,PorcAcumRetrazos
,PorcAcumEliminados
,PorcAcumAhogados
,PorcAcumEColi
,PorcAcumDescarte
,PorcAcumOtros
,PorcAcumCoccidia
,PorcAcumDeshidratados
,PorcAcumHepatitis
,PorcAcumTraumatismo
,PriLesion
,PorcPriLesion
,PriLesionNom
,SegLesion
,PorcSegLesion
,SegLesionNom
,TerLesion
,PorcTerLesion
,TerLesionNom
,PorcPriLesionAcum
,prilesionacumnom
,PorcSegLesionAcum
,seglesionacumnom
,PorcTerLesionAcum
,terlesionacumnom
,TopLesion
,2 as flagartificio
,categoria
,FlagAtipico
,U_PEAerosaculitisG2
,U_PECojera
,U_PEHigadoIcterico
,U_PEMaterialCaseoso_po1ra
,U_PEMaterialCaseosoMedRetr
,U_PENecrosisHepatica
,U_PENeumonia
,U_PESepticemia
,U_PEVomitoNegro
,PorcAerosaculitisG2
,PorcCojera
,PorcHigadoIcterico
,PorcMaterialCaseoso_po1ra
,PorcMaterialCaseosoMedRetr
,PorcNecrosisHepatica
,PorcNeumonia
,PorcSepticemia
,PorcVomitoNegro
,PorcAcumAerosaculitisG2
,PorcAcumCojera
,PorcAcumHigadoIcterico
,PorcAcumMaterialCaseoso_po1ra
,PorcAcumMaterialCaseosoMedRetr
,PorcAcumNecrosisHepatica
,PorcAcumNeumonia
,PorcAcumSepticemia
,PorcAcumVomitoNegro
,U_PEAsperguillius
,U_PEBazoGrandeMot
,U_PECorazonGrande
,PorcAsperguillius
,PorcBazoGrandeMot
,PorcCorazonGrande
,PorcAcumAsperguillius
,PorcAcumBazoGrandeMot
,PorcAcumCorazonGrande
,MortSem8
,MortSem9
,MortSem10
,MortSem11
,MortSem12
,MortSem13
,MortSem14
,MortSem15
,MortSem16
,MortSem17
,MortSem18
,MortSem19
,MortSem20
,MortSemAcum8
,MortSemAcum9
,MortSemAcum10
,MortSemAcum11
,MortSemAcum12
,MortSemAcum13
,MortSemAcum14
,MortSemAcum15
,MortSemAcum16
,MortSemAcum17
,MortSemAcum18
,MortSemAcum19
,MortSemAcum20
,U_PECuadroToxico
,PorcCuadroToxico
,PorcAcumCuadroToxico
,PavosBBMortIncub
,FlagTransfPavos
,SourceComplexEntityNo
,DestinationComplexEntityNo
,SemAccidentados
,SemAscitis
,SemBazoMoteado
,SemEnteritis
,SemErosionDeMolleja
,SemEstresPorCalor
,SemHemopericardio
,SemHemorragiaMusculos
,SemHepatomegalia
,SemHidropericardio
,SemHigadoGraso
,SemHigadoHemorragico
,SemInanicion
,SemMaterialCaseoso
,SemMuerteSubita
,SemNoViable
,SemOnfalitis
,SemPericarditis
,SemPeritonitis
,SemPicaje
,SemProblemaRespiratorio
,SemProlapso
,SemRetencionDeYema
,SemRupturaAortica
,SemSangreEnCiego
,SemSCH
,SemUratosis
,SemAerosaculitisG2
,SemCojera
,SemHigadoIcterico
,SemMaterialCaseoso_po1ra
,SemMaterialCaseosoMedRetr
,SemNecrosisHepatica
,SemNeumonia
,SemSepticemia
,SemVomitoNegro
,SemAsperguillius
,SemBazoGrandeMot
,SemCorazonGrande
,SemCuadroToxico
,SemPorcAccidentados
,SemPorcAscitis
,SemPorcBazoMoteado
,SemPorcEnteritis
,SemPorcErosionDeMolleja
,SemPorcEstresPorCalor
,SemPorcHemopericardio
,SemPorcHemorragiaMusculos
,SemPorcHepatomegalia
,SemPorcHidropericardio
,SemPorcHigadoGraso
,SemPorcHigadoHemorragico
,SemPorcInanicion
,SemPorcMaterialCaseoso
,SemPorcMuerteSubita
,SemPorcNoViable
,SemPorcOnfalitis
,SemPorcPericarditis
,SemPorcPeritonitis
,SemPorcPicaje
,SemPorcProblemaRespiratorio
,SemPorcProlapso
,SemPorcRetencionDeYema
,SemPorcRupturaAortica
,SemPorcSangreEnCiego
,SemPorcSCH
,SemPorcUratosis
,SemPorcAerosaculitisG2
,SemPorcCojera
,SemPorcHigadoIcterico
,SemPorcMaterialCaseoso_po1ra
,SemPorcMaterialCaseosoMedRetr
,SemPorcNecrosisHepatica
,SemPorcNeumonia
,SemPorcSepticemia
,SemPorcVomitoNegro
,SemPorcAsperguillius
,SemPorcBazoGrandeMot
,SemPorcCorazonGrande
,SemPorcCuadroToxico
,SemPriLesion
,SemPorcPriLesion
,SemPriLesionNom
,SemSegLesion
,SemPorcSegLesion
,SemSegLesionNom
,SemTerLesion
,SemPorcTerLesion
,SemTerLesionNom
,SemOtrosLesion
,SemPorcOtrosLesion
,SemOtrosLesionNom
,PadreMayor
,RuidosRespiratorios
,PorcIncMayor
,PorcAlojPadreMayor
,RazaMayor
,PorcRazaMayor
,EdadPadreCorral
,EdadPadreCorralDescrip
,ProductoConsumo
,CantMortSemLesionUno
,PorcMortSemLesionUno
,DescripMortSemLesionUno
,CantMortSemLesionDos
,PorcMortSemLesionDos
,DescripMortSemLesionDos
,CantMortSemLesionTres
,PorcMortSemLesionTres
,DescripMortSemLesionTres
,CantMortSemLesionOtros
,PorcMortSemLesionOtros
,DescripMortSemLesionOtros
,CantMortSemLesionAcumUno
,PorcMortSemLesionAcumUno
,DescripMortSemLesionAcumUno
,CantMortSemLesionAcumDos
,PorcMortSemLesionAcumDos
,DescripMortSemLesionAcumDos
,CantMortSemLesionAcumTres
,PorcMortSemLesionAcumTres
,DescripMortSemLesionAcumTres
,CantMortSemLesionAcumOtros
,PorcMortSemLesionAcumOtros
,DescripMortSemLesionAcumOtros
,SaldoAves
,ListaFormulaNo
,ListaFormulaName
,TipoOrigen
,MortConcatCorral
,MortSemAntCorral
,MortConcatLote
,MortSemAntLote
,MortConcatSemAnioLote
,MortConcatSemAnioCorral
,SemCaja
,SemGota
,SemIntoxicacion
,SemRetrazos
,SemEliminados
,SemAhogados
,SemEColi
,SemOtros
,SemCoccidia
,SemDeshidratados
,SemHepatitis
,SemTraumatismo
,SemPorcCaja
,SemPorcGota
,SemPorcIntoxicacion
,SemPorcRetrazos
,SemPorcEliminados
,SemPorcAhogados
,SemPorcEColi
,SemPorcOtros
,SemPorcCoccidia
,SemPorcDeshidratados
,SemPorcHepatitis
,SemPorcTraumatismo
,NoViableSem1
,NoViableSem2
,NoViableSem3
,NoViableSem4
,NoViableSem5
,NoViableSem6
,NoViableSem7
,PorcNoViableSem1
,PorcNoViableSem2
,PorcNoViableSem3
,PorcNoViableSem4
,PorcNoViableSem5
,PorcNoViableSem6
,PorcNoViableSem7
,NoViableSem8
,PorcNoViableSem8
,U_PENoEspecificado
,U_PEAnalisis_Laboratorio
,PorcNoEspecificado
,PorcAnalisisLaboratorio
,PorcAcumNoEspecificado
,PorcAcumAnalisisLaboratorio
,SemNoEspecificado
,SemAnalisisLaboratorio
,SemPorcNoEspecificado
,SemPorcAnalisisLaboratorio
,b.fecha DescripFecha
,DescripEmpresa
,DescripDivision
,DescripZona
,DescripSubzona
,Plantel
,Lote
,Galpon
,DescripSexo
,DescripStandard
,DescripProducto
,DescripTipoProducto
,DescripEspecie
,DescripEstado
,DescripAdministrador
,DescripProveedor
,DescripDiaVida
,DescripSemanaVida
from {database_name_tmp}.artificio A 
left join {database_name_gl}.ft_mortalidad_Diario C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_tiempo = C.pk_tiempo 
cross join {database_name_gl}.lk_tiempo B 
where b.fecha > A.fecha and b.fecha <= A.fechalote 
and (date_format(c.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
and pk_empresa = 1 and SUBSTRING(c.ComplexEntityNo,1,1) <> 'V'
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidadI"
}
df_ft_mortalidad_diario_ins.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidadI")
print('carga df_ft_mortalidad_diario_ins', df_ft_mortalidad_diario_ins.count())
df_ft_mortalidad_diario_upd = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidadI
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_mortalidad_diario_upd', df_ft_mortalidad_diario_upd.count())
#Se muestra ft_mortalidad_Diario
try:
    df = spark.table(f"{database_name_gl}.ft_mortalidad_Diario")
    df_ft_mortalidad_Diario_new = df_ft_mortalidad_diario_upd.union(df)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_ft_mortalidad_Diario_new = df_ft_mortalidad_diario_upd
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_mortalidad_Diario_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidad_diario_new")

df_ft_mortalidad_diario_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidad_diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_gl}.ft_mortalidad_diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_diario"
}
df_ft_mortalidad_diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_gl}.ft_mortalidad_diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.ft_mortalidad_diario_new")

print("carga INS ft_mortalidad_diario --> Registros procesados:", df_ft_mortalidad_diario_nueva.count())
df_MortalidadCorralTemp = spark.sql(f"select \
max(M.pk_tiempo) as pk_tiempo,max(M.fecha) as fecha,M.pk_empresa,M.pk_division,M.pk_zona,M.pk_subzona,M.pk_plantel \
,M.pk_lote,M.pk_galpon,M.pk_sexo,M.pk_standard,M.pk_producto,M.pk_tipoproducto,M.pk_especie,M.pk_estado,M.pk_administrador \
,M.pk_proveedor,max(M.pk_diasvida) as pk_diasvida,M.ComplexEntityNo,M.FechaNacimiento \
,max(M.FechaCierre) as FechaCierre \
,avg(M.PobInicial) as PobInicial \
,max(M.MortAcum) as MortDia \
,case when avg(M.PobInicial) = 0 then 0.0 else (((max(M.MortAcum) / avg(M.PobInicial*1.0))*100)) end as PorcMortDia \
,avg(M.MortSem1) as MortSem1,avg(M.MortSem2) as MortSem2,avg(M.MortSem3) as MortSem3,avg(M.MortSem4) as MortSem4,avg(M.MortSem5) as MortSem5 \
,avg(M.MortSem6) as MortSem6,avg(M.MortSem7) as MortSem7,avg(M.MortSemAcum1) as MortSemAcum1 \
,avg(M.MortSemAcum2) as MortSemAcum2 ,avg(M.MortSemAcum3) as MortSemAcum3,avg(M.MortSemAcum4) as MortSemAcum4 ,avg(M.MortSemAcum5) as MortSemAcum5 \
,avg(M.MortSemAcum6) as MortSemAcum6 ,avg(M.MortSemAcum7) as MortSemAcum7 \
,MAX(M.AcumPEAccidentados) U_PEAccidentados,MAX(M.AcumPEHigadoGraso) U_PEHigadoGraso,MAX(M.AcumPEHepatomegalia) U_PEHepatomegalia, \
 MAX(M.AcumPEHigadoHemorragico) U_PEHigadoHemorragico,MAX(M.AcumPEInanicion) U_PEInanicion,MAX(M.AcumPEProblemaRespiratorio) U_PEProblemaRespiratorio, \
 MAX(M.AcumPESCH) U_PESCH,MAX(M.AcumPEEnteritis) U_PEEnteritis,MAX(M.AcumPEAscitis) U_PEAscitis,MAX(M.AcumPEMuerteSubita) U_PEMuerteSubita, \
 MAX(M.AcumPEEstresPorCalor) U_PEEstresPorCalor,MAX(M.AcumPEHidropericardio) U_PEHidropericardio,MAX(M.AcumPEHemopericardio) U_PEHemopericardio, \
 MAX(M.AcumPEUratosis) U_PEUratosis,MAX(M.AcumPEMaterialCaseoso) U_PEMaterialCaseoso,MAX(M.AcumPEOnfalitis) U_PEOnfalitis, \
 MAX(M.AcumPERetencionDeYema) U_PERetencionDeYema,MAX(M.AcumPEErosionDeMolleja) U_PEErosionDeMolleja,MAX(M.AcumPEHemorragiaMusculos) U_PEHemorragiaMusculos, \
 MAX(M.AcumPESangreEnCiego) U_PESangreEnCiego,MAX(M.AcumPEPericarditis) U_PEPericarditis,MAX(M.AcumPEPeritonitis)U_PEPeritonitis, \
 MAX(M.AcumPEProlapso)U_PEProlapso,MAX(M.AcumPEPicaje)U_PEPicaje,MAX(M.AcumPERupturaAortica) U_PERupturaAortica,MAX(M.AcumPEBazoMoteado)U_PEBazoMoteado, \
 MAX(M.AcumPENoViable) U_PENoViable \
,M.IncubadoraMayor \
,M.ListaIncubadora \
,M.ListaPadre \
,M.categoria \
,max(M.FlagAtipico) FlagAtipico \
,MAX(M.AcumPEAerosaculitisG2) U_PEAerosaculitisG2 \
,MAX(M.AcumPECojera) U_PECojera \
,MAX(M.AcumPEHigadoIcterico) U_PEHigadoIcterico \
,MAX(M.AcumPEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra \
,MAX(M.AcumPEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,MAX(M.AcumPENecrosisHepatica) U_PENecrosisHepatica \
,MAX(M.AcumPENeumonia) U_PENeumonia \
,MAX(M.AcumPESepticemia) U_PESepticemia \
,MAX(M.AcumPEVomitoNegro) U_PEVomitoNegro \
,MAX(M.AcumPEAsperguillius) U_PEAsperguillius \
,MAX(M.AcumPEBazoGrandeMot) U_PEBazoGrandeMot \
,MAX(M.AcumPECorazonGrande) U_PECorazonGrande \
,avg(M.MortSem8) as MortSem8,avg(M.MortSem9) as MortSem9,avg(M.MortSem10) as MortSem10,avg(M.MortSem11) as MortSem11,avg(M.MortSem12) as MortSem12 \
,avg(M.MortSem13) as MortSem13,avg(M.MortSem14) as MortSem14,avg(M.MortSem15) as MortSem15,avg(M.MortSem16) as MortSem16,avg(M.MortSem17) as MortSem17 \
,avg(M.MortSem18) as MortSem18,avg(M.MortSem19) as MortSem19,avg(M.MortSem20) as MortSem20 \
,avg(M.MortSemAcum8) as MortSemAcum8 ,avg(M.MortSemAcum9) as MortSemAcum9 ,avg(M.MortSemAcum10) as MortSemAcum10 ,avg(M.MortSemAcum11) as MortSemAcum11 \
,avg(M.MortSemAcum12) as MortSemAcum12 ,avg(M.MortSemAcum13) as MortSemAcum13 ,avg(M.MortSemAcum14) as MortSemAcum14 ,avg(M.MortSemAcum15) as MortSemAcum15 \
,avg(M.MortSemAcum16) as MortSemAcum16 ,avg(M.MortSemAcum17) as MortSemAcum17 ,avg(M.MortSemAcum18) as MortSemAcum18 ,avg(M.MortSemAcum19) as MortSemAcum19 \
,avg(M.MortSemAcum20) as MortSemAcum20 \
,MAX(M.AcumPECuadroToxico) U_PECuadroToxico \
,max(M.PavosBBMortIncub) as PavosBBMortIncub \
,M.FlagTransfPavos \
,M.SourceComplexEntityNo \
,M.DestinationComplexEntityNo \
,substring(M.SourceComplexEntityNo,1,(length(M.SourceComplexEntityNo)-3)) SourceComplexEntityNoGalpon \
,substring(M.DestinationComplexEntityNo,1,(length(M.DestinationComplexEntityNo)-3)) DestinationComplexEntityNoGalpon \
,substring(M.SourceComplexEntityNo,1,(length(M.SourceComplexEntityNo)-6)) SourceComplexEntityNoLote \
,substring(M.DestinationComplexEntityNo,1,(length(M.DestinationComplexEntityNo)-6)) DestinationComplexEntityNoLote \
,sum(M.U_RuidosTotales) U_RuidosTotales \
,count(RR.U_RuidosTotales) CountRuidosTotales \
,M.PadreMayor \
,MAX(M.PorcIncMayor) PorcIncMayor \
,MAX(M.PorcAlojPadreMayor) PorcAlojPadreMayor \
,M.RazaMayor \
,MAX(M.PorcRazaMayor) PorcRazaMayor \
,MAX(M.EdadPadreCorral) EdadPadreCorral \
,M.EdadPadreCorralDescrip \
,M.TipoOrigen \
from {database_name_tmp}.Mortalidad M \
LEFT JOIN {database_name_tmp}.Mortalidad RR where RR.ComplexEntityNo = M.ComplexEntityNo and RR.U_RuidosTotales<>0 \
group by M.pk_empresa,M.pk_division,M.pk_zona,M.pk_subzona,M.pk_plantel,M.pk_lote,M.pk_galpon,M.pk_sexo,M.pk_standard,M.pk_producto,M.pk_tipoproducto,M.pk_especie,M.pk_estado \
,M.pk_administrador,M.pk_proveedor,M.ComplexEntityNo,M.FechaNacimiento,M.IncubadoraMayor,M.ListaIncubadora,M.ListaPadre,M.categoria,M.FlagTransfPavos \
,M.SourceComplexEntityNo \
,M.DestinationComplexEntityNo,M.PadreMayor,M.RazaMayor,M.EdadPadreCorralDescrip,M.TipoOrigen")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadCorral"
}
df_MortalidadCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadCorral")
print('carga MortalidadCorral', df_MortalidadCorralTemp.count())
df_MortalidadCorralSinAtipicoTemp =spark.sql(f"select \
max(M.pk_tiempo) as pk_tiempo,max(M.fecha) as fecha,M.pk_empresa,M.pk_division,M.pk_zona,M.pk_subzona,M.pk_plantel,M.pk_lote,M.pk_galpon \
,M.pk_sexo ,M.pk_standard,M.pk_producto,M.pk_tipoproducto,M.pk_especie,M.pk_estado,M.pk_administrador,M.pk_proveedor,max(M.pk_diasvida) as pk_diasvida \
,M.ComplexEntityNo,M.FechaNacimiento,max(M.FechaCierre) as FechaCierre,avg(M.PobInicial) as PobInicial,max(M.MortAcum) as MortDia \
,case when avg(M.PobInicial) = 0 then 0.0 else (((max(M.MortAcum) / avg(M.PobInicial*1.0))*100)) end as PorcMortDia \
,avg(M.MortSem1) as MortSem1,avg(M.MortSem2) as MortSem2,avg(M.MortSem3) as MortSem3,avg(M.MortSem4) as MortSem4,avg(M.MortSem5) as MortSem5 \
,avg(M.MortSem6) as MortSem6,avg(M.MortSem7) as MortSem7 \
,avg(M.MortSemAcum1) as MortSemAcum1 ,avg(M.MortSemAcum2) as MortSemAcum2 ,avg(M.MortSemAcum3) as MortSemAcum3,avg(M.MortSemAcum4) as MortSemAcum4 \
,avg(M.MortSemAcum5) as MortSemAcum5 ,avg(M.MortSemAcum6) as MortSemAcum6 ,avg(M.MortSemAcum7) as MortSemAcum7 \
,MAX(M.AcumPEAccidentados) U_PEAccidentados,MAX(M.AcumPEHigadoGraso) U_PEHigadoGraso,MAX(M.AcumPEHepatomegalia) U_PEHepatomegalia, \
 MAX(M.AcumPEHigadoHemorragico) U_PEHigadoHemorragico,MAX(M.AcumPEInanicion) U_PEInanicion,MAX(M.AcumPEProblemaRespiratorio) U_PEProblemaRespiratorio, \
 MAX(M.AcumPESCH) U_PESCH,MAX(M.AcumPEEnteritis) U_PEEnteritis,MAX(M.AcumPEAscitis) U_PEAscitis,MAX(M.AcumPEMuerteSubita) U_PEMuerteSubita, \
 MAX(M.AcumPEEstresPorCalor) U_PEEstresPorCalor,MAX(M.AcumPEHidropericardio) U_PEHidropericardio,MAX(M.AcumPEHemopericardio) U_PEHemopericardio, \
 MAX(M.AcumPEUratosis) U_PEUratosis,MAX(M.AcumPEMaterialCaseoso) U_PEMaterialCaseoso,MAX(M.AcumPEOnfalitis) U_PEOnfalitis, \
 MAX(M.AcumPERetencionDeYema) U_PERetencionDeYema,MAX(M.AcumPEErosionDeMolleja) U_PEErosionDeMolleja,MAX(M.AcumPEHemorragiaMusculos) U_PEHemorragiaMusculos, \
 MAX(M.AcumPESangreEnCiego) U_PESangreEnCiego,MAX(M.AcumPEPericarditis) U_PEPericarditis,MAX(M.AcumPEPeritonitis)U_PEPeritonitis, \
 MAX(M.AcumPEProlapso)U_PEProlapso,MAX(M.AcumPEPicaje)U_PEPicaje,MAX(M.AcumPERupturaAortica) U_PERupturaAortica,MAX(M.AcumPEBazoMoteado)U_PEBazoMoteado, \
 MAX(M.AcumPENoViable) U_PENoViable \
,M.IncubadoraMayor \
,M.ListaIncubadora \
,M.ListaPadre \
,M.categoria \
,1 FlagAtipico \
,MAX(M.AcumPEAerosaculitisG2) U_PEAerosaculitisG2 \
,MAX(M.AcumPECojera) U_PECojera \
,MAX(M.AcumPEHigadoIcterico) U_PEHigadoIcterico \
,MAX(M.AcumPEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra \
,MAX(M.AcumPEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,MAX(M.AcumPENecrosisHepatica) U_PENecrosisHepatica \
,MAX(M.AcumPENeumonia) U_PENeumonia \
,MAX(M.AcumPESepticemia) U_PESepticemia \
,MAX(M.AcumPEVomitoNegro) U_PEVomitoNegro \
,MAX(M.AcumPEAsperguillius) U_PEAsperguillius \
,MAX(M.AcumPEBazoGrandeMot) U_PEBazoGrandeMot \
,MAX(M.AcumPECorazonGrande) U_PECorazonGrande \
,avg(M.MortSem8) as MortSem8,avg(M.MortSem9) as MortSem9,avg(M.MortSem10) as MortSem10,avg(M.MortSem11) as MortSem11,avg(M.MortSem12) as MortSem12 \
,avg(M.MortSem13) as MortSem13,avg(M.MortSem14) as MortSem14,avg(M.MortSem15) as MortSem15,avg(M.MortSem16) as MortSem16,avg(M.MortSem17) as MortSem17 \
,avg(M.MortSem18) as MortSem18,avg(M.MortSem19) as MortSem19,avg(M.MortSem20) as MortSem20 \
,avg(M.MortSemAcum8) as MortSemAcum8 ,avg(M.MortSemAcum9) as MortSemAcum9 ,avg(M.MortSemAcum10) as MortSemAcum10 ,avg(M.MortSemAcum11) as MortSemAcum11 \
,avg(M.MortSemAcum12) as MortSemAcum12 ,avg(M.MortSemAcum13) as MortSemAcum13 ,avg(M.MortSemAcum14) as MortSemAcum14 ,avg(M.MortSemAcum15) as MortSemAcum15 \
,avg(M.MortSemAcum16) as MortSemAcum16 ,avg(M.MortSemAcum17) as MortSemAcum17 ,avg(M.MortSemAcum18) as MortSemAcum18 ,avg(M.MortSemAcum19) as MortSemAcum19 \
,avg(M.MortSemAcum20) as MortSemAcum20 ,MAX(M.AcumPECuadroToxico) U_PECuadroToxico \
,max(M.PavosBBMortIncub) as PavosBBMortIncub ,M.FlagTransfPavos ,M.SourceComplexEntityNo ,M.DestinationComplexEntityNo \
,substring(M.SourceComplexEntityNo,1,(length(M.SourceComplexEntityNo)-3)) SourceComplexEntityNoGalpon \
,substring(M.DestinationComplexEntityNo,1,(length(M.DestinationComplexEntityNo)-3)) DestinationComplexEntityNoGalpon \
,substring(M.SourceComplexEntityNo,1,(length(M.SourceComplexEntityNo)-6)) SourceComplexEntityNoLote \
,substring(M.DestinationComplexEntityNo,1,(length(M.DestinationComplexEntityNo)-6)) DestinationComplexEntityNoLote \
,sum(M.U_RuidosTotales) U_RuidosTotales \
,count(RR.U_RuidosTotales)  CountRuidosTotales \
,M.PadreMayor \
,MAX(M.PorcIncMayor) PorcIncMayor \
,MAX(M.PorcAlojPadreMayor) PorcAlojPadreMayor \
,M.RazaMayor \
,MAX(M.PorcRazaMayor) PorcRazaMayor \
,MAX(M.EdadPadreCorral) EdadPadreCorral \
,M.EdadPadreCorralDescrip \
,M.TipoOrigen \
from {database_name_tmp}.Mortalidad M \
left join {database_name_tmp}.Mortalidad RR ON RR.ComplexEntityNo = M.ComplexEntityNo and RR.U_RuidosTotales <> 0 \
where M.FlagAtipico = 1 \
group by M.pk_empresa,M.pk_division,M.pk_zona,M.pk_subzona,M.pk_plantel,M.pk_lote,M.pk_galpon,M.pk_sexo,M.pk_standard,M.pk_producto,M.pk_tipoproducto,M.pk_especie,M.pk_estado \
,M.pk_administrador,M.pk_proveedor,M.ComplexEntityNo,M.FechaNacimiento,M.IncubadoraMayor,M.ListaIncubadora,M.ListaPadre,M.categoria,M.FlagTransfPavos,M.SourceComplexEntityNo \
,M.DestinationComplexEntityNo,M.PadreMayor,M.RazaMayor,M.EdadPadreCorralDescrip,M.TipoOrigen")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadCorralSinAtipico"
}
df_MortalidadCorralSinAtipicoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadCorralSinAtipico")
print('carga MortalidadCorralSinAtipico', df_MortalidadCorralSinAtipicoTemp.count())
df_MortalidadCorral_upt = spark.sql(f"select * from {database_name_tmp}.MortalidadCorralSinAtipico \
except \
select * from {database_name_tmp}.MortalidadCorral")
print( 'df_MortalidadCorral_upt' , df_MortalidadCorral_upt.count() )
try:
    df = spark.table(f"{database_name_tmp}.MortalidadCorral")
    df_MortalidadCorral_new = df_MortalidadCorral_upt.union(df)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_MortalidadCorral_new = df_MortalidadCorral_upt
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadCorral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_MortalidadCorral_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.MortalidadCorral_new")

df_MortalidadCorral_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.MortalidadCorral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadCorral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadCorral"
}
df_MortalidadCorral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadCorral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadCorral_new")

print("carga INS MortalidadCorral --> Registros procesados:", df_MortalidadCorral_nueva.count())
df_ft_mortalidad_CorralF = spark.sql(f"SELECT \
b.pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto \
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,ComplexEntityNo,FechaNacimiento,IncubadoraMayor \
,ListaIncubadora,ListaPadre,PobInicial,MortDia,round((PorcMortDia*100),2) as PorcMortDia,MortSem1,MortSem2,MortSem3,MortSem4 \
,MortSem5,MortSem6,MortSem7,MortSemAcum1,MortSemAcum2,MortSemAcum3,MortSemAcum4,MortSemAcum5,MortSemAcum6,MortSemAcum7 \
,case when MortDia = 0 then 0.0 else U_PENoViable / (MortDia*1.0) end as TasaNoViable \
,U_PEAccidentados,U_PEHigadoGraso,U_PEHepatomegalia,U_PEHigadoHemorragico,U_PEInanicion,U_PEProblemaRespiratorio \
,U_PESCH,U_PEEnteritis,U_PEAscitis,U_PEMuerteSubita,U_PEEstresPorCalor,U_PEHidropericardio,U_PEHemopericardio \
,U_PEUratosis,U_PEMaterialCaseoso,U_PEOnfalitis,U_PERetencionDeYema,U_PEErosionDeMolleja,U_PEHemorragiaMusculos \
,U_PESangreEnCiego,U_PEPericarditis,U_PEPeritonitis,U_PEProlapso,U_PEPicaje,U_PERupturaAortica,U_PEBazoMoteado,U_PENoViable \
,'' U_PECaja \
,'' U_PEGota            \
,'' U_PEIntoxicacion    \
,'' U_PERetrazos        \
,'' U_PEEliminados      \
,'' U_PEAhogados        \
,'' U_PEEColi           \
,'' U_PEDescarte        \
,'' U_PEOtros           \
,'' U_PECoccidia        \
,'' U_PEDeshidratados   \
,'' U_PEHepatitis       \
,'' U_PETraumatismo     \
,case when PobInicial = 0 then 0 else round(((U_PEAccidentados)/(PobInicial*1.0))*100,3) end as PorcAccidentados \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoGraso)/(PobInicial*1.0))*100,3) end as PorcHigadoGraso \
,case when PobInicial = 0 then 0 else round(((U_PEHepatomegalia)/(PobInicial*1.0))*100,3) end as PorcHepatomegalia \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoHemorragico)/(PobInicial*1.0))*100,3) end as PorcHigadoHemorragico \
,case when PobInicial = 0 then 0 else round(((U_PEInanicion)/(PobInicial*1.0))*100,3) end as PorcInanicion \
,case when PobInicial = 0 then 0 else round(((U_PEProblemaRespiratorio)/(PobInicial*1.0))*100,3) end as PorcProblemaRespiratorio \
,case when PobInicial = 0 then 0 else round(((U_PESCH)/(PobInicial*1.0))*100,3) end as PorcSCH \
,case when PobInicial = 0 then 0 else round(((U_PEEnteritis)/(PobInicial*1.0))*100,3) end as PorcEnteritis \
,case when PobInicial = 0 then 0 else round(((U_PEAscitis)/(PobInicial*1.0))*100,3) end as PorcAscitis \
,case when PobInicial = 0 then 0 else round(((U_PEMuerteSubita)/(PobInicial*1.0))*100,3) end as PorcMuerteSubita \
,case when PobInicial = 0 then 0 else round(((U_PEEstresPorCalor)/(PobInicial*1.0))*100,3) end as PorcEstresPorCalor \
,case when PobInicial = 0 then 0 else round(((U_PEHidropericardio)/(PobInicial*1.0))*100,3) end as PorcHidropericardio \
,case when PobInicial = 0 then 0 else round(((U_PEHemopericardio)/(PobInicial*1.0))*100,3) end as PorcHemopericardio \
,case when PobInicial = 0 then 0 else round(((U_PEUratosis)/(PobInicial*1.0))*100,3) end as PorcUratosis \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso \
,case when PobInicial = 0 then 0 else round(((U_PEOnfalitis)/(PobInicial*1.0))*100,3) end as PorcOnfalitis \
,case when PobInicial = 0 then 0 else round(((U_PERetencionDeYema)/(PobInicial*1.0))*100,3) end as PorcRetencionDeYema \
,case when PobInicial = 0 then 0 else round(((U_PEErosionDeMolleja)/(PobInicial*1.0))*100,3) end as PorcErosionDeMolleja \
,case when PobInicial = 0 then 0 else round(((U_PEHemorragiaMusculos)/(PobInicial*1.0))*100,3) end as PorcHemorragiaMusculos \
,case when PobInicial = 0 then 0 else round(((U_PESangreEnCiego)/(PobInicial*1.0))*100,3) end as PorcSangreEnCiego \
,case when PobInicial = 0 then 0 else round(((U_PEPericarditis)/(PobInicial*1.0))*100,3) end as PorcPericarditis \
,case when PobInicial = 0 then 0 else round(((U_PEPeritonitis)/(PobInicial*1.0))*100,3) end as PorcPeritonitis \
,case when PobInicial = 0 then 0 else round(((U_PEProlapso)/(PobInicial*1.0))*100,3) end as PorcProlapso \
,case when PobInicial = 0 then 0 else round(((U_PEPicaje)/(PobInicial*1.0))*100,3) end as PorcPicaje \
,case when PobInicial = 0 then 0 else round(((U_PERupturaAortica)/(PobInicial*1.0))*100,3) end as PorcRupturaAortica \
,case when PobInicial = 0 then 0 else round(((U_PEBazoMoteado)/(PobInicial*1.0))*100,3) end as PorcBazoMoteado \
,case when PobInicial = 0 then 0 else round(((U_PENoViable)/(PobInicial*1.0))*100,3) end as PorcNoViable \
,'' PorcCaja \
,'' PorcGota            \
,'' PorcIntoxicacion    \
,'' PorcRetrazos        \
,'' PorcEliminados      \
,'' PorcAhogados        \
,'' PorcEColi           \
,'' PorcDescarte        \
,'' PorcOtros           \
,'' PorcCoccidia        \
,'' PorcDeshidratados   \
,'' PorcHepatitis       \
,'' PorcTraumatismo     \
,date_format(a.fecha,'yyyyMMdd') as EventDate,categoria,FlagAtipico,U_PEAerosaculitisG2,U_PECojera,U_PEHigadoIcterico,U_PEMaterialCaseoso_po1ra \
,U_PEMaterialCaseosoMedRetr,U_PENecrosisHepatica,U_PENeumonia,U_PESepticemia,U_PEVomitoNegro \
,case when PobInicial = 0 then 0 else round(((U_PEAerosaculitisG2)/(PobInicial*1.0))*100,3) end as PorcAerosaculitisG2 \
,case when PobInicial = 0 then 0 else round(((U_PECojera)/(PobInicial*1.0))*100,3) end as PorcCojera \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoIcterico)/(PobInicial*1.0))*100,3) end as PorcHigadoIcterico \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso_po1ra)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso_po1ra \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseosoMedRetr)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseosoMedRetr \
,case when PobInicial = 0 then 0 else round(((U_PENecrosisHepatica)/(PobInicial*1.0))*100,3) end as PorcNecrosisHepatica \
,case when PobInicial = 0 then 0 else round(((U_PENeumonia)/(PobInicial*1.0))*100,3) end as PorcNeumonia \
,case when PobInicial = 0 then 0 else round(((U_PESepticemia)/(PobInicial*1.0))*100,3) end as PorcSepticemia \
,case when PobInicial = 0 then 0 else round(((U_PEVomitoNegro)/(PobInicial*1.0))*100,3) end as PorcVomitoNegro \
,U_PEAsperguillius,U_PEBazoGrandeMot,U_PECorazonGrande \
,case when PobInicial = 0 then 0 else round(((U_PEAsperguillius)/(PobInicial*1.0))*100,3) end as PorcAsperguillius \
,case when PobInicial = 0 then 0 else round(((U_PEBazoGrandeMot)/(PobInicial*1.0))*100,3) end as PorcBazoGrandeMot \
,case when PobInicial = 0 then 0 else round(((U_PECorazonGrande)/(PobInicial*1.0))*100,3) end as PorcCorazonGrande \
,MortSem8,MortSem9,MortSem10,MortSem11,MortSem12,MortSem13,MortSem14,MortSem15,MortSem16,MortSem17,MortSem18,MortSem19 \
,MortSem20,MortSemAcum8,MortSemAcum9,MortSemAcum10,MortSemAcum11,MortSemAcum12,MortSemAcum13,MortSemAcum14,MortSemAcum15 \
,MortSemAcum16,MortSemAcum17,MortSemAcum18,MortSemAcum19,MortSemAcum20 \
,U_PECuadroToxico \
,case when PobInicial = 0 then 0 else round(((U_PECuadroToxico)/(PobInicial*1.0))*100,3) end as PorcCuadroToxico \
,PavosBBMortIncub \
,FlagTransfPavos \
,SourceComplexEntityNo \
,DestinationComplexEntityNo \
,U_RuidosTotales/nullif(CountRuidosTotales*1.0,0) RuidosRespiratorios \
,PadreMayor \
,PorcIncMayor \
,PorcAlojPadreMayor \
,RazaMayor \
,PorcRazaMayor \
,EdadPadreCorral \
,EdadPadreCorralDescrip \
,TipoOrigen \
,'' U_PENoEspecificado \
,'' U_PEAnalisis_Laboratorio    \
,'' PorcNoEspecificado          \
,'' PorcAnalisisLaboratorio     \
,b.fecha DescripFecha                \
,'' DescripEmpresa              \
,'' DescripDivision             \
,'' DescripZona                 \
,'' DescripSubzona              \
,'' Plantel                     \
,'' Lote                        \
,'' Galpon                      \
,'' DescripSexo                 \
,'' DescripStandard             \
,'' DescripProducto             \
,'' DescripTipoProducto         \
,'' DescripEspecie              \
,'' DescripEstado               \
,'' DescripAdministrador        \
,'' DescripProveedor            \
,'' DescripDiaVida              \
FROM {database_name_tmp}.MortalidadCorral A left join {database_name_gl}.lk_tiempo b on b.fecha=cast(concat(substring(a.fechacierre,1,4),'-',substring(a.fechacierre,5,2),'-',substring(a.fechacierre,7,2)) as date) \
")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_CorralF"
}
df_ft_mortalidad_CorralF.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidad_CorralF")
print('carga df_ft_mortalidad_CorralF', df_ft_mortalidad_CorralF.count())
df_ft_mortalidad_Corral = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidad_CorralF
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_mortalidad_Corral', df_ft_mortalidad_Corral.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes26 = spark.read.format("parquet").load(path_target26)
    datos_existentes26 = True
    logger.info(f"Datos existentes de ft_mortalidad_Corral cargados: {df_existentes26.count()} registros")
except:
    datos_existentes26 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Corral")

if datos_existentes26:
    existing_data26 = spark.read.format("parquet").load(path_target26)
    data_after_delete26 = existing_data26.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data26 = df_ft_mortalidad_Corral.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data26 = filtered_new_data26.union(data_after_delete26)                             
   
    cant_ingresonuevo26 = filtered_new_data26.count()
    cant_total26 = final_data26.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_CorralTemporal"
    }
    final_data26.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_CorralTemporal")
    
    final_data26_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_CorralTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_corral"
    }
    final_data26_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_corral")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Corral : {cant_ingresonuevo26}")
    print(f"Total de registros en la tabla ft_mortalidad_Corral : {cant_total26}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_CorralTemporal')
    print(f"Tabla ft_mortalidad_CorralTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_corral"
    }
    df_ft_mortalidad_Corral.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_corral")
print('carga ft_mortalidad_Corral',df_ft_mortalidad_Corral.count())
df_MortalidadGalponTemp =spark.sql(f"select \
max(pk_tiempo) as pk_tiempo,max(fecha) as fecha,a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,a.pk_galpon \
,pk_tipoproducto,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0') as pk_especie,pk_estado,pk_administrador \
,pk_proveedor,max(pk_diasvida) as pk_diasvida \
,CONCAT(Upper(clote), '-' , nogalpon) as ComplexEntityNoGalpon \
,max(FechaCierre) as FechaCierre \
,sum(PobInicial) as PobInicial \
,sum(MortDia) as MortDia \
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4 \
,sum(MortSem5) as MortSem5,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7 \
,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 ,sum(MortSemAcum3) as MortSemAcum3,sum(MortSemAcum4) as MortSemAcum4 \
,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 ,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable, categoria, max(FlagAtipico) FlagAtipico \
,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2 \
,sum(U_PECojera) U_PECojera \
,sum(U_PEHigadoIcterico) U_PEHigadoIcterico \
,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra \
,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,sum(U_PENecrosisHepatica) U_PENecrosisHepatica \
,sum(U_PENeumonia) U_PENeumonia \
,sum(U_PESepticemia) U_PESepticemia \
,sum(U_PEVomitoNegro) U_PEVomitoNegro \
,sum(U_PEAsperguillius) U_PEAsperguillius \
,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot \
,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12 \
,sum(MortSem13) as MortSem13,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17 \
,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19,sum(MortSem20) as MortSem20 \
,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 ,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 \
,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 ,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 \
,sum(MortSemAcum16) as MortSemAcum16 ,sum(MortSemAcum17) as MortSemAcum17 ,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 \
,sum(MortSemAcum20) as MortSemAcum20 ,sum(U_PECuadroToxico) U_PECuadroToxico,sum(PavosBBMortIncub) as PavosBBMortIncub \
,FlagTransfPavos,SourceComplexEntityNoGalpon,DestinationComplexEntityNoGalpon,SourceComplexEntityNoLote,DestinationComplexEntityNoLote \
from {database_name_tmp}.MortalidadCorral A \
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote \
left join {database_name_gl}.lk_galpon C on A.pk_galpon = C.pk_galpon \
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,a.pk_galpon,pk_tipoproducto,pk_estado \
,pk_administrador,pk_proveedor,clote,nogalpon,categoria,FlagTransfPavos,SourceComplexEntityNoGalpon,DestinationComplexEntityNoGalpon \
,SourceComplexEntityNoLote,DestinationComplexEntityNoLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadGalpon"
}
df_MortalidadGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadGalpon")
print('carga MortalidadGalpon', df_MortalidadGalponTemp.count())
df_MortalidadGalponSinAtipicoTemp = spark.sql(f"select \
max(pk_tiempo) as pk_tiempo,max(fecha) as fecha,a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,a.pk_galpon \
,pk_tipoproducto,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0') as pk_especie ,pk_estado,pk_administrador \
,pk_proveedor,max(pk_diasvida) as pk_diasvida \
,concat(Upper(clote) , '-' , nogalpon) as ComplexEntityNoGalpon \
,max(FechaCierre) as FechaCierre \
,sum(PobInicial) as PobInicial \
,sum(MortDia) as MortDia \
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4,sum(MortSem5) as MortSem5 \
,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 \
,sum(MortSemAcum3) as MortSemAcum3,sum(MortSemAcum4) as MortSemAcum4 ,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 \
,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable, categoria, 1 FlagAtipico \
,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2,sum(U_PECojera) U_PECojera,sum(U_PEHigadoIcterico) U_PEHigadoIcterico \
,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,sum(U_PENecrosisHepatica) U_PENecrosisHepatica,sum(U_PENeumonia) U_PENeumonia,sum(U_PESepticemia) U_PESepticemia \
,sum(U_PEVomitoNegro) U_PEVomitoNegro,sum(U_PEAsperguillius) U_PEAsperguillius,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot \
,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12 \
,sum(MortSem13) as MortSem13,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17 \
,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19,sum(MortSem20) as MortSem20,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 \
,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 ,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 \
,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 ,sum(MortSemAcum16) as MortSemAcum16 ,sum(MortSemAcum17) as MortSemAcum17 \
,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 ,sum(MortSemAcum20) as MortSemAcum20 ,sum(U_PECuadroToxico) U_PECuadroToxico \
,sum(PavosBBMortIncub) as PavosBBMortIncub,FlagTransfPavos,SourceComplexEntityNoGalpon,DestinationComplexEntityNoGalpon,SourceComplexEntityNoLote \
,DestinationComplexEntityNoLote \
from {database_name_tmp}.MortalidadCorral A \
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote \
left join {database_name_gl}.lk_galpon C on A.pk_galpon = C.pk_galpon \
where FlagAtipico = 1 \
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,a.pk_galpon,pk_tipoproducto,pk_estado \
,pk_administrador,pk_proveedor,clote,nogalpon,categoria,FlagTransfPavos,SourceComplexEntityNoGalpon \
,DestinationComplexEntityNoGalpon,SourceComplexEntityNoLote,DestinationComplexEntityNoLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadGalponSinAtipico"
}
df_MortalidadGalponSinAtipicoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadGalponSinAtipico")
print('carga MortalidadGalponSinAtipico',df_MortalidadGalponSinAtipicoTemp.count())
df_MortalidadGalpon_upd = spark.sql(f"select * from {database_name_tmp}.MortalidadGalponSinAtipico \
except \
select * from {database_name_tmp}.MortalidadGalpon")
print('carga df_MortalidadGalpon_upd',df_MortalidadGalpon_upd.count())
try:
    df = spark.table(f"{database_name_tmp}.MortalidadGalpon")
    df_MortalidadGalpon_new = df_MortalidadGalpon_upd.union(df)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_MortalidadGalpon_new = df_MortalidadGalpon_upd
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadGalpon_new"
}
# 1️⃣ Crear DataFrame intermedio
df_MortalidadGalpon_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.MortalidadGalpon_new")

df_MortalidadGalpon_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.MortalidadGalpon_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadGalpon")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadGalpon"
}
df_MortalidadGalpon_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadGalpon")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadGalpon_new")

print("carga INS MortalidadGalpon --> Registros procesados:", df_MortalidadGalpon_nueva.count())
df_ft_mortalidad_GalponF = spark.sql(f"SELECT \
b.pk_tiempo as pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_tipoproducto,pk_especie \
,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,(ComplexEntityNoGalpon) as ComplexEntityNo,PobInicial,MortDia \
,round((PorcMortDia*100),2) as PorcMortDia \
,MortSem1,MortSem2,MortSem3,MortSem4,MortSem5,MortSem6,MortSem7 \
,MortSemAcum1,MortSemAcum2,MortSemAcum3,MortSemAcum4,MortSemAcum5,MortSemAcum6,MortSemAcum7 \
,case when MortDia = 0 then 0.0 else U_PENoViable / (MortDia*1.0) end as TasaNoViable \
,U_PEAccidentados,U_PEHigadoGraso,U_PEHepatomegalia,U_PEHigadoHemorragico,U_PEInanicion \
,U_PEProblemaRespiratorio,U_PESCH,U_PEEnteritis,U_PEAscitis,U_PEMuerteSubita,U_PEEstresPorCalor \
,U_PEHidropericardio,U_PEHemopericardio,U_PEUratosis,U_PEMaterialCaseoso,U_PEOnfalitis \
,U_PERetencionDeYema,U_PEErosionDeMolleja,U_PEHemorragiaMusculos,U_PESangreEnCiego,U_PEPericarditis \
,U_PEPeritonitis,U_PEProlapso,U_PEPicaje,U_PERupturaAortica,U_PEBazoMoteado,U_PENoViable \
,'' U_PECaja \
,'' U_PEGota            \
,'' U_PEIntoxicacion    \
,'' U_PERetrazos        \
,'' U_PEEliminados      \
,'' U_PEAhogados        \
,'' U_PEEColi           \
,'' U_PEDescarte        \
,'' U_PEOtros           \
,'' U_PECoccidia        \
,'' U_PEDeshidratados   \
,'' U_PEHepatitis       \
,'' U_PETraumatismo     \
,case when PobInicial = 0 then 0 else round(((U_PEAccidentados)/(PobInicial*1.0))*100,3) end as PorcAccidentados \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoGraso)/(PobInicial*1.0))*100,3) end as PorcHigadoGraso \
,case when PobInicial = 0 then 0 else round(((U_PEHepatomegalia)/(PobInicial*1.0))*100,3) end as PorcHepatomegalia \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoHemorragico)/(PobInicial*1.0))*100,3) end as PorcHigadoHemorragico \
,case when PobInicial = 0 then 0 else round(((U_PEInanicion)/(PobInicial*1.0))*100,3) end as PorcInanicion \
,case when PobInicial = 0 then 0 else round(((U_PEProblemaRespiratorio)/(PobInicial*1.0))*100,3) end as PorcProblemaRespiratorio \
,case when PobInicial = 0 then 0 else round(((U_PESCH)/(PobInicial*1.0))*100,3) end as PorcSCH \
,case when PobInicial = 0 then 0 else round(((U_PEEnteritis)/(PobInicial*1.0))*100,3) end as PorcEnteritis \
,case when PobInicial = 0 then 0 else round(((U_PEAscitis)/(PobInicial*1.0))*100,3) end as PorcAscitis \
,case when PobInicial = 0 then 0 else round(((U_PEMuerteSubita)/(PobInicial*1.0))*100,3) end as PorcMuerteSubita \
,case when PobInicial = 0 then 0 else round(((U_PEEstresPorCalor)/(PobInicial*1.0))*100,3) end as PorcEstresPorCalor \
,case when PobInicial = 0 then 0 else round(((U_PEHidropericardio)/(PobInicial*1.0))*100,3) end as PorcHidropericardio \
,case when PobInicial = 0 then 0 else round(((U_PEHemopericardio)/(PobInicial*1.0))*100,3) end as PorcHemopericardio \
,case when PobInicial = 0 then 0 else round(((U_PEUratosis)/(PobInicial*1.0))*100,3) end as PorcUratosis \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso \
,case when PobInicial = 0 then 0 else round(((U_PEOnfalitis)/(PobInicial*1.0))*100,3) end as PorcOnfalitis \
,case when PobInicial = 0 then 0 else round(((U_PERetencionDeYema)/(PobInicial*1.0))*100,3) end as PorcRetencionDeYema \
,case when PobInicial = 0 then 0 else round(((U_PEErosionDeMolleja)/(PobInicial*1.0))*100,3) end as PorcErosionDeMolleja \
,case when PobInicial = 0 then 0 else round(((U_PEHemorragiaMusculos)/(PobInicial*1.0))*100,3) end as PorcHemorragiaMusculos \
,case when PobInicial = 0 then 0 else round(((U_PESangreEnCiego)/(PobInicial*1.0))*100,3) end as PorcSangreEnCiego \
,case when PobInicial = 0 then 0 else round(((U_PEPericarditis)/(PobInicial*1.0))*100,3) end as PorcPericarditis \
,case when PobInicial = 0 then 0 else round(((U_PEPeritonitis)/(PobInicial*1.0))*100,3) end as PorcPeritonitis \
,case when PobInicial = 0 then 0 else round(((U_PEProlapso)/(PobInicial*1.0))*100,3) end as PorcProlapso \
,case when PobInicial = 0 then 0 else round(((U_PEPicaje)/(PobInicial*1.0))*100,3) end as PorcPicaje \
,case when PobInicial = 0 then 0 else round(((U_PERupturaAortica)/(PobInicial*1.0))*100,3) end as PorcRupturaAortica \
,case when PobInicial = 0 then 0 else round(((U_PEBazoMoteado)/(PobInicial*1.0))*100,3) end as PorcBazoMoteado \
,case when PobInicial = 0 then 0 else round(((U_PENoViable)/(PobInicial*1.0))*100,3) end as PorcNoViable \
,'' PorcCaja \
,'' PorcGota            \
,'' PorcIntoxicacion    \
,'' PorcRetrazos        \
,'' PorcEliminados      \
,'' PorcAhogados        \
,'' PorcEColi           \
,'' PorcDescarte        \
,'' PorcOtros           \
,'' PorcCoccidia        \
,'' PorcDeshidratados   \
,'' PorcHepatitis       \
,'' PorcTraumatismo     \
,date_format(a.fecha,'yyyyMMdd') as EventDate,categoria \
,FlagAtipico,U_PEAerosaculitisG2,U_PECojera,U_PEHigadoIcterico,U_PEMaterialCaseoso_po1ra,U_PEMaterialCaseosoMedRetr \
,U_PENecrosisHepatica,U_PENeumonia,U_PESepticemia,U_PEVomitoNegro \
,case when PobInicial = 0 then 0 else round(((U_PEAerosaculitisG2)/(PobInicial*1.0))*100,3) end as PorcAerosaculitisG2 \
,case when PobInicial = 0 then 0 else round(((U_PECojera)/(PobInicial*1.0))*100,3) end as PorcCojera \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoIcterico)/(PobInicial*1.0))*100,3) end as PorcHigadoIcterico \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso_po1ra)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso_po1ra \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseosoMedRetr)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseosoMedRetr \
,case when PobInicial = 0 then 0 else round(((U_PENecrosisHepatica)/(PobInicial*1.0))*100,3) end as PorcNecrosisHepatica \
,case when PobInicial = 0 then 0 else round(((U_PENeumonia)/(PobInicial*1.0))*100,3) end as PorcNeumonia \
,case when PobInicial = 0 then 0 else round(((U_PESepticemia)/(PobInicial*1.0))*100,3) end as PorcSepticemia \
,case when PobInicial = 0 then 0 else round(((U_PEVomitoNegro)/(PobInicial*1.0))*100,3) end as PorcVomitoNegro \
,U_PEAsperguillius,U_PEBazoGrandeMot,U_PECorazonGrande \
,case when PobInicial = 0 then 0 else round(((U_PEAsperguillius)/(PobInicial*1.0))*100,3) end as PorcAsperguillius \
,case when PobInicial = 0 then 0 else round(((U_PEBazoGrandeMot)/(PobInicial*1.0))*100,3) end as PorcBazoGrandeMot \
,case when PobInicial = 0 then 0 else round(((U_PECorazonGrande)/(PobInicial*1.0))*100,3) end as PorcCorazonGrande \
,MortSem8,MortSem9,MortSem10,MortSem11,MortSem12,MortSem13,MortSem14,MortSem15,MortSem16,MortSem17,MortSem18,MortSem19,MortSem20 \
,MortSemAcum8,MortSemAcum9,MortSemAcum10,MortSemAcum11,MortSemAcum12,MortSemAcum13,MortSemAcum14,MortSemAcum15,MortSemAcum16 \
,MortSemAcum17,MortSemAcum18,MortSemAcum19,MortSemAcum20 \
,U_PECuadroToxico \
,case when PobInicial = 0 then 0 else round(((U_PECuadroToxico)/(PobInicial*1.0))*100,3) end as PorcCuadroToxico \
,PavosBBMortIncub,FlagTransfPavos \
,SourceComplexEntityNoGalpon SourceComplexEntityNo \
,DestinationComplexEntityNoGalpon DestinationComplexEntityNo\
,'' U_PENoEspecificado \
,'' U_PEAnalisis_Laboratorio    \
,'' PorcNoEspecificado          \
,'' PorcAnalisisLaboratorio     \
,b.fecha DescripFecha           \
,'' DescripEmpresa              \
,'' DescripDivision             \
,'' DescripZona                 \
,'' DescripSubzona              \
,'' Plantel                     \
,'' Lote                        \
,'' Galpon                      \
,'' DescripTipoProducto         \
,'' DescripEspecie              \
,'' DescripEstado               \
,'' DescripAdministrador        \
,'' DescripProveedor            \
,'' DescripDiaVida              \
FROM {database_name_tmp}.MortalidadGalpon A \
left join {database_name_gl}.lk_tiempo b on cast(concat(substring(a.fechacierre,1,4),'-',substring(a.fechacierre,5,2),'-',substring(a.fechacierre,7,2)) as date)=b.fecha \
")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_galponF"
}
df_ft_mortalidad_GalponF.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidad_galponF")
print('carga df_ft_mortalidad_GalponF', df_ft_mortalidad_GalponF.count())
df_ft_mortalidad_Galpon = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidad_galponF
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_mortalidad_Galpon', df_ft_mortalidad_Galpon.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes27 = spark.read.format("parquet").load(path_target27)
    datos_existentes27 = True
    logger.info(f"Datos existentes de ft_mortalidad_Galpon cargados: {df_existentes27.count()} registros")
except:
    datos_existentes27 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Galpon")

if datos_existentes27:
    existing_data27 = spark.read.format("parquet").load(path_target27)
    data_after_delete27 = existing_data27.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data27 = df_ft_mortalidad_Galpon.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data27 = filtered_new_data27.union(data_after_delete27)                             
   
    cant_ingresonuevo27 = filtered_new_data27.count()
    cant_total27 = final_data27.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_GalponTemporal"
    }
    final_data27.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_GalponTemporal")

    final_data27_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_GalponTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_galpon"
    }
    final_data27_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_galpon")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Galpon : {cant_ingresonuevo27}")
    print(f"Total de registros en la tabla ft_mortalidad_Galpon : {cant_total27}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_GalponTemporal')
    print(f"Tabla ft_mortalidad_GalponTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_galpon"
    }
    df_ft_mortalidad_Galpon.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_galpon")
df_OrdenIdTemp = spark.sql(f"select \
Upper(clote) as clote,pk_especie,pk_tipoproducto,sum(PobInicial) PobInicial, \
DENSE_RANK() OVER (PARTITION BY clote ORDER BY sum(PobInicial) asc) as orden \
from {database_name_tmp}.MortalidadGalpon A \
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote \
group by clote,pk_especie,pk_tipoproducto")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenId"
}
df_OrdenIdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenId")
print('carga OrdenId', df_OrdenIdTemp.count())
df_listaMortalidadGalponTemp = spark.sql(f"SELECT pk_lote, concat_ws(',',collect_list( DISTINCT categoria)) categoria FROM {database_name_tmp}.MortalidadGalpon  group by pk_lote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/listaMortalidadGalponTemp"
}
df_listaMortalidadGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.listaMortalidadGalponTemp")
print('carga listaMortalidadGalponTemp', df_listaMortalidadGalponTemp.count())
df_maxordenIdTemp =spark.sql(f"select Upper(clote) as clote,max(orden) orden from {database_name_tmp}.OrdenId  group by  clote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/maxordenIdTemp"
}
df_maxordenIdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.maxordenIdTemp")
print('carga maxordenIdTemp',df_maxordenIdTemp.count())
df_MortalidadLoteTemp = spark.sql(f"select \
max(pk_tiempo) as pk_tiempo,max(fecha) as fecha \
,case when pk_estado = 2 then date_format(max(fecha),'yyyyMM') else substring(max(FechaCierre),1,6) end idmes \
,a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,c.pk_tipoproducto,a.pk_especie,pk_estado \
,min(pk_administrador) as pk_administrador,pk_proveedor,Upper(b.clote) as ComplexEntityNoLote \
,max(FechaCierre) as FechaCierre \
,sum(a.PobInicial) as PobInicial \
,sum(MortDia) as MortDia \
,case when sum(a.PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(a.PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4,sum(MortSem5) as MortSem5 \
,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7 \
,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 ,sum(MortSemAcum3) as MortSemAcum3,sum(MortSemAcum4) as MortSemAcum4 \
,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 ,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable \
,max(f.categoria) categoria \
,max(FlagAtipico) FlagAtipico,2 FlagArtAtipico \
,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2 \
,sum(U_PECojera) U_PECojera,sum(U_PEHigadoIcterico) U_PEHigadoIcterico,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra \
,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr,sum(U_PENecrosisHepatica) U_PENecrosisHepatica \
,sum(U_PENeumonia) U_PENeumonia,sum(U_PESepticemia) U_PESepticemia,sum(U_PEVomitoNegro) U_PEVomitoNegro,sum(U_PEAsperguillius) U_PEAsperguillius \
,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12 \
,sum(MortSem13) as MortSem13,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17 \
,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19,sum(MortSem20) as MortSem20 \
,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 ,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 \
,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 ,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 \
,sum(MortSemAcum16) as MortSemAcum16 ,sum(MortSemAcum17) as MortSemAcum17 ,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 \
,sum(MortSemAcum20) as MortSemAcum20 ,sum(U_PECuadroToxico) U_PECuadroToxico,sum(PavosBBMortIncub) as PavosBBMortIncub \
,FlagTransfPavos,SourceComplexEntityNoLote,DestinationComplexEntityNoLote \
from {database_name_tmp}.MortalidadGalpon A \
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote \
left join {database_name_tmp}.OrdenId C on Upper(B.clote) = Upper(C.clote) \
left join {database_name_tmp}.listaMortalidadGalponTemp f on F.pk_lote = A.pk_lote \
left join {database_name_tmp}.maxordenIdTemp g on Upper(g.clote) = Upper(C.clote) and C.orden = g.orden \
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,c.pk_tipoproducto,pk_estado,b.clote,a.pk_especie,pk_proveedor \
,FlagTransfPavos,SourceComplexEntityNoLote,DestinationComplexEntityNoLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLote"
}

df_MortalidadLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadLote")
print('carga MortalidadLote', df_MortalidadLoteTemp.count())
df_MortalidadLoteSinAtipicoTemp =spark.sql(f"select \
max(pk_tiempo) as pk_tiempo,max(fecha) as fecha,case when pk_estado = 2 then date_format(max(fecha),'yyyyMM' ) else substring(max(FechaCierre),1,6) end idmes \
,a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,c.pk_tipoproducto,a.pk_especie,pk_estado \
,min(pk_administrador) as pk_administrador,pk_proveedor,Upper(b.clote) as ComplexEntityNoLote \
,max(FechaCierre) as FechaCierre \
,sum(a.PobInicial) as PobInicial \
,sum(MortDia) as MortDia \
,case when sum(a.PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(a.PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4,sum(MortSem5) as MortSem5 \
,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7 \
,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 ,sum(MortSemAcum3) as MortSemAcum3,sum(MortSemAcum4) as MortSemAcum4 \
,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 ,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable \
,max(f.categoria) categoria \
,1 FlagAtipico \
,1 FlagArtAtipico \
,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2,sum(U_PECojera) U_PECojera,sum(U_PEHigadoIcterico) U_PEHigadoIcterico,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra \
,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr,sum(U_PENecrosisHepatica) U_PENecrosisHepatica,sum(U_PENeumonia) U_PENeumonia,sum(U_PESepticemia) U_PESepticemia \
,sum(U_PEVomitoNegro) U_PEVomitoNegro,sum(U_PEAsperguillius) U_PEAsperguillius,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12,sum(MortSem13) as MortSem13 \
,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19 \
,sum(MortSem20) as MortSem20,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 ,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 \
,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 ,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 ,sum(MortSemAcum16) as MortSemAcum16 \
,sum(MortSemAcum17) as MortSemAcum17 ,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 ,sum(MortSemAcum20) as MortSemAcum20 ,sum(U_PECuadroToxico) U_PECuadroToxico \
,sum(PavosBBMortIncub) as PavosBBMortIncub,FlagTransfPavos,SourceComplexEntityNoLote,DestinationComplexEntityNoLote \
from {database_name_tmp}.MortalidadGalpon A \
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote \
left join {database_name_tmp}.OrdenId C on Upper(B.clote) = Upper(C.clote) \
left join {database_name_tmp}.listaMortalidadGalponTemp f on F.pk_lote = A.pk_lote \
left join {database_name_tmp}.maxordenIdTemp g on Upper(g.clote) = Upper(C.clote) and C.orden = g.orden \
where FlagAtipico = 1 \
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,a.pk_plantel,a.pk_lote,c.pk_tipoproducto,pk_estado,b.clote,a.pk_especie,pk_proveedor \
,FlagTransfPavos,SourceComplexEntityNoLote,DestinationComplexEntityNoLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLoteSinAtipico"
}

df_MortalidadLoteSinAtipicoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadLoteSinAtipico")
print('carga MortalidadLoteSinAtipico', df_MortalidadLoteSinAtipicoTemp.count())
df_MortalidadLote_upd = spark.sql(f"select * from {database_name_tmp}.MortalidadLoteSinAtipico \
except \
select * from {database_name_tmp}.MortalidadLote")
print('carga df_MortalidadLote_upd', df_MortalidadLote_upd.count())
try:
    df = spark.table(f"{database_name_tmp}.MortalidadLote")
    df_MortalidadLote_new = df_MortalidadLote_upd.union(df)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_MortalidadLote_new = df_MortalidadLote_upd
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLote_new"
}
# 1️⃣ Crear DataFrame intermedio
df_MortalidadLote_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.MortalidadLote_new")

df_MortalidadLote_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.MortalidadLote_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadLote")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLote"
}
df_MortalidadLote_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadLote")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadLote_new")

print("carga INS MortalidadLote --> Registros procesados:", df_MortalidadLote_nueva.count())
df_ft_mortalidad_LoteF = spark.sql(f"SELECT \
a.pk_tiempo as pk_tiempo \
,a.idmes,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_tipoproducto \
,pk_especie,pk_estado,pk_administrador,pk_proveedor \
,(ComplexEntityNoLote) as ComplexEntityNo,PobInicial,MortDia \
,round((PorcMortDia*100),2) as PorcMortDia \
,MortSem1,MortSem2,MortSem3,MortSem4,MortSem5,MortSem6,MortSem7 \
,MortSemAcum1,MortSemAcum2,MortSemAcum3,MortSemAcum4,MortSemAcum5,MortSemAcum6,MortSemAcum7 \
,case when MortDia = 0 then 0.0 else U_PENoViable / (MortDia*1.0) end as TasaNoViable \
,U_PEAccidentados,U_PEHigadoGraso,U_PEHepatomegalia,U_PEHigadoHemorragico,U_PEInanicion \
,U_PEProblemaRespiratorio,U_PESCH,U_PEEnteritis,U_PEAscitis,U_PEMuerteSubita,U_PEEstresPorCalor \
,U_PEHidropericardio,U_PEHemopericardio,U_PEUratosis,U_PEMaterialCaseoso,U_PEOnfalitis \
,U_PERetencionDeYema,U_PEErosionDeMolleja,U_PEHemorragiaMusculos,U_PESangreEnCiego \
,U_PEPericarditis,U_PEPeritonitis,U_PEProlapso,U_PEPicaje,U_PERupturaAortica \
,U_PEBazoMoteado,U_PENoViable \
,'' U_PECaja \
,'' U_PEGota            \
,'' U_PEIntoxicacion    \
,'' U_PERetrazos        \
,'' U_PEEliminados      \
,'' U_PEAhogados        \
,'' U_PEEColi           \
,'' U_PEDescarte        \
,'' U_PEOtros           \
,'' U_PECoccidia        \
,'' U_PEDeshidratados   \
,'' U_PEHepatitis       \
,'' U_PETraumatismo     \
,case when PobInicial = 0 then 0 else round(((U_PEAccidentados)/(PobInicial*1.0))*100,3) end as PorcAccidentados \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoGraso)/(PobInicial*1.0))*100,3) end as PorcHigadoGraso \
,case when PobInicial = 0 then 0 else round(((U_PEHepatomegalia)/(PobInicial*1.0))*100,3) end as PorcHepatomegalia \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoHemorragico)/(PobInicial*1.0))*100,3) end as PorcHigadoHemorragico \
,case when PobInicial = 0 then 0 else round(((U_PEInanicion)/(PobInicial*1.0))*100,3) end as PorcInanicion \
,case when PobInicial = 0 then 0 else round(((U_PEProblemaRespiratorio)/(PobInicial*1.0))*100,3) end as PorcProblemaRespiratorio \
,case when PobInicial = 0 then 0 else round(((U_PESCH)/(PobInicial*1.0))*100,3) end as PorcSCH \
,case when PobInicial = 0 then 0 else round(((U_PEEnteritis)/(PobInicial*1.0))*100,3) end as PorcEnteritis \
,case when PobInicial = 0 then 0 else round(((U_PEAscitis)/(PobInicial*1.0))*100,3) end as PorcAscitis \
,case when PobInicial = 0 then 0 else round(((U_PEMuerteSubita)/(PobInicial*1.0))*100,3) end as PorcMuerteSubita \
,case when PobInicial = 0 then 0 else round(((U_PEEstresPorCalor)/(PobInicial*1.0))*100,3) end as PorcEstresPorCalor \
,case when PobInicial = 0 then 0 else round(((U_PEHidropericardio)/(PobInicial*1.0))*100,3) end as PorcHidropericardio \
,case when PobInicial = 0 then 0 else round(((U_PEHemopericardio)/(PobInicial*1.0))*100,3) end as PorcHemopericardio \
,case when PobInicial = 0 then 0 else round(((U_PEUratosis)/(PobInicial*1.0))*100,3) end as PorcUratosis \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso \
,case when PobInicial = 0 then 0 else round(((U_PEOnfalitis)/(PobInicial*1.0))*100,3) end as PorcOnfalitis \
,case when PobInicial = 0 then 0 else round(((U_PERetencionDeYema)/(PobInicial*1.0))*100,3) end as PorcRetencionDeYema \
,case when PobInicial = 0 then 0 else round(((U_PEErosionDeMolleja)/(PobInicial*1.0))*100,3) end as PorcErosionDeMolleja \
,case when PobInicial = 0 then 0 else round(((U_PEHemorragiaMusculos)/(PobInicial*1.0))*100,3) end as PorcHemorragiaMusculos \
,case when PobInicial = 0 then 0 else round(((U_PESangreEnCiego)/(PobInicial*1.0))*100,3) end as PorcSangreEnCiego \
,case when PobInicial = 0 then 0 else round(((U_PEPericarditis)/(PobInicial*1.0))*100,3) end as PorcPericarditis \
,case when PobInicial = 0 then 0 else round(((U_PEPeritonitis)/(PobInicial*1.0))*100,3) end as PorcPeritonitis \
,case when PobInicial = 0 then 0 else round(((U_PEProlapso)/(PobInicial*1.0))*100,3) end as PorcProlapso \
,case when PobInicial = 0 then 0 else round(((U_PEPicaje)/(PobInicial*1.0))*100,3) end as PorcPicaje \
,case when PobInicial = 0 then 0 else round(((U_PERupturaAortica)/(PobInicial*1.0))*100,3) end as PorcRupturaAortica \
,case when PobInicial = 0 then 0 else round(((U_PEBazoMoteado)/(PobInicial*1.0))*100,3) end as PorcBazoMoteado \
,case when PobInicial = 0 then 0 else round(((U_PENoViable)/(PobInicial*1.0))*100,3) end as PorcNoViable \
,'' PorcCaja \
,'' PorcGota            \
,'' PorcIntoxicacion    \
,'' PorcRetrazos        \
,'' PorcEliminados      \
,'' PorcAhogados        \
,'' PorcEColi           \
,'' PorcDescarte        \
,'' PorcOtros           \
,'' PorcCoccidia        \
,'' PorcDeshidratados   \
,'' PorcHepatitis       \
,'' PorcTraumatismo     \
,date_format(a.fecha,'yyyyMMdd') as EventDate,categoria \
,FlagAtipico,FlagArtAtipico,U_PEAerosaculitisG2,U_PECojera,U_PEHigadoIcterico,U_PEMaterialCaseoso_po1ra \
,U_PEMaterialCaseosoMedRetr,U_PENecrosisHepatica,U_PENeumonia,U_PESepticemia,U_PEVomitoNegro \
,case when PobInicial = 0 then 0 else round(((U_PEAerosaculitisG2)/(PobInicial*1.0))*100,3) end as PorcAerosaculitisG2 \
,case when PobInicial = 0 then 0 else round(((U_PECojera)/(PobInicial*1.0))*100,3) end as PorcCojera \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoIcterico)/(PobInicial*1.0))*100,3) end as PorcHigadoIcterico \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso_po1ra)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso_po1ra \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseosoMedRetr)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseosoMedRetr \
,case when PobInicial = 0 then 0 else round(((U_PENecrosisHepatica)/(PobInicial*1.0))*100,3) end as PorcNecrosisHepatica \
,case when PobInicial = 0 then 0 else round(((U_PENeumonia)/(PobInicial*1.0))*100,3) end as PorcNeumonia \
,case when PobInicial = 0 then 0 else round(((U_PESepticemia)/(PobInicial*1.0))*100,3) end as PorcSepticemia \
,case when PobInicial = 0 then 0 else round(((U_PEVomitoNegro)/(PobInicial*1.0))*100,3) end as PorcVomitoNegro \
,U_PEAsperguillius,U_PEBazoGrandeMot,U_PECorazonGrande \
,case when PobInicial = 0 then 0 else round(((U_PEAsperguillius)/(PobInicial*1.0))*100,3) end as PorcAsperguillius \
,case when PobInicial = 0 then 0 else round(((U_PEBazoGrandeMot)/(PobInicial*1.0))*100,3) end as PorcBazoGrandeMot \
,case when PobInicial = 0 then 0 else round(((U_PECorazonGrande)/(PobInicial*1.0))*100,3) end as PorcCorazonGrande \
,MortSem8,MortSem9,MortSem10,MortSem11,MortSem12,MortSem13,MortSem14,MortSem15,MortSem16,MortSem17,MortSem18,MortSem19,MortSem20 \
,MortSemAcum8,MortSemAcum9,MortSemAcum10,MortSemAcum11,MortSemAcum12,MortSemAcum13,MortSemAcum14,MortSemAcum15,MortSemAcum16 \
,MortSemAcum17,MortSemAcum18,MortSemAcum19,MortSemAcum20,U_PECuadroToxico \
,case when PobInicial = 0 then 0 else round(((U_PECuadroToxico)/(PobInicial*1.0))*100,3) end as PorcCuadroToxico \
,PavosBBMortIncub,FlagTransfPavos,SourceComplexEntityNoLote SourceComplexEntityNo,DestinationComplexEntityNoLote DestinationComplexEntityNo \
,'' U_PENoEspecificado \
,'' U_PEAnalisis_Laboratorio    \
,'' PorcNoEspecificado          \
,'' PorcAnalisisLaboratorio     \
,b.fecha DescripFecha                \
,'' DescripMes                  \
,'' DescripEmpresa              \
,'' DescripDivision             \
,'' DescripZona                 \
,'' DescripSubzona              \
,'' Plantel                     \
,'' Lote                        \
,'' DescripTipoProducto         \
,'' DescripEspecie              \
,'' DescripEstado               \
,'' DescripAdministrador        \
,'' DescripProveedor            \
,'' DescripDiaVida              \
FROM {database_name_tmp}.MortalidadLote A \
left join {database_name_gl}.lk_tiempo b on cast(concat(substring(fechacierre,1,4),'-',substring(fechacierre,5,2),'-',substring(fechacierre,7,2)) as date)=b.fecha \
")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_LoteF"
}
df_ft_mortalidad_LoteF.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_mortalidad_LoteF")
print('df_ft_mortalidad_LoteF', df_ft_mortalidad_LoteF.count())
df_ft_mortalidad_Lote = spark.sql(f"""SELECT * from {database_name_tmp}.ft_mortalidad_LoteF
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_mortalidad_Lote', df_ft_mortalidad_Lote.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes28 = spark.read.format("parquet").load(path_target28)
    datos_existentes28 = True
    logger.info(f"Datos existentes de ft_mortalidad_Lote cargados: {df_existentes28.count()} registros")
except:
    datos_existentes28 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Lote")

if datos_existentes28:
    existing_data28 = spark.read.format("parquet").load(path_target28)
    data_after_delete28 = existing_data28.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data28 = df_ft_mortalidad_Lote.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data28 = filtered_new_data28.union(data_after_delete28)                             
   
    cant_ingresonuevo28 = filtered_new_data28.count()
    cant_total28 = final_data28.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_LoteTemporal"
    }
    final_data28.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_LoteTemporal")

    final_data28_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_LoteTemporal")      
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_lote"
    }
    final_data28_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_lote")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Lote : {cant_ingresonuevo28}")
    print(f"Total de registros en la tabla ft_mortalidad_Lote : {cant_total28}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_LoteTemporal')
    print(f"Tabla ft_mortalidad_LoteTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_lote"
    }
    df_ft_mortalidad_Lote.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_lote")
df_LesionLoteSemanalTemp = spark.sql(f"""select \
max(a.pk_tiempo) pk_tiempo 
,max(a.descripfecha) fecha 
,a.pk_empresa,
a.pk_division,
a.pk_zona,
a.pk_subzona,
a.pk_plantel,
a.pk_lote,
a.pk_estado,
a.pk_tipoproducto,
a.pk_administrador,
a.pk_proveedor,
a.pk_semanavida 
,min(a.pk_diasvida) pk_diasvida 
,substring(A.complexentityno,1,(length(A.complexentityno)-6)) as complexentityno 
,MAX(e.PobInicial) PobInicial 
,sum(a.U_PEAccidentados) as U_PEAccidentados 
,sum(a.U_PEHigadoGraso) as U_PEHigadoGraso
,sum(a.U_PEHepatomegalia) as U_PEHepatomegalia
,sum(a.U_PEHigadoHemorragico) as U_PEHigadoHemorragico 
,sum(a.U_PEInanicion) as U_PEInanicion
,sum(a.U_PEProblemaRespiratorio) as U_PEProblemaRespiratorio
,sum(a.U_PESCH) as U_PESCH 
,sum(a.U_PEEnteritis) as U_PEEnteritis
,sum(a.U_PEAscitis) as U_PEAscitis
,sum(a.U_PEMuerteSubita) as U_PEMuerteSubita
,sum(a.U_PEEstresPorCalor) as U_PEEstresPorCalor 
,sum(a.U_PEHidropericardio) as U_PEHidropericardio
,sum(a.U_PEHemopericardio) as U_PEHemopericardio
,sum(a.U_PEUratosis) as U_PEUratosis
,sum(a.U_PEMaterialCaseoso) as U_PEMaterialCaseoso 
,sum(a.U_PEOnfalitis) as U_PEOnfalitis
,sum(a.U_PERetencionDeYema) as U_PERetencionDeYema
,sum(a.U_PEErosionDeMolleja) as U_PEErosionDeMolleja
,sum(a.U_PEHemorragiaMusculos) as U_PEHemorragiaMusculos 
,sum(a.U_PESangreEnCiego) as U_PESangreEnCiego
,sum(a.U_PEPericarditis) as U_PEPericarditis
,sum(a.U_PEPeritonitis) as U_PEPeritonitis
,sum(a.U_PEProlapso) as U_PEProlapso 
,sum(a.U_PEPicaje) as U_PEPicaje
,sum(a.U_PERupturaAortica) as U_PERupturaAortica
,sum(a.U_PEBazoMoteado) as U_PEBazoMoteado
,sum(a.U_PENoViable) as U_PENoViable 
,sum(a.U_PEAerosaculitisG2) U_PEAerosaculitisG2
,sum(a.U_PECojera) U_PECojera
,sum(a.U_PEHigadoIcterico) U_PEHigadoIcterico
,sum(a.U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra 
,sum(a.U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr
,sum(a.U_PENecrosisHepatica) U_PENecrosisHepatica
,sum(a.U_PENeumonia) U_PENeumonia
,sum(a.U_PESepticemia) U_PESepticemia 
,sum(a.U_PEVomitoNegro) U_PEVomitoNegro
,sum(U_PEAsperguillius) U_PEAsperguillius
,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot
,sum(U_PECorazonGrande) U_PECorazonGrande 
,sum(U_PECuadroToxico) U_PECuadroToxico 
from {database_name_gl}.ft_mortalidad_diario a 
left join (
           select pk_lote,max(PobInicial) PobInicial from {database_name_gl}.ft_mortalidad_lote group by pk_lote
          ) e on a.pk_lote = e.pk_lote 
where a.flagartificio = 1 and pk_empresa = 1 and a.pk_division = 4
and (date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
and a.pk_lote not in (select pk_lote from {database_name_gl}.lk_lote where (Upper(clote) like 'P186%' AND SUBSTRING(clote,8,4) >= '11')) 
group by a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote,a.pk_estado,a.pk_tipoproducto 
,a.pk_administrador,a.pk_proveedor,a.pk_semanavida,substring(A.complexentityno,1,(length(A.complexentityno)-6))""")



# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LesionLoteSemanal"
}
df_LesionLoteSemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.LesionLoteSemanal")
print('carga LesionLoteSemanal', df_LesionLoteSemanalTemp.count())
df_listaMortalidadDiarioCatTemp = spark.sql(f"SELECT pk_lote, concat_ws(',',collect_list( DISTINCT categoria)) categoria \
FROM {database_name_gl}.ft_mortalidad_diario where pk_lote not in (select pk_lote from {database_name_gl}.lk_lote where (Upper(clote) like 'P186%' AND SUBSTRING(clote,8,4) >= '11')) group by pk_lote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/listaMortalidadDiarioCatTemp"
}
df_listaMortalidadDiarioCatTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.listaMortalidadDiarioCatTemp")
print('carga listaMortalidadDiarioCatTemp', df_listaMortalidadDiarioCatTemp.count())
df_MortalidadLoteSemanalTemp = spark.sql(f"""select \
 max(a.pk_tiempo) pk_tiempo 
,max(a.descripfecha) fecha 
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_especie
,a.pk_estado
,a.pk_tipoproducto
,a.pk_administrador
,a.pk_proveedor 
,a.pk_semanavida
,min(a.pk_diasvida) pk_diasvida 
,substring(A.complexentityno,1,(length(A.complexentityno)-6)) as complexentityno 
,max(a.fechanacimiento) FechaNacimiento 
,MAX(e.PobInicial) PobInicial 
,MAX(b.PobInicial) PobInicial2 
,SUM(a.MortDia) MortDia 
,sum(a.U_PEAccidentados) as U_PEAccidentados
,sum(a.U_PEHigadoGraso) as U_PEHigadoGraso
,sum(a.U_PEHepatomegalia) as U_PEHepatomegalia 
,sum(a.U_PEHigadoHemorragico) as U_PEHigadoHemorragico
,sum(a.U_PEInanicion) as U_PEInanicion
,sum(a.U_PEProblemaRespiratorio) as U_PEProblemaRespiratorio 
,sum(a.U_PESCH) as U_PESCH
,sum(a.U_PEEnteritis) as U_PEEnteritis
,sum(a.U_PEAscitis) as U_PEAscitis
,sum(a.U_PEMuerteSubita) as U_PEMuerteSubita 
,sum(a.U_PEEstresPorCalor) as U_PEEstresPorCalor
,sum(a.U_PEHidropericardio) as U_PEHidropericardio
,sum(a.U_PEHemopericardio) as U_PEHemopericardio 
,sum(a.U_PEUratosis) as U_PEUratosis
,sum(a.U_PEMaterialCaseoso) as U_PEMaterialCaseoso
,sum(a.U_PEOnfalitis) as U_PEOnfalitis 
,sum(a.U_PERetencionDeYema) as U_PERetencionDeYema
,sum(a.U_PEErosionDeMolleja) as U_PEErosionDeMolleja
,sum(a.U_PEHemorragiaMusculos) as U_PEHemorragiaMusculos 
,sum(a.U_PESangreEnCiego) as U_PESangreEnCiego
,sum(a.U_PEPericarditis) as U_PEPericarditis
,sum(a.U_PEPeritonitis) as U_PEPeritonitis 
,sum(a.U_PEProlapso) as U_PEProlapso
,sum(a.U_PEPicaje) as U_PEPicaje
,sum(a.U_PERupturaAortica) as U_PERupturaAortica 
,sum(a.U_PEBazoMoteado) as U_PEBazoMoteado
,sum(a.U_PENoViable) as U_PENoViable 
,max(F.categoria) categoria 
,sum(a.U_PEAerosaculitisG2) U_PEAerosaculitisG2 
,sum(a.U_PECojera) U_PECojera 
,sum(a.U_PEHigadoIcterico) U_PEHigadoIcterico \
,sum(a.U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra 
,sum(a.U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr 
,sum(a.U_PENecrosisHepatica) U_PENecrosisHepatica 
,sum(a.U_PENeumonia) U_PENeumonia 
,sum(a.U_PESepticemia) U_PESepticemia 
,sum(a.U_PEVomitoNegro) U_PEVomitoNegro 
,sum(a.U_PEAsperguillius) U_PEAsperguillius 
,sum(a.U_PEBazoGrandeMot) U_PEBazoGrandeMot 
,sum(a.U_PECorazonGrande) U_PECorazonGrande 
,sum(a.U_PECuadroToxico) U_PECuadroToxico 
,MAX(B.U_PEAccidentados) SemAccidentados 
,MAX(B.U_PEHigadoGraso) SemHigadoGraso 
,MAX(B.U_PEHepatomegalia) SemHepatomegalia 
,MAX(B.U_PEHigadoHemorragico) SemHigadoHemorragico 
,MAX(B.U_PEInanicion) SemInanicion 
,MAX(B.U_PEProblemaRespiratorio) SemProblemaRespiratorio 
,MAX(B.U_PESCH) SemSCH 
,MAX(B.U_PEEnteritis) SemEnteritis 
,MAX(B.U_PEAscitis) SemAscitis 
,MAX(B.U_PEMuerteSubita) SemMuerteSubita 
,MAX(B.U_PEEstresPorCalor) SemEstresPorCalor 
,MAX(B.U_PEHidropericardio) SemHidropericardio 
,MAX(B.U_PEHemopericardio) SemHemopericardio 
,MAX(B.U_PEUratosis) SemUratosis 
,MAX(B.U_PEMaterialCaseoso) SemMaterialCaseoso 
,MAX(B.U_PEOnfalitis) SemOnfalitis 
,MAX(B.U_PERetencionDeYema) SemRetencionDeYema 
,MAX(B.U_PEErosionDeMolleja) SemErosionDeMolleja 
,MAX(B.U_PEHemorragiaMusculos) SemHemorragiaMusculos 
,MAX(B.U_PESangreEnCiego) SemSangreEnCiego 
,MAX(B.U_PEPericarditis) SemPericarditis 
,MAX(B.U_PEPeritonitis) SemPeritonitis 
,MAX(B.U_PEProlapso) SemProlapso 
,MAX(B.U_PEPicaje) SemPicaje 
,MAX(B.U_PERupturaAortica) SemRupturaAortica 
,MAX(B.U_PEBazoMoteado) SemBazoMoteado 
,MAX(B.U_PENoViable) SemNoViable 
,MAX(B.U_PEAerosaculitisG2) SemAerosaculitisG2 
,MAX(B.U_PECojera) SemCojera 
,MAX(B.U_PEHigadoIcterico) SemHigadoIcterico 
,MAX(B.U_PEMaterialCaseoso_po1ra) SemMaterialCaseoso_po1ra 
,MAX(B.U_PEMaterialCaseosoMedRetr) SemMaterialCaseosoMedRetr 
,MAX(B.U_PENecrosisHepatica) SemNecrosisHepatica 
,MAX(B.U_PENeumonia) SemNeumonia 
,MAX(B.U_PESepticemia) SemSepticemia 
,MAX(B.U_PEVomitoNegro) SemVomitoNegro 
,MAX(B.U_PEAsperguillius) SemAsperguillius 
,MAX(B.U_PEBazoGrandeMot) SemBazoGrandeMot 
,MAX(B.U_PECorazonGrande) SemCorazonGrande 
,MAX(B.U_PECuadroToxico) SemCuadroToxico 
from {database_name_gl}.ft_mortalidad_diario a 
left join {database_name_tmp}.LesionLoteSemanal b on substring(A.complexentityno,1,(length(A.complexentityno)-6)) = b.complexentityno and 
a.pk_semanavida = b.pk_semanavida and b.pk_tipoproducto = a.pk_tipoproducto 
left join (select pk_lote,max(PobInicial) PobInicial from {database_name_gl}.ft_mortalidad_lote  group by pk_lote) e on a.pk_lote = e.pk_lote 
left join {database_name_tmp}.listaMortalidadDiarioCatTemp F on F.pk_lote = A.pk_lote 
where a.flagartificio = 1 and a.pk_empresa = 1 and a.pk_division = 4
and (date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))
and a.pk_lote not in (select pk_lote from {database_name_gl}.lk_lote where (Upper(clote) like 'P186%' AND SUBSTRING(clote,8,4) >= '11'))
group by a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote,a.pk_especie,a.pk_estado,a.pk_tipoproducto,a.pk_administrador,a.pk_proveedor,a.pk_semanavida, 
substring(A.complexentityno,1,(length(A.complexentityno)-6))""")



# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLoteSemanal"
}
df_MortalidadLoteSemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadLoteSemanal")
print('carga MortalidadLoteSemanal', df_MortalidadLoteSemanalTemp.count())
df_MortalidadLoteSemanalCompleto = spark.sql(f"""select \
a.pk_tiempo
,a.fecha
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_especie 
,a.pk_estado
,a.pk_tipoproducto
,a.pk_administrador
,a.pk_proveedor
,a.pk_semanavida
,a.complexentityno 
,max(c.fecha) as FechaIngreso 
,max(e.descripfecha) as FechaCierre 
,d.nsemana as SemanaCalendario 
,max(a.PobInicial)PobInicial 
,max(a.MortDia) MortDia 
,sum(b.MortDia) MortDiaAcum 
,case when max(a.PobInicial) = 0 then 0 else max(a.MortDia)/max(a.PobInicial*1.0) end PorcMortDia 
,case when max(a.PobInicial) = 0 then 0 else sum(b.MortDia)/max(a.PobInicial*1.0) end PorcMortDiaAcum 
,max(a.U_PEAccidentados) as U_PEAccidentados 
,max(a.U_PEHigadoGraso) as U_PEHigadoGraso 
,max(a.U_PEHepatomegalia) as U_PEHepatomegalia 
,max(a.U_PEHigadoHemorragico) as U_PEHigadoHemorragico 
,max(a.U_PEInanicion) as U_PEInanicion 
,max(a.U_PEProblemaRespiratorio) as U_PEProblemaRespiratorio 
,max(a.U_PESCH) as U_PESCH 
,max(a.U_PEEnteritis) as U_PEEnteritis 
,max(a.U_PEAscitis) as U_PEAscitis 
,max(a.U_PEMuerteSubita) as U_PEMuerteSubita 
,max(a.U_PEEstresPorCalor) as U_PEEstresPorCalor 
,max(a.U_PEHidropericardio) as U_PEHidropericardio 
,max(a.U_PEHemopericardio) as U_PEHemopericardio 
,max(a.U_PEUratosis) as U_PEUratosis 
,max(a.U_PEMaterialCaseoso) as U_PEMaterialCaseoso 
,max(a.U_PEOnfalitis) as U_PEOnfalitis 
,max(a.U_PERetencionDeYema) as U_PERetencionDeYema 
,max(a.U_PEErosionDeMolleja) as U_PEErosionDeMolleja 
,max(a.U_PEHemorragiaMusculos) as U_PEHemorragiaMusculos 
,max(a.U_PESangreEnCiego) as U_PESangreEnCiego 
,max(a.U_PEPericarditis) as U_PEPericarditis 
,max(a.U_PEPeritonitis) as U_PEPeritonitis 
,max(a.U_PEProlapso) as U_PEProlapso 
,max(a.U_PEPicaje) as U_PEPicaje 
,max(a.U_PERupturaAortica) as U_PERupturaAortica 
,max(a.U_PEBazoMoteado) as U_PEBazoMoteado 
,max(a.U_PENoViable) as U_PENoViable 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEAccidentados)/max(a.PobInicial*1.0))*1000)/10 end as PorcAccidentados 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHigadoGraso)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoGraso 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHepatomegalia)/max(a.PobInicial*1.0))*1000)/10 end as PorcHepatomegalia 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHigadoHemorragico)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoHemorragico 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEInanicion)/max(a.PobInicial*1.0))*1000)/10 end as PorcInanicion 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEProblemaRespiratorio)/max(a.PobInicial*1.0))*1000)/10 end as PorcProblemaRespiratorio 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PESCH)/max(a.PobInicial*1.0))*1000)/10 end as PorcSCH 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEEnteritis)/max(a.PobInicial*1.0))*1000)/10 end as PorcEnteritis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEAscitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcAscitis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEMuerteSubita)/max(a.PobInicial*1.0))*1000)/10 end as PorcMuerteSubita 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEEstresPorCalor)/max(a.PobInicial*1.0))*1000)/10 end as PorcEstresPorCalor 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHidropericardio)/max(a.PobInicial*1.0))*1000)/10 end as PorcHidropericardio 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHemopericardio)/max(a.PobInicial*1.0))*1000)/10 end as PorcHemopericardio 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEUratosis)/max(a.PobInicial*1.0))*1000)/10 end as PorcUratosis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEMaterialCaseoso)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseoso 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEOnfalitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcOnfalitis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PERetencionDeYema)/max(a.PobInicial*1.0))*1000)/10 end as PorcRetencionDeYema 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEErosionDeMolleja)/max(a.PobInicial*1.0))*1000)/10 end as PorcErosionDeMolleja 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHemorragiaMusculos)/max(a.PobInicial*1.0))*1000)/10 end as PorcHemorragiaMusculos 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PESangreEnCiego)/max(a.PobInicial*1.0))*1000)/10 end as PorcSangreEnCiego 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEPericarditis)/max(a.PobInicial*1.0))*1000)/10 end as PorcPericarditis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEPeritonitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcPeritonitis 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEProlapso)/max(a.PobInicial*1.0))*1000)/10 end as PorcProlapso 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEPicaje)/max(a.PobInicial*1.0))*1000)/10 end as PorcPicaje 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PERupturaAortica)/max(a.PobInicial*1.0))*1000)/10 end as PorcRupturaAortica 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEBazoMoteado)/max(a.PobInicial*1.0))*1000)/10 end as PorcBazoMoteado 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PENoViable)/max(a.PobInicial*1.0))*1000)/10 end as PorcNoViable 
,sum(b.U_PEAccidentados) as U_PEAccidentadosAcum 
,sum(b.U_PEHigadoGraso) as U_PEHigadoGrasoAcum 
,sum(b.U_PEHepatomegalia) as U_PEHepatomegaliaAcum 
,sum(b.U_PEHigadoHemorragico) as U_PEHigadoHemorragicoAcum 
,sum(b.U_PEInanicion) as U_PEInanicionAcum 
,sum(b.U_PEProblemaRespiratorio) as U_PEProblemaRespiratorioAcum 
,sum(b.U_PESCH) as U_PESCHAcum 
,sum(b.U_PEEnteritis) as U_PEEnteritisAcum 
,sum(b.U_PEAscitis) as U_PEAscitisAcum 
,sum(b.U_PEMuerteSubita) as U_PEMuerteSubitaAcum 
,sum(b.U_PEEstresPorCalor) as U_PEEstresPorCalorAcum 
,sum(b.U_PEHidropericardio) as U_PEHidropericardioAcum 
,sum(b.U_PEHemopericardio) as U_PEHemopericardioAcum 
,sum(b.U_PEUratosis) as U_PEUratosisAcum 
,sum(b.U_PEMaterialCaseoso) as U_PEMaterialCaseosoAcum 
,sum(b.U_PEOnfalitis) as U_PEOnfalitisAcum 
,sum(b.U_PERetencionDeYema) as U_PERetencionDeYemaAcum 
,sum(b.U_PEErosionDeMolleja) as U_PEErosionDeMollejaAcum 
,sum(b.U_PEHemorragiaMusculos) as U_PEHemorragiaMusculosAcum 
,sum(b.U_PESangreEnCiego) as U_PESangreEnCiegoAcum 
,sum(b.U_PEPericarditis) as U_PEPericarditisAcum 
,sum(b.U_PEPeritonitis) as U_PEPeritonitisAcum 
,sum(b.U_PEProlapso) as U_PEProlapsoAcum 
,sum(b.U_PEPicaje) as U_PEPicajeAcum 
,sum(b.U_PERupturaAortica) as U_PERupturaAorticaAcum 
,sum(b.U_PEBazoMoteado) as U_PEBazoMoteadoAcum 
,sum(b.U_PENoViable) as U_PENoViableAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEAccidentados)/max(a.PobInicial*1.0))*1000)/10 end as PorcAccidentadosAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHigadoGraso)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoGrasoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHepatomegalia)/max(a.PobInicial*1.0))*1000)/10 end as PorcHepatomegaliaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHigadoHemorragico)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoHemorragicoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEInanicion)/max(a.PobInicial*1.0))*1000)/10 end as PorcInanicionAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEProblemaRespiratorio)/max(a.PobInicial*1.0))*1000)/10 end as PorcProblemaRespiratorioAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PESCH)/max(a.PobInicial*1.0))*1000)/10 end as PorcSCHAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEEnteritis)/max(a.PobInicial*1.0))*1000)/10 end as PorcEnteritisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEAscitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcAscitisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEMuerteSubita)/max(a.PobInicial*1.0))*1000)/10 end as PorcMuerteSubitaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEEstresPorCalor)/max(a.PobInicial*1.0))*1000)/10 end as PorcEstresPorCalorAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHidropericardio)/max(a.PobInicial*1.0))*1000)/10 end as PorcHidropericardioAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHemopericardio)/max(a.PobInicial*1.0))*1000)/10 end as PorcHemopericardioAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEUratosis)/max(a.PobInicial*1.0))*1000)/10 end as PorcUratosisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEMaterialCaseoso)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseosoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEOnfalitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcOnfalitisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PERetencionDeYema)/max(a.PobInicial*1.0))*1000)/10 end as PorcRetencionDeYemaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEErosionDeMolleja)/max(a.PobInicial*1.0))*1000)/10 end as PorcErosionDeMollejaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHemorragiaMusculos)/max(a.PobInicial*1.0))*1000)/10 end as PorcHemorragiaMusculosAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PESangreEnCiego)/max(a.PobInicial*1.0))*1000)/10 end as PorcSangreEnCiegoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEPericarditis)/max(a.PobInicial*1.0))*1000)/10 end as PorcPericarditisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEPeritonitis)/max(a.PobInicial*1.0))*1000)/10 end as PorcPeritonitisAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEProlapso)/max(a.PobInicial*1.0))*1000)/10 end as PorcProlapsoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEPicaje)/max(a.PobInicial*1.0))*1000)/10 end as PorcPicajeAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PERupturaAortica)/max(a.PobInicial*1.0))*1000)/10 end as PorcRupturaAorticaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEBazoMoteado)/max(a.PobInicial*1.0))*1000)/10 end as PorcBazoMoteadoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PENoViable)/max(a.PobInicial*1.0))*1000)/10 end as PorcNoViableAcum 
,1 as flagartificio 
,a.categoria 
,max(a.U_PEAerosaculitisG2) as U_PEAerosaculitisG2,max(a.U_PECojera) as U_PECojera 
,max(a.U_PEHigadoIcterico) as U_PEHigadoIcterico,max(a.U_PEMaterialCaseoso_po1ra) as U_PEMaterialCaseoso_po1ra 
,max(a.U_PEMaterialCaseosoMedRetr) as U_PEMaterialCaseosoMedRetr,max(a.U_PENecrosisHepatica) as U_PENecrosisHepatica 
,max(a.U_PENeumonia) as U_PENeumonia,max(a.U_PESepticemia) as U_PESepticemia,max(a.U_PEVomitoNegro) as U_PEVomitoNegro 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEAerosaculitisG2)/max(a.PobInicial*1.0))*1000)/10 end as PorcAerosaculitisG2 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PECojera)/max(a.PobInicial*1.0))*1000)/10 end as PorcCojera 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEHigadoIcterico)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoIcterico 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEMaterialCaseoso_po1ra)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseoso_po1ra 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEMaterialCaseosoMedRetr)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseosoMedRetr 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PENecrosisHepatica)/max(a.PobInicial*1.0))*1000)/10 end as PorcNecrosisHepatic 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PENeumonia)/max(a.PobInicial*1.0))*1000)/10 end as PorcNeumonia 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PESepticemia)/max(a.PobInicial*1.0))*1000)/10 end as PorcSepticemia 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEVomitoNegro)/max(a.PobInicial*1.0))*1000)/10 end as PorcVomitoNegro 
,sum(b.U_PEAerosaculitisG2) as U_PEAerosaculitisG2Acum,sum(b.U_PECojera) as U_PECojeraAcum,sum(b.U_PEHigadoIcterico) as U_PEHigadoIctericoAcum 
,sum(b.U_PEMaterialCaseoso_po1ra) as U_PEMaterialCaseoso_po1raAcum,sum(b.U_PEMaterialCaseosoMedRetr) as U_PEMaterialCaseosoMedRetrAcum 
,sum(b.U_PENecrosisHepatica) as U_PENecrosisHepaticAcum 
,sum(b.U_PENeumonia) as U_PENeumoniaAcum,sum(b.U_PESepticemia) as U_PESepticemiaAcum,sum(b.U_PEVomitoNegro) as U_PEVomitoNegroAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEAerosaculitisG2)/max(a.PobInicial*1.0))*1000)/10 end as PorcAerosaculitisG2Acum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PECojera)/max(a.PobInicial*1.0))*1000)/10 end as PorcCojeraAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEHigadoIcterico)/max(a.PobInicial*1.0))*1000)/10 end as PorcHigadoIctericoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEMaterialCaseoso_po1ra)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseoso_po1raAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEMaterialCaseosoMedRetr)/max(a.PobInicial*1.0))*1000)/10 end as PorcMaterialCaseosoMedRetrAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PENecrosisHepatica)/max(a.PobInicial*1.0))*1000)/10 end as PorcNecrosisHepaticAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PENeumonia)/max(a.PobInicial*1.0))*1000)/10 end as PorcNeumoniaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PESepticemia)/max(a.PobInicial*1.0))*1000)/10 end as PorcSepticemiaAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEVomitoNegro)/max(a.PobInicial*1.0))*1000)/10 end as PorcVomitoNegroAcum 
,max(a.U_PEAsperguillius) as U_PEAsperguillius,max(a.U_PEBazoGrandeMot) as U_PEBazoGrandeMot ,max(a.U_PECorazonGrande) as U_PECorazonGrande 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEAsperguillius)/max(a.PobInicial*1.0))*1000)/10 end as PorcAsperguillius 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PEBazoGrandeMot)/max(a.PobInicial*1.0))*1000)/10 end as PorcBazoGrandeMot 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PECorazonGrande)/max(a.PobInicial*1.0))*1000)/10 end as PorcCorazonGrande 
,sum(b.U_PEAsperguillius) as U_PEAsperguilliusAcum,sum(b.U_PEBazoGrandeMot) as U_PEBazoGrandeMotAcum,sum(b.U_PECorazonGrande) as U_PECorazonGrandeAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEAsperguillius)/max(a.PobInicial*1.0))*1000)/10 end as PorcAsperguilliusAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PEBazoGrandeMot)/max(a.PobInicial*1.0))*1000)/10 end as PorcBazoGrandeMotAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PECorazonGrande)/max(a.PobInicial*1.0))*1000)/10 end as PorcCorazonGrandeAcum 
,max(a.U_PECuadroToxico) as U_PECuadroToxico 
,case when max(a.PobInicial)= 0 then 0 else ((max(a.U_PECuadroToxico)/max(a.PobInicial*1.0))*1000)/10 end as PorcCuadroToxico 
,sum(b.U_PECuadroToxico) as U_PECuadroToxicoAcum 
,case when max(a.PobInicial)= 0 then 0 else ((sum(b.U_PECuadroToxico)/max(a.PobInicial*1.0))*1000)/10 end as PorcCuadroToxicoAcum 
,MAX(a.SemAccidentados) SemAccidentados,MAX(a.SemHigadoGraso) SemHigadoGraso 
,MAX(a.SemHepatomegalia) SemHepatomegalia,MAX(a.SemHigadoHemorragico) SemHigadoHemorragico 
,MAX(a.SemInanicion) SemInanicion,MAX(a.SemProblemaRespiratorio) SemProblemaRespiratorio 
,MAX(a.SemSCH) SemSCH,MAX(a.SemEnteritis) SemEnteritis 
,MAX(a.SemAscitis) SemAscitis,MAX(a.SemMuerteSubita) SemMuerteSubita 
,MAX(a.SemEstresPorCalor) SemEstresPorCalor,MAX(a.SemHidropericardio) SemHidropericardio 
,MAX(a.SemHemopericardio) SemHemopericardio,MAX(a.SemUratosis) SemUratosis 
,MAX(a.SemMaterialCaseoso) SemMaterialCaseoso,MAX(a.SemOnfalitis) SemOnfalitis 
,MAX(a.SemRetencionDeYema) SemRetencionDeYema,MAX(a.SemErosionDeMolleja) SemErosionDeMolleja 
,MAX(a.SemHemorragiaMusculos) SemHemorragiaMusculos,MAX(a.SemSangreEnCiego) SemSangreEnCiego 
,MAX(a.SemPericarditis) SemPericarditis,MAX(a.SemPeritonitis) SemPeritonitis 
,MAX(a.SemProlapso) SemProlapso,MAX(a.SemPicaje) SemPicaje 
,MAX(a.SemRupturaAortica) SemRupturaAortica,MAX(a.SemBazoMoteado) SemBazoMoteado 
,MAX(a.SemNoViable) SemNoViable,MAX(a.SemAerosaculitisG2) SemAerosaculitisG2 
,MAX(a.SemCojera) SemCojera,MAX(a.SemHigadoIcterico) SemHigadoIcterico 
,MAX(a.SemMaterialCaseoso_po1ra) SemMaterialCaseoso_po1ra,MAX(a.SemMaterialCaseosoMedRetr) SemMaterialCaseosoMedRetr 
,MAX(a.SemNecrosisHepatica) SemNecrosisHepatica,MAX(a.SemNeumonia) SemNeumonia,MAX(a.SemSepticemia) SemSepticemia 
,MAX(a.SemVomitoNegro) SemVomitoNegro,MAX(a.SemAsperguillius) SemAsperguillius 
,MAX(a.SemBazoGrandeMot) SemBazoGrandeMot,MAX(a.SemCorazonGrande) SemCorazonGrande 
,MAX(a.SemCuadroToxico) SemCuadroToxico 
,((MAX(a.SemAccidentados)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcAccidentados 
,((MAX(a.SemHigadoGraso)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHigadoGraso 
,((MAX(a.SemHepatomegalia)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHepatomegalia 
,((MAX(a.SemHigadoHemorragico)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHigadoHemorragico 
,((MAX(a.SemInanicion)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcInanicion 
,((MAX(a.SemProblemaRespiratorio)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcProblemaRespiratorio 
,((MAX(a.SemSCH)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcSCH 
,((MAX(a.SemEnteritis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcEnteritis 
,((MAX(a.SemAscitis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcAscitis 
,((MAX(a.SemMuerteSubita)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcMuerteSubita 
,((MAX(a.SemEstresPorCalor)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcEstresPorCalor 
,((MAX(a.SemHidropericardio)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHidropericardio 
,((MAX(a.SemHemopericardio)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHemopericardio 
,((MAX(a.SemUratosis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcUratosis 
,((MAX(a.SemMaterialCaseoso)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcMaterialCaseoso 
,((MAX(a.SemOnfalitis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcOnfalitis 
,((MAX(a.SemRetencionDeYema)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcRetencionDeYema 
,((MAX(a.SemErosionDeMolleja)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcErosionDeMolleja 
,((MAX(a.SemHemorragiaMusculos)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHemorragiaMusculos 
,((MAX(a.SemSangreEnCiego)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcSangreEnCiego 
,((MAX(a.SemPericarditis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcPericarditis 
,((MAX(a.SemPeritonitis)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcPeritonitis 
,((MAX(a.SemProlapso)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcProlapso 
,((MAX(a.SemPicaje)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcPicaje 
,((MAX(a.SemRupturaAortica)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcRupturaAortica 
,((MAX(a.SemBazoMoteado)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcBazoMoteado 
,((MAX(a.SemNoViable)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcNoViable 
,((MAX(a.SemAerosaculitisG2)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcAerosaculitisG2 
,((MAX(a.SemCojera)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcCojera 
,((MAX(a.SemHigadoIcterico)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcHigadoIcterico 
,((MAX(a.SemMaterialCaseoso_po1ra)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcMaterialCaseoso_po1ra 
,((MAX(a.SemMaterialCaseosoMedRetr)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcMaterialCaseosoMedRetr 
,((MAX(a.SemNecrosisHepatica)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcNecrosisHepatica 
,((MAX(a.SemNeumonia)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcNeumonia 
,((MAX(a.SemSepticemia)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcSepticemia 
,((MAX(a.SemVomitoNegro)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcVomitoNegro 
,((MAX(a.SemAsperguillius)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcAsperguillius 
,((MAX(a.SemBazoGrandeMot)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcBazoGrandeMot 
,((MAX(a.SemCorazonGrande)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcCorazonGrande 
,((MAX(a.SemCuadroToxico)/(NULLIF(MAX(a.PobInicial*1.0),0)))*1000)/10 SemPorcCuadroToxico 
from {database_name_tmp}.MortalidadLoteSemanal a 
left join {database_name_tmp}.MortalidadLoteSemanal b on a.complexentityno = b.complexentityno  
and b.pk_semanavida <= a.pk_semanavida 
left join (select substring(complexentityno,1,(length(complexentityno)-6)) complexentityno,max(t.pk_tiempo) pk_tiempo,max(t.descripfecha) fecha 
            from {database_name_gl}.ft_ingresocons t 
            group by substring(complexentityno,1,(length(complexentityno)-6)) 
          ) c on c.complexentityno = a.complexentityno 
left join {database_name_gl}.lk_tiempo d on c.pk_tiempo = d.pk_tiempo 
left join {database_name_gl}.ft_mortalidad_lote e on a.pk_lote = e.pk_lote and flagartatipico = 2 
where (date_format(a.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
and a.pk_empresa = 1 and a.pk_division = 4 
and e.pk_lote not in (select pk_lote from {database_name_gl}.lk_lote where (Upper(clote) like 'P186%' AND SUBSTRING(clote,8,4) >= '11'))
group by a.pk_tiempo,a.fecha,a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote,a.pk_especie,a.pk_estado,a.pk_tipoproducto,a.pk_administrador 
,a.pk_proveedor,a.pk_semanavida,a.complexentityno,d.nsemana,a.categoria""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadLoteSemanalCompleto"
}
df_MortalidadLoteSemanalCompleto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadLoteSemanalCompleto")
print('carga MortalidadLoteSemanalCompleto', df_MortalidadLoteSemanalCompleto.count())
df_LesionesMortalidadLoteSemanalTemp = spark.sql(f"select \
MAX(pk_tiempo) pk_tiempo,MAX(fecha) fecha,pk_empresa,pk_division,pk_semanavida,complexentityno, \
SUM(MortDia) MortDia,MAX(MortDiaAcum) MortDiaAcum,MAX(PobInicial)PobInicial, \
SUM(U_PEAccidentados) U_PEAccidentados,SUM(U_PEAscitis) U_PEAscitis,SUM(U_PEBazoMoteado) U_PEBazoMoteado, \
SUM(U_PEEnteritis) U_PEEnteritis,SUM(U_PEErosionDeMolleja) U_PEErosionDeMolleja, \
SUM(U_PEEstresPorCalor) U_PEEstresPorCalor,SUM(U_PEHemopericardio) U_PEHemopericardio, \
SUM(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos,SUM(U_PEHepatomegalia) U_PEHepatomegalia, \
SUM(U_PEHidropericardio) U_PEHidropericardio,SUM(U_PEHigadoGraso) U_PEHigadoGraso, \
SUM(U_PEHigadoHemorragico) U_PEHigadoHemorragico,SUM(U_PEInanicion) U_PEInanicion, \
SUM(U_PEMaterialCaseoso) U_PEMaterialCaseoso,SUM(U_PEMuerteSubita) U_PEMuerteSubita, \
SUM(U_PENoViable) U_PENoViable,SUM(U_PEOnfalitis) U_PEOnfalitis, \
SUM(U_PEPericarditis) U_PEPericarditis,SUM(U_PEPeritonitis) U_PEPeritonitis, \
SUM(U_PEPicaje) U_PEPicaje,SUM(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
SUM(U_PEProlapso) U_PEProlapso,SUM(U_PERetencionDeYema) U_PERetencionDeYema, \
SUM(U_PERupturaAortica) U_PERupturaAortica,SUM(U_PESangreEnCiego) U_PESangreEnCiego, \
SUM(U_PESCH) U_PESCH,SUM(U_PEUratosis) U_PEUratosis, \
SUM(U_PEAerosaculitisG2) U_PEAerosaculitisG2,SUM(U_PECojera) U_PECojera, \
SUM(U_PEHigadoIcterico) U_PEHigadoIcterico,SUM(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra, \
SUM(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr,SUM(U_PENecrosisHepatica) U_PENecrosisHepatica, \
SUM(U_PENeumonia) U_PENeumonia,SUM(U_PESepticemia) U_PESepticemia, \
SUM(U_PEVomitoNegro) U_PEVomitoNegro,SUM(U_PEAsperguillius) U_PEAsperguillius, \
SUM(U_PEBazoGrandeMot) U_PEBazoGrandeMot,SUM(U_PECorazonGrande) U_PECorazonGrande, \
SUM(U_PECuadroToxico) U_PECuadroToxico, \
MAX(U_PEAccidentadosAcum) AcumPEAccidentados,MAX(U_PEAscitisAcum) AcumPEAscitis, \
MAX(U_PEBazoMoteadoAcum) AcumPEBazoMoteado,MAX(U_PEEnteritisAcum) AcumPEEnteritis, \
MAX(U_PEErosionDeMollejaAcum) AcumPEErosionDeMolleja,MAX(U_PEEstresPorCalorAcum) AcumPEEstresPorCalor, \
MAX(U_PEHemopericardioAcum) AcumPEHemopericardio,MAX(U_PEHemorragiaMusculosAcum) AcumPEHemorragiaMusculos, \
MAX(U_PEHepatomegaliaAcum) AcumPEHepatomegalia,MAX(U_PEHidropericardioAcum) AcumPEHidropericardio, \
MAX(U_PEHigadoGrasoAcum) AcumPEHigadoGraso,MAX(U_PEHigadoHemorragicoAcum) AcumPEHigadoHemorragico, \
MAX(U_PEInanicionAcum) AcumPEInanicion,MAX(U_PEMaterialCaseosoAcum) AcumPEMaterialCaseoso, \
MAX(U_PEMuerteSubitaAcum) AcumPEMuerteSubita,MAX(U_PENoViableAcum) AcumPENoViable, \
MAX(U_PEOnfalitisAcum) AcumPEOnfalitis,MAX(U_PEPericarditisAcum) AcumPEPericarditis, \
MAX(U_PEPeritonitisAcum) AcumPEPeritonitis,MAX(U_PEPicajeAcum) AcumPEPicaje, \
MAX(U_PEProblemaRespiratorioAcum) AcumPEProblemaRespiratorio,MAX(U_PEProlapsoAcum) AcumPEProlapso, \
MAX(U_PERetencionDeYemaAcum) AcumPERetencionDeYema,MAX(U_PERupturaAorticaAcum) AcumPERupturaAortica, \
MAX(U_PESangreEnCiegoAcum) AcumPESangreEnCiego,MAX(U_PESCHAcum) AcumPESCH, \
MAX(U_PEUratosisAcum) AcumPEUratosis,MAX(U_PEAerosaculitisG2Acum) AcumPEAerosaculitisG2, \
MAX(U_PECojeraAcum) AcumPECojera,MAX(U_PEHigadoIctericoAcum) AcumPEHigadoIcterico, \
MAX(U_PEMaterialCaseoso_po1raAcum) AcumPEMaterialCaseoso_po1ra,MAX(U_PEMaterialCaseosoMedRetrAcum) AcumPEMaterialCaseosoMedRetr, \
MAX(U_PENecrosisHepaticAcum) AcumPENecrosisHepatica, \
MAX(U_PENeumoniaAcum) AcumPENeumonia,MAX(U_PESepticemiaAcum) AcumPESepticemia, \
MAX(U_PEVomitoNegroAcum) AcumPEVomitoNegro, \
MAX(U_PEAsperguilliusAcum) AcumPEAsperguillius,MAX(U_PEBazoGrandeMotAcum) AcumPEBazoGrandeMot, \
MAX(U_PECorazonGrandeAcum) AcumPECorazonGrande,MAX(U_PECuadroToxicoAcum) AcumPECuadroToxico \
from {database_name_tmp}.MortalidadLoteSemanalCompleto \
GROUP BY pk_empresa,pk_division,pk_semanavida,complexentityno")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LesionesMortalidadLoteSemanal"
}
df_LesionesMortalidadLoteSemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.LesionesMortalidadLoteSemanal")
print('carga LesionesMortalidadLoteSemanal', df_LesionesMortalidadLoteSemanalTemp.count())
df_OrdenarMortalidadesTemp = spark.sql(f"""
SELECT
    *,
    ROUND((cmortalidad / NULLIF(PobInicial * 1.0, 0) * 100), 5) AS pcmortalidad,
    ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, fecha ORDER BY cmortalidad DESC) AS orden,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 1 THEN 'CantMortSemLesionUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 2 THEN 'CantMortSemLesionDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 3 THEN 'CantMortSemLesionTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) >= 4 THEN 'CantMortSemLesionOtros'
    END AS OrdenCantidad,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 1 THEN 'DescripMortSemLesionUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 2 THEN 'DescripMortSemLesionDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 3 THEN 'DescripMortSemLesionTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) >= 4 THEN 'DescripMortSemLesionOtros'
    END AS OrdenNombre,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 1 THEN 'PorcMortSemLesionUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 2 THEN 'PorcMortSemLesionDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) = 3 THEN 'PorcMortSemLesionTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidad DESC) >= 4 THEN 'PorcMortSemLesionOtros'
    END AS OrdenPorcentaje
FROM
(
select * ,
    stack(40,
        'U_PEAccidentados', U_PEAccidentados,
        'U_PEAscitis', U_PEAscitis,
        'U_PEBazoMoteado', U_PEBazoMoteado,
        'U_PEEnteritis', U_PEEnteritis,
        'U_PEErosionDeMolleja', U_PEErosionDeMolleja,
        'U_PEEstresPorCalor', U_PEEstresPorCalor,
        'U_PEHemopericardio', U_PEHemopericardio,
        'U_PEHemorragiaMusculos', U_PEHemorragiaMusculos,
        'U_PEHepatomegalia', U_PEHepatomegalia,
        'U_PEHidropericardio', U_PEHidropericardio,
        'U_PEHigadoGraso', U_PEHigadoGraso,
        'U_PEHigadoHemorragico', U_PEHigadoHemorragico,
        'U_PEInanicion', U_PEInanicion,
        'U_PEMaterialCaseoso', U_PEMaterialCaseoso,
        'U_PEMuerteSubita', U_PEMuerteSubita,
        'U_PENoViable', U_PENoViable,
        'U_PEOnfalitis', U_PEOnfalitis,
        'U_PEPericarditis', U_PEPericarditis,
        'U_PEPeritonitis', U_PEPeritonitis,
        'U_PEPicaje', U_PEPicaje,
        'U_PEProblemaRespiratorio', U_PEProblemaRespiratorio,
        'U_PEProlapso', U_PEProlapso,
        'U_PERetencionDeYema', U_PERetencionDeYema,
        'U_PERupturaAortica', U_PERupturaAortica,
        'U_PESangreEnCieGO', U_PESangreEnCieGO,
        'U_PESCH', U_PESCH,
        'U_PEUratosis', U_PEUratosis,
        'U_PEAerosaculitisG2', U_PEAerosaculitisG2,
        'U_PECojera', U_PECojera,
        'U_PEHigadoIcterico', U_PEHigadoIcterico,
        'U_PEMaterialCaseoso_po1ra', U_PEMaterialCaseoso_po1ra,
        'U_PEMaterialCaseosoMedRetr', U_PEMaterialCaseosoMedRetr,
        'U_PENecrosisHepatica', U_PENecrosisHepatica,
        'U_PENeumonia', U_PENeumonia,
        'U_PESepticemia', U_PESepticemia,
        'U_PEVomitoNegro', U_PEVomitoNegro,
        'U_PEAsperguillius', U_PEAsperguillius,
        'U_PEBazoGrandeMot', U_PEBazoGrandeMot,
        'U_PECorazonGrande', U_PECorazonGrande,
        'U_PECuadroToxico', U_PECuadroToxico
    ) AS (causa, cmortalidad)
from {database_name_tmp}.LesionesMortalidadLoteSemanal
)
WHERE
    cmortalidad <> 0
ORDER BY
    pk_semanavida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidades"
}
df_OrdenarMortalidadesTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidades")
print('carga OrdenarMortalidades', df_OrdenarMortalidadesTemp.count())
df_OrdenarMortalidades2Temp = spark.sql(f"""
SELECT
    A.pk_semanavida,
    A.ComplexEntityNo,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionUno' THEN P1.cmortalidad ELSE NULL END) AS CantMortSemLesionUno,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionDos' THEN P1.cmortalidad ELSE NULL END) AS CantMortSemLesionDos,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionTres' THEN P1.cmortalidad ELSE NULL END) AS CantMortSemLesionTres,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionOtros' THEN P1.cmortalidad ELSE NULL END) AS CantMortSemLesionOtros,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionUno' THEN P2.pcmortalidad ELSE NULL END) AS PorcMortSemLesionUno,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionDos' THEN P2.pcmortalidad ELSE NULL END) AS PorcMortSemLesionDos,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionTres' THEN P2.pcmortalidad ELSE NULL END) AS PorcMortSemLesionTres,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionOtros' THEN P2.pcmortalidad ELSE NULL END) AS PorcMortSemLesionOtros,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionUno' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionUno,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionDos' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionDos,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionTres' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionTres,
    'Otros' AS DescripMortSemLesionOtros
FROM
    (SELECT pk_semanavida, complexentityno FROM {database_name_tmp}.OrdenarMortalidades GROUP BY pk_semanavida, complexentityno) AS A
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, AVG(cmortalidad) AS cmortalidad, ordenCantidad FROM {database_name_tmp}.OrdenarMortalidades GROUP BY pk_semanavida, complexentityno, ordenCantidad) AS P1
    ON A.ComplexEntityNo = P1.ComplexEntityNo AND A.pk_semanavida = P1.pk_semanavida
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, AVG(pcmortalidad) AS pcmortalidad, ordenPorcentaje FROM {database_name_tmp}.OrdenarMortalidades GROUP BY pk_semanavida, complexentityno, ordenPorcentaje) AS P2
    ON A.ComplexEntityNo = P2.ComplexEntityNo AND A.pk_semanavida = P2.pk_semanavida
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, MAX(causa) AS causa, ordenNombre FROM {database_name_tmp}.OrdenarMortalidades GROUP BY pk_semanavida, complexentityno, ordenNombre) AS P3
    ON A.ComplexEntityNo = P3.ComplexEntityNo AND A.pk_semanavida = P3.pk_semanavida
GROUP BY
    A.pk_semanavida, A.ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidades2"
}
df_OrdenarMortalidades2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidades2")
print('carga OrdenarMortalidades2', df_OrdenarMortalidades2Temp.count())
df_OrdenarMortalidadesAcumTemp = spark.sql(f"""
SELECT
    *,
    ROUND((cmortalidadacum / NULLIF(PobInicial * 1.0, 0) * 100), 5) AS pcmortalidadacum,
    ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_tiempo ORDER BY cmortalidadacum DESC) AS orden,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 1 THEN 'CantMortSemLesionAcumUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 2 THEN 'CantMortSemLesionAcumDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 3 THEN 'CantMortSemLesionAcumTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) >= 4 THEN 'CantMortSemLesionAcumOtros'
    END AS OrdenCantidad,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 1 THEN 'DescripMortSemLesionAcumUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 2 THEN 'DescripMortSemLesionAcumDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 3 THEN 'DescripMortSemLesionAcumTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) >= 4 THEN 'DescripMortSemLesionAcumOtros'
    END AS OrdenNombre,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 1 THEN 'PorcMortSemLesionAcumUno'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 2 THEN 'PorcMortSemLesionAcumDos'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) = 3 THEN 'PorcMortSemLesionAcumTres'
        WHEN ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY cmortalidadacum DESC) >= 4 THEN 'PorcMortSemLesionAcumOtros'
    END AS OrdenPorcentaje
FROM 
( 
select * ,
    stack(40,
        'AcumPEAccidentados', AcumPEAccidentados,
        'AcumPEAscitis', AcumPEAscitis,
        'AcumPEBazoMoteado', AcumPEBazoMoteado,
        'AcumPEEnteritis', AcumPEEnteritis,
        'AcumPEErosionDeMolleja', AcumPEErosionDeMolleja,
        'AcumPEEstresPorCalor', AcumPEEstresPorCalor,
        'AcumPEHemopericardio', AcumPEHemopericardio,
        'AcumPEHemorragiaMusculos', AcumPEHemorragiaMusculos,
        'AcumPEHepatomegalia', AcumPEHepatomegalia,
        'AcumPEHidropericardio', AcumPEHidropericardio,
        'AcumPEHigadoGraso', AcumPEHigadoGraso,
        'AcumPEHigadoHemorragico', AcumPEHigadoHemorragico,
        'AcumPEInanicion', AcumPEInanicion,
        'AcumPEMaterialCaseoso', AcumPEMaterialCaseoso,
        'AcumPEMuerteSubita', AcumPEMuerteSubita,
        'AcumPENoViable', AcumPENoViable,
        'AcumPEOnfalitis', AcumPEOnfalitis,
        'AcumPEPericarditis', AcumPEPericarditis,
        'AcumPEPeritonitis', AcumPEPeritonitis,
        'AcumPEPicaje', AcumPEPicaje,
        'AcumPEProblemaRespiratorio', AcumPEProblemaRespiratorio,
        'AcumPEProlapso', AcumPEProlapso,
        'AcumPERetencionDeYema', AcumPERetencionDeYema,
        'AcumPERupturaAortica', AcumPERupturaAortica,
        'AcumPESangreEnCieGO', AcumPESangreEnCieGO,
        'AcumPESCH', AcumPESCH,
        'AcumPEUratosis', AcumPEUratosis,
        'AcumPEAerosaculitisG2', AcumPEAerosaculitisG2,
        'AcumPECojera', AcumPECojera,
        'AcumPEHigadoIcterico', AcumPEHigadoIcterico,
        'AcumPEMaterialCaseoso_po1ra', AcumPEMaterialCaseoso_po1ra,
        'AcumPEMaterialCaseosoMedRetr', AcumPEMaterialCaseosoMedRetr,
        'AcumPENecrosisHepatica', AcumPENecrosisHepatica,
        'AcumPENeumonia', AcumPENeumonia,
        'AcumPESepticemia', AcumPESepticemia,
        'AcumPEVomitoNegro', AcumPEVomitoNegro,
        'AcumPEAsperguillius', AcumPEAsperguillius,
        'AcumPEBazoGrandeMot', AcumPEBazoGrandeMot,
        'AcumPECorazonGrande', AcumPECorazonGrande,
        'AcumPECuadroToxico', AcumPECuadroToxico
    ) AS (causa, cmortalidadacum)
from 
    {database_name_tmp}.LesionesMortalidadLoteSemanal
)
WHERE
    cmortalidadacum <> 0
ORDER BY
    pk_semanavida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesAcum"
}
df_OrdenarMortalidadesAcumTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidadesAcum")
print('carga OrdenarMortalidadesAcum', df_OrdenarMortalidadesAcumTemp.count())
df_OrdenarMortalidadesAcum2Temp = spark.sql(f"""
SELECT
    A.pk_semanavida,
    A.ComplexEntityNo,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionAcumUno' THEN P1.cmortalidadacum ELSE NULL END) AS CantMortSemLesionAcumUno,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionAcumDos' THEN P1.cmortalidadacum ELSE NULL END) AS CantMortSemLesionAcumDos,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionAcumTres' THEN P1.cmortalidadacum ELSE NULL END) AS CantMortSemLesionAcumTres,
    MAX(CASE WHEN P1.ordenCantidad = 'CantMortSemLesionAcumOtros' THEN P1.cmortalidadacum ELSE NULL END) AS CantMortSemLesionAcumOtros,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionAcumUno' THEN P2.pcmortalidadacum ELSE NULL END) AS PorcMortSemLesionAcumUno,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionAcumDos' THEN P2.pcmortalidadacum ELSE NULL END) AS PorcMortSemLesionAcumDos,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionAcumTres' THEN P2.pcmortalidadacum ELSE NULL END) AS PorcMortSemLesionAcumTres,
    MAX(CASE WHEN P2.ordenPorcentaje = 'PorcMortSemLesionAcumOtros' THEN P2.pcmortalidadacum ELSE NULL END) AS PorcMortSemLesionAcumOtros,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionAcumUno' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionAcumUno,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionAcumDos' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionAcumDos,
    MAX(CASE WHEN P3.ordenNombre = 'DescripMortSemLesionAcumTres' THEN P3.causa ELSE NULL END) AS DescripMortSemLesionAcumTres,
    'Otros' AS DescripMortSemLesionAcumOtros
FROM
    (SELECT pk_semanavida, complexentityno FROM {database_name_tmp}.OrdenarMortalidadesAcum GROUP BY pk_semanavida, complexentityno) AS A
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, AVG(cmortalidadacum) AS cmortalidadacum, ordenCantidad FROM {database_name_tmp}.OrdenarMortalidadesAcum GROUP BY pk_semanavida, complexentityno, ordenCantidad) AS P1
    ON A.ComplexEntityNo = P1.ComplexEntityNo AND A.pk_semanavida = P1.pk_semanavida
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, AVG(pcmortalidadacum) AS pcmortalidadacum, ordenPorcentaje FROM {database_name_tmp}.OrdenarMortalidadesAcum GROUP BY pk_semanavida, complexentityno, ordenPorcentaje) AS P2
    ON A.ComplexEntityNo = P2.ComplexEntityNo AND A.pk_semanavida = P2.pk_semanavida
LEFT JOIN
    (SELECT pk_semanavida, complexentityno, MAX(causa) AS causa, ordenNombre FROM {database_name_tmp}.OrdenarMortalidadesAcum GROUP BY pk_semanavida, complexentityno, ordenNombre) AS P3
    ON A.ComplexEntityNo = P3.ComplexEntityNo AND A.pk_semanavida = P3.pk_semanavida
GROUP BY
    A.pk_semanavida, A.ComplexEntityNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesAcum2"
}
df_OrdenarMortalidadesAcum2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidadesAcum2")
print('carga OrdenarMortalidadesAcum2', df_OrdenarMortalidadesAcum2Temp.count())
#df_OrdenarMortalidadesAcum2Temp = spark.sql(f"""
#WITH PivotedData AS (
#    SELECT 
#        pk_semanavida,
#        ComplexEntityNo,
#        MAX(CASE WHEN ordenCantidad = 'CantMortSemLesionAcumUno' THEN cmortalidadacum END) AS CantMortSemLesionAcumUno,
#        MAX(CASE WHEN ordenCantidad = 'CantMortSemLesionAcumDos' THEN cmortalidadacum END) AS CantMortSemLesionAcumDos,
#        MAX(CASE WHEN ordenCantidad = 'CantMortSemLesionAcumTres' THEN cmortalidadacum END) AS CantMortSemLesionAcumTres,
#        SUM(CASE WHEN ordenCantidad = 'CantMortSemLesionAcumOtros' THEN cmortalidadacum END) AS CantMortSemLesionAcumOtros,
#        
#        MAX(CASE WHEN ordenPorcentaje = 'PorcMortSemLesionAcumUno' THEN pcmortalidadacum END) AS PorcMortSemLesionAcumUno,
#        MAX(CASE WHEN ordenPorcentaje = 'PorcMortSemLesionAcumDos' THEN pcmortalidadacum END) AS PorcMortSemLesionAcumDos,
#        MAX(CASE WHEN ordenPorcentaje = 'PorcMortSemLesionAcumTres' THEN pcmortalidadacum END) AS PorcMortSemLesionAcumTres,
#        SUM(CASE WHEN ordenPorcentaje = 'PorcMortSemLesionAcumOtros' THEN pcmortalidadacum END) AS PorcMortSemLesionAcumOtros,
#        
#        MAX(CASE WHEN ordenNombre = 'DescripMortSemLesionAcumUno' THEN causa END) AS DescripMortSemLesionAcumUno,
#        MAX(CASE WHEN ordenNombre = 'DescripMortSemLesionAcumDos' THEN causa END) AS DescripMortSemLesionAcumDos,
#        MAX(CASE WHEN ordenNombre = 'DescripMortSemLesionAcumTres' THEN causa END) AS DescripMortSemLesionAcumTres
#    FROM {database_name}.OrdenarMortalidadesAcum
#    GROUP BY pk_semanavida, ComplexEntityNo
#)
#
#SELECT 
#    pk_semanavida,
#    ComplexEntityNo,
#    CantMortSemLesionAcumUno,
#    CantMortSemLesionAcumDos,
#    CantMortSemLesionAcumTres,
#    CantMortSemLesionAcumOtros,
#    PorcMortSemLesionAcumUno,
#    PorcMortSemLesionAcumDos,
#    PorcMortSemLesionAcumTres,
#    PorcMortSemLesionAcumOtros,
#    DescripMortSemLesionAcumUno,
#    DescripMortSemLesionAcumDos,
#    DescripMortSemLesionAcumTres,
#    'Otros' AS DescripMortSemLesionAcumOtros
#FROM PivotedData""")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesAcum2"
#}
#df_OrdenarMortalidadesAcum2Temp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.OrdenarMortalidadesAcum2")
#print('carga OrdenarMortalidadesAcum2', df_OrdenarMortalidadesAcum2Temp.count())
df_ft_mortalidad_Lote_Semanal =spark.sql(f"""select 
a.pk_tiempo
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_especie
,a.pk_estado
,a.pk_tipoproducto
,a.pk_administrador 
,a.pk_proveedor
,a.pk_semanavida
,a.complexentityno
,a.FechaIngreso
,a.FechaCierre
,a.SemanaCalendario
,a.PobInicial
,a.MortDia
,a.MortDiaAcum
,a.PorcMortDia 
,a.PorcMortDiaAcum
,a.U_PEAccidentados
,a.U_PEHigadoGraso
,a.U_PEHepatomegalia
,a.U_PEHigadoHemorragico
,a.U_PEInanicion
,a.U_PEProblemaRespiratorio
,a.U_PESCH 
,a.U_PEEnteritis
,a.U_PEAscitis
,a.U_PEMuerteSubita
,a.U_PEEstresPorCalor
,a.U_PEHidropericardio
,a.U_PEHemopericardio
,a.U_PEUratosis
,a.U_PEMaterialCaseoso 
,a.U_PEOnfalitis
,a.U_PERetencionDeYema
,a.U_PEErosionDeMolleja
,a.U_PEHemorragiaMusculos
,a.U_PESangreEnCiego
,a.U_PEPericarditis
,a.U_PEPeritonitis 
,a.U_PEProlapso
,a.U_PEPicaje
,a.U_PERupturaAortica
,a.U_PEBazoMoteado
,a.U_PENoViable
,0.0  as U_PECaja
,0.0  as U_PEGota
,0.0 as U_PEIntoxicacion
,0.0  as U_PERetrazos
,0.0  as U_PEEliminados
,0.0  as U_PEAhogados
,0.0  as U_PEEColi
,0.0  as U_PEDescarte
,0.0  as U_PEOtros
,0.0  as U_PECoccidia
,0.0 as U_PEDeshidratados
,0.0 as U_PEHepatitis
,0.0 as U_PETraumatismo
,a.PorcAccidentados
,a.PorcHigadoGraso
,a.PorcHepatomegalia 
,a.PorcHigadoHemorragico
,a.PorcInanicion
,a.PorcProblemaRespiratorio
,a.PorcSCH
,a.PorcEnteritis
,a.PorcAscitis
,a.PorcMuerteSubita
,a.PorcEstresPorCalor 
,a.PorcHidropericardio
,a.PorcHemopericardio
,a.PorcUratosis
,a.PorcMaterialCaseoso
,a.PorcOnfalitis
,a.PorcRetencionDeYema
,a.PorcErosionDeMolleja 
,a.PorcHemorragiaMusculos
,a.PorcSangreEnCiego
,a.PorcPericarditis
,a.PorcPeritonitis
,a.PorcProlapso
,a.PorcPicaje
,a.PorcRupturaAortica 
,a.PorcBazoMoteado
,a.PorcNoViable
,0.0 as PorcCaja
,0.0 as PorcGota
,0.0 as PorcIntoxicacion
,0.0 as PorcRetrazos
,0.0 as PorcEliminados
,0.0 as PorcAhogados
,0.0 as PorcEColi
,0.0 as PorcDescarte
,0.0 as PorcOtros
,0.0 as PorcCoccidia
,0.0 as PorcDeshidratados
,0.0 as PorcHepatitis
,0.0 as PorcTraumatismo
,0.0 as U_PECajaAcum
,0.0 as U_PEGotaAcum
,a.U_PEAccidentadosAcum
,a.U_PEHigadoGrasoAcum
,a.U_PEHepatomegaliaAcum
,a.U_PEHigadoHemorragicoAcum
,a.U_PEInanicionAcum 
,a.U_PEProblemaRespiratorioAcum
,a.U_PESCHAcum
,a.U_PEEnteritisAcum
,a.U_PEAscitisAcum
,a.U_PEMuerteSubitaAcum
,a.U_PEEstresPorCalorAcum 
,a.U_PEHidropericardioAcum
,a.U_PEHemopericardioAcum
,a.U_PEUratosisAcum
,a.U_PEMaterialCaseosoAcum
,a.U_PEOnfalitisAcum
,a.U_PERetencionDeYemaAcum 
,a.U_PEErosionDeMollejaAcum
,a.U_PEHemorragiaMusculosAcum
,a.U_PESangreEnCiegoAcum
,a.U_PEPericarditisAcum
,a.U_PEPeritonitisAcum
,a.U_PEProlapsoAcum 
,a.U_PEPicajeAcum
,a.U_PERupturaAorticaAcum
,a.U_PEBazoMoteadoAcum
,a.U_PENoViableAcum
,0.0 as U_PEIntoxicacionAcum
,0.0 as U_PERetrazosAcum
,0.0 as U_PEEliminadosAcum
,0.0 as U_PEAhogadosAcum
,0.0 as U_PEEColiAcum
,0.0 as U_PEDescarteAcum
,0.0 as U_PEOtrosAcum
,0.0 as U_PECoccidiaAcum
,0.0 as U_PEDeshidratadosAcum
,0.0 as U_PEHepatitisAcum
,0.0 as U_PETraumatismoAcum
,a.PorcAccidentadosAcum
,a.PorcHigadoGrasoAcum 
,a.PorcHepatomegaliaAcum
,a.PorcHigadoHemorragicoAcum
,a.PorcInanicionAcum
,a.PorcProblemaRespiratorioAcum
,a.PorcSCHAcum
,a.PorcEnteritisAcum 
,a.PorcAscitisAcum
,a.PorcMuerteSubitaAcum
,a.PorcEstresPorCalorAcum
,a.PorcHidropericardioAcum
,a.PorcHemopericardioAcum
,a.PorcUratosisAcum 
,a.PorcMaterialCaseosoAcum
,a.PorcOnfalitisAcum
,a.PorcRetencionDeYemaAcum
,a.PorcErosionDeMollejaAcum
,a.PorcHemorragiaMusculosAcum 
,a.PorcSangreEnCiegoAcum
,a.PorcPericarditisAcum
,a.PorcPeritonitisAcum
,a.PorcProlapsoAcum
,a.PorcPicajeAcum
,a.PorcRupturaAorticaAcum 
,a.PorcBazoMoteadoAcum
,a.PorcNoViableAcum
,0.0 as PorcCajaAcum
,0.0 as PorcGotaAcum
,0.0 as PorcIntoxicacionAcum
,0.0 as PorcRetrazosAcum
,0.0 as PorcEliminadosAcum
,0.0 as PorcAhogadosAcum
,0.0 as PorcEColiAcum
,0.0 as PorcDescarteAcum
,0.0 as PorcOtrosAcum
,0.0 as PorcCoccidiaAcum
,0.0 as PorcDeshidratadosAcum
,0.0 as PorcHepatitisAcum
,0.0 as PorcTraumatismoAcum
,a.flagartificio
,a.categoria
,0 FlagAtipico
,a.U_PEAerosaculitisG2
,a.U_PECojera
,a.U_PEHigadoIcterico 
,a.U_PEMaterialCaseoso_po1ra
,a.U_PEMaterialCaseosoMedRetr
,a.U_PENecrosisHepatica
,a.U_PENeumonia
,a.U_PESepticemia
,a.U_PEVomitoNegro 
,a.PorcAerosaculitisG2
,a.PorcCojera
,a.PorcHigadoIcterico
,a.PorcMaterialCaseoso_po1ra
,a.PorcMaterialCaseosoMedRetr
,a.PorcNecrosisHepatic 
,a.PorcNeumonia
,a.PorcSepticemia
,a.PorcVomitoNegro
,a.U_PEAerosaculitisG2Acum
,a.U_PECojeraAcum
,a.U_PEHigadoIctericoAcum 
,a.U_PEMaterialCaseoso_po1raAcum
,a.U_PEMaterialCaseosoMedRetrAcum
,a.U_PENecrosisHepaticAcum
,a.U_PENeumoniaAcum
,a.U_PESepticemiaAcum 
,a.U_PEVomitoNegroAcum
,a.PorcAerosaculitisG2Acum
,a.PorcCojeraAcum
,a.PorcHigadoIctericoAcum
,a.PorcMaterialCaseoso_po1raAcum 
,a.PorcMaterialCaseosoMedRetrAcum
,a.PorcNecrosisHepaticAcum
,a.PorcNeumoniaAcum
,a.PorcSepticemiaAcum
,a.PorcVomitoNegroAcum 
,a.U_PEAsperguillius
,a.U_PEBazoGrandeMot
,a.U_PECorazonGrande
,a.PorcAsperguillius
,a.PorcBazoGrandeMot
,a.PorcCorazonGrande 
,a.U_PEAsperguilliusAcum
,a.U_PEBazoGrandeMotAcum
,a.U_PECorazonGrandeAcum
,a.PorcAsperguilliusAcum
,a.PorcBazoGrandeMotAcum 
,a.PorcCorazonGrandeAcum
,a.U_PECuadroToxico
,a.PorcCuadroToxico
,a.U_PECuadroToxicoAcum
,a.PorcCuadroToxicoAcum 
,b.CantMortSemLesionUno
,b.PorcMortSemLesionUno
,b.DescripMortSemLesionUno
,b.CantMortSemLesionDos
,b.PorcMortSemLesionDos 
,b.DescripMortSemLesionDos
,b.CantMortSemLesionTres
,b.PorcMortSemLesionTres
,b.DescripMortSemLesionTres
,b.CantMortSemLesionOtros 
,b.PorcMortSemLesionOtros
,b.DescripMortSemLesionOtros
,c.CantMortSemLesionAcumUno
,c.PorcMortSemLesionAcumUno 
,c.DescripMortSemLesionAcumUno
,c.CantMortSemLesionAcumDos
,c.PorcMortSemLesionAcumDos
,c.DescripMortSemLesionAcumDos 
,c.CantMortSemLesionAcumTres
,c.PorcMortSemLesionAcumTres
,c.DescripMortSemLesionAcumTres
,c.CantMortSemLesionAcumOtros 
,c.PorcMortSemLesionAcumOtros
,c.DescripMortSemLesionAcumOtros
,a.SemAccidentados
,a.SemHigadoGraso
,a.SemHepatomegalia 
,a.SemHigadoHemorragico
,a.SemInanicion
,a.SemProblemaRespiratorio
,a.SemSCH
,a.SemEnteritis
,a.SemAscitis
,a.SemMuerteSubita 
,a.SemEstresPorCalor
,a.SemHidropericardio
,a.SemHemopericardio
,a.SemUratosis
,a.SemMaterialCaseoso
,a.SemOnfalitis 
,a.SemRetencionDeYema
,a.SemErosionDeMolleja
,a.SemHemorragiaMusculos
,a.SemSangreEnCiego
,a.SemPericarditis
,a.SemPeritonitis 
,a.SemProlapso
,a.SemPicaje
,a.SemRupturaAortica
,a.SemBazoMoteado
,a.SemNoViable
,a.SemAerosaculitisG2
,a.SemCojera 
,a.SemHigadoIcterico
,a.SemMaterialCaseoso_po1ra
,a.SemMaterialCaseosoMedRetr
,a.SemNecrosisHepatica
,a.SemNeumonia
,a.SemSepticemia 
,a.SemVomitoNegro
,a.SemAsperguillius
,a.SemBazoGrandeMot
,a.SemCorazonGrande
,a.SemCuadroToxico
,a.SemPorcAccidentados 
,a.SemPorcHigadoGraso
,a.SemPorcHepatomegalia
,a.SemPorcHigadoHemorragico
,a.SemPorcInanicion
,a.SemPorcProblemaRespiratorio 
,a.SemPorcSCH
,a.SemPorcEnteritis
,a.SemPorcAscitis
,a.SemPorcMuerteSubita
,a.SemPorcEstresPorCalor
,a.SemPorcHidropericardio 
,a.SemPorcHemopericardio
,a.SemPorcUratosis
,a.SemPorcMaterialCaseoso
,a.SemPorcOnfalitis
,a.SemPorcRetencionDeYema 
,a.SemPorcErosionDeMolleja
,a.SemPorcHemorragiaMusculos
,a.SemPorcSangreEnCiego
,a.SemPorcPericarditis
,a.SemPorcPeritonitis 
,a.SemPorcProlapso
,a.SemPorcPicaje
,a.SemPorcRupturaAortica
,a.SemPorcBazoMoteado
,a.SemPorcNoViable
,a.SemPorcAerosaculitisG2 
,a.SemPorcCojera
,a.SemPorcHigadoIcterico
,a.SemPorcMaterialCaseoso_po1ra
,a.SemPorcMaterialCaseosoMedRetr 
,a.SemPorcNecrosisHepatica
,a.SemPorcNeumonia
,a.SemPorcSepticemia
,a.SemPorcVomitoNegro
,a.SemPorcAsperguillius 
,a.SemPorcBazoGrandeMot
,a.SemPorcCorazonGrande
,a.SemPorcCuadroToxico
,0 as SemCaja
,0 as SemGota
,0 as SemIntoxicacion
,0 as SemRetrazos
,0 as SemEliminados
,0 as SemAhogados
,0 as SemEColi
,0 as SemOtros
,0 as SemCoccidia
,0 as SemDeshidratados
,0 as SemHepatitis
,0 as SemTraumatismo
,0 as SemPorcCaja
,0 as SemPorcGota
,0 as SemPorcIntoxicacion
,0 as SemPorcRetrazos
,0 as SemPorcEliminados
,0 as SemPorcAhogados
,0 as SemPorcEColi
,0 as SemPorcOtros
,0 as SemPorcCoccidia
,0 as SemPorcDeshidratados
,0 as SemPorcHepatitis
,0 as SemPorcTraumatismo
,0.0 as U_PENoEspecificado
,0.0 as U_PEAnalisis_Laboratorio
,0.0 as PorcNoEspecificado
,0.0 as PorcAnalisisLaboratorio
,0.0 as U_PENoEspecificadoAcum
,0.0 as U_PEAnalisisLaboratorioAcum
,0.0 as PorcNoEspecificadoAcum
,0.0 as PorcAnalisisLaboratorioAcum
,0 as SemNoEspecificado
,0 as SemAnalisisLaboratorio
,0 as SemPorcNoEspecificado
,0 as SemPorcAnalisisLaboratorio
,a.fecha as DescripFecha
,'' as DescripEmpresa
,'' as DescripDivision
,'' as DescripZona
,'' as DescripSubzona
,'' as Plantel
,'' as Lote
,'' as DescripEspecie
,'' as DescripEstado
,'' as DescripTipoProducto
,'' as DescripAdministrador
,'' as DescripProveedor
,'' as DescripSemanaVida
from {database_name_tmp}.MortalidadLoteSemanalCompleto a 
left join {database_name_tmp}.OrdenarMortalidades2 b on a.complexentityno = b.complexentityno and a.pk_semanavida = b.pk_semanavida 
left join {database_name_tmp}.OrdenarMortalidadesAcum2 C on a.complexentityno = c.complexentityno and a.pk_semanavida = c.pk_semanavida 
where (date_format(a.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))""")
print('carga df_ft_mortalidad_Lote_Semanal', df_ft_mortalidad_Lote_Semanal.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes30 = spark.read.format("parquet").load(path_target30)
    datos_existentes30 = True
    logger.info(f"Datos existentes de ft_mortalidad_Lote_Semanal cargados: {df_existentes30.count()} registros")
except:
    datos_existentes30 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Lote_Semanal")
    
if datos_existentes30:
    existing_data30 = spark.read.format("parquet").load(path_target30)
    data_after_delete30 = existing_data30.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data30 = df_ft_mortalidad_Lote_Semanal.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data30 = filtered_new_data30.union(data_after_delete30)                             
   
    cant_ingresonuevo30 = filtered_new_data30.count()
    cant_total30 = final_data30.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_Lote_SemanalTemporal"
    }
    final_data30.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_Lote_SemanalTemporal")

    final_data30_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_Lote_SemanalTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_lote_semanal"
    }
    final_data30_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_lote_semanal")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Lote_Semanal : {cant_ingresonuevo30}")
    print(f"Total de registros en la tabla ft_mortalidad_Lote_Semanal : {cant_total30}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_Lote_SemanalTemporal')
    print(f"Tabla ft_mortalidad_Lote_SemanalTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_lote_semanal"
    }
    df_ft_mortalidad_Lote_Semanal.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_lote_semanal")
df_atificio_Lote_SemanalTemp = spark.sql(f"""select A.complexentityno,max(A.pk_semanavida) pk_semanavida, \
max(XA.pk_semanavida) as pk_semanavidalote, 
date_format(A.fechacierre,'yyyyMM') MesCierre 
from {database_name_gl}.ft_mortalidad_Lote_Semanal  A 
left join {database_name_gl}.ft_mortalidad_Lote_Semanal  XA on date_format(xa.fechacierre,'yyyyMM')=  date_format(a.fechacierre,'yyyyMM')
where A.pk_empresa = 1 and date_format(A.fechacierre,'yyyyMM') <> '189911' and a.pk_division = 4 and xa.pk_division = 4 
AND (date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))
group by  A.complexentityno,date_format(A.fechacierre,'yyyyMM')""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/atificio_Lote_Semanal"
}
df_atificio_Lote_SemanalTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.atificio_Lote_Semanal")
print('carga atificio_Lote_Semanal', df_atificio_Lote_SemanalTemp.count())
df_Mort_Lote_Agregado = spark.sql(f"""
select
pk_tiempo
,pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_lote
,pk_especie
,pk_estado
,pk_tipoproducto
,pk_administrador
,pk_proveedor
,B.pk_semanavida 
,C.complexentityno
,FechaIngreso
,FechaCierre
,SemanaCalendario
,PobInicial
,0 MortDia
,MortDiaAcum
,0 PorcMortDia
,PorcMortDiaAcum 
,0 U_PEAccidentados
,0 U_PEHigadoGraso
,0 U_PEHepatomegalia
,0 U_PEHigadoHemorragico
,0 U_PEInanicion
,0 U_PEProblemaRespiratorio
,0 U_PESCH 
,0 U_PEEnteritis
,0 U_PEAscitis
,0 U_PEMuerteSubita
,0 U_PEEstresPorCalor
,0 U_PEHidropericardio
,0 U_PEHemopericardio
,0 U_PEUratosis 
,0 U_PEMaterialCaseoso
,0 U_PEOnfalitis
,0 U_PERetencionDeYema
,0 U_PEErosionDeMolleja
,0 U_PEHemorragiaMusculos
,0 U_PESangreEnCiego 
,0 U_PEPericarditis
,0 U_PEPeritonitis
,0 U_PEProlapso
,0 U_PEPicaje
,0 U_PERupturaAortica
,0 U_PEBazoMoteado
,0 U_PENoViable 
,0 U_PECaja
,0 U_PEGota
,0 U_PEIntoxicacion
,0 U_PERetrazos
,0 U_PEEliminados
,0 U_PEAhogados
,0 U_PEEColi
,0 U_PEDescarte
,0 U_PEOtros
,0 U_PECoccidia
,0 U_PEDeshidratados
,0 U_PEHepatitis
,0 U_PETraumatismo
,0 PorcAccidentados
,0 PorcHigadoGraso
,0 PorcHepatomegalia
,0 PorcHigadoHemorragico
,0 PorcInanicion
,0 PorcProblemaRespiratorio 
,0 PorcSCH
,0 PorcEnteritis
,0 PorcAscitis
,0 PorcMuerteSubita
,0 PorcEstresPorCalor
,0 PorcHidropericardio
,0 PorcHemopericardio 
,0 PorcUratosis
,0 PorcMaterialCaseoso
,0 PorcOnfalitis
,0 PorcRetencionDeYema
,0 PorcErosionDeMolleja
,0 PorcHemorragiaMusculos 
,0 PorcSangreEnCiego
,0 PorcPericarditis
,0 PorcPeritonitis
,0 PorcProlapso
,0 PorcPicaje
,0 PorcRupturaAortica
,0 PorcBazoMoteado 
,0 PorcNoViable 
,0 PorcCaja
,0 PorcGota
,0 PorcIntoxicacion
,0 PorcRetrazos
,0 PorcEliminados
,0 PorcAhogados
,0 PorcEColi
,0 PorcDescarte
,0 PorcOtros
,0 PorcCoccidia
,0 porcdeshidratados
,0 PorcHepatitis
,0 PorcTraumatismo
,0 U_PECajaAcum
,0 U_PEGotaAcum
,U_PEAccidentadosAcum 
,U_PEHigadoGrasoAcum 
,U_PEHepatomegaliaAcum 
,U_PEHigadoHemorragicoAcum 
,U_PEInanicionAcum 
,U_PEProblemaRespiratorioAcum 
,U_PESCHAcum 
,U_PEEnteritisAcum 
,U_PEAscitisAcum 
,U_PEMuerteSubitaAcum 
,U_PEEstresPorCalorAcum 
,U_PEHidropericardioAcum 
,U_PEHemopericardioAcum 
,U_PEUratosisAcum 
,U_PEMaterialCaseosoAcum 
,U_PEOnfalitisAcum 
,U_PERetencionDeYemaAcum 
,U_PEErosionDeMollejaAcum 
,U_PEHemorragiaMusculosAcum 
,U_PESangreEnCiegoAcum 
,U_PEPericarditisAcum 
,U_PEPeritonitisAcum 
,U_PEProlapsoAcum 
,U_PEPicajeAcum 
,U_PERupturaAorticaAcum 
,U_PEBazoMoteadoAcum 
,U_PENoViableAcum 
,0 U_PEIntoxicacionAcum
,0 U_PERetrazosAcum
,0 U_PEEliminadosAcum
,0 U_PEAhogadosAcum
,0 U_PEEColiAcum
,0 U_PEDescarteAcum
,0 U_PEOtrosAcum
,0 U_PECoccidiaAcum
,0 U_PEDeshidratadosAcum
,0 U_PEHepatitisAcum
,0 U_PETraumatismoAcum
,PorcAccidentadosAcum 
,PorcHigadoGrasoAcum 
,PorcHepatomegaliaAcum 
,PorcHigadoHemorragicoAcum 
,PorcInanicionAcum 
,PorcProblemaRespiratorioAcum 
,PorcSCHAcum 
,PorcEnteritisAcum 
,PorcAscitisAcum 
,PorcMuerteSubitaAcum 
,PorcEstresPorCalorAcum 
,PorcHidropericardioAcum 
,PorcHemopericardioAcum
,PorcUratosisAcum 
,PorcMaterialCaseosoAcum 
,PorcOnfalitisAcum 
,PorcRetencionDeYemaAcum 
,PorcErosionDeMollejaAcum 
,PorcHemorragiaMusculosAcum 
,PorcSangreEnCiegoAcum 
,PorcPericarditisAcum 
,PorcPeritonitisAcum 
,PorcProlapsoAcum 
,PorcPicajeAcum 
,PorcRupturaAorticaAcum 
,PorcBazoMoteadoAcum 
,PorcNoViableAcum 
,0 PorcCajaAcum
,0 PorcGotaAcum
,0 PorcIntoxicacionAcum
,0 PorcRetrazosAcum
,0 PorcEliminadosAcum
,0 PorcAhogadosAcum
,0 PorcEColiAcum
,0 PorcDescarteAcum
,0 PorcOtrosAcum
,0 PorcCoccidiaAcum
,0 PorcDeshidratadosAcum
,0 PorcHepatitisAcum
,0 PorcTraumatismoAcum
,2 as flagartificio 
,categoria 
,FlagAtipico
,0 U_PEAerosaculitisG2 
,0 U_PECojera 
,0 U_PEHigadoIcterico 
,0 U_PEMaterialCaseoso_po1ra 
,0 U_PEMaterialCaseosoMedRetr 
,0 U_PENecrosisHepatica 
,0 U_PENeumonia 
,0 U_PESepticemia 
,0 U_PEVomitoNegro 
,0 PorcAerosaculitisG2 
,0 PorcCojera 
,0 PorcHigadoIcterico 
,0 PorcMaterialCaseoso_po1ra 
,0 PorcMaterialCaseosoMedRetr 
,0 PorcNecrosisHepatic 
,0 PorcNeumonia 
,0 PorcSepticemia 
,0 PorcVomitoNegro 
,U_PEAerosaculitisG2Acum 
,U_PECojeraAcum 
,U_PEHigadoIctericoAcum 
,U_PEMaterialCaseoso_po1raAcum 
,U_PEMaterialCaseosoMedRetrAcum 
,U_PENecrosisHepaticAcum 
,U_PENeumoniaAcum 
,U_PESepticemiaAcum 
,U_PEVomitoNegroAcum 
,PorcAerosaculitisG2Acum 
,PorcCojeraAcum 
,PorcHigadoIctericoAcum 
,PorcMaterialCaseoso_po1raAcum 
,PorcMaterialCaseosoMedRetrAcum 
,PorcNecrosisHepaticAcum 
,PorcNeumoniaAcum 
,PorcSepticemiaAcum 
,PorcVomitoNegroAcum 
,0 U_PEAsperguillius 
,0 U_PEBazoGrandeMot 
,0 U_PECorazonGrande 
,0 PorcAsperguillius 
,0 PorcBazoGrandeMot 
,0 PorcCorazonGrande 
,U_PEAsperguilliusAcum 
,U_PEBazoGrandeMotAcum 
,U_PECorazonGrandeAcum 
,PorcAsperguilliusAcum 
,PorcBazoGrandeMotAcum 
,PorcCorazonGrandeAcum 
,0 U_PECuadroToxico 
,0 PorcCuadroToxico 
,U_PECuadroToxicoAcum 
,PorcCuadroToxicoAcum 
,CantMortSemLesionUno
,PorcMortSemLesionUno
,DescripMortSemLesionUno
,CantMortSemLesionDos
,PorcMortSemLesionDos 
,DescripMortSemLesionDos
,CantMortSemLesionTres
,PorcMortSemLesionTres
,DescripMortSemLesionTres
,CantMortSemLesionOtros 
,PorcMortSemLesionOtros
,DescripMortSemLesionOtros
,CantMortSemLesionAcumUno
,PorcMortSemLesionAcumUno 
,DescripMortSemLesionAcumUno
,CantMortSemLesionAcumDos
,PorcMortSemLesionAcumDos
,DescripMortSemLesionAcumDos 
,CantMortSemLesionAcumTres
,PorcMortSemLesionAcumTres
,DescripMortSemLesionAcumTres
,CantMortSemLesionAcumOtros 
,PorcMortSemLesionAcumOtros
,DescripMortSemLesionAcumOtros
,SemAccidentados
,SemHigadoGraso
,SemHepatomegalia 
,SemHigadoHemorragico
,SemInanicion
,SemProblemaRespiratorio
,SemSCH
,SemEnteritis
,SemAscitis
,SemMuerteSubita 
,SemEstresPorCalor
,SemHidropericardio
,SemHemopericardio
,SemUratosis
,SemMaterialCaseoso
,SemOnfalitis 
,SemRetencionDeYema
,SemErosionDeMolleja
,SemHemorragiaMusculos
,SemSangreEnCiego
,SemPericarditis
,SemPeritonitis 
,SemProlapso
,SemPicaje
,SemRupturaAortica
,SemBazoMoteado
,SemNoViable
,SemAerosaculitisG2
,SemCojera 
,SemHigadoIcterico
,SemMaterialCaseoso_po1ra
,SemMaterialCaseosoMedRetr
,SemNecrosisHepatica
,SemNeumonia
,SemSepticemia 
,SemVomitoNegro
,SemAsperguillius
,SemBazoGrandeMot
,SemCorazonGrande
,SemCuadroToxico
,SemPorcAccidentados 
,SemPorcHigadoGraso
,SemPorcHepatomegalia
,SemPorcHigadoHemorragico
,SemPorcInanicion
,SemPorcProblemaRespiratorio 
,SemPorcSCH
,SemPorcEnteritis
,SemPorcAscitis
,SemPorcMuerteSubita
,SemPorcEstresPorCalor
,SemPorcHidropericardio 
,SemPorcHemopericardio
,SemPorcUratosis
,SemPorcMaterialCaseoso
,SemPorcOnfalitis
,SemPorcRetencionDeYema 
,SemPorcErosionDeMolleja
,SemPorcHemorragiaMusculos
,SemPorcSangreEnCiego
,SemPorcPericarditis
,SemPorcPeritonitis 
,SemPorcProlapso
,SemPorcPicaje
,SemPorcRupturaAortica
,SemPorcBazoMoteado
,SemPorcNoViable
,SemPorcAerosaculitisG2 
,SemPorcCojera
,SemPorcHigadoIcterico
,SemPorcMaterialCaseoso_po1ra
,SemPorcMaterialCaseosoMedRetr 
,SemPorcNecrosisHepatica
,SemPorcNeumonia
,SemPorcSepticemia
,SemPorcVomitoNegro
,SemPorcAsperguillius 
,SemPorcBazoGrandeMot
,SemPorcCorazonGrande
,SemPorcCuadroToxico
,SemCaja
,SemGota
,SemIntoxicacion
,SemRetrazos
,SemEliminados
,SemAhogados
,SemEColi
,SemOtros
,SemCoccidia
,SemDeshidratados
,SemHepatitis
,SemTraumatismo
,SemPorcCaja
,SemPorcGota
,SemPorcIntoxicacion
,SemPorcRetrazos
,SemPorcEliminados
,SemPorcAhogados
,SemPorcEColi
,SemPorcOtros
,SemPorcCoccidia
,SemPorcDeshidratados
,SemPorcHepatitis
,SemPorcTraumatismo
,U_PENoEspecificado
,U_PEAnalisis_Laboratorio
,PorcNoEspecificado
,PorcAnalisisLaboratorio
,U_PENoEspecificadoAcum
,U_PEAnalisisLaboratorioAcum
,PorcNoEspecificadoAcum
,PorcAnalisisLaboratorioAcum
,SemNoEspecificado
,SemAnalisisLaboratorio
,SemPorcNoEspecificado
,SemPorcAnalisisLaboratorio
,DescripFecha
,DescripEmpresa
,DescripDivision
,DescripZona
,DescripSubzona
,Plantel
,Lote
,DescripEspecie
,DescripEstado
,DescripTipoProducto
,DescripAdministrador
,DescripProveedor
,DescripSemanaVida
from {database_name_tmp}.atificio_Lote_Semanal A 
cross join {database_name_gl}.lk_semanavida B 
left join {database_name_gl}.ft_mortalidad_Lote_Semanal C on A.complexentityno = C.complexentityno and A.pk_semanavida = C.pk_semanavida 
where 
A.pk_semanavida < B.pk_semanavida and A.pk_semanavidalote >= B.pk_semanavida 
and c.pk_empresa = 1 AND (date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')) 
and c.pk_division=4""")

##and c.pk_division=4

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_lote_semanal"
}
df_Mort_Lote_Agregado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_gl}.ft_mortalidad_lote_semanal")
print('se inserta la data a ft_mortalidad_lote_semanal', df_Mort_Lote_Agregado.count())
df_MortalidadMensual =spark.sql(f"select \
idmes,a.pk_empresa,sum(a.PobInicial) as PobInicial,sum(MortDia) as MortDia \
,case when sum(a.PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(a.PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4,sum(MortSem5) as MortSem5 \
,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 \
,sum(MortSemAcum3) as MortSemAcum3,sum(MortSemAcum4) as MortSemAcum4 ,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 \
,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable,max(FlagAtipico) FlagAtipico, max(FlagArtAtipico) FlagArtAtipico \
,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2,sum(U_PECojera) U_PECojera,sum(U_PEHigadoIcterico) U_PEHigadoIcterico \
,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,sum(U_PENecrosisHepatica) U_PENecrosisHepatica,sum(U_PENeumonia) U_PENeumonia,sum(U_PESepticemia) U_PESepticemia,sum(U_PEVomitoNegro) U_PEVomitoNegro \
,sum(U_PEAsperguillius) U_PEAsperguillius,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12,sum(MortSem13) as MortSem13 \
,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19 \
,sum(MortSem20) as MortSem20,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 ,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 \
,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 ,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 ,sum(MortSemAcum16) as MortSemAcum16 \
,sum(MortSemAcum17) as MortSemAcum17 ,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 ,sum(MortSemAcum20) as MortSemAcum20 \
,sum(U_PECuadroToxico) U_PECuadroToxico,sum(PavosBBMortIncub) as PavosBBMortIncub \
from {database_name_tmp}.MortalidadLote A \
where FlagArtAtipico = 2 \
group by idmes,pk_empresa")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadMensual"
}
df_MortalidadMensual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadMensual")
print('carga MortalidadMensual', df_MortalidadMensual.count())
df_MortalidadMensualTemp =spark.sql(f"select \
idmes,a.pk_empresa,sum(a.PobInicial) as PobInicial,sum(MortDia) as MortDia \
,case when sum(a.PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(a.PobInicial*1.0))*100)) end as PorcMortDia \
,sum(MortSem1) as MortSem1,sum(MortSem2) as MortSem2,sum(MortSem3) as MortSem3,sum(MortSem4) as MortSem4,sum(MortSem5) as MortSem5 \
,sum(MortSem6) as MortSem6,sum(MortSem7) as MortSem7 \
,sum(MortSemAcum1) as MortSemAcum1 ,sum(MortSemAcum2) as MortSemAcum2 ,sum(MortSemAcum3) as MortSemAcum3 \
,sum(MortSemAcum4) as MortSemAcum4 ,sum(MortSemAcum5) as MortSemAcum5 ,sum(MortSemAcum6) as MortSemAcum6 ,sum(MortSemAcum7) as MortSemAcum7 \
,sum(U_PEAccidentados) U_PEAccidentados,sum(U_PEHigadoGraso) U_PEHigadoGraso,sum(U_PEHepatomegalia) U_PEHepatomegalia, \
sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico,sum(U_PEInanicion) U_PEInanicion,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio, \
sum(U_PESCH) U_PESCH,sum(U_PEEnteritis) U_PEEnteritis,sum(U_PEAscitis) U_PEAscitis,sum(U_PEMuerteSubita) U_PEMuerteSubita, \
sum(U_PEEstresPorCalor) U_PEEstresPorCalor,sum(U_PEHidropericardio) U_PEHidropericardio,sum(U_PEHemopericardio) U_PEHemopericardio, \
sum(U_PEUratosis) U_PEUratosis,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso,sum(U_PEOnfalitis) U_PEOnfalitis, \
sum(U_PERetencionDeYema) U_PERetencionDeYema,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos, \
sum(U_PESangreEnCiego) U_PESangreEnCiego,sum(U_PEPericarditis) U_PEPericarditis,sum(U_PEPeritonitis)U_PEPeritonitis, \
sum(U_PEProlapso)U_PEProlapso,sum(U_PEPicaje)U_PEPicaje,sum(U_PERupturaAortica) U_PERupturaAortica,sum(U_PEBazoMoteado)U_PEBazoMoteado, \
sum(U_PENoViable) U_PENoViable,1 FlagAtipico, max(FlagArtAtipico) FlagArtAtipico,sum(U_PEAerosaculitisG2) U_PEAerosaculitisG2 \
,sum(U_PECojera) U_PECojera,sum(U_PEHigadoIcterico) U_PEHigadoIcterico \
,sum(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra,sum(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr \
,sum(U_PENecrosisHepatica) U_PENecrosisHepatica \
,sum(U_PENeumonia) U_PENeumonia,sum(U_PESepticemia) U_PESepticemia,sum(U_PEVomitoNegro) U_PEVomitoNegro,sum(U_PEAsperguillius) U_PEAsperguillius \
,sum(U_PEBazoGrandeMot) U_PEBazoGrandeMot,sum(U_PECorazonGrande) U_PECorazonGrande \
,sum(MortSem8) as MortSem8,sum(MortSem9) as MortSem9,sum(MortSem10) as MortSem10,sum(MortSem11) as MortSem11,sum(MortSem12) as MortSem12 \
,sum(MortSem13) as MortSem13,sum(MortSem14) as MortSem14,sum(MortSem15) as MortSem15,sum(MortSem16) as MortSem16,sum(MortSem17) as MortSem17 \
,sum(MortSem18) as MortSem18,sum(MortSem19) as MortSem19,sum(MortSem20) as MortSem20,sum(MortSemAcum8) as MortSemAcum8 ,sum(MortSemAcum9) as MortSemAcum9 \
,sum(MortSemAcum10) as MortSemAcum10 ,sum(MortSemAcum11) as MortSemAcum11 ,sum(MortSemAcum12) as MortSemAcum12 ,sum(MortSemAcum13) as MortSemAcum13 \
,sum(MortSemAcum14) as MortSemAcum14 ,sum(MortSemAcum15) as MortSemAcum15 ,sum(MortSemAcum16) as MortSemAcum16 ,sum(MortSemAcum17) as MortSemAcum17 \
,sum(MortSemAcum18) as MortSemAcum18 ,sum(MortSemAcum19) as MortSemAcum19 ,sum(MortSemAcum20) as MortSemAcum20 ,sum(U_PECuadroToxico) U_PECuadroToxico \
,sum(PavosBBMortIncub) as PavosBBMortIncub \
from {database_name_tmp}.MortalidadLote A \
where FlagAtipico = 1 and FlagArtAtipico = 1 \
group by idmes,pk_empresa")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadMensualTemp"
}
df_MortalidadMensualTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadMensualTemp")
print('carga MortalidadMensualTemp', df_MortalidadMensualTemp.count())
df_MortalidadMensual_upd = spark.sql(f"select * from {database_name_tmp}.MortalidadMensualTemp \
except \
select * from {database_name_tmp}.MortalidadMensual")
print('carga df_MortalidadMensual_upd', df_MortalidadMensual_upd.count())
try:
    df = spark.table(f"{database_name_tmp}.MortalidadMensual")
    df_MortalidadMensual_new = df_MortalidadMensual_upd.union(df)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_MortalidadMensual_new = df_MortalidadMensual_upd
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadMensual_new"
}
# 1️⃣ Crear DataFrame intermedio
df_MortalidadMensual_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name_tmp}.MortalidadMensual_new")

df_MortalidadMensual_nueva = spark.sql(f"""SELECT * from {database_name_tmp}.MortalidadMensual_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadMensual")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MortalidadMensual"
}
df_MortalidadMensual_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.MortalidadMensual")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name_tmp}.MortalidadMensual_new")

print("carga INS MortalidadMensual --> Registros procesados:", df_MortalidadMensual_nueva.count())
df_ft_mortalidad_Mensual = spark.sql(f"SELECT \
 idmes ,pk_empresa,PobInicial,MortDia \
,round((PorcMortDia*100),2) as PorcMortDia \
,MortSem1,MortSem2,MortSem3,MortSem4,MortSem5,MortSem6,MortSem7 \
,MortSemAcum1,MortSemAcum2,MortSemAcum3,MortSemAcum4,MortSemAcum5,MortSemAcum6,MortSemAcum7 \
,case when MortDia = 0 then 0.0 else U_PENoViable / (MortDia*1.0) end as TasaNoViable \
,U_PEAccidentados,U_PEHigadoGraso,U_PEHepatomegalia,U_PEHigadoHemorragico,U_PEInanicion,U_PEProblemaRespiratorio \
,U_PESCH,U_PEEnteritis,U_PEAscitis,U_PEMuerteSubita,U_PEEstresPorCalor,U_PEHidropericardio,U_PEHemopericardio \
,U_PEUratosis,U_PEMaterialCaseoso,U_PEOnfalitis,U_PERetencionDeYema,U_PEErosionDeMolleja,U_PEHemorragiaMusculos \
,U_PESangreEnCiego,U_PEPericarditis,U_PEPeritonitis,U_PEProlapso,U_PEPicaje,U_PERupturaAortica,U_PEBazoMoteado \
,U_PENoViable \
,'' U_PECaja \
,'' U_PEGota            \
,'' U_PEIntoxicacion    \
,'' U_PERetrazos        \
,'' U_PEEliminados      \
,'' U_PEAhogados        \
,'' U_PEEColi           \
,'' U_PEDescarte        \
,'' U_PEOtros           \
,'' U_PECoccidia        \
,'' U_PEDeshidratados   \
,'' U_PEHepatitis       \
,'' U_PETraumatismo     \
,case when PobInicial = 0 then 0 else round(((U_PEAccidentados)/(PobInicial*1.0))*100,3) end as PorcAccidentados \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoGraso)/(PobInicial*1.0))*100,3) end as PorcHigadoGraso \
,case when PobInicial = 0 then 0 else round(((U_PEHepatomegalia)/(PobInicial*1.0))*100,3) end as PorcHepatomegalia \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoHemorragico)/(PobInicial*1.0))*100,3) end as PorcHigadoHemorragico \
,case when PobInicial = 0 then 0 else round(((U_PEInanicion)/(PobInicial*1.0))*100,3) end as PorcInanicion \
,case when PobInicial = 0 then 0 else round(((U_PEProblemaRespiratorio)/(PobInicial*1.0))*100,3) end as PorcProblemaRespiratorio \
,case when PobInicial = 0 then 0 else round(((U_PESCH)/(PobInicial*1.0))*100,3) end as PorcSCH \
,case when PobInicial = 0 then 0 else round(((U_PEEnteritis)/(PobInicial*1.0))*100,3) end as PorcEnteritis \
,case when PobInicial = 0 then 0 else round(((U_PEAscitis)/(PobInicial*1.0))*100,3) end as PorcAscitis \
,case when PobInicial = 0 then 0 else round(((U_PEMuerteSubita)/(PobInicial*1.0))*100,3) end as PorcMuerteSubita \
,case when PobInicial = 0 then 0 else round(((U_PEEstresPorCalor)/(PobInicial*1.0))*100,3) end as PorcEstresPorCalor \
,case when PobInicial = 0 then 0 else round(((U_PEHidropericardio)/(PobInicial*1.0))*100,3) end as PorcHidropericardio \
,case when PobInicial = 0 then 0 else round(((U_PEHemopericardio)/(PobInicial*1.0))*100,3) end as PorcHemopericardio \
,case when PobInicial = 0 then 0 else round(((U_PEUratosis)/(PobInicial*1.0))*100,3) end as PorcUratosis \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso \
,case when PobInicial = 0 then 0 else round(((U_PEOnfalitis)/(PobInicial*1.0))*100,3) end as PorcOnfalitis \
,case when PobInicial = 0 then 0 else round(((U_PERetencionDeYema)/(PobInicial*1.0))*100,3) end as PorcRetencionDeYema \
,case when PobInicial = 0 then 0 else round(((U_PEErosionDeMolleja)/(PobInicial*1.0))*100,3) end as PorcErosionDeMolleja \
,case when PobInicial = 0 then 0 else round(((U_PEHemorragiaMusculos)/(PobInicial*1.0))*100,3) end as PorcHemorragiaMusculos \
,case when PobInicial = 0 then 0 else round(((U_PESangreEnCiego)/(PobInicial*1.0))*100,3) end as PorcSangreEnCiego \
,case when PobInicial = 0 then 0 else round(((U_PEPericarditis)/(PobInicial*1.0))*100,3) end as PorcPericarditis \
,case when PobInicial = 0 then 0 else round(((U_PEPeritonitis)/(PobInicial*1.0))*100,3) end as PorcPeritonitis \
,case when PobInicial = 0 then 0 else round(((U_PEProlapso)/(PobInicial*1.0))*100,3) end as PorcProlapso \
,case when PobInicial = 0 then 0 else round(((U_PEPicaje)/(PobInicial*1.0))*100,3) end as PorcPicaje \
,case when PobInicial = 0 then 0 else round(((U_PERupturaAortica)/(PobInicial*1.0))*100,3) end as PorcRupturaAortica \
,case when PobInicial = 0 then 0 else round(((U_PEBazoMoteado)/(PobInicial*1.0))*100,3) end as PorcBazoMoteado \
,case when PobInicial = 0 then 0 else round(((U_PENoViable)/(PobInicial*1.0))*100,3) end as PorcNoViable \
,'' PorcCaja \
,'' PorcGota            \
,'' PorcIntoxicacion    \
,'' PorcRetrazos        \
,'' PorcEliminados      \
,'' PorcAhogados        \
,'' PorcEColi           \
,'' PorcDescarte        \
,'' PorcOtros           \
,'' PorcCoccidia        \
,'' PorcDeshidratados   \
,'' PorcHepatitis       \
,'' PorcTraumatismo     \
,FlagAtipico,FlagArtAtipico \
,U_PEAerosaculitisG2,U_PECojera,U_PEHigadoIcterico,U_PEMaterialCaseoso_po1ra,U_PEMaterialCaseosoMedRetr,U_PENecrosisHepatica \
,U_PENeumonia,U_PESepticemia,U_PEVomitoNegro \
,case when PobInicial = 0 then 0 else round(((U_PEAerosaculitisG2)/(PobInicial*1.0))*100,3) end as PorcAerosaculitisG2 \
,case when PobInicial = 0 then 0 else round(((U_PECojera)/(PobInicial*1.0))*100,3) end as PorcCojera \
,case when PobInicial = 0 then 0 else round(((U_PEHigadoIcterico)/(PobInicial*1.0))*100,3) end as PorcHigadoIcterico \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseoso_po1ra)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseoso_po1ra \
,case when PobInicial = 0 then 0 else round(((U_PEMaterialCaseosoMedRetr)/(PobInicial*1.0))*100,3) end as PorcMaterialCaseosoMedRetr \
,case when PobInicial = 0 then 0 else round(((U_PENecrosisHepatica)/(PobInicial*1.0))*100,3) end as PorcNecrosisHepatica \
,case when PobInicial = 0 then 0 else round(((U_PENeumonia)/(PobInicial*1.0))*100,3) end as PorcNeumonia \
,case when PobInicial = 0 then 0 else round(((U_PESepticemia)/(PobInicial*1.0))*100,3) end as PorcSepticemia \
,case when PobInicial = 0 then 0 else round(((U_PEVomitoNegro)/(PobInicial*1.0))*100,3) end as PorcVomitoNegro \
,U_PEAsperguillius,U_PEBazoGrandeMot,U_PECorazonGrande \
,case when PobInicial = 0 then 0 else round(((U_PEAsperguillius)/(PobInicial*1.0))*100,3) end as PorcAsperguillius \
,case when PobInicial = 0 then 0 else round(((U_PEBazoGrandeMot)/(PobInicial*1.0))*100,3) end as PorcBazoGrandeMot \
,case when PobInicial = 0 then 0 else round(((U_PECorazonGrande)/(PobInicial*1.0))*100,3) end as PorcCorazonGrande \
,MortSem8,MortSem9,MortSem10,MortSem11,MortSem12,MortSem13,MortSem14,MortSem15,MortSem16,MortSem17,MortSem18,MortSem19,MortSem20 \
,MortSemAcum8,MortSemAcum9,MortSemAcum10,MortSemAcum11,MortSemAcum12,MortSemAcum13,MortSemAcum14,MortSemAcum15,MortSemAcum16 \
,MortSemAcum17,MortSemAcum18,MortSemAcum19,MortSemAcum20,U_PECuadroToxico \
,case when PobInicial = 0 then 0 else round(((U_PECuadroToxico)/(PobInicial*1.0))*100,3) end as PorcCuadroToxico \
,PavosBBMortIncub \
,'' U_PENoEspecificado \
,'' U_PEAnalisis_Laboratorio    \
,'' PorcNoEspecificado          \
,'' PorcAnalisisLaboratorio     \
,idmes DescripMes                  \
,'' DescripEmpresa              \
FROM {database_name_tmp}.MortalidadMensual A \
WHERE (idmes >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM'))")
print('carga df_ft_mortalidad_Mensual', df_ft_mortalidad_Mensual.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y%m")

try:
    df_existentes29 = spark.read.format("parquet").load(path_target29)
    datos_existentes29 = True
    logger.info(f"Datos existentes de ft_mortalidad_Mensual cargados: {df_existentes29.count()} registros")
except:
    datos_existentes29 = False
    logger.info("No se encontraron datos existentes en ft_mortalidad_Mensual")


if datos_existentes29:
    existing_data29 = spark.read.format("parquet").load(path_target29)
    data_after_delete29 = existing_data29.filter(~((col("idmes") >= fecha_str)))
    filtered_new_data29 = df_ft_mortalidad_Mensual.filter((col("idmes") >= fecha_str))
    final_data29 = filtered_new_data29.union(data_after_delete29)                             
   
    cant_ingresonuevo29 = filtered_new_data29.count()
    cant_total29 = final_data29.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_MensualTemporal"
    }
    final_data29.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_mortalidad_MensualTemporal")

    final_data29_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_mortalidad_MensualTemporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_mensual"
    }
    final_data29_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_mensual")
            
    print(f"agrega registros nuevos a la tabla ft_mortalidad_Mensual : {cant_ingresonuevo29}")
    print(f"Total de registros en la tabla ft_mortalidad_Mensual : {cant_total29}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_mortalidad_MensualTemporal')
    print(f"Tabla ft_mortalidad_MensualTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_mortalidad_mensual"
    }
    df_ft_mortalidad_Mensual.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_mortalidad_mensual")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()