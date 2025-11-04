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
JOB_NAME = "nt_prd_tbl_Peso_gold"

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

file_name_target19 = f"{bucket_name_prdmtech}ft_peso_Diario/"
path_target19 = f"s3://{bucket_name_target}/{file_name_target19}"
print('cargando rutas')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
df_MaxPesodia = spark.sql(f"select ComplexEntityNo,pk_diasvida,nvl(MAX(Pesodia),0) MaxPesodia \
FROM {database_name}.stg_ProduccionDetalle  group by  ComplexEntityNo,pk_diasvida")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxPesodia"
}
df_MaxPesodia.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaxPesodia")
print('carga MaxPesodia', df_MaxPesodia.count())
#Tabla temporal para insertar el peso en los dias 4,5,7,14,21,28,35,42,49,56,
df_PesoAcumuladorTemp = spark.sql(f"""SELECT M.ComplexEntityNo 
,MAX(M.xDate) xDate,M.pk_diasvida,M.pk_semanavida,MAX(M.Pesodia)Peso 
,CASE WHEN M.pk_diasvida IN (8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) AND SUM(M.Pesodia) = 0 THEN MAX(MT1.MaxPesodia) 
      WHEN M.pk_diasvida in (5,6,8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) then MAX(M.Pesodia) 
      ELSE 0 END PesoSem 
,MAX(MT2.MaxPesodia) PesoDiarioAcum 
FROM {database_name}.stg_ProduccionDetalle M 
left join {database_name}.MaxPesodia MT1 on MT1.ComplexEntityNo = M.ComplexEntityNo and MT1.pk_diasvida BETWEEN M.pk_diasvida AND M.pk_diasvida + 2 
left join {database_name}.MaxPesodia MT2 on MT2.ComplexEntityNo = M.ComplexEntityNo and MT2.pk_diasvida <= M.pk_diasvida 
WHERE (date_format(M.descripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
GROUP BY M.ComplexEntityNo,M.pk_diasvida,M.pk_semanavida""")
#df_PesoAcumuladorTemp.createOrReplaceTempView("PesoAcumulador")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PesoAcumulador"
}
df_PesoAcumuladorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PesoAcumulador")
print('carga PesoAcumulador',df_PesoAcumuladorTemp.count())
df_SumPesoDiarioAcum = spark.sql(f"select ComplexEntityNo,pk_diasvida,pk_semanavida,nvl(sum(PesoDiarioAcum),0) PesoDiarioAcum \
from {database_name}.PesoAcumulador  group by ComplexEntityNo,pk_diasvida,pk_semanavida ")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumPesoDiarioAcum"
}
df_SumPesoDiarioAcum.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumPesoDiarioAcum")
print('carga SumPesoDiarioAcum',df_SumPesoDiarioAcum.count())
##Tabla temporal para calcular el peso semanal entre 7
#df_gananciaTemp1 = spark.sql(f"select \
#M.ComplexEntityNo, \
#M.xDate, \
#M.pk_diasvida, \
#M.pk_semanavida, \
#case when M.pk_diasvida in (5,6,8,45,52,59,66,73,80,87,94,101,108,115,122,129,17,25,32,40,133+1,140+1) then 0 else (M.PesoDiarioAcum / 7) end as ganancia, \
#case when M.pk_diasvida in (5,6,8,45,52,59,66,73,80,87,94,101,108,115,122,129,17,25,32,40,133+1,140+1) then 0 \
#else MT.PesoDiarioAcum / 7 end as acumganancia \
#from {database_name}.PesoAcumulador M \
#left join (Select ComplexEntityNo,pk_diasvida,pk_semanavida, NVL(sum(PesoDiarioAcum),0) PesoDiarioAcum \
#From {database_name}.PesoAcumulador Group By ComplexEntityNo,pk_diasvida,pk_semanavida) MT \
#on MT.ComplexEntityNo= M.ComplexEntityNo and MT.pk_diasvida<=M.pk_diasvida AND MT.pk_semanavida = M.pk_semanavida \
#where substring(M.complexentityno,1,1) like 'P%'")
#print('carga df_gananciaTemp1')
#
##left join {database_name}.SumPesoDiarioAcum MT on MT.ComplexEntityNo= M.ComplexEntityNo and MT.pk_diasvida<=M.pk_diasvida AND MT.pk_semanavida = M.pk_semanavida \
df_gananciaTemp1 = spark.sql(f"WITH BSD AS ( \
    SELECT *,  \
           SUM(PesoDiarioAcum) OVER ( \
                PARTITION BY ComplexEntityNo, pk_semanavida \
                ORDER BY pk_diasvida  \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
           ) AS acumganancia \
    FROM {database_name}.PesoAcumulador \
) \
SELECT ComplexEntityNo,xDate,pk_diasvida,pk_semanavida, \
		case  \
			when pk_diasvida in (5,6,8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) then 0 \
			else PesoDiarioAcum / 7 end as ganancia, \
		case  \
			when pk_diasvida in (5,6,8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) then 0 \
			else acumganancia / 7 end acumganancia \
FROM BSD \
WHERE substring(complexentityno,1,1) like 'P%' \
ORDER BY ComplexEntityNo, pk_semanavida, pk_diasvida")
print('carga df_gananciaTemp1',df_gananciaTemp1.count())
##insert into	#ganancia --drop table #ganancia
#df_gananciaTemp2 = spark.sql(f"select M.ComplexEntityNo,M.xDate,M.pk_diasvida,M.pk_semanavida, \
#case when M.pk_diasvida in (8,45,52,59,66,73,80,87,94,101,108,115,122,129,17,25,32,40,133+1,140+1) then 0 else (M.PesoDiarioAcum / 7) end as ganancia, \
#case when M.pk_diasvida in (8,45,52,59,66,73,80,87,94,101,108,115,122,129,17,25,32,40,133+1,140+1) then 0 \
#else MT.PesoDiarioAcum / 7 end as acumganancia \
#from {database_name}.PesoAcumulador M \
#left join (Select ComplexEntityNo,pk_diasvida,pk_semanavida, NVL(sum(PesoDiarioAcum),0) PesoDiarioAcum \
#From {database_name}.PesoAcumulador Group By ComplexEntityNo,pk_diasvida,pk_semanavida) MT \
#on MT.ComplexEntityNo= M.ComplexEntityNo and MT.pk_diasvida<=M.pk_diasvida AND MT.pk_semanavida = M.pk_semanavida \
#where substring(M.complexentityno,1,1) like 'V%'")  
#print('carga df_gananciaTemp2')
#
##left join {database_name}.SumPesoDiarioAcum MT on MT.ComplexEntityNo= M.ComplexEntityNo and MT.pk_diasvida<=M.pk_diasvida AND MT.pk_semanavida = M.pk_semanavida \
df_gananciaTemp2 = spark.sql(f"WITH BSD AS ( \
    SELECT *,  \
           SUM(PesoDiarioAcum) OVER ( \
                PARTITION BY ComplexEntityNo, pk_semanavida \
                ORDER BY pk_diasvida  \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
           ) AS acumganancia \
    FROM {database_name}.PesoAcumulador \
) \
SELECT ComplexEntityNo,xDate,pk_diasvida,pk_semanavida, \
		case  \
			when pk_diasvida in (8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) then 0 \
			else PesoDiarioAcum / 7 end as ganancia, \
		case  \
			when pk_diasvida in (8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) then 0 \
			else acumganancia / 7 end acumganancia \
FROM BSD \
WHERE substring(complexentityno,1,1) like 'V%' \
ORDER BY ComplexEntityNo, pk_semanavida, pk_diasvida")
print('carga df_gananciaTemp2',df_gananciaTemp2.count())
df_ganancia = df_gananciaTemp1.union(df_gananciaTemp2)
#df_ganancia.createOrReplaceTempView("ganancia")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ganancia"
}
df_ganancia.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ganancia")
print('carga ganancia',df_ganancia.count())
#Tabla para agrupar el peso y ganancia por día
#df_PesoGananciaTemp = spark.sql(f"select ComplexEntityNo,xDate,FirstHatchDateAge, MAX(nvl(PESO,0)) Peso, min(nvl(Ganancia,0)) Ganancia \
#FROM GSF_DMPecuario.STGPecuario.Pesos\
#group by ComplexEntityNo,xDate,FirstHatchDateAge")
#df_PesoGananciaTemp.createOrReplaceTempView("PesoGanancia")
#Realiza el cálculo de Pollos rendidos
df_PollosRendidosTemp = spark.sql(f"""select 
ProteinFarmsIRN,ComplexEntityNo,sum(FarmHdCount) as AvesRendidas,
sum(FarmWtNet) as KilosRendidos,min(cast(cast(xdate as timestamp) as date)) as InicioSaca,max(cast(cast(xdate as timestamp) as date)) as FinSaca 
from {database_name}.si_mvpmtsprocrecvtranshousedetail 
where (date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
and PostTransactionId is not null 
group by proteinfarmsirn,complexentityno""")
#df_PollosRendidosTemp.createOrReplaceTempView("PollosRendidos")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PollosRendidos"
}
df_PollosRendidosTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PollosRendidos")
print('carga PollosRendidos',df_PollosRendidosTemp.count())
##Realiza el cálculo de Pollos rendidos
#df_PollosRendidosTemp = spark.sql(f"""select 
#ProteinFarmsIRN,ComplexEntityNo,sum(FarmHdCount) as AvesRendidas,
#sum(FarmWtNet) as KilosRendidos,min(cast(cast(xdate as timestamp) as date)) as InicioSaca,max(cast(cast(xdate as timestamp) as date)) as FinSaca 
#from (select * from {database_name}.si_mvpmtsprocrecvtranshousedetail 
#      where (date_format(cast(eventdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-12),'yyyyMM')) ) 
#where (date_format(cast(xdate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
#and PostTransactionId is not null 
#group by proteinfarmsirn,complexentityno""")
##df_PollosRendidosTemp.createOrReplaceTempView("PollosRendidos")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PollosRendidos"
#}
#df_PollosRendidosTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.PollosRendidos")
#print('carga PollosRendidos',df_PollosRendidosTemp.count())
#Tabla temporal para obtener Incubadora mayoritaria
df_ft_alojamientoTemp = spark.sql(f"SELECT CAST(ComplexEntityNo as varchar(50)) ComplexEntityNo,RazaMayor,ListaPadre,ListaIncubadora,IncubadoraMayor \
,AVG(TotalAloj)TotalAloj,AVG(PesoHvoPond) PesoHvoPond,MAX(CantAlojXROSS) CantAlojXROSS,MAX(PorcAlojXROSS) PorcAlojXROSS,MAX(CantAlojXTROSS) CantAlojXTROSS,MAX(PorcAlojXTROSS) PorcAlojXTROSS \
,MAX(PesoAlojPond) PesoAlojPond,MAX(PorcRazaMayor) PorcRaza,TipoOrigen \
FROM {database_name}.ft_alojamiento A \
where pk_empresa = 1 \
GROUP BY ComplexEntityNo,ListaPadre,ListaIncubadora,IncubadoraMayor,RazaMayor,TipoOrigen")
#df_ft_alojamientoTemp.createOrReplaceTempView("ft_alojamientoTemp")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_alojamientoTemp"
}
df_ft_alojamientoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_alojamientoTemp")
print('carga ft_alojamientoTemp',df_ft_alojamientoTemp.count())
## df_TablaPivotPesoSemTemp = spark.sql(f"select ComplexEntityNo,max(`1`) PesoSem1,max(`2`) PesoSem2, max(`3`) PesoSem3, max(`4`) PesoSem4, max(`5`) PesoSem5, max(`6`) PesoSem6,max(`7`) PesoSem7, \
## max(`8`) PesoSem8,max(`9`) PesoSem9,max(`10`) PesoSem10,max(`11`) PesoSem11,max(`12`) PesoSem12,max(`13`) PesoSem13,max(`14`) PesoSem14,max(`15`) PesoSem15,max(`16`) PesoSem16,max(`17`) PesoSem17, \
## max(`18`) PesoSem18,max(`19`) PesoSem19,max(`20`) PesoSem20 \
## from ( \
## select ComplexEntityNo,'' as `1`,'' as `2`,'' as `3`,'' as `4`,'' as `5`,'' as `6`,'' as `7`,'' as `8`,'' as `9`,'' as `10`,'' as `11`,'' as `12`,'' as `13`,'' as `14`,'' as `15`,'' as `16`,'' as `17`,'' as `18`,'' as `19`,'' as `20` \
## from {database_name}.PesoAcumulador \
## pivot \
## ( \
## max(PesoSem) \
## for pk_semanavida in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) \
## ))A \
## group by A.ComplexEntityNo")
## #df_TablaPivotPesoSemTemp.createOrReplaceTempView("TablaPivotPesoSem")
## # Escribir los resultados en ruta temporal
## additional_options = {
## "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotPesoSem"
## }
## df_TablaPivotPesoSemTemp.write \
##     .format("parquet") \
##     .options(**additional_options) \
##     .mode("overwrite") \
##     .saveAsTable(f"{database_name}.TablaPivotPesoSem")
## print('carga TablaPivotPesoSem')
df_TablaPivotPesoSemTemp = spark.sql(f"select ComplexEntityNo,max(`2`) PesoSem1,max(`3`) PesoSem2, max(`4`) PesoSem3, max(`5`) PesoSem4, max(`6`) PesoSem5, max(`7`) PesoSem6,max(`8`) PesoSem7, \
max(`9`) PesoSem8,max(`10`) PesoSem9,max(`11`) PesoSem10,max(`12`) PesoSem11,max(`13`) PesoSem12,max(`14`) PesoSem13,max(`15`) PesoSem14,max(`16`) PesoSem15,max(`17`) PesoSem16,max(`18`) PesoSem17, \
max(`19`) PesoSem18,max(`20`) PesoSem19,max(`21`) PesoSem20,max(`22`) PesoSem21 \
from ( \
select ComplexEntityNo, `2`,`3`,`4`,`5`,`6`,`7`,`8`,`9`,`10`,`11`,`12`,`13`,`14`,`15`,`16`,`17`,`18`,`19`,`20`,`21`,`22`   \
from {database_name}.PesoAcumulador \
pivot (max(PesoSem) \
for pk_semanavida in (2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22) \
))A \
group by A.ComplexEntityNo")
#df_TablaPivotPesoSemTemp.createOrReplaceTempView("TablaPivotPesoSem")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotPesoSem"
}
df_TablaPivotPesoSemTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaPivotPesoSem")
print('carga TablaPivotPesoSem',df_TablaPivotPesoSemTemp.count())
#Tabla donde se inserta las dimensiones, métricas e indicadores de pesos
df_PesoDetalleTemp1=spark.sql(f"select \
 PD.pk_tiempo \
,PD.pk_empresa \
,nvl(PD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) as pk_division \
,nvl(PD.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) as pk_zona \
,nvl(PD.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) as pk_subzona \
,nvl(PD.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) as pk_plantel \
,nvl(PD.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) as pk_lote \
,nvl(PD.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) as pk_galpon \
,nvl(PD.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo \
,nvl(PD.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard \
,nvl(PD.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(PD.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(LEP.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))pk_especie \
,nvl(PD.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(PD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador \
,nvl(PD.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,PD.pk_semanavida \
,PD.pk_diasvida \
,nvl(PD.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimento \
,PD.ComplexEntityNo \
,PD.FechaNacimiento \
,PD.Edad as Edad \
,nvl(FIC.Inventario,0) AS PobInicial \
,nvl(PR.AvesRendidas,0) AS AvesRendidas \
,nvl(PR.KilosRendidos,0) AS KilosRendidos \
,case when ACPE.PesoDiarioAcum = 0.0 then nvl(PD.STDPeso,0.0) + nvl(GAN.acumganancia,0.0) else ACPE.PesoDiarioAcum + nvl(GAN.acumganancia,0.0) end as PesoDia \
,nvl(PD.Pesodia,0) AS Peso \
,nvl(ACPE.PesoDiarioAcum,0) AS PesoDiaAcum \
,case when PD.pk_diasvida = 2 then nvl(ALO.PesoAlojPond,0)/1000 else ACPE.PesoSem end AS PesoSem \
,nvl((select avg(PesoSem) from {database_name}.PesoAcumulador a where a.ComplexEntityNo = PD.ComplexEntityNo and pk_diasvida = 6),0) as Peso5Dias \
,nvl(TPM.PesoSem1,0) PesoSem1 \
,nvl(TPM.PesoSem2,0) PesoSem2 \
,nvl(TPM.PesoSem3,0) PesoSem3 \
,nvl(TPM.PesoSem4,0) PesoSem4 \
,nvl(TPM.PesoSem5,0) PesoSem5 \
,nvl(TPM.PesoSem6,0) PesoSem6 \
,nvl(TPM.PesoSem7,0) PesoSem7 \
,nvl(TPM.PesoSem8,0) PesoSem8 \
,nvl(TPM.PesoSem9,0) PesoSem9 \
,nvl(TPM.PesoSem10,0) PesoSem10 \
,nvl(TPM.PesoSem11,0) PesoSem11 \
,nvl(TPM.PesoSem12,0) PesoSem12 \
,nvl(TPM.PesoSem13,0) PesoSem13 \
,nvl(TPM.PesoSem14,0) PesoSem14 \
,nvl(TPM.PesoSem15,0) PesoSem15 \
,nvl(TPM.PesoSem16,0) PesoSem16 \
,nvl(TPM.PesoSem17,0) PesoSem17 \
,nvl(TPM.PesoSem18,0) PesoSem18 \
,nvl(TPM.PesoSem19,0) PesoSem19 \
,nvl(TPM.PesoSem20,0) PesoSem20 \
,nvl(ALO.PesoAlojPond,0) as PesoAlo \
,nvl(PD.STDPeso,0.0) as STDPeso \
,ALO.PorcRaza PorcCodigoRaza \
,PD.BrimFieldTransIRN as IRN \
,ALO.IncubadoraMayor \
,ALO.ListaIncubadora \
,ALO.ListaPadre \
,PD.U_categoria categoria \
,nvl(PD.FlagAtipico,1) FlagAtipico \
,ALO.PesoHvoPond \
,ALO.CantAlojXROSS \
,ALO.PorcAlojXROSS \
,ALO.CantAlojXTROSS \
,ALO.PorcAlojXTROSS \
,PD.FlagTransfPavos \
,PD.SourceComplexEntityNo \
,PD.DestinationComplexEntityNo \
,PD.U_CausaPesoBajo \
,PD.U_AccionPesoBajo \
,PD.U_RuidosTotales \
,ALO.TipoOrigen \
,PD.descripFecha fecha \
from {database_name}.stg_ProduccionDetalle PD \
LEFT JOIN {database_name}.ft_ingresocons FIC ON FIC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name}.PollosRendidos PR ON cast(PR.ProteinFarmsIRN as varchar(50)) = cast(PD.ProteinFarmsIRN as varchar(50)) and PR.complexentityno =PD.ComplexEntityNo \
LEFT JOIN {database_name}.PesoAcumulador ACPE ON ACPE.ComplexEntityNo=PD.ComplexEntityNo AND PD.pk_diasvida= ACPE.pk_diasvida \
LEFT JOIN {database_name}.ganancia GAN ON GAN.ComplexEntityNo=PD.ComplexEntityNo AND PD.pk_diasvida= GAN.pk_diasvida \
LEFT JOIN {database_name}.TablaPivotPesoSem TPM ON PD.ComplexEntityNo = TPM.ComplexEntityNo \
LEFT JOIN {database_name}.ft_alojamientoTemp ALO ON PD.complexentityno = ALO.complexentityno \
LEFT JOIN {database_name}.lk_especie LEP ON LEP.cespecie = ALO.RazaMayor \
WHERE PD.pk_diasvida > 0 AND \
(date_format(PD.descripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
and PD.pk_plantel not in (91,451) and PD.GRN = 'P' and PD.pk_division = 4 \
and PD.pk_lote not in (select pk_lote from {database_name}.lk_lote where (clote like 'P186%' AND substring(clote,8,4) >= '11'))") 
#and PD.pk_lote in (select pk_lote from {database_name}.lk_lote where substring(clote,8,4) in ('01','02','03','04','05','06','07','08','09','10') )")
print('carga df_PesoDetalleTemp1',df_PesoDetalleTemp1.count())
#72,82
# 3 --solo pollos
#--nnvl(TPES.Peso,0) AS Peso_STD \
#--,nvl(TPES.Ganancia,0) AS Ganancia \ 
#--LEFT JOIN PesoGanancia TPES ON TPES.ComplexEntityNo = PD.ComplexEntityNo and TPES.FirstHatchDateAge = PD.Edad--MO.FirstHatchDateAge \
#insert into STGPECUARIO.PesoDetalle
df_PesoDetalleTemp2=spark.sql(f"select\
 PD.pk_tiempo \
,PD.pk_empresa \
,nvl(PD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) as pk_division \
,nvl(PD.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) as pk_zona \
,nvl(PD.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) as pk_subzona \
,nvl(PD.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) as pk_plantel \
,nvl(PD.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) as pk_lote \
,nvl(PD.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) as pk_galpon \
,nvl(PD.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo \
,nvl(PD.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard \
,nvl(PD.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(PD.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(LEP.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))pk_especie \
,nvl(PD.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(PD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador \
,nvl(PD.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,PD.pk_semanavida \
,PD.pk_diasvida \
,nvl(PD.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimento \
,PD.ComplexEntityNo \
,PD.FechaNacimiento \
,PD.Edad as Edad \
,nvl(FIC.Inventario,0) AS PobInicial \
,nvl(PR.AvesRendidas,0) AS AvesRendidas \
,nvl(PR.KilosRendidos,0) AS KilosRendidos \
,case when ACPE.PesoDiarioAcum = 0 then nvl(PD.STDPeso,0.0) + nvl(GAN.acumganancia,0.0) else ACPE.PesoDiarioAcum + nvl(GAN.acumganancia,0.0) end as PesoDia \
,nvl(PD.Pesodia,0) AS Peso \
,nvl(ACPE.PesoDiarioAcum,0) AS PesoDiaAcum \
,case when PD.pk_diasvida = 2 then nvl(PD.Pesodia,0) else ACPE.PesoSem end AS PesoSem \
,nvl((select avg(PesoSem) from {database_name}.PesoAcumulador a where a.ComplexEntityNo = PD.ComplexEntityNo and pk_diasvida = 6),0) as Peso5Dias \
,nvl(TPM.PesoSem1,0) PesoSem1 \
,nvl(TPM.PesoSem2,0) PesoSem2 \
,nvl(TPM.PesoSem3,0) PesoSem3 \
,nvl(TPM.PesoSem4,0) PesoSem4 \
,nvl(TPM.PesoSem5,0) PesoSem5 \
,nvl(TPM.PesoSem6,0) PesoSem6 \
,nvl(TPM.PesoSem7,0) PesoSem7 \
,nvl(TPM.PesoSem8,0) PesoSem8 \
,nvl(TPM.PesoSem9,0) PesoSem9 \
,nvl(TPM.PesoSem10,0) PesoSem10 \
,nvl(TPM.PesoSem11,0) PesoSem11 \
,nvl(TPM.PesoSem12,0) PesoSem12 \
,nvl(TPM.PesoSem13,0) PesoSem13 \
,nvl(TPM.PesoSem14,0) PesoSem14 \
,nvl(TPM.PesoSem15,0) PesoSem15 \
,nvl(TPM.PesoSem16,0) PesoSem16 \
,nvl(TPM.PesoSem17,0) PesoSem17 \
,nvl(TPM.PesoSem18,0) PesoSem18 \
,nvl(TPM.PesoSem19,0) PesoSem19 \
,nvl(TPM.PesoSem20,0) PesoSem20 \
,nvl(ALO.PesoAlojPond,0) as PesoAlo \
,nvl(PD.STDPeso,0.0) as STDPeso \
,ALO.PorcRaza PorcCodigoRaza \
,PD.BrimFieldTransIRN as IRN \
,ALO.IncubadoraMayor \
,ALO.ListaIncubadora \
,ALO.ListaPadre \
,PD.U_categoria categoria \
,nvl(PD.FlagAtipico,1) FlagAtipico \
,ALO.PesoHvoPond \
,ALO.CantAlojXROSS \
,ALO.PorcAlojXROSS \
,ALO.CantAlojXTROSS \
,ALO.PorcAlojXTROSS \
,PD.FlagTransfPavos \
,PD.ComplexEntityNo SourceComplexEntityNo \
,PD.ComplexEntityNo DestinationComplexEntityNo \
,PD.U_CausaPesoBajo \
,PD.U_AccionPesoBajo \
,PD.U_RuidosTotales \
,ALO.TipoOrigen \
,PD.descripFecha fecha \
from {database_name}.stg_ProduccionDetalle PD \
LEFT JOIN {database_name}.ft_ingresocons FIC ON FIC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name}.PollosRendidos PR ON cast(PR.ProteinFarmsIRN as varchar(50)) = cast(PD.ProteinFarmsIRN as varchar(50)) and PR.complexentityno =PD.ComplexEntityNo \
LEFT JOIN {database_name}.PesoAcumulador ACPE ON ACPE.ComplexEntityNo=PD.ComplexEntityNo AND PD.pk_diasvida= ACPE.pk_diasvida \
LEFT JOIN {database_name}.ganancia GAN ON GAN.ComplexEntityNo=PD.ComplexEntityNo AND PD.pk_diasvida= GAN.pk_diasvida \
LEFT JOIN {database_name}.TablaPivotPesoSem TPM ON PD.ComplexEntityNo = TPM.ComplexEntityNo \
LEFT JOIN {database_name}.ft_alojamientoTemp ALO ON PD.complexentityno = ALO.complexentityno \
LEFT JOIN {database_name}.lk_especie LEP ON LEP.cespecie = ALO.RazaMayor \
WHERE PD.pk_diasvida > 0 AND (date_format(PD.descripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) and PD.GRN = 'P' and PD.pk_division = 2")
#4 --solo pavos
#--,nvl(TPES.Peso,0) AS Peso_STD \
#--,nvl(TPES.Ganancia,0) AS Ganancia \
#--LEFT JOIN PesoGanancia TPES ON TPES.ComplexEntityNo = PD.ComplexEntityNo and TPES.FirstHatchDateAge = PD.Edad--MO.FirstHatchDateAge \
print('carga df_PesoDetalleTemp2',df_PesoDetalleTemp2.count())
df_PesoDetalleTemp = df_PesoDetalleTemp1.union(df_PesoDetalleTemp2)
#df_PesoDetalleTemp.createOrReplaceTempView("PesoDetalle")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PesoDetalle"
}
df_PesoDetalleTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PesoDetalle")
print('carga PesoDetalle',df_PesoDetalleTemp.count())
#Tabla temporal donde inserta pesos diferente a 0
df_PesoTemp =  spark.sql(f"select ComplexEntityNo, max(Peso) Peso, min(pk_diasvida) pk_diasvida,DENSE_RANK() OVER(PARTITION BY ComplexEntityNo ORDER BY min(pk_diasvida)) Orden \
from(select ComplexEntityNo, sum(peso) Peso, pk_diasvida, DENSE_RANK() OVER(PARTITION BY ComplexEntityNo ORDER BY sum(peso)) Orden \
from {database_name}.PesoDetalle \
Group by ComplexEntityNo,pk_diasvida \
) A \
where peso <> 0 \
Group by A.ComplexEntityNo, A.Orden \
Order by A.ComplexEntityNo, A.Orden")
#df_PesoTemp.createOrReplaceTempView("Peso")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Peso"
}
df_PesoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Peso")
print('carga Peso',df_PesoTemp.count())
#Tabla temporal donde se calcula la Ganancia Peso Diario
df_GananciaPesoDiaTemp = spark.sql(f"select \
A.ComplexEntityNo, \
A.pk_diasvida, \
A.Orden, \
A.Peso, \
nvl(A.Peso - B.Peso,0) as DifPesoActPesoAnt, \
nvl(A.pk_diasvida - B.pk_diasvida,0) as CantDia, \
nvl((A.Peso - B.Peso) / (A.pk_diasvida - B.pk_diasvida),0) GananciaPesoDia \
from {database_name}.Peso A \
left join {database_name}.Peso B on A.ComplexEntityNo = B.ComplexEntityNo and A.Orden = B.Orden + 1 \
order by A.Orden")
#df_GananciaPesoDiaTemp.createOrReplaceTempView("GananciaPesoDia")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/GananciaPesoDia"
}
df_GananciaPesoDiaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.GananciaPesoDia")
print('carga GananciaPesoDia',df_GananciaPesoDiaTemp.count())
df_ConcatCorralTemp = spark.sql(f"select A.ComplexEntityNo,A.pk_tiempo,A.fecha,A.pk_semanavida,B.semanaIncub pk_semanaCalenIncub, \
CONCAT(substring(A.complexentityno,1,(length(A.complexentityno)-6)),' ',RTRIM(B.semanaIncub), '-' ,A.pk_semanavida) ConcatCorral, \
CONCAT(rtrim(B.semanaIncub),'-',B.anio,'-',A.pk_semanavida) ConcatSemAnioCorral \
from (select PD.ComplexEntityNo,MAX(PD.pk_tiempo) pk_tiempo, MAX(PD.fecha) fecha,PD.pk_empresa, PD.pk_division, MAX(pk_semanavida) pk_semanavida \
from {database_name}.PesoDetalle PD \
where PesoSem <> 0 and PD.pk_empresa = 1 and PD.pk_division = 4 \
group by PD.ComplexEntityNo, PD.pk_empresa, PD.pk_division) A \
left join {database_name}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo") 
                                #3 pollos
#df_ConcatCorralTemp.createOrReplaceTempView("ConcatCorral")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConcatCorral"
}
df_ConcatCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConcatCorral")
print('carga ConcatCorral',df_ConcatCorralTemp.count())
df_TotalGeneralCorralTemp = spark.sql(f"select SUM(X.PobInicial) PobInicial, SUM(X.PobInicialXPesoSem) PobInicialXPesoSem,SUM(X.PobInicialXPesoSem)/SUM(X.PobInicial) PesoSemAntCorral, \
X.ConcatCorral, X.pk_semanavida \
from \
(select A.pk_tiempo,A.ComplexEntityNo,A.PobInicial,A.PesoSem,A.PobInicial * A.PesoSem PobInicialXPesoSem, ConcatCorral, ConcatSemAnioCorral,A.pk_semanavida \
from \
(select PD.pk_tiempo, PD.ComplexEntityNo,MAX(PD.PobInicial) PobInicial, MAX(PD.PesoSem) PesoSem, PD.pk_semanavida \
from {database_name}.PesoDetalle PD \
where PD.pk_diasvida >= 8 and PD.pk_empresa = 1 and PD.pk_division = 4 \
group by PD.pk_tiempo, PD.ComplexEntityNo, PD.pk_semanavida) A \
left join {database_name}.ConcatCorral B on A.ComplexEntityNo = B.ComplexEntityNo \
where A.PesoSem <> 0 \
) X \
group by X.ConcatCorral, X.pk_semanavida") 
#df_TotalGeneralCorralTemp.createOrReplaceTempView("TotalGeneralCorral")  
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TotalGeneralCorral"
}
df_TotalGeneralCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TotalGeneralCorral")
print('carga TotalGeneralCorral',df_TotalGeneralCorralTemp.count())
df_ConcatLoteTemp = spark.sql(f"select A.ComplexEntityNoLote ComplexEntityNo,A.pk_tiempo,A.fecha, A.pk_semanavida,B.semanaIncub pk_semanaCalenIncub, \
CONCAT(A.ComplexEntityNoLote,' ',RTRIM(B.semanaIncub), '-' ,A.pk_semanavida) ConcatLote, \
CONCAT(rtrim(B.semanaIncub),'-',B.anio,'-',A.pk_semanavida) ConcatSemAnioLote \
from (select substring(PD.complexentityno,1,(length(PD.complexentityno)-6)) ComplexEntityNoLote,MAX(PD.pk_tiempo) pk_tiempo, MAX(PD.fecha) fecha,PD.pk_empresa, PD.pk_division, MAX(pk_semanavida) pk_semanavida \
from {database_name}.PesoDetalle PD \
where PesoSem <> 0 \
group by substring(PD.complexentityno,1,(length(PD.complexentityno)-6)), PD.pk_empresa, PD.pk_division) A \
left join {database_name}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo \
where A.pk_empresa = 1 and A.pk_division = 4")
#df_ConcatLoteTemp.createOrReplaceTempView("ConcatLote")                              
                              #A.pk_division = 3
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConcatLote"
}
df_ConcatLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConcatLote")
print('carga ConcatLote',df_ConcatLoteTemp.count())
df_TotalGeneralLoteTemp = spark.sql(f"select SUM(X.PobInicial) PobInicial, SUM(X.PobInicialXPesoSem) PobInicialXPesoSem,SUM(X.PobInicialXPesoSem)/SUM(X.PobInicial) PesoSemAntLote, \
X.ConcatSemAnioLote, X.pk_semanavida \
from \
(select PDR.pk_tiempo,substring(PDR.complexentityno,1,(length(PDR.complexentityno)-6)) complexentityno,PDR.complexentityno complexentityno2,PDR.PobInicial,PDR.PesoSem, \
PDR.PobInicial * PDR.PesoSem PobInicialXPesoSem,PDR.pk_semanavida,ConcatLote, ConcatSemAnioLote \
from \
(select PD.pk_tiempo, PD.ComplexEntityNo,MAX(PD.PobInicial) PobInicial, MAX(PD.PesoSem) PesoSem, PD.pk_semanavida \
from {database_name}.PesoDetalle PD \
where PD.pk_diasvida >= 8 \
and PD.PesoSem <> 0 \
group by PD.pk_tiempo, PD.ComplexEntityNo, PD.pk_semanavida \
)PDR \
left join {database_name}.ConcatLote B on substring(PDR.complexentityno,1,(length(PDR.complexentityno)-6)) = B.ComplexEntityNo \
) X \
group by X.ConcatSemAnioLote, X.pk_semanavida")
#df_TotalGeneralLoteTemp.createOrReplaceTempView("TotalGeneralLote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TotalGeneralLote"
}
df_TotalGeneralLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TotalGeneralLote")
print('carga TotalGeneralLote',df_TotalGeneralLoteTemp.count())
df_ft_peso_Diario = spark.sql(f"""
SELECT 
a.pk_tiempo
,a.pk_empresa 
,a.pk_division 
,a.pk_zona 
,a.pk_subzona 
,a.pk_plantel 
,a.pk_lote 
,a.pk_galpon 
,a.pk_sexo 
,a.pk_standard 
,a.pk_producto 
,a.pk_tipoproducto 
,a.pk_especie 
,a.pk_estado 
,a.pk_administrador 
,a.pk_proveedor 
,a.pk_semanavida 
,a.pk_diasvida 
,a.ComplexEntityNo 
,a.FechaNacimiento 
,a.IncubadoraMayor 
,a.ListaIncubadora 
,a.ListaPadre 
,a.Edad 
,a.PorcCodigoRaza 
,a.PobInicial
,a.PesoMasSTD 
,a.PesoGananciaDiaAcum 
,a.PesoSemAcum 
,a.Peso 
,a.PesoSem 
,a.STDPeso 
,a.DifPeso_STD 
,a.PesoProm 
,a.PesoAlo 
,a.PesoSem1 
,a.PesoSem2 
,a.PesoSem3 
,a.PesoSem4 
,a.PesoSem5 
,a.PesoSem6 
,a.PesoSem7 
,a.TasaCre 
,a.GananciaDia 
,a.GananciaSem
,a.DifPesoActPesoAnt 
,a.CantDia 
,a.GananciaPesoDia 
,a.categoria 
,a.FlagAtipico 
,a.PesoHvoPond 
,a.PesoSem8 
,a.PesoSem9 
,a.PesoSem10 
,a.PesoSem11 
,a.PesoSem12 
,a.PesoSem13 
,a.PesoSem14 
,a.PesoSem15 
,a.PesoSem16 
,a.PesoSem17 
,a.PesoSem18 
,a.PesoSem19 
,a.PesoSem20 
,a.CantAlojXROSS 
,a.PorcAlojXROSS 
,a.CantAlojXTROSS 
,a.PorcAlojXTROSS 
,a.FlagTransfPavos 
,a.SourceComplexEntityNo 
,a.DestinationComplexEntityNo 
,a.U_CausaPesoBajo 
,a.U_AccionPesoBajo 
,a.RuidosRespiratorios 
,a.ConcatCorral 
,a.PesoSemAntCorral 
,a.ConcatLote 
,a.PesoSemAntLote 
,a.ConcatSemAnioLote 
,a.Peso5Dias 
,a.TipoOrigen 
,a.ConcatSemAnioCorral 
,a.GananciaPesoSem
,a.DescripEmpresa
,a.DescripDivision
,a.DescripZona
,a.DescripSubzona
,a.Plantel
,a.Lote
,a.Galpon
,a.DescripSexo
,a.DescripStandard
,a.DescripProducto
,a.DescripTipoProducto
,a.DescripEspecie
,a.DescripEstado
,a.DescripAdministrador
,a.DescripProveedor
,a.DescripSemanaVida
,a.DescripDiaVida
,b.fecha DescripFecha 
FROM (
    select 
     max(b.pk_tiempo) as pk_tiempo
    ,b.pk_empresa 
    ,b.pk_division 
    ,b.pk_zona 
    ,b.pk_subzona 
    ,b.pk_plantel 
    ,b.pk_lote 
    ,b.pk_galpon 
    ,b.pk_sexo 
    ,b.pk_standard 
    ,b.pk_producto 
    ,b.pk_tipoproducto 
    ,b.pk_especie 
    ,b.pk_estado 
    ,b.pk_administrador 
    ,b.pk_proveedor 
    ,b.pk_semanavida 
    ,B.pk_diasvida 
    ,B.ComplexEntityNo 
    ,b.FechaNacimiento 
    ,b.IncubadoraMayor 
    ,b.ListaIncubadora 
    ,b.ListaPadre 
    ,max(b.Edad) as Edad 
    ,avg(b.PorcCodigoRaza) as PorcCodigoRaza 
    ,avg(b.PobInicial) as PobInicial
    ,0 as PesoMasSTD 
    ,max(b.PesoDia) as PesoGananciaDiaAcum 
    ,max(b.PesoDiaAcum) as PesoSemAcum 
    ,max(B.Peso) as Peso 
    ,max(b.PesoSem) as PesoSem 
    ,max(b.STDPeso) as STDPeso 
    ,case when max(b.PesoSem) = 0 then 0.0 else (max(b.PesoSem) - max(b.STDPeso)) * 1000 end as DifPeso_STD 
    ,case when AVG(b.AvesRendidas) = 0 then 0.0 else round((avg(b.KilosRendidos)/avg(b.AvesRendidas)),3) end as PesoProm 
    ,avg(b.PesoAlo)/1000 as PesoAlo 
    ,avg(b.PesoSem1) as PesoSem1 
    ,avg(b.PesoSem2) as PesoSem2 
    ,avg(b.PesoSem3) as PesoSem3 
    ,avg(b.PesoSem4) as PesoSem4 
    ,avg(b.PesoSem5) as PesoSem5 
    ,avg(b.PesoSem6) as PesoSem6 
    ,avg(b.PesoSem7) as PesoSem7 
    ,case when avg(b.PesoAlo)/1000 = 0 then 0 when B.pk_diasvida in (5,6,8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,137,134,141) then max(b.PesoSem) / (avg(b.PesoAlo)/1000) else 0 end as TasaCre 
    ,case when max(b.edad) = 0 then 0 else round(((max(B.Peso)/max(b.Edad))*1000),2) end as GananciaDia 
    ,case when B.pk_diasvida in (5,6,8,15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,137,134,141) and max(b.PesoSem) >0 
    then max(b.PesoSem)-nvl(max(c.PesoSem),0) else 0 end as GananciaSem 
    ,nvl(MAX(GP.DifPesoActPesoAnt),0) DifPesoActPesoAnt 
    ,nvl(MAX(GP.CantDia),0) CantDia 
    ,nvl(MAX(GP.GananciaPesoDia),0) GananciaPesoDia 
    ,b.categoria 
    ,b.FlagAtipico 
    ,b.PesoHvoPond 
    ,avg(b.PesoSem8) as PesoSem8 
    ,avg(b.PesoSem9) as PesoSem9 
    ,avg(b.PesoSem10) as PesoSem10 
    ,avg(b.PesoSem11) as PesoSem11 
    ,avg(b.PesoSem12) as PesoSem12 
    ,avg(b.PesoSem13) as PesoSem13 
    ,avg(b.PesoSem14) as PesoSem14 
    ,avg(b.PesoSem15) as PesoSem15 
    ,avg(b.PesoSem16) as PesoSem16 
    ,avg(b.PesoSem17) as PesoSem17 
    ,avg(b.PesoSem18) as PesoSem18 
    ,avg(b.PesoSem19) as PesoSem19 
    ,avg(b.PesoSem20) as PesoSem20 
    ,max(b.CantAlojXROSS) CantAlojXROSS 
    ,max(b.PorcAlojXROSS) PorcAlojXROSS 
    ,max(b.CantAlojXTROSS) CantAlojXTROSS 
    ,max(b.PorcAlojXTROSS) PorcAlojXTROSS 
    ,b.FlagTransfPavos 
    ,b.SourceComplexEntityNo 
    ,b.DestinationComplexEntityNo 
    ,b.U_CausaPesoBajo 
    ,b.U_AccionPesoBajo 
    ,MAX(b.U_RuidosTotales) RuidosRespiratorios 
    ,TGC.ConcatCorral 
    ,MAX(TGC.PesoSemAntCorral) PesoSemAntCorral 
    ,CL.ConcatLote 
    ,MAX(TGL.PesoSemAntLote) PesoSemAntLote 
    ,CL.ConcatSemAnioLote 
    ,avg(b.Peso5Dias) as Peso5Dias 
    ,b.TipoOrigen 
    ,CC.ConcatSemAnioCorral 
    ,case when B.pk_diasvida = 1 then 0 when B.pk_diasvida = 8 and nvl(max(c.PesoSem) ,0) >0 
    then max(b.PesoSem)-nvl(max(c.PesoSem),0) 
    when B.pk_diasvida in (15,22,29,36,43,50,57,64,71,78,85,92,99,106,113,120,127,134,141) and max(b.PesoSem) >0 and nvl(max(d.PesoSem) ,0)> 0 
    then max(b.PesoSem)-nvl(max(d.PesoSem),0) 
    else 0 end as GananciaPesoSem
    ,'' DescripEmpresa
    ,'' DescripDivision
    ,'' DescripZona
    ,'' DescripSubzona
    ,'' Plantel
    ,'' Lote
    ,'' Galpon
    ,'' DescripSexo
    ,'' DescripStandard
    ,'' DescripProducto
    ,'' DescripTipoProducto
    ,'' DescripEspecie
    ,'' DescripEstado
    ,'' DescripAdministrador
    ,'' DescripProveedor
    ,'' DescripSemanaVida
    ,'' DescripDiaVida
    from {database_name}.PesoDetalle b 
    LEFT JOIN {database_name}.GananciaPesoDia GP ON GP.ComplexEntityNo = b.ComplexEntityNo AND GP.pk_diasvida = b.pk_diasvida 
    left join {database_name}.ConcatCorral CC on b.ComplexEntityNo = CC.ComplexEntityNo and b.fecha <= CC.fecha 
    left join {database_name}.TotalGeneralCorral TGC on CC.ConcatCorral = TGC.ConcatCorral and b.pk_semanavida = TGC.pk_semanavida 
    left join {database_name}.ConcatLote CL on substring(b.complexentityno,1,(length(b.complexentityno)-6)) = CL.ComplexEntityNo and b.fecha <= CL.fecha 
    left join {database_name}.TotalGeneralLote TGL on CL.ConcatSemAnioLote = TGL.ConcatSemAnioLote and b.pk_semanavida = TGL.pk_semanavida 
    left join {database_name}.PesoDetalle c on c.ComplexEntityNo = b.ComplexEntityNo and c.pk_diasvida  = 2 
    left join {database_name}.PesoDetalle d on d.ComplexEntityNo = b.ComplexEntityNo and d.pk_diasvida  = b.pk_diasvida  - (8) 
    where (date_format(b.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) 
    group by b.pk_empresa,b.pk_division,b.pk_zona,b.pk_subzona,b.pk_plantel,b.pk_lote,b.pk_galpon,b.pk_sexo,b.pk_standard,b.pk_producto,b.pk_tipoproducto,b.pk_especie,b.pk_estado 
    ,b.pk_administrador,b.pk_proveedor,B.pk_diasvida,b.pk_semanavida,B.ComplexEntityNo,b.FechaNacimiento,b.IncubadoraMayor,b.ListaIncubadora,b.ListaPadre,b.categoria,b.FlagAtipico,b.PesoHvoPond 
    ,b.FlagTransfPavos,b.SourceComplexEntityNo,b.DestinationComplexEntityNo,b.U_CausaPesoBajo,b.U_AccionPesoBajo
    ,TGC.ConcatCorral,CL.ConcatLote,CL.ConcatSemAnioLote
    ,b.TipoOrigen
    ,CC.ConcatSemAnioCorral)
A  LEFT JOIN {database_name}.lk_tiempo B ON A.pk_tiempo=b.pk_tiempo order by 1 """)
#avg(Peso_STD) as PesoMasSTD 
print('df_ft_peso_Diario',df_ft_peso_Diario.count())

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_peso_DiarioTemp"
}
df_ft_peso_Diario.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_peso_DiarioTemp")
print('carga ft_peso_DiarioTemp')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target19)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")


try:
    df_existentes = spark.read.format("parquet").load(path_target19)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_peso_Diario cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_peso_Diario")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target19)
    data_after_delete = existing_data.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_peso_Diario.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_peso_Diario : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_peso_Diario : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_peso_DiarioTemporal')
    print(f"Tabla ft_peso_DiarioTemporal eliminada correctamente de la base de datos {database_name}.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    df_ft_peso_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
print('carga ft_peso_Diario')
#df_stdpesonuevo = spark.sql(f"""
#select 
#A.pk_tiempo,
#A.pk_empresa,
#A.pk_diasvida,
#A.ComplexEntityNo,
#A.STDPeso,
#B.STDPeso + (B.Diferencia  * ROW_NUMBER() OVER ( PARTITION BY A.ComplexEntityNo ORDER BY A.pk_diasvida asc )) STDPesoNuevo
#from {database_name}.ft_peso_Diario A
#left join  (
#				select --distinct
#					A.pk_tiempo,
#					A.pk_diasvida,
#					A.ComplexEntityNo,
#					A.STDPEso,
#					(A.STDPEso - B.STDPEso) Diferencia
#				from {database_name}.ft_peso_Diario A
#				left join {database_name}.ft_peso_Diario B on A.ComplexEntityNo = B.ComplexEntityNo and A.Edad-1 = B.Edad and A.STDPeso <> 0.0
#				inner join (
#								select 
#										 min(pk_tiempo) pk_tiempo
#										,min(pk_diasvida) pk_diasvida
#										,ComplexEntityNo
#										,min(STDPeso) STDPEso 
#								from {database_name}.ft_peso_Diario
#								where STDPeso = 0.0 and pk_empresa = 1
#								group by ComplexEntityNo
#							) C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_diasvida = c.pk_diasvida - 1
#			) B on A.ComplexEntityNo = B.ComplexEntityNo
#where 
#A.STDPeso = 0.0 and A.pk_empresa = 1
#order by A.pk_tiempo
#""")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPesoNuevo"
#}
#df_stdpesonuevo.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.STDPesoNuevo")
#print('carga STDPesoNuevo',df_stdpesonuevo.count())
df_stdpesonuevo = spark.sql(f"""
WITH stdPesoNuevos as (
    select 
    A.pk_tiempo,
    A.pk_empresa,
    A.pk_diasvida,
    A.ComplexEntityNo,
    A.STDPeso,
    B.STDPeso  STDPeso2,
    B.Diferencia,
    ROW_NUMBER() OVER (PARTITION BY A.ComplexEntityNo ORDER BY A.pk_diasvida)  AS ORDEN
    from {database_name}.ft_peso_Diario A
    inner join  (
        select 
    	A.pk_tiempo,
    	A.pk_diasvida,
    	A.ComplexEntityNo,
    	A.STDPEso,
    	(A.STDPEso - B.STDPEso) Diferencia 
        from {database_name}.ft_peso_Diario A
        left join {database_name}.ft_peso_Diario B 
                   on A.ComplexEntityNo = B.ComplexEntityNo and A.Edad - 1 = B.Edad    and A.STDPeso <> 0.0 
        inner join (
        			select 
        				 min(pk_tiempo) pk_tiempo
        				,min(pk_diasvida) pk_diasvida 
        				,ComplexEntityNo
        				,min(STDPeso) STDPEso 
        			from {database_name}.ft_peso_Diario
        			where STDPeso = 0.0 and pk_empresa = 1 
        			group by ComplexEntityNo
        		 ) C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_diasvida = c.pk_diasvida - 1 
        		
        		
    ) B on A.ComplexEntityNo = B.ComplexEntityNo 
    where 
    A.STDPeso = 0.0 and A.pk_empresa = 1 
)
SELECT 
pk_tiempo,
pk_empresa,
pk_diasvida,
ComplexEntityNo,
STDPeso,
STDPeso2 + (Diferencia  * MIN(ORDEN) OVER (PARTITION BY ComplexEntityNo,pk_diasvida ORDER BY pk_diasvida) ) STDPesoNuevo
FROM stdPesoNuevos
ORDER BY 5,4
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPesoNuevo"
}
df_stdpesonuevo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPesoNuevo")
print('carga STDPesoNuevo',df_stdpesonuevo.count())
df_upd_ft_peso_diario_1 = spark.sql(f"""select 
 a.pk_tiempo  
,a.pk_empresa 
,a.pk_division 
,a.pk_zona 
,a.pk_subzona 
,a.pk_plantel 
,a.pk_lote 
,a.pk_galpon 
,a.pk_sexo 
,a.pk_standard 
,a.pk_producto 
,a.pk_tipoproducto 
,a.pk_especie 
,a.pk_estado 
,a.pk_administrador 
,a.pk_proveedor 
,a.pk_semanavida 
,a.pk_diasvida 
,a.ComplexEntityNo 
,a.FechaNacimiento 
,a.IncubadoraMayor 
,a.ListaIncubadora 
,a.ListaPadre 
,a.Edad 
,a.PorcCodigoRaza 
,a.PobInicial
,a.PesoMasSTD 
,a.PesoGananciaDiaAcum 
,a.PesoSemAcum 
,a.Peso 
,a.PesoSem 
,case when a.STDPeso = 0 and a.pk_empresa = 1 then b.STDPesoNuevo else a.STDPeso end STDPeso
,a.DifPeso_STD 
,a.PesoProm 
,a.PesoAlo 
,a.PesoSem1 
,a.PesoSem2 
,a.PesoSem3 
,a.PesoSem4 
,a.PesoSem5 
,a.PesoSem6 
,a.PesoSem7 
,a.TasaCre 
,a.GananciaDia 
,a.GananciaSem 
,a.DifPesoActPesoAnt 
,a.CantDia 
,a.GananciaPesoDia 
,a.categoria 
,a.FlagAtipico 
,a.PesoHvoPond PesoHvo
,a.PesoSem8 
,a.PesoSem9 
,a.PesoSem10 
,a.PesoSem11 
,a.PesoSem12 
,a.PesoSem13 
,a.PesoSem14 
,a.PesoSem15 
,a.PesoSem16 
,a.PesoSem17 
,a.PesoSem18 
,a.PesoSem19 
,a.PesoSem20 
,a.CantAlojXROSS 
,a.PorcAlojXROSS 
,a.CantAlojXTROSS 
,a.PorcAlojXTROSS 
,a.FlagTransfPavos 
,a.SourceComplexEntityNo 
,a.DestinationComplexEntityNo 
,a.U_CausaPesoBajo 
,a.U_AccionPesoBajo 
,a.RuidosRespiratorios 
,a.ConcatCorral 
,a.PesoSemAntCorral 
,a.ConcatLote 
,a.PesoSemAntLote 
,a.ConcatSemAnioLote 
,a.Peso5Dias 
,a.TipoOrigen 
,a.ConcatSemAnioCorral 
,a.GananciaPesoSem
,a.DescripEmpresa
,a.DescripDivision
,a.DescripZona
,a.DescripSubzona
,a.Plantel
,a.Lote
,a.Galpon
,a.DescripSexo
,a.DescripStandard
,a.DescripProducto
,a.DescripTipoProducto
,a.DescripEspecie
,a.DescripEstado
,a.DescripAdministrador
,a.DescripProveedor
,a.DescripSemanaVida
,a.DescripDiaVida
,a.DescripFecha
from {database_name}.ft_peso_Diario  a left join (select distinct * from {database_name}.STDPesoNuevo) b on a.ComplexEntityNo = b.ComplexEntityNo 
and a.pk_diasvida = b.pk_diasvida and a.pk_empresa=b.pk_empresa and a.pk_tiempo=b.pk_tiempo

""")

print('df_upd_ft_peso_diario_1',df_upd_ft_peso_diario_1.count())

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
 
fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target19)     
try:
    df_existentes = spark.read.format("parquet").load(path_target19)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_peso_Diario cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_peso_Diario")

if datos_existentes:
    #existing_data = spark.read.format("parquet").load(path_target19)
    #data_after_delete = existing_data.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    #filtered_new_data = df_ft_peso_Diario.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = df_upd_ft_peso_diario_1#filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = final_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_peso_Diario : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_peso_Diario : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_peso_DiarioTemporal')
    print(f"Tabla ft_peso_DiarioTemporal eliminada correctamente de la base de datos {database_name}.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    df_upd_ft_peso_diario_1.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
print('carga ft_peso_Diario')
df_upd_ft_peso_diario_2 = spark.sql(f"""select 
 a.pk_tiempo  
,a.pk_empresa 
,a.pk_division 
,a.pk_zona 
,a.pk_subzona 
,a.pk_plantel 
,a.pk_lote 
,a.pk_galpon 
,a.pk_sexo 
,a.pk_standard 
,a.pk_producto 
,a.pk_tipoproducto 
,a.pk_especie 
,a.pk_estado 
,a.pk_administrador 
,a.pk_proveedor 
,a.pk_semanavida 
,a.pk_diasvida 
,a.ComplexEntityNo 
,a.FechaNacimiento 
,a.IncubadoraMayor 
,a.ListaIncubadora 
,a.ListaPadre 
,a.Edad 
,a.PorcCodigoRaza 
,a.PobInicial
,a.PesoMasSTD 
,a.PesoGananciaDiaAcum 
,a.PesoSemAcum 
,a.Peso 
,a.PesoSem 
,a.STDPeso
,a.DifPeso_STD 
,a.PesoProm 
,a.PesoAlo 
,a.PesoSem1 
,a.PesoSem2 
,a.PesoSem3 
,a.PesoSem4 
,a.PesoSem5 
,a.PesoSem6 
,a.PesoSem7 
,a.TasaCre 
,a.GananciaDia 
,a.GananciaSem 
,a.DifPesoActPesoAnt 
,a.CantDia 
,a.GananciaPesoDia 
,case when a.pk_empresa=1 and (a.categoria is null or a.categoria='-') then nvl(b.categoria,'-') else a.categoria end categoria
,a.FlagAtipico 
,a.PesoHvo 
,a.PesoSem8 
,a.PesoSem9 
,a.PesoSem10 
,a.PesoSem11 
,a.PesoSem12 
,a.PesoSem13 
,a.PesoSem14 
,a.PesoSem15 
,a.PesoSem16 
,a.PesoSem17 
,a.PesoSem18 
,a.PesoSem19 
,a.PesoSem20 
,a.CantAlojXROSS 
,a.PorcAlojXROSS 
,a.CantAlojXTROSS 
,a.PorcAlojXTROSS 
,a.FlagTransfPavos 
,a.SourceComplexEntityNo 
,a.DestinationComplexEntityNo 
,a.U_CausaPesoBajo 
,a.U_AccionPesoBajo 
,a.RuidosRespiratorios 
,a.ConcatCorral 
,a.PesoSemAntCorral 
,a.ConcatLote 
,a.PesoSemAntLote 
,a.ConcatSemAnioLote 
,a.Peso5Dias 
,a.TipoOrigen 
,a.ConcatSemAnioCorral 
,a.GananciaPesoSem
,a.DescripEmpresa
,a.DescripDivision
,a.DescripZona
,a.DescripSubzona
,a.Plantel
,a.Lote
,a.Galpon
,a.DescripSexo
,a.DescripStandard
,a.DescripProducto
,a.DescripTipoProducto
,a.DescripEspecie
,a.DescripEstado
,a.DescripAdministrador
,a.DescripProveedor
,a.DescripSemanaVida
,a.DescripDiaVida
,a.DescripFecha
from {database_name}.ft_peso_Diario a left join 
{database_name}.lk_ft_excel_categoria b on a.pk_lote = b.pk_lote
""")
print('carga df_upd_ft_peso_diario_2 ',df_upd_ft_peso_diario_2.count())
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
 
fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target19)     
try:
    df_existentes = spark.read.format("parquet").load(path_target19)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_peso_Diario cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_peso_Diario")

if datos_existentes:
    #existing_data = spark.read.format("parquet").load(path_target19)
    #data_after_delete = existing_data.filter(~((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str)))
    #filtered_new_data = df_ft_peso_Diario.filter((date_format(col("DescripFecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = df_upd_ft_peso_diario_2#filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = final_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_peso_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_peso_Diario : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_peso_Diario : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_peso_DiarioTemporal')
    print(f"Tabla ft_peso_DiarioTemporal eliminada correctamente de la base de datos {database_name}.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_peso_Diario"
    }
    df_upd_ft_peso_diario_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_peso_Diario")
print('carga ft_peso_Diario')
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()