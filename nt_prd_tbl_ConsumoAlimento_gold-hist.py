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
JOB_NAME = "nt_prd_tbl_ConsumoAlimento_gold"

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

file_name_target3 = f"{bucket_name_prdmtech}ft_ConsumoAlimento/"
path_target3 = f"s3://{bucket_name_target}/{file_name_target3}"
print('cargando ruta')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
df_ConsumoAlimento = spark.sql(f"SELECT nvl(MO.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha=cast('1899-11-30' as date))) as pk_tiempo, \
nvl(MO.fecha,cast('1899-11-30' as date)) as fecha, \
MO.pk_empresa, \
nvl(MO.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division, \
nvl(MO.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona, \
nvl(MO.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona, \
nvl(MO.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel, \
nvl(MO.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote, \
nvl(MO.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon, \
nvl(MO.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo, \
nvl(MO.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard, \
nvl(MO.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto, \
nvl(LPC.pk_productoconsumo,(select pk_productoconsumo from {database_name}.lk_productoconsumo where cproductoconsumo='0')) pk_productoconsumo, \
nvl(MO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name}.lk_grupoconsumo where cgrupoconsumo='0')) pk_grupoconsumo, \
nvl(LSG.pk_subgrupoconsumo,(select pk_subgrupoconsumo from {database_name}.lk_subgrupoconsumo where csubgrupoconsumo=0)) pk_subgrupoconsumo, \
nvl(MO.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto, \
nvl(MO.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie, \
nvl(MO.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado, \
nvl(MO.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador, \
nvl(MO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor, \
MO.pk_diasvida, \
MO.pk_semanavida, \
MO.ComplexEntityNo, \
MO.FechaNacimiento Nacimiento, \
MO.Edad AS Edad, \
MO.ConsDia, \
nvl(MO.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) pk_alimento, \
nvl(LFR.pk_formula,(select pk_formula from {database_name}.lk_formula where cformula='0')) pk_formula, \
MO.U_categoria categoria, \
MO.Price Valor, \
CAST(CAST(MO.FechaCierre AS VARCHAR(25)) AS timestamp) FechaCierre \
FROM {database_name}.producciondetalle_upd3 MO  \
LEFT JOIN {database_name}.lk_formula LFR ON LFR.ProteinProductsIRN = cast(MO.ProteinProductsIRN as varchar(50)) \
LEFT JOIN {database_name}.lk_productoconsumo LPC ON LFR.ProteinProductsIRN = LPC.IRN \
LEFT JOIN {database_name}.lk_subgrupoconsumo LSG ON LPC.pk_subgrupoconsumo = LSG.pk_subgrupoconsumo \
WHERE MO.Consdia <> 0 ")

#df_ConsumoAlimento.createOrReplaceTempView("consumoalimento")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoalimento"
}
df_ConsumoAlimento.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoalimento")
print('carga consumoalimento')
#Se actualiza toda la información tenga edad cero a: iddiavida = 1 y idsemanavida = 1
df_ConsumoAlimento_upd = spark.sql(f"SELECT \
pk_tiempo ,fecha ,pk_empresa ,pk_division ,pk_zona ,pk_subzona \
,pk_plantel ,pk_lote ,pk_galpon ,pk_sexo ,pk_standard ,pk_producto ,pk_productoconsumo ,pk_grupoconsumo \
,pk_subgrupoconsumo ,pk_tipoproducto ,pk_especie ,pk_estado ,pk_administrador ,pk_proveedor \
,CASE WHEN (Edad = 0 or Edad = -1) then 1 \
WHEN Edad <= 0 AND SUBSTRING(ComplexEntityNo,1,1) = 'V' then 1 \
else pk_diasvida end as pk_diasvida \
,CASE WHEN (Edad = 0 or Edad = -1) then 1 \
WHEN Edad <= 0 AND SUBSTRING(ComplexEntityNo,1,1) = 'V' then 1 \
else pk_semanavida end as pk_semanavida \
,ComplexEntityNo \
,Nacimiento \
,Edad \
,ConsDia \
,pk_alimento \
,pk_formula \
,categoria \
,Valor \
,FechaCierre \
from {database_name}.consumoalimento")

#df_ConsumoAlimento_upd.createOrReplaceTempView("consumoalimentoupd")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoalimentoupd"
}
df_ConsumoAlimento_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoalimentoupd")
print('carga consumoalimentoupd')
#Inserta en la tabla STGPECUARIO.consumoalimento el consumo alimento agrupado por cada dimensión
df_ConsumoAlimento0 = spark.sql(f"SELECT MAX(pk_tiempo) pk_tiempo,MAX(fecha) fecha ,pk_empresa ,pk_division ,pk_zona ,pk_subzona ,pk_plantel ,pk_lote ,pk_galpon ,pk_sexo ,pk_standard, \
pk_producto ,pk_productoconsumo ,pk_grupoconsumo ,pk_subgrupoconsumo ,pk_tipoproducto,pk_especie ,pk_estado ,pk_administrador ,pk_proveedor ,pk_diasvida ,pk_semanavida ,complexentityno, \
nacimiento ,MAX(edad) edad ,SUM(ConsDia) ConsDia,pk_alimento ,pk_formula ,categoria,SUM(Valor) Valor, MAX(FechaCierre) FechaCierre \
FROM {database_name}.consumoalimentoupd \
GROUP BY pk_empresa ,pk_division,pk_zona ,pk_subzona ,pk_plantel ,pk_lote ,pk_galpon ,pk_sexo ,pk_standard \
,pk_producto ,pk_productoconsumo ,pk_grupoconsumo ,pk_subgrupoconsumo ,pk_tipoproducto ,pk_especie ,pk_estado \
,pk_administrador ,pk_proveedor ,pk_diasvida ,pk_semanavida ,complexentityno ,nacimiento ,pk_alimento ,pk_formula ,categoria")

#df_ConsumoAlimento0.createOrReplaceTempView("consumoalimento0")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoalimento0"
}
df_ConsumoAlimento0.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoalimento0")
print('carga consumoalimento0')
df_ConsumoAlimento = spark.sql(f"SELECT * FROM {database_name}.consumoalimento0 WHERE consdia <> 0 ORDER BY fecha DESC")

#df_ConsumoAlimento.createOrReplaceTempView("consumoalimento1")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoalimento1"
}
df_ConsumoAlimento.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoalimento1")
print('carga consumoalimento1')
df_estadoTemp = spark.sql(f"select complexentityno,pk_estado,pk_empresa from {database_name}.producciondetalle_upd3 \
group by complexentityno,pk_estado,pk_empresa")

#df_estadoTemp.createOrReplaceTempView("estadoTemp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/estadoTemp"
}
df_estadoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.estadoTemp")
print('carga estadoTemp')
df_ft_consumoalimento_upd = spark.sql(f"SELECT pk_tiempo,fecha ,A.pk_empresa ,pk_division ,pk_zona ,pk_subzona ,pk_plantel ,pk_lote ,pk_galpon ,pk_sexo ,pk_standard, \
pk_producto ,pk_productoconsumo ,pk_grupoconsumo ,pk_subgrupoconsumo ,pk_tipoproducto,pk_especie , CASE WHEN isnotnull(B.PK_estado) = true THEN B.pk_estado ELSE A.pk_estado END AS pk_estado, \
pk_administrador ,pk_proveedor ,pk_diasvida ,pk_semanavida ,A.complexentityno, \
nacimiento ,edad ,ConsDia,pk_alimento ,pk_formula ,categoria,Valor, FechaCierre \
FROM {database_name}.consumoalimento1 A left join {database_name}.estadoTemp B on A.ComplexEntityNo = B.ComplexEntityNo")

df_ft_consumoalimento = df_ft_consumoalimento_upd

print('inicio de carga de la tabla df_ft_consumoalimento')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target3)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target3)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ConsumoAlimento cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ConsumoAlimento")



if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target3)
    data_after_delete = existing_data.filter(~((date_format(F.col("fecha"),"yyyyMM") >= fecha_str)))
    filtered_new_data = df_ft_consumoalimento.filter((date_format(F.col("fecha"),"yyyyMM") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ConsumoAlimentoTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ConsumoAlimentoTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ConsumoAlimentoTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ConsumoAlimento"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ConsumoAlimento")
            
    print(f"agrega registros nuevos a la tabla ft_ConsumoAlimento : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ConsumoAlimento : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ConsumoAlimentoTemporal')
    print(f"Tabla ft_ConsumoAlimentoTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ConsumoAlimento"
    }
    df_ft_consumoalimento.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ConsumoAlimento")

df_FormulaTemp = spark.sql(f"select ComplexEntityNo,pk_diasvida,rtrim(FormulaNo) FormulaNo,FormulaName \
from {database_name}.producciondetalle_upd3 \
where pk_alimento <> 1 \
AND SUBSTRING(ComplexEntityNo,8,2) IN ('01','02','03','04','05','06','07','08','09','10') \
group by ComplexEntityNo,pk_diasvida,FormulaNo,FormulaName")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/FormulaTemp"
}
df_FormulaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.FormulaTemp")
print('carga FormulaTemp')
df_ListaFormulaNoNameTemp =spark.sql(f"SELECT F.ComplexEntityNo, F.pk_diasvida, \
concat_ws(',' ,collect_list( DISTINCT F.FormulaNo)) FormulaNo, concat_ws(',' ,collect_list( DISTINCT F.FormulaName)) FormulaName \
FROM {database_name}.FormulaTemp F group by  F.ComplexEntityNo, F.pk_diasvida" )

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ListaFormulaNoNameTemp"
}
df_ListaFormulaNoNameTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ListaFormulaNoNameTemp")
print('carga ListaFormulaNoNameTemp')
df_TablaConsumoAlimentoXFormulaTemp =spark.sql(f"select A.ComplexEntityNo,A.pk_diasvida \
,F.FormulaNo as ListaFormulaNo \
,F.FormulaName as ListaFormulaName \
from {database_name}.FormulaTemp A \
left join ListaFormulaNoNameTemp F on F.ComplexEntityNo = A.ComplexEntityNo and F.pk_diasvida = A.pk_diasvida \
group by A.ComplexEntityNo,A.pk_diasvida,F.FormulaNo,F.FormulaName")
#concat_ws(',' , collect_list( DISTINCT Categoria))
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaConsumoAlimentoXFormula"
}
df_TablaConsumoAlimentoXFormulaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaConsumoAlimentoXFormula")
print('carga TablaConsumoAlimentoXFormula')
df_consumoTemp = spark.sql(f"SELECT ComplexEntityNo,pk_diasvida,pk_semanavida,pk_alimento,sum(ConsDia) ConsDia, MAX(STDConsDia) STDConsDia, MAX(STDConsAcum) STDConsAcum \
FROM {database_name}.producciondetalle_upd3 M \
WHERE ConsDia <>0.00 \
GROUP BY ComplexEntityNo,pk_diasvida,pk_semanavida,pk_alimento")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoTemp"
}
df_consumoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoTemp")
print('carga consumoTemp')
df_consumoSTemp = spark.sql(f"SELECT ComplexEntityNo,pk_semanavida,sum(ConsDia) ConsDia \
FROM {database_name}.producciondetalle_upd3 M \
WHERE ConsDia <>0.00 \
GROUP BY ComplexEntityNo,pk_semanavida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumoSTemp"
}
df_consumoSTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumoSTemp")
print('carga consumoSTemp')
df_TablaPivotConsumoXTipoAlimentoTemp = spark.sql(f"select ComplexEntityNo,pk_diasvida,max(`22`) PreInicio,max(`5`) Inicio, max(`72`) Acabado, max(`42`) Terminado, max(`13`) Finalizador, \
max(`49`) PavoIni,max(`21`) Pavo1,max(`35`) Pavo2,max(`10`) Pavo3,max(`39`) Pavo4,max(`47`) Pavo5,max(`65`) Pavo6,max(`14`) Pavo7 \
from ( \
select ComplexEntityNo,pk_diasvida,'' as `22`,'' as `5`,'' as `72`,'' as `42`,'' as `13`,'' as `49`,'' as `21`,'' as `35`,'' as `10`,'' as `39`,'' as `47`,'' as `65`,'' as `14` \
from {database_name}.consumoTemp \
pivot \
( \
max(ConsDia) \
for pk_alimento in (22,5,72,42,13,49,21,35,10,39,47,65,14) \
) \
)A \
group by A.ComplexEntityNo,pk_diasvida")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotConsumoXTipoAlimento"
}
df_TablaPivotConsumoXTipoAlimentoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaPivotConsumoXTipoAlimento")
print('carga TablaPivotConsumoXTipoAlimento')
df_TablaPivotConsumoXSemanaTemp = spark.sql(f"select ComplexEntityNo,max(`1`) ConsSem1,max(`2`) ConsSem2, max(`3`) ConsSem3, max(`4`) ConsSem4, max(`5`) ConsSem5, \
max(`6`) ConsSem6,max(`7`) ConsSem7,max(`8`) ConsSem8,max(`9`) ConsSem9,max(`10`) ConsSem10, max(`11`) ConsSem11,max(`12`) ConsSem12,max(`13`) ConsSem13, \
max(`14`) ConsSem14,max(`15`) ConsSem15,max(`16`) ConsSem16,max(`17`) ConsSem17,max(`18`) ConsSem18,max(`19`) ConsSem19,max(`20`) ConsSem20 \
from ( \
select ComplexEntityNo,'' as `1`,'' as `2`,'' as `3`,'' as `4`,'' as `5`,'' as `6`,'' as `7`,'' as `8`,'' as `9`,'' as `10`,'' as `11`,'' as `12`,'' as `13`, \
'' as `14`,'' as `15`,'' as `16`,'' as `17`,'' as `18`,'' as `19`,'' as `20` \
from {database_name}.consumoSTemp \
pivot \
( \
max(ConsDia) \
for pk_semanavida in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) \
) \
)A \
group by A.ComplexEntityNo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPivotConsumoXSemana"
}
df_TablaPivotConsumoXSemanaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaPivotConsumoXSemana")
print('carga TablaPivotConsumoXSemana')
                                            
df_ConsumosXTipoAlimentoTemp = spark.sql(f"select A.ComplexEntityno,A.pk_diasvida,A.pk_semanavida,A.pk_alimento \
,MAX(A.ConsDia) ConsDia \
,nvl(SUM(SUM(A.ConsDia)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) ConsDiaAcum \
,MAX(A.STDConsDia) STDConsDia \
,MAX(A.STDConsAcum) STDConsAcum \
,MAX(nvl(B.Preinicio,0)) Preinicio \
,MAX(nvl(B.Inicio,0)) Inicio \
,MAX(nvl(B.Acabado,0)) Acabado \
,MAX(nvl(B.Terminado,0)) Terminado \
,MAX(nvl(B.Finalizador,0)) Finalizador \
,nvl(SUM(SUM(B.Preinicio)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) PreinicioAcum \
,nvl(SUM(SUM(B.Inicio)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) InicioAcum \
,nvl(SUM(SUM(B.Acabado)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) AcabadoAcum \
,nvl(SUM(SUM(B.Terminado)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) TerminadoAcum \
,nvl(SUM(SUM(B.Finalizador)) OVER (PARTITION BY A.ComplexEntityno ORDER BY A.pk_diasvida),0) FinalizadorAcum \
,MAX(nvl(B.PavoIni,0)) PavoIni \
,MAX(nvl(B.Pavo1,0)) Pavo1 \
,MAX(nvl(B.Pavo2,0)) Pavo2 \
,MAX(nvl(B.Pavo3,0)) Pavo3 \
,MAX(nvl(B.Pavo4,0)) Pavo4 \
,MAX(nvl(B.Pavo5,0)) Pavo5 \
,MAX(nvl(B.Pavo6,0)) Pavo6 \
,MAX(nvl(B.Pavo7,0)) Pavo7 \
,MAX(nvl(C.ConsSem1,0)) ConsSem1 \
,MAX(nvl(C.ConsSem2,0)) ConsSem2 \
,MAX(nvl(C.ConsSem3,0)) ConsSem3 \
,MAX(nvl(C.ConsSem4,0)) ConsSem4 \
,MAX(nvl(C.ConsSem5,0)) ConsSem5 \
,MAX(nvl(C.ConsSem6,0)) ConsSem6 \
,MAX(nvl(C.ConsSem7,0)) ConsSem7 \
,MAX(nvl(C.ConsSem8,0)) ConsSem8 \
,MAX(nvl(C.ConsSem9,0)) ConsSem9 \
,MAX(nvl(C.ConsSem10,0)) ConsSem10 \
,MAX(nvl(C.ConsSem11,0)) ConsSem11 \
,MAX(nvl(C.ConsSem12,0)) ConsSem12 \
,MAX(nvl(C.ConsSem13,0)) ConsSem13 \
,MAX(nvl(C.ConsSem14,0)) ConsSem14 \
,MAX(nvl(C.ConsSem15,0)) ConsSem15 \
,MAX(nvl(C.ConsSem16,0)) ConsSem16 \
,MAX(nvl(C.ConsSem17,0)) ConsSem17 \
,MAX(nvl(C.ConsSem18,0)) ConsSem18 \
,MAX(nvl(C.ConsSem19,0)) ConsSem19 \
,MAX(nvl(C.ConsSem20,0)) ConsSem20 \
from {database_name}.consumoTemp A \
left join {database_name}.TablaPivotConsumoXTipoAlimento B on A.ComplexEntityNo = B.ComplexEntityNo and A.pk_diasvida = B.pk_diasvida \
left join {database_name}.TablaPivotConsumoXSemana C on A.ComplexEntityNo = C.ComplexEntityNo \
group by A.ComplexEntityno,A.pk_diasvida,A.pk_semanavida,A.pk_alimento")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsumosXTipoAlimento"
}
df_ConsumosXTipoAlimentoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsumosXTipoAlimento")
print('carga ConsumosXTipoAlimento')
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()