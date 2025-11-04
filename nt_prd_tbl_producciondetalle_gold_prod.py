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
JOB_NAME = "nt_prd_tbl_producciondetalle_gold"

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

print("inicia spark")
# Parámetros de entrada global
bucket_name_target = "ue1stgprodas3dtl005-gold"
bucket_name_source = "ue1stgprodas3dtl005-silver"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

file_name_target1 = f"{bucket_name_prdmtech}stg_ProduccionDetalle/"
path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
print('cargando ruta')
database_name_tmp = "bi_sf_tmp"
database_name_br ="mtech_prd_sf_br"
database_name_si ="mtech_prd_sf_si"
database_name_gl ="mtech_prd_sf_gl"

df_parametros = spark.sql(f"select * from {database_name_br}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
#tabla CategoriaGalpon
import pyspark.sql.functions as F
df_CategoriaGalponTemp = spark.sql(f"SELECT ComplexEntityNo, RTRIM(code) Categoria,e.pk_plantel,f.pk_lote,g.pk_galpon \
                                    FROM {database_name_si}.si_brimentities a LEFT JOIN {database_name_si}.si_proteinentities b on a.ProteinEntitiesIRN=b.IRN \
                                    LEFT JOIN {database_name_si}.si_mvbrimentities c ON c.ProteinEntitiesIRN=a.ProteinEntitiesIRN \
                                    LEFT JOIN {database_name_si}.si_mtsysuserfieldcodes d ON a.U_Categoria=d.IRN \
                                    LEFT JOIN {database_name_gl}.lk_plantel e ON e.IRN=CAST(b.ProteinFarmsIRN AS VARCHAR(50)) \
                                    LEFT JOIN {database_name_gl}.lk_lote f ON f.PK_Plantel=e.PK_Plantel and f.nlote=rtrim(b.EntityNo) and f.ActiveFlag in (0,1) \
                                    LEFT JOIN {database_name_gl}.lk_galpon g ON g.IRN= cast(b.ProteinHousesIRN as varchar(50)) and f.ActiveFlag in (0,1) \
                                    WHERE a.U_Categoria is not null and GRN = 'H'")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/CategoriaGalponTemp"
}
df_CategoriaGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.CategoriaGalponTemp")
print('carga df_CategoriaGalponTemp', df_CategoriaGalponTemp.count())
df_CategoriaLoteTemp = spark.sql(f"""
WITH CategoriaPorLote AS (
    SELECT 
        pk_lote,
        concat_ws(',', sort_array(collect_set(Categoria))) AS categoria --concat_ws(',', collect_set(Categoria)) AS categoria
    FROM {database_name_tmp}.CategoriaGalponTemp
    GROUP BY pk_lote
)
SELECT clote, 
COALESCE(cg.categoria, '') AS categoria,
e.pk_plantel,f.pk_lote, 1 pk_galpon 
FROM {database_name_si}.si_brimentities a 
LEFT JOIN {database_name_si}.si_proteinentities b on a.ProteinEntitiesIRN=b.IRN 
LEFT JOIN {database_name_si}.si_mvbrimentities c ON c.ProteinEntitiesIRN=a.ProteinEntitiesIRN 
LEFT JOIN {database_name_si}.si_mtsysuserfieldcodes d ON a.U_Categoria=d.IRN 
LEFT JOIN {database_name_gl}.lk_plantel e ON e.IRN=CAST(b.ProteinFarmsIRN AS VARCHAR(50)) 
LEFT JOIN {database_name_gl}.lk_lote f ON f.PK_Plantel=e.PK_Plantel and f.nlote=rtrim(b.EntityNo) and f.ActiveFlag in (0,1) 
LEFT JOIN {database_name_gl}.lk_galpon g ON g.IRN= cast(b.ProteinHousesIRN as varchar(50)) and f.ActiveFlag in (0,1) 
LEFT JOIN CategoriaPorLote cg ON cg.pk_lote = f.pk_lote
WHERE a.U_Categoria is not null 
GROUP BY clote,categoria,e.pk_plantel,f.pk_lote
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/CategoriaLoteTemp"
}
df_CategoriaLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.CategoriaLoteTemp")
print('carga CategoriaLoteTemp', df_CategoriaLoteTemp.count())
df_Categoria = spark.sql(f"""select * from {database_name_tmp}.CategoriaGalponTemp 
                          UNION 
                          select * from {database_name_tmp}.CategoriaLoteTemp""") 

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/categoria"
}
df_Categoria.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.categoria")
print('carga categoria', df_Categoria.count())
df_U_MortAcumPercent=spark.sql(f"""WITH BSD AS (
    SELECT *, 
           SUM(U_MortPercent) OVER (
                PARTITION BY ProteinStandardVersionsIRN 
                ORDER BY Age 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS U_MortAcumPercent
    FROM {database_name_si}.si_brimstandardsdata
)
SELECT * FROM BSD
ORDER BY ProteinStandardVersionsIRN, Age""")
#df_U_MortAcumPercent.createOrReplaceTempView("mortacumpercent")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mortacumpercent"
}
df_U_MortAcumPercent.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.mortacumpercent")
print('carga mortacumpercent')
df_CausaPesoBajo = spark.sql(f"select a.U_CausaPesoBajo ,a.complexentityno,a.FirstHatchDateAge, \
ROW_NUMBER() OVER (PARTITION BY a.complexentityno \
ORDER BY cast(cast(a.FirstHatchDate as timestamp) as date)) AS ROWNUM \
from {database_name_tmp}.mortdia a inner join {database_name_tmp}.mortdia b \
on a.complexentityno=b.complexentityno and cast(cast(a.FirstHatchDate as timestamp) as date)=cast(cast(b.FirstHatchDate as timestamp) as date) \
where length(a.U_CausaPesoBajo) != 0 LIMIT 1")
#df_CausaPesoBajo.createOrReplaceTempView("CausaPesoBajo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/CausaPesoBajo"
}
df_CausaPesoBajo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.CausaPesoBajo")
print('carga CausaPesoBajo')
df_AccionPesoBajo = spark.sql(f"select a.U_AccionPesoBajo ,a.complexentityno,a.FirstHatchDateAge, \
ROW_NUMBER() OVER (PARTITION BY a.complexentityno \
ORDER BY cast(cast(a.FirstHatchDate as timestamp) as date)) AS ROWNUM \
from {database_name_tmp}.mortdia a inner join {database_name_tmp}.mortdia b on \
a.complexentityno=b.complexentityno and cast(cast(a.FirstHatchDate as timestamp) as date)=cast(cast(b.FirstHatchDate as timestamp) as date) \
where length(a.U_AccionPesoBajo) != 0 LIMIT 1")
#df_CausaPesoBajo.createOrReplaceTempView("CausaPesoBajo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AccionPesoBajo"
}
df_AccionPesoBajo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AccionPesoBajo")
print('carga AccionPesoBajo')
df_producciondetalle1 = spark.sql(f"SELECT \
                   MO.IRN as BrimFieldTransIRN, MO.ProteinEntitiesIRN, MO.ProteinGrowoutCodesIRN,\
                   MO.ProteinFarmsIRN, MO.ProteinCostCentersIRN,MO.FmimFeedTypesIRN, PCC.ProteinDivisionsIRN, \
                   PE.ProteinHousesIRN,PE.ProteinBreedCodesIRN,PE.ProteinStandardVersionsIRN, \
                   PE.ProteinTechSupervisorsIRN,PE.ProteinProductsAnimalsIRN,PSV.ProteinStandardsIRN,BE.IRN AS BrimEntitiesIRN \
                   ,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo \
                   ,nvl(LT.fecha,cast('1899-11-30' as date)) fecha \
                   ,(select distinct pk_empresa from {database_name_gl}.lk_empresa where cempresa=1) as pk_empresa \
                   ,nvl(LD.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) as pk_division \
                   ,nvl(LP.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) as pk_zona \
                   ,nvl(LP.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) as pk_subzona \
                   ,nvl(LP.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) as pk_plantel \
                   ,nvl(LL.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) as pk_lote \
                   ,nvl(LG.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) as pk_galpon \
                   ,nvl(LS.pk_sexo,(select pk_sexo from {database_name_gl}.lk_sexo where csexo=0)) pk_sexo \
                   ,nvl(LST.pk_standard,(select pk_standard from {database_name_gl}.lk_standard where cstandard='0')) pk_standard \
                   ,nvl(LPR.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto \
                   ,nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name_gl}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
                   ,nvl(GCO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name_gl}.lk_grupoconsumo where cgrupoconsumo='0')) as pk_grupoconsumo \
                   ,nvl(LEP.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie \
                   ,nvl(LES.pk_estado,(select pk_estado from {database_name_gl}.lk_estado where cestado=0)) pk_estado \
                   ,nvl(LAD.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0'))pk_administrador \
                   ,nvl(FAL.pk_alimento,(select pk_alimento from {database_name_gl}.lk_alimento where calimento='0')) as pk_alimento \
                   ,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) as pk_proveedor \
                   ,LSV.pk_semanavida \
                   ,LDV.pk_diasvida \
                   ,MO.ComplexEntityNo \
                   ,BE.FirstHatchDate FechaNacimiento \
                   ,MO.FirstHatchDateAge as Edad \
                   ,date_format(BE.LastDateSold,'yyyyMMdd') as FechaCierre \
                   ,date_format(MVB.FirstDateSold,'yyyyMMdd') as FechaInicioSaca \
                   ,date_format(MVB.LastDateSold,'yyyyMMdd')  as FechaFinSaca \
                   ,MO.xDate \
                   ,LG.area as AreaGalpon \
                   ,nvl(MO.Mortality,0) AS MortDia \
                   ,nvl(MO.Weight,0) Pesodia \
                   ,round(nvl(MO.FeedConsumed,0),3) AS ConsDia \
                   ,nvl(PE.U_UnidadesSeleccionadas,0) AS UnidSeleccion \
                   ,nvl(BSD.U_MortPercent, 0) as STDMortDia \
                   ,nvl(BSD.U_MortPorcAcm,0) as STDMortDiaAcum \
                   ,nvl(BSD.U_MortPorcSem,0) as STDMortSem \
                   ,nvl(BSD.U_PesoVivo,0) as STDPeso \
                   ,nvl(BSD.U_FeedConsumed,0) as STDConsDia \
                   ,nvl(BSD.U_ConsAlimAcum,0) as STDConsAcum \
                   ,nvl(BSD.U_IEP,0) as STD_IEP \
                   ,nvl(BSD.U_FeedConversionBC,0) as U_FeedConversionBC \
                   ,nvl(BSD.U_WeightGainDay,0) as U_WeightGainDay \
                   ,nvl(BSD.U_MortAcumPercent,0) as U_MortAcumPercent \
                   ,DATEDIFF(BE.FirstDateSold, BE.FirstHatchDate) AS EdadInicioSaca \
                   ,nvl(MO.U_PEAccidentados,0) as U_PEAccidentados \
                   ,nvl(MO.U_PEHigadoGraso,0) as U_PEHigadoGraso \
                   ,nvl(MO.U_PEHepatomegalia,0) as U_PEHepatomegalia \
                   ,nvl(MO.U_PEHigadoHemorragico,0) as U_PEHigadoHemorragico \
                   ,nvl(MO.U_PEInanicion,0) as U_PEInanicion \
                   ,nvl(MO.U_PEProblemaRespiratorio,0) as U_PEProblemaRespiratorio \
                   ,nvl(MO.U_PESCH,0) as U_PESCH \
                   ,nvl(MO.U_PEEnteritis,0) as U_PEEnteritis \
                   ,nvl(MO.U_PEAscitis,0) as U_PEAscitis \
                   ,nvl(MO.U_PEMuerteSubita,0) as U_PEMuerteSubita \
                   ,nvl(MO.U_PEEstresPorCalor,0) as U_PEEstresPorCalor \
                   ,nvl(MO.U_PEHidropericardio,0) as U_PEHidropericardio \
                   ,nvl(MO.U_PEHemopericardio,0) as U_PEHemopericardio \
                   ,nvl(MO.U_PEUratosis,0) as U_PEUratosis \
                   ,nvl(MO.U_PEMaterialCaseoso,0) as U_PEMaterialCaseoso \
                   ,nvl(MO.U_PEOnfalitis,0) as U_PEOnfalitis \
                   ,nvl(MO.U_PERetencionDeYema,0) as U_PERetencionDeYema \
                   ,nvl(MO.U_PEErosionDeMolleja,0) as U_PEErosionDeMolleja \
                   ,nvl(MO.U_PEHemorragiaMusculos,0) as U_PEHemorragiaMusculos \
                   ,nvl(MO.U_PESangreEnCiego,0) as U_PESangreEnCiego \
                   ,nvl(MO.U_PEPericarditis,0) as U_PEPericarditis \
                   ,nvl(MO.U_PEPeritonitis,0) as U_PEPeritonitis \
                   ,nvl(MO.U_PEProlapso,0) as U_PEProlapso \
                   ,nvl(MO.U_PEPicaje,0) as U_PEPicaje \
                   ,nvl(MO.U_PERupturaAortica,0) as U_PERupturaAortica \
                   ,nvl(MO.U_PEBazoMoteado,0) as U_PEBazoMoteado \
                   ,nvl(MO.U_PENoViable,0) as U_PENoViable \
                   ,nvl(MO.U_Pigmentacion,0) as Pigmentacion \
                   ,MVB.GRN \
                   ,nvl(CAT.categoria,'-') U_categoria \
                   ,nvl(AT.FlagAtipico,1) FlagAtipico \
                   ,nvl(MO.U_PEAerosaculitisG2,0) as U_PEAerosaculitisG2 \
                   ,nvl(MO.U_PECojera,0) as U_PECojera \
                   ,nvl(MO.U_PEHigadoIcterico,0) as U_PEHigadoIcterico \
                   ,nvl(MO.U_PEMaterialCaseoso_po1ra,0) as U_PEMaterialCaseoso_po1ra \
                   ,nvl(MO.U_PEMaterialCaseosoMedRetr,0) as U_PEMaterialCaseosoMedRetr \
                   ,nvl(MO.U_PENecrosisHepatica,0) as U_PENecrosisHepatica \
                   ,nvl(MO.U_PENeumonia,0) as U_PENeumonia \
                   ,nvl(MO.U_PESepticemia,0) as U_PESepticemia \
                   ,nvl(MO.U_PEVomitoNegro,0) as U_PEVomitoNegro \
                   ,nvl(MO.U_PEAsperguillius,0) as U_PEAsperguillius \
                   ,nvl(MO.U_PEBazoGrandeMot,0) as U_PEBazoGrandeMot \
                   ,nvl(MO.U_PECorazonGrande,0) as U_PECorazonGrande \
                   ,nvl(MO.U_PECuadroToxico,0) as U_PECuadroToxico \
                   ,nvl(MO.U_PavosBB,0) as U_PavosBB \
                   ,FlagTransfPavos \
                   ,SourceComplexEntityNo \
                   ,DestinationComplexEntityNo \
                   ,MO.ProteinProductsIRN \
                   ,CPB.U_CausaPesoBajo \
                   ,APB.U_AccionPesoBajo \
                   ,nvl(MO.U_RuidosTotales,0) U_RuidosTotales \
                   ,round(nvl(MO.Price,0),3) Price \
                   ,BSD.U_ConsumoGasInvierno \
                   ,BSD.U_ConsumoGasVerano \
                   ,MO.FormulaNo \
                   ,MO.FormulaName \
                   ,MO.FeedTypeNo \
                   ,MO.FeedTypeName \
                   ,MVBF.GrowerType idtipogranja \
                   ,MO.uniformity \
                   ,MO.CV \
                   ,LT.fecha DescripFecha \
                   ,''DescripEmpresa \
                   ,''DescripDivision \
                   ,''DescripZona \
                   ,''DescripSubzona \
                   ,''Plantel \
                   ,''Lote \
                   ,''Galpon \
                   ,''DescripSexo \
                   ,''DescripStandard \
                   ,''DescripProducto \
                   ,''DescripTipoproducto \
                   ,''DescripGrupoconsumo \
                   ,''DescripEspecie \
                   ,''DescripEstado \
                   ,''DescripAdministrador \
                   ,''DescripAlimento \
                   ,''DescripProveedor \
                   ,''DescripSemanaVida \
                   ,''DescripDiaVida \
                   ,''DescripTipoGranja \
                   FROM  {database_name_tmp}.mortdia MO \
                   LEFT JOIN {database_name_si}.si_proteincostcenters PCC ON MO.ProteinCostCentersIRN = PCC.IRN \
                   LEFT JOIN {database_name_si}.si_proteinentities PE ON PE.IRN = MO.ProteinEntitiesIRN \
                   LEFT JOIN {database_name_si}.si_proteinstandardversions PSV ON PSV.IRN = PE.ProteinStandardVersionsIRN \
                   LEFT JOIN {database_name_tmp}.mortacumpercent BSD ON BSD.ProteinStandardVersionsIRN = PSV.IRN and BSD.age = MO.FirstHatchDateAge \
                   LEFT JOIN {database_name_si}.si_brimentities BE ON BE.ProteinEntitiesIRN= MO.ProteinEntitiesIRN \
                   LEFT JOIN {database_name_si}.si_mvbrimentities MVB ON MVB.ProteinEntitiesIRN = MO.ProteinEntitiesIRN \
                   LEFT JOIN {database_name_si}.si_mvbrimfarms MVBF ON MVBF.ProteinFarmsIRN = MO.ProteinFarmsIRN \
                   LEFT JOIN {database_name_gl}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd')= date_format(MO.EventDate, 'yyyyMMdd') \
                   LEFT JOIN {database_name_gl}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50)) \
                   LEFT JOIN {database_name_gl}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1) \
                   LEFT JOIN {database_name_gl}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and lg.activeflag in (false,true) \
                   LEFT JOIN {database_name_gl}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50)) \
                   LEFT JOIN {database_name_gl}.lk_diasvida  LDV ON LDV.firsthatchdateage = MO.FirstHatchDateAge \
                   LEFT JOIN {database_name_gl}.lk_semanavida LSV ON LSV.pk_semanavida = LDV.pk_semanavida \
                   LEFT JOIN {database_name_gl}.lk_sexo LS ON LS.csexo = rtrim(MO.PenNo) \
                   LEFT JOIN {database_name_gl}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                   LEFT JOIN {database_name_gl}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50)) \
                   LEFT JOIN {database_name_gl}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50)) \
                   LEFT JOIN {database_name_gl}.lk_grupoconsumo GCO ON LPR.pk_grupoconsumo = GCO.pk_grupoconsumo \
                   LEFT JOIN {database_name_gl}.lk_estado LES ON LES.cestado=PE.Status \
                   LEFT JOIN {database_name_gl}.lk_especie LEP ON LEP.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                   LEFT JOIN {database_name_gl}.lk_alimento FAL ON FAL.IRN = CAST(MO.FmimFeedTypesIRN AS varchar (50)) \
                   LEFT JOIN {database_name_gl}.lk_tipoproducto TP ON Upper(tp.ntipoproducto) = Upper(lpr.grupoproducto) \
                   LEFT JOIN {database_name_gl}.lk_proveedor PRO ON PRO.cproveedor = MVBF.VendorNo \
                   LEFT JOIN {database_name_tmp}.categoria CAT ON CAT.pk_plantel = LP.pk_plantel and CAT.pk_lote = LL.pk_lote and CAT.pk_galpon = LG.pk_galpon \
                   LEFT JOIN {database_name_tmp}.atipicos AT ON MO.ComplexEntityNo = AT.ComplexEntityNo \
                   LEFT JOIN {database_name_tmp}.CausaPesoBajo CPB ON CPB.complexentityno = MO.complexentityno and CPB.FirstHatchDateAge= MO.FirstHatchDateAge \
                   LEFT JOIN {database_name_tmp}.AccionPesoBajo APB ON APB.complexentityno = MO.complexentityno and APB.FirstHatchDateAge= MO.FirstHatchDateAge \
                   WHERE INT(MO.FirstHatchDateAge) >= 0 AND LD.cdivision = '1' AND MO.VoidFlag <> 'true' AND (date_format(MO.EventDate,'yyyyMM') >= \
                   date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))" )
#date_format(MO.EventDate,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) -INTERVAL 8 MONTH), 'yyyyMM')

#df_producciondetalle1.show(10)
print('cargando tabla de df_producciondetalle1')
df_producciondetalle2 = spark.sql(f"SELECT MO.IRN as BrimFieldTransIRN, MO.ProteinEntitiesIRN, MO.ProteinGrowoutCodesIRN,MO.ProteinFarmsIRN, MO.ProteinCostCentersIRN,MO.FmimFeedTypesIRN, PCC.ProteinDivisionsIRN, \
                                  PE.ProteinHousesIRN,PE.ProteinBreedCodesIRN,PE.ProteinStandardVersionsIRN,PE.ProteinTechSupervisorsIRN,PE.ProteinProductsAnimalsIRN,PSV.ProteinStandardsIRN,BE.IRN AS BrimEntitiesIRN, \
                                  nvl(LT.pk_tiempo,(select pk_tiempo from {database_name_gl}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo, \
                                  nvl(LT.fecha,cast('1899-11-30' as date)) fecha, \
                                  (select pk_empresa from {database_name_gl}.lk_empresa where cempresa=1) pk_empresa, \
                                  nvl(LD.pk_division,(select pk_division from {database_name_gl}.lk_division where cdivision=0)) as pk_division, \
                                  nvl(LP.pk_zona,(select pk_zona from {database_name_gl}.lk_zona where czona='0')) as pk_zona, \
                                  nvl(LP.pk_subzona,(select pk_subzona from {database_name_gl}.lk_subzona where csubzona='0')) as pk_subzona, \
                                  nvl(LP.pk_plantel,(select pk_plantel from {database_name_gl}.lk_plantel where cplantel='0')) as pk_plantel, \
                                  nvl(LL.pk_lote,(select pk_lote from {database_name_gl}.lk_lote where clote='0')) as pk_lote, \
                                  nvl(LG.pk_galpon,(select pk_galpon from {database_name_gl}.lk_galpon where cgalpon='0')) as pk_galpon, \
                                  nvl(LS.pk_sexo,(select pk_sexo from {database_name_gl}.lk_sexo where csexo=0)) pk_sexo, \
                                  nvl(LST.pk_standard,(select pk_standard from {database_name_gl}.lk_standard where cstandard='0')) pk_standard, \
                                  nvl(LPR.pk_producto,(select pk_producto from {database_name_gl}.lk_producto where cproducto='0')) pk_producto, \
                                  nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name_gl}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto, \
                                  nvl(GCO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name_gl}.lk_grupoconsumo where cgrupoconsumo='0')) as pk_grupoconsumo, \
                                  nvl(LEP.pk_especie,(select pk_especie from {database_name_gl}.lk_especie where cespecie='0')) pk_especie, \
                                  nvl(LES.pk_estado,(select pk_estado from {database_name_gl}.lk_estado where cestado=0)) pk_estado, \
                                  nvl(LAD.pk_administrador,(select pk_administrador from {database_name_gl}.lk_administrador where cadministrador='0'))pk_administrador, \
                                  nvl(FAL.pk_alimento,(select pk_alimento from {database_name_gl}.lk_alimento where calimento='0')) as pk_alimento, \
                                  nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name_gl}.lk_proveedor where cproveedor=0)) as pk_proveedor, \
                                  LSV.pk_semanavida, \
                                  LDV.pk_diasvida, \
                                  MO.ComplexEntityNo, \
                                  BE.FirstHatchDate FechaNacimiento, \
                                  MO.FirstHatchDateAge as Edad, \
                                  date_format(BE.LastDateSold,'yyyyMMdd') as FechaCierre, \
                                  date_format(MVB.FirstDateSold,'yyyyMMdd') as FechaInicioSaca, \
                                  date_format(MVB.LastDateSold,'yyyyMMdd') as FechaFinSaca, \
                                  MO.xDate,LG.area as AreaGalpon, \
                                  nvl(MO.Mortality,0) AS MortDia, \
                                  nvl(MO.Weight,0) Pesodia, \
                                  round(nvl(MO.FeedConsumed,0),3) AS ConsDia, \
                                  nvl(PE.U_UnidadesSeleccionadas,0) AS UnidSeleccion, \
                                  nvl(BSD.U_MortPercent, 0) as STDMortDia, \
                                  nvl(BSD.U_MortPorcAcm,0) as STDMortDiaAcum, \
                                  nvl(BSD.U_MortPorcSem,0) as STDMortSem, \
                                  nvl(BSD.U_PesoVivo,0) as STDPeso, \
                                  nvl(BSD.U_FeedConsumed,0) as STDConsDia, \
                                  nvl(BSD.U_ConsAlimAcum,0) as STDConsAcum, \
                                  nvl(BSD.U_IEP,0) as STD_IEP, \
                                  nvl(BSD.U_FeedConversionBC,0) as U_FeedConversionBC, \
                                  nvl(BSD.U_WeightGainDay,0) as U_WeightGainDay, \
                                  nvl(BSD.U_MortAcumPercent,0) as U_MortAcumPercent, \
                                  DATEDIFF(BE.FirstDateSold,BE.FirstHatchDate) AS EdadInicioSaca, \
                                  nvl(MO.U_PEAccidentados,0) as U_PEAccidentados, \
                                  nvl(MO.U_PEHigadoGraso,0) as U_PEHigadoGraso, \
                                  nvl(MO.U_PEHepatomegalia,0) as U_PEHepatomegalia, \
                                  nvl(MO.U_PEHigadoHemorragico,0) as U_PEHigadoHemorragico, \
                                  nvl(MO.U_PEInanicion,0) as U_PEInanicion, \
                                  nvl(MO.U_PEProblemaRespiratorio,0) as U_PEProblemaRespiratorio, \
                                  nvl(MO.U_PESCH,0) as U_PESCH, \
                                  nvl(MO.U_PEEnteritis,0) as U_PEEnteritis, \
                                  nvl(MO.U_PEAscitis,0) as U_PEAscitis, \
                                  nvl(MO.U_PEMuerteSubita,0) as U_PEMuerteSubita, \
                                  nvl(MO.U_PEEstresPorCalor,0) as U_PEEstresPorCalor, \
                                  nvl(MO.U_PEHidropericardio,0) as U_PEHidropericardio, \
                                  nvl(MO.U_PEHemopericardio,0) as U_PEHemopericardio, \
                                  nvl(MO.U_PEUratosis,0) as U_PEUratosis, \
                                  nvl(MO.U_PEMaterialCaseoso,0) as U_PEMaterialCaseoso, \
                                  nvl(MO.U_PEOnfalitis,0) as U_PEOnfalitis, \
                                  nvl(MO.U_PERetencionDeYema,0) as U_PERetencionDeYema, \
                                  nvl(MO.U_PEErosionDeMolleja,0) as U_PEErosionDeMolleja, \
                                  nvl(MO.U_PEHemorragiaMusculos,0) as U_PEHemorragiaMusculos, \
                                  nvl(MO.U_PESangreEnCiego,0) as U_PESangreEnCiego, \
                                  nvl(MO.U_PEPericarditis,0) as U_PEPericarditis, \
                                  nvl(MO.U_PEPeritonitis,0) as U_PEPeritonitis, \
                                  nvl(MO.U_PEProlapso,0) as U_PEProlapso, \
                                  nvl(MO.U_PEPicaje,0) as U_PEPicaje, \
                                  nvl(MO.U_PERupturaAortica,0) as U_PERupturaAortica, \
                                  nvl(MO.U_PEBazoMoteado,0) as U_PEBazoMoteado, \
                                  nvl(MO.U_PENoViable,0) as U_PENoViable, \
                                  nvl(MO.U_Pigmentacion,0) as Pigmentacion, \
                                  MVB.GRN, \
                                  nvl(CAT.categoria,'-') U_categoria, \
                                  nvl(AT.FlagAtipico,1) FlagAtipico, \
                                  nvl(MO.U_PEAerosaculitisG2,0) as U_PEAerosaculitisG2, \
                                  nvl(MO.U_PECojera,0) as U_PECojera, \
                                  nvl(MO.U_PEHigadoIcterico,0) as U_PEHigadoIcterico, \
                                  nvl(MO.U_PEMaterialCaseoso_po1ra,0) as U_PEMaterialCaseoso_po1ra, \
                                  nvl(MO.U_PEMaterialCaseosoMedRetr,0) as U_PEMaterialCaseosoMedRetr, \
                                  nvl(MO.U_PENecrosisHepatica,0) as U_PENecrosisHepatica, \
                                  nvl(MO.U_PENeumonia,0) as U_PENeumonia, \
                                  nvl(MO.U_PESepticemia,0) as U_PESepticemia, \
                                  nvl(MO.U_PEVomitoNegro,0) as U_PEVomitoNegro, \
                                  nvl(MO.U_PEAsperguillius,0) as U_PEAsperguillius, \
                                  nvl(MO.U_PEBazoGrandeMot,0) as U_PEBazoGrandeMot, \
                                  nvl(MO.U_PECorazonGrande,0) as U_PECorazonGrande, \
                                  nvl(MO.U_PECuadroToxico,0) as U_PECuadroToxico, \
                                  nvl(MO.U_PavosBB,0) as U_PavosBB, \
                                  FlagTransfPavos, \
                                  SourceComplexEntityNo, \
                                  DestinationComplexEntityNo, \
                                  MO.ProteinProductsIRN, \
                                  CPB.U_CausaPesoBajo, \
                                  APB.U_AccionPesoBajo, \
                                  nvl(MO.U_RuidosTotales,0) U_RuidosTotales, \
                                  round(nvl(MO.Price,0),3) Price, \
                                  BSD.U_ConsumoGasInvierno, \
                                  BSD.U_ConsumoGasVerano, \
                                  MO.FormulaNo, \
                                  MO.FormulaName, \
                                  MO.FeedTypeNo, \
                                  MO.FeedTypeName, \
                                  MVBF.GrowerType idtipogranja, \
                                  MO.uniformity, \
                                  MO.CV, \
                                  LT.fecha DescripFecha, \
                                  ''DescripEmpresa, \
                                  ''DescripDivision, \
                                  ''DescripZona, \
                                  ''DescripSubzona, \
                                  ''Plantel, \
                                  ''Lote, \
                                  ''Galpon, \
                                  ''DescripSexo, \
                                  ''DescripStandard, \
                                  ''DescripProducto, \
                                  ''DescripTipoproducto, \
                                  ''DescripGrupoconsumo, \
                                  ''DescripEspecie, \
                                  ''DescripEstado, \
                                  ''DescripAdministrador, \
                                  ''DescripAlimento, \
                                  ''DescripProveedor, \
                                  ''DescripSemanaVida, \
                                  ''DescripDiaVida, \
                                  ''DescripTipoGranja \
                                  FROM  {database_name_tmp}.mortdia MO \
                                  LEFT JOIN {database_name_si}.si_proteincostcenters PCC ON MO.ProteinCostCentersIRN = PCC.IRN \
                                  LEFT JOIN {database_name_si}.si_proteinentities PE ON PE.IRN = MO.ProteinEntitiesIRN \
                                  LEFT JOIN {database_name_si}.si_proteinstandardversions PSV ON PSV.IRN = PE.ProteinStandardVersionsIRN \
                                  LEFT JOIN {database_name_tmp}.mortacumpercent BSD ON BSD.ProteinStandardVersionsIRN = PSV.IRN and BSD.age = MO.FirstHatchDateAge \
                                  LEFT JOIN {database_name_si}.si_brimentities BE ON BE.ProteinEntitiesIRN= MO.ProteinEntitiesIRN \
                                  LEFT JOIN {database_name_si}.si_mvbrimentities MVB ON MVB.ProteinEntitiesIRN = MO.ProteinEntitiesIRN \
                                  LEFT JOIN {database_name_si}.si_mvbrimfarms MVBF ON MVBF.ProteinFarmsIRN = MO.ProteinFarmsIRN \
                                  LEFT JOIN {database_name_gl}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd')= date_format(MO.EventDate, 'yyyyMMdd') \
                                  LEFT JOIN {database_name_gl}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50)) \
                                  LEFT JOIN {database_name_gl}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1) \
                                  LEFT JOIN {database_name_gl}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and lg.activeflag in (false,true) \
                                  LEFT JOIN {database_name_gl}.lk_division LD ON LD.irn = cast(PCC.ProteinDivisionsIRN as varchar(50)) \
                                  LEFT JOIN {database_name_gl}.lk_diasvida  LDV ON LDV.firsthatchdateage = MO.FirstHatchDateAge \
                                  LEFT JOIN {database_name_gl}.lk_semanavida LSV ON LSV.pk_semanavida = LDV.pk_semanavida \
                                  LEFT JOIN {database_name_gl}.lk_sexo LS ON LS.csexo = rtrim(MO.PenNo) \
                                  LEFT JOIN {database_name_gl}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                                  LEFT JOIN {database_name_gl}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50)) \
                                  LEFT JOIN {database_name_gl}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50)) \
                                  LEFT JOIN {database_name_gl}.lk_grupoconsumo GCO ON LPR.pk_grupoconsumo = GCO.pk_grupoconsumo \
                                  LEFT JOIN {database_name_gl}.lk_estado LES ON LES.cestado=PE.Status \
                                  LEFT JOIN {database_name_gl}.lk_especie LEP ON LEP.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                                  LEFT JOIN {database_name_gl}.lk_alimento FAL ON FAL.IRN = CAST(MO.FmimFeedTypesIRN AS varchar (50)) \
                                  LEFT JOIN {database_name_gl}.lk_tipoproducto TP ON Upper(tp.ntipoproducto) = Upper(lpr.grupoproducto) \
                                  LEFT JOIN {database_name_gl}.lk_proveedor PRO ON PRO.cproveedor = MVBF.VendorNo \
                                  LEFT JOIN {database_name_tmp}.categoria CAT ON CAT.pk_plantel = LP.pk_plantel and CAT.pk_lote = LL.pk_lote and CAT.pk_galpon = LG.pk_galpon \
                                  LEFT JOIN {database_name_tmp}.atipicos AT ON MO.ComplexEntityNo = AT.ComplexEntityNo \
                                  LEFT JOIN {database_name_tmp}.CausaPesoBajo CPB ON CPB.complexentityno = MO.complexentityno and CPB.FirstHatchDateAge= MO.FirstHatchDateAge \
                                  LEFT JOIN {database_name_tmp}.AccionPesoBajo APB ON APB.complexentityno = MO.complexentityno and APB.FirstHatchDateAge= MO.FirstHatchDateAge \
                                  WHERE (date_format(MO.EventDate,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) AND LD.cdivision = '2' \
                                  AND MO.VoidFlag <> 'true' ")
#date_format(MO.EventDate,'yyyyMM')>= date_format(((current_date() - INTERVAL 5 HOURS) -INTERVAL 8 MONTH), 'yyyyMM')
#df_producciondetalle2.show(10)
print('cargando tabla de df_producciondetalle2')
df_producciondetalle = df_producciondetalle1.union(df_producciondetalle2)
#df_producciondetalle.createOrReplaceTempView("producciondetalle")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/producciondetalle"
}
df_producciondetalle.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.producciondetalle")
print('carga producciondetalle')
df_producciondetalle_upd = spark.sql(f"select \
                                      BrimFieldTransIRN, ProteinEntitiesIRN, ProteinGrowoutCodesIRN, ProteinFarmsIRN, ProteinCostCentersIRN,FmimFeedTypesIRN,ProteinDivisionsIRN, \
                                      ProteinHousesIRN,ProteinBreedCodesIRN,ProteinStandardVersionsIRN,ProteinTechSupervisorsIRN,ProteinProductsAnimalsIRN,ProteinStandardsIRN,BrimEntitiesIRN \
                                      ,pk_tiempo \
                                      ,fecha \
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
                                      ,pk_grupoconsumo \
                                      ,pk_especie \
                                      ,pk_estado \
                                      ,pk_administrador \
                                      ,pk_alimento \
                                      ,pk_proveedor \
                                      ,CASE WHEN (Edad = 0 or Edad = -1) and (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) and pk_division = 2 then 2 \
                                              WHEN Edad <= 0 and (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) and pk_division = 4 then 2 \
                                          else pk_semanavida end as pk_semanavida \
                                      ,CASE WHEN (Edad = 0 or Edad = -1) and (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))  and pk_division = 2 then 2 \
                                              WHEN Edad <= 0 and (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))  and pk_division = 4 then 2 \
                                          else pk_diasvida end as pk_diasvida \
                                      ,ComplexEntityNo \
                                      ,FechaNacimiento \
                                      ,Edad \
                                      ,FechaCierre \
                                      ,FechaInicioSaca \
                                      ,FechaFinSaca \
                                      ,xDate \
                                      ,AreaGalpon \
                                      ,MortDia \
                                      ,Pesodia \
                                      ,ConsDia \
                                      ,UnidSeleccion \
                                      ,STDMortDia \
                                      ,STDMortDiaAcum \
                                      ,STDMortSem \
                                      ,STDPeso \
                                      ,STDConsDia \
                                      ,STDConsAcum \
                                      ,STD_IEP \
                                      ,U_FeedConversionBC \
                                      ,U_WeightGainDay \
                                      ,U_MortAcumPercent \
                                      ,EdadInicioSaca \
                                      ,U_PEAccidentados \
                                      ,U_PEHigadoGraso \
                                      ,U_PEHepatomegalia \
                                      ,U_PEHigadoHemorragico \
                                      ,U_PEInanicion \
                                      ,U_PEProblemaRespiratorio \
                                      ,U_PESCH \
                                      ,U_PEEnteritis \
                                      ,U_PEAscitis \
                                      ,U_PEMuerteSubita \
                                      ,U_PEEstresPorCalor \
                                      ,U_PEHidropericardio \
                                      ,U_PEHemopericardio \
                                      ,U_PEUratosis \
                                      ,U_PEMaterialCaseoso \
                                      ,U_PEOnfalitis \
                                      ,U_PERetencionDeYema \
                                      ,U_PEErosionDeMolleja \
                                      ,U_PEHemorragiaMusculos \
                                      ,U_PESangreEnCiego \
                                      ,U_PEPericarditis \
                                      ,U_PEPeritonitis \
                                      ,U_PEProlapso \
                                      ,U_PEPicaje \
                                      ,U_PERupturaAortica \
                                      ,U_PEBazoMoteado \
                                      ,U_PENoViable \
                                      ,Pigmentacion \
                                      ,GRN \
                                      ,U_categoria \
                                      ,FlagAtipico \
                                      ,U_PEAerosaculitisG2 \
                                      ,U_PECojera \
                                      ,U_PEHigadoIcterico \
                                      ,U_PEMaterialCaseoso_po1ra \
                                      ,U_PEMaterialCaseosoMedRetr \
                                      ,U_PENecrosisHepatica \
                                      ,U_PENeumonia \
                                      ,U_PESepticemia \
                                      ,U_PEVomitoNegro \
                                      ,U_PEAsperguillius \
                                      ,U_PEBazoGrandeMot \
                                      ,U_PECorazonGrande \
                                      ,U_PECuadroToxico \
                                      ,U_PavosBB \
                                      ,FlagTransfPavos \
                                      ,SourceComplexEntityNo \
                                      ,DestinationComplexEntityNo \
                                      ,ProteinProductsIRN \
                                      ,U_CausaPesoBajo \
                                      ,U_AccionPesoBajo \
                                      ,U_RuidosTotales \
                                      ,Price \
                                      ,U_ConsumoGasInvierno \
                                      ,U_ConsumoGasVerano \
                                      ,FormulaNo \
                                      ,FormulaName \
                                      ,FeedTypeNo \
                                      ,FeedTypeName \
                                      ,idtipogranja \
                                      ,uniformity \
                                      ,CV \
                                      ,DescripFecha \
                                      ,DescripEmpresa \
                                      ,DescripDivision \
                                      ,DescripZona \
                                      ,DescripSubzona \
                                      ,Plantel \
                                      ,Lote \
                                      ,Galpon \
                                      ,DescripSexo \
                                      ,DescripStandard \
                                      ,DescripProducto \
                                      ,DescripTipoproducto \
                                      ,DescripGrupoconsumo \
                                      ,DescripEspecie \
                                      ,DescripEstado \
                                      ,DescripAdministrador \
                                      ,DescripAlimento \
                                      ,DescripProveedor \
                                      ,DescripSemanaVida \
                                      ,DescripDiaVida \
                                      ,DescripTipoGranja \
                                       from {database_name_tmp}.producciondetalle")

#df_producciondetalle_upd.createOrReplaceTempView("producciondetalle_upd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/producciondetalle_upd"
}
df_producciondetalle_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.producciondetalle_upd")
print('carga producciondetalle_upd')

#Tabla temporal para insertar el maximo dia donde se encuentra el STD
df_STDMAS42DIAS = spark.sql(f"SELECT pk_tiempo,pk_diasvida,ComplexEntityNo,STDConsDia,STDConsAcum,U_WeightGainDay,U_FeedConversionBC \
                             FROM {database_name_tmp}.producciondetalle_upd MD \
                             WHERE pk_diasvida = (SELECT MAX(pk_diasvida) \
                                                  FROM {database_name_tmp}.producciondetalle_upd MD1 \
                                                  WHERE md.ComplexEntityNo = MD1.ComplexEntityNo AND pk_diasvida >= 43 AND STDConsDia <> 0  AND STDConsAcum <> 0 AND U_WeightGainDay<>0 AND U_FeedConversionBC<>0 \
                                                  ) \
                             AND pk_empresa = 1 AND pk_division = 4 AND (date_format(fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                             GROUP BY pk_tiempo,pk_diasvida,ComplexEntityNo,STDConsDia,STDConsAcum,U_WeightGainDay,U_FeedConversionBC")
#df_STDMAS42DIAS.createOrReplaceTempView("STDMAS42DIAS")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDMAS42DIAS"
}
df_STDMAS42DIAS.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDMAS42DIAS")
print('carga STDMAS42DIAS')
#Actualiza los STD mayores de 43 que no tienen valores
df_producciondetalle_upd2 = spark.sql(f"select \
                                          A.BrimFieldTransIRN, A.ProteinEntitiesIRN, A.ProteinGrowoutCodesIRN, A.ProteinFarmsIRN, A.ProteinCostCentersIRN, A.FmimFeedTypesIRN, A.ProteinDivisionsIRN, \
                                          A.ProteinHousesIRN, A.ProteinBreedCodesIRN, A.ProteinStandardVersionsIRN, A.ProteinTechSupervisorsIRN, A.ProteinProductsAnimalsIRN, A.ProteinStandardsIRN, A.BrimEntitiesIRN \
                                         ,A.pk_tiempo \
                                         ,A.pk_empresa \
                                         ,A.pk_division \
                                         ,A.pk_zona \
                                         ,A.pk_subzona \
                                         ,A.pk_plantel \
                                         ,A.pk_lote \
                                         ,A.pk_galpon \
                                         ,A.pk_sexo \
                                         ,A.pk_standard \
                                         ,A.pk_producto \
                                         ,A.pk_tipoproducto \
                                         ,A.pk_grupoconsumo \
                                         ,A.pk_especie \
                                         ,A.pk_estado \
                                         ,A.pk_administrador \
                                         ,A.pk_alimento \
                                         ,A.pk_proveedor \
                                         ,A.pk_semanavida \
                                         ,A.pk_diasvida \
                                         ,A.ComplexEntityNo \
                                         ,A.FechaNacimiento \
                                         ,A.Edad \
                                         ,A.FechaCierre \
                                         ,A.FechaInicioSaca \
                                         ,A.FechaFinSaca \
                                         ,A.xDate \
                                         ,A.AreaGalpon \
                                         ,A.MortDia \
                                         ,A.Pesodia \
                                         ,A.ConsDia \
                                         ,A.UnidSeleccion \
                                         ,A.STDMortDia \
                                         ,A.STDMortDiaAcum \
                                         ,A.STDMortSem \
                                         ,A.STDPeso \
                                         ,CASE WHEN A.pk_diasvida > 43 AND A.STDConsdia = 0 AND A.STDConsAcum = 0 AND A.U_WeightGainDay = 0 AND A.U_FeedConversionBC = 0 AND (date_format(A.fecha,'yyyyMM') >= \
                                                   date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) THEN  B.STDConsDia ELSE  A.STDConsDia END AS STDConsDia \
                                         ,CASE WHEN A.pk_diasvida > 43 AND A.STDConsdia = 0 AND A.STDConsAcum = 0 AND A.U_WeightGainDay = 0 AND A.U_FeedConversionBC = 0 AND (date_format(A.fecha,'yyyyMM') >= \
                                                   date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) THEN  B.STDConsAcum ELSE  A.STDConsAcum END AS STDConsAcum \
                                         ,A.STD_IEP \
                                         ,CASE WHEN A.pk_diasvida > 43 AND A.STDConsdia = 0 AND A.STDConsAcum = 0 AND A.U_WeightGainDay = 0 AND A.U_FeedConversionBC = 0 AND (date_format(A.fecha,'yyyyMM') >= \
                                                   date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) THEN  B.U_FeedConversionBC ELSE  A.U_FeedConversionBC END AS U_FeedConversionBC \
                                         ,CASE WHEN A.pk_diasvida > 43 AND A.STDConsdia = 0 AND A.STDConsAcum = 0 AND A.U_WeightGainDay = 0 AND A.U_FeedConversionBC = 0 AND (date_format(A.fecha,'yyyyMM') >= \
                                                   date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) THEN  B.U_WeightGainDay ELSE  A.U_WeightGainDay END AS U_WeightGainDay \
                                         ,A.U_MortAcumPercent \
                                         ,A.EdadInicioSaca \
                                         ,A.U_PEAccidentados \
                                         ,A.U_PEHigadoGraso \
                                         ,A.U_PEHepatomegalia \
                                         ,A.U_PEHigadoHemorragico \
                                         ,A.U_PEInanicion \
                                         ,A.U_PEProblemaRespiratorio \
                                         ,A.U_PESCH \
                                         ,A.U_PEEnteritis \
                                         ,A.U_PEAscitis \
                                         ,A.U_PEMuerteSubita \
                                         ,A.U_PEEstresPorCalor \
                                         ,A.U_PEHidropericardio \
                                         ,A.U_PEHemopericardio \
                                         ,A.U_PEUratosis \
                                         ,A.U_PEMaterialCaseoso \
                                         ,A.U_PEOnfalitis \
                                         ,A.U_PERetencionDeYema \
                                         ,A.U_PEErosionDeMolleja \
                                         ,A.U_PEHemorragiaMusculos \
                                         ,A.U_PESangreEnCiego \
                                         ,A.U_PEPericarditis \
                                         ,A.U_PEPeritonitis \
                                         ,A.U_PEProlapso \
                                         ,A.U_PEPicaje \
                                         ,A.U_PERupturaAortica \
                                         ,A.U_PEBazoMoteado \
                                         ,A.U_PENoViable \
                                         ,A.Pigmentacion \
                                         ,A.GRN \
                                         ,A.U_categoria \
                                         ,A.FlagAtipico \
                                         ,A.U_PEAerosaculitisG2 \
                                         ,A.U_PECojera \
                                         ,A.U_PEHigadoIcterico \
                                         ,A.U_PEMaterialCaseoso_po1ra \
                                         ,A.U_PEMaterialCaseosoMedRetr \
                                         ,A.U_PENecrosisHepatica \
                                         ,A.U_PENeumonia \
                                         ,A.U_PESepticemia \
                                         ,A.U_PEVomitoNegro \
                                         ,A.U_PEAsperguillius \
                                         ,A.U_PEBazoGrandeMot \
                                         ,A.U_PECorazonGrande \
                                         ,A.U_PECuadroToxico \
                                         ,A.U_PavosBB \
                                         ,A.FlagTransfPavos \
                                         ,A.SourceComplexEntityNo \
                                         ,A.DestinationComplexEntityNo \
                                         ,A.ProteinProductsIRN \
                                         ,A.U_CausaPesoBajo \
                                         ,A.U_AccionPesoBajo \
                                         ,A.U_RuidosTotales \
                                         ,A.Price \
                                         ,A.U_ConsumoGasInvierno \
                                         ,A.U_ConsumoGasVerano \
                                         ,A.FormulaNo \
                                         ,A.FormulaName \
                                         ,A.FeedTypeNo \
                                         ,A.FeedTypeName \
                                         ,A.idtipogranja \
                                         ,A.uniformity \
                                         ,A.CV \
                                         ,A.DescripFecha \
                                         ,A.DescripEmpresa \
                                         ,A.DescripDivision \
                                         ,A.DescripZona \
                                         ,A.DescripSubzona \
                                         ,A.Plantel \
                                         ,A.Lote \
                                         ,A.Galpon \
                                         ,A.DescripSexo \
                                         ,A.DescripStandard \
                                         ,A.DescripProducto \
                                         ,A.DescripTipoproducto \
                                         ,A.DescripGrupoconsumo \
                                         ,A.DescripEspecie \
                                         ,A.DescripEstado \
                                         ,A.DescripAdministrador \
                                         ,A.DescripAlimento \
                                         ,A.DescripProveedor \
                                         ,A.DescripSemanaVida \
                                         ,A.DescripDiaVida \
                                         ,A.DescripTipoGranja \
                                          from {database_name_tmp}.producciondetalle_upd A \
                                          LEFT JOIN {database_name_tmp}.STDMAS42DIAS B ON A.ComplexEntityNo = B.ComplexEntityNo")
#df_producciondetalle_upd2.createOrReplaceTempView("producciondetalle_upd2")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/producciondetalle_upd2"
}
df_producciondetalle_upd2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.producciondetalle_upd2")
print('carga producciondetalle_upd2')
#Inserto en tabla temporal todos los destinos de transferencia, aplica solo para pavos.
df_DestinationComplexEntityNo = spark.sql(f"select ProteinEntitiesIRN \
                                                  ,ProteinGrowoutCodesIRN \
                                                  ,ProteinFarmsIRN \
                                                  ,ProteinCostCentersIRN \
                                                  ,ProteinDivisionsIRN \
                                                  ,ProteinHousesIRN \
                                                  ,ProteinBreedCodesIRN \
                                                  ,ProteinStandardVersionsIRN \
                                                  ,ProteinTechSupervisorsIRN \
                                                  ,ProteinProductsAnimalsIRN \
                                                  ,ProteinStandardsIRN \
                                                  ,BrimEntitiesIRN \
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
                                                  ,pk_grupoconsumo \
                                                  ,pk_especie \
                                                  ,pk_estado \
                                                  ,pk_administrador \
                                                  ,pk_proveedor \
                                                  ,ComplexEntityNo \
                                                  ,FechaNacimiento \
                                                  ,FechaCierre \
                                                  ,FechaInicioSaca \
                                                  ,FechaFinSaca \
                                                  ,AreaGalpon \
                                                  ,EdadInicioSaca \
                                                  ,GRN \
                                                  ,U_categoria \
                                                  ,FlagAtipico \
                                                  from {database_name_tmp}.producciondetalle_upd2 \
                                                  where ComplexEntityNo in (select DestinationComplexEntityNo \
                                                                            from (select MIN(cast(cast(xdate as timestamp) as date)) xdate,SourceComplexEntityNo, \
                                                                                  DestinationComplexEntityNo \
                                                                                  from {database_name_si}.si_mvpmtstransferdestdetails \
                                                                                  GROUP BY SourceComplexEntityNo,DestinationComplexEntityNo)  \
                                                                            where DEstinationComplexEntityNo like 'V%') \
                                                  group by ProteinEntitiesIRN \
                                                           ,ProteinGrowoutCodesIRN \
                                                           ,ProteinFarmsIRN \
                                                           ,ProteinCostCentersIRN \
                                                           ,ProteinDivisionsIRN \
                                                           ,ProteinHousesIRN \
                                                           ,ProteinBreedCodesIRN \
                                                           ,ProteinStandardVersionsIRN \
                                                           ,ProteinTechSupervisorsIRN \
                                                           ,ProteinProductsAnimalsIRN \
                                                           ,ProteinStandardsIRN \
                                                           ,BrimEntitiesIRN \
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
                                                           ,pk_grupoconsumo \
                                                           ,pk_especie \
                                                           ,pk_estado \
                                                           ,pk_administrador \
                                                           ,pk_proveedor \
                                                           ,ComplexEntityNo \
                                                           ,FechaNacimiento \
                                                           ,FechaCierre \
                                                           ,FechaInicioSaca \
                                                           ,FechaFinSaca \
                                                           ,AreaGalpon \
                                                           ,EdadInicioSaca \
                                                           ,GRN \
                                                           ,U_categoria \
                                                           ,FlagAtipico")
#df_DestinationComplexEntityNo.createOrReplaceTempView("DestinationComplexEntityNo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DestinationComplexEntityNo"
}
df_DestinationComplexEntityNo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.DestinationComplexEntityNo")
print('carga DestinationComplexEntityNo')
df_filter = spark.sql(f"select  DISTINCT SourceComplexEntityNo \
                        from (select MIN(cast(cast(xdate as timestamp) as date)) xdate,SourceComplexEntityNo,DestinationComplexEntityNo \
                              from {database_name_si}.si_mvpmtstransferdestdetails \
                              GROUP BY SourceComplexEntityNo,DestinationComplexEntityNo) \
                 where SourceComplexEntityNo like 'V%'")
#df_filter.createOrReplaceTempView("filter")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/filter"
}
df_filter.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.filter")
print('carga filter')
#Actualizo el complexentityno que inicio por el destino que realizo la transferencia. Aplica solo para pavos.
df_producciondetalle_upd3 = spark.sql(f"select DISTINCT \
                                      A.BrimFieldTransIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 and isnotnull(B.pk_empresa) = true THEN B.ProteinEntitiesIRN ELSE A.ProteinEntitiesIRN END AS ProteinEntitiesIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinGrowoutCodesIRN ELSE A.ProteinGrowoutCodesIRN END AS ProteinGrowoutCodesIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinFarmsIRN ELSE A.ProteinFarmsIRN END AS ProteinFarmsIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinCostCentersIRN ELSE A.ProteinCostCentersIRN END AS ProteinCostCentersIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinDivisionsIRN ELSE A.ProteinDivisionsIRN END AS ProteinDivisionsIRN, \
                                      A.FmimFeedTypesIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinHousesIRN ELSE A.ProteinHousesIRN END AS ProteinHousesIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinBreedCodesIRN ELSE A.ProteinBreedCodesIRN END AS ProteinBreedCodesIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinStandardVersionsIRN ELSE A.ProteinStandardVersionsIRN END AS ProteinStandardVersionsIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinTechSupervisorsIRN ELSE A.ProteinTechSupervisorsIRN END AS ProteinTechSupervisorsIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinProductsAnimalsIRN ELSE A.ProteinProductsAnimalsIRN END AS ProteinProductsAnimalsIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ProteinStandardsIRN ELSE A.ProteinStandardsIRN END AS ProteinStandardsIRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.BrimEntitiesIRN ELSE A.BrimEntitiesIRN END AS BrimEntitiesIRN, \
                                      A.pk_tiempo, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_empresa ELSE A.pk_empresa END AS pk_empresa, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_division ELSE A.pk_division END AS pk_division, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_zona ELSE A.pk_zona END AS pk_zona, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_subzona ELSE A.pk_subzona END AS pk_subzona, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_plantel ELSE A.pk_plantel END AS pk_plantel, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_lote ELSE A.pk_lote END AS pk_lote, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_galpon ELSE A.pk_galpon END AS pk_galpon, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_sexo ELSE A.pk_sexo END AS pk_sexo, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_standard ELSE A.pk_standard END AS pk_standard, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_producto ELSE A.pk_producto END AS pk_producto, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_tipoproducto ELSE A.pk_tipoproducto END AS pk_tipoproducto, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_grupoconsumo ELSE A.pk_grupoconsumo END AS pk_grupoconsumo, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_especie ELSE A.pk_especie END AS pk_especie, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_estado ELSE A.pk_estado END AS pk_estado, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_administrador ELSE A.pk_administrador END AS pk_administrador, \
                                      A.pk_alimento, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.pk_proveedor ELSE A.pk_proveedor END AS pk_proveedor, \
                                      A.pk_semanavida, \
                                      A.pk_diasvida, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.ComplexEntityNo ELSE A.ComplexEntityNo END AS ComplexEntityNo, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.FechaNacimiento ELSE A.FechaNacimiento END AS FechaNacimiento, \
                                      A.Edad, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.FechaCierre ELSE A.FechaCierre END AS FechaCierre, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.FechaInicioSaca ELSE A.FechaInicioSaca END AS FechaInicioSaca, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.FechaFinSaca ELSE A.FechaFinSaca END AS FechaFinSaca, \
                                      A.xDate, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.AreaGalpon ELSE A.AreaGalpon END AS AreaGalpon, \
                                      A.MortDia,A.Pesodia,A.ConsDia,A.UnidSeleccion,A.STDMortDia,A.STDMortDiaAcum,A.STDMortSem,A.STDPeso, \
                                      A.STDConsDia, \
                                      A.STDConsAcum, \
                                      A.STD_IEP,A.U_FeedConversionBC,A.U_WeightGainDay,A.U_MortAcumPercent, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.EdadInicioSaca ELSE A.EdadInicioSaca END AS EdadInicioSaca, \
                                      A.U_PEAccidentados,A.U_PEHigadoGraso,A.U_PEHepatomegalia,A.U_PEHigadoHemorragico,A.U_PEInanicion,A.U_PEProblemaRespiratorio,A.U_PESCH,A.U_PEEnteritis,A.U_PEAscitis, \
                                      A.U_PEMuerteSubita,A.U_PEEstresPorCalor,A.U_PEHidropericardio,A.U_PEHemopericardio,A.U_PEUratosis,A.U_PEMaterialCaseoso,A.U_PEOnfalitis,A.U_PERetencionDeYema, \
                                      A.U_PEErosionDeMolleja,A.U_PEHemorragiaMusculos,A.U_PESangreEnCiego,A.U_PEPericarditis,A.U_PEPeritonitis,A.U_PEProlapso,A.U_PEPicaje,A.U_PERupturaAortica,A.U_PEBazoMoteado, \
                                      A.U_PENoViable,A.Pigmentacion, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.GRN ELSE A.GRN END AS GRN, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.U_categoria ELSE A.U_categoria END AS U_categoria, \
                                      CASE WHEN isnotnull(C.SourceComplexEntityNo) = true AND A.pk_division = 2 AND isnotnull(B.pk_empresa) = true THEN B.FlagAtipico ELSE A.FlagAtipico END FlagAtipico, \
                                      A.U_PEAerosaculitisG2,A.U_PECojera,A.U_PEHigadoIcterico,A.U_PEMaterialCaseoso_po1ra,A.U_PEMaterialCaseosoMedRetr,A.U_PENecrosisHepatica,A.U_PENeumonia, \
                                      A.U_PESepticemia,A.U_PEVomitoNegro,A.U_PEAsperguillius,A.U_PEBazoGrandeMot,A.U_PECorazonGrande,A.U_PECuadroToxico,A.U_PavosBB,A.FlagTransfPavos,A.SourceComplexEntityNo, \
                                      A.DestinationComplexEntityNo,A.ProteinProductsIRN,A.U_CausaPesoBajo,A.U_AccionPesoBajo,A.U_RuidosTotales,A.Price,A.U_ConsumoGasInvierno,A.U_ConsumoGasVerano,A.FormulaNo, \
                                      A.FormulaName,A.FeedTypeNo,A.FeedTypeName,A.idtipogranja,A.uniformity,A.CV \
                                      ,A.DescripFecha \
                                      ,A.DescripEmpresa \
                                      ,A.DescripDivision \
                                      ,A.DescripZona \
                                      ,A.DescripSubzona \
                                      ,A.Plantel \
                                      ,A.Lote \
                                      ,A.Galpon \
                                      ,A.DescripSexo \
                                      ,A.DescripStandard \
                                      ,A.DescripProducto \
                                      ,A.DescripTipoproducto \
                                      ,A.DescripGrupoconsumo \
                                      ,A.DescripEspecie \
                                      ,A.DescripEstado \
                                      ,A.DescripAdministrador \
                                      ,A.DescripAlimento \
                                      ,A.DescripProveedor \
                                      ,A.DescripSemanaVida \
                                      ,A.DescripDiaVida \
                                      ,A.DescripTipoGranja \
                                      from {database_name_tmp}.producciondetalle_upd2 A \
                                      LEFT JOIN {database_name_tmp}.DestinationComplexEntityNo B ON A.DestinationComplexEntityNo = B.ComplexEntityNo \
                                      LEFT JOIN {database_name_tmp}.filter C ON C.SourceComplexEntityNo=A.ComplexEntityNo " )

#(select  DISTINCT SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'V%')
#df_producciondetalle_upd3.createOrReplaceTempView("producciondetalle_upd3")
#df_ft_producciondetalle = df_producciondetalle_upd3
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/producciondetalle_upd3"
}
df_producciondetalle_upd3.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.producciondetalle_upd3")
print('carga producciondetalle_upd3')

# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target1)   
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=9)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target1)
    datos_existentes = True
    logger.info(f"Datos existentes de stg_ProduccionDetalle cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en stg_ProduccionDetalle")



if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target1)
    data_after_delete = existing_data.filter(~((date_format(F.col("descripfecha"),'yyyy-MM-dd') >= fecha_str)))
    filtered_new_data = df_producciondetalle_upd3.filter((date_format(F.col("descripfecha"),'yyyy-MM-dd') >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/stg_ProduccionDetalleTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.stg_ProduccionDetalleTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/stg_ProduccionDetalleTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}stg_ProduccionDetalle"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.stg_ProduccionDetalle")
            
    print(f"agrega registros nuevos a la tabla stg_ProduccionDetalle : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla stg_ProduccionDetalle : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='stg_ProduccionDetalleTemporal')
    print(f"Tabla stg_ProduccionDetalleTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}stg_ProduccionDetalle"
    }
    df_producciondetalle_upd3.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.stg_ProduccionDetalle")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()