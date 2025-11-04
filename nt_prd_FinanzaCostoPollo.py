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
JOB_NAME = "nt_prd_FinanzaCostoPollo"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_date, current_timestamp,date_format,col
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

table_name1="ft_CostosIndicadores"
table_name2="ft_CostosEstructura"
table_name3="ft_CostosIndicadoresV"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
file_name_target2 = f"{bucket_name_prdmtech}{table_name2}/"
file_name_target3 = f"{bucket_name_prdmtech}{table_name3}/"

path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
path_target3 = f"s3://{bucket_name_target}/{file_name_target3}"
#database_name = "default"
print('cargando ruta')
df_Indicadores = spark.sql(f"""select 
 substring(A.FechaCierre,1,6) mes 
,A.categoria 
,A.ComplexEntityNo 
,B.cplantel Plantel 
,A.FechaInicioGranja FechaInicioCampana 
,A.FechaCrianza FechaInicioCrianza 
,C.nzona Zona 
,LSZ.csubzona SubZona 
,F.nproveedor Proveedor 
,D.ntipoproducto TipoProducto 
,E.abrevadministrador Administrador 
,A.FechaCierre FechaCierreCampana 
,A.PobInicial 
,A.MortDia MortCampo 
,(A.PobInicial - A.AvesRendidas) MortSistema 
,A.AvesLogradas 
,A.AvesRendidas 
,SobranFaltan 
,KilosRendidos 
,A.Gas 
,ROUND((A.Gas*A.PobInicial)/1000,0) GasGalones 
,A.AvesXm2 
,ROUND(A.PobInicial/A.AvesXm2,0) AreaCrianza 
,A.PorMort PorcMortCampo 
,round((((A.PobInicial - A.AvesRendidas)*1.0)/nullif(A.PobInicial,0))*100,2) PorcMortSistema 
,A.ICA 
,A.ICAAjustado 
,A.EdadGranja Edad 
,(A.EdadGranja * A.AvesRendidas) EdadPond 
,A.PesoProm Peso 
,ROUND(A.PorcMacho,1) PorcPobMacho 
,ROUND((A.PorcMacho * A.PobInicial)/100,0) PobMacho 
,A.GananciaDiaVenta GananciaPesoDia 
,A.KgXm2 
,ROUND((a.AvesRendidasMediano/(A.PobInicial*1.0))*100,2) PorcPolloMediano 
,A.AvesRendidasMediano PolloMediano 
,A.IEP 
,G.Cama 
,(G.Cama * A.PobInicial)/1000 CamaM3 
,G.Agua 
,(G.Agua * A.KilosRendidos) AguaL 
,A.DiasLimpieza 
,a.DiasCrianza 
,A.TotalCampana 
,A.DiasSaca 
,A.PorcPreIni 
,A.PorcIni 
,A.PorcAcab 
,A.PorcTerm 
,A.PorcFin 
,A.PorcConsumo PorcConsumoTotal 
,A.PreInicio 
,A.Inicio 
,A.Acabado 
,A.Terminado 
,A.Finalizador 
,A.ConsDia ConsumoTotal 
,'SAN FERNANDO' Empresa 
from {database_name_gl}.ft_consolidado_Lote A 
left join {database_name_gl}.lk_plantel B on A.pk_plantel = B.pk_plantel 
left join {database_name_gl}.lk_zona C on A.pk_zona = C.pk_zona 
left join {database_name_gl}.lk_subzona LSZ on A.pk_subzona = LSZ.pk_subzona 
left join {database_name_gl}.lk_tipoproducto D on A.pk_tipoproducto = D.pk_tipoproducto 
left join {database_name_gl}.lk_administrador E on A.pk_administrador = E.pk_administrador 
left join {database_name_gl}.lk_proveedor F on A.pk_proveedor = F.pk_proveedor 
left join {database_name_gl}.stg_gasaguacama G on A.ComplexEntityNo = G.ComplexEntityNo 
where substring(A.FechaCierre,1,6) = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and A.FlagArtAtipico = 2 and A.pk_empresa = 1 and A.pk_division = 4 and A.pk_estado in (3,4) 
order by A.FechaCierre,A.ComplexEntityNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Indicadores"
}
df_Indicadores.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Indicadores")
print('carga temporal df_Indicadores', df_Indicadores.count())
df_mvProteinJournalTrans = spark.sql(f"""select * 
from {database_name_costos_si}.si_mvProteinJournalTrans 
where SourceCode = 'EOP BRIM - Farm>Plant' 
and date_format(cast(xDate as timestamp),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -3)), 'yyyyMM')
and date_format(cast(xDate as timestamp),'yyyyMM') <= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemStageNo = 'GROW' 
and SystemElementUserNo = 'OUT' 
and SystemLocationGroupNo = 'BRLR'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvProteinJournalTransTemp1"
}
df_mvProteinJournalTrans.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.mvProteinJournalTransTemp1")
print('carga temporal mvProteinJournalTransTemp1',df_mvProteinJournalTrans.count())
df_PivotCosto = spark.sql(f"""
WITH pivot_data AS (
    SELECT 
        ComplexEntityNo,
        SystemCostElementNo,
        SUM(RelativeAmount) AS TotalAmount
    FROM {database_name_costos_tmp}.mvProteinJournalTransTemp1
    GROUP BY ComplexEntityNo, SystemCostElementNo
)

SELECT 
    ComplexEntityNo,
    SUM(CASE WHEN SystemCostElementNo = 'MED' THEN TotalAmount ELSE 0 END) AS MED,
    SUM(CASE WHEN SystemCostElementNo = 'SUMI1' THEN TotalAmount ELSE 0 END) AS SUMI1,
    SUM(CASE WHEN SystemCostElementNo = 'BRLRMALE' THEN TotalAmount ELSE 0 END) AS BRLRMALE,
    SUM(CASE WHEN SystemCostElementNo = 'TRANS' THEN TotalAmount ELSE 0 END) AS TRANS,
    SUM(CASE WHEN SystemCostElementNo = 'REPM' THEN TotalAmount ELSE 0 END) AS REPM,
    SUM(CASE WHEN SystemCostElementNo = 'GPAYBAS' THEN TotalAmount ELSE 0 END) AS GPAYBAS,
    SUM(CASE WHEN SystemCostElementNo = 'GAS' THEN TotalAmount ELSE 0 END) AS GAS,
    SUM(CASE WHEN SystemCostElementNo = 'AGUA' THEN TotalAmount ELSE 0 END) AS AGUA,
    SUM(CASE WHEN SystemCostElementNo = 'SERV' THEN TotalAmount ELSE 0 END) AS SERV,
    SUM(CASE WHEN SystemCostElementNo = 'BRLRFEMALE' THEN TotalAmount ELSE 0 END) AS BRLRFEMALE,
    SUM(CASE WHEN SystemCostElementNo = 'INDREC' THEN TotalAmount ELSE 0 END) AS INDREC,
    SUM(CASE WHEN SystemCostElementNo = 'DEPA' THEN TotalAmount ELSE 0 END) AS DEPA,
    SUM(CASE WHEN SystemCostElementNo = 'VACC' THEN TotalAmount ELSE 0 END) AS VACC,
    SUM(CASE WHEN SystemCostElementNo = 'INDR' THEN TotalAmount ELSE 0 END) AS INDR,
    SUM(CASE WHEN SystemCostElementNo = 'ING' THEN TotalAmount ELSE 0 END) AS ING,
    SUM(CASE WHEN SystemCostElementNo = 'SUMI' THEN TotalAmount ELSE 0 END) AS SUMI,
    SUM(CASE WHEN SystemCostElementNo = 'MAOB' THEN TotalAmount ELSE 0 END) AS MAOB,
    SUM(CASE WHEN SystemCostElementNo = 'CAMA' THEN TotalAmount ELSE 0 END) AS CAMA,
    SUM(CASE WHEN SystemCostElementNo = 'VIT' THEN TotalAmount ELSE 0 END) AS VIT,
    SUM(CASE WHEN SystemCostElementNo = 'ALQU' THEN TotalAmount ELSE 0 END) AS ALQU,
    SUM(CASE WHEN SystemCostElementNo = 'ELEC' THEN TotalAmount ELSE 0 END) AS ELEC,
    SUM(CASE WHEN SystemCostElementNo = 'MISC' THEN TotalAmount ELSE 0 END) AS MISC
FROM pivot_data
GROUP BY ComplexEntityNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PivotCostoTemp1"
}
df_PivotCosto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.PivotCostoTemp1")
print('carga temporal PivotCostoTemp1', df_PivotCosto.count())
df_costo1 = spark.sql(f"""select 
A.ComplexEntityNo 
,sum(nvl(RelativeAmount,0)*-1.0)CostoTotal 
,max(nvl(BRLRMALE,0)*-1.0) + max(nvl(BRLRFEMALE,0)*-1.0) CostoDirecPolloBB 
,max(nvl(ING,0)*-1.0) CostoDirecAlimento 
,max(nvl(MED,0)*-1.0) CostoDirecMedicina 
,max(nvl(VIT,0)*-1.0) CostoDirecVitamina 
,max(nvl(VACC,0)*-1.0) CostoDirecVacuna 
,max(nvl(GAS,0)*-1.0) CostoDirecGas 
,max(nvl(CAMA,0)*-1.0) CostoDirecCama 
,max(nvl(AGUA,0)*-1.0) CostoDirecAgua 
,max(nvl(GPAYBAS,0)*-1.0) CostoDirecServCrianza 
,max(nvl(MAOB,0)*-1.0) CostoIndirecMaOb 
,max(nvl(SUMI,0)*-1.0) CostoIndirecSumiPropio 
,max(nvl(SUMI1,0)*-1.0) CostoIndirecSumiRecib 
,max(nvl(ELEC,0)*-1.0) CostoIndirecElectri 
,max(nvl(REPM,0)*-1.0) CostoIndirecRepMan 
,max(nvl(INDR,0)*-1.0) CostoIndirecPropios 
,max(nvl(INDREC,0)*-1.0) CostoIndirecRecibidos 
,max(nvl(ALQU,0)*-1.0) CostoIndirecAlquileres 
,max(nvl(DEPA,0)*-1.0) CostoIndirecDeprec 
,max(nvl(TRANS,0)*-1.0) CostoIndirecTrans
,max(nvl(SERV,0)*-1.0) CostoIndirecServ 
from {database_name_costos_tmp}.mvProteinJournalTransTemp1 A 
left join {database_name_costos_tmp}.PivotCostoTemp1 B on A.ComplexEntityNo = B.ComplexEntityNo 
group by A.ComplexEntityNo""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costo1Temp1"
}
df_costo1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costo1Temp1")
print('carga temporal costo1Temp1', df_costo1.count())
df_ft_CostosIndicadores = spark.sql(f"""select 
A.Mes 
,A.Categoria 
,A.ComplexEntityNo 
,A.Plantel 
,A.FechaInicioCampana 
,A.FechaInicioCrianza 
,A.Zona 
,A.SubZona 
,A.Proveedor 
,A.TipoProducto 
,A.Administrador 
,A.FechaCierreCampana 
,A.PobInicial 
,A.MortCampo 
,A.MortSistema 
,A.AvesLogradas 
,A.AvesRendidas 
,A.SobranFaltan 
,round(A.KilosRendidos,4) KilosRendidos 
,round(A.Gas,4) Gas 
,round(A.GasGalones,4) GasGalones 
,round(A.AvesXm2 ,4) AvesXm2 
,round(a.AreaCrianza,4) AreaCrianza 
,round(A.PorcMortCampo,4) PorcMortCampo 
,round(A.PorcMortSistema,4) PorcMortSistema 
,round(A.ICA,4) ICA 
,round(A.ICAAjustado,4) ICAAjustado 
,round(A.Edad,4) Edad 
,round(A.EdadPond,4) EdadPond 
,round(A.Peso,4) Peso 
,round(A.PorcPobMacho,4) PorcPobMacho 
,round(A.PobMacho,4) PobMacho
,round(A.GananciaPesoDia,4) GananciaPesoDia 
,round(A.KgXm2 ,4) KgXm2 
,round(A.PorcPolloMediano,4) PorcPolloMediano 
,A.PolloMediano 
,round(A.IEP,4) IEP 
,round(A.Cama,4) Cama 
,round(A.CamaM3,4) CamaM3 
,round(A.Agua,4) Agua 
,round(A.AguaL,4) AguaL 
,A.DiasLimpieza 
,a.DiasCrianza 
,A.TotalCampana 
,A.DiasSaca
,round(A.PorcPreIni,4) PorcPreIni 
,round(A.PorcIni,4) PorcIni 
,round(A.PorcAcab,4) PorcAcab 
,round(A.PorcTerm,4) PorcTerm 
,round(A.PorcFin,4) PorcFin 
,round(A.PorcConsumoTotal,4) PorcConsumoTotal 
,round(A.PreInicio,4) PreInicio 
,round(A.Inicio,4) Inicio 
,round(A.Acabado,4) Acabado 
,round(A.Terminado,4) Terminado 
,round(A.Finalizador,4) Finalizador 
,round(A.ConsumoTotal,4) ConsumoTotal 
,round(B.CostoTotal / A.KilosRendidos,4) CostoKgprod 
,round(B.CostoDirecAlimento/A.ConsumoTotal,4) CostoAlimKgcons 
,round(B.CostoDirecPolloBB/A.PobInicial,4) CostoBBalojado 
,round(B.CostoDirecPolloBB,4) CostoDirecPolloBB 
,round(B.CostoDirecAlimento,4) CostoDirecAlimento 
,round(B.CostoDirecMedicina,4) CostoDirecMedicina
,round(B.CostoDirecVitamina,4) CostoDirecVitamina 
,round(B.CostoDirecVacuna,4) CostoDirecVacuna 
,round(B.CostoDirecGas,4) CostoDirecGas 
,round(B.CostoDirecCama,4) CostoDirecCama 
,round(B.CostoDirecAgua,4) CostoDirecAgua 
,round(B.CostoDirecServCrianza,4) CostoDirecServCrianza 
,round(B.CostoDirecPolloBB + B.CostoDirecAlimento + B.CostoDirecMedicina + B.CostoDirecVitamina + B.CostoDirecVacuna + B.CostoDirecGas + B.CostoDirecCama + B.CostoDirecAgua + B.CostoDirecServCrianza,4) CostoDirecto 
,round(B.CostoIndirecMaOb,4) CostoIndirecMaOb 
,round(B.CostoIndirecSumiPropio,4) CostoIndirecSumiPropio 
,round(B.CostoIndirecSumiRecib,4) CostoIndirecSumiRecib 
,round(B.CostoIndirecElectri,4) CostoIndirecElectri 
,round(B.CostoIndirecRepMan,4) CostoIndirecRepMan 
,round(B.CostoIndirecPropios,4) CostoIndirecPropios 
,round(B.CostoIndirecRecibidos,4) CostoIndirecRecibidos 
,round(B.CostoIndirecAlquileres,4) CostoIndirecAlquileres 
,round(B.CostoIndirecDeprec,4) CostoIndirecDeprec 
,round(B.CostoIndirecTrans,4) CostoIndirecTrans 
,round(B.CostoIndirecServ,4) CostoIndirecServ 
,round(B.CostoIndirecMaOb + B.CostoIndirecSumiPropio + B.CostoIndirecSumiRecib + B.CostoIndirecElectri + B.CostoIndirecRepMan + B.CostoIndirecPropios + 
B.CostoIndirecRecibidos + B.CostoIndirecAlquileres + B.CostoIndirecDeprec + B.CostoIndirecTrans + B.CostoIndirecServ,4) CostoIndirecto 
,round(B.CostoTotal,4) CostoTotal 
,round(B.CostoDirecPolloBB/A.KilosRendidos,4) DirecPolloBB 
,round(B.CostoDirecAlimento/A.KilosRendidos,4) DirecAlimento 
,round(B.CostoDirecMedicina/A.KilosRendidos,4) DirecMedicinas 
,round(B.CostoDirecVitamina/A.KilosRendidos,4) DirecVitaminas 
,round(B.CostoDirecVacuna/A.KilosRendidos,4) DirecVacunas 
,round(B.CostoDirecGas/A.KilosRendidos,4) DirecGas 
,round(B.CostoDirecCama/A.KilosRendidos,4) DirecCama 
,round(B.CostoDirecAgua/A.KilosRendidos,4) DirecAgua 
,round(B.CostoDirecServCrianza/A.KilosRendidos,4) DirecServCrianza 
,round(((B.CostoDirecPolloBB/A.KilosRendidos) + (B.CostoDirecAlimento/A.KilosRendidos) + (B.CostoDirecMedicina/A.KilosRendidos) + (B.CostoDirecVitamina/A.KilosRendidos) + 
(B.CostoDirecVacuna/A.KilosRendidos) + (B.CostoDirecGas/A.KilosRendidos) + (B.CostoDirecCama/A.KilosRendidos) + (B.CostoDirecAgua/A.KilosRendidos) + (B.CostoDirecServCrianza/A.KilosRendidos)),4) TotalDirectos 
,round(B.CostoIndirecMaOb/A.KilosRendidos,4) IndirecManoDeObra 
,round(B.CostoIndirecSumiPropio/A.KilosRendidos,4) IndirecSuministrosPropios 
,round(B.CostoIndirecSumiRecib/A.KilosRendidos,4) IndirecSuministrosRecib 
,round(B.CostoIndirecElectri/A.KilosRendidos,4) IndirecElectricidad 
,round(B.CostoIndirecRepMan/A.KilosRendidos,4) IndirecReparManten 
,round(B.CostoIndirecPropios/A.KilosRendidos,4) IndirecPropios 
,round(B.CostoIndirecRecibidos/A.KilosRendidos,4) IndirecRecibidos 
,round(B.CostoIndirecAlquileres/A.KilosRendidos,4) IndirecAlquileres 
,round(B.CostoIndirecDeprec/A.KilosRendidos,4) IndirecDepreciacion 
,round(B.CostoIndirecTrans/A.KilosRendidos,4) IndirecTransporte 
,round(B.CostoIndirecServ/A.KilosRendidos,4) IndirecServicios 
,round(((B.CostoIndirecMaob/A.KilosRendidos) + (B.CostoIndirecSumiPropio/A.KilosRendidos) + (B.CostoIndirecSumiRecib/A.KilosRendidos) + (B.CostoIndirecElectri/A.KilosRendidos) 
+ (B.CostoIndirecRepMan/A.KilosRendidos) + (B.CostoIndirecPropios/A.KilosRendidos) + (B.CostoIndirecRecibidos/A.KilosRendidos) + (B.CostoIndirecAlquileres/A.KilosRendidos) 
+ (B.CostoIndirecDeprec/A.KilosRendidos) + (B.CostoIndirecTrans/A.KilosRendidos) + (B.CostoIndirecServ/A.KilosRendidos)),4) TotalIndirectos 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,A.Empresa 
from {database_name_costos_tmp}.Indicadores A 
left join {database_name_costos_tmp}.costo1Temp1 B on A.ComplexEntityNo = B.ComplexEntityNo 
where mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_ft_CostosIndicadores', df_ft_CostosIndicadores.count())
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
    filtered_new_data = df_ft_CostosIndicadores
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
    df_ft_CostosIndicadores.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_MesCicloCostosIndicadores = spark.sql(f"""select distinct mes, 
CONCAT('C',substring(cast(add_months(cast(concat(substring(mes,1,4),'-',substring(mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_CostosIndicadores 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') """)
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloCostosIndicadores"
}
df_MesCicloCostosIndicadores.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloCostosIndicadores")
print('carga temporal MesCicloCostosIndicadores', df_MesCicloCostosIndicadores.count())
df_ft_CostosIndicadoresTemp = spark.sql(f"""select A.* 
from {database_name_costos_gl}.ft_CostosIndicadores A 
left join {database_name_costos_tmp}.MesCicloCostosIndicadores B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null and A.Empresa = 'SAN FERNANDO' 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_CostosIndicadoresTemp"
}
df_ft_CostosIndicadoresTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_CostosIndicadoresTemp")
print('carga temporal ft_CostosIndicadoresTemp', df_ft_CostosIndicadoresTemp.count())
df_ft_CostosIndicadores2 = spark.sql(f"select \
B.Mes \
,B.Categoria \
,B.ComplexEntityNo \
,B.Plantel \
,B.FechaInicioCampana \
,B.FechaInicioCrianza \
,B.Zona \
,B.SubZona \
,B.Proveedor \
,B.TipoProducto \
,B.Administrador \
,B.FechaCierreCampana \
,B.PobInicial \
,B.MortCampo \
,B.MortSistema \
,B.AvesLogradas \
,B.AvesRendidas \
,B.SobranFaltan \
,B.KilosRendidos \
,B.Gas \
,B.GasGalones \
,B.AvesXm2 \
,B.AreaCrianza \
,B.PorcMortCampo \
,B.PorcMortSistema \
,B.ICA \
,B.ICAAjustado \
,B.Edad \
,B.EdadPond \
,B.Peso \
,B.PorcPobMacho \
,B.PobMacho \
,B.GananciaPesoDia \
,B.KgXm2 \
,B.PorcPolloMediano \
,B.PolloMediano \
,B.IEP \
,B.Cama \
,B.CamaM3 \
,B.Agua \
,B.AguaL \
,B.DiasLimpieza \
,B.DiasCrianza \
,B.TotalCampana \
,B.DiasSaca \
,B.PorcPreIni \
,B.PorcIni \
,B.PorcAcab \
,B.PorcTerm \
,B.PorcFin \
,B.PorcConsumoTotal \
,B.PreInicio \
,B.Inicio \
,B.Acabado \
,B.Terminado \
,B.Finalizador \
,B.ConsumoTotal \
,B.CostoKgprod \
,B.CostoAlimKgcons \
,B.CostoBBalojado \
,B.CostoDirecPolloBB \
,B.CostoDirecAlimento \
,B.CostoDirecMedicina \
,B.CostoDirecVitamina \
,B.CostoDirecVacuna \
,B.CostoDirecGas \
,B.CostoDirecCama \
,B.CostoDirecAgua \
,B.CostoDirecServCrianza \
,B.CostoDirecto \
,B.CostoIndirecMaOb \
,B.CostoIndirecSumiPropio \
,B.CostoIndirecSumiRecib \
,B.CostoIndirecElectri \
,B.CostoIndirecRepMan \
,B.CostoIndirecPropios \
,B.CostoIndirecRecibidos \
,B.CostoIndirecAlquileres \
,B.CostoIndirecDeprec \
,B.CostoIndirecTrans \
,B.CostoIndirecServ \
,B.CostoIndirecto \
,B.CostoTotal \
,B.DirecPolloBB \
,B.DirecAlimento \
,B.DirecMedicinas \
,B.DirecVitaminas \
,B.DirecVacunas \
,B.DirecGas \
,B.DirecCama \
,B.DirecAgua \
,B.DirecServCrianza \
,B.TotalDirectos \
,B.IndirecManoDeObra \
,B.IndirecSuministrosPropios \
,B.IndirecSuministrosRecib \
,B.IndirecElectricidad \
,B.IndirecReparManten \
,B.IndirecPropios \
,B.IndirecRecibidos \
,B.IndirecAlquileres \
,B.IndirecDepreciacion \
,B.IndirecTransporte \
,B.IndirecServicios \
,B.TotalIndirectos \
,A.Ciclo \
,B.Empresa \
from {database_name_costos_tmp}.MesCicloCostosIndicadores A \
left join {database_name_costos_tmp}.ft_CostosIndicadoresTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes \
where B.Mes is not null \
except \
select * from {database_name_costos_gl}.ft_CostosIndicadores \
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') and Empresa = 'SAN FERNANDO'")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
}
df_ft_CostosIndicadores2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
print('carga temporal ft_CostosIndicadores2', df_ft_CostosIndicadores2.count())
df_mvProteinJournalTrans1 = spark.sql(f"""select *, 
case when ProductName like '%BENEFICIO%' then 'BENEFICIO' else 'VIVO' end Tipoproducto 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP BRIM - Farm>Plant' 
and date_format(cast(xDate as timestamp),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -3)), 'yyyyMM') 
and date_format(cast(xDate as timestamp),'yyyyMM') <= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemLocationGroupNo = 'PLANT' and ProductNo not in ('142664')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvProteinJournalTrans1"
}
df_mvProteinJournalTrans1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.mvProteinJournalTrans1")
print('carga temporal mvProteinJournalTrans1', df_mvProteinJournalTrans1.count())
df_KilosRendidos = spark.sql(f"""select date_format(cast(xDate as timestamp),'yyyyMM') Mes, TipoProducto, sum(Units) Units 
from {database_name_costos_tmp}.mvProteinJournalTrans1 
group by date_format(cast(xDate as timestamp),'yyyyMM'), TipoProducto order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/KilosRendidos"
}
df_KilosRendidos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.KilosRendidos")
print('carga temporal KilosRendidos', df_KilosRendidos.count())
df_costo = spark.sql(f"select \
A.xDate Fecha \
,date_format(cast(xDate as timestamp),'yyyyMM') Mes \
,case when A.ProductName like '%beneficio%' then 'BENEFICIO' else 'VIVO' end Tipoproducto \
,A.ComplexEntityNo \
,(A.RelativeAmount*-1.0) Cantidad \
,(A.RelativeUnits *-1.0) Unidad \
,A.SystemCostElementNo ElementoContable \
,A.SystemCostElementName NombreElementoContable \
,A.AccountName NombreCuenta \
,A.SystemComplexAccountNo NroCuenta \
from {database_name_costos_si}.si_mvProteinJournalTrans A \
where date_format(cast(xDate as timestamp),'yyyyMM') = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') \
order by A.ComplexEntityNo")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costo"
}
df_costo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costo")
print('carga temporal costo', df_costo.count())
df_ft_CostosEstructura = spark.sql(f"select \
A.Fecha \
,A.Mes \
,A.Tipoproducto \
,A.ComplexEntityNo \
,Cantidad \
,Unidad \
,ElementoContable \
,NombreElementoContable \
,NombreCuenta \
,NroCuenta \
,Units KilosRendidos \
,CONCAT('C',substring(cast(add_months(cast(concat(substring(A.mes,1,4),'-',substring(A.mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo \
,'SAN FERNANDO' Empresa \
from {database_name_costos_tmp}.costo A \
left join {database_name_costos_tmp}.KilosRendidos B on A.Tipoproducto = B.Tipoproducto and A.Mes = B.Mes")
print('carga df_ft_CostosEstructura', df_ft_CostosEstructura.count())
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
    filtered_new_data = df_ft_CostosEstructura
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
    df_ft_CostosEstructura.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
df_MesCicloCostosEstructura = spark.sql(f"select distinct mes, \
CONCAT('C',substring(cast(add_months(cast(concat(substring(mes,1,4),'-',substring(mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo \
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador \
from {database_name_costos_gl}.ft_CostosEstructura \
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) \
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') ")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloCostosEstructura"
}
df_MesCicloCostosEstructura.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloCostosEstructura")
print('carga temporal MesCicloCostosEstructura', df_MesCicloCostosEstructura.count())
df_ft_CostosEstructuraTemp = spark.sql(f"select A.* \
from {database_name_costos_gl}.ft_CostosEstructura A \
left join {database_name_costos_tmp}.MesCicloCostosEstructura B on A.Mes = B.Mes and A.Ciclo = B.Ciclo \
where B.Ciclo is not null and A.Empresa = 'SAN FERNANDO' \
order by 1")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_CostosEstructuraTemp"
}
df_ft_CostosEstructuraTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_CostosEstructuraTemp")
print('carga temporal ft_CostosEstructuraTemp', df_ft_CostosEstructuraTemp.count())
df_ft_CostosEstructura2 = spark.sql(f"select \
B.Fecha \
,B.Mes \
,B.Tipoproducto \
,B.ComplexEntityNo \
,B.Cantidad \
,B.Unidad \
,B.ElementoContable \
,B.NombreElementoContable \
,B.NombreCuenta \
,B.NroCuenta \
,B.KilosRendidos \
,A.Ciclo \
,'SAN FERNANDO' Empresa \
from {database_name_costos_tmp}.MesCicloCostosEstructura A \
left join {database_name_costos_tmp}.ft_CostosEstructuraTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes \
where B.Mes is not null and A.mes =DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM') \
except \
select * from {database_name_costos_gl}.ft_CostosEstructura \
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') and Empresa = 'SAN FERNANDO'")
# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
}
df_ft_CostosEstructura2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
print('carga ft_CostosEstructura2', df_ft_CostosEstructura2.count())
df_IndicadoresEnteros01 = spark.sql(f"""
WITH unpivot_data AS (
    SELECT 
        Mes,
        ComplexEntityNo,
        'PobInicial' AS Indicadores, PobInicial AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'MortCampo' AS Indicadores, MortCampo AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'MortSistema' AS Indicadores, MortSistema AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'AvesLogradas' AS Indicadores, AvesLogradas AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'AvesRendidas' AS Indicadores, AvesRendidas AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'SobranFaltan' AS Indicadores, SobranFaltan AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'PolloMediano' AS Indicadores, PolloMediano AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'DiasLimpieza' AS Indicadores, DiasLimpieza AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'DiasCrianza' AS Indicadores, DiasCrianza AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'TotalCampana' AS Indicadores, TotalCampana AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
    UNION ALL
    SELECT 
        Mes,
        ComplexEntityNo,
        'DiasSaca' AS Indicadores, DiasSaca AS valor FROM {database_name_costos_gl}.ft_CostosIndicadores
)

SELECT * 
FROM unpivot_data
WHERE Mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_IndicadoresEnteros01')
df_IndicadoresEnteros02 = spark.sql(f"""
SELECT 
    Mes,
    ComplexEntityNo,
    col.Indicadores,
    col.valor
FROM {database_name_costos_gl}.ft_CostosIndicadores 
LATERAL VIEW EXPLODE(
    ARRAY(
        NAMED_STRUCT('Indicadores', 'KilosRendidos', 'valor', KilosRendidos),
        NAMED_STRUCT('Indicadores', 'Gas', 'valor', Gas),
        NAMED_STRUCT('Indicadores', 'GasGalones', 'valor', GasGalones),
        NAMED_STRUCT('Indicadores', 'AvesXm2', 'valor', AvesXm2),
        NAMED_STRUCT('Indicadores', 'AreaCrianza', 'valor', AreaCrianza),
        NAMED_STRUCT('Indicadores', 'PorcMortCampo', 'valor', PorcMortCampo),
        NAMED_STRUCT('Indicadores', 'PorcMortSistema', 'valor', PorcMortSistema),
		NAMED_STRUCT('Indicadores', 'ICA', 'valor',ICA),
		NAMED_STRUCT('Indicadores', 'ICAAjustado', 'valor',ICAAjustado),
		NAMED_STRUCT('Indicadores', 'Edad', 'valor',Edad),
		NAMED_STRUCT('Indicadores', 'EdadPond', 'valor',EdadPond),
		NAMED_STRUCT('Indicadores', 'Peso', 'valor',Peso),
		NAMED_STRUCT('Indicadores', 'PorcPobMacho', 'valor',PorcPobMacho),
		NAMED_STRUCT('Indicadores', 'PobMacho', 'valor',PobMacho),
		NAMED_STRUCT('Indicadores', 'GananciaPesoDia', 'valor',GananciaPesoDia),
		NAMED_STRUCT('Indicadores', 'KgXm2', 'valor',KgXm2),
		NAMED_STRUCT('Indicadores', 'PorcPolloMediano', 'valor',PorcPolloMediano),
		NAMED_STRUCT('Indicadores', 'IEP', 'valor',IEP),
		NAMED_STRUCT('Indicadores', 'Cama', 'valor',Cama),
		NAMED_STRUCT('Indicadores', 'CamaM3', 'valor',CamaM3),
		NAMED_STRUCT('Indicadores', 'Agua', 'valor',Agua),
		NAMED_STRUCT('Indicadores', 'AguaL', 'valor',AguaL),
		NAMED_STRUCT('Indicadores', 'PorcPreIni', 'valor',PorcPreIni),
		NAMED_STRUCT('Indicadores', 'PorcIni', 'valor',PorcIni),
		NAMED_STRUCT('Indicadores', 'PorcAcab', 'valor',PorcAcab),
		NAMED_STRUCT('Indicadores', 'PorcTerm', 'valor',PorcTerm),
		NAMED_STRUCT('Indicadores', 'PorcFin', 'valor',PorcFin),
		NAMED_STRUCT('Indicadores', 'PorcConsumoTotal', 'valor',PorcConsumoTotal),
		NAMED_STRUCT('Indicadores', 'PreInicio', 'valor',PreInicio),
		NAMED_STRUCT('Indicadores', 'Inicio', 'valor',Inicio),
		NAMED_STRUCT('Indicadores', 'Acabado', 'valor',Acabado),
		NAMED_STRUCT('Indicadores', 'terminado', 'valor',terminado),
		NAMED_STRUCT('Indicadores', 'Finalizador', 'valor',Finalizador),
		NAMED_STRUCT('Indicadores', 'ConsumoTotal', 'valor',ConsumoTotal),
		NAMED_STRUCT('Indicadores', 'CostoKgprod', 'valor',CostoKgprod),
		NAMED_STRUCT('Indicadores', 'CostoAlimKgcons', 'valor',CostoAlimKgcons),
		NAMED_STRUCT('Indicadores', 'CostoBBalojado', 'valor',CostoBBalojado),
		NAMED_STRUCT('Indicadores', 'CostoDirecPolloBB', 'valor',CostoDirecPolloBB),
		NAMED_STRUCT('Indicadores', 'CostoDirecAlimento', 'valor',CostoDirecAlimento),
		NAMED_STRUCT('Indicadores', 'CostoDirecMedicina', 'valor',CostoDirecMedicina),
		NAMED_STRUCT('Indicadores', 'CostoDirecVitamina', 'valor',CostoDirecVitamina),
		NAMED_STRUCT('Indicadores', 'CostoDirecVacuna', 'valor',CostoDirecVacuna),
		NAMED_STRUCT('Indicadores', 'CostoDirecGas', 'valor',CostoDirecGas),
		NAMED_STRUCT('Indicadores', 'CostoDirecCama', 'valor',CostoDirecCama),
		NAMED_STRUCT('Indicadores', 'CostoDirecAgua', 'valor',CostoDirecAgua),
		NAMED_STRUCT('Indicadores', 'CostoDirecServCrianza', 'valor',CostoDirecServCrianza),
		NAMED_STRUCT('Indicadores', 'CostoDirecto', 'valor',CostoDirecto),
		NAMED_STRUCT('Indicadores', 'CostoIndirecMaOb', 'valor',CostoIndirecMaOb),
		NAMED_STRUCT('Indicadores', 'CostoIndirecSumiPropio', 'valor',CostoIndirecSumiPropio),
		NAMED_STRUCT('Indicadores', 'CostoIndirecSumiRecib', 'valor',CostoIndirecSumiRecib),
		NAMED_STRUCT('Indicadores', 'CostoIndirecElectri', 'valor',CostoIndirecElectri),
		NAMED_STRUCT('Indicadores', 'CostoIndirecRepMan', 'valor',CostoIndirecRepMan),
		NAMED_STRUCT('Indicadores', 'CostoIndirecPropios', 'valor',CostoIndirecPropios),
		NAMED_STRUCT('Indicadores', 'CostoIndirecRecibidos', 'valor',CostoIndirecRecibidos),
		NAMED_STRUCT('Indicadores', 'CostoIndirecAlquileres', 'valor',CostoIndirecAlquileres),
		NAMED_STRUCT('Indicadores', 'CostoIndirecdeprec', 'valor',CostoIndirecdeprec),
		NAMED_STRUCT('Indicadores', 'CostoIndirecTrans', 'valor',CostoIndirecTrans),
		NAMED_STRUCT('Indicadores', 'CostoIndirecServ', 'valor',CostoIndirecServ),
		NAMED_STRUCT('Indicadores', 'CostoIndirecto', 'valor',CostoIndirecto),
		NAMED_STRUCT('Indicadores', 'CostoTotal', 'valor',CostoTotal),
		NAMED_STRUCT('Indicadores', 'DirecPolloBB', 'valor',DirecPolloBB),
		NAMED_STRUCT('Indicadores', 'DirecAlimento', 'valor',DirecAlimento),
		NAMED_STRUCT('Indicadores', 'DirecMedicinas', 'valor',DirecMedicinas),
		NAMED_STRUCT('Indicadores', 'DirecVitaminas', 'valor',DirecVitaminas),
		NAMED_STRUCT('Indicadores', 'DirecVacunas', 'valor',DirecVacunas),
		NAMED_STRUCT('Indicadores', 'DirecGas', 'valor',DirecGas),
		NAMED_STRUCT('Indicadores', 'DirecCama', 'valor',DirecCama),
		NAMED_STRUCT('Indicadores', 'DirecAgua', 'valor',DirecAgua),
		NAMED_STRUCT('Indicadores', 'DirecServCrianza', 'valor',DirecServCrianza),
		NAMED_STRUCT('Indicadores', 'TotalDirectos', 'valor',TotalDirectos),
		NAMED_STRUCT('Indicadores', 'IndirecManoDeObra', 'valor',IndirecManoDeObra),
		NAMED_STRUCT('Indicadores', 'IndirecSuministrosPropios', 'valor',IndirecSuministrosPropios),
		NAMED_STRUCT('Indicadores', 'IndirecSuministrosRecib', 'valor',IndirecSuministrosRecib),
		NAMED_STRUCT('Indicadores', 'IndirecElectricidad', 'valor',IndirecElectricidad),
		NAMED_STRUCT('Indicadores', 'IndirecReparManten', 'valor',IndirecReparManten),
		NAMED_STRUCT('Indicadores', 'IndirecPropios', 'valor',IndirecPropios),
		NAMED_STRUCT('Indicadores', 'IndirecRecibidos', 'valor',IndirecRecibidos),
		NAMED_STRUCT('Indicadores', 'IndirecAlquileres', 'valor',IndirecAlquileres),
		NAMED_STRUCT('Indicadores', 'IndirecDepreciacion', 'valor',IndirecDepreciacion),
		NAMED_STRUCT('Indicadores', 'TotalIndirectos', 'valor',TotalIndirectos)
    )
) col
WHERE Mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_IndicadoresEnteros02')
df_IndicadoresEnteros03 = df_IndicadoresEnteros01.union(df_IndicadoresEnteros02)
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/IndicadoresEnteros03"
}
df_IndicadoresEnteros03.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.IndicadoresEnteros03")
print('carga temporal IndicadoresEnteros03', df_IndicadoresEnteros03.count())
df_ft_CostosIndicadoresV = spark.sql(f"select \
distinct \
A.Mes \
,A.Categoria \
,A.ComplexEntityNo \
,A.Plantel \
,A.FechaInicioCampana \
,A.FechaInicioCrianza \
,A.Zona \
,A.SubZona \
,A.Proveedor \
,A.TipoProducto \
,A.Administrador \
,A.FechaCierreCampana \
,B.Indicadores \
,cast(B.valor as decimal(20,5)) valor \
,CONCAT('C',substring(cast(add_months(cast(concat(substring(A.mes,1,4),'-',substring(A.mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo \
,A.Empresa \
from {database_name_costos_gl}.ft_CostosIndicadores A \
left join {database_name_costos_tmp}.IndicadoresEnteros03 B on A.ComplexEntityNo = B.ComplexEntityNo and A.Mes = B.Mes \
where A.mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')")
print('carga temporal df_ft_CostosIndicadoresV', df_ft_CostosIndicadoresV.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=1)
fecha_str = fecha_menos.strftime("%Y%m")
try:
    df_existentes = spark.read.format("parquet").load(path_target3)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name3} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name3}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target3)
    data_after_delete = existing_data.filter(~(col("Mes")== fecha_str))
    filtered_new_data = df_ft_CostosIndicadoresV
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name3}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name3}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name3}Temporal")
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
            
    print(f"agrega registros nuevos a la tabla {table_name3} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name3} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name3}Temporal')
    print(f"Tabla {table_name3}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
    }
    df_ft_CostosIndicadoresV.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
df_MesCiclo = spark.sql(f"select distinct mes,CONCAT('C',substring(cast(add_months(cast(concat(substring(mes,1,4),'-',substring(mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo \
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador \
from {database_name_costos_gl}.ft_CostosIndicadoresV \
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) \
and mes >=  DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCiclo"
}
df_MesCiclo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCiclo")
print('carga temporal MesCiclo', df_MesCiclo.count())
df_ft_CostosIndicadoresVTemp = spark.sql(f"select A.* \
from {database_name_costos_gl}.ft_CostosIndicadoresV A \
left join {database_name_costos_tmp}.MesCiclo B on A.Mes = B.Mes and A.Ciclo = B.Ciclo \
where B.Ciclo is not null and A.Empresa = 'SAN FERNANDO' \
order by 1")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_CostosIndicadoresVTemp"
}
df_ft_CostosIndicadoresVTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_CostosIndicadoresVTemp")
print('carga temporal ft_CostosIndicadoresVTemp',df_ft_CostosIndicadoresVTemp.count())
df_ft_CostosIndicadoresV2 = spark.sql(f"select \
B.Mes \
,B.Categoria \
,B.ComplexEntityNo \
,B.Plantel \
,B.FechaInicioCampana \
,B.FechaInicioCrianza \
,B.Zona \
,B.SubZona \
,B.Proveedor \
,B.TipoProducto \
,B.Administrador \
,B.FechaCierreCampana \
,B.Indicadores \
,B.valor \
,A.Ciclo \
,B.Empresa \
from {database_name_costos_tmp}.MesCiclo A \
left join {database_name_costos_tmp}.ft_CostosIndicadoresVTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes \
where B.Mes is not null and A.mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')\
except \
select * from {database_name_costos_gl}.ft_CostosIndicadoresV \
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM') and Empresa = 'SAN FERNANDO'")
# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name3}"
}
df_ft_CostosIndicadoresV2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name3}")
print('carga temporal df_ft_CostosIndicadoresV2',df_ft_CostosIndicadoresV2.count())
df_ft_CostosEstructura_Actual = spark.sql(f"select * from {database_name_costos_gl}.ft_CostosEstructura where Mes >= '202401'")

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_CostosEstructura_Actual"
}
df_ft_CostosEstructura_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_CostosEstructura_Actual")
print('carga temporal ft_CostosEstructura_Actual',df_ft_CostosEstructura_Actual.count())
df_ft_CostosIndicadores_Actual = spark.sql(f"select * from {database_name_costos_gl}.ft_CostosIndicadores where Mes >= '202401'")

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_CostosIndicadores_Actual"
}
df_ft_CostosIndicadores_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_CostosIndicadores_Actual")
print('carga temporal ft_CostosIndicadores_Actual', df_ft_CostosIndicadores_Actual.count())
df_ft_CostosIndicadoresV_Actual = spark.sql(f"select * from {database_name_costos_gl}.ft_CostosIndicadoresV where Mes >= '202401'")

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_CostosIndicadoresV_Actual"
}
df_ft_CostosIndicadoresV_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_CostosIndicadoresV_Actual")
print('carga temporal ft_CostosIndicadoresV_Actual', df_ft_CostosIndicadoresV_Actual.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()