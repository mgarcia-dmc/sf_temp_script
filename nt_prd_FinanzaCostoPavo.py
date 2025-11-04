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
JOB_NAME = "nt_prd_FinanzaCostoPavo"

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

table_name1="ft_Pavos_CostosIndicadores"
table_name2="ft_Pavos_CostosEstructura"
file_name_target1 = f"{bucket_name_prdmtech}{table_name1}/"
file_name_target2 = f"{bucket_name_prdmtech}{table_name2}/"

path_target1 = f"s3://{bucket_name_target}/{file_name_target1}"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
#database_name="default"
print('cargando ruta')
df_Indicadores_Pavos = spark.sql(f"""
select 
substring(A.FechaCierre,1,6) mes 
,A.categoria 
,A.ComplexEntityNo 
,B.cplantel Plantel 
,A.FechaInicioGranja FechaInicioCampana 
,A.FechaCrianza FechaInicioCrianza 
,C.nzona Zona 
,LSZ.nsubzona SubZona 
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
,A.EdadGranja * A.AvesRendidas EdadPond 
,A.PesoProm Peso 
,ROUND(A.PorcMacho,1) PorcPobMacho 
,ROUND((A.PorcMacho * A.PobInicial)/100,0) PobMacho 
,A.GananciaDiaVenta GananciaPesoDia 
,A.KgXm2 
,ROUND((a.AvesRendidasMediano/(A.PobInicial*1.0))*100,2) PorcPolloMediano 
,A.AvesRendidasMediano PolloMediano 
,A.IEP 
,A.Cama 
,(A.Cama * A.PobInicial)/1000 CamaM3 
,A.Agua 
,ROUND(A.Agua * A.KilosRendidos,0) AguaL 
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
from {database_name_gl}.ft_consolidado_Lote A 
left join {database_name_gl}.lk_plantel B on A.pk_plantel = B.pk_plantel 
left join {database_name_gl}.lk_zona C on A.pk_zona = C.pk_zona 
left join {database_name_gl}.lk_subzona LSZ on A.pk_subzona = LSZ.pk_subzona 
left join {database_name_gl}.lk_tipoproducto D on A.pk_tipoproducto = D.pk_tipoproducto 
left join {database_name_gl}.lk_administrador E on A.pk_administrador = E.pk_administrador 
left join {database_name_gl}.lk_proveedor F on A.pk_proveedor = F.pk_proveedor 
where substring(A.FechaCierre,1,6) = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and A.FlagArtAtipico = 2 
and A.pk_empresa = 1 and A.pk_division = 2 
and A.pk_estado in (3,4) 
order by A.FechaCierre,A.ComplexEntityNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Indicadores_Pavos"
}
df_Indicadores_Pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.Indicadores_Pavos")
print('carga temporal Indicadores_Pavos', df_Indicadores_Pavos.count())
df_mvProteinJournalTrans_Pavos = spark.sql(f"""select * 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP TIM - Farm>Plant' 
and date_format(cast(xDate as timestamp),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -3)), 'yyyyMM') 
and date_format(cast(xDate as timestamp),'yyyyMM') <= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemStageNo = 'GROW' 
and SystemElementUserNo = 'OUT' 
and SystemLocationGroupNo = 'TURKEY'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvProteinJournalTrans_Pavos"
}
df_mvProteinJournalTrans_Pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.mvProteinJournalTrans_Pavos")
print('carga temporal mvProteinJournalTrans_Pavos', df_mvProteinJournalTrans_Pavos.count())
df_PivotCosto_Pavos = spark.sql(f"""
WITH pivot_data AS (
    SELECT 
        ComplexEntityNo,
        SystemCostElementNo,
        SUM(RelativeAmount) AS TotalAmount
    FROM {database_name_costos_tmp}.mvProteinJournalTrans_Pavos
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
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PivotCosto_Pavos"
}
df_PivotCosto_Pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.PivotCosto_Pavos")
print('carga temporal PivotCosto_Pavos', df_PivotCosto_Pavos.count())
df_costo1_pavos = spark.sql(f"""select 
A.ComplexEntityNo 
,sum(COALESCE(RelativeAmount,0)*-1.0)CostoTotal 
,max(COALESCE(BRLRMALE,0)*-1.0) + max(COALESCE(BRLRFEMALE,0)*-1.0) CostoDirecPolloBB 
,max(COALESCE(ING,0)*-1.0) CostoDirecAlimento 
,max(COALESCE(MED,0)*-1.0) CostoDirecMedicina 
,max(COALESCE(VIT,0)*-1.0) CostoDirecVitamina 
,max(COALESCE(VACC,0)*-1.0) CostoDirecVacuna 
,max(COALESCE(GAS,0)*-1.0) CostoDirecGas 
,max(COALESCE(CAMA,0)*-1.0) CostoDirecCama 
,max(COALESCE(AGUA,0)*-1.0) CostoDirecAgua 
,max(COALESCE(GPAYBAS,0)*-1.0) CostoDirecServCrianza 
,max(COALESCE(MAOB,0)*-1.0) CostoIndirecMaOb 
,max(COALESCE(SUMI,0)*-1.0) CostoIndirecSumiPropio 
,max(COALESCE(SUMI1,0)*-1.0) CostoIndirecSumiRecib 
,max(COALESCE(ELEC,0)*-1.0) CostoIndirecElectri 
,max(COALESCE(REPM,0)*-1.0) CostoIndirecRepMan 
,max(COALESCE(INDR,0)*-1.0) CostoIndirecPropios 
,max(COALESCE(INDREC,0)*-1.0) CostoIndirecRecibidos 
,max(COALESCE(ALQU,0)*-1.0) CostoIndirecAlquileres 
,max(COALESCE(DEPA,0)*-1.0) CostoIndirecDeprec 
,max(COALESCE(TRANS,0)*-1.0) CostoIndirecTrans 
,max(COALESCE(SERV,0)*-1.0) CostoIndirecServ 
from {database_name_costos_tmp}.mvProteinJournalTrans_Pavos A 
left join {database_name_costos_tmp}.PivotCosto_Pavos B on A.ComplexEntityNo = B.ComplexEntityNo 
group by A.ComplexEntityNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costo1_pavos"
}
df_costo1_pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costo1_pavos")
print('carga temporal costo1_pavos', df_costo1_pavos.count())
df_ft_Pavos_CostosIndicadores = spark.sql(f"""select 
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
,A.KilosRendidos 
,A.Gas 
,A.GasGalones 
,A.AvesXm2 
,a.AreaCrianza 
,A.PorcMortCampo 
,A.PorcMortSistema 
,A.ICA 
,A.ICAAjustado 
,A.Edad 
,A.EdadPond 
,A.Peso 
,A.PorcPobMacho 
,A.PobMacho 
,A.GananciaPesoDia 
,A.KgXm2 
,A.PorcPolloMediano 
,A.PolloMediano 
,A.IEP 
,A.Cama 
,A.CamaM3 
,A.Agua 
,A.AguaL 
,A.DiasLimpieza 
,a.DiasCrianza 
,A.TotalCampana 
,A.DiasSaca 
,A.PorcPreIni 
,A.PorcIni 
,A.PorcAcab 
,A.PorcTerm 
,A.PorcFin 
,A.PorcConsumoTotal 
,A.PreInicio 
,A.Inicio 
,A.Acabado 
,A.Terminado 
,A.Finalizador 
,A.ConsumoTotal 
,(B.CostoTotal / A.KilosRendidos) CostoKgprod 
,(B.CostoDirecAlimento/A.ConsumoTotal) CostoAlimKgcons 
,(B.CostoDirecPolloBB/A.PobInicial) CostoBBalojado 
,B.CostoDirecPolloBB 
,B.CostoDirecAlimento 
,B.CostoDirecMedicina 
,B.CostoDirecVitamina 
,B.CostoDirecVacuna 
,B.CostoDirecGas 
,B.CostoDirecCama 
,B.CostoDirecAgua 
,B.CostoDirecServCrianza 
,B.CostoDirecPolloBB + B.CostoDirecAlimento + B.CostoDirecMedicina + B.CostoDirecVitamina + B.CostoDirecVacuna + B.CostoDirecGas + B.CostoDirecCama + B.CostoDirecAgua + B.CostoDirecServCrianza CostoDirecto 
,B.CostoIndirecMaOb 
,B.CostoIndirecSumiPropio 
,B.CostoIndirecSumiRecib 
,B.CostoIndirecElectri 
,B.CostoIndirecRepMan 
,B.CostoIndirecPropios 
,B.CostoIndirecRecibidos 
,B.CostoIndirecAlquileres 
,B.CostoIndirecDeprec 
,B.CostoIndirecTrans 
,B.CostoIndirecServ 
,B.CostoIndirecMaOb + B.CostoIndirecSumiPropio + B.CostoIndirecSumiRecib + B.CostoIndirecElectri + B.CostoIndirecRepMan + B.CostoIndirecPropios + B.CostoIndirecRecibidos + 
B.CostoIndirecAlquileres + B.CostoIndirecDeprec + B.CostoIndirecTrans + B.CostoIndirecServ CostoIndirecto 
,B.CostoTotal 
,(B.CostoDirecPolloBB/A.KilosRendidos) DirecPolloBB 
,(B.CostoDirecAlimento/A.KilosRendidos) DirecAlimento 
,(B.CostoDirecMedicina/A.KilosRendidos) DirecMedicinas 
,(B.CostoDirecVitamina/A.KilosRendidos) DirecVitaminas 
,(B.CostoDirecVacuna/A.KilosRendidos) DirecVacunas 
,(B.CostoDirecGas/A.KilosRendidos) DirecGas 
,(B.CostoDirecCama/A.KilosRendidos) DirecCama 
,(B.CostoDirecAgua/A.KilosRendidos) DirecAgua 
,(B.CostoDirecServCrianza/A.KilosRendidos) DirecServCrianza 
,((B.CostoDirecPolloBB/A.KilosRendidos) + (B.CostoDirecAlimento/A.KilosRendidos) + (B.CostoDirecMedicina/A.KilosRendidos) + (B.CostoDirecVitamina/A.KilosRendidos) + 
(B.CostoDirecVacuna/A.KilosRendidos) + (B.CostoDirecGas/A.KilosRendidos) + (B.CostoDirecCama/A.KilosRendidos) + (B.CostoDirecAgua/A.KilosRendidos) + (B.CostoDirecServCrianza/A.KilosRendidos)) TotalDirectos 
,(B.CostoIndirecMaOb/A.KilosRendidos) IndirecManoDeObra 
,(B.CostoIndirecSumiPropio/A.KilosRendidos) IndirecSuministrosPropios 
,(B.CostoIndirecSumiRecib/A.KilosRendidos) IndirecSuministrosRecib 
,(B.CostoIndirecElectri/A.KilosRendidos) IndirecElectricidad 
,(B.CostoIndirecRepMan/A.KilosRendidos) IndirecReparManten 
,(B.CostoIndirecPropios/A.KilosRendidos) IndirecPropios 
,(B.CostoIndirecRecibidos/A.KilosRendidos) IndirecRecibidos 
,(B.CostoIndirecAlquileres/A.KilosRendidos) IndirecAlquileres 
,(B.CostoIndirecDeprec/A.KilosRendidos) IndirecDepreciacion 
,(B.CostoIndirecTrans/A.KilosRendidos) IndirecTransporte 
,(B.CostoIndirecServ/A.KilosRendidos) IndirecServicios 
,(B.CostoIndirecMaob/A.KilosRendidos) + (B.CostoIndirecSumiPropio/A.KilosRendidos) + (B.CostoIndirecSumiRecib/A.KilosRendidos) + (B.CostoIndirecElectri/A.KilosRendidos) + (B.CostoIndirecRepMan/A.KilosRendidos) + 
(B.CostoIndirecPropios/A.KilosRendidos) + (B.CostoIndirecRecibidos/A.KilosRendidos) + (B.CostoIndirecAlquileres/A.KilosRendidos) + (B.CostoIndirecDeprec/A.KilosRendidos) + (B.CostoIndirecTrans/A.KilosRendidos) + 
(B.CostoIndirecServ/A.KilosRendidos) TotalIndirectos 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(A.Mes,1,4),'-',substring(A.Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
from {database_name_costos_tmp}.Indicadores_Pavos A 
left join {database_name_costos_tmp}.costo1_pavos B on A.ComplexEntityNo = B.ComplexEntityNo 
where mes = DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
""")
print('carga temporal df_ft_Pavos_CostosIndicadores', df_ft_Pavos_CostosIndicadores.count())
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
    filtered_new_data = df_ft_Pavos_CostosIndicadores
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
    df_ft_Pavos_CostosIndicadores.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
df_MesCicloPavos_CostosIndicadores = spark.sql(f"""select distinct mes, 
CONCAT('C',substring(cast(add_months(cast(concat(substring(mes,1,4),'-',substring(mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_Pavos_CostosIndicadores 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloPavos_CostosIndicadores"
}
df_MesCicloPavos_CostosIndicadores.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloPavos_CostosIndicadores")
print('carga temporal MesCicloPavos_CostosIndicadores', df_MesCicloPavos_CostosIndicadores.count())
df_ft_Pavos_CostosIndicadoresTemp = spark.sql(f"""select A.* 
from {database_name_costos_gl}.ft_Pavos_CostosIndicadores A 
left join {database_name_costos_tmp}.MesCicloPavos_CostosIndicadores B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Pavos_CostosIndicadoresTemp"
}
df_ft_Pavos_CostosIndicadoresTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_Pavos_CostosIndicadoresTemp")
print('carga temporal ft_Pavos_CostosIndicadoresTemp', df_ft_Pavos_CostosIndicadoresTemp.count())
df_ft_Pavos_CostosIndicadores2 = spark.sql(f"""select 
B.Mes 
,B.Categoria 
,B.ComplexEntityNo 
,B.Plantel 
,B.FechaInicioCampana 
,B.FechaInicioCrianza 
,B.Zona 
,B.SubZona 
,B.Proveedor 
,B.TipoProducto 
,B.Administrador 
,B.FechaCierreCampana 
,B.PobInicial 
,B.MortCampo 
,B.MortSistema 
,B.AvesLogradas 
,B.AvesRendidas 
,B.SobranFaltan 
,B.KilosRendidos 
,B.Gas 
,B.GasGalones 
,B.AvesXm2 
,B.AreaCrianza 
,B.PorcMortCampo 
,B.PorcMortSistema 
,B.ICA 
,B.ICAAjustado 
,B.Edad 
,B.EdadPond 
,B.Peso 
,B.PorcPobMacho 
,B.PobMacho 
,B.GananciaPesoDia 
,B.KgXm2 
,B.PorcPolloMediano 
,B.PolloMediano 
,B.IEP 
,B.Cama 
,B.CamaM3 
,B.Agua 
,B.AguaL 
,B.DiasLimpieza 
,B.DiasCrianza 
,B.TotalCampana 
,B.DiasSaca 
,B.PorcPreIni 
,B.PorcIni 
,B.PorcAcab 
,B.PorcTerm 
,B.PorcFin 
,B.PorcConsumoTotal 
,B.PreInicio 
,B.Inicio 
,B.Acabado 
,B.Terminado 
,B.Finalizador 
,B.ConsumoTotal 
,B.CostoKgprod 
,B.CostoAlimKgcons 
,B.CostoBBalojado 
,B.CostoDirecPolloBB 
,B.CostoDirecAlimento 
,B.CostoDirecMedicina 
,B.CostoDirecVitamina 
,B.CostoDirecVacuna 
,B.CostoDirecGas 
,B.CostoDirecCama 
,B.CostoDirecAgua 
,B.CostoDirecServCrianza 
,B.CostoDirecto 
,B.CostoIndirecMaOb 
,B.CostoIndirecSumiPropio 
,B.CostoIndirecSumiRecib 
,B.CostoIndirecElectri 
,B.CostoIndirecRepMan 
,B.CostoIndirecPropios 
,B.CostoIndirecRecibidos 
,B.CostoIndirecAlquileres 
,B.CostoIndirecDeprec 
,B.CostoIndirecTrans 
,B.CostoIndirecServ 
,B.CostoIndirecto 
,B.CostoTotal 
,B.DirecPolloBB 
,B.DirecAlimento 
,B.DirecMedicinas 
,B.DirecVitaminas 
,B.DirecVacunas 
,B.DirecGas 
,B.DirecCama 
,B.DirecAgua 
,B.DirecServCrianza 
,B.TotalDirectos 
,B.IndirecManoDeObra 
,B.IndirecSuministrosPropios 
,B.IndirecSuministrosRecib 
,B.IndirecElectricidad 
,B.IndirecReparManten 
,B.IndirecPropios 
,B.IndirecRecibidos 
,B.IndirecAlquileres 
,B.IndirecDepreciacion 
,B.IndirecTransporte 
,B.IndirecServicios 
,B.TotalIndirectos 
,A.Ciclo 
from {database_name_costos_tmp}.MesCicloPavos_CostosIndicadores A 
left join {database_name_costos_tmp}.ft_Pavos_CostosIndicadoresTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null 
except 
select * from {database_name_costos_gl}.ft_Pavos_CostosIndicadores 
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name1}"
}
df_ft_Pavos_CostosIndicadores2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name1}")
print('carga temporal df_ft_Pavos_CostosIndicadores2', df_ft_Pavos_CostosIndicadores2.count())
df_mvProteinJournalTrans1_pavos = spark.sql(f"""
select *, case when ProductName like '%beneficio%' then 'BENEFICIO' else 'VIVO' end Tipoproducto 
from {database_name_costos_si}.si_mvproteinjournaltrans 
where SourceCode = 'EOP TIM - Farm>Plant' 
and date_format(cast(xDate as timestamp),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -3)), 'yyyyMM') 
and date_format(cast(xDate as timestamp),'yyyyMM') <= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
and SystemLocationGroupNo = 'PLANT'
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvProteinJournalTrans1_pavos"
}
df_mvProteinJournalTrans1_pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.mvProteinJournalTrans1_pavos")
print('carga temporal mvProteinJournalTrans1_pavos', df_mvProteinJournalTrans1_pavos.count())
df_KilosRendidos_pavos = spark.sql(f"""
select date_format(cast(xDate as timestamp),'yyyyMM') Mes, TipoProducto, sum(Units) Units 
from {database_name_costos_tmp}.mvProteinJournalTrans1_pavos 
group by date_format(cast(xDate as timestamp),'yyyyMM'),TipoProducto 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/KilosRendidos_pavos"
}
df_KilosRendidos_pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.KilosRendidos_pavos")
print('carga temporal KilosRendidos_pavos', df_KilosRendidos_pavos.count())
df_costo_pavos = spark.sql(f"""select 
A.xDate Fecha 
,date_format(cast(xDate as timestamp),'yyyyMM') Mes 
,case when A.ProductName like '%beneficio%' then 'BENEFICIO' 
else 'VIVO' end Tipoproducto 
,A.ComplexEntityNo 
,(A.RelativeAmount*-1.0) Cantidad 
,(A.RelativeUnits *-1.0) Unidad 
,A.SystemCostElementNo ElementoContable 
,A.SystemCostElementName NombreElementoContable 
,A.AccountName NombreCuenta 
,A.SystemComplexAccountNo NroCuenta 
from {database_name_costos_tmp}.mvProteinJournalTrans_Pavos A 
where date_format(cast(xDate as timestamp),'yyyyMM') >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -1)), 'yyyyMM')
order by A.ComplexEntityNo""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/costo_pavos"
}
df_costo_pavos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.costo_pavos")
print('carga temporal costo_pavos', df_costo_pavos.count())
df_ft_Pavos_CostosEstructura = spark.sql(f"""select 
A.Fecha 
,A.Mes 
,A.Tipoproducto 
,A.ComplexEntityNo 
,Cantidad 
,Unidad 
,ElementoContable 
,NombreElementoContable 
,NombreCuenta 
,NroCuenta 
,Units KilosRendidos 
,CONCAT('C',substring(cast(add_months(cast(concat(substring(A.Mes,1,4),'-',substring(A.Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
from {database_name_costos_tmp}.costo_pavos A 
left join {database_name_costos_tmp}.KilosRendidos_pavos B on A.Tipoproducto = B.Tipoproducto and A.Mes = B.Mes
""")
print('carga temporal df_ft_Pavos_CostosEstructura',df_ft_Pavos_CostosEstructura.count())
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
    filtered_new_data = df_ft_Pavos_CostosEstructura
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
    df_ft_Pavos_CostosEstructura.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
df_MesCicloPavos_CostosEstructura = spark.sql(f"""
select distinct mes, 
CONCAT('C',substring(cast(add_months(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date),1) as varchar(10)),6,2)) Ciclo 
,substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) Indicador 
from {database_name_costos_gl}.ft_Pavos_CostosEstructura 
where substring(mes,5,2) = substring(cast(cast(concat(substring(Mes,1,4),'-',substring(Mes,5,2),'-','01') as date) as varchar(10)),6,2) 
and mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MesCicloPavos_CostosEstructura"
}
df_MesCicloPavos_CostosEstructura.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.MesCicloPavos_CostosEstructura")
print('carga temporal MesCicloPavos_CostosEstructura', df_MesCicloPavos_CostosEstructura.count())
df_ft_Pavos_CostosEstructuraTemp = spark.sql(f"""select A.* 
from {database_name_costos_gl}.ft_Pavos_CostosEstructura A 
left join {database_name_costos_tmp}.MesCicloPavos_CostosEstructura B on A.Mes = B.Mes and A.Ciclo = B.Ciclo 
where B.Ciclo is not null 
order by 1""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Pavos_CostosEstructuraTemp"
}
df_ft_Pavos_CostosEstructuraTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ft_Pavos_CostosEstructuraTemp")
print('carga temporal ft_Pavos_CostosEstructuraTemp', df_ft_Pavos_CostosEstructuraTemp.count())
df_ft_Pavos_CostosEstructura2 = spark.sql(f"""select 
 B.Fecha 
,B.Mes 
,B.Tipoproducto 
,B.ComplexEntityNo 
,B.Cantidad 
,B.Unidad 
,B.ElementoContable 
,B.NombreElementoContable 
,B.NombreCuenta 
,B.NroCuenta 
,B.KilosRendidos 
,A.Ciclo 
from {database_name_costos_tmp}.MesCicloPavos_CostosEstructura A 
left join {database_name_costos_tmp}.ft_Pavos_CostosEstructuraTemp B on DATE_FORMAT(LAST_DAY(ADD_MONTHS(TO_DATE(CONCAT(A.Mes, '01'), 'yyyyMMdd'),1)),'yyyyMM') > B.Mes 
where B.Mes is not null 
except 
select * from {database_name_costos_gl}.ft_Pavos_CostosEstructura 
where mes >= DATE_FORMAT(LAST_DAY(ADD_MONTHS(CURRENT_DATE(), -12)), 'yyyyMM')
""") 
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name2}"
}
df_ft_Pavos_CostosEstructura2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name2}")
print('carga temporal df_ft_Pavos_CostosEstructura2', df_ft_Pavos_CostosEstructura2.count())
df_ft_Pavos_CostosIndicadores_Actual = spark.sql(f"""select * from {database_name_costos_gl}.ft_Pavos_CostosIndicadores where Mes >= '202401' """)
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Pavos_CostosIndicadores_Actual"
}
df_ft_Pavos_CostosIndicadores_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Pavos_CostosIndicadores_Actual")
print('carga ft_Pavos_CostosIndicadores_Actual', df_ft_Pavos_CostosIndicadores_Actual.count())
df_ft_Pavos_CostosEstructura_Actual = spark.sql(f"""select * from {database_name_costos_gl}.ft_Pavos_CostosEstructura where Mes >= '202401' """)
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Pavos_CostosEstructura_Actual"
}
df_ft_Pavos_CostosEstructura_Actual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_Pavos_CostosEstructura_Actual")
print('carga ft_Pavos_CostosEstructura_Actual', df_ft_Pavos_CostosEstructura_Actual.count())
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()