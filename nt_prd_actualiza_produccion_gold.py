# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, trunc, add_months, date_format,date_add,col
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_actualiza_produccion_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
### Parámetros de entrada global
##database_name = "default"
##
##bucket_name_target = "ue1stgtestas3dtl005-gold"
##bucket_name_source = "ue1stgtestas3dtl005-silver"
##bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
##
##print('cargando rutas')

# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")

amb = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
#database_name_br  = ssm.get_parameter(Name='p_db_prd_sf_pec_br', WithDecryption=True)['Parameter']['Value']
database_name_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']
database_name_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
database_name_tmp  = ssm.get_parameter(Name='p_db_prd_sf_pec_tmp', WithDecryption=True)['Parameter']['Value']
bucket_name_target = f"ue1stg{amb}as3dtl005-gold"
bucket_name_source = f"ue1stg{amb}as3dtl005-silver"
bucket_name_prdmtech = f"UE1STG{amb.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
print('cargando ruta')
##_OrdenarFechas
df_OrdenarFechas1= spark.sql(f"""SELECT ComplexEntityNo,ProteinEntitiesIRN,FirstDatePlaced,LastDateSold,GRN,FarmNo,EntityNo,HouseNo,PenNo, 
                                DENSE_RANK() OVER (PARTITION BY FarmNo,HouseNo ORDER BY FarmNo,EntityNo ASC) AS fila 
                                FROM {database_name_si}.si_mvbrimentities 
                                WHERE GRN = 'P' and FarmType = 1 and SpeciesType = 1""")

df_OrdenarFechas2= spark.sql(f"""SELECT ComplexEntityNo,ProteinEntitiesIRN,FirstDatePlaced,LastDateSold,GRN,FarmNo,nlote2 EntityNo,HouseNo,PenNo, 
                                DENSE_RANK() OVER (PARTITION BY FarmNo,HouseNo ORDER BY FarmNo,PlantelCampana ASC) AS fila
                                FROM {database_name_si}.si_mvbrimentities A 
                                left join {database_name_gl}.lk_lote B on A.FarmNo = B.noplantel and A.EntityNo = B.nlote 
                                WHERE GRN = 'P' and FarmType = 7 and SpeciesType = 2""")

df_OrdenarFechas = df_OrdenarFechas1.union(df_OrdenarFechas2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarFechas"
}
df_OrdenarFechas.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarFechas")
print('carga OrdenarFechas ', df_OrdenarFechas.count())
df_DiasCampana= spark.sql(f"""
SELECT ProteinEntitiesIRN,ComplexEntityNo,(SELECT MIN(FirstDatePlaced) FROM {database_name_tmp}.OrdenarFechas A 
WHERE A.FarmNo = BE.FarmNo AND A.fila = BE.fila AND A.HouseNo = BE.HouseNo AND FirstDatePlaced <> '1899-11-30 00:00:00.000') AS FechaCrianza, 
(SELECT MAX(LastDateSold) FROM {database_name_tmp}.OrdenarFechas A WHERE A.FarmNo = BE.FarmNo AND A.fila = BE.fila-1  AND A.HouseNo = BE.HouseNo) AS FechaInicioGranja 
FROM {database_name_tmp}.OrdenarFechas BE 
WHERE GRN = 'P'""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DiasCampana"
}
df_DiasCampana.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.DiasCampana")
print('carga DiasCampana', df_DiasCampana.count())
df_AlojamientoMayor =spark.sql(f"""
select ComplexEntityNo,PadreMayor,RazaMayor,IncubadoraMayor,EdadPadreCorralDescrip,MAX(PorcAlojPadreMayor) PorcAlojPadreMayor, 
MAX(PorcRazaMayor) PorcRazaMayor,MAX(PorcIncMayor) PorcIncMayor,MAX(DiasAloj) DiasAloj,MAX(PorcAlojamientoXEdadPadre) PorcAlojamientoXEdadPadre,TipoOrigen 
from {database_name_gl}.ft_alojamiento 
where pk_empresa = 1 
group by ComplexEntityNo,PadreMayor,RazaMayor,IncubadoraMayor,EdadPadreCorralDescrip,TipoOrigen order by 1""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AlojamientoMayor"
}
df_AlojamientoMayor.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.AlojamientoMayor")
print('carga AlojamientoMayor ', df_AlojamientoMayor.count())
df_EdadPadre = spark.sql(f"""
select ComplexEntityNo,max(EdadPadreCorral) EdadPadreCorral,max(EdadPadreGalpon) EdadPadreGalpon,max(EdadPadreLote) EdadPadreLote
from {database_name_gl}.ft_alojamiento 
where pk_empresa = 1 group by ComplexEntityNo""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadPadre"
}
df_EdadPadre.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.EdadPadre")
print('carga EdadPadre ', df_EdadPadre.count())
#Realiza el cálculo de SacaYSTD
df_SacaYSTD= spark.sql(f"""
select 
 pk_tiempo
,descripfecha
,pk_plantel
,pk_lote
,pk_galpon
,pk_sexo
,ComplexEntityNo
,EdadDiaSaca
,AvesGranja
,STDPorcMortAcum
,AvesGranja * STDPorcMortAcum STDPorcMortAcumXAvesGranja
,STDPeso,AvesGranja * STDPeso STDPesoXAvesGranja 
,STDConsAcum
,AvesGranja * STDConsAcum STDConsAcumXAvesGranja
,STDICA,AvesGranja * STDICA STDICAXAvesGranja
,STDPorcConsGasInvierno 
,AvesGranja * STDPorcConsGasInvierno STDPorcConsGasInviernoXAvesGranja
,STDPorcConsGasVerano
,AvesGranja * STDPorcConsGasVerano STDPorcConsGasVeranoXAvesGranja
from (
select
 A.pk_tiempo
,A.descripfecha
,A.pk_plantel
,A.pk_lote
,A.pk_galpon
,A.pk_sexo
,A.ComplexEntityNo
,A.EdadDiaSaca
,SUM(AvesGranja) AvesGranja
,MAX(STDPorcMortAcum)STDPorcMortAcum
,MAX(STDPeso)STDPeso
,MAX(STDConsAcum)STDConsAcum
,MAX(STDICA)STDICA
,MAX(STDPorcConsGasInvierno) STDPorcConsGasInvierno
,MAX(STDPorcConsGasVerano) STDPorcConsGasVerano
from {database_name_gl}.ft_ventas_CD A
left join {database_name_gl}.ft_consolidado_Diario B on A.pk_tiempo = B.pk_tiempo and A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_galpon = B.pk_galpon and A.pk_sexo = B.pk_sexo
where A.pk_empresa = 1
group by A.pk_tiempo,A.descripfecha,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,A.EdadDiaSaca
)A 
WHERE date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaYSTD"
}
df_SacaYSTD.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.SacaYSTD")
print('carga SacaYSTD', df_SacaYSTD.count())
#Realiza el cálculo de STDPond
df_STDPond = spark.sql(f"""
WITH CalculatedSTDs AS (
    SELECT
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_sexo,
        ComplexEntityNo,
        MAX(pk_tiempo) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo, ComplexEntityNo) AS pk_tiempo,
        MAX(descripfecha) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo, ComplexEntityNo) AS fecha,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN STDPorcMortAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_mort_sex,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_mort_sex,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN STDPorcMortAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_mort_galpon,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_mort_galpon,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN STDPorcMortAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_mort_lote,
        SUM(CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_mort_lote,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN STDPesoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_peso_sex,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_peso_sex,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN STDPesoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_peso_galpon,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_peso_galpon,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN STDPesoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_peso_lote,
        SUM(CASE WHEN STDPesoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_peso_lote,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN STDConsAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_cons_sex,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_cons_sex,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN STDConsAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_cons_galpon,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_cons_galpon,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN STDConsAcumXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_cons_lote,
        SUM(CASE WHEN STDConsAcumXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_cons_lote,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN STDICAXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_ica_sex,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_ica_sex,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN STDICAXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_ica_galpon,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_ica_galpon,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN STDICAXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_ica_lote,
        SUM(CASE WHEN STDICAXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_ica_lote,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN STDPorcConsGasInviernoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_gas_inv_sex,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_gas_inv_sex,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN STDPorcConsGasInviernoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_gas_inv_galpon,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_gas_inv_galpon,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN STDPorcConsGasInviernoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_gas_inv_lote,
        SUM(CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_gas_inv_lote,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN STDPorcConsGasVeranoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_gas_ver_sex,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon, pk_sexo) AS sum_aves_gas_ver_sex,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN STDPorcConsGasVeranoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_gas_ver_galpon,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) AS sum_aves_gas_ver_galpon,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN STDPorcConsGasVeranoXAvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_gas_ver_lote,
        SUM(CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN AvesGranja ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) AS sum_aves_gas_ver_lote,
        CASE WHEN STDPorcMortAcumXAvesGranja <> 0 THEN STDPorcMortAcumXAvesGranja ELSE NULL END AS STDPorcMortAcumXAvesGranja_orig,
        CASE WHEN STDPesoXAvesGranja <> 0 THEN STDPesoXAvesGranja ELSE NULL END AS STDPesoXAvesGranja_orig,
        CASE WHEN STDConsAcumXAvesGranja <> 0 THEN STDConsAcumXAvesGranja ELSE NULL END AS STDConsAcumXAvesGranja_orig,
        CASE WHEN STDICAXAvesGranja <> 0 THEN STDICAXAvesGranja ELSE NULL END AS STDICAXAvesGranja_orig,
        CASE WHEN STDPorcConsGasInviernoXAvesGranja <> 0 THEN STDPorcConsGasInviernoXAvesGranja ELSE NULL END AS STDPorcConsGasInviernoXAvesGranja_orig,
        CASE WHEN STDPorcConsGasVeranoXAvesGranja <> 0 THEN STDPorcConsGasVeranoXAvesGranja ELSE NULL END AS STDPorcConsGasVeranoXAvesGranja_orig,
        AvesGranja
    FROM
        {database_name_tmp}.SacaYSTD
),
FinalJoin AS (
    SELECT DISTINCT
        pk_tiempo,
        fecha,
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_sexo,
        ComplexEntityNo,
        CASE WHEN sum_aves_mort_sex > 0 THEN sum_mort_sex / sum_aves_mort_sex ELSE NULL END AS STDPorcMortAcumC,
        CASE WHEN sum_aves_mort_galpon > 0 THEN sum_mort_galpon / sum_aves_mort_galpon ELSE NULL END AS STDPorcMortAcumG,
        CASE WHEN sum_aves_mort_lote > 0 THEN sum_mort_lote / sum_aves_mort_lote ELSE NULL END AS STDPorcMortAcumL,
        CASE WHEN sum_aves_peso_sex > 0 THEN sum_peso_sex / sum_aves_peso_sex ELSE NULL END AS STDPesoC,
        CASE WHEN sum_aves_peso_galpon > 0 THEN sum_peso_galpon / sum_aves_peso_galpon ELSE NULL END AS STDPesoG,
        CASE WHEN sum_aves_peso_lote > 0 THEN sum_peso_lote / sum_aves_peso_lote ELSE NULL END AS STDPesoL,
        CASE WHEN sum_aves_cons_sex > 0 THEN sum_cons_sex / sum_aves_cons_sex ELSE NULL END AS STDConsAcumC,
        CASE WHEN sum_aves_cons_galpon > 0 THEN sum_cons_galpon / sum_aves_cons_galpon ELSE NULL END AS STDConsAcumG,
        CASE WHEN sum_aves_cons_lote > 0 THEN sum_cons_lote / sum_aves_cons_lote ELSE NULL END AS STDConsAcumL,
        CASE WHEN sum_aves_ica_sex > 0 THEN sum_ica_sex / sum_aves_ica_sex ELSE NULL END AS STDICAC,
        CASE WHEN sum_aves_ica_galpon > 0 THEN sum_ica_galpon / sum_aves_ica_galpon ELSE NULL END AS STDICAG,
        CASE WHEN sum_aves_ica_lote > 0 THEN sum_ica_lote / sum_aves_ica_lote ELSE NULL END AS STDICAL,
        CASE WHEN sum_aves_gas_inv_sex > 0 THEN sum_gas_inv_sex / sum_aves_gas_inv_sex ELSE NULL END AS STDPorcConsGasInviernoC,
        CASE WHEN sum_aves_gas_inv_galpon > 0 THEN sum_gas_inv_galpon / sum_aves_gas_inv_galpon ELSE NULL END AS STDPorcConsGasInviernoG,
        CASE WHEN sum_aves_gas_inv_lote > 0 THEN sum_gas_inv_lote / sum_aves_gas_inv_lote ELSE NULL END AS STDPorcConsGasInviernoL,
        CASE WHEN sum_aves_gas_ver_sex > 0 THEN sum_gas_ver_sex / sum_aves_gas_ver_sex ELSE NULL END AS STDPorcConsGasVeranoC,
        CASE WHEN sum_aves_gas_ver_galpon > 0 THEN sum_gas_ver_galpon / sum_aves_gas_ver_galpon ELSE NULL END AS STDPorcConsGasVeranoG,
        CASE WHEN sum_aves_gas_ver_lote > 0 THEN sum_gas_ver_lote / sum_aves_gas_ver_lote ELSE NULL END AS STDPorcConsGasVeranoL
    FROM
        CalculatedSTDs
)
SELECT
    pk_tiempo,
    fecha,
    pk_plantel,
    pk_lote,
    pk_galpon,
    pk_sexo,
    ComplexEntityNo,
    MAX(STDPorcMortAcumC) AS STDPorcMortAcumC,
    MAX(STDPorcMortAcumG) AS STDPorcMortAcumG,
    MAX(STDPorcMortAcumL) AS STDPorcMortAcumL,
    MAX(STDPesoC) AS STDPesoC,
    MAX(STDPesoG) AS STDPesoG,
    MAX(STDPesoL) AS STDPesoL,
    MAX(STDConsAcumC) AS STDConsAcumC,
    MAX(STDConsAcumG) AS STDConsAcumG,
    MAX(STDConsAcumL) AS STDConsAcumL,
    MAX(STDICAC) AS STDICAC,
    MAX(STDICAG) AS STDICAG,
    MAX(STDICAL) AS STDICAL,
    MAX(STDPorcConsGasInviernoC) AS STDPorcConsGasInviernoC,
    MAX(STDPorcConsGasInviernoG) AS STDPorcConsGasInviernoG,
    MAX(STDPorcConsGasInviernoL) AS STDPorcConsGasInviernoL,
    MAX(STDPorcConsGasVeranoC) AS STDPorcConsGasVeranoC,
    MAX(STDPorcConsGasVeranoG) AS STDPorcConsGasVeranoG,
    MAX(STDPorcConsGasVeranoL) AS STDPorcConsGasVeranoL
FROM
    FinalJoin
GROUP BY
    pk_tiempo,
    fecha,
    pk_plantel,
    pk_lote,
    pk_galpon,
    pk_sexo,
    ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPond"
}
df_STDPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPond")
print('carga STDPond', df_STDPond.count())
#Realiza el cálculo de acumuladoSaca
df_acumuladoSaca = spark.sql(f"""
    WITH VentasMax AS (
        SELECT 
            ComplexEntityNo, 
            MAX(COALESCE(avesgranjaacum, 0)) AS AvesRendidas,
            MAX(COALESCE(PesoGranjaAcum, 0)) AS kilosRendidos
        FROM {database_name_gl}.ft_ventas_CD
        GROUP BY ComplexEntityNo
    ),
    VentasAcum AS (
        SELECT 
            SS.ComplexEntityNo, 
            PD.descripfecha fecha,
            MAX(COALESCE(SS.avesgranjaacum, 0)) AS AvesRendidasAcum,
            MAX(COALESCE(SS.PesoGranjaAcum, 0)) AS kilosRendidosAcum
        FROM {database_name_gl}.stg_ProduccionDetalle PD
        LEFT JOIN {database_name_gl}.ft_ventas_CD SS 
            ON PD.ComplexEntityNo = SS.ComplexEntityNo 
            AND PD.descripfecha >= SS.descripfecha
        GROUP BY SS.ComplexEntityNo, PD.descripfecha
    )
    SELECT 
        PD.pk_tiempo,
        PD.descripfecha fecha,
        PD.ComplexEntityNo,
        COALESCE(VM.AvesRendidas, 0.0) AS AvesRendidas,
        COALESCE(VA.AvesRendidasAcum, 0.0) AS AvesRendidasAcum,
        COALESCE(VM.kilosRendidos, 0.0) AS kilosRendidos,
        COALESCE(VA.kilosRendidosAcum, 0.0) AS kilosRendidosAcum
    FROM {database_name_gl}.stg_ProduccionDetalle PD
    LEFT JOIN VentasMax VM ON PD.ComplexEntityNo = VM.ComplexEntityNo
    LEFT JOIN VentasAcum VA ON PD.ComplexEntityNo = VA.ComplexEntityNo AND PD.descripfecha = VA.fecha
    WHERE DATE_FORMAT(PD.descripfecha, 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
    GROUP BY PD.pk_tiempo, PD.descripfecha, PD.ComplexEntityNo, VM.AvesRendidas, VA.AvesRendidasAcum, VM.kilosRendidos, VA.kilosRendidosAcum
    ORDER BY PD.descripfecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/acumuladoSaca"
}
df_acumuladoSaca.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.acumuladoSaca")
print('carga acumuladoSaca', df_acumuladoSaca.count())
#Realiza el cálculo de STDPorcMort
df_STDPorcMort = spark.sql(f"""
select A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,cast(MAX(PobInicial) as int) PobInicial,MAX(STDPorcMortSem)STDPorcMortSem,MAX(STDPorcMortSemAcum)STDPorcMortSemAcum
from {database_name_gl}.ft_mortalidad_diario A
where A.pk_empresa = 1 and DATE_FORMAT(A.descripfecha, 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcMort"
}
df_STDPorcMort.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcMort")
print('carga STDPorcMort', df_STDPorcMort.count())
#Realiza el cálculo de STDPorcMortXPobInicial
df_STDPorcMortXPobInicial = spark.sql(f"""
WITH aggregated_data AS (
    SELECT 
        ComplexEntityNo,pk_semanavida,MAX(STDPorcMortSem) AS STDPorcMortSem,MAX(STDPorcMortSemAcum) AS STDPorcMortSemAcum
    FROM {database_name_tmp}.STDPorcMort
    GROUP BY ComplexEntityNo,pk_semanavida
)

select 
A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,
    -- Pivot Mortality Percentage by Week
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 2  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem1,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 3  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem2,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 4  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem3,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 5  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem4,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 6  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem5,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 7  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem6,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 8  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem7,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 9  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem8,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem9,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem10,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem11,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem12,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem13,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem14,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem15,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem16,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem17,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem18,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem19,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 21 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem20,
    -- Pivot Accumulated Mortality Percentage by Week
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 2  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum1,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 3  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum2,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 4  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum3,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 5  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum4,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 6  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum5,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 7  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum6,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 8  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum7,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 9  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum8,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum9,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum10,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum11,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum12,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum13,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum14,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum15,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum16,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum17,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum18,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum19,
    COALESCE(MAX(CASE WHEN B.pk_semanavida = 21 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum20,
    -- Pivot PobInicial x Mortality Percentage by Week
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 2  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem1XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 3  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem2XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 4  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem3XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 5  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem4XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 6  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem5XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 7  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem6XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 8  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem7XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 9  THEN B.STDPorcMortSem END),0) AS STDPorcMortSem8XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem9XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem10XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem11XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem12XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem13XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem14XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem15XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem16XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem17XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem18XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem19XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 21 THEN B.STDPorcMortSem END),0) AS STDPorcMortSem20XPobInicial,
    -- Pivot PobInicial x Accumulated Mortality Percentage by Week
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 2  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum1XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 3  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum2XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 4  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum3XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 5  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum4XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 6  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum5XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 7  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum6XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 8  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum7XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 9  THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum8XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum9XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum10XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum11XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum12XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum13XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum14XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum15XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum16XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum17XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum18XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum19XPobInicial,
    COALESCE(MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 21 THEN B.STDPorcMortSemAcum END),0) AS STDPorcMortSemAcum20XPobInicial
FROM {database_name_tmp}.STDPorcMort A
LEFT JOIN aggregated_data B ON A.ComplexEntityNo = B.ComplexEntityNo
GROUP BY A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcMortXPobInicial"
}
df_STDPorcMortXPobInicial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcMortXPobInicial")
print('carga STDPorcMortXPobInicial', df_STDPorcMortXPobInicial.count())
#Realiza el cálculo de STDMortPond
df_STDMortPond = spark.sql(f"""
WITH aggregated_mortality AS (
    SELECT 
        pk_plantel, 
        pk_lote, 
        pk_galpon, 
        SUM(stdporcmortsem1xpobinicial) / SUM(pobinicial) AS stdporcmortsem1G,
        SUM(stdporcmortsem2xpobinicial) / SUM(pobinicial) AS stdporcmortsem2G,
        SUM(stdporcmortsem3xpobinicial) / SUM(pobinicial) AS stdporcmortsem3G,
        SUM(stdporcmortsem4xpobinicial) / SUM(pobinicial) AS stdporcmortsem4G,
        SUM(stdporcmortsem5xpobinicial) / SUM(pobinicial) AS stdporcmortsem5G,
        SUM(stdporcmortsem6xpobinicial) / SUM(pobinicial) AS stdporcmortsem6G,
        SUM(stdporcmortsem7xpobinicial) / SUM(pobinicial) AS stdporcmortsem7G,
        SUM(stdporcmortsem8xpobinicial) / SUM(pobinicial) AS stdporcmortsem8G,
        SUM(stdporcmortsem9xpobinicial) / SUM(pobinicial) AS stdporcmortsem9G,
        SUM(stdporcmortsem10xpobinicial) / SUM(pobinicial) AS stdporcmortsem10G,
        SUM(stdporcmortsem11xpobinicial) / SUM(pobinicial) AS stdporcmortsem11G,
        SUM(stdporcmortsem12xpobinicial) / SUM(pobinicial) AS stdporcmortsem12G,
        SUM(stdporcmortsem13xpobinicial) / SUM(pobinicial) AS stdporcmortsem13G,
        SUM(stdporcmortsem14xpobinicial) / SUM(pobinicial) AS stdporcmortsem14G,
        SUM(stdporcmortsem15xpobinicial) / SUM(pobinicial) AS stdporcmortsem15G,
        SUM(stdporcmortsem16xpobinicial) / SUM(pobinicial) AS stdporcmortsem16G,
        SUM(stdporcmortsem17xpobinicial) / SUM(pobinicial) AS stdporcmortsem17G,
        SUM(stdporcmortsem18xpobinicial) / SUM(pobinicial) AS stdporcmortsem18G,
        SUM(stdporcmortsem19xpobinicial) / SUM(pobinicial) AS stdporcmortsem19G,
        SUM(stdporcmortsem20xpobinicial) / SUM(pobinicial) AS stdporcmortsem20G,
        -- Mortalidad acumulada
        SUM(stdporcmortsemacum1xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum1G,
        SUM(stdporcmortsemacum2xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum2G,
        SUM(stdporcmortsemacum3xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum3G,
        SUM(stdporcmortsemacum4xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum4G,
        SUM(stdporcmortsemacum5xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum5G,
        SUM(stdporcmortsemacum6xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum6G,
        SUM(stdporcmortsemacum7xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum7G,
        SUM(stdporcmortsemacum8xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum8G,
        SUM(stdporcmortsemacum9xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum9G,
        SUM(stdporcmortsemacum10xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum10G,
        SUM(stdporcmortsemacum11xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum11G,
        SUM(stdporcmortsemacum12xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum12G,
        SUM(stdporcmortsemacum13xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum13G,
        SUM(stdporcmortsemacum14xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum14G,
        SUM(stdporcmortsemacum15xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum15G,
        SUM(stdporcmortsemacum16xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum16G,
        SUM(stdporcmortsemacum17xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum17G,
        SUM(stdporcmortsemacum18xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum18G,
        SUM(stdporcmortsemacum19xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum19G,
        SUM(stdporcmortsemacum20xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum20G
    FROM {database_name_tmp}.STDPorcMortXPobInicial
    WHERE pobinicial <> 0
    GROUP BY pk_plantel, pk_lote, pk_galpon
)
,aggregated_mortality_a AS (
    SELECT 
        pk_plantel, 
        pk_lote, 
        SUM(stdporcmortsem1xpobinicial) / SUM(pobinicial) AS stdporcmortsem1G,
        SUM(stdporcmortsem2xpobinicial) / SUM(pobinicial) AS stdporcmortsem2G,
        SUM(stdporcmortsem3xpobinicial) / SUM(pobinicial) AS stdporcmortsem3G,
        SUM(stdporcmortsem4xpobinicial) / SUM(pobinicial) AS stdporcmortsem4G,
        SUM(stdporcmortsem5xpobinicial) / SUM(pobinicial) AS stdporcmortsem5G,
        SUM(stdporcmortsem6xpobinicial) / SUM(pobinicial) AS stdporcmortsem6G,
        SUM(stdporcmortsem7xpobinicial) / SUM(pobinicial) AS stdporcmortsem7G,
        SUM(stdporcmortsem8xpobinicial) / SUM(pobinicial) AS stdporcmortsem8G,
        SUM(stdporcmortsem9xpobinicial) / SUM(pobinicial) AS stdporcmortsem9G,
        SUM(stdporcmortsem10xpobinicial) / SUM(pobinicial) AS stdporcmortsem10G,
        SUM(stdporcmortsem11xpobinicial) / SUM(pobinicial) AS stdporcmortsem11G,
        SUM(stdporcmortsem12xpobinicial) / SUM(pobinicial) AS stdporcmortsem12G,
        SUM(stdporcmortsem13xpobinicial) / SUM(pobinicial) AS stdporcmortsem13G,
        SUM(stdporcmortsem14xpobinicial) / SUM(pobinicial) AS stdporcmortsem14G,
        SUM(stdporcmortsem15xpobinicial) / SUM(pobinicial) AS stdporcmortsem15G,
        SUM(stdporcmortsem16xpobinicial) / SUM(pobinicial) AS stdporcmortsem16G,
        SUM(stdporcmortsem17xpobinicial) / SUM(pobinicial) AS stdporcmortsem17G,
        SUM(stdporcmortsem18xpobinicial) / SUM(pobinicial) AS stdporcmortsem18G,
        SUM(stdporcmortsem19xpobinicial) / SUM(pobinicial) AS stdporcmortsem19G,
        SUM(stdporcmortsem20xpobinicial) / SUM(pobinicial) AS stdporcmortsem20G,
        -- Mortalidad acumulada
        SUM(stdporcmortsemacum1xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum1G,
        SUM(stdporcmortsemacum2xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum2G,
        SUM(stdporcmortsemacum3xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum3G,
        SUM(stdporcmortsemacum4xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum4G,
        SUM(stdporcmortsemacum5xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum5G,
        SUM(stdporcmortsemacum6xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum6G,
        SUM(stdporcmortsemacum7xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum7G,
        SUM(stdporcmortsemacum8xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum8G,
        SUM(stdporcmortsemacum9xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum9G,
        SUM(stdporcmortsemacum10xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum10G,
        SUM(stdporcmortsemacum11xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum11G,
        SUM(stdporcmortsemacum12xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum12G,
        SUM(stdporcmortsemacum13xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum13G,
        SUM(stdporcmortsemacum14xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum14G,
        SUM(stdporcmortsemacum15xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum15G,
        SUM(stdporcmortsemacum16xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum16G,
        SUM(stdporcmortsemacum17xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum17G,
        SUM(stdporcmortsemacum18xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum18G,
        SUM(stdporcmortsemacum19xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum19G,
        SUM(stdporcmortsemacum20xpobinicial) / SUM(pobinicial) AS stdporcmortsemacum20G
    FROM {database_name_tmp}.STDPorcMortXPobInicial
    WHERE pobinicial <> 0
    GROUP BY pk_plantel, pk_lote	
)
select  
        A.pk_plantel, 
        A.pk_lote, 
        A.pk_galpon, 
        A.pk_sexo, 
        A.complexentityno,
		STDPorcMortSem1,
		STDPorcMortSem2,
		STDPorcMortSem3,
		STDPorcMortSem4,
		STDPorcMortSem5,
		STDPorcMortSem6,
		STDPorcMortSem7,
		STDPorcMortSem8,
		STDPorcMortSem9,
		STDPorcMortSem10,
		STDPorcMortSem11,
		STDPorcMortSem12,
		STDPorcMortSem13,
		STDPorcMortSem14,
		STDPorcMortSem15,
		STDPorcMortSem16,
		STDPorcMortSem17,
		STDPorcMortSem18,
		STDPorcMortSem19,
		STDPorcMortSem20,
		STDPorcMortSemAcum1,
		STDPorcMortSemAcum2,
		STDPorcMortSemAcum3,
		STDPorcMortSemAcum4,
		STDPorcMortSemAcum5,
		STDPorcMortSemAcum6,
		STDPorcMortSemAcum7,
		STDPorcMortSemAcum8,
		STDPorcMortSemAcum9,
		STDPorcMortSemAcum10,
		STDPorcMortSemAcum11,
		STDPorcMortSemAcum12,
		STDPorcMortSemAcum13,
		STDPorcMortSemAcum14,
		STDPorcMortSemAcum15,
		STDPorcMortSemAcum16,
		STDPorcMortSemAcum17,
		STDPorcMortSemAcum18,
		STDPorcMortSemAcum19,
		STDPorcMortSemAcum20,
    B.STDPorcMortSem1G,
    B.STDPorcMortSem2G,
    B.STDPorcMortSem3G,
    B.STDPorcMortSem4G,
    B.STDPorcMortSem5G,
    B.STDPorcMortSem6G,
    B.STDPorcMortSem7G,
    B.STDPorcMortSem8G,
    B.STDPorcMortSem9G,
    B.stdporcmortsem10G,
    B.stdporcmortsem11G,
    B.stdporcmortsem12G,
    B.stdporcmortsem13G,
    B.stdporcmortsem14G,
    B.stdporcmortsem15G,
    B.stdporcmortsem16G,
    B.stdporcmortsem17G,
    B.stdporcmortsem18G,
    B.stdporcmortsem19G,
    B.stdporcmortsem20G,
    B.stdporcmortsemacum1G,
    B.stdporcmortsemacum2G,
    B.stdporcmortsemacum3G,
    B.stdporcmortsemacum4G,
    B.stdporcmortsemacum5G,
    B.stdporcmortsemacum6G,
    B.stdporcmortsemacum7G,
    B.stdporcmortsemacum8G,
    B.stdporcmortsemacum9G,
    B.stdporcmortsemacum10G,
    B.stdporcmortsemacum11G,
    B.stdporcmortsemacum12G,
    B.stdporcmortsemacum13G,
    B.stdporcmortsemacum14G,
    B.stdporcmortsemacum15G,
    B.stdporcmortsemacum16G,
    B.stdporcmortsemacum17G,
    B.stdporcmortsemacum18G,
    B.stdporcmortsemacum19G,
    B.stdporcmortsemacum20G,
    C.STDPorcMortSem1G as STDPorcMortSem1L,
    C.STDPorcMortSem2G as STDPorcMortSem2L,
    C.STDPorcMortSem3G as STDPorcMortSem3L,
    C.STDPorcMortSem4G as STDPorcMortSem4L,
    C.STDPorcMortSem5G as STDPorcMortSem5L,
    C.STDPorcMortSem6G as STDPorcMortSem6L,
    C.STDPorcMortSem7G as STDPorcMortSem7L,
    C.STDPorcMortSem8G as STDPorcMortSem8L,
    C.STDPorcMortSem9G as STDPorcMortSem9L,
    C.stdporcmortsem10G AS stdporcmortsem10L,    
    C.stdporcmortsem11G AS stdporcmortsem11L,    
    C.stdporcmortsem12G AS stdporcmortsem12L,    
    C.stdporcmortsem13G AS stdporcmortsem13L,    
    C.stdporcmortsem14G AS stdporcmortsem14L,    
    C.stdporcmortsem15G AS stdporcmortsem15L,    
    C.stdporcmortsem16G AS stdporcmortsem16L,    
    C.stdporcmortsem17G AS stdporcmortsem17L,    
    C.stdporcmortsem18G AS stdporcmortsem18L,    
    C.stdporcmortsem19G AS stdporcmortsem19L,    
    C.stdporcmortsem20G AS stdporcmortsem20L,    
    C.stdporcmortsemacum1G as stdporcmortsemacum1L,
    C.stdporcmortsemacum2G as stdporcmortsemacum2L,
    C.stdporcmortsemacum3G as stdporcmortsemacum3L,
    C.stdporcmortsemacum4G as stdporcmortsemacum4L,
    C.stdporcmortsemacum5G as stdporcmortsemacum5L,
    C.stdporcmortsemacum6G as stdporcmortsemacum6L,
    C.stdporcmortsemacum7G as stdporcmortsemacum7L,
    C.stdporcmortsemacum8G as stdporcmortsemacum8L,
    C.stdporcmortsemacum9G as stdporcmortsemacum9L,
    C.stdporcmortsemacum10G AS stdporcmortsemacum10L,  
    C.stdporcmortsemacum11G AS stdporcmortsemacum11L,    
    C.stdporcmortsemacum12G AS stdporcmortsemacum12L,    
    C.stdporcmortsemacum13G AS stdporcmortsemacum13L,    
    C.stdporcmortsemacum14G AS stdporcmortsemacum14L,    
    C.stdporcmortsemacum15G AS stdporcmortsemacum15L,    
    C.stdporcmortsemacum16G AS stdporcmortsemacum16L,    
    C.stdporcmortsemacum17G AS stdporcmortsemacum17L,    
    C.stdporcmortsemacum18G AS stdporcmortsemacum18L,    
    C.stdporcmortsemacum19G AS stdporcmortsemacum19L,    
    C.stdporcmortsemacum20G AS stdporcmortsemacum20L   
FROM {database_name_tmp}.stdporcmortxpobinicial A
LEFT JOIN aggregated_mortality B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon
LEFT JOIN aggregated_mortality_a C ON A.pk_plantel = C.pk_plantel AND A.pk_lote = C.pk_lote
ORDER BY A.complexentityno
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDMortPond"
}
df_STDMortPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDMortPond")
print('carga STDMortPond', df_STDMortPond.count())
#Realiza el cálculo de STDPorcPeso
df_STDPorcPeso1 = spark.sql(f"""select A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,MAX(STDPeso) STDPeso 
                            from {database_name_gl}.ft_peso_diario A where A.pk_empresa = 1 group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo""")

df_STDPorcPeso2 = spark.sql(f"""select 0 pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,MAX(STDPeso) STDPeso
                            from {database_name_gl}.ft_peso_diario A where A.pk_empresa = 1 and A.pk_diasvida = 6 group by A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo""")

df_STDPorcPeso= df_STDPorcPeso1.union(df_STDPorcPeso2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcPeso"
}
df_STDPorcPeso.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcPeso")
print('carga STDPorcPeso', df_STDPorcPeso.count())
#Realiza el cálculo de STDPorcPesoXPobInicial
df_STDPorcPesoXPobInicial = spark.sql(f"""WITH aggregated_weights AS (
    SELECT 
        ComplexEntityNo,
        MAX(CASE WHEN pk_semanavida = 2  THEN STDPeso ELSE NULL END) AS STDPesoSem1,
        MAX(CASE WHEN pk_semanavida = 3  THEN STDPeso ELSE NULL END) AS STDPesoSem2,
        MAX(CASE WHEN pk_semanavida = 4  THEN STDPeso ELSE NULL END) AS STDPesoSem3,
        MAX(CASE WHEN pk_semanavida = 5  THEN STDPeso ELSE NULL END) AS STDPesoSem4,
        MAX(CASE WHEN pk_semanavida = 6  THEN STDPeso ELSE NULL END) AS STDPesoSem5,
        MAX(CASE WHEN pk_semanavida = 7  THEN STDPeso ELSE NULL END) AS STDPesoSem6,
        MAX(CASE WHEN pk_semanavida = 8  THEN STDPeso ELSE NULL END) AS STDPesoSem7,
        MAX(CASE WHEN pk_semanavida = 9  THEN STDPeso ELSE NULL END) AS STDPesoSem8,
        MAX(CASE WHEN pk_semanavida = 10 THEN STDPeso ELSE NULL END) AS STDPesoSem9,
        MAX(CASE WHEN pk_semanavida = 11 THEN STDPeso ELSE NULL END) AS STDPesoSem10,
        MAX(CASE WHEN pk_semanavida = 12 THEN STDPeso ELSE NULL END) AS STDPesoSem11,
        MAX(CASE WHEN pk_semanavida = 13 THEN STDPeso ELSE NULL END) AS STDPesoSem12,
        MAX(CASE WHEN pk_semanavida = 14 THEN STDPeso ELSE NULL END) AS STDPesoSem13,
        MAX(CASE WHEN pk_semanavida = 15 THEN STDPeso ELSE NULL END) AS STDPesoSem14,
        MAX(CASE WHEN pk_semanavida = 16 THEN STDPeso ELSE NULL END) AS STDPesoSem15,
        MAX(CASE WHEN pk_semanavida = 17 THEN STDPeso ELSE NULL END) AS STDPesoSem16,
        MAX(CASE WHEN pk_semanavida = 18 THEN STDPeso ELSE NULL END) AS STDPesoSem17,
        MAX(CASE WHEN pk_semanavida = 19 THEN STDPeso ELSE NULL END) AS STDPesoSem18,
        MAX(CASE WHEN pk_semanavida = 20 THEN STDPeso ELSE NULL END) AS STDPesoSem19,
        MAX(CASE WHEN pk_semanavida = 21 THEN STDPeso ELSE NULL END) AS STDPesoSem20,
        MAX(CASE WHEN pk_semanavida = 0 THEN STDPeso ELSE NULL END) AS STDPeso5Dias
    FROM {database_name_tmp}.STDPorcPeso
    GROUP BY ComplexEntityNo
)

SELECT 
    A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo,
    MAX(A.PobInicial) AS PobInicial,
    COALESCE(B.STDPesoSem1, 0) AS STDPesoSem1,
    COALESCE(B.STDPesoSem2, 0) AS STDPesoSem2,
    COALESCE(B.STDPesoSem3, 0) AS STDPesoSem3,
    COALESCE(B.STDPesoSem4, 0) AS STDPesoSem4,
    COALESCE(B.STDPesoSem5, 0) AS STDPesoSem5,
    COALESCE(B.STDPesoSem6, 0) AS STDPesoSem6,
    COALESCE(B.STDPesoSem7, 0) AS STDPesoSem7,
    COALESCE(B.STDPesoSem8, 0) AS STDPesoSem8,
    COALESCE(B.STDPesoSem9, 0) AS STDPesoSem9,
    COALESCE(B.STDPesoSem10, 0) AS STDPesoSem10,
    COALESCE(B.STDPesoSem11, 0) AS STDPesoSem11,
    COALESCE(B.STDPesoSem12, 0) AS STDPesoSem12,
    COALESCE(B.STDPesoSem13, 0) AS STDPesoSem13,
    COALESCE(B.STDPesoSem14, 0) AS STDPesoSem14,
    COALESCE(B.STDPesoSem15, 0) AS STDPesoSem15,
    COALESCE(B.STDPesoSem16, 0) AS STDPesoSem16,
    COALESCE(B.STDPesoSem17, 0) AS STDPesoSem17,
    COALESCE(B.STDPesoSem18, 0) AS STDPesoSem18,
    COALESCE(B.STDPesoSem19, 0) AS STDPesoSem19,
    COALESCE(B.STDPesoSem20, 0) AS STDPesoSem20,
    COALESCE(B.STDPeso5Dias, 0) AS STDPeso5Dias,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem1, 0) AS STDPesoSem1XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem2, 0) AS STDPesoSem2XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem3, 0) AS STDPesoSem3XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem4, 0) AS STDPesoSem4XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem5, 0) AS STDPesoSem5XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem6, 0) AS STDPesoSem6XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem7, 0) AS STDPesoSem7XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem8, 0) AS STDPesoSem8XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem9, 0) AS STDPesoSem9XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem10, 0) AS STDPesoSem10XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem11, 0) AS STDPesoSem11XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem12, 0) AS STDPesoSem12XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem13, 0) AS STDPesoSem13XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem14, 0) AS STDPesoSem14XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem15, 0) AS STDPesoSem15XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem16, 0) AS STDPesoSem16XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem17, 0) AS STDPesoSem17XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem18, 0) AS STDPesoSem18XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem19, 0) AS STDPesoSem19XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPesoSem20, 0) AS STDPesoSem20XPobInicial,
    MAX(A.PobInicial) * COALESCE(B.STDPeso5Dias, 0) AS STDPeso5DiasXPobInicial
FROM {database_name_tmp}.STDPorcPeso A
LEFT JOIN aggregated_weights B ON A.ComplexEntityNo = B.ComplexEntityNo
GROUP BY A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo,B.STDPesoSem1,B.STDPesoSem2,B.STDPesoSem3,B.STDPesoSem4,B.STDPesoSem5,B.STDPesoSem6,B.STDPesoSem7,B.STDPesoSem8,B.STDPesoSem9,B.STDPesoSem10,B.STDPesoSem11,B.STDPesoSem12,B.STDPesoSem13,B.STDPesoSem14,B.STDPesoSem15,B.STDPesoSem16,B.STDPesoSem17,B.STDPesoSem18,B.STDPesoSem19,B.STDPesoSem20,B.STDPeso5Dias
ORDER BY A.ComplexEntityNo """)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcPesoXPobInicial"
}
df_STDPorcPesoXPobInicial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcPesoXPobInicial")
print('carga STDPorcPesoXPobInicial', df_STDPorcPesoXPobInicial.count())
#Realiza el cálculo de STDPesoPond
df_STDPesoPond = spark.sql(f"""
WITH PonderedPeso AS (
    SELECT
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_sexo,
        ComplexEntityNo,
        STDPesoSem1,
        STDPesoSem2,
        STDPesoSem3,
        STDPesoSem4,
        STDPesoSem5,
        STDPesoSem6,
        STDPesoSem7,
        STDPesoSem8,
        STDPesoSem9,
        STDPesoSem10,
        STDPesoSem11,
        STDPesoSem12,
        STDPesoSem13,
        STDPesoSem14,
        STDPesoSem15,
        STDPesoSem16,
        STDPesoSem17,
        STDPesoSem18,
        STDPesoSem19,
        STDPesoSem20,
        STDPeso5Dias,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN STDPesoSem1XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem1G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN STDPesoSem2XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem2G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN STDPesoSem3XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem3G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN STDPesoSem4XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem4G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN STDPesoSem5XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem5G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN STDPesoSem6XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem6G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN STDPesoSem7XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem7G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN STDPesoSem8XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem8G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN STDPesoSem9XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem9G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN STDPesoSem10XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem10G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN STDPesoSem11XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem11G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN STDPesoSem12XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem12G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN STDPesoSem13XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem13G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN STDPesoSem14XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem14G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN STDPesoSem15XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem15G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN STDPesoSem16XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem16G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN STDPesoSem17XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem17G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN STDPesoSem18XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem18G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN STDPesoSem19XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem19G,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN STDPesoSem20XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPesoSem20G,
        CASE
            WHEN SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) > 0
            THEN SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN STDPeso5diasXPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon) / SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote, pk_galpon)
            ELSE NULL
        END AS STDPorcPeso5DiasG,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN STDPesoSem1XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem1XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem1L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN STDPesoSem2XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem2XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem2L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN STDPesoSem3XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem3XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem3L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN STDPesoSem4XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem4XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem4L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN STDPesoSem5XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem5XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem5L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN STDPesoSem6XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem6XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem6L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN STDPesoSem7XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem7XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem7L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN STDPesoSem8XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem8XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem8L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN STDPesoSem9XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem9XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem9L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN STDPesoSem10XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem10XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem10L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN STDPesoSem11XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem11XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem11L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN STDPesoSem12XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem12XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem12L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN STDPesoSem13XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem13XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem13L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN STDPesoSem14XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem14XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem14L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN STDPesoSem15XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem15XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem15L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN STDPesoSem16XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem16XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem16L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN STDPesoSem17XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem17XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem17L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN STDPesoSem18XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem18XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem18L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN STDPesoSem19XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem19XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem19L,
        CASE
            WHEN SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN STDPesoSem20XPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPesoSem20XPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPesoSem20L,
        CASE
            WHEN SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) > 0
            THEN SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN STDPeso5diasXPobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote) / SUM(CASE WHEN STDPeso5diasXPobInicial <> 0 THEN PobInicial ELSE 0 END) OVER (PARTITION BY pk_plantel, pk_lote)
            ELSE NULL
        END AS STDPorcPeso5DiasL
    FROM
        {database_name_tmp}.STDPorcPesoXPobInicial
)
SELECT
    pk_plantel,
    pk_lote,
    pk_galpon,
    pk_sexo,
    ComplexEntityNo,
    MAX(STDPesoSem1) AS STDPesoSem1,
    MAX(STDPesoSem2) AS STDPesoSem2,
    MAX(STDPesoSem3) AS STDPesoSem3,
    MAX(STDPesoSem4) AS STDPesoSem4,
    MAX(STDPesoSem5) AS STDPesoSem5,
    MAX(STDPesoSem6) AS STDPesoSem6,
    MAX(STDPesoSem7) AS STDPesoSem7,
    MAX(STDPesoSem8) AS STDPesoSem8,
    MAX(STDPesoSem9) AS STDPesoSem9,
    MAX(STDPesoSem10) AS STDPesoSem10,
    MAX(STDPesoSem11) AS STDPesoSem11,
    MAX(STDPesoSem12) AS STDPesoSem12,
    MAX(STDPesoSem13) AS STDPesoSem13,
    MAX(STDPesoSem14) AS STDPesoSem14,
    MAX(STDPesoSem15) AS STDPesoSem15,
    MAX(STDPesoSem16) AS STDPesoSem16,
    MAX(STDPesoSem17) AS STDPesoSem17,
    MAX(STDPesoSem18) AS STDPesoSem18,
    MAX(STDPesoSem19) AS STDPesoSem19,
    MAX(STDPesoSem20) AS STDPesoSem20,
    MAX(STDPeso5Dias) AS STDPeso5Dias,
    MAX(STDPorcPesoSem1G) AS STDPorcPesoSem1G,
    MAX(STDPorcPesoSem2G) AS STDPorcPesoSem2G,
    MAX(STDPorcPesoSem3G) AS STDPorcPesoSem3G,
    MAX(STDPorcPesoSem4G) AS STDPorcPesoSem4G,
    MAX(STDPorcPesoSem5G) AS STDPorcPesoSem5G,
    MAX(STDPorcPesoSem6G) AS STDPorcPesoSem6G,
    MAX(STDPorcPesoSem7G) AS STDPorcPesoSem7G,
    MAX(STDPorcPesoSem8G) AS STDPorcPesoSem8G,
    MAX(STDPorcPesoSem9G) AS STDPorcPesoSem9G,
    MAX(STDPorcPesoSem10G) AS STDPorcPesoSem10G,
    MAX(STDPorcPesoSem11G) AS STDPorcPesoSem11G,
    MAX(STDPorcPesoSem12G) AS STDPorcPesoSem12G,
    MAX(STDPorcPesoSem13G) AS STDPorcPesoSem13G,
    MAX(STDPorcPesoSem14G) AS STDPorcPesoSem14G,
    MAX(STDPorcPesoSem15G) AS STDPorcPesoSem15G,
    MAX(STDPorcPesoSem16G) AS STDPorcPesoSem16G,
    MAX(STDPorcPesoSem17G) AS STDPorcPesoSem17G,
    MAX(STDPorcPesoSem18G) AS STDPorcPesoSem18G,
    MAX(STDPorcPesoSem19G) AS STDPorcPesoSem19G,
    MAX(STDPorcPesoSem20G) AS STDPorcPesoSem20G,
    MAX(STDPorcPeso5DiasG) AS STDPorcPeso5DiasG,
    MAX(STDPorcPesoSem1L) AS STDPorcPesoSem1L,
    MAX(STDPorcPesoSem2L) AS STDPorcPesoSem2L,
    MAX(STDPorcPesoSem3L) AS STDPorcPesoSem3L,
    MAX(STDPorcPesoSem4L) AS STDPorcPesoSem4L,
    MAX(STDPorcPesoSem5L) AS STDPorcPesoSem5L,
    MAX(STDPorcPesoSem6L) AS STDPorcPesoSem6L,
    MAX(STDPorcPesoSem7L) AS STDPorcPesoSem7L,
    MAX(STDPorcPesoSem8L) AS STDPorcPesoSem8L,
    MAX(STDPorcPesoSem9L) AS STDPorcPesoSem9L,
    MAX(STDPorcPesoSem10L) AS STDPorcPesoSem10L,
    MAX(STDPorcPesoSem11L) AS STDPorcPesoSem11L,
    MAX(STDPorcPesoSem12L) AS STDPorcPesoSem12L,
    MAX(STDPorcPesoSem13L) AS STDPorcPesoSem13L,
    MAX(STDPorcPesoSem14L) AS STDPorcPesoSem14L,
    MAX(STDPorcPesoSem15L) AS STDPorcPesoSem15L,
    MAX(STDPorcPesoSem16L) AS STDPorcPesoSem16L,
    MAX(STDPorcPesoSem17L) AS STDPorcPesoSem17L,
    MAX(STDPorcPesoSem18L) AS STDPorcPesoSem18L,
    MAX(STDPorcPesoSem19L) AS STDPorcPesoSem19L,
    MAX(STDPorcPesoSem20L) AS STDPorcPesoSem20L,
    MAX(STDPorcPeso5DiasL) AS STDPorcPeso5DiasL
FROM
    PonderedPeso
GROUP BY
    pk_plantel,
    pk_lote,
    pk_galpon,
    pk_sexo,
    ComplexEntityNo
ORDER BY
    ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPesoPond"
}
df_STDPesoPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPesoPond")
print('carga STDPesoPond', df_STDPesoPond.count())
#Se muestra el DescripTipoAlimentoXTipoProducto
df_DescripTipoAlimentoXTipoProducto = spark.sql(f"""select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, Upper(B.grupoproducto) as grupoproducto, 
CASE WHEN A.pk_diasvida <= 9 THEN 'PREINICIO' 
WHEN (A.pk_diasvida >= 10 AND  A.pk_diasvida <= 19) THEN 'INICIO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 33) AND Upper(B.grupoproducto) = 'VIVO' THEN 'ACABADO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 29) AND Upper(B.grupoproducto) = 'BENEFICIO' THEN 'ACABADO' 
WHEN  A.pk_diasvida >= 34 AND Upper(B.grupoproducto) = 'VIVO' THEN 'FINALIZADOR' 
WHEN  A.pk_diasvida >= 30 AND Upper(B.grupoproducto) = 'BENEFICIO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto 
from {database_name_gl}.stg_ProduccionDetalle A 
left join {database_name_gl}.lk_producto B on A.pk_producto = B.pk_producto 
left join {database_name_gl}.lk_standard C on A.pk_standard = C.pk_standard 
where A.pk_empresa = 1 and A.pk_division = 4 and A.GRN = 'P' and Upper(C.nstandard) not in ('DINUT - VERANO', 'DINUT - INVIERNO') 
and date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') 
union 
select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, Upper(B.grupoproducto) as grupoproducto, 
CASE WHEN A.pk_diasvida <= 9 THEN 'PREINICIO' 
WHEN (A.pk_diasvida >= 10  AND A.pk_diasvida <= 18) THEN 'INICIO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 35) AND Upper(B.grupoproducto) = 'VIVO' AND Upper(C.nstandard) = 'DINUT - VERANO' THEN 'ACABADO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 33) AND Upper(B.grupoproducto) = 'VIVO' AND Upper(C.nstandard) = 'DINUT - INVIERNO' THEN 'ACABADO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 31) AND Upper(B.grupoproducto) = 'BENEFICIO' AND Upper(C.nstandard) = 'DINUT - VERANO' THEN 'ACABADO' 
WHEN (A.pk_diasvida >= 20 AND A.pk_diasvida <= 30) AND Upper(B.grupoproducto) = 'BENEFICIO' AND Upper(C.nstandard) = 'DINUT - INVIERNO' THEN 'ACABADO' 
WHEN (A.pk_diasvida >= 36 AND A.pk_diasvida <= 43) AND Upper(B.grupoproducto) = 'VIVO' AND Upper(C.nstandard) = 'DINUT - VERANO' THEN 'FINALIZADOR' 
WHEN (A.pk_diasvida >= 34 AND A.pk_diasvida <= 41) AND Upper(B.grupoproducto) = 'VIVO' AND Upper(C.nstandard) = 'DINUT - INVIERNO' THEN 'FINALIZADOR' 
WHEN (A.pk_diasvida >= 32 AND A.pk_diasvida <= 39) AND Upper(B.grupoproducto) = 'BENEFICIO' AND Upper(C.nstandard) = 'DINUT - VERANO' THEN 'FINALIZADOR' 
WHEN (A.pk_diasvida >= 31 AND A.pk_diasvida <= 38) AND Upper(B.grupoproducto) = 'BENEFICIO' AND Upper(C.nstandard) = 'DINUT - INVIERNO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto 
from {database_name_gl}.stg_ProduccionDetalle A 
left join {database_name_gl}.lk_producto B on A.pk_producto = B.pk_producto 
left join {database_name_gl}.lk_standard C on A.pk_standard = C.pk_standard 
where A.pk_empresa = 1 and A.pk_division = 4 and A.GRN = 'P' and Upper(C.nstandard) in ('DINUT - VERANO', 'DINUT - INVIERNO') and C.SpeciesType = '1' 
and date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DescripTipoAlimentoXTipoProducto"
}
df_DescripTipoAlimentoXTipoProducto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.DescripTipoAlimentoXTipoProducto")
print('carga DescripTipoAlimentoXTipoProducto', df_DescripTipoAlimentoXTipoProducto.count())
#Se muestra el STDConsDiaNuevo
df_STDConsDiaNuevo = spark.sql(f"""select A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto, SUM(A.STDConsDia) STDConsDia,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3)) ComplexEntityNoGalpon 
from ( select A.ComplexEntityNo, A.pk_diasvida, MAX(A.STDConsDia) STDConsDia 
from {database_name_gl}.stg_ProduccionDetalle A 
where A.ComplexEntityNo in (select ComplexEntityNo from {database_name_gl}.ft_alojamiento where participacionhm <> 0) and pk_empresa = 1 and pk_division = 4 and GRN = 'P' 
and date_format(a.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
group by A.ComplexEntityNo, A.pk_diasvida) A 
left join {database_name_tmp}.DescripTipoAlimentoXTipoProducto B on A.ComplexEntityNo = B.ComplexEntityNo and A.pk_diasvida = B.pk_diasvida 
group by A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3))""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDConsDiaNuevo"
}
df_STDConsDiaNuevo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDConsDiaNuevo")
print('carga STDConsDiaNuevo', df_STDConsDiaNuevo.count())
#Inserta los datos en la tabla _Produccion
df_Produccion = spark.sql(f" SELECT PD.pk_tiempo \
        ,PD.descripfecha fecha \
		,PD.pk_empresa \
		,COALESCE(PD.pk_division,1) pk_division \
		,COALESCE(PD.pk_zona,1) pk_zona \
		,COALESCE(PD.pk_subzona,1) pk_subzona \
		,COALESCE(PD.pk_plantel,1) pk_plantel \
		,COALESCE(PD.pk_lote,1) pk_lote \
		,COALESCE(PD.pk_galpon,1) pk_galpon \
		,COALESCE(PD.pk_sexo,1) pk_sexo \
		,COALESCE(PD.pk_standard,1) pk_standard \
		,COALESCE(PD.pk_producto,1) pk_producto \
		,PD.pk_tipoproducto \
		,COALESCE(PD.pk_grupoconsumo,1) pk_grupoconsumo \
		,COALESCE(LEP.pk_especie,1)pk_especie \
		,PD.pk_estado \
		,COALESCE(PD.pk_administrador,1) pk_administrador \
		,COALESCE(PD.pk_proveedor,1) pk_proveedor \
		,PD.pk_semanavida \
		,PD.pk_diasvida \
		,COALESCE(PD.pk_alimento,1) pk_alimento \
		,PD.ComplexEntityNo \
		,PD.FechaNacimiento \
		,PD.Edad \
		,PD.FechaCierre \
		,IC.descripFecha FechaAlojamiento \
		,DATE_FORMAT(DC.FechaCrianza, 'yyyyMMdd') AS FechaCrianza \
		,DATE_FORMAT(DC.FechaInicioGranja, 'yyyyMMdd') AS FechaInicioGranja \
		,PD.FechaInicioSaca \
		,PD.FechaFinSaca \
		,PD.AreaGalpon \
		,IC.Inventario PobInicial \
		,ASA.AvesRendidas \
		,ASA.AvesRendidasAcum \
		,ASA.kilosRendidos \
		,ASA.kilosRendidosAcum \
		,MD.MortDia AS MortDia  \
		,MD.MortDiaAcum AS MortAcum \
		,MD.MortSem AS MortSem \
		,MD.PorcMortDia  \
		,MD.PorcMortDiaAcum AS PorcMortAcum \
		,MD.PorcMortSem  \
		,MD.MortSem1 \
		,MD.MortSem2 \
		,MD.MortSem3 \
		,MD.MortSem4 \
		,MD.MortSem5 \
		,MD.MortSem6 \
		,MD.MortSem7 \
		,MD.MortSem8 \
		,MD.MortSem9 \
		,MD.MortSem10 \
		,MD.MortSem11 \
		,MD.MortSem12 \
		,MD.MortSem13 \
		,MD.MortSem14 \
		,MD.MortSem15 \
		,MD.MortSem16 \
		,MD.MortSem17 \
		,MD.MortSem18 \
		,MD.MortSem19 \
		,MD.MortSem20 \
		,MD.MortSemAcum1 \
		,MD.MortSemAcum2 \
		,MD.MortSemAcum3 \
		,MD.MortSemAcum4 \
		,MD.MortSemAcum5 \
		,MD.MortSemAcum6 \
		,MD.MortSemAcum7 \
		,MD.MortSemAcum8 \
		,MD.MortSemAcum9 \
		,MD.MortSemAcum10 \
		,MD.MortSemAcum11 \
		,MD.MortSemAcum12 \
		,MD.MortSemAcum13 \
		,MD.MortSemAcum14 \
		,MD.MortSemAcum15 \
		,MD.MortSemAcum16 \
		,MD.MortSemAcum17 \
		,MD.MortSemAcum18 \
		,MD.MortSemAcum19 \
		,MD.MortSemAcum20 \
		,PED.stdpeso AS Peso_STD \
		,PED.PesoGananciaDiaAcum AS PesoDia \
		,PED.Peso \
		,PED.PesoSem \
		,PED.PesoSem1 \
		,PED.PesoSem2 \
		,PED.PesoSem3 \
		,PED.PesoSem4 \
		,PED.PesoSem5 \
		,PED.PesoSem6 \
		,PED.PesoSem7 \
		,PED.PesoSem8 \
		,PED.PesoSem9 \
		,PED.PesoSem10 \
		,PED.PesoSem11 \
		,PED.PesoSem12 \
		,PED.PesoSem13 \
		,PED.PesoSem14 \
		,PED.PesoSem15 \
		,PED.PesoSem16 \
		,PED.PesoSem17 \
		,PED.PesoSem18 \
		,PED.PesoSem19 \
		,PED.PesoSem20 \
		,PED.Peso5Dias \
		,PED.PesoAlo \
		,PED.pesohvo as PesoHvo \
		,PED.DifPesoActPesoAnt \
		,PED.CantDia \
		,PED.GananciaPesoDia \
		,PD.UnidSeleccion \
		,PD.ConsDia \
		,CXTA.ConsDiaAcum ConsAcum \
		,CXTA.PreInicio \
		,CXTA.Inicio \
		,CXTA.Acabado \
		,CXTA.Terminado \
		,CXTA.Finalizador \
		,CXTA.PavoIni \
		,CXTA.Pavo1 \
		,CXTA.Pavo2 \
		,CXTA.Pavo3 \
		,CXTA.Pavo4 \
		,CXTA.Pavo5 \
		,CXTA.Pavo6 \
		,CXTA.Pavo7 \
		,CXTA.ConsSem1 \
		,CXTA.ConsSem2 \
		,CXTA.ConsSem3 \
		,CXTA.ConsSem4 \
		,CXTA.ConsSem5 \
		,CXTA.ConsSem6 \
		,CXTA.ConsSem7 \
		,CXTA.ConsSem8 \
		,CXTA.ConsSem9 \
		,CXTA.ConsSem10 \
		,CXTA.ConsSem11 \
		,CXTA.ConsSem12 \
		,CXTA.ConsSem13 \
		,CXTA.ConsSem14 \
		,CXTA.ConsSem15 \
		,CXTA.ConsSem16 \
		,CXTA.ConsSem17 \
		,CXTA.ConsSem18 \
		,CXTA.ConsSem19 \
		,CXTA.ConsSem20 \
		,0 AS Ganancia \
		,MD.STDPorcMortDia AS STDMortDia \
		,MD.STDPorcMortDiaAcum AS STDMortAcum \
		,PD.STDConsDia \
		,PD.STDConsAcum \
		,PED.STDPeso \
		,PD.U_WeightGainDay AS STDGanancia \
		,PD.U_FeedConversionBC AS STDICA \
		,VE.avesgranjaacum AS CantInicioSaca \
		,VE.edadiniciosaca \
		,VE.edadgranja AS EdadGranjaCorral \
		,VE.edadgalpon AS EdadGranjaGalpon \
		,VE.edadlote AS EdadGranjaLote \
		,CS.gas \
		,CS.cama \
		,CS.agua \
		,COALESCE(PD.U_PEAccidentados,0) AS U_PEAccidentados \
		,COALESCE(PD.U_PEHigadoGraso,0) AS U_PEHigadoGraso \
		,COALESCE(PD.U_PEHepatomegalia,0) AS U_PEHepatomegalia \
		,COALESCE(PD.U_PEHigadoHemorragico,0) AS U_PEHigadoHemorragico \
		,COALESCE(PD.U_PEInanicion,0) AS U_PEInanicion \
		,COALESCE(PD.U_PEProblemaRespiratorio,0) AS U_PEProblemaRespiratorio \
		,COALESCE(PD.U_PESCH,0) AS U_PESCH \
		,COALESCE(PD.U_PEEnteritis,0) AS U_PEEnteritis \
		,COALESCE(PD.U_PEAscitis,0) AS U_PEAscitis \
		,COALESCE(PD.U_PEMuerteSubita,0) AS U_PEMuerteSubita \
		,COALESCE(PD.U_PEEstresPorCalor,0) AS U_PEEstresPorCalor \
		,COALESCE(PD.U_PEHidropericardio,0) AS U_PEHidropericardio \
		,COALESCE(PD.U_PEHemopericardio,0) AS U_PEHemopericardio \
		,COALESCE(PD.U_PEUratosis,0) AS U_PEUratosis \
		,COALESCE(PD.U_PEMaterialCaseoso,0) AS U_PEMaterialCaseoso \
		,COALESCE(PD.U_PEOnfalitis,0) AS U_PEOnfalitis \
		,COALESCE(PD.U_PERetencionDeYema,0) AS U_PERetencionDeYema \
		,COALESCE(PD.U_PEErosionDeMolleja,0) AS U_PEErosionDeMolleja \
		,COALESCE(PD.U_PEHemorragiaMusculos,0) AS U_PEHemorragiaMusculos \
		,COALESCE(PD.U_PESangreEnCiego,0) AS U_PESangreEnCiego \
		,COALESCE(PD.U_PEPericarditis,0) AS U_PEPericarditis \
		,COALESCE(PD.U_PEPeritonitis,0) AS U_PEPeritonitis \
		,COALESCE(PD.U_PEProlapso,0) AS U_PEProlapso \
		,COALESCE(PD.U_PEPicaje,0) AS U_PEPicaje \
		,COALESCE(PD.U_PERupturaAortica,0) AS U_PERupturaAortica \
		,COALESCE(PD.U_PEBazoMoteado,0) AS U_PEBazoMoteado \
		,COALESCE(PD.U_PENoViable,0) AS U_PENoViable \
		,COALESCE(PD.Pigmentacion,0) AS Pigmentacion \
		,PD.brimfieldtransirn AS IRN \
		,AM.PadreMayor \
		,AM.RazaMayor \
		,AM.IncubadoraMayor \
		,AM.PorcAlojPadreMayor as PorcPadreMayor \
		,AM.PorcRazaMayor \
		,AM.PorcIncMayor \
		,U_categoria categoria \
		,PD.FlagAtipico \
		,EP.EdadPadreCorral \
		,EP.EdadPadreGalpon \
		,EP.EdadPadreLote \
		,STDP.STDPorcMortAcumC \
		,STDP.STDPorcMortAcumG \
		,STDP.STDPorcMortAcumL \
		,STDP.STDPesoC \
		,STDP.STDPesoG \
		,STDP.STDPesoL \
		,STDP.STDConsAcumC \
		,STDP.STDConsAcumG \
		,STDP.STDConsAcumL \
		,STDMP.STDPorcMortSem1 \
		,STDMP.STDPorcMortSem2 \
		,STDMP.STDPorcMortSem3 \
		,STDMP.STDPorcMortSem4 \
		,STDMP.STDPorcMortSem5 \
		,STDMP.STDPorcMortSem6 \
		,STDMP.STDPorcMortSem7 \
		,STDMP.STDPorcMortSem8 \
		,STDMP.STDPorcMortSem9 \
		,STDMP.STDPorcMortSem10 \
		,STDMP.STDPorcMortSem11 \
		,STDMP.STDPorcMortSem12 \
		,STDMP.STDPorcMortSem13 \
		,STDMP.STDPorcMortSem14 \
		,STDMP.STDPorcMortSem15 \
		,STDMP.STDPorcMortSem16 \
		,STDMP.STDPorcMortSem17 \
		,STDMP.STDPorcMortSem18 \
		,STDMP.STDPorcMortSem19 \
		,STDMP.STDPorcMortSem20 \
		,STDMP.STDPorcMortSemAcum1 \
		,STDMP.STDPorcMortSemAcum2 \
		,STDMP.STDPorcMortSemAcum3 \
		,STDMP.STDPorcMortSemAcum4 \
		,STDMP.STDPorcMortSemAcum5 \
		,STDMP.STDPorcMortSemAcum6 \
		,STDMP.STDPorcMortSemAcum7 \
		,STDMP.STDPorcMortSemAcum8 \
		,STDMP.STDPorcMortSemAcum9 \
		,STDMP.STDPorcMortSemAcum10 \
		,STDMP.STDPorcMortSemAcum11 \
		,STDMP.STDPorcMortSemAcum12 \
		,STDMP.STDPorcMortSemAcum13 \
		,STDMP.STDPorcMortSemAcum14 \
		,STDMP.STDPorcMortSemAcum15 \
		,STDMP.STDPorcMortSemAcum16 \
		,STDMP.STDPorcMortSemAcum17 \
		,STDMP.STDPorcMortSemAcum18 \
		,STDMP.STDPorcMortSemAcum19 \
		,STDMP.STDPorcMortSemAcum20 \
		,STDMP.STDPorcMortSem1G \
		,STDMP.STDPorcMortSem2G \
		,STDMP.STDPorcMortSem3G \
		,STDMP.STDPorcMortSem4G \
		,STDMP.STDPorcMortSem5G \
		,STDMP.STDPorcMortSem6G \
		,STDMP.STDPorcMortSem7G \
		,STDMP.STDPorcMortSem8G \
		,STDMP.STDPorcMortSem9G \
		,STDMP.STDPorcMortSem10G \
		,STDMP.STDPorcMortSem11G \
		,STDMP.STDPorcMortSem12G \
		,STDMP.STDPorcMortSem13G \
		,STDMP.STDPorcMortSem14G \
		,STDMP.STDPorcMortSem15G \
		,STDMP.STDPorcMortSem16G \
		,STDMP.STDPorcMortSem17G \
		,STDMP.STDPorcMortSem18G \
		,STDMP.STDPorcMortSem19G \
		,STDMP.STDPorcMortSem20G \
		,STDMP.STDPorcMortSemAcum1G \
		,STDMP.STDPorcMortSemAcum2G \
		,STDMP.STDPorcMortSemAcum3G \
		,STDMP.STDPorcMortSemAcum4G \
		,STDMP.STDPorcMortSemAcum5G \
		,STDMP.STDPorcMortSemAcum6G \
		,STDMP.STDPorcMortSemAcum7G \
		,STDMP.STDPorcMortSemAcum8G \
		,STDMP.STDPorcMortSemAcum9G \
		,STDMP.STDPorcMortSemAcum10G \
		,STDMP.STDPorcMortSemAcum11G \
		,STDMP.STDPorcMortSemAcum12G \
		,STDMP.STDPorcMortSemAcum13G \
		,STDMP.STDPorcMortSemAcum14G \
		,STDMP.STDPorcMortSemAcum15G \
		,STDMP.STDPorcMortSemAcum16G \
		,STDMP.STDPorcMortSemAcum17G \
		,STDMP.STDPorcMortSemAcum18G \
		,STDMP.STDPorcMortSemAcum19G \
		,STDMP.STDPorcMortSemAcum20G \
		,STDMP.STDPorcMortSem1L \
		,STDMP.STDPorcMortSem2L \
		,STDMP.STDPorcMortSem3L \
		,STDMP.STDPorcMortSem4L \
		,STDMP.STDPorcMortSem5L \
		,STDMP.STDPorcMortSem6L \
		,STDMP.STDPorcMortSem7L \
		,STDMP.STDPorcMortSem8L \
		,STDMP.STDPorcMortSem9L \
		,STDMP.STDPorcMortSem10L \
		,STDMP.STDPorcMortSem11L \
		,STDMP.STDPorcMortSem12L \
		,STDMP.STDPorcMortSem13L \
		,STDMP.STDPorcMortSem14L \
		,STDMP.STDPorcMortSem15L \
		,STDMP.STDPorcMortSem16L \
		,STDMP.STDPorcMortSem17L \
		,STDMP.STDPorcMortSem18L \
		,STDMP.STDPorcMortSem19L \
		,STDMP.STDPorcMortSem20L \
		,STDMP.STDPorcMortSemAcum1L \
		,STDMP.STDPorcMortSemAcum2L \
		,STDMP.STDPorcMortSemAcum3L \
		,STDMP.STDPorcMortSemAcum4L \
		,STDMP.STDPorcMortSemAcum5L \
		,STDMP.STDPorcMortSemAcum6L \
		,STDMP.STDPorcMortSemAcum7L \
		,STDMP.STDPorcMortSemAcum8L \
		,STDMP.STDPorcMortSemAcum9L \
		,STDMP.STDPorcMortSemAcum10L \
		,STDMP.STDPorcMortSemAcum11L \
		,STDMP.STDPorcMortSemAcum12L \
		,STDMP.STDPorcMortSemAcum13L \
		,STDMP.STDPorcMortSemAcum14L \
		,STDMP.STDPorcMortSemAcum15L \
		,STDMP.STDPorcMortSemAcum16L \
		,STDMP.STDPorcMortSemAcum17L \
		,STDMP.STDPorcMortSemAcum18L \
		,STDMP.STDPorcMortSemAcum19L \
		,STDMP.STDPorcMortSemAcum20L \
		,STDPP.STDPesoSem1 \
		,STDPP.STDPesoSem2 \
		,STDPP.STDPesoSem3 \
		,STDPP.STDPesoSem4 \
		,STDPP.STDPesoSem5 \
		,STDPP.STDPesoSem6 \
		,STDPP.STDPesoSem7 \
		,STDPP.STDPesoSem8 \
		,STDPP.STDPesoSem9 \
		,STDPP.STDPesoSem10 \
		,STDPP.STDPesoSem11 \
		,STDPP.STDPesoSem12 \
		,STDPP.STDPesoSem13 \
		,STDPP.STDPesoSem14 \
		,STDPP.STDPesoSem15 \
		,STDPP.STDPesoSem16 \
		,STDPP.STDPesoSem17 \
		,STDPP.STDPesoSem18 \
		,STDPP.STDPesoSem19 \
		,STDPP.STDPesoSem20 \
		,STDPP.STDPeso5Dias \
		,STDPP.STDPorcPesoSem1G \
		,STDPP.STDPorcPesoSem2G \
		,STDPP.STDPorcPesoSem3G \
		,STDPP.STDPorcPesoSem4G \
		,STDPP.STDPorcPesoSem5G \
		,STDPP.STDPorcPesoSem6G \
		,STDPP.STDPorcPesoSem7G \
		,STDPP.STDPorcPesoSem8G \
		,STDPP.STDPorcPesoSem9G \
		,STDPP.STDPorcPesoSem10G \
		,STDPP.STDPorcPesoSem11G \
		,STDPP.STDPorcPesoSem12G \
		,STDPP.STDPorcPesoSem13G \
		,STDPP.STDPorcPesoSem14G \
		,STDPP.STDPorcPesoSem15G \
		,STDPP.STDPorcPesoSem16G \
		,STDPP.STDPorcPesoSem17G \
		,STDPP.STDPorcPesoSem18G \
		,STDPP.STDPorcPesoSem19G \
		,STDPP.STDPorcPesoSem20G \
		,STDPP.STDPorcPeso5DiasG \
		,STDPP.STDPorcPesoSem1L \
		,STDPP.STDPorcPesoSem2L \
		,STDPP.STDPorcPesoSem3L \
		,STDPP.STDPorcPesoSem4L \
		,STDPP.STDPorcPesoSem5L \
		,STDPP.STDPorcPesoSem6L \
		,STDPP.STDPorcPesoSem7L \
		,STDPP.STDPorcPesoSem8L \
		,STDPP.STDPorcPesoSem9L \
		,STDPP.STDPorcPesoSem10L \
		,STDPP.STDPorcPesoSem11L \
		,STDPP.STDPorcPesoSem12L \
		,STDPP.STDPorcPesoSem13L \
		,STDPP.STDPorcPesoSem14L \
		,STDPP.STDPorcPesoSem15L \
		,STDPP.STDPorcPesoSem16L \
		,STDPP.STDPorcPesoSem17L \
		,STDPP.STDPorcPesoSem18L \
		,STDPP.STDPorcPesoSem19L \
		,STDPP.STDPorcPesoSem20L \
		,STDPP.STDPorcPeso5DiasL \
		,STDP.STDICAC \
		,STDP.STDICAG \
		,STDP.STDICAL \
		,COALESCE(PD.U_PEAerosaculitisG2,0) AS U_PEAerosaculitisG2 \
		,COALESCE(PD.U_PECojera,0) AS U_PECojera \
		,COALESCE(PD.U_PEHigadoIcterico,0) AS U_PEHigadoIcterico \
		,COALESCE(PD.U_PEMaterialCaseoso_po1ra,0) AS U_PEMaterialCaseoso_po1ra \
		,COALESCE(PD.U_PEMaterialCaseosoMedRetr,0) AS U_PEMaterialCaseosoMedRetr \
		,COALESCE(PD.U_PENecrosisHepatica,0) AS U_PENecrosisHepatica \
		,COALESCE(PD.U_PENeumonia,0) AS U_PENeumonia \
		,COALESCE(PD.U_PESepticemia,0) AS U_PESepticemia \
		,COALESCE(PD.U_PEVomitoNegro,0) AS U_PEVomitoNegro \
		,COALESCE(PD.U_PEAsperguillius,0) AS U_PEAsperguillius \
		,COALESCE(PD.U_PEBazoGrandeMot,0) AS U_PEBazoGrandeMot \
		,COALESCE(PD.U_PECorazonGrande,0) AS U_PECorazonGrande \
		,COALESCE(PD.U_PECuadroToxico,0) AS U_PECuadroToxico \
		,MD.PavosBBMortIncub \
		,AM.EdadPadreCorralDescrip \
		,PED.U_CausaPesoBajo \
		,PED.U_AccionPesoBajo \
		,AM.DiasAloj \
		,CXTA.PreinicioAcum \
		,CXTA.InicioAcum \
		,CXTA.AcabadoAcum \
		,CXTA.TerminadoAcum \
		,CXTA.FinalizadorAcum \
		,COALESCE(PD.U_RuidosTotales,0) U_RuidosTotales \
		,0 as ProductoConsumo \
		,VE.DiasSacaEfectivo \
		,PD.U_ConsumoGasVerano \
		,PD.U_ConsumoGasInvierno \
		,STDP.STDPorcConsGasInviernoC \
		,STDP.STDPorcConsGasInviernoG \
		,STDP.STDPorcConsGasInviernoL \
		,STDP.STDPorcConsGasVeranoC \
		,STDP.STDPorcConsGasVeranoG \
		,STDP.STDPorcConsGasVeranoL \
		,AM.PorcAlojamientoXEdadPadre \
		,MD.PorcMortDiaAcum \
		,MD.DifPorcMortDiaAcum_STDDiaAcum \
		,MD.TasaNoViable \
		,MD.SemPorcPriLesion \
		,MD.SemPorcSegLesion \
		,MD.SemPorcTerLesion \
		,DTA.DescripTipoAlimentoXTipoProducto \
		,STDCN.STDConsDia STDConsDiaXTipoAlimento \
		,AM.TipoOrigen \
		,PD.FormulaNo \
		,PD.FormulaName \
		,PD.FeedTypeNo \
		,PD.FeedTypeName \
		,MD.ListaFormulaNo \
		,MD.ListaFormulaName \
		,MD.MortConcatCorral \
		,MD.MortSemAntCorral \
		,MD.MortConcatLote \
		,MD.MortSemAntLote \
		,MD.MortConcatSemAnioLote \
		,PED.ConcatCorral PesoConcatCorral \
		,PED.PesoSemAntCorral \
		,PED.ConcatLote PesoConcatLote \
		,PED.PesoSemAntLote \
		,PED.ConcatSemAnioLote PesoConcatSemAnioLote \
		,MD.MortConcatSemAnioCorral \
		,PED.ConcatSemAnioCorral PesoConcatSemAnioCorral \
		,MD.NoViableSem1 \
		,MD.NoViableSem2 \
		,MD.NoViableSem3 \
		,MD.NoViableSem4 \
		,MD.NoViableSem5 \
		,MD.NoViableSem6 \
		,MD.NoViableSem7 \
		,MD.PorcNoViableSem1 \
		,MD.PorcNoViableSem2 \
		,MD.PorcNoViableSem3 \
		,MD.PorcNoViableSem4 \
		,MD.PorcNoViableSem5 \
		,MD.PorcNoViableSem6 \
		,MD.PorcNoViableSem7 \
		,MD.NoViableSem8 \
		,MD.PorcNoViableSem8 \
		,PD.idtipogranja as pk_tipogranja \
		,PED.GananciaPesoSem  \
		,PD.CV \
FROM {database_name_gl}.stg_ProduccionDetalle PD \
LEFT JOIN {database_name_gl}.ft_ingresocons IC ON PD.complexentityno = IC.complexentityno \
LEFT JOIN {database_name_gl}.ft_mortalidad_diario MD ON PD.ComplexEntityNo = MD.ComplexEntityNo AND PD.pk_diasvida = MD.pk_diasvida AND MD.flagartificio = 1 \
LEFT JOIN {database_name_gl}.ft_peso_diario PED ON PD.ComplexEntityNo = PED.ComplexEntityNo AND PD.pk_diasvida = PED.pk_diasvida \
LEFT JOIN {database_name_gl}.lk_alimento FAL ON PD.pk_alimento = FAL.pk_alimento \
LEFT JOIN (SELECT complexentityno, MIN(descripfecha) fecha, MIN(avesgranjaacum)avesgranjaacum, MIN(edadgranja)edadgranja,MIN(edadgalpon)edadgalpon, MIN(edadlote) edadlote, \
            MIN(edadiniciosaca) edadiniciosaca,COUNT(distinct descripfecha) DiasSacaEfectivo FROM {database_name_gl}.ft_ventas_CD where flagartificio=1 \
            GROUP BY complexentityno) VE ON PD.complexentityno = VE.complexentityno \
LEFT JOIN {database_name_gl}.stg_GasAguaCama CS ON CS.ComplexEntityNo = SUBSTRING(PD.ComplexEntityNo,1,9) \
LEFT JOIN {database_name_tmp}.DiasCampana DC ON CAST(DC.ProteinEntitiesIRN AS VARCHAR(50)) = CAST(PD.ProteinEntitiesIRN AS VARCHAR(50)) AND DC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.AlojamientoMayor AM ON PD.complexentityno = AM.complexentityno \
LEFT JOIN {database_name_tmp}.EdadPadre EP ON PD.complexentityno = EP.complexentityno \
LEFT JOIN {database_name_tmp}.STDPond STDP ON PD.pk_plantel = STDP.pk_plantel AND PD.pk_lote = STDP.pk_lote AND PD.pk_galpon = STDP.pk_galpon AND PD.pk_sexo =STDP.pk_sexo \
LEFT JOIN {database_name_tmp}.STDMortPond STDMP ON PD.pk_plantel = STDMP.pk_plantel AND PD.pk_lote = STDMP.pk_lote AND PD.pk_galpon = STDMP.pk_galpon AND PD.pk_sexo =STDMP.pk_sexo \
LEFT JOIN {database_name_tmp}.STDPesoPond STDPP ON PD.pk_plantel = STDPP.pk_plantel AND PD.pk_lote = STDPP.pk_lote AND PD.pk_galpon = STDPP.pk_galpon AND PD.pk_sexo =STDPP.pk_sexo \
LEFT JOIN {database_name_tmp}.acumuladoSaca ASA ON PD.ComplexEntityNo = ASA.ComplexEntityNo and PD.descripfecha = ASA.fecha \
LEFT JOIN {database_name_gl}.lk_especie LEP ON LEP.cespecie = AM.RazaMayor \
LEFT JOIN {database_name_tmp}.DescripTipoAlimentoXTipoProducto DTA on PD.ComplexEntityNo = DTA.ComplexEntityNo and PD.pk_diasvida = DTA.pk_diasvida \
LEFT JOIN {database_name_tmp}.STDConsDiaNuevo STDCN on substring(DTA.complexentityno,1,(LENGTH(DTA.complexentityno)-3)) = STDCN.ComplexEntityNoGalpon and \
    Upper(DTA.DescripTipoAlimentoXTipoProducto) = Upper(STDCN.DescripTipoAlimentoXTipoProducto) \
LEFT JOIN {database_name_tmp}.ConsumosXTipoAlimento CXTA ON PD.complexEntityNo = CXTA.ComplexEntityNo and PD.pk_diasvida = CXTA.pk_diasvida and PD.pk_alimento = CXTA.pk_alimento \
WHERE date_format(PD.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
AND PD.pk_diasvida >= 0 \
AND PD.GRN = 'P' AND PD.pk_division = 4 \
AND PD.pk_plantel NOT IN (21,181) \
AND SUBSTRING(PD.ComplexEntityNo,8,2) IN ('01','02','03','04','05','06','07','08','09','10') \
union all \
SELECT PD.pk_tiempo \
        ,PD.descripfecha fecha \
		,PD.pk_empresa \
		,COALESCE(PD.pk_division,1) pk_division \
		,COALESCE(PD.pk_zona,1) pk_zona \
		,COALESCE(PD.pk_subzona,1) pk_subzona \
		,COALESCE(PD.pk_plantel,1) pk_plantel \
		,COALESCE(PD.pk_lote,1) pk_lote \
		,COALESCE(PD.pk_galpon,1) pk_galpon \
		,COALESCE(PD.pk_sexo,1) pk_sexo \
		,COALESCE(PD.pk_standard,1) pk_standard \
		,COALESCE(PD.pk_producto,1) pk_producto \
		,PD.pk_tipoproducto \
		,COALESCE(PD.pk_grupoconsumo,1) pk_grupoconsumo \
		,COALESCE(PED.pk_especie,1) pk_especie \
		,PD.pk_estado \
		,COALESCE(PD.pk_administrador,1) pk_administrador \
		,COALESCE(PD.pk_proveedor,1) pk_proveedor \
		,PD.pk_semanavida \
		,PD.pk_diasvida \
		,COALESCE(PD.pk_alimento,1) pk_alimento \
		,PD.ComplexEntityNo \
		,PD.FechaNacimiento \
		,PD.Edad \
		,PD.FechaCierre \
		,IC.descripFecha FechaAlojamiento \
		,DATE_FORMAT(DC.FechaCrianza, 'yyyyMMdd') AS FechaCrianza \
		,DATE_FORMAT(DC.FechaInicioGranja, 'yyyyMMdd') AS FechaInicioGranja \
		,PD.FechaInicioSaca \
		,PD.FechaFinSaca \
		,PD.AreaGalpon \
		,IC.Inventario PobInicial \
		,ASA.AvesRendidas \
		,ASA.AvesRendidasAcum \
		,ASA.kilosRendidos \
		,ASA.kilosRendidosAcum \
		,MD.MortDia AS MortDia  \
		,MD.MortDiaAcum AS MortAcum \
		,MD.MortSem AS MortSem \
		,MD.PorcMortDia \
		,MD.PorcMortDiaAcum AS PorcMortAcum \
		,MD.PorcMortSem \
		,MD.MortSem1 \
		,MD.MortSem2 \
		,MD.MortSem3 \
		,MD.MortSem4 \
		,MD.MortSem5 \
		,MD.MortSem6 \
		,MD.MortSem7 \
		,MD.MortSem8 \
		,MD.MortSem9 \
		,MD.MortSem10 \
		,MD.MortSem11 \
		,MD.MortSem12 \
		,MD.MortSem13 \
		,MD.MortSem14 \
		,MD.MortSem15 \
		,MD.MortSem16 \
		,MD.MortSem17 \
		,MD.MortSem18 \
		,MD.MortSem19 \
		,MD.MortSem20 \
		,MD.MortSemAcum1 \
		,MD.MortSemAcum2 \
		,MD.MortSemAcum3 \
		,MD.MortSemAcum4 \
		,MD.MortSemAcum5 \
		,MD.MortSemAcum6 \
		,MD.MortSemAcum7 \
		,MD.MortSemAcum8 \
		,MD.MortSemAcum9 \
		,MD.MortSemAcum10 \
		,MD.MortSemAcum11 \
		,MD.MortSemAcum12 \
		,MD.MortSemAcum13 \
		,MD.MortSemAcum14 \
		,MD.MortSemAcum15 \
		,MD.MortSemAcum16 \
		,MD.MortSemAcum17 \
		,MD.MortSemAcum18 \
		,MD.MortSemAcum19 \
		,MD.MortSemAcum20 \
		,PED.stdpeso AS Peso_STD \
		,PED.PesoGananciaDiaAcum AS PesoDia \
		,PED.Peso \
		,PED.PesoSem \
		,PED.PesoSem1 \
		,PED.PesoSem2 \
		,PED.PesoSem3 \
		,PED.PesoSem4 \
		,PED.PesoSem5 \
		,PED.PesoSem6 \
		,PED.PesoSem7 \
		,PED.PesoSem8 \
		,PED.PesoSem9 \
		,PED.PesoSem10 \
		,PED.PesoSem11 \
		,PED.PesoSem12 \
		,PED.PesoSem13 \
		,PED.PesoSem14 \
		,PED.PesoSem15 \
		,PED.PesoSem16 \
		,PED.PesoSem17 \
		,PED.PesoSem18 \
		,PED.PesoSem19 \
		,PED.PesoSem20 \
		,PED.Peso5Dias \
		,PED.PesoAlo \
		,PED.pesohvo as PesoHvo \
		,PED.DifPesoActPesoAnt \
		,PED.CantDia \
		,PED.GananciaPesoDia \
		,PD.UnidSeleccion \
		,PD.ConsDia \
		,CXTA.ConsDiaAcum ConsAcum \
		,CXTA.PreInicio \
		,CXTA.Inicio \
		,CXTA.Acabado \
		,CXTA.Terminado \
		,CXTA.Finalizador \
		,CXTA.PavoIni \
		,CXTA.Pavo1 \
		,CXTA.Pavo2 \
		,CXTA.Pavo3 \
		,CXTA.Pavo4 \
		,CXTA.Pavo5 \
		,CXTA.Pavo6 \
		,CXTA.Pavo7 \
		,CXTA.ConsSem1 \
		,CXTA.ConsSem2 \
		,CXTA.ConsSem3 \
		,CXTA.ConsSem4 \
		,CXTA.ConsSem5 \
		,CXTA.ConsSem6 \
		,CXTA.ConsSem7 \
		,CXTA.ConsSem8 \
		,CXTA.ConsSem9 \
		,CXTA.ConsSem10 \
		,CXTA.ConsSem11 \
		,CXTA.ConsSem12 \
		,CXTA.ConsSem13 \
		,CXTA.ConsSem14 \
		,CXTA.ConsSem15 \
		,CXTA.ConsSem16 \
		,CXTA.ConsSem17 \
		,CXTA.ConsSem18 \
		,CXTA.ConsSem19 \
		,CXTA.ConsSem20 \
		,0 AS Ganancia \
		,MD.STDPorcMortDia AS STDMortDia \
		,MD.STDPorcMortDiaAcum AS STDMortAcum \
		,PD.STDConsDia \
		,PD.STDConsAcum \
		,PED.STDPeso \
		,PD.U_WeightGainDay AS STDGanancia \
		,PD.U_FeedConversionBC AS STDICA \
		,VE.avesgranjaacum AS CantInicioSaca \
		,VE.edadiniciosaca \
		,VE.edadgranja AS EdadGranjaCorral \
		,VE.edadgalpon AS EdadGranjaGalpon \
		,VE.edadlote AS EdadGranjaLote \
		,CS.gas \
		,CS.cama \
		,CS.agua \
		,COALESCE(PD.U_PEAccidentados,0) AS U_PEAccidentados \
		,COALESCE(PD.U_PEHigadoGraso,0) AS U_PEHigadoGraso \
		,COALESCE(PD.U_PEHepatomegalia,0) AS U_PEHepatomegalia \
		,COALESCE(PD.U_PEHigadoHemorragico,0) AS U_PEHigadoHemorragico \
		,COALESCE(PD.U_PEInanicion,0) AS U_PEInanicion \
		,COALESCE(PD.U_PEProblemaRespiratorio,0) AS U_PEProblemaRespiratorio \
		,COALESCE(PD.U_PESCH,0) AS U_PESCH \
		,COALESCE(PD.U_PEEnteritis,0) AS U_PEEnteritis \
		,COALESCE(PD.U_PEAscitis,0) AS U_PEAscitis \
		,COALESCE(PD.U_PEMuerteSubita,0) AS U_PEMuerteSubita \
		,COALESCE(PD.U_PEEstresPorCalor,0) AS U_PEEstresPorCalor \
		,COALESCE(PD.U_PEHidropericardio,0) AS U_PEHidropericardio \
		,COALESCE(PD.U_PEHemopericardio,0) AS U_PEHemopericardio \
		,COALESCE(PD.U_PEUratosis,0) AS U_PEUratosis \
		,COALESCE(PD.U_PEMaterialCaseoso,0) AS U_PEMaterialCaseoso \
		,COALESCE(PD.U_PEOnfalitis,0) AS U_PEOnfalitis \
		,COALESCE(PD.U_PERetencionDeYema,0) AS U_PERetencionDeYema \
		,COALESCE(PD.U_PEErosionDeMolleja,0) AS U_PEErosionDeMolleja \
		,COALESCE(PD.U_PEHemorragiaMusculos,0) AS U_PEHemorragiaMusculos \
		,COALESCE(PD.U_PESangreEnCiego,0) AS U_PESangreEnCiego \
		,COALESCE(PD.U_PEPericarditis,0) AS U_PEPericarditis \
		,COALESCE(PD.U_PEPeritonitis,0) AS U_PEPeritonitis \
		,COALESCE(PD.U_PEProlapso,0) AS U_PEProlapso \
		,COALESCE(PD.U_PEPicaje,0) AS U_PEPicaje \
		,COALESCE(PD.U_PERupturaAortica,0) AS U_PERupturaAortica \
		,COALESCE(PD.U_PEBazoMoteado,0) AS U_PEBazoMoteado \
		,COALESCE(PD.U_PENoViable,0) AS U_PENoViable \
		,COALESCE(PD.Pigmentacion,0) AS Pigmentacion \
		,PD.brimfieldtransirn AS IRN \
		,AM.PadreMayor \
		,AM.RazaMayor \
		,AM.IncubadoraMayor \
		,AM.PorcAlojPadreMayor as PorcPadreMayor \
		,AM.PorcRazaMayor \
		,AM.PorcIncMayor \
		,U_categoria categoria \
		,PD.FlagAtipico \
		,EP.EdadPadreCorral \
		,EP.EdadPadreGalpon \
		,EP.EdadPadreLote \
		,STDP.STDPorcMortAcumC \
		,STDP.STDPorcMortAcumG \
		,STDP.STDPorcMortAcumL \
		,STDP.STDPesoC \
		,STDP.STDPesoG \
		,STDP.STDPesoL \
		,STDP.STDConsAcumC \
		,STDP.STDConsAcumG \
		,STDP.STDConsAcumL \
		,STDMP.STDPorcMortSem1 \
		,STDMP.STDPorcMortSem2 \
		,STDMP.STDPorcMortSem3 \
		,STDMP.STDPorcMortSem4 \
		,STDMP.STDPorcMortSem5 \
		,STDMP.STDPorcMortSem6 \
		,STDMP.STDPorcMortSem7 \
		,STDMP.STDPorcMortSem8 \
		,STDMP.STDPorcMortSem9 \
		,STDMP.STDPorcMortSem10 \
		,STDMP.STDPorcMortSem11 \
		,STDMP.STDPorcMortSem12 \
		,STDMP.STDPorcMortSem13 \
		,STDMP.STDPorcMortSem14 \
		,STDMP.STDPorcMortSem15 \
		,STDMP.STDPorcMortSem16 \
		,STDMP.STDPorcMortSem17 \
		,STDMP.STDPorcMortSem18 \
		,STDMP.STDPorcMortSem19 \
		,STDMP.STDPorcMortSem20 \
		,STDMP.STDPorcMortSemAcum1 \
		,STDMP.STDPorcMortSemAcum2 \
		,STDMP.STDPorcMortSemAcum3 \
		,STDMP.STDPorcMortSemAcum4 \
		,STDMP.STDPorcMortSemAcum5 \
		,STDMP.STDPorcMortSemAcum6 \
		,STDMP.STDPorcMortSemAcum7 \
		,STDMP.STDPorcMortSemAcum8 \
		,STDMP.STDPorcMortSemAcum9 \
		,STDMP.STDPorcMortSemAcum10 \
		,STDMP.STDPorcMortSemAcum11 \
		,STDMP.STDPorcMortSemAcum12 \
		,STDMP.STDPorcMortSemAcum13 \
		,STDMP.STDPorcMortSemAcum14 \
		,STDMP.STDPorcMortSemAcum15 \
		,STDMP.STDPorcMortSemAcum16 \
		,STDMP.STDPorcMortSemAcum17 \
		,STDMP.STDPorcMortSemAcum18 \
		,STDMP.STDPorcMortSemAcum19 \
		,STDMP.STDPorcMortSemAcum20 \
		,STDMP.STDPorcMortSem1G \
		,STDMP.STDPorcMortSem2G \
		,STDMP.STDPorcMortSem3G \
		,STDMP.STDPorcMortSem4G \
		,STDMP.STDPorcMortSem5G \
		,STDMP.STDPorcMortSem6G \
		,STDMP.STDPorcMortSem7G \
		,STDMP.STDPorcMortSem8G \
		,STDMP.STDPorcMortSem9G \
		,STDMP.STDPorcMortSem10G \
		,STDMP.STDPorcMortSem11G \
		,STDMP.STDPorcMortSem12G \
		,STDMP.STDPorcMortSem13G \
		,STDMP.STDPorcMortSem14G \
		,STDMP.STDPorcMortSem15G \
		,STDMP.STDPorcMortSem16G \
		,STDMP.STDPorcMortSem17G \
		,STDMP.STDPorcMortSem18G \
		,STDMP.STDPorcMortSem19G \
		,STDMP.STDPorcMortSem20G \
		,STDMP.STDPorcMortSemAcum1G \
		,STDMP.STDPorcMortSemAcum2G \
		,STDMP.STDPorcMortSemAcum3G \
		,STDMP.STDPorcMortSemAcum4G \
		,STDMP.STDPorcMortSemAcum5G \
		,STDMP.STDPorcMortSemAcum6G \
		,STDMP.STDPorcMortSemAcum7G \
		,STDMP.STDPorcMortSemAcum8G \
		,STDMP.STDPorcMortSemAcum9G \
		,STDMP.STDPorcMortSemAcum10G \
		,STDMP.STDPorcMortSemAcum11G \
		,STDMP.STDPorcMortSemAcum12G \
		,STDMP.STDPorcMortSemAcum13G \
		,STDMP.STDPorcMortSemAcum14G \
		,STDMP.STDPorcMortSemAcum15G \
		,STDMP.STDPorcMortSemAcum16G \
		,STDMP.STDPorcMortSemAcum17G \
		,STDMP.STDPorcMortSemAcum18G \
		,STDMP.STDPorcMortSemAcum19G \
		,STDMP.STDPorcMortSemAcum20G \
		,STDMP.STDPorcMortSem1L \
		,STDMP.STDPorcMortSem2L \
		,STDMP.STDPorcMortSem3L \
		,STDMP.STDPorcMortSem4L \
		,STDMP.STDPorcMortSem5L \
		,STDMP.STDPorcMortSem6L \
		,STDMP.STDPorcMortSem7L \
		,STDMP.STDPorcMortSem8L \
		,STDMP.STDPorcMortSem9L \
		,STDMP.STDPorcMortSem10L \
		,STDMP.STDPorcMortSem11L \
		,STDMP.STDPorcMortSem12L \
		,STDMP.STDPorcMortSem13L \
		,STDMP.STDPorcMortSem14L \
		,STDMP.STDPorcMortSem15L \
		,STDMP.STDPorcMortSem16L \
		,STDMP.STDPorcMortSem17L \
		,STDMP.STDPorcMortSem18L \
		,STDMP.STDPorcMortSem19L \
		,STDMP.STDPorcMortSem20L \
		,STDMP.STDPorcMortSemAcum1L \
		,STDMP.STDPorcMortSemAcum2L \
		,STDMP.STDPorcMortSemAcum3L \
		,STDMP.STDPorcMortSemAcum4L \
		,STDMP.STDPorcMortSemAcum5L \
		,STDMP.STDPorcMortSemAcum6L \
		,STDMP.STDPorcMortSemAcum7L \
		,STDMP.STDPorcMortSemAcum8L \
		,STDMP.STDPorcMortSemAcum9L \
		,STDMP.STDPorcMortSemAcum10L \
		,STDMP.STDPorcMortSemAcum11L \
		,STDMP.STDPorcMortSemAcum12L \
		,STDMP.STDPorcMortSemAcum13L \
		,STDMP.STDPorcMortSemAcum14L \
		,STDMP.STDPorcMortSemAcum15L \
		,STDMP.STDPorcMortSemAcum16L \
		,STDMP.STDPorcMortSemAcum17L \
		,STDMP.STDPorcMortSemAcum18L \
		,STDMP.STDPorcMortSemAcum19L \
		,STDMP.STDPorcMortSemAcum20L \
		,STDPP.STDPesoSem1 \
		,STDPP.STDPesoSem2 \
		,STDPP.STDPesoSem3 \
		,STDPP.STDPesoSem4 \
		,STDPP.STDPesoSem5 \
		,STDPP.STDPesoSem6 \
		,STDPP.STDPesoSem7 \
		,STDPP.STDPesoSem8 \
		,STDPP.STDPesoSem9 \
		,STDPP.STDPesoSem10 \
		,STDPP.STDPesoSem11 \
		,STDPP.STDPesoSem12 \
		,STDPP.STDPesoSem13 \
		,STDPP.STDPesoSem14 \
		,STDPP.STDPesoSem15 \
		,STDPP.STDPesoSem16 \
		,STDPP.STDPesoSem17 \
		,STDPP.STDPesoSem18 \
		,STDPP.STDPesoSem19 \
		,STDPP.STDPesoSem20 \
		,STDPP.STDPeso5Dias \
		,STDPP.STDPorcPesoSem1G \
		,STDPP.STDPorcPesoSem2G \
		,STDPP.STDPorcPesoSem3G \
		,STDPP.STDPorcPesoSem4G \
		,STDPP.STDPorcPesoSem5G \
		,STDPP.STDPorcPesoSem6G \
		,STDPP.STDPorcPesoSem7G \
		,STDPP.STDPorcPesoSem8G \
		,STDPP.STDPorcPesoSem9G \
		,STDPP.STDPorcPesoSem10G \
		,STDPP.STDPorcPesoSem11G \
		,STDPP.STDPorcPesoSem12G \
		,STDPP.STDPorcPesoSem13G \
		,STDPP.STDPorcPesoSem14G \
		,STDPP.STDPorcPesoSem15G \
		,STDPP.STDPorcPesoSem16G \
		,STDPP.STDPorcPesoSem17G \
		,STDPP.STDPorcPesoSem18G \
		,STDPP.STDPorcPesoSem19G \
		,STDPP.STDPorcPesoSem20G \
		,STDPP.STDPorcPeso5DiasG \
		,STDPP.STDPorcPesoSem1L \
		,STDPP.STDPorcPesoSem2L \
		,STDPP.STDPorcPesoSem3L \
		,STDPP.STDPorcPesoSem4L \
		,STDPP.STDPorcPesoSem5L \
		,STDPP.STDPorcPesoSem6L \
		,STDPP.STDPorcPesoSem7L \
		,STDPP.STDPorcPesoSem8L \
		,STDPP.STDPorcPesoSem9L \
		,STDPP.STDPorcPesoSem10L \
		,STDPP.STDPorcPesoSem11L \
		,STDPP.STDPorcPesoSem12L \
		,STDPP.STDPorcPesoSem13L \
		,STDPP.STDPorcPesoSem14L \
		,STDPP.STDPorcPesoSem15L \
		,STDPP.STDPorcPesoSem16L \
		,STDPP.STDPorcPesoSem17L \
		,STDPP.STDPorcPesoSem18L \
		,STDPP.STDPorcPesoSem19L \
		,STDPP.STDPorcPesoSem20L \
		,STDPP.STDPorcPeso5DiasL \
		,STDP.STDICAC \
		,STDP.STDICAG \
		,STDP.STDICAL \
		,COALESCE(PD.U_PEAerosaculitisG2,0) AS U_PEAerosaculitisG2 \
		,COALESCE(PD.U_PECojera,0) AS U_PECojera \
		,COALESCE(PD.U_PEHigadoIcterico,0) AS U_PEHigadoIcterico \
		,COALESCE(PD.U_PEMaterialCaseoso_po1ra,0) AS U_PEMaterialCaseoso_po1ra \
		,COALESCE(PD.U_PEMaterialCaseosoMedRetr,0) AS U_PEMaterialCaseosoMedRetr \
		,COALESCE(PD.U_PENecrosisHepatica,0) AS U_PENecrosisHepatica \
		,COALESCE(PD.U_PENeumonia,0) AS U_PENeumonia \
		,COALESCE(PD.U_PESepticemia,0) AS U_PESepticemia \
		,COALESCE(PD.U_PEVomitoNegro,0) AS U_PEVomitoNegro \
		,COALESCE(PD.U_PEAsperguillius,0) AS U_PEAsperguillius \
		,COALESCE(PD.U_PEBazoGrandeMot,0) AS U_PEBazoGrandeMot \
		,COALESCE(PD.U_PECorazonGrande,0) AS U_PECorazonGrande \
		,COALESCE(PD.U_PECuadroToxico,0) AS U_PECuadroToxico \
		,MD.PavosBBMortIncub \
		,AM.EdadPadreCorralDescrip \
		,PED.U_CausaPesoBajo \
		,PED.U_AccionPesoBajo \
		,AM.DiasAloj \
		,0 as PreinicioAcum \
		,0 as InicioAcum \
		,0 as AcabadoAcum \
		,0 as TerminadoAcum \
		,0 as FinalizadorAcum \
		,COALESCE(PD.U_RuidosTotales,0) U_RuidosTotales \
		,null as ProductoConsumo \
		,VE.DiasSacaEfectivo \
		,PD.U_ConsumoGasVerano \
		,PD.U_ConsumoGasInvierno \
		,STDP.STDPorcConsGasInviernoC \
		,STDP.STDPorcConsGasInviernoG \
		,STDP.STDPorcConsGasInviernoL \
		,STDP.STDPorcConsGasVeranoC \
		,STDP.STDPorcConsGasVeranoG \
		,STDP.STDPorcConsGasVeranoL \
		,AM.PorcAlojamientoXEdadPadre \
		,MD.PorcMortDiaAcum  \
		,MD.DifPorcMortDiaAcum_STDDiaAcum \
		,MD.TasaNoViable \
		,MD.SemPorcPriLesion \
		,MD.SemPorcSegLesion \
		,MD.SemPorcTerLesion \
		,NULL DescripTipoAlimentoXTipoProducto \
		,NULL STDConsDiaXTipoAlimento \
		,AM.TipoOrigen \
		,PD.FormulaNo \
		,PD.FormulaName \
		,PD.FeedTypeNo \
		,PD.FeedTypeName \
		,MD.ListaFormulaNo \
		,MD.ListaFormulaName \
		,MD.MortConcatCorral \
		,MD.MortSemAntCorral \
		,MD.MortConcatLote \
		,MD.MortSemAntLote \
		,MD.MortConcatSemAnioLote \
		,PED.ConcatCorral PesoConcatCorral \
		,PED.PesoSemAntCorral \
		,PED.ConcatLote PesoConcatLote \
		,PED.PesoSemAntLote \
		,PED.ConcatSemAnioLote PesoConcatSemAnioLote \
		,MD.MortConcatSemAnioCorral \
		,PED.ConcatSemAnioCorral PesoConcatSemAnioCorral \
        ,NULL NoViableSem1 \
		,NULL NoViableSem2 \
		,NULL NoViableSem3 \
		,NULL NoViableSem4 \
		,NULL NoViableSem5 \
		,NULL NoViableSem6 \
		,NULL NoViableSem7 \
		,NULL PorcNoViableSem1 \
		,NULL PorcNoViableSem2 \
		,NULL PorcNoViableSem3 \
		,NULL PorcNoViableSem4 \
		,NULL PorcNoViableSem5 \
		,NULL PorcNoViableSem6 \
		,NULL PorcNoViableSem7 \
		,NULL NoViableSem8 \
		,NULL PorcNoViableSem8 \
		,PD.idtipogranja as pk_tipogranja \
		,PED.GananciaPesoSem \
		,CV \
FROM {database_name_gl}.stg_ProduccionDetalle PD \
LEFT JOIN {database_name_gl}.ft_ingresocons IC ON PD.complexentityno = IC.complexentityno \
LEFT JOIN {database_name_gl}.ft_mortalidad_Diario MD ON PD.ComplexEntityNo = MD.ComplexEntityNo AND PD.pk_diasvida = MD.pk_diasvida AND MD.flagartificio = 1 \
LEFT JOIN {database_name_gl}.ft_peso_Diario PED ON PD.ComplexEntityNo = PED.ComplexEntityNo AND PD.pk_diasvida = PED.pk_diasvida \
LEFT JOIN {database_name_gl}.lk_alimento FAL ON PD.pk_alimento = FAL.pk_alimento \
LEFT JOIN (SELECT complexentityno, MIN(descripfecha)fecha, MIN(avesgranjaacum)avesgranjaacum, MIN(edadgranja)edadgranja,MIN(edadgalpon)edadgalpon, MIN(edadlote) edadlote, \
            MIN(edadiniciosaca) edadiniciosaca,COUNT(distinct descripfecha) DiasSacaEfectivo FROM {database_name_gl}.ft_ventas_CD \
            where flagartificio=1 GROUP BY complexentityno ) VE ON PD.complexentityno = VE.complexentityno \
LEFT JOIN {database_name_gl}.stg_GasAguaCama CS ON CS.ComplexEntityNo = substring(PD.ComplexEntityNo,1,(LENGTH(PD.ComplexEntityNo)-6)) \
LEFT JOIN {database_name_tmp}.DiasCampana DC ON CAST(DC.ProteinEntitiesIRN AS VARCHAR(50)) = CAST(PD.ProteinEntitiesIRN AS VARCHAR(50)) AND DC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name_tmp}.AlojamientoMayor AM ON PD.complexentityno = AM.complexentityno \
LEFT JOIN {database_name_tmp}.EdadPadre EP ON PD.complexentityno = EP.complexentityno \
LEFT JOIN {database_name_tmp}.STDPond STDP ON PD.pk_plantel = STDP.pk_plantel AND PD.pk_lote = STDP.pk_lote AND PD.pk_galpon = STDP.pk_galpon AND PD.pk_sexo =STDP.pk_sexo \
LEFT JOIN {database_name_tmp}.STDMortPond STDMP ON PD.pk_plantel = STDMP.pk_plantel AND PD.pk_lote = STDMP.pk_lote AND PD.pk_galpon = STDMP.pk_galpon AND PD.pk_sexo =STDMP.pk_sexo \
LEFT JOIN {database_name_tmp}.STDPesoPond STDPP ON PD.pk_plantel = STDPP.pk_plantel AND PD.pk_lote = STDPP.pk_lote AND PD.pk_galpon = STDPP.pk_galpon AND PD.pk_sexo =STDPP.pk_sexo \
left join {database_name_tmp}.acumuladoSaca ASA ON PD.ComplexEntityNo = ASA.ComplexEntityNo and PD.descripfecha = ASA.fecha \
LEFT JOIN {database_name_tmp}.ConsumosXTipoAlimento CXTA ON PD.complexEntityNo = CXTA.ComplexEntityNo and PD.pk_diasvida = CXTA.pk_diasvida and PD.pk_alimento = CXTA.pk_alimento \
WHERE date_format(PD.descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM') \
AND PD.pk_diasvida >= 0 \
AND PD.GRN = 'P' \
AND PD.pk_division = 2")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Produccion"
}
df_Produccion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Produccion")
print('carga Produccion', df_Produccion.count())
df_UPD_Produccion = spark.sql(f"""select
 pk_tiempo           
,fecha               
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
,pk_grupoconsumo     
,pk_especie          
,pk_estado           
,pk_administrador    
,pk_proveedor        
,2 pk_semanavida       
,2 pk_diasvida         
,pk_alimento         
,complexentityno     
,fechanacimiento     
,edad                
,fechacierre         
,fechaalojamiento    
,fechacrianza        
,fechainiciogranja   
,fechainiciosaca     
,fechafinsaca        
,areagalpon          
,pobinicial          
,avesrendidas        
,avesrendidasacum    
,kilosrendidos       
,kilosrendidosacum   
,mortdia             
,mortacum            
,mortsem             
,porcmortdia         
,porcmortacum        
,porcmortsem         
,mortsem1            
,mortsem2            
,mortsem3            
,mortsem4            
,mortsem5            
,mortsem6            
,mortsem7            
,mortsem8            
,mortsem9            
,mortsem10           
,mortsem11           
,mortsem12           
,mortsem13           
,mortsem14           
,mortsem15           
,mortsem16           
,mortsem17           
,mortsem18           
,mortsem19           
,mortsem20           
,mortsemacum1        
,mortsemacum2        
,mortsemacum3        
,mortsemacum4        
,mortsemacum5        
,mortsemacum6        
,mortsemacum7        
,mortsemacum8        
,mortsemacum9        
,mortsemacum10       
,mortsemacum11       
,mortsemacum12       
,mortsemacum13       
,mortsemacum14       
,mortsemacum15       
,mortsemacum16       
,mortsemacum17       
,mortsemacum18       
,mortsemacum19       
,mortsemacum20       
,peso_std            
,pesodia             
,peso                
,pesosem             
,pesosem1            
,pesosem2            
,pesosem3            
,pesosem4            
,pesosem5            
,pesosem6            
,pesosem7            
,pesosem8            
,pesosem9            
,pesosem10           
,pesosem11           
,pesosem12           
,pesosem13           
,pesosem14           
,pesosem15           
,pesosem16           
,pesosem17           
,pesosem18           
,pesosem19           
,pesosem20           
,peso5dias           
,pesoalo             
,pesohvo             
,difpesoactpesoant   
,cantdia             
,gananciapesodia     
,unidseleccion       
,consdia             
,consacum            
,preinicio           
,inicio              
,acabado             
,terminado           
,finalizador         
,pavoini             
,pavo1               
,pavo2               
,pavo3               
,pavo4               
,pavo5               
,pavo6               
,pavo7               
,conssem1            
,conssem2            
,conssem3            
,conssem4            
,conssem5            
,conssem6            
,conssem7            
,conssem8            
,conssem9            
,conssem10           
,conssem11           
,conssem12           
,conssem13           
,conssem14           
,conssem15           
,conssem16           
,conssem17           
,conssem18           
,conssem19           
,conssem20           
,ganancia            
,stdmortdia          
,stdmortacum         
,stdconsdia          
,stdconsacum         
,stdpeso             
,stdganancia         
,stdica              
,cantiniciosaca      
,edadiniciosaca      
,edadgranjacorral    
,edadgranjagalpon    
,edadgranjalote      
,gas                 
,cama                
,agua                
,u_peaccidentados    
,u_pehigadograso     
,u_pehepatomegalia   
,u_pehigadohemorragico
,u_peinanicion       
,u_peproblemarespiratorio
,u_pesch             
,u_peenteritis       
,u_peascitis         
,u_pemuertesubita    
,u_peestresporcalor  
,u_pehidropericardio 
,u_pehemopericardio  
,u_peuratosis        
,u_pematerialcaseoso 
,u_peonfalitis       
,u_peretenciondeyema 
,u_peerosiondemolleja
,u_pehemorragiamusculos
,u_pesangreenciego   
,u_pepericarditis    
,u_peperitonitis     
,u_peprolapso        
,u_pepicaje          
,u_perupturaaortica  
,u_pebazomoteado     
,u_penoviable        
,pigmentacion        
,irn                 
,padremayor          
,razamayor           
,incubadoramayor     
,porcpadremayor      
,porcrazamayor       
,porcincmayor        
,categoria           
,flagatipico         
,edadpadrecorral     
,edadpadregalpon     
,edadpadrelote       
,stdporcmortacumc    
,stdporcmortacumg    
,stdporcmortacuml    
,stdpesoc            
,stdpesog            
,stdpesol            
,stdconsacumc        
,stdconsacumg        
,stdconsacuml        
,stdporcmortsem1     
,stdporcmortsem2     
,stdporcmortsem3     
,stdporcmortsem4     
,stdporcmortsem5     
,stdporcmortsem6     
,stdporcmortsem7     
,stdporcmortsem8     
,stdporcmortsem9     
,stdporcmortsem10    
,stdporcmortsem11    
,stdporcmortsem12    
,stdporcmortsem13    
,stdporcmortsem14    
,stdporcmortsem15    
,stdporcmortsem16    
,stdporcmortsem17    
,stdporcmortsem18    
,stdporcmortsem19    
,stdporcmortsem20    
,stdporcmortsemacum1 
,stdporcmortsemacum2 
,stdporcmortsemacum3 
,stdporcmortsemacum4 
,stdporcmortsemacum5 
,stdporcmortsemacum6 
,stdporcmortsemacum7 
,stdporcmortsemacum8 
,stdporcmortsemacum9 
,stdporcmortsemacum10
,stdporcmortsemacum11
,stdporcmortsemacum12
,stdporcmortsemacum13
,stdporcmortsemacum14
,stdporcmortsemacum15
,stdporcmortsemacum16
,stdporcmortsemacum17
,stdporcmortsemacum18
,stdporcmortsemacum19
,stdporcmortsemacum20
,stdporcmortsem1g    
,stdporcmortsem2g    
,stdporcmortsem3g    
,stdporcmortsem4g    
,stdporcmortsem5g    
,stdporcmortsem6g    
,stdporcmortsem7g    
,stdporcmortsem8g    
,stdporcmortsem9g    
,stdporcmortsem10g   
,stdporcmortsem11g   
,stdporcmortsem12g   
,stdporcmortsem13g   
,stdporcmortsem14g   
,stdporcmortsem15g   
,stdporcmortsem16g   
,stdporcmortsem17g   
,stdporcmortsem18g   
,stdporcmortsem19g   
,stdporcmortsem20g   
,stdporcmortsemacum1g
,stdporcmortsemacum2g
,stdporcmortsemacum3g
,stdporcmortsemacum4g
,stdporcmortsemacum5g
,stdporcmortsemacum6g
,stdporcmortsemacum7g
,stdporcmortsemacum8g
,stdporcmortsemacum9g
,stdporcmortsemacum10g
,stdporcmortsemacum11g
,stdporcmortsemacum12g
,stdporcmortsemacum13g
,stdporcmortsemacum14g
,stdporcmortsemacum15g
,stdporcmortsemacum16g
,stdporcmortsemacum17g
,stdporcmortsemacum18g
,stdporcmortsemacum19g
,stdporcmortsemacum20g
,stdporcmortsem1l    
,stdporcmortsem2l    
,stdporcmortsem3l    
,stdporcmortsem4l    
,stdporcmortsem5l    
,stdporcmortsem6l    
,stdporcmortsem7l    
,stdporcmortsem8l    
,stdporcmortsem9l    
,stdporcmortsem10l   
,stdporcmortsem11l   
,stdporcmortsem12l   
,stdporcmortsem13l   
,stdporcmortsem14l   
,stdporcmortsem15l   
,stdporcmortsem16l   
,stdporcmortsem17l   
,stdporcmortsem18l   
,stdporcmortsem19l   
,stdporcmortsem20l   
,stdporcmortsemacum1l
,stdporcmortsemacum2l
,stdporcmortsemacum3l
,stdporcmortsemacum4l
,stdporcmortsemacum5l
,stdporcmortsemacum6l
,stdporcmortsemacum7l
,stdporcmortsemacum8l
,stdporcmortsemacum9l
,stdporcmortsemacum10l
,stdporcmortsemacum11l
,stdporcmortsemacum12l
,stdporcmortsemacum13l
,stdporcmortsemacum14l
,stdporcmortsemacum15l
,stdporcmortsemacum16l
,stdporcmortsemacum17l
,stdporcmortsemacum18l
,stdporcmortsemacum19l
,stdporcmortsemacum20l
,stdpesosem1         
,stdpesosem2         
,stdpesosem3         
,stdpesosem4         
,stdpesosem5         
,stdpesosem6         
,stdpesosem7         
,stdpesosem8         
,stdpesosem9         
,stdpesosem10        
,stdpesosem11        
,stdpesosem12        
,stdpesosem13        
,stdpesosem14        
,stdpesosem15        
,stdpesosem16        
,stdpesosem17        
,stdpesosem18        
,stdpesosem19        
,stdpesosem20        
,stdpeso5dias        
,stdporcpesosem1g    
,stdporcpesosem2g    
,stdporcpesosem3g    
,stdporcpesosem4g    
,stdporcpesosem5g    
,stdporcpesosem6g    
,stdporcpesosem7g    
,stdporcpesosem8g    
,stdporcpesosem9g    
,stdporcpesosem10g   
,stdporcpesosem11g   
,stdporcpesosem12g   
,stdporcpesosem13g   
,stdporcpesosem14g   
,stdporcpesosem15g   
,stdporcpesosem16g   
,stdporcpesosem17g   
,stdporcpesosem18g   
,stdporcpesosem19g   
,stdporcpesosem20g   
,stdporcpeso5diasg   
,stdporcpesosem1l    
,stdporcpesosem2l    
,stdporcpesosem3l    
,stdporcpesosem4l    
,stdporcpesosem5l    
,stdporcpesosem6l    
,stdporcpesosem7l    
,stdporcpesosem8l    
,stdporcpesosem9l    
,stdporcpesosem10l   
,stdporcpesosem11l   
,stdporcpesosem12l   
,stdporcpesosem13l   
,stdporcpesosem14l   
,stdporcpesosem15l   
,stdporcpesosem16l   
,stdporcpesosem17l   
,stdporcpesosem18l   
,stdporcpesosem19l   
,stdporcpesosem20l   
,stdporcpeso5diasl   
,stdicac             
,stdicag             
,stdical             
,u_peaerosaculitisg2 
,u_pecojera          
,u_pehigadoicterico  
,u_pematerialcaseoso_po1ra
,u_pematerialcaseosomedretr
,u_penecrosishepatica
,u_peneumonia        
,u_pesepticemia      
,u_pevomitonegro     
,u_peasperguillius   
,u_pebazograndemot   
,u_pecorazongrande   
,u_pecuadrotoxico    
,pavosbbmortincub    
,edadpadrecorraldescrip
,u_causapesobajo     
,u_accionpesobajo    
,diasaloj            
,preinicioacum       
,inicioacum          
,acabadoacum         
,terminadoacum       
,finalizadoracum     
,u_ruidostotales     
,productoconsumo     
,diassacaefectivo    
,u_consumogasverano  
,u_consumogasinvierno
,stdporcconsgasinviernoc
,stdporcconsgasinviernog
,stdporcconsgasinviernol
,stdporcconsgasveranoc
,stdporcconsgasveranog
,stdporcconsgasveranol
,porcalojamientoxedadpadre
,porcmortdiaacum     
,difporcmortdiaacum_stddiaacum
,tasanoviable        
,semporcprilesion    
,semporcseglesion    
,semporcterlesion    
,descriptipoalimentoxtipoproducto
,stdconsdiaxtipoalimento
,tipoorigen          
,formulano           
,formulaname         
,feedtypeno          
,feedtypename        
,listaformulano      
,listaformulaname    
,mortconcatcorral    
,mortsemantcorral    
,mortconcatlote      
,mortsemantlote      
,mortconcatsemaniolote
,pesoconcatcorral    
,pesosemantcorral    
,pesoconcatlote      
,pesosemantlote      
,pesoconcatsemaniolote
,mortconcatsemaniocorral
,pesoconcatsemaniocorral
,noviablesem1        
,noviablesem2        
,noviablesem3        
,noviablesem4        
,noviablesem5        
,noviablesem6        
,noviablesem7        
,porcnoviablesem1    
,porcnoviablesem2    
,porcnoviablesem3    
,porcnoviablesem4    
,porcnoviablesem5    
,porcnoviablesem6    
,porcnoviablesem7    
,noviablesem8        
,porcnoviablesem8    
,pk_tipogranja       
,gananciapesosem     
,cv
from {database_name_tmp}.Produccion
where edad = 0
""")
print('carga df_UPD_Produccion' , df_UPD_Produccion.count())
#Se muestra el STDPorcConsDia
df_STDPorcConsDia = spark.sql(f"""select
 A.pk_semanavida
,A.pk_plantel
,A.pk_lote
,A.pk_galpon
,A.pk_sexo
,A.ComplexEntityNo
,MAX(AvesRendidas) AvesRendidas
,SUM(ConsDia) ConsSem
,SUM(STDPorcConsDia) STDPorcConsSem
,SUM(STDConsDia) STDConsSem
from(select A.pk_semanavida,A.pk_diasvida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(AvesRendidas) AvesRendidas
    ,SUM(ConsDia) ConsDia,CASE WHEN A.pk_diasvida<= 43 THEN MAX(STDConsDia) ELSE 0 END STDPorcConsDia
    ,((CASE WHEN A.pk_diasvida<= 43 THEN MAX(STDConsDia) ELSE 0 END)*MAX(AvesRendidas))STDConsDia
    from {database_name_tmp}.produccion A where A.pk_empresa = 1 
    group by A.pk_semanavida,A.pk_diasvida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo) A
group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo
""" )

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcConsDia"
}
df_STDPorcConsDia.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcConsDia")
print('carga STDPorcConsDia', df_STDPorcConsDia.count())
path_target1 = f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Produccion"
existing_data = spark.read.format("parquet").load(path_target1)
data_after_delete = existing_data.filter(~(col("edad")== 0))
filtered_new_data = df_UPD_Produccion.filter(col("edad")== 0)
final_data = filtered_new_data.union(data_after_delete)                           

cant_ingresonuevo = filtered_new_data.count()
cant_total = final_data.count()

# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal"
}
final_data.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionTemporal")

final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal")         
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Produccion"
}
final_data2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Produccion")

print(f"agrega registros nuevos a la tabla Produccion : {cant_ingresonuevo}")
print(f"Total de registros en la tabla Produccion : {cant_total}")
 #Limpia la ubicación temporal
glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
glue_client.delete_table(DatabaseName=database_name_tmp, Name='ProduccionTemporal')
print(f"Tabla ProduccionTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
#Se muestra el STDPorcConsDiaHorizontal
df_STDPorcConsDiaHorizontal = spark.sql(f"""
    SELECT
        A.pk_plantel,
        A.pk_lote,
        A.pk_galpon,
        A.pk_sexo,
        A.ComplexEntityNo,
        MAX(A.AvesRendidas) AS AvesRendidas,
        MAX(CASE WHEN AA.pk_semanavida = 2 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem1,
        MAX(CASE WHEN AA.pk_semanavida = 3 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem2,
        MAX(CASE WHEN AA.pk_semanavida = 4 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem3,
        MAX(CASE WHEN AA.pk_semanavida = 5 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem4,
        MAX(CASE WHEN AA.pk_semanavida = 6 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem5,
        MAX(CASE WHEN AA.pk_semanavida = 7 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem6,
        MAX(CASE WHEN AA.pk_semanavida = 8 THEN AA.STDPorcConsSem ELSE 0 END) AS STDPorcConsSem7,
        MAX(CASE WHEN AA.pk_semanavida = 2 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem1,
        MAX(CASE WHEN AA.pk_semanavida = 3 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem2,
        MAX(CASE WHEN AA.pk_semanavida = 4 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem3,
        MAX(CASE WHEN AA.pk_semanavida = 5 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem4,
        MAX(CASE WHEN AA.pk_semanavida = 6 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem5,
        MAX(CASE WHEN AA.pk_semanavida = 7 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem6,
        MAX(CASE WHEN AA.pk_semanavida = 8 THEN AA.STDConsSem ELSE 0 END) AS STDConsSem7
    FROM
        {database_name_tmp}.STDPorcConsDia A
    LEFT JOIN
        {database_name_tmp}.STDPorcConsDia AA ON A.ComplexEntityNo = AA.ComplexEntityNo AND AA.pk_semanavida BETWEEN 2 AND 8
    GROUP BY
        A.pk_plantel,
        A.pk_lote,
        A.pk_galpon,
        A.pk_sexo,
        A.ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcConsDiaHorizontal"
}
df_STDPorcConsDiaHorizontal.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDPorcConsDiaHorizontal")
print('carga STDPorcConsDiaHorizontal', df_STDPorcConsDiaHorizontal.count())
# 1️⃣ Crear DataFrame con datos actualizados
df_UPDATEproduccion = spark.sql(f"""
SELECT 
 A.pk_tiempo
,A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_galpon
,A.pk_sexo
,A.pk_standard
,A.pk_producto
,A.pk_tipoproducto
,A.pk_grupoconsumo
,A.pk_especie
,A.pk_estado
,A.pk_administrador
,A.pk_proveedor
,A.pk_semanavida
,A.pk_diasvida
,A.pk_alimento
,A.complexentityno
,A.fechanacimiento
,A.edad
,A.fechacierre
,A.fechaalojamiento
,A.fechacrianza
,A.fechainiciogranja
,A.fechainiciosaca
,A.fechafinsaca
,A.areagalpon
,A.pobinicial
,A.avesrendidas
,A.avesrendidasacum
,A.kilosrendidos
,A.kilosrendidosacum
,A.mortdia
,A.mortacum
,A.mortsem
,A.porcmortdia
,A.porcmortacum
,A.porcmortsem
,A.mortsem1
,A.mortsem2
,A.mortsem3
,A.mortsem4
,A.mortsem5
,A.mortsem6
,A.mortsem7
,A.mortsem8
,A.mortsem9
,A.mortsem10
,A.mortsem11
,A.mortsem12
,A.mortsem13
,A.mortsem14
,A.mortsem15
,A.mortsem16
,A.mortsem17
,A.mortsem18
,A.mortsem19
,A.mortsem20
,A.mortsemacum1
,A.mortsemacum2
,A.mortsemacum3
,A.mortsemacum4
,A.mortsemacum5
,A.mortsemacum6
,A.mortsemacum7
,A.mortsemacum8
,A.mortsemacum9
,A.mortsemacum10
,A.mortsemacum11
,A.mortsemacum12
,A.mortsemacum13
,A.mortsemacum14
,A.mortsemacum15
,A.mortsemacum16
,A.mortsemacum17
,A.mortsemacum18
,A.mortsemacum19
,A.mortsemacum20
,A.peso_std
,A.pesodia
,A.peso
,A.pesosem
,A.pesosem1
,A.pesosem2
,A.pesosem3
,A.pesosem4
,A.pesosem5
,A.pesosem6
,A.pesosem7
,A.pesosem8
,A.pesosem9
,A.pesosem10
,A.pesosem11
,A.pesosem12
,A.pesosem13
,A.pesosem14
,A.pesosem15
,A.pesosem16
,A.pesosem17
,A.pesosem18
,A.pesosem19
,A.pesosem20
,A.peso5dias
,A.pesoalo
,A.pesohvo
,A.difpesoactpesoant
,A.cantdia
,A.gananciapesodia
,A.unidseleccion
,A.consdia
,A.consacum
,A.preinicio
,A.inicio
,A.acabado
,A.terminado
,A.finalizador
,A.pavoini
,A.pavo1
,A.pavo2
,A.pavo3
,A.pavo4
,A.pavo5
,A.pavo6
,A.pavo7
,A.conssem1
,A.conssem2
,A.conssem3
,A.conssem4
,A.conssem5
,A.conssem6
,A.conssem7
,A.conssem8
,A.conssem9
,A.conssem10
,A.conssem11
,A.conssem12
,A.conssem13
,A.conssem14
,A.conssem15
,A.conssem16
,A.conssem17
,A.conssem18
,A.conssem19
,A.conssem20
,A.ganancia
,A.stdmortdia
,A.stdmortacum
,A.stdconsdia
,A.stdconsacum
,A.stdpeso
,A.stdganancia
,A.stdica
,A.cantiniciosaca
,A.edadiniciosaca
,A.edadgranjacorral
,A.edadgranjagalpon
,A.edadgranjalote
,A.gas
,A.cama
,A.agua
,A.u_peaccidentados
,A.u_pehigadograso
,A.u_pehepatomegalia
,A.u_pehigadohemorragico
,A.u_peinanicion
,A.u_peproblemarespiratorio
,A.u_pesch
,A.u_peenteritis
,A.u_peascitis
,A.u_pemuertesubita
,A.u_peestresporcalor
,A.u_pehidropericardio
,A.u_pehemopericardio
,A.u_peuratosis
,A.u_pematerialcaseoso
,A.u_peonfalitis
,A.u_peretenciondeyema
,A.u_peerosiondemolleja
,A.u_pehemorragiamusculos
,A.u_pesangreenciego
,A.u_pepericarditis
,A.u_peperitonitis
,A.u_peprolapso
,A.u_pepicaje
,A.u_perupturaaortica
,A.u_pebazomoteado
,A.u_penoviable
,A.pigmentacion
,A.irn
,A.padremayor
,A.razamayor
,A.incubadoramayor
,A.porcpadremayor
,A.porcrazamayor
,A.porcincmayor
,A.categoria
,A.flagatipico
,A.edadpadrecorral
,A.edadpadregalpon
,A.edadpadrelote
,A.stdporcmortacumc
,A.stdporcmortacumg
,A.stdporcmortacuml
,A.stdpesoc
,A.stdpesog
,A.stdpesol
,A.stdconsacumc
,A.stdconsacumg
,A.stdconsacuml
,A.stdporcmortsem1
,A.stdporcmortsem2
,A.stdporcmortsem3
,A.stdporcmortsem4
,A.stdporcmortsem5
,A.stdporcmortsem6
,A.stdporcmortsem7
,A.stdporcmortsem8
,A.stdporcmortsem9
,A.stdporcmortsem10
,A.stdporcmortsem11
,A.stdporcmortsem12
,A.stdporcmortsem13
,A.stdporcmortsem14
,A.stdporcmortsem15
,A.stdporcmortsem16
,A.stdporcmortsem17
,A.stdporcmortsem18
,A.stdporcmortsem19
,A.stdporcmortsem20
,A.stdporcmortsemacum1
,A.stdporcmortsemacum2
,A.stdporcmortsemacum3
,A.stdporcmortsemacum4
,A.stdporcmortsemacum5
,A.stdporcmortsemacum6
,A.stdporcmortsemacum7
,A.stdporcmortsemacum8
,A.stdporcmortsemacum9
,A.stdporcmortsemacum10
,A.stdporcmortsemacum11
,A.stdporcmortsemacum12
,A.stdporcmortsemacum13
,A.stdporcmortsemacum14
,A.stdporcmortsemacum15
,A.stdporcmortsemacum16
,A.stdporcmortsemacum17
,A.stdporcmortsemacum18
,A.stdporcmortsemacum19
,A.stdporcmortsemacum20
,A.stdporcmortsem1g
,A.stdporcmortsem2g
,A.stdporcmortsem3g
,A.stdporcmortsem4g
,A.stdporcmortsem5g
,A.stdporcmortsem6g
,A.stdporcmortsem7g
,A.stdporcmortsem8g
,A.stdporcmortsem9g
,A.stdporcmortsem10g
,A.stdporcmortsem11g
,A.stdporcmortsem12g
,A.stdporcmortsem13g
,A.stdporcmortsem14g
,A.stdporcmortsem15g
,A.stdporcmortsem16g
,A.stdporcmortsem17g
,A.stdporcmortsem18g
,A.stdporcmortsem19g
,A.stdporcmortsem20g
,A.stdporcmortsemacum1g
,A.stdporcmortsemacum2g
,A.stdporcmortsemacum3g
,A.stdporcmortsemacum4g
,A.stdporcmortsemacum5g
,A.stdporcmortsemacum6g
,A.stdporcmortsemacum7g
,A.stdporcmortsemacum8g
,A.stdporcmortsemacum9g
,A.stdporcmortsemacum10g
,A.stdporcmortsemacum11g
,A.stdporcmortsemacum12g
,A.stdporcmortsemacum13g
,A.stdporcmortsemacum14g
,A.stdporcmortsemacum15g
,A.stdporcmortsemacum16g
,A.stdporcmortsemacum17g
,A.stdporcmortsemacum18g
,A.stdporcmortsemacum19g
,A.stdporcmortsemacum20g
,A.stdporcmortsem1l
,A.stdporcmortsem2l
,A.stdporcmortsem3l
,A.stdporcmortsem4l
,A.stdporcmortsem5l
,A.stdporcmortsem6l
,A.stdporcmortsem7l
,A.stdporcmortsem8l
,A.stdporcmortsem9l
,A.stdporcmortsem10l
,A.stdporcmortsem11l
,A.stdporcmortsem12l
,A.stdporcmortsem13l
,A.stdporcmortsem14l
,A.stdporcmortsem15l
,A.stdporcmortsem16l
,A.stdporcmortsem17l
,A.stdporcmortsem18l
,A.stdporcmortsem19l
,A.stdporcmortsem20l
,A.stdporcmortsemacum1l
,A.stdporcmortsemacum2l
,A.stdporcmortsemacum3l
,A.stdporcmortsemacum4l
,A.stdporcmortsemacum5l
,A.stdporcmortsemacum6l
,A.stdporcmortsemacum7l
,A.stdporcmortsemacum8l
,A.stdporcmortsemacum9l
,A.stdporcmortsemacum10l
,A.stdporcmortsemacum11l
,A.stdporcmortsemacum12l
,A.stdporcmortsemacum13l
,A.stdporcmortsemacum14l
,A.stdporcmortsemacum15l
,A.stdporcmortsemacum16l
,A.stdporcmortsemacum17l
,A.stdporcmortsemacum18l
,A.stdporcmortsemacum19l
,A.stdporcmortsemacum20l
,A.stdpesosem1
,A.stdpesosem2
,A.stdpesosem3
,A.stdpesosem4
,A.stdpesosem5
,A.stdpesosem6
,A.stdpesosem7
,A.stdpesosem8
,A.stdpesosem9
,A.stdpesosem10
,A.stdpesosem11
,A.stdpesosem12
,A.stdpesosem13
,A.stdpesosem14
,A.stdpesosem15
,A.stdpesosem16
,A.stdpesosem17
,A.stdpesosem18
,A.stdpesosem19
,A.stdpesosem20
,A.stdpeso5dias
,A.stdporcpesosem1g
,A.stdporcpesosem2g
,A.stdporcpesosem3g
,A.stdporcpesosem4g
,A.stdporcpesosem5g
,A.stdporcpesosem6g
,A.stdporcpesosem7g
,A.stdporcpesosem8g
,A.stdporcpesosem9g
,A.stdporcpesosem10g
,A.stdporcpesosem11g
,A.stdporcpesosem12g
,A.stdporcpesosem13g
,A.stdporcpesosem14g
,A.stdporcpesosem15g
,A.stdporcpesosem16g
,A.stdporcpesosem17g
,A.stdporcpesosem18g
,A.stdporcpesosem19g
,A.stdporcpesosem20g
,A.stdporcpeso5diasg
,A.stdporcpesosem1l
,A.stdporcpesosem2l
,A.stdporcpesosem3l
,A.stdporcpesosem4l
,A.stdporcpesosem5l
,A.stdporcpesosem6l
,A.stdporcpesosem7l
,A.stdporcpesosem8l
,A.stdporcpesosem9l
,A.stdporcpesosem10l
,A.stdporcpesosem11l
,A.stdporcpesosem12l
,A.stdporcpesosem13l
,A.stdporcpesosem14l
,A.stdporcpesosem15l
,A.stdporcpesosem16l
,A.stdporcpesosem17l
,A.stdporcpesosem18l
,A.stdporcpesosem19l
,A.stdporcpesosem20l
,A.stdporcpeso5diasl
,A.stdicac
,A.stdicag
,A.stdical
,A.u_peaerosaculitisg2
,A.u_pecojera
,A.u_pehigadoicterico
,A.u_pematerialcaseoso_po1ra
,A.u_pematerialcaseosomedretr
,A.u_penecrosishepatica
,A.u_peneumonia
,A.u_pesepticemia
,A.u_pevomitonegro
,A.u_peasperguillius
,A.u_pebazograndemot
,A.u_pecorazongrande
,A.u_pecuadrotoxico
,A.pavosbbmortincub
,A.edadpadrecorraldescrip
,A.u_causapesobajo
,A.u_accionpesobajo
,A.diasaloj
,A.preinicioacum
,A.inicioacum
,A.acabadoacum
,A.terminadoacum
,A.finalizadoracum
,A.u_ruidostotales
,A.productoconsumo
,A.diassacaefectivo
,A.u_consumogasverano
,A.u_consumogasinvierno
,A.stdporcconsgasinviernoc
,A.stdporcconsgasinviernog
,A.stdporcconsgasinviernol
,A.stdporcconsgasveranoc
,A.stdporcconsgasveranog
,A.stdporcconsgasveranol
,A.porcalojamientoxedadpadre
,A.porcmortdiaacum
,A.difporcmortdiaacum_stddiaacum
,A.tasanoviable
,A.semporcprilesion
,A.semporcseglesion
,A.semporcterlesion
,A.descriptipoalimentoxtipoproducto
,A.stdconsdiaxtipoalimento
,A.tipoorigen
,A.formulano
,A.formulaname
,A.feedtypeno
,A.feedtypename
,A.listaformulano
,A.listaformulaname
,A.mortconcatcorral
,A.mortsemantcorral
,A.mortconcatlote
,A.mortsemantlote
,A.mortconcatsemaniolote
,A.pesoconcatcorral
,A.pesosemantcorral
,A.pesoconcatlote
,A.pesosemantlote
,A.pesoconcatsemaniolote
,A.mortconcatsemaniocorral
,A.pesoconcatsemaniocorral
,A.noviablesem1
,A.noviablesem2
,A.noviablesem3
,A.noviablesem4
,A.noviablesem5
,A.noviablesem6
,A.noviablesem7
,A.porcnoviablesem1
,A.porcnoviablesem2
,A.porcnoviablesem3
,A.porcnoviablesem4
,A.porcnoviablesem5
,A.porcnoviablesem6
,A.porcnoviablesem7
,A.noviablesem8
,A.porcnoviablesem8
,A.pk_tipogranja
,A.gananciapesosem
,A.cv
,B.STDPorcConsSem1 AS STDPorcConsSem1
,B.STDPorcConsSem2 AS STDPorcConsSem2
,B.STDPorcConsSem3 AS STDPorcConsSem3
,B.STDPorcConsSem4 AS STDPorcConsSem4
,B.STDPorcConsSem5 AS STDPorcConsSem5
,B.STDPorcConsSem6 AS STDPorcConsSem6
,B.STDPorcConsSem7 AS STDPorcConsSem7
,B.STDConsSem1 AS STDConsSem1
,B.STDConsSem2 AS STDConsSem2
,B.STDConsSem3 AS STDConsSem3
,B.STDConsSem4 AS STDConsSem4
,B.STDConsSem5 AS STDConsSem5
,B.STDConsSem6 AS STDConsSem6
,B.STDConsSem7 AS STDConsSem7
,'' stdconssem
,'' stdporcconssem
,'' conssem
FROM {database_name_tmp}.produccion A
LEFT JOIN {database_name_tmp}.STDPorcConsDiaHorizontal B 
ON A.pk_plantel = B.pk_plantel 
AND A.pk_lote = B.pk_lote 
AND A.pk_galpon = B.pk_galpon 
AND A.pk_sexo = B.pk_sexo
""")
print('carga produccion', df_UPDATEproduccion.count())
final_data = df_UPDATEproduccion
cant_total = final_data.count()

# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal"
}
final_data.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionTemporal")

final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal")         
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Produccion"
}
final_data2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Produccion")

print(f"Total de registros en la tabla Produccion : {cant_total}")
 #Limpia la ubicación temporal
glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
glue_client.delete_table(DatabaseName=database_name_tmp, Name='ProduccionTemporal')
print(f"Tabla ProduccionTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
# 1️⃣ Crear DataFrame con datos actualizados
df_UPDATEproduccion2 = spark.sql(f"""
SELECT 
 A.pk_tiempo
,A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_galpon
,A.pk_sexo
,A.pk_standard
,A.pk_producto
,A.pk_tipoproducto
,A.pk_grupoconsumo
,A.pk_especie
,A.pk_estado
,A.pk_administrador
,A.pk_proveedor
,A.pk_semanavida
,A.pk_diasvida
,A.pk_alimento
,A.complexentityno
,A.fechanacimiento
,A.edad
,A.fechacierre
,A.fechaalojamiento
,A.fechacrianza
,A.fechainiciogranja
,A.fechainiciosaca
,A.fechafinsaca
,A.areagalpon
,A.pobinicial
,A.avesrendidas
,A.avesrendidasacum
,A.kilosrendidos
,A.kilosrendidosacum
,A.mortdia
,A.mortacum
,A.mortsem
,A.porcmortdia
,A.porcmortacum
,A.porcmortsem
,A.mortsem1
,A.mortsem2
,A.mortsem3
,A.mortsem4
,A.mortsem5
,A.mortsem6
,A.mortsem7
,A.mortsem8
,A.mortsem9
,A.mortsem10
,A.mortsem11
,A.mortsem12
,A.mortsem13
,A.mortsem14
,A.mortsem15
,A.mortsem16
,A.mortsem17
,A.mortsem18
,A.mortsem19
,A.mortsem20
,A.mortsemacum1
,A.mortsemacum2
,A.mortsemacum3
,A.mortsemacum4
,A.mortsemacum5
,A.mortsemacum6
,A.mortsemacum7
,A.mortsemacum8
,A.mortsemacum9
,A.mortsemacum10
,A.mortsemacum11
,A.mortsemacum12
,A.mortsemacum13
,A.mortsemacum14
,A.mortsemacum15
,A.mortsemacum16
,A.mortsemacum17
,A.mortsemacum18
,A.mortsemacum19
,A.mortsemacum20
,A.peso_std
,A.pesodia
,A.peso
,A.pesosem
,A.pesosem1
,A.pesosem2
,A.pesosem3
,A.pesosem4
,A.pesosem5
,A.pesosem6
,A.pesosem7
,A.pesosem8
,A.pesosem9
,A.pesosem10
,A.pesosem11
,A.pesosem12
,A.pesosem13
,A.pesosem14
,A.pesosem15
,A.pesosem16
,A.pesosem17
,A.pesosem18
,A.pesosem19
,A.pesosem20
,A.peso5dias
,A.pesoalo
,A.pesohvo
,A.difpesoactpesoant
,A.cantdia
,A.gananciapesodia
,A.unidseleccion
,A.consdia
,A.consacum
,A.preinicio
,A.inicio
,A.acabado
,A.terminado
,A.finalizador
,A.pavoini
,A.pavo1
,A.pavo2
,A.pavo3
,A.pavo4
,A.pavo5
,A.pavo6
,A.pavo7
,A.conssem1
,A.conssem2
,A.conssem3
,A.conssem4
,A.conssem5
,A.conssem6
,A.conssem7
,A.conssem8
,A.conssem9
,A.conssem10
,A.conssem11
,A.conssem12
,A.conssem13
,A.conssem14
,A.conssem15
,A.conssem16
,A.conssem17
,A.conssem18
,A.conssem19
,A.conssem20
,A.ganancia
,A.stdmortdia
,A.stdmortacum
,A.stdconsdia
,A.stdconsacum
,A.stdpeso
,A.stdganancia
,A.stdica
,A.cantiniciosaca
,A.edadiniciosaca
,A.edadgranjacorral
,A.edadgranjagalpon
,A.edadgranjalote
,A.gas
,A.cama
,A.agua
,A.u_peaccidentados
,A.u_pehigadograso
,A.u_pehepatomegalia
,A.u_pehigadohemorragico
,A.u_peinanicion
,A.u_peproblemarespiratorio
,A.u_pesch
,A.u_peenteritis
,A.u_peascitis
,A.u_pemuertesubita
,A.u_peestresporcalor
,A.u_pehidropericardio
,A.u_pehemopericardio
,A.u_peuratosis
,A.u_pematerialcaseoso
,A.u_peonfalitis
,A.u_peretenciondeyema
,A.u_peerosiondemolleja
,A.u_pehemorragiamusculos
,A.u_pesangreenciego
,A.u_pepericarditis
,A.u_peperitonitis
,A.u_peprolapso
,A.u_pepicaje
,A.u_perupturaaortica
,A.u_pebazomoteado
,A.u_penoviable
,A.pigmentacion
,A.irn
,A.padremayor
,A.razamayor
,A.incubadoramayor
,A.porcpadremayor
,A.porcrazamayor
,A.porcincmayor
,A.categoria
,A.flagatipico
,A.edadpadrecorral
,A.edadpadregalpon
,A.edadpadrelote
,A.stdporcmortacumc
,A.stdporcmortacumg
,A.stdporcmortacuml
,A.stdpesoc
,A.stdpesog
,A.stdpesol
,A.stdconsacumc
,A.stdconsacumg
,A.stdconsacuml
,A.stdporcmortsem1
,A.stdporcmortsem2
,A.stdporcmortsem3
,A.stdporcmortsem4
,A.stdporcmortsem5
,A.stdporcmortsem6
,A.stdporcmortsem7
,A.stdporcmortsem8
,A.stdporcmortsem9
,A.stdporcmortsem10
,A.stdporcmortsem11
,A.stdporcmortsem12
,A.stdporcmortsem13
,A.stdporcmortsem14
,A.stdporcmortsem15
,A.stdporcmortsem16
,A.stdporcmortsem17
,A.stdporcmortsem18
,A.stdporcmortsem19
,A.stdporcmortsem20
,A.stdporcmortsemacum1
,A.stdporcmortsemacum2
,A.stdporcmortsemacum3
,A.stdporcmortsemacum4
,A.stdporcmortsemacum5
,A.stdporcmortsemacum6
,A.stdporcmortsemacum7
,A.stdporcmortsemacum8
,A.stdporcmortsemacum9
,A.stdporcmortsemacum10
,A.stdporcmortsemacum11
,A.stdporcmortsemacum12
,A.stdporcmortsemacum13
,A.stdporcmortsemacum14
,A.stdporcmortsemacum15
,A.stdporcmortsemacum16
,A.stdporcmortsemacum17
,A.stdporcmortsemacum18
,A.stdporcmortsemacum19
,A.stdporcmortsemacum20
,A.stdporcmortsem1g
,A.stdporcmortsem2g
,A.stdporcmortsem3g
,A.stdporcmortsem4g
,A.stdporcmortsem5g
,A.stdporcmortsem6g
,A.stdporcmortsem7g
,A.stdporcmortsem8g
,A.stdporcmortsem9g
,A.stdporcmortsem10g
,A.stdporcmortsem11g
,A.stdporcmortsem12g
,A.stdporcmortsem13g
,A.stdporcmortsem14g
,A.stdporcmortsem15g
,A.stdporcmortsem16g
,A.stdporcmortsem17g
,A.stdporcmortsem18g
,A.stdporcmortsem19g
,A.stdporcmortsem20g
,A.stdporcmortsemacum1g
,A.stdporcmortsemacum2g
,A.stdporcmortsemacum3g
,A.stdporcmortsemacum4g
,A.stdporcmortsemacum5g
,A.stdporcmortsemacum6g
,A.stdporcmortsemacum7g
,A.stdporcmortsemacum8g
,A.stdporcmortsemacum9g
,A.stdporcmortsemacum10g
,A.stdporcmortsemacum11g
,A.stdporcmortsemacum12g
,A.stdporcmortsemacum13g
,A.stdporcmortsemacum14g
,A.stdporcmortsemacum15g
,A.stdporcmortsemacum16g
,A.stdporcmortsemacum17g
,A.stdporcmortsemacum18g
,A.stdporcmortsemacum19g
,A.stdporcmortsemacum20g
,A.stdporcmortsem1l
,A.stdporcmortsem2l
,A.stdporcmortsem3l
,A.stdporcmortsem4l
,A.stdporcmortsem5l
,A.stdporcmortsem6l
,A.stdporcmortsem7l
,A.stdporcmortsem8l
,A.stdporcmortsem9l
,A.stdporcmortsem10l
,A.stdporcmortsem11l
,A.stdporcmortsem12l
,A.stdporcmortsem13l
,A.stdporcmortsem14l
,A.stdporcmortsem15l
,A.stdporcmortsem16l
,A.stdporcmortsem17l
,A.stdporcmortsem18l
,A.stdporcmortsem19l
,A.stdporcmortsem20l
,A.stdporcmortsemacum1l
,A.stdporcmortsemacum2l
,A.stdporcmortsemacum3l
,A.stdporcmortsemacum4l
,A.stdporcmortsemacum5l
,A.stdporcmortsemacum6l
,A.stdporcmortsemacum7l
,A.stdporcmortsemacum8l
,A.stdporcmortsemacum9l
,A.stdporcmortsemacum10l
,A.stdporcmortsemacum11l
,A.stdporcmortsemacum12l
,A.stdporcmortsemacum13l
,A.stdporcmortsemacum14l
,A.stdporcmortsemacum15l
,A.stdporcmortsemacum16l
,A.stdporcmortsemacum17l
,A.stdporcmortsemacum18l
,A.stdporcmortsemacum19l
,A.stdporcmortsemacum20l
,A.stdpesosem1
,A.stdpesosem2
,A.stdpesosem3
,A.stdpesosem4
,A.stdpesosem5
,A.stdpesosem6
,A.stdpesosem7
,A.stdpesosem8
,A.stdpesosem9
,A.stdpesosem10
,A.stdpesosem11
,A.stdpesosem12
,A.stdpesosem13
,A.stdpesosem14
,A.stdpesosem15
,A.stdpesosem16
,A.stdpesosem17
,A.stdpesosem18
,A.stdpesosem19
,A.stdpesosem20
,A.stdpeso5dias
,A.stdporcpesosem1g
,A.stdporcpesosem2g
,A.stdporcpesosem3g
,A.stdporcpesosem4g
,A.stdporcpesosem5g
,A.stdporcpesosem6g
,A.stdporcpesosem7g
,A.stdporcpesosem8g
,A.stdporcpesosem9g
,A.stdporcpesosem10g
,A.stdporcpesosem11g
,A.stdporcpesosem12g
,A.stdporcpesosem13g
,A.stdporcpesosem14g
,A.stdporcpesosem15g
,A.stdporcpesosem16g
,A.stdporcpesosem17g
,A.stdporcpesosem18g
,A.stdporcpesosem19g
,A.stdporcpesosem20g
,A.stdporcpeso5diasg
,A.stdporcpesosem1l
,A.stdporcpesosem2l
,A.stdporcpesosem3l
,A.stdporcpesosem4l
,A.stdporcpesosem5l
,A.stdporcpesosem6l
,A.stdporcpesosem7l
,A.stdporcpesosem8l
,A.stdporcpesosem9l
,A.stdporcpesosem10l
,A.stdporcpesosem11l
,A.stdporcpesosem12l
,A.stdporcpesosem13l
,A.stdporcpesosem14l
,A.stdporcpesosem15l
,A.stdporcpesosem16l
,A.stdporcpesosem17l
,A.stdporcpesosem18l
,A.stdporcpesosem19l
,A.stdporcpesosem20l
,A.stdporcpeso5diasl
,A.stdicac
,A.stdicag
,A.stdical
,A.u_peaerosaculitisg2
,A.u_pecojera
,A.u_pehigadoicterico
,A.u_pematerialcaseoso_po1ra
,A.u_pematerialcaseosomedretr
,A.u_penecrosishepatica
,A.u_peneumonia
,A.u_pesepticemia
,A.u_pevomitonegro
,A.u_peasperguillius
,A.u_pebazograndemot
,A.u_pecorazongrande
,A.u_pecuadrotoxico
,A.pavosbbmortincub
,A.edadpadrecorraldescrip
,A.u_causapesobajo
,A.u_accionpesobajo
,A.diasaloj
,A.preinicioacum
,A.inicioacum
,A.acabadoacum
,A.terminadoacum
,A.finalizadoracum
,A.u_ruidostotales
,A.productoconsumo
,A.diassacaefectivo
,A.u_consumogasverano
,A.u_consumogasinvierno
,A.stdporcconsgasinviernoc
,A.stdporcconsgasinviernog
,A.stdporcconsgasinviernol
,A.stdporcconsgasveranoc
,A.stdporcconsgasveranog
,A.stdporcconsgasveranol
,A.porcalojamientoxedadpadre
,A.porcmortdiaacum
,A.difporcmortdiaacum_stddiaacum
,A.tasanoviable
,A.semporcprilesion
,A.semporcseglesion
,A.semporcterlesion
,A.descriptipoalimentoxtipoproducto
,A.stdconsdiaxtipoalimento
,A.tipoorigen
,A.formulano
,A.formulaname
,A.feedtypeno
,A.feedtypename
,A.listaformulano
,A.listaformulaname
,A.mortconcatcorral
,A.mortsemantcorral
,A.mortconcatlote
,A.mortsemantlote
,A.mortconcatsemaniolote
,A.pesoconcatcorral
,A.pesosemantcorral
,A.pesoconcatlote
,A.pesosemantlote
,A.pesoconcatsemaniolote
,A.mortconcatsemaniocorral
,A.pesoconcatsemaniocorral
,A.noviablesem1
,A.noviablesem2
,A.noviablesem3
,A.noviablesem4
,A.noviablesem5
,A.noviablesem6
,A.noviablesem7
,A.porcnoviablesem1
,A.porcnoviablesem2
,A.porcnoviablesem3
,A.porcnoviablesem4
,A.porcnoviablesem5
,A.porcnoviablesem6
,A.porcnoviablesem7
,A.noviablesem8
,A.porcnoviablesem8
,A.pk_tipogranja
,A.gananciapesosem
,A.cv
,A.STDPorcConsSem1
,A.STDPorcConsSem2
,A.STDPorcConsSem3
,A.STDPorcConsSem4
,A.STDPorcConsSem5
,A.STDPorcConsSem6
,A.STDPorcConsSem7
,A.STDConsSem1
,A.STDConsSem2
,A.STDConsSem3
,A.STDConsSem4
,A.STDConsSem5
,A.STDConsSem6
,A.STDConsSem7
,B.STDConsSem
,B.STDPorcConsSem
,B.ConsSem
FROM {database_name_tmp}.produccion A
LEFT JOIN {database_name_tmp}.STDPorcConsDia B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon AND A.pk_sexo =B.pk_sexo AND A.pk_semanavida = B.pk_semanavida
""")
print('carga UPDATE produccion 2 ', df_UPDATEproduccion2.count())
final_data = df_UPDATEproduccion2
cant_total = final_data.count()

# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal"
}
final_data.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionTemporal")

final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ProduccionTemporal")         
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Produccion"
}
final_data2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Produccion")

print(f"Total de registros en la tabla Produccion : {cant_total}")
 #Limpia la ubicación temporal
glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
glue_client.delete_table(DatabaseName=database_name_tmp, Name='ProduccionTemporal')
print(f"Tabla ProduccionTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
#Se muestra ProduccionDetalleEdad
df_ProduccionDetalleEdad = spark.sql(f"""SELECT 
		 MAX(A.fecha) AS fecha
         ,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_tipoproducto,pk_grupoconsumo,pk_especie
         ,pk_estado,pk_administrador,MAX(pk_proveedor) pk_proveedor,pk_semanavida,A.pk_diasvida
		,A.ComplexEntityNo,FechaNacimiento,MAX(Edad) Edad,FechaCierre
		,FechaAlojamiento,FechaCrianza,FechaInicioGranja,FechaInicioSaca,FechaFinSaca
		,AVG(AreaGalpon) AS AreaGalpon
		,AVG(PobInicial) AS PobInicial
		,AVG(AvesRendidas) AS AvesRendidas
		,AVG(KilosRendidos) AS KilosRendidos
		,MAX(MortDia) AS MortDia
		,MAX(MortAcum) AS MortAcum
		,MAX(MortSem) AS MortSem
		,MAX(PorcMortDia) AS PorcMortDia
		,MAX(PorcMortAcum) AS PorcMortAcum
		,MAX(PorcMortSem) AS PorcMortSem 
		,AVG(MortSem1) AS MortSem1
		,AVG(MortSem2) AS MortSem2
		,AVG(MortSem3) AS MortSem3
		,AVG(MortSem4) AS MortSem4
		,AVG(MortSem5) AS MortSem5
		,AVG(MortSem6) AS MortSem6
		,AVG(MortSem7) AS MortSem7
		,AVG(MortSem8) AS MortSem8
		,AVG(MortSem9) AS MortSem9
		,AVG(MortSem10) AS MortSem10
		,AVG(MortSem11) AS MortSem11
		,AVG(MortSem12) AS MortSem12
		,AVG(MortSem13) AS MortSem13
		,AVG(MortSem14) AS MortSem14
		,AVG(MortSem15) AS MortSem15
		,AVG(MortSem16) AS MortSem16
		,AVG(MortSem17) AS MortSem17
		,AVG(MortSem18) AS MortSem18
		,AVG(MortSem19) AS MortSem19
		,AVG(MortSem20) AS MortSem20
		,AVG(MortSemAcum1) AS MortSemAcum1 
		,AVG(MortSemAcum2) AS MortSemAcum2 
		,AVG(MortSemAcum3) AS MortSemAcum3
		,AVG(MortSemAcum4) AS MortSemAcum4 
		,AVG(MortSemAcum5) AS MortSemAcum5 
		,AVG(MortSemAcum6) AS MortSemAcum6 
		,AVG(MortSemAcum7) AS MortSemAcum7 
		,AVG(MortSemAcum8) AS MortSemAcum8 
		,AVG(MortSemAcum9) AS MortSemAcum9 
		,AVG(MortSemAcum10) AS MortSemAcum10 
		,AVG(MortSemAcum11) AS MortSemAcum11 
		,AVG(MortSemAcum12) AS MortSemAcum12 
		,AVG(MortSemAcum13) AS MortSemAcum13 
		,AVG(MortSemAcum14) AS MortSemAcum14 
		,AVG(MortSemAcum15) AS MortSemAcum15 
		,AVG(MortSemAcum16) AS MortSemAcum16 
		,AVG(MortSemAcum17) AS MortSemAcum17 
		,AVG(MortSemAcum18) AS MortSemAcum18 
		,AVG(MortSemAcum19) AS MortSemAcum19 
		,AVG(MortSemAcum20) AS MortSemAcum20 
		,MAX(Peso_STD) AS Peso_STD
		,MAX(PesoDia) AS PesoDia
		,MAX(Peso) AS Peso
		,AVG(PesoSem) AS PesoSem 
		,AVG(PesoSem1) AS PesoSem1
		,AVG(PesoSem2) AS PesoSem2
		,AVG(PesoSem3) AS PesoSem3
		,AVG(PesoSem4) AS PesoSem4
		,AVG(PesoSem5) AS PesoSem5
		,AVG(PesoSem6) AS PesoSem6
		,AVG(PesoSem7) AS PesoSem7
		,AVG(PesoSem8) AS PesoSem8
		,AVG(PesoSem9) AS PesoSem9
		,AVG(PesoSem10) AS PesoSem10
		,AVG(PesoSem11) AS PesoSem11
		,AVG(PesoSem12) AS PesoSem12
		,AVG(PesoSem13) AS PesoSem13
		,AVG(PesoSem14) AS PesoSem14
		,AVG(PesoSem15) AS PesoSem15
		,AVG(PesoSem16) AS PesoSem16
		,AVG(PesoSem17) AS PesoSem17
		,AVG(PesoSem18) AS PesoSem18
		,AVG(PesoSem19) AS PesoSem19
		,AVG(PesoSem20) AS PesoSem20
		,AVG(Peso5Dias) AS Peso5Dias
		,AVG(PesoAlo) AS PesoAlo
		,AVG(PesoHvo) AS PesoHvo
		,AVG(UnidSeleccion) AS UnidSeleccion
		,SUM(ConsDia) AS ConsDia
		,MAX(ConsAcum) AS ConsAcum
		,MAX(PreInicio) AS PreInicio
		,MAX(Inicio) AS Inicio
		,MAX(Acabado) AS Acabado
		,MAX(Terminado) AS Terminado
		,MAX(Finalizador) AS Finalizador
		,MAX(PavoIni) AS PavoIni
		,MAX(Pavo1) AS Pavo1
		,MAX(Pavo2) AS Pavo2
		,MAX(Pavo3) AS Pavo3
		,MAX(Pavo4) AS Pavo4
		,MAX(Pavo5) AS Pavo5
		,MAX(Pavo6) AS Pavo6
		,MAX(Pavo7) AS Pavo7
		,AVG(Ganancia) AS Ganancia
		,MAX(STDMortDia) AS STDMortDia
		,MAX(STDMortAcum) AS STDMortAcum
		,MAX(STDConsDia) AS STDConsDia
		,MAX(STDConsAcum) AS STDConsAcum
		,MAX(STDPeso) AS STDPeso
		,MAX(STDICA) AS STDICA
		,AVG(CantInicioSaca) AS CantInicioSaca
		,AVG(EdadInicioSaca) AS EdadInicioSaca
		,AVG(EdadGranjaCorral) AS EdadGranjaCorral
		,AVG(EdadGranjaGalpon) AS EdadGranjaGalpon
		,AVG(EdadGranjaLote) AS EdadGranjaLote
		,AVG(gas) AS Gas
		,AVG(cama) AS Cama
		,AVG(agua) AS Agua
		,CASE WHEN pk_sexo = 2 THEN AVG(PobInicial) ELSE 0 END AS CantMacho
		,SUM(U_PEAccidentados) AS U_PEAccidentados
		,SUM(U_PEHigadoGraso) AS U_PEHigadoGraso
		,SUM(U_PEHepatomegalia) AS U_PEHepatomegalia
		,SUM(U_PEHigadoHemorragico) AS U_PEHigadoHemorragico
		,SUM(U_PEInanicion) AS U_PEInanicion
		,SUM(U_PEProblemaRespiratorio) AS U_PEProblemaRespiratorio
		,SUM(U_PESCH) AS U_PESCH
		,SUM(U_PEEnteritis) AS U_PEEnteritis
		,SUM(U_PEAscitis) AS U_PEAscitis
		,SUM(U_PEMuerteSubita) AS U_PEMuerteSubita
		,SUM(U_PEEstresPorCalor) AS U_PEEstresPorCalor
		,SUM(U_PEHidropericardio) AS U_PEHidropericardio
		,SUM(U_PEHemopericardio) AS U_PEHemopericardio
		,SUM(U_PEUratosis) AS U_PEUratosis
		,SUM(U_PEMaterialCaseoso) AS U_PEMaterialCaseoso
		,SUM(U_PEOnfalitis) AS U_PEOnfalitis
		,SUM(U_PERetencionDeYema) AS U_PERetencionDeYema
		,SUM(U_PEErosionDeMolleja) AS U_PEErosionDeMolleja
		,SUM(U_PEHemorragiaMusculos) AS U_PEHemorragiaMusculos
		,SUM(U_PESangreEnCiego) AS U_PESangreEnCiego
		,SUM(U_PEPericarditis) AS U_PEPericarditis
		,SUM(U_PEPeritonitis) AS U_PEPeritonitis
		,SUM(U_PEProlapso) AS U_PEProlapso
		,SUM(U_PEPicaje) AS U_PEPicaje
		,SUM(U_PERupturaAortica) AS U_PERupturaAortica
		,SUM(U_PEBazoMoteado) AS U_PEBazoMoteado
		,SUM(U_PENoViable) AS U_PENoViable
		,AVG(Pigmentacion) AS Pigmentacion
		,PadreMayor
		,RazaMayor
		,IncubadoraMayor
		,MAX(PorcPadreMayor) PorcPadreMayor
		,MAX(PorcRazaMayor)PorcRazaMayor
		,MAX(PorcIncMayor) PorcIncMayor
		,categoria
		,FlagAtipico
		,MAX(EdadPadreCorral) EdadPadreCorral
		,MAX(EdadPadreGalpon) EdadPadreGalpon
		,MAX(EdadPadreLote) EdadPadreLote
		,MAX(STDPorcMortAcumC) STDPorcMortAcumC
		,MAX(STDPorcMortAcumG) STDPorcMortAcumG
		,MAX(STDPorcMortAcumL) STDPorcMortAcumL
		,MAX(STDPesoC) STDPesoC
		,MAX(STDPesoG) STDPesoG
		,MAX(STDPesoL) STDPesoL
		,MAX(STDConsAcumC) STDConsAcumC
		,MAX(STDConsAcumG) STDConsAcumG
		,MAX(STDConsAcumL) STDConsAcumL
		,MAX(STDPorcMortSem1) STDPorcMortSem1
		,MAX(STDPorcMortSem2) STDPorcMortSem2
		,MAX(STDPorcMortSem3) STDPorcMortSem3
		,MAX(STDPorcMortSem4) STDPorcMortSem4
		,MAX(STDPorcMortSem5) STDPorcMortSem5
		,MAX(STDPorcMortSem6) STDPorcMortSem6
		,MAX(STDPorcMortSem7) STDPorcMortSem7
		,MAX(STDPorcMortSem8) STDPorcMortSem8
		,MAX(STDPorcMortSem9) STDPorcMortSem9
		,MAX(STDPorcMortSem10) STDPorcMortSem10
		,MAX(STDPorcMortSem11) STDPorcMortSem11
		,MAX(STDPorcMortSem12) STDPorcMortSem12
		,MAX(STDPorcMortSem13) STDPorcMortSem13
		,MAX(STDPorcMortSem14) STDPorcMortSem14
		,MAX(STDPorcMortSem15) STDPorcMortSem15
		,MAX(STDPorcMortSem16) STDPorcMortSem16
		,MAX(STDPorcMortSem17) STDPorcMortSem17
		,MAX(STDPorcMortSem18) STDPorcMortSem18
		,MAX(STDPorcMortSem19) STDPorcMortSem19
		,MAX(STDPorcMortSem20) STDPorcMortSem20
		,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
		,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
		,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
		,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
		,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
		,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
		,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
		,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
		,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
		,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
		,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
		,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
		,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
		,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
		,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
		,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
		,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
		,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
		,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
		,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
		,MAX(STDPorcMortSem1G) STDPorcMortSem1G
		,MAX(STDPorcMortSem2G) STDPorcMortSem2G
		,MAX(STDPorcMortSem3G) STDPorcMortSem3G
		,MAX(STDPorcMortSem4G) STDPorcMortSem4G
		,MAX(STDPorcMortSem5G) STDPorcMortSem5G
		,MAX(STDPorcMortSem6G) STDPorcMortSem6G
		,MAX(STDPorcMortSem7G) STDPorcMortSem7G
		,MAX(STDPorcMortSem8G) STDPorcMortSem8G
		,MAX(STDPorcMortSem9G) STDPorcMortSem9G
		,MAX(STDPorcMortSem10G) STDPorcMortSem10G
		,MAX(STDPorcMortSem11G) STDPorcMortSem11G
		,MAX(STDPorcMortSem12G) STDPorcMortSem12G
		,MAX(STDPorcMortSem13G) STDPorcMortSem13G
		,MAX(STDPorcMortSem14G) STDPorcMortSem14G
		,MAX(STDPorcMortSem15G) STDPorcMortSem15G
		,MAX(STDPorcMortSem16G) STDPorcMortSem16G
		,MAX(STDPorcMortSem17G) STDPorcMortSem17G
		,MAX(STDPorcMortSem18G) STDPorcMortSem18G
		,MAX(STDPorcMortSem19G) STDPorcMortSem19G
		,MAX(STDPorcMortSem20G) STDPorcMortSem20G
		,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1G
		,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2G
		,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3G
		,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4G
		,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5G
		,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6G
		,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7G
		,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8G
		,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9G
		,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10G
		,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11G
		,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12G
		,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13G
		,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14G
		,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15G
		,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16G
		,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17G
		,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18G
		,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19G
		,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20G
		,MAX(STDPorcMortSem1L) STDPorcMortSem1L
		,MAX(STDPorcMortSem2L) STDPorcMortSem2L
		,MAX(STDPorcMortSem3L) STDPorcMortSem3L
		,MAX(STDPorcMortSem4L) STDPorcMortSem4L
		,MAX(STDPorcMortSem5L) STDPorcMortSem5L
		,MAX(STDPorcMortSem6L) STDPorcMortSem6L
		,MAX(STDPorcMortSem7L) STDPorcMortSem7L
		,MAX(STDPorcMortSem8L) STDPorcMortSem8L
		,MAX(STDPorcMortSem9L) STDPorcMortSem9L
		,MAX(STDPorcMortSem10L) STDPorcMortSem10L
		,MAX(STDPorcMortSem11L) STDPorcMortSem11L
		,MAX(STDPorcMortSem12L) STDPorcMortSem12L
		,MAX(STDPorcMortSem13L) STDPorcMortSem13L
		,MAX(STDPorcMortSem14L) STDPorcMortSem14L
		,MAX(STDPorcMortSem15L) STDPorcMortSem15L
		,MAX(STDPorcMortSem16L) STDPorcMortSem16L
		,MAX(STDPorcMortSem17L) STDPorcMortSem17L
		,MAX(STDPorcMortSem18L) STDPorcMortSem18L
		,MAX(STDPorcMortSem19L) STDPorcMortSem19L
		,MAX(STDPorcMortSem20L) STDPorcMortSem20L
		,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1L
		,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2L
		,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3L
		,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4L
		,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5L
		,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6L
		,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7L
		,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8L
		,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9L
		,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10L
		,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11L
		,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12L
		,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13L
		,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14L
		,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15L
		,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16L
		,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17L
		,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18L
		,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19L
		,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20L
		,MAX(STDPesoSem1)STDPesoSem1
		,MAX(STDPesoSem2)STDPesoSem2
		,MAX(STDPesoSem3)STDPesoSem3
		,MAX(STDPesoSem4)STDPesoSem4
		,MAX(STDPesoSem5)STDPesoSem5
		,MAX(STDPesoSem6)STDPesoSem6
		,MAX(STDPesoSem7)STDPesoSem7
		,MAX(STDPesoSem8)STDPesoSem8
		,MAX(STDPesoSem9)STDPesoSem9
		,MAX(STDPesoSem10)STDPesoSem10
		,MAX(STDPesoSem11)STDPesoSem11
		,MAX(STDPesoSem12)STDPesoSem12
		,MAX(STDPesoSem13)STDPesoSem13
		,MAX(STDPesoSem14)STDPesoSem14
		,MAX(STDPesoSem15)STDPesoSem15
		,MAX(STDPesoSem16)STDPesoSem16
		,MAX(STDPesoSem17)STDPesoSem17
		,MAX(STDPesoSem18)STDPesoSem18
		,MAX(STDPesoSem19)STDPesoSem19
		,MAX(STDPesoSem20)STDPesoSem20
		,MAX(STDPeso5Dias)STDPeso5Dias
		,MAX(STDPorcPesoSem1G)STDPorcPesoSem1G
		,MAX(STDPorcPesoSem2G)STDPorcPesoSem2G
		,MAX(STDPorcPesoSem3G)STDPorcPesoSem3G
		,MAX(STDPorcPesoSem4G)STDPorcPesoSem4G
		,MAX(STDPorcPesoSem5G)STDPorcPesoSem5G
		,MAX(STDPorcPesoSem6G)STDPorcPesoSem6G
		,MAX(STDPorcPesoSem7G)STDPorcPesoSem7G
		,MAX(STDPorcPesoSem8G)STDPorcPesoSem8G
		,MAX(STDPorcPesoSem9G)STDPorcPesoSem9G
		,MAX(STDPorcPesoSem10G)STDPorcPesoSem10G
		,MAX(STDPorcPesoSem11G)STDPorcPesoSem11G
		,MAX(STDPorcPesoSem12G)STDPorcPesoSem12G
		,MAX(STDPorcPesoSem13G)STDPorcPesoSem13G
		,MAX(STDPorcPesoSem14G)STDPorcPesoSem14G
		,MAX(STDPorcPesoSem15G)STDPorcPesoSem15G
		,MAX(STDPorcPesoSem16G)STDPorcPesoSem16G
		,MAX(STDPorcPesoSem17G)STDPorcPesoSem17G
		,MAX(STDPorcPesoSem18G)STDPorcPesoSem18G
		,MAX(STDPorcPesoSem19G)STDPorcPesoSem19G
		,MAX(STDPorcPesoSem20G)STDPorcPesoSem20G
		,MAX(STDPorcPeso5DiasG)STDPorcPeso5DiasG
		,MAX(STDPorcPesoSem1L)STDPorcPesoSem1L
		,MAX(STDPorcPesoSem2L)STDPorcPesoSem2L
		,MAX(STDPorcPesoSem3L)STDPorcPesoSem3L
		,MAX(STDPorcPesoSem4L)STDPorcPesoSem4L
		,MAX(STDPorcPesoSem5L)STDPorcPesoSem5L
		,MAX(STDPorcPesoSem6L)STDPorcPesoSem6L
		,MAX(STDPorcPesoSem7L)STDPorcPesoSem7L
		,MAX(STDPorcPesoSem8L)STDPorcPesoSem8L
		,MAX(STDPorcPesoSem9L)STDPorcPesoSem9L
		,MAX(STDPorcPesoSem10L)STDPorcPesoSem10L
		,MAX(STDPorcPesoSem11L)STDPorcPesoSem11L
		,MAX(STDPorcPesoSem12L)STDPorcPesoSem12L
		,MAX(STDPorcPesoSem13L)STDPorcPesoSem13L
		,MAX(STDPorcPesoSem14L)STDPorcPesoSem14L
		,MAX(STDPorcPesoSem15L)STDPorcPesoSem15L
		,MAX(STDPorcPesoSem16L)STDPorcPesoSem16L
		,MAX(STDPorcPesoSem17L)STDPorcPesoSem17L
		,MAX(STDPorcPesoSem18L)STDPorcPesoSem18L
		,MAX(STDPorcPesoSem19L)STDPorcPesoSem19L
		,MAX(STDPorcPesoSem20L)STDPorcPesoSem20L
		,MAX(STDPorcPeso5DiasL)STDPorcPeso5DiasL
		,MAX(STDICAC) STDICAC
		,MAX(STDICAG) STDICAG
		,MAX(STDICAL) STDICAL
		,SUM(U_PEAerosaculitisG2) AS U_PEAerosaculitisG2
		,SUM(U_PECojera) AS U_PECojera
		,SUM(U_PEHigadoIcterico) AS U_PEHigadoIcterico
		,SUM(U_PEMaterialCaseoso_po1ra) AS U_PEMaterialCaseoso_po1ra
		,SUM(U_PEMaterialCaseosoMedRetr) AS U_PEMaterialCaseosoMedRetr
		,SUM(U_PENecrosisHepatica) AS U_PENecrosisHepatica
		,SUM(U_PENeumonia) AS U_PENeumonia
		,SUM(U_PESepticemia) AS U_PESepticemia
		,SUM(U_PEVomitoNegro) AS U_PEVomitoNegro
		,SUM(U_PEAsperguillius) AS U_PEAsperguillius
		,SUM(U_PEBazoGrandeMot) AS U_PEBazoGrandeMot
		,SUM(U_PECorazonGrande) AS U_PECorazonGrande
		,SUM(U_PECuadroToxico) AS U_PECuadroToxico
		,MAX(PavosBBMortIncub) AS PavosBBMortIncub
		,EdadPadreCorralDescrip
		,U_CausaPesoBajo
		,U_AccionPesoBajo
		,MAX(DiasAloj) DiasAloj
		,MAX(U_RuidosTotales) U_RuidosTotales
		,MAX(ConsSem1) ConsSem1
		,MAX(ConsSem2) ConsSem2
		,MAX(ConsSem3) ConsSem3
		,MAX(ConsSem4) ConsSem4
		,MAX(ConsSem5) ConsSem5
		,MAX(ConsSem6) ConsSem6
		,MAX(ConsSem7) ConsSem7
		,MAX(ConsSem8) ConsSem8
		,MAX(ConsSem9) ConsSem9
		,MAX(ConsSem10) ConsSem10
		,MAX(ConsSem11) ConsSem11
		,MAX(ConsSem12) ConsSem12
		,MAX(ConsSem13) ConsSem13
		,MAX(ConsSem14) ConsSem14
		,MAX(ConsSem15) ConsSem15
		,MAX(ConsSem16) ConsSem16
		,MAX(ConsSem17) ConsSem17
		,MAX(ConsSem18) ConsSem18
		,MAX(ConsSem19) ConsSem19
		,MAX(ConsSem20) ConsSem20
		,MAX(STDPorcConsSem1) STDPorcConsSem1
		,MAX(STDPorcConsSem2) STDPorcConsSem2
		,MAX(STDPorcConsSem3) STDPorcConsSem3
		,MAX(STDPorcConsSem4) STDPorcConsSem4
		,MAX(STDPorcConsSem5) STDPorcConsSem5
		,MAX(STDPorcConsSem6) STDPorcConsSem6
		,MAX(STDPorcConsSem7) STDPorcConsSem7
		,MAX(STDConsSem1) STDConsSem1
		,MAX(STDConsSem2) STDConsSem2
		,MAX(STDConsSem3) STDConsSem3
		,MAX(STDConsSem4) STDConsSem4
		,MAX(STDConsSem5) STDConsSem5
		,MAX(STDConsSem6) STDConsSem6
		,MAX(STDConsSem7) STDConsSem7
		,MAX(STDConsSem) STDConsSem
		,MAX(STDPorcConsSem) STDPorcConsSem
		,MAX(ConsSem) ConsSem
		,MAX(DiasSacaEfectivo) DiasSacaEfectivo
		,MAX(U_ConsumoGasVerano) U_ConsumoGasVerano
		,MAX(U_ConsumoGasInvierno) U_ConsumoGasInvierno
		,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInviernoC
		,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInviernoG
		,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInviernoL
		,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVeranoC
		,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVeranoG
		,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVeranoL
		,MAX(PorcAlojamientoXEdadPadre) PorcAlojamientoXEdadPadre
		,TipoOrigen
		,MAX(NoViableSem1) NoViableSem1
		,MAX(NoViableSem2) NoViableSem2
		,MAX(NoViableSem3) NoViableSem3
		,MAX(NoViableSem4) NoViableSem4
		,MAX(NoViableSem5) NoViableSem5
		,MAX(NoViableSem6) NoViableSem6
		,MAX(NoViableSem7) NoViableSem7
		,MAX(PorcNoViableSem1) PorcNoViableSem1
		,MAX(PorcNoViableSem2) PorcNoViableSem2
		,MAX(PorcNoViableSem3) PorcNoViableSem3
		,MAX(PorcNoViableSem4) PorcNoViableSem4
		,MAX(PorcNoViableSem5) PorcNoViableSem5
		,MAX(PorcNoViableSem6) PorcNoViableSem6
		,MAX(PorcNoViableSem7) PorcNoViableSem7
		,MAX(NoViableSem8) NoViableSem8
		,MAX(PorcNoViableSem8) PorcNoViableSem8
		,A.pk_tipogranja
		,MAX(GananciaPesoSem) GananciaPesoSem
		,MAX(CV) CV
FROM {database_name_tmp}.produccion A
GROUP BY pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_tipoproducto,pk_grupoconsumo,pk_especie,pk_estado
,pk_administrador,pk_semanavida,A.pk_diasvida,A.ComplexEntityNo,FechaNacimiento,FechaCierre,FechaAlojamiento,FechaCrianza,FechaInicioGranja,FechaInicioSaca,FechaFinSaca
,PadreMayor,RazaMayor,IncubadoraMayor,categoria,FlagAtipico,EdadPadreCorralDescrip,U_CausaPesoBajo,U_AccionPesoBajo,TipoOrigen,A.pk_tipogranja
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleEdad"
}
df_ProduccionDetalleEdad.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleEdad")
print('carga ProduccionDetalleEdad', df_ProduccionDetalleEdad.count())   
#Se muestra OrdenarMortalidadesCorral
df_OrdenarMortalidadesCorral = spark.sql(f"""
    SELECT 
        descripfecha fecha, pk_empresa, pk_division, ComplexEntityNo, 
        causa, cmortalidad, 
        ROUND((cmortalidad / NULLIF(PobInicial * 1.0, 0) * 100), 5) AS pcmortalidad,
        ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) AS orden,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 1 THEN 'CantPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 2 THEN 'CantSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 3 THEN 'CantTerLesion'
        END AS OrdenCantidad,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 1 THEN 'NomPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 2 THEN 'NomSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 3 THEN 'NomTerLesion'
        END AS OrdenNombre,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 1 THEN 'PorcPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 2 THEN 'PorcSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, descripfecha ORDER BY cmortalidad DESC) = 3 THEN 'PorcTerLesion'
        END AS OrdenPorcentaje
    FROM {database_name_gl}.ft_mortalidad_corral
    LATERAL VIEW EXPLODE(MAP(
        'U_PEAccidentados', U_PEAccidentados,
        'U_PEHigadoGraso', U_PEHigadoGraso,
        'U_PEHepatomegalia', U_PEHepatomegalia,
        'U_PEHigadoHemorragico', U_PEHigadoHemorragico,
        'U_PEInanicion', U_PEInanicion,
        'U_PEProblemaRespiratorio', U_PEProblemaRespiratorio,
        'U_PESCH', U_PESCH,
        'U_PEEnteritis', U_PEEnteritis,
        'U_PEAscitis', U_PEAscitis,
        'U_PEMuerteSubita', U_PEMuerteSubita,
        'U_PEEstresPorCalor', U_PEEstresPorCalor,
        'U_PEHidropericardio', U_PEHidropericardio,
        'U_PEHemopericardio', U_PEHemopericardio,
        'U_PEUratosis', U_PEUratosis,
        'U_PEMaterialCaseoso', U_PEMaterialCaseoso,
        'U_PEOnfalitis', U_PEOnfalitis,
        'U_PERetencionDeYema', U_PERetencionDeYema,
        'U_PEErosionDeMolleja', U_PEErosionDeMolleja,
        'U_PEHemorragiaMusculos', U_PEHemorragiaMusculos,
        'U_PESangreEnCiego', U_PESangreEnCiego,
        'U_PEPericarditis', U_PEPericarditis,
        'U_PEPeritonitis', U_PEPeritonitis,
        'U_PEProlapso', U_PEProlapso,
        'U_PEPicaje', U_PEPicaje,
        'U_PERupturaAortica', U_PERupturaAortica,
        'U_PEBazoMoteado', U_PEBazoMoteado,
        'U_PENoViable', U_PENoViable,
        'U_PECaja', U_PECaja,
        'U_PEGota', U_PEGota,
        'U_PEIntoxicacion', U_PEIntoxicacion,
        'U_PERetrazos', U_PERetrazos,
        'U_PEEliminados', U_PEEliminados,
        'U_PEAhogados', U_PEAhogados,
        'U_PEEColi', U_PEEColi,
        'U_PEDescarte', U_PEDescarte,
        'U_PEOtros', U_PEOtros,
        'U_PECoccidia', U_PECoccidia,
        'U_PEDeshidratados', U_PEDeshidratados,
        'U_PEHepatitis', U_PEHepatitis,
        'U_PETraumatismo', U_PETraumatismo,
        'U_PEAerosaculitisG2', U_PEAerosaculitisG2,
        'U_PECojera', U_PECojera,
        'U_PEHigadoIcterico', U_PEHigadoIcterico,
        'U_PEMaterialCaseoso_po1ra', U_PEMaterialCaseoso_po1ra,
        'U_PEMaterialCaseosoMedRetr', U_PEMaterialCaseosoMedRetr,
        'U_PENecrosisHepatica', U_PENecrosisHepatica,
        'U_PENeumonia', U_PENeumonia,
        'U_PESepticemia', U_PESepticemia,
        'U_PEVomitoNegro', U_PEVomitoNegro
    )) AS causa, cmortalidad
    WHERE cmortalidad <> 0 
    AND pk_empresa = 1
    AND date_format(descripfecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesCorral"
}
df_OrdenarMortalidadesCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidadesCorral")
print('carga OrdenarMortalidadesCorral', df_OrdenarMortalidadesCorral.count())
#Se muestra OrdenarMortalidadesCorral
df_OrdenarMortalidadesCorral2 = spark.sql(f"""
SELECT 
    fecha, ComplexEntityNo, pk_empresa, pk_division,
    -- Cantidades
    MAX(CASE WHEN ordenCantidad = 'CantPrimLesion' THEN cmortalidad END) AS CantPrimLesion,
    MAX(CASE WHEN ordenCantidad = 'CantSegLesion' THEN cmortalidad END) AS CantSegLesion,
    MAX(CASE WHEN ordenCantidad = 'CantTerLesion' THEN cmortalidad END) AS CantTerLesion,
    -- Porcentajes
    MAX(CASE WHEN ordenPorcentaje = 'PorcPrimLesion' THEN pcmortalidad END) AS PorcPrimLesion,
    MAX(CASE WHEN ordenPorcentaje = 'PorcSegLesion' THEN pcmortalidad END) AS PorcSegLesion,
    MAX(CASE WHEN ordenPorcentaje = 'PorcTerLesion' THEN pcmortalidad END) AS PorcTerLesion,
    -- Nombres
    MAX(CASE WHEN ordenNombre = 'NomPrimLesion' THEN causa END) AS NomPrimLesion,
    MAX(CASE WHEN ordenNombre = 'NomSegLesion' THEN causa END) AS NomSegLesion,
    MAX(CASE WHEN ordenNombre = 'NomTerLesion' THEN causa END) AS NomTerLesion
FROM {database_name_tmp}.OrdenarMortalidadesCorral
GROUP BY fecha, ComplexEntityNo, pk_empresa, pk_division
ORDER BY fecha, ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesCorral2"
}
df_OrdenarMortalidadesCorral2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenarMortalidadesCorral2")
print('carga OrdenarMortalidadesCorral2', df_OrdenarMortalidadesCorral2.count())   
#Se muestra ProductoConsumo1
df_ProductoConsumo1 = spark.sql(f"""
select A.ComplexEntityNo,A.pk_semanavida,A.nproductoconsumo,MAX(ROW_NUMBER) ROW_NUMBER
from (select A.fecha,A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo,ROW_NUMBER() OVER (PARTITION BY A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo ORDER BY A.fecha) ROW_NUMBER
		from {database_name_tmp}.ProduccionDetalleEdad A
		left join {database_name_gl}.ft_consumos B on A.complexentityno = b. complexentityno and a.pk_diasvida = b.pk_diasvida
		left join {database_name_gl}.lk_productoconsumo c on b.pk_productoconsumo = c.pk_productoconsumo
		where c.pk_grupoconsumo = 11 and A.pk_division = 4 and A.pk_empresa = 1 ) A
GROUP BY A.ComplexEntityNo,A.pk_semanavida,A.nproductoconsumo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProductoConsumo1"
}
df_ProductoConsumo1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProductoConsumo1")
print('carga ProductoConsumo1', df_ProductoConsumo1.count())   
#Se muestra ProductoConsumo2
df_ProductoConsumo2 = spark.sql(f"""
WITH RankedData AS (
    SELECT ComplexEntityNo,pk_semanavida,nproductoconsumo,ROW_NUMBER AS row_num
    FROM {database_name_tmp}.ProductoConsumo1
    order by ComplexEntityNo,pk_semanavida,nproductoconsumo
)
SELECT 
    ComplexEntityNo,pk_semanavida,
    -- Concatenar valores de 'nproductoconsumo'
    CONCAT_WS(',', COLLECT_LIST(nproductoconsumo)) AS MedSem,
    -- Concatenar valores de 'row_num'
    CONCAT_WS(',', COLLECT_LIST(CAST(row_num AS STRING))) AS DiasMedSem
FROM RankedData
GROUP BY ComplexEntityNo, pk_semanavida
ORDER BY pk_semanavida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProductoConsumo2"
}
df_ProductoConsumo2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProductoConsumo2")
print('carga ProductoConsumo2', df_ProductoConsumo2.count())   
#Se muestra Medicaciones
df_Medicaciones = spark.sql(f"""
WITH ProductoConsumo AS (SELECT ComplexEntityNo,pk_semanavida,MedSem,DiasMedSem FROM {database_name_tmp}.ProductoConsumo2)
SELECT A.ComplexEntityNo,
    MAX(CASE WHEN A.pk_semanavida = 2 THEN A.MedSem END) AS MedSem1,
    MAX(CASE WHEN A.pk_semanavida = 3 THEN A.MedSem END) AS MedSem2,
    MAX(CASE WHEN A.pk_semanavida = 4 THEN A.MedSem END) AS MedSem3,
    MAX(CASE WHEN A.pk_semanavida = 5 THEN A.MedSem END) AS MedSem4,
    MAX(CASE WHEN A.pk_semanavida = 6 THEN A.MedSem END) AS MedSem5,
    MAX(CASE WHEN A.pk_semanavida = 7 THEN A.MedSem END) AS MedSem6,
    MAX(CASE WHEN A.pk_semanavida = 8 THEN A.MedSem END) AS MedSem7,
    MAX(CASE WHEN B.pk_semanavida = 2 THEN B.DiasMedSem END) AS DiasMedSem1,
    MAX(CASE WHEN B.pk_semanavida = 3 THEN B.DiasMedSem END) AS DiasMedSem2,
    MAX(CASE WHEN B.pk_semanavida = 4 THEN B.DiasMedSem END) AS DiasMedSem3,
    MAX(CASE WHEN B.pk_semanavida = 5 THEN B.DiasMedSem END) AS DiasMedSem4,
    MAX(CASE WHEN B.pk_semanavida = 6 THEN B.DiasMedSem END) AS DiasMedSem5,
    MAX(CASE WHEN B.pk_semanavida = 7 THEN B.DiasMedSem END) AS DiasMedSem6,
    MAX(CASE WHEN B.pk_semanavida = 8 THEN B.DiasMedSem END) AS DiasMedSem7
FROM {database_name_tmp}.ProductoConsumo2 A
LEFT JOIN {database_name_tmp}.ProductoConsumo2 B 
    ON A.ComplexEntityNo = B.ComplexEntityNo AND A.pk_semanavida = B.pk_semanavida
GROUP BY A.ComplexEntityNo
ORDER BY A.ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Medicaciones"
}
df_Medicaciones.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Medicaciones")
print('carga Medicaciones', df_Medicaciones.count())   
#Se muestra ProduccionDetalleCorral
df_ProduccionDetalleCorral = spark.sql(f"""
WITH ProduccionCierre AS (SELECT pk_lote,MAX(FechaCierre) AS FechaCierreLote FROM {database_name_tmp}.ProduccionDetalleEdad GROUP BY pk_lote),
RuidosTotales AS (SELECT ComplexEntityNo,COUNT(U_RuidosTotales) AS CountRuidosTotales FROM {database_name_tmp}.ProduccionDetalleEdad WHERE U_RuidosTotales <> 0 GROUP BY ComplexEntityNo)

SELECT   MAX(B.fecha) AS fecha
		,B.pk_empresa
		,B.pk_division
		,B.pk_zona
		,B.pk_subzona
		,B.pk_plantel
		,B.pk_lote
		,B.pk_galpon
		,B.pk_sexo 
		,B.pk_standard
		,B.pk_producto
		,B.pk_tipoproducto
		,B.pk_especie
		,B.pk_estado
		,B.pk_administrador
		,MAX(B.pk_proveedor) pk_proveedor
		,MAX(B.pk_diasvida) AS pk_diasvida
		,B.ComplexEntityNo
		,B.FechaNacimiento
		,B.FechaCierre
		,P.FechaCierreLote
		,B.FechaAlojamiento,B.FechaCrianza,B.FechaInicioGranja,B.FechaInicioSaca,B.FechaFinSaca
		,AVG(B.AreaGalpon) AS AreaGalpon
		,AVG(B.PobInicial) AS PobInicial
		,AVG(B.AvesRendidas) AS AvesRendidas
		,AVG(B.KilosRendidos) AS KilosRendidos
		,MAX(B.MortAcum) AS MortDia
		,CASE WHEN AVG(B.PobInicial) = 0 THEN 0.0 ELSE (((MAX(B.MortAcum) / AVG(B.PobInicial*1.0))*100)) END AS PorMort
		,AVG(B.MortSem1) AS MortSem1
		,AVG(B.MortSem2) AS MortSem2
		,AVG(B.MortSem3) AS MortSem3
		,AVG(B.MortSem4) AS MortSem4
		,AVG(B.MortSem5) AS MortSem5
		,AVG(B.MortSem6) AS MortSem6
		,AVG(B.MortSem7) AS MortSem7
		,AVG(B.MortSem8) AS MortSem8
		,AVG(B.MortSem9) AS MortSem9
		,AVG(B.MortSem10) AS MortSem10
		,AVG(B.MortSem11) AS MortSem11
		,AVG(B.MortSem12) AS MortSem12
		,AVG(B.MortSem13) AS MortSem13
		,AVG(B.MortSem14) AS MortSem14
		,AVG(B.MortSem15) AS MortSem15
		,AVG(B.MortSem16) AS MortSem16
		,AVG(B.MortSem17) AS MortSem17
		,AVG(B.MortSem18) AS MortSem18
		,AVG(B.MortSem19) AS MortSem19
		,AVG(B.MortSem20) AS MortSem20
		,AVG(B.MortSemAcum1) AS MortSemAcum1 
		,AVG(B.MortSemAcum2) AS MortSemAcum2 
		,AVG(B.MortSemAcum3) AS MortSemAcum3
		,AVG(B.MortSemAcum4) AS MortSemAcum4 
		,AVG(B.MortSemAcum5) AS MortSemAcum5 
		,AVG(B.MortSemAcum6) AS MortSemAcum6 
		,AVG(B.MortSemAcum7) AS MortSemAcum7 
		,AVG(B.MortSemAcum8) AS MortSemAcum8 
		,AVG(B.MortSemAcum9) AS MortSemAcum9 
		,AVG(B.MortSemAcum10) AS MortSemAcum10 
		,AVG(B.MortSemAcum11) AS MortSemAcum11 
		,AVG(B.MortSemAcum12) AS MortSemAcum12 
		,AVG(B.MortSemAcum13) AS MortSemAcum13 
		,AVG(B.MortSemAcum14) AS MortSemAcum14 
		,AVG(B.MortSemAcum15) AS MortSemAcum15 
		,AVG(B.MortSemAcum16) AS MortSemAcum16 
		,AVG(B.MortSemAcum17) AS MortSemAcum17 
		,AVG(B.MortSemAcum18) AS MortSemAcum18 
		,AVG(B.MortSemAcum19) AS MortSemAcum19 
		,AVG(B.MortSemAcum20) AS MortSemAcum20 
		,MAX(B.Peso) AS Peso
		,AVG(B.PesoSem) AS PesoSem
		,AVG(B.PesoSem1) AS PesoSem1
		,AVG(B.PesoSem2) AS PesoSem2
		,AVG(B.PesoSem3) AS PesoSem3
		,AVG(B.PesoSem4) AS PesoSem4
		,AVG(B.PesoSem5) AS PesoSem5
		,AVG(B.PesoSem6) AS PesoSem6
		,AVG(B.PesoSem7) AS PesoSem7
		,AVG(B.PesoSem8) AS PesoSem8
		,AVG(B.PesoSem9) AS PesoSem9
		,AVG(B.PesoSem10) AS PesoSem10
		,AVG(B.PesoSem11) AS PesoSem11
		,AVG(B.PesoSem12) AS PesoSem12
		,AVG(B.PesoSem13) AS PesoSem13
		,AVG(B.PesoSem14) AS PesoSem14
		,AVG(B.PesoSem15) AS PesoSem15
		,AVG(B.PesoSem16) AS PesoSem16
		,AVG(B.PesoSem17) AS PesoSem17
		,AVG(B.PesoSem18) AS PesoSem18
		,AVG(B.PesoSem19) AS PesoSem19
		,AVG(B.PesoSem20) AS PesoSem20
		,AVG(B.Peso5Dias) AS Peso5Dias
		,AVG(B.PesoAlo) AS PesoAlo
		,AVG(B.PesoHvo) AS PesoHvo
		,AVG(B.PesoAlo) * AVG(B.PobInicial) AS PesoAloXPobInicial
		,AVG(B.PesoHvo) * AVG(B.PobInicial) AS PesoHvoXPobInicial
		,AVG(B.PesoSem1) * AVG(B.PobInicial) AS PesoSem1XPobInicial
		,AVG(B.PesoSem2) * AVG(B.PobInicial) AS PesoSem2XPobInicial
		,AVG(B.PesoSem3) * AVG(B.PobInicial) AS PesoSem3XPobInicial
		,AVG(B.PesoSem4) * AVG(B.PobInicial) AS PesoSem4XPobInicial
		,AVG(B.PesoSem5) * AVG(B.PobInicial) AS PesoSem5XPobInicial
		,AVG(B.PesoSem6) * AVG(B.PobInicial) AS PesoSem6XPobInicial
		,AVG(B.PesoSem7) * AVG(B.PobInicial) AS PesoSem7XPobInicial
		,AVG(B.PesoSem8) * AVG(B.PobInicial) AS PesoSem8XPobInicial
		,AVG(B.PesoSem9) * AVG(B.PobInicial) AS PesoSem9XPobInicial
		,AVG(B.PesoSem10) * AVG(B.PobInicial) AS PesoSem10XPobInicial
		,AVG(B.PesoSem11) * AVG(B.PobInicial) AS PesoSem11XPobInicial
		,AVG(B.PesoSem12) * AVG(B.PobInicial) AS PesoSem12XPobInicial
		,AVG(B.PesoSem13) * AVG(B.PobInicial) AS PesoSem13XPobInicial
		,AVG(B.PesoSem14) * AVG(B.PobInicial) AS PesoSem14XPobInicial
		,AVG(B.PesoSem15) * AVG(B.PobInicial) AS PesoSem15XPobInicial
		,AVG(B.PesoSem16) * AVG(B.PobInicial) AS PesoSem16XPobInicial
		,AVG(B.PesoSem17) * AVG(B.PobInicial) AS PesoSem17XPobInicial
		,AVG(B.PesoSem18) * AVG(B.PobInicial) AS PesoSem18XPobInicial
		,AVG(B.PesoSem19) * AVG(B.PobInicial) AS PesoSem19XPobInicial
		,AVG(B.PesoSem20) * AVG(B.PobInicial) AS PesoSem20XPobInicial
		,AVG(B.Peso5Dias) * AVG(B.PobInicial) AS Peso5DiasXPobInicial
		,CASE WHEN AVG(AvesRendidas) = 0 THEN 0.0 ELSE floor((AVG(KilosRendidos)/AVG(AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
		,AVG(STDPeso) AS STDPeso
		,AVG(UnidSeleccion) AS UnidSeleccion
		,SUM(ConsDia) AS ConsDia
		,SUM(PreInicio) AS PreInicio
		,SUM(Inicio) AS Inicio
		,SUM(Acabado) AS Acabado
		,SUM(Terminado) AS Terminado
		,SUM(Finalizador) AS Finalizador
		,SUM(PavoIni) AS PavoIni
		,SUM(Pavo1) AS Pavo1
		,SUM(Pavo2) AS Pavo2
		,SUM(Pavo3) AS Pavo3
		,SUM(Pavo4) AS Pavo4
		,SUM(Pavo5) AS Pavo5
		,SUM(Pavo6) AS Pavo6
		,SUM(Pavo7) AS Pavo7
        ,CASE WHEN AVG(KilosRendidos) = 0 THEN 0.0 ELSE  floor((cast(SUM(ConsDia) as decimal(30,10))/cast(AVG(KilosRendidos) as decimal(30,10))) * 1000000) / 1000000 END AS ICA
		,AVG(CantInicioSaca) AS CantInicioSaca
		,AVG(EdadInicioSaca) AS EdadInicioSaca
		,AVG(EdadGranjaCorral) AS EdadGranjaCorral
		,AVG(EdadGranjaGalpon) AS EdadGranjaGalpon
		,AVG(EdadGranjaLote) AS EdadGranjaLote
		,AVG(Gas) AS Gas
		,AVG(Cama) AS Cama
		,AVG(Agua) AS Agua
		,CASE WHEN B.pk_sexo = 2 THEN AVG(B.PobInicial) ELSE 0 END AS CantMacho
		,AVG(Pigmentacion) AS Pigmentacion
		,B.PadreMayor
		,B.RazaMayor
		,B.IncubadoraMayor
		,MAX(B.PorcPadreMayor) PorcPadreMayor
		,MAX(B.PorcRazaMayor)PorcRazaMayor
		,MAX(B.PorcIncMayor) PorcIncMayor
		,B.categoria
		,MAX(B.FlagAtipico) FlagAtipico
		,MAX(B.EdadPadreCorral) EdadPadreCorral
		,MAX(B.EdadPadreGalpon) EdadPadreGalpon
		,MAX(B.EdadPadreLote) EdadPadreLote
		,MAX(STDMortAcum) AS STDMortAcum
		,MAX(STDMortAcum) * AVG(B.PobInicial) AS PobInicialXSTDMortAcumC
		,MAX(STDPorcMortAcumC) STDPorcMortAcumC
		,MAX(STDPorcMortAcumG) STDPorcMortAcumG
		,MAX(STDPorcMortAcumL) STDPorcMortAcumL
		,MAX(STDPesoC) STDPesoC
		,MAX(STDPesoG) STDPesoG
		,MAX(STDPesoL) STDPesoL
		,MAX(STDConsAcumC) STDConsAcumC
		,MAX(STDConsAcumG) STDConsAcumG
		,MAX(STDConsAcumL) STDConsAcumL
		,MAX(STDPorcMortSem1) STDPorcMortSem1
		,MAX(STDPorcMortSem2) STDPorcMortSem2
		,MAX(STDPorcMortSem3) STDPorcMortSem3
		,MAX(STDPorcMortSem4) STDPorcMortSem4
		,MAX(STDPorcMortSem5) STDPorcMortSem5
		,MAX(STDPorcMortSem6) STDPorcMortSem6
		,MAX(STDPorcMortSem7) STDPorcMortSem7
		,MAX(STDPorcMortSem8) STDPorcMortSem8
		,MAX(STDPorcMortSem9) STDPorcMortSem9
		,MAX(STDPorcMortSem10) STDPorcMortSem10
		,MAX(STDPorcMortSem11) STDPorcMortSem11
		,MAX(STDPorcMortSem12) STDPorcMortSem12
		,MAX(STDPorcMortSem13) STDPorcMortSem13
		,MAX(STDPorcMortSem14) STDPorcMortSem14
		,MAX(STDPorcMortSem15) STDPorcMortSem15
		,MAX(STDPorcMortSem16) STDPorcMortSem16
		,MAX(STDPorcMortSem17) STDPorcMortSem17
		,MAX(STDPorcMortSem18) STDPorcMortSem18
		,MAX(STDPorcMortSem19) STDPorcMortSem19
		,MAX(STDPorcMortSem20) STDPorcMortSem20
		,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
		,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
		,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
		,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
		,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
		,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
		,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
		,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
		,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
		,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
		,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
		,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
		,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
		,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
		,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
		,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
		,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
		,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
		,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
		,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
		,MAX(STDPorcMortSem1G) STDPorcMortSem1G
		,MAX(STDPorcMortSem2G) STDPorcMortSem2G
		,MAX(STDPorcMortSem3G) STDPorcMortSem3G
		,MAX(STDPorcMortSem4G) STDPorcMortSem4G
		,MAX(STDPorcMortSem5G) STDPorcMortSem5G
		,MAX(STDPorcMortSem6G) STDPorcMortSem6G
		,MAX(STDPorcMortSem7G) STDPorcMortSem7G
		,MAX(STDPorcMortSem8G) STDPorcMortSem8G
		,MAX(STDPorcMortSem9G) STDPorcMortSem9G
		,MAX(STDPorcMortSem10G) STDPorcMortSem10G
		,MAX(STDPorcMortSem11G) STDPorcMortSem11G
		,MAX(STDPorcMortSem12G) STDPorcMortSem12G
		,MAX(STDPorcMortSem13G) STDPorcMortSem13G
		,MAX(STDPorcMortSem14G) STDPorcMortSem14G
		,MAX(STDPorcMortSem15G) STDPorcMortSem15G
		,MAX(STDPorcMortSem16G) STDPorcMortSem16G
		,MAX(STDPorcMortSem17G) STDPorcMortSem17G
		,MAX(STDPorcMortSem18G) STDPorcMortSem18G
		,MAX(STDPorcMortSem19G) STDPorcMortSem19G
		,MAX(STDPorcMortSem20G) STDPorcMortSem20G
		,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1G
		,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2G
		,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3G
		,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4G
		,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5G
		,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6G
		,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7G
		,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8G
		,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9G
		,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10G
		,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11G
		,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12G
		,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13G
		,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14G
		,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15G
		,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16G
		,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17G
		,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18G
		,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19G
		,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20G
		,MAX(STDPorcMortSem1L) STDPorcMortSem1L
		,MAX(STDPorcMortSem2L) STDPorcMortSem2L
		,MAX(STDPorcMortSem3L) STDPorcMortSem3L
		,MAX(STDPorcMortSem4L) STDPorcMortSem4L
		,MAX(STDPorcMortSem5L) STDPorcMortSem5L
		,MAX(STDPorcMortSem6L) STDPorcMortSem6L
		,MAX(STDPorcMortSem7L) STDPorcMortSem7L
		,MAX(STDPorcMortSem8L) STDPorcMortSem8L
		,MAX(STDPorcMortSem9L) STDPorcMortSem9L
		,MAX(STDPorcMortSem10L) STDPorcMortSem10L
		,MAX(STDPorcMortSem11L) STDPorcMortSem11L
		,MAX(STDPorcMortSem12L) STDPorcMortSem12L
		,MAX(STDPorcMortSem13L) STDPorcMortSem13L
		,MAX(STDPorcMortSem14L) STDPorcMortSem14L
		,MAX(STDPorcMortSem15L) STDPorcMortSem15L
		,MAX(STDPorcMortSem16L) STDPorcMortSem16L
		,MAX(STDPorcMortSem17L) STDPorcMortSem17L
		,MAX(STDPorcMortSem18L) STDPorcMortSem18L
		,MAX(STDPorcMortSem19L) STDPorcMortSem19L
		,MAX(STDPorcMortSem20L) STDPorcMortSem20L
		,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1L
		,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2L
		,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3L
		,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4L
		,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5L
		,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6L
		,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7L
		,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8L
		,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9L
		,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10L
		,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11L
		,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12L
		,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13L
		,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14L
		,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15L
		,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16L
		,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17L
		,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18L
		,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19L
		,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20L
		,MAX(STDPesoSem1)STDPesoSem1
		,MAX(STDPesoSem2)STDPesoSem2
		,MAX(STDPesoSem3)STDPesoSem3
		,MAX(STDPesoSem4)STDPesoSem4
		,MAX(STDPesoSem5)STDPesoSem5
		,MAX(STDPesoSem6)STDPesoSem6
		,MAX(STDPesoSem7)STDPesoSem7
		,MAX(STDPesoSem8)STDPesoSem8
		,MAX(STDPesoSem9)STDPesoSem9
		,MAX(STDPesoSem10)STDPesoSem10
		,MAX(STDPesoSem11)STDPesoSem11
		,MAX(STDPesoSem12)STDPesoSem12
		,MAX(STDPesoSem13)STDPesoSem13
		,MAX(STDPesoSem14)STDPesoSem14
		,MAX(STDPesoSem15)STDPesoSem15
		,MAX(STDPesoSem16)STDPesoSem16
		,MAX(STDPesoSem17)STDPesoSem17
		,MAX(STDPesoSem18)STDPesoSem18
		,MAX(STDPesoSem19)STDPesoSem19
		,MAX(STDPesoSem20)STDPesoSem20
		,MAX(STDPeso5Dias)STDPeso5Dias
		,MAX(STDPorcPesoSem1G)STDPorcPesoSem1G
		,MAX(STDPorcPesoSem2G)STDPorcPesoSem2G
		,MAX(STDPorcPesoSem3G)STDPorcPesoSem3G
		,MAX(STDPorcPesoSem4G)STDPorcPesoSem4G
		,MAX(STDPorcPesoSem5G)STDPorcPesoSem5G
		,MAX(STDPorcPesoSem6G)STDPorcPesoSem6G
		,MAX(STDPorcPesoSem7G)STDPorcPesoSem7G
		,MAX(STDPorcPesoSem8G)STDPorcPesoSem8G
		,MAX(STDPorcPesoSem9G)STDPorcPesoSem9G
		,MAX(STDPorcPesoSem10G)STDPorcPesoSem10G
		,MAX(STDPorcPesoSem11G)STDPorcPesoSem11G
		,MAX(STDPorcPesoSem12G)STDPorcPesoSem12G
		,MAX(STDPorcPesoSem13G)STDPorcPesoSem13G
		,MAX(STDPorcPesoSem14G)STDPorcPesoSem14G
		,MAX(STDPorcPesoSem15G)STDPorcPesoSem15G
		,MAX(STDPorcPesoSem16G)STDPorcPesoSem16G
		,MAX(STDPorcPesoSem17G)STDPorcPesoSem17G
		,MAX(STDPorcPesoSem18G)STDPorcPesoSem18G
		,MAX(STDPorcPesoSem19G)STDPorcPesoSem19G
		,MAX(STDPorcPesoSem20G)STDPorcPesoSem20G
		,MAX(STDPorcPeso5DiasG)STDPorcPeso5DiasG
		,MAX(STDPorcPesoSem1L)STDPorcPesoSem1L
		,MAX(STDPorcPesoSem2L)STDPorcPesoSem2L
		,MAX(STDPorcPesoSem3L)STDPorcPesoSem3L
		,MAX(STDPorcPesoSem4L)STDPorcPesoSem4L
		,MAX(STDPorcPesoSem5L)STDPorcPesoSem5L
		,MAX(STDPorcPesoSem6L)STDPorcPesoSem6L
		,MAX(STDPorcPesoSem7L)STDPorcPesoSem7L
		,MAX(STDPorcPesoSem8L)STDPorcPesoSem8L
		,MAX(STDPorcPesoSem9L)STDPorcPesoSem9L
		,MAX(STDPorcPesoSem10L)STDPorcPesoSem10L
		,MAX(STDPorcPesoSem11L)STDPorcPesoSem11L
		,MAX(STDPorcPesoSem12L)STDPorcPesoSem12L
		,MAX(STDPorcPesoSem13L)STDPorcPesoSem13L
		,MAX(STDPorcPesoSem14L)STDPorcPesoSem14L
		,MAX(STDPorcPesoSem15L)STDPorcPesoSem15L
		,MAX(STDPorcPesoSem16L)STDPorcPesoSem16L
		,MAX(STDPorcPesoSem17L)STDPorcPesoSem17L
		,MAX(STDPorcPesoSem18L)STDPorcPesoSem18L
		,MAX(STDPorcPesoSem19L)STDPorcPesoSem19L
		,MAX(STDPorcPesoSem20L)STDPorcPesoSem20L
		,MAX(STDPorcPeso5DiasL)STDPorcPeso5DiasL
		,MAX(STDICAC) STDICAC
		,MAX(STDICAG) STDICAG
		,MAX(STDICAL) STDICAL
		,MAX(B.PavosBBMortIncub) AS PavosBBMortIncub
		,B.EdadPadreCorralDescrip
		,MAX(DiasAloj) DiasAloj
		,sum(B.U_RuidosTotales) U_RuidosTotales
		,R.CountRuidosTotales
		,MAX(ConsSem1) ConsSem1
		,MAX(ConsSem2) ConsSem2
		,MAX(ConsSem3) ConsSem3
		,MAX(ConsSem4) ConsSem4
		,MAX(ConsSem5) ConsSem5
		,MAX(ConsSem6) ConsSem6
		,MAX(ConsSem7) ConsSem7
		,MAX(ConsSem8) ConsSem8
		,MAX(ConsSem9) ConsSem9
		,MAX(ConsSem10) ConsSem10
		,MAX(ConsSem11) ConsSem11
		,MAX(ConsSem12) ConsSem12
		,MAX(ConsSem13) ConsSem13
		,MAX(ConsSem14) ConsSem14
		,MAX(ConsSem15) ConsSem15
		,MAX(ConsSem16) ConsSem16
		,MAX(ConsSem17) ConsSem17
		,MAX(ConsSem18) ConsSem18
		,MAX(ConsSem19) ConsSem19
		,MAX(ConsSem20) ConsSem20
		,MAX(STDPorcConsSem1) STDPorcConsSem1
		,MAX(STDPorcConsSem2) STDPorcConsSem2
		,MAX(STDPorcConsSem3) STDPorcConsSem3
		,MAX(STDPorcConsSem4) STDPorcConsSem4
		,MAX(STDPorcConsSem5) STDPorcConsSem5
		,MAX(STDPorcConsSem6) STDPorcConsSem6
		,MAX(STDPorcConsSem7) STDPorcConsSem7
		,MAX(STDConsSem1) STDConsSem1
		,MAX(STDConsSem2) STDConsSem2
		,MAX(STDConsSem3) STDConsSem3
		,MAX(STDConsSem4) STDConsSem4
		,MAX(STDConsSem5) STDConsSem5
		,MAX(STDConsSem6) STDConsSem6
		,MAX(STDConsSem7) STDConsSem7
		,MAX(CantPrimLesion) CantPrimLesion
		,MAX(CantSegLesion) CantSegLesion
		,MAX(CantTerLesion) CantTerLesion
		,MAX(PorcPrimLesion) PorcPrimLesion
		,MAX(PorcSegLesion) PorcSegLesion
		,MAX(PorcTerLesion) PorcTerLesion
		,NomPrimLesion
		,NomSegLesion
		,NomTerLesion
		,MAX(DiasSacaEfectivo) DiasSacaEfectivo
		,MAX(D.U_PEAccidentados) U_PEAccidentados
		,MAX(D.U_PEHigadoGraso) U_PEHigadoGraso
		,MAX(D.U_PEHepatomegalia) U_PEHepatomegalia
		,MAX(D.U_PEHigadoHemorragico) U_PEHigadoHemorragico
		,MAX(D.U_PEInanicion) U_PEInanicion
		,MAX(D.U_PEProblemaRespiratorio) U_PEProblemaRespiratorio
		,MAX(D.U_PESCH) U_PESCH
		,MAX(D.U_PEEnteritis) U_PEEnteritis
		,MAX(D.U_PEAscitis) U_PEAscitis
		,MAX(D.U_PEMuerteSubita) U_PEMuerteSubita
		,MAX(D.U_PEEstresPorCalor) U_PEEstresPorCalor
		,MAX(D.U_PEHidropericardio) U_PEHidropericardio
		,MAX(D.U_PEHemopericardio) U_PEHemopericardio
		,MAX(D.U_PEUratosis) U_PEUratosis
		,MAX(D.U_PEMaterialCaseoso) U_PEMaterialCaseoso
		,MAX(D.U_PEOnfalitis) U_PEOnfalitis
		,MAX(D.U_PERetencionDeYema) U_PERetencionDeYema
		,MAX(D.U_PEErosionDeMolleja) U_PEErosionDeMolleja
		,MAX(D.U_PEHemorragiaMusculos)U_PEHemorragiaMusculos
		,MAX(D.U_PESangreEnCiego) U_PESangreEnCiego
		,MAX(D.U_PEPericarditis) U_PEPericarditis
		,MAX(D.U_PEPeritonitis) U_PEPeritonitis
		,MAX(D.U_PEProlapso)U_PEProlapso
		,MAX(D.U_PEPicaje) U_PEPicaje
		,MAX(D.U_PERupturaAortica) U_PERupturaAortica
		,MAX(D.U_PEBazoMoteado) U_PEBazoMoteado
		,MAX(D.U_PENoViable) U_PENoViable
		,MAX(D.U_PEAerosaculitisG2) U_PEAerosaculitisG2
		,MAX(D.U_PECojera) U_PECojera
		,MAX(D.U_PEHigadoIcterico)U_PEHigadoIcterico
		,MAX(D.U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra
		,MAX(D.U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr
		,MAX(D.U_PENecrosisHepatica) U_PENecrosisHepatica
		,MAX(D.U_PENeumonia) U_PENeumonia
		,MAX(D.U_PESepticemia) U_PESepticemia
		,MAX(D.U_PEVomitoNegro) U_PEVomitoNegro
		,MAX(D.PorcAccidentados) PorcAccidentados
		,MAX(D.PorcHigadoGraso) PorcHigadoGraso
		,MAX(D.PorcHepatomegalia) PorcHepatomegalia
		,MAX(D.PorcHigadoHemorragico) PorcHigadoHemorragico
		,MAX(D.PorcInanicion) PorcInanicion
		,MAX(D.PorcProblemaRespiratorio) PorcProblemaRespiratorio
		,MAX(D.PorcSCH) PorcSCH
		,MAX(D.PorcEnteritis) PorcEnteritis
		,MAX(D.PorcAscitis) PorcAscitis
		,MAX(D.PorcMuerteSubita) PorcMuerteSubita
		,MAX(D.PorcEstresPorCalor) PorcEstresPorCalor
		,MAX(D.PorcHidropericardio) PorcHidropericardio
		,MAX(D.PorcHemopericardio) PorcHemopericardio
		,MAX(D.PorcUratosis) PorcUratosis
		,MAX(D.PorcMaterialCaseoso) PorcMaterialCaseoso
		,MAX(D.PorcOnfalitis) PorcOnfalitis
		,MAX(D.PorcRetencionDeYema) PorcRetencionDeYema
		,MAX(D.PorcErosionDeMolleja) PorcErosionDeMolleja
		,MAX(D.PorcHemorragiaMusculos) PorcHemorragiaMusculos
		,MAX(D.PorcSangreEnCiego) PorcSangreEnCiego
		,MAX(D.PorcPericarditis)PorcPericarditis
		,MAX(D.PorcPeritonitis) PorcPeritonitis
		,MAX(D.PorcProlapso) PorcProlapso
		,MAX(D.PorcPicaje) PorcPicaje
		,MAX(D.PorcRupturaAortica) PorcRupturaAortica
		,MAX(D.PorcBazoMoteado) PorcBazoMoteado
		,MAX(D.PorcNoViable) PorcNoViable
		,MAX(D.PorcAerosaculitisG2) PorcAerosaculitisG2
		,MAX(D.PorcCojera) PorcCojera
		,MAX(D.PorcHigadoIcterico) PorcHigadoIcterico
		,MAX(D.PorcMaterialCaseoso_po1ra) PorcMaterialCaseoso_po1ra
		,MAX(D.PorcMaterialCaseosoMedRetr) PorcMaterialCaseosoMedRetr
		,MAX(D.PorcNecrosisHepatica) PorcNecrosisHepatica
		,MAX(D.PorcNeumonia) PorcNeumonia
		,MAX(D.PorcSepticemia) PorcSepticemia
		,MAX(D.PorcVomitoNegro) PorcVomitoNegro
		,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInviernoC
		,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInviernoG
		,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInviernoL
		,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVeranoC
		,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVeranoG
		,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVeranoL
		,MedSem1
		,MedSem2
		,MedSem3
		,MedSem4
		,MedSem5
		,MedSem6
		,MedSem7
		,MAX(DiasMedSem1) DiasMedSem1
		,MAX(DiasMedSem2) DiasMedSem2
		,MAX(DiasMedSem3) DiasMedSem3
		,MAX(DiasMedSem4) DiasMedSem4
		,MAX(DiasMedSem5) DiasMedSem5
		,MAX(DiasMedSem6) DiasMedSem6
		,MAX(DiasMedSem7) DiasMedSem7
		,MAX(PorcAlojamientoXEdadPadre) PorcAlojamientoXEdadPadre
		,B.TipoOrigen
		,MAX(B.NoViableSem1) NoViableSem1
		,MAX(B.NoViableSem2) NoViableSem2
		,MAX(B.NoViableSem3) NoViableSem3
		,MAX(B.NoViableSem4) NoViableSem4
		,MAX(B.NoViableSem5) NoViableSem5
		,MAX(B.NoViableSem6) NoViableSem6
		,MAX(B.NoViableSem7) NoViableSem7
		,MAX(B.PorcNoViableSem1) PorcNoViableSem1
		,MAX(B.PorcNoViableSem2) PorcNoViableSem2
		,MAX(B.PorcNoViableSem3) PorcNoViableSem3
		,MAX(B.PorcNoViableSem4) PorcNoViableSem4
		,MAX(B.PorcNoViableSem5) PorcNoViableSem5
		,MAX(B.PorcNoViableSem6) PorcNoViableSem6
		,MAX(B.PorcNoViableSem7) PorcNoViableSem7
		,MAX(B.PorcNoViableSem1) * AVG(B.PobInicial) AS PorcNoViableSem1XPobInicial
		,MAX(B.PorcNoViableSem2) * AVG(B.PobInicial) AS PorcNoViableSem2XPobInicial
		,MAX(B.PorcNoViableSem3) * AVG(B.PobInicial) AS PorcNoViableSem3XPobInicial
		,MAX(B.PorcNoViableSem4) * AVG(B.PobInicial) AS PorcNoViableSem4XPobInicial
		,MAX(B.PorcNoViableSem5) * AVG(B.PobInicial) AS PorcNoViableSem5XPobInicial
		,MAX(B.PorcNoViableSem6) * AVG(B.PobInicial) AS PorcNoViableSem6XPobInicial
		,MAX(B.PorcNoViableSem7) * AVG(B.PobInicial) AS PorcNoViableSem7XPobInicial
		,MAX(B.NoViableSem8) NoViableSem8
		,MAX(B.PorcNoViableSem8) PorcNoViableSem8
		,MAX(B.PorcNoViableSem8) * AVG(B.PobInicial) AS PorcNoViableSem8XPobInicial
		,B.pk_tipogranja
		,MAX(B.CV) CV
FROM {database_name_tmp}.ProduccionDetalleEdad B
left join {database_name_tmp}.OrdenarMortalidadesCorral2 C on B.ComplexEntityNo = C.ComplexEntityNo
LEFT JOIN {database_name_gl}.ft_mortalidad_Corral D on B.ComplexEntityNo = D.ComplexEntityNo and B.FlagAtipico = D.FlagAtipico 
left join {database_name_tmp}.Medicaciones E on B.ComplexEntityNo = E.ComplexEntityNo
LEFT JOIN ProduccionCierre P ON B.pk_lote = P.pk_lote
LEFT JOIN RuidosTotales R ON B.ComplexEntityNo = R.ComplexEntityNo
GROUP BY B.pk_empresa,B.pk_division,B.pk_zona,B.pk_subzona,B.pk_plantel,B.pk_lote,B.pk_galpon,B.pk_sexo,B.pk_standard,B.pk_producto,B.pk_tipoproducto
,B.pk_especie,B.pk_estado,B.pk_administrador,B.ComplexEntityNo,B.FechaNacimiento
,B.FechaCierre,B.FechaAlojamiento,B.FechaCrianza,B.FechaInicioGranja,B.FechaInicioSaca
,B.FechaFinSaca,B.PadreMayor,B.RazaMayor,B.IncubadoraMayor,B.categoria,B.EdadPadreCorralDescrip,R.CountRuidosTotales,P.FechaCierreLote
,NomPrimLesion,NomSegLesion,NomTerLesion,MedSem1,MedSem2,MedSem3,MedSem4,MedSem5,MedSem6,MedSem7,B.TipoOrigen,B.pk_tipogranja
""")

#df_ProduccionDetalleCorral = df_ProduccionDetalleCorral.withColumn('ICA', col('ICA').cast(DecimalType(38,8)))\

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleCorral"
}
df_ProduccionDetalleCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleCorral")
print('carga ProduccionDetalleCorral', df_ProduccionDetalleCorral.count())   
#Se muestra ProduccionDetalleGalpon
df_ProduccionDetalleGalpon = spark.sql(f"""
WITH PobInicialSuma AS (SELECT pk_lote,pk_galpon,SUM(PobInicial) AS PobInicialTotal FROM {database_name_tmp}.ProduccionDetalleCorral GROUP BY pk_lote, pk_galpon),
PobotroSuma AS (SELECT pk_lote,pk_galpon,SUM(PesoAloXPobInicial) AS PesoAloXPobInicial,SUM(PesoHvoXPobInicial) AS PesoHvoXPobInicial,SUM(PobInicialXSTDMortAcumC) AS PobInicialXSTDMortAcumC FROM {database_name_tmp}.ProduccionDetalleCorral GROUP BY pk_lote, pk_galpon),
PorcNoViable AS (SELECT pk_lote,pk_galpon, 
        SUM(PorcNoViableSem1XPobInicial) AS PorcNoViableSem1XPobInicial,
        SUM(PorcNoViableSem2XPobInicial) AS PorcNoViableSem2XPobInicial,
        SUM(PorcNoViableSem3XPobInicial) AS PorcNoViableSem3XPobInicial,
        SUM(PorcNoViableSem4XPobInicial) AS PorcNoViableSem4XPobInicial,
        SUM(PorcNoViableSem5XPobInicial) AS PorcNoViableSem5XPobInicial,
        SUM(PorcNoViableSem6XPobInicial) AS PorcNoViableSem6XPobInicial,
        SUM(PorcNoViableSem7XPobInicial) AS PorcNoViableSem7XPobInicial,
        SUM(PorcNoViableSem8XPobInicial) AS PorcNoViableSem8XPobInicial
    FROM {database_name_tmp}.ProduccionDetalleCorral
    WHERE 
        PorcNoViableSem1 <> 0 OR 
        PorcNoViableSem2 <> 0 OR 
        PorcNoViableSem3 <> 0 OR 
        PorcNoViableSem4 <> 0 OR 
        PorcNoViableSem5 <> 0 OR 
        PorcNoViableSem6 <> 0 OR 
        PorcNoViableSem7 <> 0 OR
        PorcNoViableSem8 <> 0
    GROUP BY pk_lote, pk_galpon
),
Pesos AS (
    SELECT 
        pk_lote, 
        pk_galpon, 
        SUM(PesoSem1XPobInicial) AS PesoSem1XPobInicial,
        SUM(PesoSem2XPobInicial) AS PesoSem2XPobInicial,
        SUM(PesoSem3XPobInicial) AS PesoSem3XPobInicial,
        SUM(PesoSem4XPobInicial) AS PesoSem4XPobInicial,
        SUM(PesoSem5XPobInicial) AS PesoSem5XPobInicial,
        SUM(PesoSem6XPobInicial) AS PesoSem6XPobInicial,
        SUM(PesoSem7XPobInicial) AS PesoSem7XPobInicial,
        SUM(PesoSem8XPobInicial) AS PesoSem8XPobInicial,
        SUM(PesoSem9XPobInicial) AS PesoSem9XPobInicial,
        SUM(PesoSem10XPobInicial) AS PesoSem10XPobInicial,
        SUM(PesoSem11XPobInicial) AS PesoSem11XPobInicial,
        SUM(PesoSem12XPobInicial) AS PesoSem12XPobInicial,
        SUM(PesoSem13XPobInicial) AS PesoSem13XPobInicial,
        SUM(PesoSem14XPobInicial) AS PesoSem14XPobInicial,
        SUM(PesoSem15XPobInicial) AS PesoSem15XPobInicial,
        SUM(PesoSem16XPobInicial) AS PesoSem16XPobInicial,
        SUM(PesoSem17XPobInicial) AS PesoSem17XPobInicial,
        SUM(PesoSem18XPobInicial) AS PesoSem18XPobInicial,
        SUM(PesoSem19XPobInicial) AS PesoSem19XPobInicial,
        SUM(PesoSem20XPobInicial) AS PesoSem20XPobInicial,        
        SUM(Peso5DiasXPobInicial) AS Peso5DiasXPobInicial
    FROM {database_name_tmp}.ProduccionDetalleCorral
    WHERE 
        PesoSem1 <> 0 OR 
        PesoSem2 <> 0 OR 
        PesoSem3 <> 0 OR 
        PesoSem4 <> 0 OR 
        PesoSem5 <> 0 OR 
        PesoSem6 <> 0 OR 
        PesoSem7 <> 0 OR 
        PesoSem8 <> 0
    GROUP BY pk_lote, pk_galpon
)
SELECT 
		 MAX(fecha) AS fecha
		,a.pk_empresa
		,pk_division
		,pk_zona
		,pk_subzona
		,A.pk_plantel
		,A.pk_lote
		,A.pk_galpon
		,pk_tipoproducto
		,29 AS pk_especie
		,pk_estado
		,pk_administrador
		,MAX(pk_proveedor) pk_proveedor
		,MAX(pk_diasvida) AS pk_diasvida
		,MAX(pk_diasvida) * AVG(PobInicial) AS pk_diasvidaXPobInicial
		,concat(clote,'-',nogalpon) AS ComplexEntityNoGalpon
		,MAX(FechaCierre) AS FechaCierre
		,MIN(FechaAlojamiento) AS FechaAlojamiento
		,MIN(FechaCrianza) AS FechaCrianza
		,MAX(FechaInicioGranja) AS FechaInicioGranja
		,MIN(FechaInicioSaca) AS FechaInicioSaca 
		,MAX(FechaFinSaca) AS FechaFinSaca
		,AVG(AreaGalpon) AS AreaGalpon
		,SUM(PobInicial) AS PobInicial
		,SUM(AvesRendidas) AS AvesRendidas
		,SUM(KilosRendidos) AS KilosRendidos
		,SUM(MortDia) AS MortDia
		,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE ((cast(SUM(MortDia) as integer) / SUM(PobInicial*1.0) )*100) END AS PorMort
		,SUM(MortSem1) AS MortSem1
		,SUM(MortSem2) AS MortSem2
		,SUM(MortSem3) AS MortSem3
		,SUM(MortSem4) AS MortSem4
		,SUM(MortSem5) AS MortSem5
		,SUM(MortSem6) AS MortSem6
		,SUM(MortSem7) AS MortSem7
		,SUM(MortSem8) AS MortSem8
		,SUM(MortSem9) AS MortSem9
		,SUM(MortSem10) AS MortSem10
		,SUM(MortSem11) AS MortSem11
		,SUM(MortSem12) AS MortSem12
		,SUM(MortSem13) AS MortSem13
		,SUM(MortSem14) AS MortSem14
		,SUM(MortSem15) AS MortSem15
		,SUM(MortSem16) AS MortSem16
		,SUM(MortSem17) AS MortSem17
		,SUM(MortSem18) AS MortSem18
		,SUM(MortSem19) AS MortSem19
		,SUM(MortSem20) AS MortSem20
		,SUM(MortSemAcum1) AS MortSemAcum1 
		,SUM(MortSemAcum2) AS MortSemAcum2 
		,SUM(MortSemAcum3) AS MortSemAcum3
		,SUM(MortSemAcum4) AS MortSemAcum4 
		,SUM(MortSemAcum5) AS MortSemAcum5 
		,SUM(MortSemAcum6) AS MortSemAcum6 
		,SUM(MortSemAcum7) AS MortSemAcum7 
		,SUM(MortSemAcum8) AS MortSemAcum8 
		,SUM(MortSemAcum9) AS MortSemAcum9 
		,SUM(MortSemAcum10) AS MortSemAcum10 
		,SUM(MortSemAcum11) AS MortSemAcum11 
		,SUM(MortSemAcum12) AS MortSemAcum12 
		,SUM(MortSemAcum13) AS MortSemAcum13 
		,SUM(MortSemAcum14) AS MortSemAcum14 
		,SUM(MortSemAcum15) AS MortSemAcum15 
		,SUM(MortSemAcum16) AS MortSemAcum16 
		,SUM(MortSemAcum17) AS MortSemAcum17 
		,SUM(MortSemAcum18) AS MortSemAcum18 
		,SUM(MortSemAcum19) AS MortSemAcum19 
		,SUM(MortSemAcum20) AS MortSemAcum20 
		,MAX(Peso) AS Peso
        ,CASE WHEN sum(AvesRendidas) = 0 THEN 0.0 ELSE floor((sum(KilosRendidos)/sum(AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
		,avg(STDPeso) as STDPeso
		,sum(UnidSeleccion) as UnidSeleccion
		,sum(ConsDia) as ConsDia
		,sum(PreInicio) as PreInicio
		,sum(Inicio) as Inicio
		,sum(Acabado) as Acabado
		,sum(Terminado) as Terminado
		,sum(Finalizador) as Finalizador
		,sum(PavoIni) as PavoIni
		,sum(Pavo1) as Pavo1
		,sum(Pavo2) as Pavo2
		,sum(Pavo3) as Pavo3
		,sum(Pavo4) as Pavo4
		,sum(Pavo5) as Pavo5
		,sum(Pavo6) as Pavo6
		,sum(Pavo7) as Pavo7
        ,CASE WHEN SUM(KilosRendidos) = 0 THEN 0.0 ELSE  floor((cast(SUM(ConsDia) as decimal(30,10))/cast(SUM(KilosRendidos) as decimal(30,10))) * 1000000) / 1000000 END AS ICA
		,sum(CantInicioSaca) as CantInicioSaca
		,min(EdadInicioSaca) as EdadInicioSaca
		,avg(EdadGranjaGalpon) as EdadGranjaGalpon
		,avg(EdadGranjaLote) as EdadGranjaLote
		,avg(D.Gas) as Gas
		,avg(D.Cama) as Cama
		,avg(D.Agua) as Agua
		,sum(CantMacho) as CantMacho
		,avg(Pigmentacion) as Pigmentacion
		,categoria
		,max(FlagAtipico) FlagAtipico
		,MAX(EdadPadreGalpon) EdadPadreGalpon
		,MAX(EdadPadreLote) EdadPadreLote
		,MAX(STDPorcMortAcumC) STDPorcMortAcumC
		,MAX(STDPorcMortAcumG) STDPorcMortAcumG
		,MAX(STDPorcMortAcumL) STDPorcMortAcumL
		,MAX(STDPesoC) STDPesoC
		,MAX(STDPesoG) STDPesoG
		,MAX(STDPesoL) STDPesoL
		,MAX(STDConsAcumC) STDConsAcumC
		,MAX(STDConsAcumG) STDConsAcumG
		,MAX(STDConsAcumL) STDConsAcumL
		,MAX(STDPorcMortSem1) STDPorcMortSem1
		,MAX(STDPorcMortSem2) STDPorcMortSem2
		,MAX(STDPorcMortSem3) STDPorcMortSem3
		,MAX(STDPorcMortSem4) STDPorcMortSem4
		,MAX(STDPorcMortSem5) STDPorcMortSem5
		,MAX(STDPorcMortSem6) STDPorcMortSem6
		,MAX(STDPorcMortSem7) STDPorcMortSem7
		,MAX(STDPorcMortSem8) STDPorcMortSem8
		,MAX(STDPorcMortSem9) STDPorcMortSem9
		,MAX(STDPorcMortSem10) STDPorcMortSem10
		,MAX(STDPorcMortSem11) STDPorcMortSem11
		,MAX(STDPorcMortSem12) STDPorcMortSem12
		,MAX(STDPorcMortSem13) STDPorcMortSem13
		,MAX(STDPorcMortSem14) STDPorcMortSem14
		,MAX(STDPorcMortSem15) STDPorcMortSem15
		,MAX(STDPorcMortSem16) STDPorcMortSem16
		,MAX(STDPorcMortSem17) STDPorcMortSem17
		,MAX(STDPorcMortSem18) STDPorcMortSem18
		,MAX(STDPorcMortSem19) STDPorcMortSem19
		,MAX(STDPorcMortSem20) STDPorcMortSem20
		,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
		,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
		,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
		,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
		,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
		,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
		,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
		,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
		,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
		,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
		,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
		,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
		,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
		,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
		,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
		,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
		,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
		,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
		,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
		,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
		,MAX(STDPorcMortSem1G) STDPorcMortSem1G
		,MAX(STDPorcMortSem2G) STDPorcMortSem2G
		,MAX(STDPorcMortSem3G) STDPorcMortSem3G
		,MAX(STDPorcMortSem4G) STDPorcMortSem4G
		,MAX(STDPorcMortSem5G) STDPorcMortSem5G
		,MAX(STDPorcMortSem6G) STDPorcMortSem6G
		,MAX(STDPorcMortSem7G) STDPorcMortSem7G
		,MAX(STDPorcMortSem8G) STDPorcMortSem8G
		,MAX(STDPorcMortSem9G) STDPorcMortSem9G
		,MAX(STDPorcMortSem10G) STDPorcMortSem10G
		,MAX(STDPorcMortSem11G) STDPorcMortSem11G
		,MAX(STDPorcMortSem12G) STDPorcMortSem12G
		,MAX(STDPorcMortSem13G) STDPorcMortSem13G
		,MAX(STDPorcMortSem14G) STDPorcMortSem14G
		,MAX(STDPorcMortSem15G) STDPorcMortSem15G
		,MAX(STDPorcMortSem16G) STDPorcMortSem16G
		,MAX(STDPorcMortSem17G) STDPorcMortSem17G
		,MAX(STDPorcMortSem18G) STDPorcMortSem18G
		,MAX(STDPorcMortSem19G) STDPorcMortSem19G
		,MAX(STDPorcMortSem20G) STDPorcMortSem20G
		,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1G
		,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2G
		,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3G
		,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4G
		,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5G
		,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6G
		,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7G
		,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8G
		,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9G
		,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10G
		,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11G
		,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12G
		,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13G
		,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14G
		,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15G
		,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16G
		,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17G
		,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18G
		,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19G
		,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20G
		,MAX(STDPorcMortSem1L) STDPorcMortSem1L
		,MAX(STDPorcMortSem2L) STDPorcMortSem2L
		,MAX(STDPorcMortSem3L) STDPorcMortSem3L
		,MAX(STDPorcMortSem4L) STDPorcMortSem4L
		,MAX(STDPorcMortSem5L) STDPorcMortSem5L
		,MAX(STDPorcMortSem6L) STDPorcMortSem6L
		,MAX(STDPorcMortSem7L) STDPorcMortSem7L
		,MAX(STDPorcMortSem8L) STDPorcMortSem8L
		,MAX(STDPorcMortSem9L) STDPorcMortSem9L
		,MAX(STDPorcMortSem10L) STDPorcMortSem10L
		,MAX(STDPorcMortSem11L) STDPorcMortSem11L
		,MAX(STDPorcMortSem12L) STDPorcMortSem12L
		,MAX(STDPorcMortSem13L) STDPorcMortSem13L
		,MAX(STDPorcMortSem14L) STDPorcMortSem14L
		,MAX(STDPorcMortSem15L) STDPorcMortSem15L
		,MAX(STDPorcMortSem16L) STDPorcMortSem16L
		,MAX(STDPorcMortSem17L) STDPorcMortSem17L
		,MAX(STDPorcMortSem18L) STDPorcMortSem18L
		,MAX(STDPorcMortSem19L) STDPorcMortSem19L
		,MAX(STDPorcMortSem20L) STDPorcMortSem20L
		,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1L
		,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2L
		,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3L
		,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4L
		,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5L
		,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6L
		,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7L
		,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8L
		,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9L
		,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10L
		,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11L
		,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12L
		,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13L
		,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14L
		,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15L
		,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16L
		,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17L
		,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18L
		,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19L
		,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20L
		,MAX(STDPesoSem1)STDPesoSem1
		,MAX(STDPesoSem2)STDPesoSem2
		,MAX(STDPesoSem3)STDPesoSem3
		,MAX(STDPesoSem4)STDPesoSem4
		,MAX(STDPesoSem5)STDPesoSem5
		,MAX(STDPesoSem6)STDPesoSem6
		,MAX(STDPesoSem7)STDPesoSem7
		,MAX(STDPesoSem8)STDPesoSem8
		,MAX(STDPesoSem9)STDPesoSem9
		,MAX(STDPesoSem10)STDPesoSem10
		,MAX(STDPesoSem11)STDPesoSem11
		,MAX(STDPesoSem12)STDPesoSem12
		,MAX(STDPesoSem13)STDPesoSem13
		,MAX(STDPesoSem14)STDPesoSem14
		,MAX(STDPesoSem15)STDPesoSem15
		,MAX(STDPesoSem16)STDPesoSem16
		,MAX(STDPesoSem17)STDPesoSem17
		,MAX(STDPesoSem18)STDPesoSem18
		,MAX(STDPesoSem19)STDPesoSem19
		,MAX(STDPesoSem20)STDPesoSem20
		,MAX(STDPeso5Dias)STDPeso5Dias
		,MAX(STDPorcPesoSem1G)STDPorcPesoSem1G
		,MAX(STDPorcPesoSem2G)STDPorcPesoSem2G
		,MAX(STDPorcPesoSem3G)STDPorcPesoSem3G
		,MAX(STDPorcPesoSem4G)STDPorcPesoSem4G
		,MAX(STDPorcPesoSem5G)STDPorcPesoSem5G
		,MAX(STDPorcPesoSem6G)STDPorcPesoSem6G
		,MAX(STDPorcPesoSem7G)STDPorcPesoSem7G
		,MAX(STDPorcPesoSem8G)STDPorcPesoSem8G
		,MAX(STDPorcPesoSem9G)STDPorcPesoSem9G
		,MAX(STDPorcPesoSem10G)STDPorcPesoSem10G
		,MAX(STDPorcPesoSem11G)STDPorcPesoSem11G
		,MAX(STDPorcPesoSem12G)STDPorcPesoSem12G
		,MAX(STDPorcPesoSem13G)STDPorcPesoSem13G
		,MAX(STDPorcPesoSem14G)STDPorcPesoSem14G
		,MAX(STDPorcPesoSem15G)STDPorcPesoSem15G
		,MAX(STDPorcPesoSem16G)STDPorcPesoSem16G
		,MAX(STDPorcPesoSem17G)STDPorcPesoSem17G
		,MAX(STDPorcPesoSem18G)STDPorcPesoSem18G
		,MAX(STDPorcPesoSem19G)STDPorcPesoSem19G
		,MAX(STDPorcPesoSem20G)STDPorcPesoSem20G
		,MAX(STDPorcPeso5DiasG)STDPorcPeso5DiasG
		,MAX(STDPorcPesoSem1L)STDPorcPesoSem1L
		,MAX(STDPorcPesoSem2L)STDPorcPesoSem2L
		,MAX(STDPorcPesoSem3L)STDPorcPesoSem3L
		,MAX(STDPorcPesoSem4L)STDPorcPesoSem4L
		,MAX(STDPorcPesoSem5L)STDPorcPesoSem5L
		,MAX(STDPorcPesoSem6L)STDPorcPesoSem6L
		,MAX(STDPorcPesoSem7L)STDPorcPesoSem7L
		,MAX(STDPorcPesoSem8L)STDPorcPesoSem8L
		,MAX(STDPorcPesoSem9L)STDPorcPesoSem9L
		,MAX(STDPorcPesoSem10L)STDPorcPesoSem10L
		,MAX(STDPorcPesoSem11L)STDPorcPesoSem11L
		,MAX(STDPorcPesoSem12L)STDPorcPesoSem12L
		,MAX(STDPorcPesoSem13L)STDPorcPesoSem13L
		,MAX(STDPorcPesoSem14L)STDPorcPesoSem14L
		,MAX(STDPorcPesoSem15L)STDPorcPesoSem15L
		,MAX(STDPorcPesoSem16L)STDPorcPesoSem16L
		,MAX(STDPorcPesoSem17L)STDPorcPesoSem17L
		,MAX(STDPorcPesoSem18L)STDPorcPesoSem18L
		,MAX(STDPorcPesoSem19L)STDPorcPesoSem19L
		,MAX(STDPorcPesoSem20L)STDPorcPesoSem20L
		,MAX(STDPorcPeso5DiasL)STDPorcPeso5DiasL
		,MAX(STDICAC) STDICAC
		,MAX(STDICAG) STDICAG
		,MAX(STDICAL) STDICAL
		,MAX(PavosBBMortIncub) AS PavosBBMortIncub
		,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInviernoC
		,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInviernoG
		,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInviernoL
		,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVeranoC
		,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVeranoG
		,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVeranoL
		,SUM(NoViableSem1) NoViableSem1
		,SUM(NoViableSem2) NoViableSem2
		,SUM(NoViableSem3) NoViableSem3
		,SUM(NoViableSem4) NoViableSem4
		,SUM(NoViableSem5) NoViableSem5
		,SUM(NoViableSem6) NoViableSem6
		,SUM(NoViableSem7) NoViableSem7
		,SUM(NoViableSem8) NoViableSem8        
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem1XPobInicial / P.PobInicialTotal END as PorcNoViableSem1
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem2XPobInicial / P.PobInicialTotal END as PorcNoViableSem2
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem3XPobInicial / P.PobInicialTotal END as PorcNoViableSem3
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem4XPobInicial / P.PobInicialTotal END as PorcNoViableSem4
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem5XPobInicial / P.PobInicialTotal END as PorcNoViableSem5
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem6XPobInicial / P.PobInicialTotal END as PorcNoViableSem6
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem7XPobInicial / P.PobInicialTotal END as PorcNoViableSem7
        ,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem8XPobInicial / P.PobInicialTotal END as PorcNoViableSem8
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem1XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem1XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem2XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem2XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem3XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem3XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem4XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem4XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem5XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem5XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem6XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem6XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem7XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem7XPobInicial
        ,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PN.PorcNoViableSem8XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PorcNoViableSem8XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem1XPobInicial / P.PobInicialTotal END as PesoSem1
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem2XPobInicial / P.PobInicialTotal END as PesoSem2
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem3XPobInicial / P.PobInicialTotal END as PesoSem3
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem4XPobInicial / P.PobInicialTotal END as PesoSem4
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem5XPobInicial / P.PobInicialTotal END as PesoSem5
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem6XPobInicial / P.PobInicialTotal END as PesoSem6
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem7XPobInicial / P.PobInicialTotal END as PesoSem7
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem8XPobInicial / P.PobInicialTotal END as PesoSem8
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem9XPobInicial / P.PobInicialTotal END as PesoSem9
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem10XPobInicial / P.PobInicialTotal END as PesoSem10
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem11XPobInicial / P.PobInicialTotal END as PesoSem11
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem12XPobInicial / P.PobInicialTotal END as PesoSem12
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem13XPobInicial / P.PobInicialTotal END as PesoSem13
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem14XPobInicial / P.PobInicialTotal END as PesoSem14
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem15XPobInicial / P.PobInicialTotal END as PesoSem15
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem16XPobInicial / P.PobInicialTotal END as PesoSem16
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem17XPobInicial / P.PobInicialTotal END as PesoSem17
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem18XPobInicial / P.PobInicialTotal END as PesoSem18
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem19XPobInicial / P.PobInicialTotal END as PesoSem19
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem20XPobInicial / P.PobInicialTotal END as PesoSem20
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.Peso5DiasXPobInicial / P.PobInicialTotal END as Peso5Dias
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem1XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem1XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem2XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem2XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem3XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem3XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem4XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem4XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem5XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem5XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem6XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem6XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem7XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem7XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem8XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem8XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem9XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem9XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem10XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem10XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem11XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem11XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem12XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem12XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem13XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem13XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem14XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem14XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem15XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem15XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem16XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem16XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem17XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem17XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem18XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem18XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem19XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem19XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.PesoSem20XPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoSem20XPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PS.Peso5DiasXPobInicial / P.PobInicialTotal END * P.PobInicialTotal as Peso5DiasXPobInicial
        ,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PesoAloXPobInicial / P.PobInicialTotal END as PesoAlo
        ,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PesoHvoXPobInicial / P.PobInicialTotal END as PesoHvo
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PesoAloXPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoAloXPobInicial
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PesoHvoXPobInicial / P.PobInicialTotal END * P.PobInicialTotal as PesoHvoXPobInicial
        ,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PobInicialXSTDMortAcumC / P.PobInicialTotal END as STDMortAcumG
		,CASE WHEN P.PobInicialTotal = 0 THEN 0.0 ELSE PO.PobInicialXSTDMortAcumC / P.PobInicialTotal END * P.PobInicialTotal as PobInicialXSTDMortAcumG
        ,A.pk_tipogranja
from {database_name_tmp}.ProduccionDetalleCorral A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name_gl}.lk_galpon C on A.pk_galpon = C.pk_galpon
left join {database_name_gl}.stg_GasAguaCama D on concat(B.clote,'-',c.nogalpon) = D.ComplexEntityNo
LEFT JOIN PobInicialSuma P ON A.pk_lote = P.pk_lote AND A.pk_galpon = P.pk_galpon
LEFT JOIN PorcNoViable PN ON A.pk_lote = PN.pk_lote AND A.pk_galpon = PN.pk_galpon
LEFT JOIN PobotroSuma PO ON A.pk_lote = PO.pk_lote AND A.pk_galpon = PO.pk_galpon
LEFT JOIN Pesos PS ON A.pk_lote = PS.pk_lote AND A.pk_galpon = PS.pk_galpon
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,pk_tipoproducto,pk_estado,pk_administrador,nogalpon,clote
,categoria,A.pk_tipogranja,P.PobInicialTotal,PN.PorcNoViableSem1XPobInicial,PN.PorcNoViableSem2XPobInicial,PN.PorcNoViableSem3XPobInicial
,PN.PorcNoViableSem4XPobInicial,PN.PorcNoViableSem5XPobInicial,PN.PorcNoViableSem6XPobInicial,PN.PorcNoViableSem7XPobInicial,PN.PorcNoViableSem8XPobInicial
,PS.PesoSem1XPobInicial,PS.PesoSem2XPobInicial,PS.PesoSem3XPobInicial,PS.PesoSem4XPobInicial,PS.PesoSem5XPobInicial,PS.PesoSem6XPobInicial 
,PS.PesoSem7XPobInicial,PS.PesoSem8XPobInicial,PS.PesoSem9XPobInicial,PS.PesoSem10XPobInicial,PS.PesoSem11XPobInicial,PS.PesoSem12XPobInicial
,PS.PesoSem13XPobInicial,PS.PesoSem14XPobInicial,PS.PesoSem15XPobInicial,PS.PesoSem16XPobInicial,PO.PobInicialXSTDMortAcumC
,PS.PesoSem17XPobInicial,PS.PesoSem18XPobInicial,PS.PesoSem19XPobInicial,PS.PesoSem20XPobInicial,PS.Peso5DiasXPobInicial,PO.PesoAloXPobInicial,PO.PesoHvoXPobInicial
""")
#caSO ica por revisar Carlos andia
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleGalpon"
}
df_ProduccionDetalleGalpon.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleGalpon")
print('carga ProduccionDetalleGalpon', df_ProduccionDetalleGalpon.count())   
#Se muestra OrdenId
df_OrdenId = spark.sql(f"""
select clote,pk_especie,pk_tipoproducto,sum(AvesRendidas) AvesRendidas,DENSE_RANK() OVER (PARTITION BY clote ORDER BY sum(AvesRendidas) asc) as orden
from {database_name_tmp}.ProduccionDetalleGalpon A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
group by clote,pk_especie,pk_tipoproducto
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenId"
}
df_OrdenId.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.OrdenId")
print('carga OrdenId',df_OrdenId.count())   
#Se muestra PorcPoblacion
df_PorcPoblacion = spark.sql(f"""
select A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.pk_standard,C.lstandard,A.ComplexEntityNo,MAX(A.PobInicial)PobInicial,MAX(B.PesoAlo) PesoAlo
from {database_name_gl}.ft_consolidado_Corral A
left join {database_name_gl}.ft_peso_diario B on A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_galpon = B.pk_galpon and A.pk_sexo = B.pk_sexo
left join {database_name_gl}.lk_standard C on A.pk_standard = C.pk_standard
where A.pk_empresa = 1
group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.pk_standard,C.lstandard,A.ComplexEntityNo
order by A.complexentityno
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PorcPoblacion"
}
df_PorcPoblacion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.PorcPoblacion")
print('carga PorcPoblacion', df_PorcPoblacion.count())   
#Se muestra CalculosPorcPoblacion
df_CalculosPorcPoblacion = spark.sql(f"""
WITH PobLote AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialLote FROM {database_name_tmp}.PorcPoblacion GROUP BY pk_plantel, pk_lote),
PobJoven AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialJoven FROM {database_name_tmp}.PorcPoblacion WHERE lstandard = 'Joven' GROUP BY pk_plantel, pk_lote),
PobPesoMenor AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialPesoMenor FROM {database_name_tmp}.PorcPoblacion WHERE PesoAlo <= 0.033 GROUP BY pk_plantel, pk_lote),
CantidadGalpon AS (SELECT pk_plantel,pk_lote,COUNT(DISTINCT pk_galpon) AS CantidadGalpon FROM {database_name_tmp}.PorcPoblacion GROUP BY pk_plantel, pk_lote),
GalponMixto AS (SELECT pk_galpon,pk_plantel,pk_lote,case when sum(pk_sexo) = 4 then 1 end TotalGalponMixto FROM {database_name_tmp}.PorcPoblacion GROUP BY pk_lote,pk_plantel,pk_galpon)

SELECT A.pk_empresa,A.pk_division, A.pk_plantel, A.pk_lote
    ,COALESCE(PJ.PobInicialJoven * 1.0 / PL.PobInicialLote, 0) AS PorcPolloJoven
    ,COALESCE(PPM.PobInicialPesoMenor * 1.0 / PL.PobInicialLote, 0) AS PorcPesoAloMenor
    ,COALESCE((GM.TotalGalponMixto * 1.0 / 2) / CG.CantidadGalpon, 0) AS PorcGalponesMixtos
FROM {database_name_tmp}.PorcPoblacion A
LEFT JOIN PobLote PL ON A.pk_plantel = PL.pk_plantel AND A.pk_lote = PL.pk_lote
LEFT JOIN PobJoven PJ ON A.pk_plantel = PJ.pk_plantel AND A.pk_lote = PJ.pk_lote
LEFT JOIN PobPesoMenor PPM ON A.pk_plantel = PPM.pk_plantel AND A.pk_lote = PPM.pk_lote
LEFT JOIN CantidadGalpon CG ON A.pk_plantel = CG.pk_plantel AND A.pk_lote = CG.pk_lote
LEFT JOIN GalponMixto GM ON A.pk_plantel = GM.pk_plantel AND A.pk_lote = GM.pk_lote and A.pk_galpon = GM.pk_galpon 
GROUP BY A.pk_empresa, A.pk_division, A.pk_plantel, A.pk_lote, PJ.PobInicialJoven, PL.PobInicialLote, PPM.PobInicialPesoMenor, CG.CantidadGalpon, GM.TotalGalponMixto
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/CalculosPorcPoblacion"
}
df_CalculosPorcPoblacion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.CalculosPorcPoblacion")
print('carga CalculosPorcPoblacion', df_CalculosPorcPoblacion.count())   
#Se muestra ProduccionDetalleLote
df_ProduccionDetalleLote = spark.sql(f"""
WITH MaxOrden AS (SELECT clote, MAX(orden) AS max_orden FROM {database_name_tmp}.OrdenId GROUP BY clote),
Categorias AS (SELECT pk_lote,CONCAT_WS(',', COLLECT_SET(categoria)) AS categoria FROM {database_name_tmp}.ProduccionDetalleGalpon GROUP BY pk_lote),
PobInicialLote AS (SELECT pk_lote, SUM(PobInicial) AS TotalPobInicial FROM {database_name_tmp}.ProduccionDetalleGalpon GROUP BY pk_lote),
PesosPorSemana AS (
    SELECT 
        pk_lote,
        SUM(CASE WHEN PesoSem1 <> 0 THEN PesoSem1XPobInicial ELSE 0 END) AS PesoSem1XPobInicial,
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem2XPobInicial ELSE 0 END) AS PesoSem2XPobInicial, 
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem3XPobInicial ELSE 0 END) AS PesoSem3XPobInicial, 
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem4XPobInicial ELSE 0 END) AS PesoSem4XPobInicial, 
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem5XPobInicial ELSE 0 END) AS PesoSem5XPobInicial, 
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem6XPobInicial ELSE 0 END) AS PesoSem6XPobInicial, 
        SUM(CASE WHEN PesoSem7 <> 0 THEN PesoSem7XPobInicial ELSE 0 END) AS PesoSem7XPobInicial, 
        SUM(CASE WHEN PesoSem8 <> 0 THEN PesoSem8XPobInicial ELSE 0 END) AS PesoSem8XPobInicial, 
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem9XPobInicial ELSE 0 END) AS PesoSem9XPobInicial, 
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem10XPobInicial ELSE 0 END) AS PesoSem10XPobInicial,
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem11XPobInicial ELSE 0 END) AS PesoSem11XPobInicial,
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem12XPobInicial ELSE 0 END) AS PesoSem12XPobInicial,
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem13XPobInicial ELSE 0 END) AS PesoSem13XPobInicial,
        SUM(CASE WHEN PesoSem7 <> 0 THEN PesoSem14XPobInicial ELSE 0 END) AS PesoSem14XPobInicial,
        SUM(CASE WHEN PesoSem8 <> 0 THEN PesoSem15XPobInicial ELSE 0 END) AS PesoSem15XPobInicial,
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem16XPobInicial ELSE 0 END) AS PesoSem16XPobInicial,
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem17XPobInicial ELSE 0 END) AS PesoSem17XPobInicial,
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem18XPobInicial ELSE 0 END) AS PesoSem18XPobInicial,
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem19XPobInicial ELSE 0 END) AS PesoSem19XPobInicial,
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem20XPobInicial ELSE 0 END) AS PesoSem20XPobInicial,
        SUM(CASE WHEN PesoSem7 <> 0 THEN Peso5DiasXPobInicial ELSE 0 END) AS Peso5DiasXPobInicial
    FROM {database_name_tmp}.ProduccionDetalleGalpon
    GROUP BY pk_lote
),
NoViablesPorSemana AS (
    SELECT 
        pk_lote,
        SUM(CASE WHEN PorcNoViableSem1 <> 0 THEN PorcNoViableSem1XPobInicial ELSE 0 END) AS PorcNoViableSem1XPobInicial,
        SUM(CASE WHEN PorcNoViableSem2 <> 0 THEN PorcNoViableSem2XPobInicial ELSE 0 END) AS PorcNoViableSem2XPobInicial,
        SUM(CASE WHEN PorcNoViableSem3 <> 0 THEN PorcNoViableSem3XPobInicial ELSE 0 END) AS PorcNoViableSem3XPobInicial,
        SUM(CASE WHEN PorcNoViableSem4 <> 0 THEN PorcNoViableSem4XPobInicial ELSE 0 END) AS PorcNoViableSem4XPobInicial,
        SUM(CASE WHEN PorcNoViableSem5 <> 0 THEN PorcNoViableSem5XPobInicial ELSE 0 END) AS PorcNoViableSem5XPobInicial,
        SUM(CASE WHEN PorcNoViableSem6 <> 0 THEN PorcNoViableSem6XPobInicial ELSE 0 END) AS PorcNoViableSem6XPobInicial,
        SUM(CASE WHEN PorcNoViableSem7 <> 0 THEN PorcNoViableSem7XPobInicial ELSE 0 END) AS PorcNoViableSem7XPobInicial,
        SUM(CASE WHEN PorcNoViableSem8 <> 0 THEN PorcNoViableSem8XPobInicial ELSE 0 END) AS PorcNoViableSem8XPobInicial,
        SUM(CASE WHEN PesoAlo <> 0 THEN PesoAloXPobInicial ELSE 0 END) AS PesoAloXPobInicial,
        SUM(CASE WHEN PesoHvo <> 0 THEN PesoHvoXPobInicial ELSE 0 END) AS PesoHvoXPobInicial,
        SUM(CASE WHEN PobInicialXSTDMortAcumG <> 0 THEN PobInicialXSTDMortAcumG ELSE 0 END) AS PobInicialXSTDMortAcumG
    FROM {database_name_tmp}.ProduccionDetalleGalpon
    GROUP BY pk_lote
)

select max(fecha) as fecha
,case when min(pk_estado) = 1 then substring((cast(max(fecha) as varchar(8))),1,6) else substring((cast(max(FechaCierre) as varchar(8))),1,6) end pk_mes
,a.pk_empresa
,a.pk_division
,pk_zona
,pk_subzona
,A.pk_plantel
,A.pk_lote
,c.pk_tipoproducto
,29 as pk_especie
,min(pk_estado) pk_estado 
,min(pk_administrador) as pk_administrador 
,MAX(pk_proveedor) pk_proveedor
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0)) as int) as pk_diasvida
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0)) as int) * sum(PobInicial) as pk_diasvidaXPobInicial
,b.clote as ComplexEntityNoLote
,max(FechaCierre) as FechaCierre
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,max(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(AreaGalpon) as AreaGalpon
,sum(PobInicial) as PobInicial
,sum(a.AvesRendidas) as AvesRendidas
,sum(KilosRendidos) as KilosRendidos
,sum(MortDia) as MortDia
,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE ((cast(SUM(MortDia) as integer) / SUM(PobInicial*1.0) )*100) END AS PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,sum(MortSem8) AS MortSem8
,sum(MortSem9) AS MortSem9
,sum(MortSem10) AS MortSem10
,sum(MortSem11) AS MortSem11
,sum(MortSem12) AS MortSem12
,sum(MortSem13) AS MortSem13
,sum(MortSem14) AS MortSem14
,sum(MortSem15) AS MortSem15
,sum(MortSem16) AS MortSem16
,sum(MortSem17) AS MortSem17
,sum(MortSem18) AS MortSem18
,sum(MortSem19) AS MortSem19
,sum(MortSem20) AS MortSem20
,sum(MortSemAcum1) as MortSemAcum1 
,sum(MortSemAcum2) as MortSemAcum2 
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4 
,sum(MortSemAcum5) as MortSemAcum5 
,sum(MortSemAcum6) as MortSemAcum6 
,sum(MortSemAcum7) as MortSemAcum7 
,sum(MortSemAcum8) AS MortSemAcum8 
,sum(MortSemAcum9) AS MortSemAcum9 
,sum(MortSemAcum10) AS MortSemAcum10 
,sum(MortSemAcum11) AS MortSemAcum11 
,sum(MortSemAcum12) AS MortSemAcum12 
,sum(MortSemAcum13) AS MortSemAcum13 
,sum(MortSemAcum14) AS MortSemAcum14 
,sum(MortSemAcum15) AS MortSemAcum15 
,sum(MortSemAcum16) AS MortSemAcum16 
,sum(MortSemAcum17) AS MortSemAcum17 
,sum(MortSemAcum18) AS MortSemAcum18 
,sum(MortSemAcum19) AS MortSemAcum19 
,sum(MortSemAcum20) AS MortSemAcum20
,max(Peso) as Peso
,CASE WHEN sum(a.AvesRendidas) = 0 THEN 0.0 ELSE floor((sum(KilosRendidos)/sum(a.AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
,avg(STDPeso) as STDPeso
,sum(UnidSeleccion) as UnidSeleccion
,sum(ConsDia) as ConsDia
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,CASE WHEN SUM(KilosRendidos) = 0 THEN 0.0 ELSE  floor((cast(SUM(ConsDia) as decimal(30,10))/cast(SUM(KilosRendidos) as decimal(30,10))) * 1000000) / 1000000 END AS ICA
,sum(CantInicioSaca) as CantInicioSaca
,min(EdadInicioSaca) as EdadInicioSaca
,avg(EdadGranjaLote) as EdadGranjaLote
,avg(D.Gas) as Gas
,avg(D.Cama) as Cama
,avg(D.Agua) as Agua
,sum(CantMacho) as CantMacho
,avg(Pigmentacion) as Pigmentacion
,max(FlagAtipico) FlagAtipico
,2 FlagArtAtipico
,MAX(EdadPadreLote) EdadPadreLote
,MAX(STDPorcMortAcumC) STDPorcMortAcumC
,MAX(STDPorcMortAcumG) STDPorcMortAcumG
,MAX(STDPorcMortAcumL) STDPorcMortAcumL
,MAX(STDPesoC) STDPesoC
,MAX(STDPesoG) STDPesoG
,MAX(STDPesoL) STDPesoL
,MAX(STDConsAcumC) STDConsAcumC
,MAX(STDConsAcumG) STDConsAcumG
,MAX(STDConsAcumL) STDConsAcumL
,MAX(STDPorcMortSem1) STDPorcMortSem1
,MAX(STDPorcMortSem2) STDPorcMortSem2
,MAX(STDPorcMortSem3) STDPorcMortSem3
,MAX(STDPorcMortSem4) STDPorcMortSem4
,MAX(STDPorcMortSem5) STDPorcMortSem5
,MAX(STDPorcMortSem6) STDPorcMortSem6
,MAX(STDPorcMortSem7) STDPorcMortSem7
,MAX(STDPorcMortSem8) STDPorcMortSem8
,MAX(STDPorcMortSem9) STDPorcMortSem9
,MAX(STDPorcMortSem10) STDPorcMortSem10
,MAX(STDPorcMortSem11) STDPorcMortSem11
,MAX(STDPorcMortSem12) STDPorcMortSem12
,MAX(STDPorcMortSem13) STDPorcMortSem13
,MAX(STDPorcMortSem14) STDPorcMortSem14
,MAX(STDPorcMortSem15) STDPorcMortSem15
,MAX(STDPorcMortSem16) STDPorcMortSem16
,MAX(STDPorcMortSem17) STDPorcMortSem17
,MAX(STDPorcMortSem18) STDPorcMortSem18
,MAX(STDPorcMortSem19) STDPorcMortSem19
,MAX(STDPorcMortSem20) STDPorcMortSem20
,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
,MAX(STDPorcMortSem1G) STDPorcMortSem1G
,MAX(STDPorcMortSem2G) STDPorcMortSem2G
,MAX(STDPorcMortSem3G) STDPorcMortSem3G
,MAX(STDPorcMortSem4G) STDPorcMortSem4G
,MAX(STDPorcMortSem5G) STDPorcMortSem5G
,MAX(STDPorcMortSem6G) STDPorcMortSem6G
,MAX(STDPorcMortSem7G) STDPorcMortSem7G
,MAX(STDPorcMortSem8G) STDPorcMortSem8G
,MAX(STDPorcMortSem9G) STDPorcMortSem9G
,MAX(STDPorcMortSem10G) STDPorcMortSem10G
,MAX(STDPorcMortSem11G) STDPorcMortSem11G
,MAX(STDPorcMortSem12G) STDPorcMortSem12G
,MAX(STDPorcMortSem13G) STDPorcMortSem13G
,MAX(STDPorcMortSem14G) STDPorcMortSem14G
,MAX(STDPorcMortSem15G) STDPorcMortSem15G
,MAX(STDPorcMortSem16G) STDPorcMortSem16G
,MAX(STDPorcMortSem17G) STDPorcMortSem17G
,MAX(STDPorcMortSem18G) STDPorcMortSem18G
,MAX(STDPorcMortSem19G) STDPorcMortSem19G
,MAX(STDPorcMortSem20G) STDPorcMortSem20G
,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1G
,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2G
,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3G
,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4G
,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5G
,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6G
,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7G
,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8G
,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9G
,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10G
,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11G
,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12G
,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13G
,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14G
,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15G
,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16G
,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17G
,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18G
,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19G
,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20G
,MAX(STDPorcMortSem1L) STDPorcMortSem1L
,MAX(STDPorcMortSem2L) STDPorcMortSem2L
,MAX(STDPorcMortSem3L) STDPorcMortSem3L
,MAX(STDPorcMortSem4L) STDPorcMortSem4L
,MAX(STDPorcMortSem5L) STDPorcMortSem5L
,MAX(STDPorcMortSem6L) STDPorcMortSem6L
,MAX(STDPorcMortSem7L) STDPorcMortSem7L
,MAX(STDPorcMortSem8L) STDPorcMortSem8L
,MAX(STDPorcMortSem9L) STDPorcMortSem9L
,MAX(STDPorcMortSem10L) STDPorcMortSem10L
,MAX(STDPorcMortSem11L) STDPorcMortSem11L
,MAX(STDPorcMortSem12L) STDPorcMortSem12L
,MAX(STDPorcMortSem13L) STDPorcMortSem13L
,MAX(STDPorcMortSem14L) STDPorcMortSem14L
,MAX(STDPorcMortSem15L) STDPorcMortSem15L
,MAX(STDPorcMortSem16L) STDPorcMortSem16L
,MAX(STDPorcMortSem17L) STDPorcMortSem17L
,MAX(STDPorcMortSem18L) STDPorcMortSem18L
,MAX(STDPorcMortSem19L) STDPorcMortSem19L
,MAX(STDPorcMortSem20L) STDPorcMortSem20L
,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1L
,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2L
,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3L
,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4L
,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5L
,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6L
,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7L
,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8L
,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9L
,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10L
,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11L
,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12L
,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13L
,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14L
,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15L
,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16L
,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17L
,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18L
,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19L
,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20L
,MAX(STDPesoSem1)STDPesoSem1
,MAX(STDPesoSem2)STDPesoSem2
,MAX(STDPesoSem3)STDPesoSem3
,MAX(STDPesoSem4)STDPesoSem4
,MAX(STDPesoSem5)STDPesoSem5
,MAX(STDPesoSem6)STDPesoSem6
,MAX(STDPesoSem7)STDPesoSem7
,MAX(STDPesoSem8)STDPesoSem8
,MAX(STDPesoSem9)STDPesoSem9
,MAX(STDPesoSem10)STDPesoSem10
,MAX(STDPesoSem11)STDPesoSem11
,MAX(STDPesoSem12)STDPesoSem12
,MAX(STDPesoSem13)STDPesoSem13
,MAX(STDPesoSem14)STDPesoSem14
,MAX(STDPesoSem15)STDPesoSem15
,MAX(STDPesoSem16)STDPesoSem16
,MAX(STDPesoSem17)STDPesoSem17
,MAX(STDPesoSem18)STDPesoSem18
,MAX(STDPesoSem19)STDPesoSem19
,MAX(STDPesoSem20)STDPesoSem20
,MAX(STDPeso5Dias)STDPeso5Dias
,MAX(STDPorcPesoSem1G)STDPorcPesoSem1G
,MAX(STDPorcPesoSem2G)STDPorcPesoSem2G
,MAX(STDPorcPesoSem3G)STDPorcPesoSem3G
,MAX(STDPorcPesoSem4G)STDPorcPesoSem4G
,MAX(STDPorcPesoSem5G)STDPorcPesoSem5G
,MAX(STDPorcPesoSem6G)STDPorcPesoSem6G
,MAX(STDPorcPesoSem7G)STDPorcPesoSem7G
,MAX(STDPorcPesoSem8G)STDPorcPesoSem8G
,MAX(STDPorcPesoSem9G)STDPorcPesoSem9G
,MAX(STDPorcPesoSem10G)STDPorcPesoSem10G
,MAX(STDPorcPesoSem11G)STDPorcPesoSem11G
,MAX(STDPorcPesoSem12G)STDPorcPesoSem12G
,MAX(STDPorcPesoSem13G)STDPorcPesoSem13G
,MAX(STDPorcPesoSem14G)STDPorcPesoSem14G
,MAX(STDPorcPesoSem15G)STDPorcPesoSem15G
,MAX(STDPorcPesoSem16G)STDPorcPesoSem16G
,MAX(STDPorcPesoSem17G)STDPorcPesoSem17G
,MAX(STDPorcPesoSem18G)STDPorcPesoSem18G
,MAX(STDPorcPesoSem19G)STDPorcPesoSem19G
,MAX(STDPorcPesoSem20G)STDPorcPesoSem20G
,MAX(STDPorcPeso5DiasG)STDPorcPeso5DiasG
,MAX(STDPorcPesoSem1L)STDPorcPesoSem1L
,MAX(STDPorcPesoSem2L)STDPorcPesoSem2L
,MAX(STDPorcPesoSem3L)STDPorcPesoSem3L
,MAX(STDPorcPesoSem4L)STDPorcPesoSem4L
,MAX(STDPorcPesoSem5L)STDPorcPesoSem5L
,MAX(STDPorcPesoSem6L)STDPorcPesoSem6L
,MAX(STDPorcPesoSem7L)STDPorcPesoSem7L
,MAX(STDPorcPesoSem8L)STDPorcPesoSem8L
,MAX(STDPorcPesoSem9L)STDPorcPesoSem9L
,MAX(STDPorcPesoSem10L)STDPorcPesoSem10L
,MAX(STDPorcPesoSem11L)STDPorcPesoSem11L
,MAX(STDPorcPesoSem12L)STDPorcPesoSem12L
,MAX(STDPorcPesoSem13L)STDPorcPesoSem13L
,MAX(STDPorcPesoSem14L)STDPorcPesoSem14L
,MAX(STDPorcPesoSem15L)STDPorcPesoSem15L
,MAX(STDPorcPesoSem16L)STDPorcPesoSem16L
,MAX(STDPorcPesoSem17L)STDPorcPesoSem17L
,MAX(STDPorcPesoSem18L)STDPorcPesoSem18L
,MAX(STDPorcPesoSem19L)STDPorcPesoSem19L
,MAX(STDPorcPesoSem20L)STDPorcPesoSem20L
,MAX(STDPorcPeso5DiasL)STDPorcPeso5DiasL
,MAX(PorcPolloJoven) PorcPolloJoven
,MAX(PorcPesoAloMenor) PorcPesoAloMenor
,MAX(PorcGalponesMixtos) PorcGalponesMixtos
,MAX(STDICAC) STDICAC
,MAX(STDICAG) STDICAG
,MAX(STDICAL) STDICAL
,MAX(PavosBBMortIncub) AS PavosBBMortIncub
,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInviernoC
,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInviernoG
,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInviernoL
,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVeranoC
,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVeranoG
,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVeranoL
,SUM(NoViableSem1) NoViableSem1
,SUM(NoViableSem2) NoViableSem2
,SUM(NoViableSem3) NoViableSem3
,SUM(NoViableSem4) NoViableSem4
,SUM(NoViableSem5) NoViableSem5
,SUM(NoViableSem6) NoViableSem6
,SUM(NoViableSem7) NoViableSem7
,SUM(NoViableSem8) NoViableSem8
,COALESCE(P.PesoSem1XPobInicial / L.TotalPobInicial, 0) as PesoSem1
,COALESCE(P.PesoSem2XPobInicial / L.TotalPobInicial, 0) as PesoSem2
,COALESCE(P.PesoSem3XPobInicial / L.TotalPobInicial, 0) as PesoSem3
,COALESCE(P.PesoSem4XPobInicial / L.TotalPobInicial, 0) as PesoSem4
,COALESCE(P.PesoSem5XPobInicial / L.TotalPobInicial, 0) as PesoSem5
,COALESCE(P.PesoSem6XPobInicial / L.TotalPobInicial, 0) as PesoSem6
,COALESCE(P.PesoSem7XPobInicial / L.TotalPobInicial, 0) as PesoSem7
,COALESCE(P.PesoSem8XPobInicial / L.TotalPobInicial, 0) as PesoSem8
,COALESCE(P.PesoSem9XPobInicial / L.TotalPobInicial, 0) as PesoSem9
,COALESCE(P.PesoSem10XPobInicial / L.TotalPobInicial, 0) as PesoSem10
,COALESCE(P.PesoSem11XPobInicial / L.TotalPobInicial, 0) as PesoSem11
,COALESCE(P.PesoSem12XPobInicial / L.TotalPobInicial, 0) as PesoSem12
,COALESCE(P.PesoSem13XPobInicial / L.TotalPobInicial, 0) as PesoSem13
,COALESCE(P.PesoSem14XPobInicial / L.TotalPobInicial, 0) as PesoSem14
,COALESCE(P.PesoSem15XPobInicial / L.TotalPobInicial, 0) as PesoSem15
,COALESCE(P.PesoSem16XPobInicial / L.TotalPobInicial, 0) as PesoSem16
,COALESCE(P.PesoSem17XPobInicial / L.TotalPobInicial, 0) as PesoSem17
,COALESCE(P.PesoSem18XPobInicial / L.TotalPobInicial, 0) as PesoSem18
,COALESCE(P.PesoSem19XPobInicial / L.TotalPobInicial, 0) as PesoSem19
,COALESCE(P.PesoSem20XPobInicial / L.TotalPobInicial, 0) as PesoSem20
,COALESCE(P.Peso5DiasXPobInicial / L.TotalPobInicial, 0) as Peso5Dias
,COALESCE(P.PesoSem1XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem1XPobInicial
,COALESCE(P.PesoSem2XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem2XPobInicial
,COALESCE(P.PesoSem3XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem3XPobInicial
,COALESCE(P.PesoSem4XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem4XPobInicial
,COALESCE(P.PesoSem5XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem5XPobInicial
,COALESCE(P.PesoSem6XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem6XPobInicial
,COALESCE(P.PesoSem7XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem7XPobInicial
,COALESCE(P.PesoSem8XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem8XPobInicial
,COALESCE(P.PesoSem9XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem9XPobInicial
,COALESCE(P.PesoSem10XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem10XPobInicial
,COALESCE(P.PesoSem11XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem11XPobInicial
,COALESCE(P.PesoSem12XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem12XPobInicial
,COALESCE(P.PesoSem13XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem13XPobInicial
,COALESCE(P.PesoSem14XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem14XPobInicial
,COALESCE(P.PesoSem15XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem15XPobInicial
,COALESCE(P.PesoSem16XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem16XPobInicial
,COALESCE(P.PesoSem17XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem17XPobInicial
,COALESCE(P.PesoSem18XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem18XPobInicial
,COALESCE(P.PesoSem19XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem19XPobInicial
,COALESCE(P.PesoSem20XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem20XPobInicial
,COALESCE(P.Peso5DiasXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as Peso5DiasXPobInicial
,COALESCE(NV.PorcNoViableSem1XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem1
,COALESCE(NV.PorcNoViableSem2XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem2
,COALESCE(NV.PorcNoViableSem3XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem3
,COALESCE(NV.PorcNoViableSem4XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem4
,COALESCE(NV.PorcNoViableSem5XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem5
,COALESCE(NV.PorcNoViableSem6XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem6
,COALESCE(NV.PorcNoViableSem7XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem7
,COALESCE(NV.PorcNoViableSem8XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem8
,COALESCE(NV.PesoAloXPobInicial / L.TotalPobInicial, 0) as PesoAlo
,COALESCE(NV.PesoHvoXPobInicial / L.TotalPobInicial, 0) as PesoHvo
,COALESCE(NV.PesoAloXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoAloXPobInicial
,COALESCE(NV.PesoHvoXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoHvoXPobInicial
,COALESCE(NV.PobInicialXSTDMortAcumG / L.TotalPobInicial, 0) as STDMortAcumL
,COALESCE(NV.PobInicialXSTDMortAcumG / L.TotalPobInicial, 0) * L.TotalPobInicial as PobInicialXSTDMortAcumL
,Ca.categoria
from {database_name_tmp}.ProduccionDetalleGalpon A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN MaxOrden X on X.clote = B.clote
LEFT JOIN {database_name_tmp}.OrdenId C ON B.clote = C.clote AND C.orden = X.max_orden
left join {database_name_gl}.stg_GasAguaCama D on b.clote = D.ComplexEntityNo
left join {database_name_tmp}.CalculosPorcPoblacion E on A.pk_empresa = E.pk_empresa and A.pk_division = E.pk_division and A.pk_plantel = E.pk_plantel and A.pk_lote = E.pk_lote
LEFT JOIN PobInicialLote L ON A.pk_lote = L.pk_lote
LEFT JOIN PesosPorSemana P ON A.pk_lote = P.pk_lote
LEFT JOIN NoViablesPorSemana NV ON A.pk_lote = NV.pk_lote
LEFT JOIN Categorias Ca ON A.pk_lote = Ca.pk_lote
group by a.pk_empresa,A.pk_division,pk_zona,pk_subzona,A.pk_plantel,A.pk_lote,c.pk_tipoproducto,b.clote,L.TotalPobInicial
,P.PesoSem1XPobInicial,P.PesoSem2XPobInicial,P.PesoSem3XPobInicial,P.PesoSem4XPobInicial,P.PesoSem5XPobInicial,P.PesoSem6XPobInicial 
,P.PesoSem7XPobInicial,P.PesoSem8XPobInicial,P.PesoSem9XPobInicial,P.PesoSem10XPobInicial,P.PesoSem11XPobInicial,P.PesoSem12XPobInicial
,P.PesoSem13XPobInicial,P.PesoSem14XPobInicial,P.PesoSem15XPobInicial,P.PesoSem16XPobInicial,P.PesoSem17XPobInicial,P.PesoSem18XPobInicial
,P.PesoSem19XPobInicial,P.PesoSem20XPobInicial,P.Peso5DiasXPobInicial,NV.PorcNoViableSem1XPobInicial,NV.PorcNoViableSem2XPobInicial
,NV.PorcNoViableSem3XPobInicial,NV.PorcNoViableSem4XPobInicial,NV.PorcNoViableSem5XPobInicial,NV.PorcNoViableSem6XPobInicial
,NV.PorcNoViableSem7XPobInicial,NV.PorcNoViableSem8XPobInicial,NV.PesoAloXPobInicial,NV.PesoHvoXPobInicial,NV.PobInicialXSTDMortAcumG,Ca.categoria
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleLote"
}
df_ProduccionDetalleLote.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleLote")
print('carga ProduccionDetalleLote', df_ProduccionDetalleLote.count())   
#Se muestra ProduccionDetalleLoteSinAtipico
df_ProduccionDetalleLoteSinAtipico = spark.sql(f"""
WITH MaxOrden AS (SELECT clote, MAX(orden) AS max_orden FROM {database_name_tmp}.OrdenId GROUP BY clote),
Categorias AS (SELECT pk_lote,CONCAT_WS(',', COLLECT_SET(categoria)) AS categoria FROM {database_name_tmp}.ProduccionDetalleGalpon GROUP BY pk_lote),
PobInicialLote AS (SELECT pk_lote, SUM(PobInicial) AS TotalPobInicial FROM {database_name_tmp}.ProduccionDetalleGalpon GROUP BY pk_lote),
PesosPorSemana AS (
    SELECT 
        pk_lote,
        SUM(CASE WHEN PesoSem1 <> 0 THEN PesoSem1XPobInicial ELSE 0 END) AS PesoSem1XPobInicial,
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem2XPobInicial ELSE 0 END) AS PesoSem2XPobInicial, 
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem3XPobInicial ELSE 0 END) AS PesoSem3XPobInicial, 
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem4XPobInicial ELSE 0 END) AS PesoSem4XPobInicial, 
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem5XPobInicial ELSE 0 END) AS PesoSem5XPobInicial, 
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem6XPobInicial ELSE 0 END) AS PesoSem6XPobInicial, 
        SUM(CASE WHEN PesoSem7 <> 0 THEN PesoSem7XPobInicial ELSE 0 END) AS PesoSem7XPobInicial, 
        SUM(CASE WHEN PesoSem8 <> 0 THEN PesoSem8XPobInicial ELSE 0 END) AS PesoSem8XPobInicial, 
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem9XPobInicial ELSE 0 END) AS PesoSem9XPobInicial, 
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem10XPobInicial ELSE 0 END) AS PesoSem10XPobInicial,
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem11XPobInicial ELSE 0 END) AS PesoSem11XPobInicial,
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem12XPobInicial ELSE 0 END) AS PesoSem12XPobInicial,
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem13XPobInicial ELSE 0 END) AS PesoSem13XPobInicial,
        SUM(CASE WHEN PesoSem7 <> 0 THEN PesoSem14XPobInicial ELSE 0 END) AS PesoSem14XPobInicial,
        SUM(CASE WHEN PesoSem8 <> 0 THEN PesoSem15XPobInicial ELSE 0 END) AS PesoSem15XPobInicial,
        SUM(CASE WHEN PesoSem2 <> 0 THEN PesoSem16XPobInicial ELSE 0 END) AS PesoSem16XPobInicial,
        SUM(CASE WHEN PesoSem3 <> 0 THEN PesoSem17XPobInicial ELSE 0 END) AS PesoSem17XPobInicial,
        SUM(CASE WHEN PesoSem4 <> 0 THEN PesoSem18XPobInicial ELSE 0 END) AS PesoSem18XPobInicial,
        SUM(CASE WHEN PesoSem5 <> 0 THEN PesoSem19XPobInicial ELSE 0 END) AS PesoSem19XPobInicial,
        SUM(CASE WHEN PesoSem6 <> 0 THEN PesoSem20XPobInicial ELSE 0 END) AS PesoSem20XPobInicial,
        SUM(CASE WHEN PesoSem7 <> 0 THEN Peso5DiasXPobInicial ELSE 0 END) AS Peso5DiasXPobInicial
    FROM {database_name_tmp}.ProduccionDetalleGalpon
    GROUP BY pk_lote
),
NoViablesPorSemana AS (
    SELECT 
        pk_lote,
        SUM(CASE WHEN PorcNoViableSem1 <> 0 THEN PorcNoViableSem1XPobInicial ELSE 0 END) AS PorcNoViableSem1XPobInicial,
        SUM(CASE WHEN PorcNoViableSem2 <> 0 THEN PorcNoViableSem2XPobInicial ELSE 0 END) AS PorcNoViableSem2XPobInicial,
        SUM(CASE WHEN PorcNoViableSem3 <> 0 THEN PorcNoViableSem3XPobInicial ELSE 0 END) AS PorcNoViableSem3XPobInicial,
        SUM(CASE WHEN PorcNoViableSem4 <> 0 THEN PorcNoViableSem4XPobInicial ELSE 0 END) AS PorcNoViableSem4XPobInicial,
        SUM(CASE WHEN PorcNoViableSem5 <> 0 THEN PorcNoViableSem5XPobInicial ELSE 0 END) AS PorcNoViableSem5XPobInicial,
        SUM(CASE WHEN PorcNoViableSem6 <> 0 THEN PorcNoViableSem6XPobInicial ELSE 0 END) AS PorcNoViableSem6XPobInicial,
        SUM(CASE WHEN PorcNoViableSem7 <> 0 THEN PorcNoViableSem7XPobInicial ELSE 0 END) AS PorcNoViableSem7XPobInicial,
        SUM(CASE WHEN PorcNoViableSem8 <> 0 THEN PorcNoViableSem8XPobInicial ELSE 0 END) AS PorcNoViableSem8XPobInicial,
        SUM(CASE WHEN PesoAlo <> 0 THEN PesoAloXPobInicial ELSE 0 END) AS PesoAloXPobInicial,
        SUM(CASE WHEN PesoHvo <> 0 THEN PesoHvoXPobInicial ELSE 0 END) AS PesoHvoXPobInicial,
        SUM(CASE WHEN PobInicialXSTDMortAcumG <> 0 THEN PobInicialXSTDMortAcumG ELSE 0 END) AS PobInicialXSTDMortAcumG
    FROM {database_name_tmp}.ProduccionDetalleGalpon
    GROUP BY pk_lote
)

select 
 max(fecha) as fecha
,case when min(pk_estado) = 1 then substring((cast(max(fecha) as varchar(8))),1,6) else substring((cast(max(FechaCierre) as varchar(8))),1,6) end pk_mes
,a.pk_empresa
,A.pk_division
,pk_zona
,pk_subzona
,A.pk_plantel
,A.pk_lote
,c.pk_tipoproducto
,29 as pk_especie
,min(pk_estado) pk_estado
,min(pk_administrador) as pk_administrador
,MAX(pk_proveedor) pk_proveedor
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0)) as int) as pk_diasvida
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0)) as int) * sum(PobInicial) as pk_diasvidaXPobInicial
,b.clote as ComplexEntityNoLote
,max(FechaCierre) as FechaCierre
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,max(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(AreaGalpon) as AreaGalpon
,sum(PobInicial) as PobInicial
,sum(a.AvesRendidas) as AvesRendidas
,sum(KilosRendidos) as KilosRendidos
,sum(MortDia) as MortDia
,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE ((cast(SUM(MortDia) as integer) / SUM(PobInicial*1.0) )*100) END AS PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,sum(MortSem8) AS MortSem8
,sum(MortSem9) AS MortSem9
,sum(MortSem10) AS MortSem10
,sum(MortSem11) AS MortSem11
,sum(MortSem12) AS MortSem12
,sum(MortSem13) AS MortSem13
,sum(MortSem14) AS MortSem14
,sum(MortSem15) AS MortSem15
,sum(MortSem16) AS MortSem16
,sum(MortSem17) AS MortSem17
,sum(MortSem18) AS MortSem18
,sum(MortSem19) AS MortSem19
,sum(MortSem20) AS MortSem20
,sum(MortSemAcum1) as MortSemAcum1 
,sum(MortSemAcum2) as MortSemAcum2 
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4 
,sum(MortSemAcum5) as MortSemAcum5 
,sum(MortSemAcum6) as MortSemAcum6 
,sum(MortSemAcum7) as MortSemAcum7 
,sum(MortSemAcum8) AS MortSemAcum8 
,sum(MortSemAcum9) AS MortSemAcum9 
,sum(MortSemAcum10) AS MortSemAcum10 
,sum(MortSemAcum11) AS MortSemAcum11 
,sum(MortSemAcum12) AS MortSemAcum12 
,sum(MortSemAcum13) AS MortSemAcum13 
,sum(MortSemAcum14) AS MortSemAcum14 
,sum(MortSemAcum15) AS MortSemAcum15 
,sum(MortSemAcum16) AS MortSemAcum16 
,sum(MortSemAcum17) AS MortSemAcum17 
,sum(MortSemAcum18) AS MortSemAcum18 
,sum(MortSemAcum19) AS MortSemAcum19 
,sum(MortSemAcum20) AS MortSemAcum20
,max(Peso) as Peso
,avg(STDPeso) as STDPeso
,sum(UnidSeleccion) as UnidSeleccion
,sum(ConsDia) as ConsDia
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,CASE WHEN sum(KilosRendidos) = 0 THEN 0.0 ELSE floor((cast(SUM(ConsDia) as integer)/sum(KilosRendidos)) * 1000000) / 1000000 END AS ICA
,sum(CantInicioSaca) as CantInicioSaca
,min(EdadInicioSaca) as EdadInicioSaca
,avg(EdadGranjaLote) as EdadGranjaLote
,avg(D.Gas) as Gas
,avg(D.Cama) as Cama
,avg(D.Agua) as Agua
,sum(CantMacho) as CantMacho
,avg(Pigmentacion) as Pigmentacion
,1 FlagAtipico
,1 FlagArtAtipico
,MAX(EdadPadreLote) EdadPadreLote
,MAX(STDPorcMortAcumC) STDPorcMortAcumC
,MAX(STDPorcMortAcumG) STDPorcMortAcumG
,MAX(STDPorcMortAcumL) STDPorcMortAcumL
,MAX(STDPesoC) STDPesoC
,MAX(STDPesoG) STDPesoG
,MAX(STDPesoL) STDPesoL
,MAX(STDConsAcumC) STDConsAcumC
,MAX(STDConsAcumG) STDConsAcumG
,MAX(STDConsAcumL) STDConsAcumL
,MAX(STDPorcMortSem1) STDPorcMortSem1
,MAX(STDPorcMortSem2) STDPorcMortSem2
,MAX(STDPorcMortSem3) STDPorcMortSem3
,MAX(STDPorcMortSem4) STDPorcMortSem4
,MAX(STDPorcMortSem5) STDPorcMortSem5
,MAX(STDPorcMortSem6) STDPorcMortSem6
,MAX(STDPorcMortSem7) STDPorcMortSem7
,MAX(STDPorcMortSem8) STDPorcMortSem8
,MAX(STDPorcMortSem9) STDPorcMortSem9
,MAX(STDPorcMortSem10) STDPorcMortSem10
,MAX(STDPorcMortSem11) STDPorcMortSem11
,MAX(STDPorcMortSem12) STDPorcMortSem12
,MAX(STDPorcMortSem13) STDPorcMortSem13
,MAX(STDPorcMortSem14) STDPorcMortSem14
,MAX(STDPorcMortSem15) STDPorcMortSem15
,MAX(STDPorcMortSem16) STDPorcMortSem16
,MAX(STDPorcMortSem17) STDPorcMortSem17
,MAX(STDPorcMortSem18) STDPorcMortSem18
,MAX(STDPorcMortSem19) STDPorcMortSem19
,MAX(STDPorcMortSem20) STDPorcMortSem20
,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
,MAX(STDPorcMortSem1G) STDPorcMortSem1G
,MAX(STDPorcMortSem2G) STDPorcMortSem2G
,MAX(STDPorcMortSem3G) STDPorcMortSem3G
,MAX(STDPorcMortSem4G) STDPorcMortSem4G
,MAX(STDPorcMortSem5G) STDPorcMortSem5G
,MAX(STDPorcMortSem6G) STDPorcMortSem6G
,MAX(STDPorcMortSem7G) STDPorcMortSem7G
,MAX(STDPorcMortSem8G) STDPorcMortSem8G
,MAX(STDPorcMortSem9G) STDPorcMortSem9G
,MAX(STDPorcMortSem10G) STDPorcMortSem10G
,MAX(STDPorcMortSem11G) STDPorcMortSem11G
,MAX(STDPorcMortSem12G) STDPorcMortSem12G
,MAX(STDPorcMortSem13G) STDPorcMortSem13G
,MAX(STDPorcMortSem14G) STDPorcMortSem14G
,MAX(STDPorcMortSem15G) STDPorcMortSem15G
,MAX(STDPorcMortSem16G) STDPorcMortSem16G
,MAX(STDPorcMortSem17G) STDPorcMortSem17G
,MAX(STDPorcMortSem18G) STDPorcMortSem18G
,MAX(STDPorcMortSem19G) STDPorcMortSem19G
,MAX(STDPorcMortSem20G) STDPorcMortSem20G
,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1G
,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2G
,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3G
,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4G
,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5G
,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6G
,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7G
,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8G
,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9G
,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10G
,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11G
,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12G
,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13G
,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14G
,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15G
,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16G
,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17G
,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18G
,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19G
,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20G
,MAX(STDPorcMortSem1L) STDPorcMortSem1L
,MAX(STDPorcMortSem2L) STDPorcMortSem2L
,MAX(STDPorcMortSem3L) STDPorcMortSem3L
,MAX(STDPorcMortSem4L) STDPorcMortSem4L
,MAX(STDPorcMortSem5L) STDPorcMortSem5L
,MAX(STDPorcMortSem6L) STDPorcMortSem6L
,MAX(STDPorcMortSem7L) STDPorcMortSem7L
,MAX(STDPorcMortSem8L) STDPorcMortSem8L
,MAX(STDPorcMortSem9L) STDPorcMortSem9L
,MAX(STDPorcMortSem10L) STDPorcMortSem10L
,MAX(STDPorcMortSem11L) STDPorcMortSem11L
,MAX(STDPorcMortSem12L) STDPorcMortSem12L
,MAX(STDPorcMortSem13L) STDPorcMortSem13L
,MAX(STDPorcMortSem14L) STDPorcMortSem14L
,MAX(STDPorcMortSem15L) STDPorcMortSem15L
,MAX(STDPorcMortSem16L) STDPorcMortSem16L
,MAX(STDPorcMortSem17L) STDPorcMortSem17L
,MAX(STDPorcMortSem18L) STDPorcMortSem18L
,MAX(STDPorcMortSem19L) STDPorcMortSem19L
,MAX(STDPorcMortSem20L) STDPorcMortSem20L
,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1L
,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2L
,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3L
,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4L
,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5L
,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6L
,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7L
,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8L
,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9L
,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10L
,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11L
,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12L
,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13L
,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14L
,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15L
,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16L
,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17L
,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18L
,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19L
,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20L
,MAX(STDPesoSem1)STDPesoSem1
,MAX(STDPesoSem2)STDPesoSem2
,MAX(STDPesoSem3)STDPesoSem3
,MAX(STDPesoSem4)STDPesoSem4
,MAX(STDPesoSem5)STDPesoSem5
,MAX(STDPesoSem6)STDPesoSem6
,MAX(STDPesoSem7)STDPesoSem7
,MAX(STDPesoSem8)STDPesoSem8
,MAX(STDPesoSem9)STDPesoSem9
,MAX(STDPesoSem10)STDPesoSem10
,MAX(STDPesoSem11)STDPesoSem11
,MAX(STDPesoSem12)STDPesoSem12
,MAX(STDPesoSem13)STDPesoSem13
,MAX(STDPesoSem14)STDPesoSem14
,MAX(STDPesoSem15)STDPesoSem15
,MAX(STDPesoSem16)STDPesoSem16
,MAX(STDPesoSem17)STDPesoSem17
,MAX(STDPesoSem18)STDPesoSem18
,MAX(STDPesoSem19)STDPesoSem19
,MAX(STDPesoSem20)STDPesoSem20
,MAX(STDPeso5Dias)STDPeso5Dias
,MAX(STDPorcPesoSem1G)STDPorcPesoSem1G
,MAX(STDPorcPesoSem2G)STDPorcPesoSem2G
,MAX(STDPorcPesoSem3G)STDPorcPesoSem3G
,MAX(STDPorcPesoSem4G)STDPorcPesoSem4G
,MAX(STDPorcPesoSem5G)STDPorcPesoSem5G
,MAX(STDPorcPesoSem6G)STDPorcPesoSem6G
,MAX(STDPorcPesoSem7G)STDPorcPesoSem7G
,MAX(STDPorcPesoSem8G)STDPorcPesoSem8G
,MAX(STDPorcPesoSem9G)STDPorcPesoSem9G
,MAX(STDPorcPesoSem10G)STDPorcPesoSem10G
,MAX(STDPorcPesoSem11G)STDPorcPesoSem11G
,MAX(STDPorcPesoSem12G)STDPorcPesoSem12G
,MAX(STDPorcPesoSem13G)STDPorcPesoSem13G
,MAX(STDPorcPesoSem14G)STDPorcPesoSem14G
,MAX(STDPorcPesoSem15G)STDPorcPesoSem15G
,MAX(STDPorcPesoSem16G)STDPorcPesoSem16G
,MAX(STDPorcPesoSem17G)STDPorcPesoSem17G
,MAX(STDPorcPesoSem18G)STDPorcPesoSem18G
,MAX(STDPorcPesoSem19G)STDPorcPesoSem19G
,MAX(STDPorcPesoSem20G)STDPorcPesoSem20G
,MAX(STDPorcPeso5DiasG)STDPorcPeso5DiasG
,MAX(STDPorcPesoSem1L)STDPorcPesoSem1L
,MAX(STDPorcPesoSem2L)STDPorcPesoSem2L
,MAX(STDPorcPesoSem3L)STDPorcPesoSem3L
,MAX(STDPorcPesoSem4L)STDPorcPesoSem4L
,MAX(STDPorcPesoSem5L)STDPorcPesoSem5L
,MAX(STDPorcPesoSem6L)STDPorcPesoSem6L
,MAX(STDPorcPesoSem7L)STDPorcPesoSem7L
,MAX(STDPorcPesoSem8L)STDPorcPesoSem8L
,MAX(STDPorcPesoSem9L)STDPorcPesoSem9L
,MAX(STDPorcPesoSem10L)STDPorcPesoSem10L
,MAX(STDPorcPesoSem11L)STDPorcPesoSem11L
,MAX(STDPorcPesoSem12L)STDPorcPesoSem12L
,MAX(STDPorcPesoSem13L)STDPorcPesoSem13L
,MAX(STDPorcPesoSem14L)STDPorcPesoSem14L
,MAX(STDPorcPesoSem15L)STDPorcPesoSem15L
,MAX(STDPorcPesoSem16L)STDPorcPesoSem16L
,MAX(STDPorcPesoSem17L)STDPorcPesoSem17L
,MAX(STDPorcPesoSem18L)STDPorcPesoSem18L
,MAX(STDPorcPesoSem19L)STDPorcPesoSem19L
,MAX(STDPorcPesoSem20L)STDPorcPesoSem20L
,MAX(STDPorcPeso5DiasL)STDPorcPeso5DiasL
,MAX(PorcPolloJoven) PorcPolloJoven
,MAX(PorcPesoAloMenor) PorcPesoAloMenor
,MAX(PorcGalponesMixtos) PorcGalponesMixtos
,MAX(STDICAC) STDICAC
,MAX(STDICAG) STDICAG
,MAX(STDICAL) STDICAL
,MAX(PavosBBMortIncub) AS PavosBBMortIncub
,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInviernoC
,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInviernoG
,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInviernoL
,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVeranoC
,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVeranoG
,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVeranoL
,SUM(NoViableSem1) NoViableSem1
,SUM(NoViableSem2) NoViableSem2
,SUM(NoViableSem3) NoViableSem3
,SUM(NoViableSem4) NoViableSem4
,SUM(NoViableSem5) NoViableSem5
,SUM(NoViableSem6) NoViableSem6
,SUM(NoViableSem7) NoViableSem7
,SUM(NoViableSem8) NoViableSem8
,CASE WHEN sum(a.AvesRendidas) = 0 THEN 0.0 ELSE floor((sum(KilosRendidos)/sum(a.AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
,COALESCE(P.PesoSem1XPobInicial / L.TotalPobInicial, 0) as PesoSem1
,COALESCE(P.PesoSem2XPobInicial / L.TotalPobInicial, 0) as PesoSem2
,COALESCE(P.PesoSem3XPobInicial / L.TotalPobInicial, 0) as PesoSem3
,COALESCE(P.PesoSem4XPobInicial / L.TotalPobInicial, 0) as PesoSem4
,COALESCE(P.PesoSem5XPobInicial / L.TotalPobInicial, 0) as PesoSem5
,COALESCE(P.PesoSem6XPobInicial / L.TotalPobInicial, 0) as PesoSem6
,COALESCE(P.PesoSem7XPobInicial / L.TotalPobInicial, 0) as PesoSem7
,COALESCE(P.PesoSem8XPobInicial / L.TotalPobInicial, 0) as PesoSem8
,COALESCE(P.PesoSem9XPobInicial / L.TotalPobInicial, 0) as PesoSem9
,COALESCE(P.PesoSem10XPobInicial / L.TotalPobInicial, 0) as PesoSem10
,COALESCE(P.PesoSem11XPobInicial / L.TotalPobInicial, 0) as PesoSem11
,COALESCE(P.PesoSem12XPobInicial / L.TotalPobInicial, 0) as PesoSem12
,COALESCE(P.PesoSem13XPobInicial / L.TotalPobInicial, 0) as PesoSem13
,COALESCE(P.PesoSem14XPobInicial / L.TotalPobInicial, 0) as PesoSem14
,COALESCE(P.PesoSem15XPobInicial / L.TotalPobInicial, 0) as PesoSem15
,COALESCE(P.PesoSem16XPobInicial / L.TotalPobInicial, 0) as PesoSem16
,COALESCE(P.PesoSem17XPobInicial / L.TotalPobInicial, 0) as PesoSem17
,COALESCE(P.PesoSem18XPobInicial / L.TotalPobInicial, 0) as PesoSem18
,COALESCE(P.PesoSem19XPobInicial / L.TotalPobInicial, 0) as PesoSem19
,COALESCE(P.PesoSem20XPobInicial / L.TotalPobInicial, 0) as PesoSem20
,COALESCE(P.Peso5DiasXPobInicial / L.TotalPobInicial, 0) as Peso5Dias
,COALESCE(P.PesoSem1XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem1XPobInicial
,COALESCE(P.PesoSem2XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem2XPobInicial
,COALESCE(P.PesoSem3XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem3XPobInicial
,COALESCE(P.PesoSem4XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem4XPobInicial
,COALESCE(P.PesoSem5XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem5XPobInicial
,COALESCE(P.PesoSem6XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem6XPobInicial
,COALESCE(P.PesoSem7XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem7XPobInicial
,COALESCE(P.PesoSem8XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem8XPobInicial
,COALESCE(P.PesoSem9XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem9XPobInicial
,COALESCE(P.PesoSem10XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem10XPobInicial
,COALESCE(P.PesoSem11XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem11XPobInicial
,COALESCE(P.PesoSem12XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem12XPobInicial
,COALESCE(P.PesoSem13XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem13XPobInicial
,COALESCE(P.PesoSem14XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem14XPobInicial
,COALESCE(P.PesoSem15XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem15XPobInicial
,COALESCE(P.PesoSem16XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem16XPobInicial
,COALESCE(P.PesoSem17XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem17XPobInicial
,COALESCE(P.PesoSem18XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem18XPobInicial
,COALESCE(P.PesoSem19XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem19XPobInicial
,COALESCE(P.PesoSem20XPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoSem20XPobInicial
,COALESCE(P.Peso5DiasXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as Peso5DiasXPobInicial
,COALESCE(NV.PorcNoViableSem1XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem1
,COALESCE(NV.PorcNoViableSem2XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem2
,COALESCE(NV.PorcNoViableSem3XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem3
,COALESCE(NV.PorcNoViableSem4XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem4
,COALESCE(NV.PorcNoViableSem5XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem5
,COALESCE(NV.PorcNoViableSem6XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem6
,COALESCE(NV.PorcNoViableSem7XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem7
,COALESCE(NV.PorcNoViableSem8XPobInicial / L.TotalPobInicial, 0) as PorcNoViableSem8
,COALESCE(NV.PesoAloXPobInicial / L.TotalPobInicial, 0) as PesoAlo
,COALESCE(NV.PesoHvoXPobInicial / L.TotalPobInicial, 0) as PesoHvo
,COALESCE(NV.PesoAloXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoAloXPobInicial
,COALESCE(NV.PesoHvoXPobInicial / L.TotalPobInicial, 0) * L.TotalPobInicial as PesoHvoXPobInicial
,COALESCE(NV.PobInicialXSTDMortAcumG / L.TotalPobInicial, 0) as STDMortAcumL
,COALESCE(NV.PobInicialXSTDMortAcumG / L.TotalPobInicial, 0) * L.TotalPobInicial as PobInicialXSTDMortAcumL
,Ca.categoria
from {database_name_tmp}.ProduccionDetalleGalpon A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN MaxOrden X on X.clote = B.clote
LEFT JOIN {database_name_tmp}.OrdenId C ON B.clote = C.clote AND C.orden = X.max_orden
left join {database_name_gl}.stg_GasAguaCama D on b.clote = D.ComplexEntityNo
left join {database_name_tmp}.CalculosPorcPoblacion E on A.pk_empresa = E.pk_empresa and A.pk_division = E.pk_division and A.pk_plantel = E.pk_plantel and A.pk_lote = E.pk_lote
LEFT JOIN PobInicialLote L ON A.pk_lote = L.pk_lote
LEFT JOIN PesosPorSemana P ON A.pk_lote = P.pk_lote
LEFT JOIN NoViablesPorSemana NV ON A.pk_lote = NV.pk_lote
LEFT JOIN Categorias Ca ON A.pk_lote = Ca.pk_lote
where FlagAtipico = 1
group by a.pk_empresa,A.pk_division,pk_zona,pk_subzona,A.pk_plantel,A.pk_lote,c.pk_tipoproducto,b.clote,L.TotalPobInicial
,P.PesoSem1XPobInicial,P.PesoSem2XPobInicial,P.PesoSem3XPobInicial,P.PesoSem4XPobInicial,P.PesoSem5XPobInicial,P.PesoSem6XPobInicial 
,P.PesoSem7XPobInicial,P.PesoSem8XPobInicial,P.PesoSem9XPobInicial,P.PesoSem10XPobInicial,P.PesoSem11XPobInicial,P.PesoSem12XPobInicial
,P.PesoSem13XPobInicial,P.PesoSem14XPobInicial,P.PesoSem15XPobInicial,P.PesoSem16XPobInicial,P.PesoSem17XPobInicial,P.PesoSem18XPobInicial
,P.PesoSem19XPobInicial,P.PesoSem20XPobInicial,P.Peso5DiasXPobInicial,NV.PorcNoViableSem1XPobInicial,NV.PorcNoViableSem2XPobInicial
,NV.PorcNoViableSem3XPobInicial,NV.PorcNoViableSem4XPobInicial,NV.PorcNoViableSem5XPobInicial,NV.PorcNoViableSem6XPobInicial
,NV.PorcNoViableSem7XPobInicial,NV.PorcNoViableSem8XPobInicial,NV.PesoAloXPobInicial,NV.PesoHvoXPobInicial,NV.PobInicialXSTDMortAcumG,Ca.categoria
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleLoteSinAtipico"
}
df_ProduccionDetalleLoteSinAtipico.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleLoteSinAtipico")
print('carga ProduccionDetalleLoteSinAtipico', df_ProduccionDetalleLoteSinAtipico.count())   
#Se muestra ProduccionDetalleLote
df_INSProduccionDetalleLote = spark.sql(f"""
select * from {database_name_tmp}.ProduccionDetalleLoteSinAtipico
except
select * from {database_name_tmp}.ProduccionDetalleLote
""")
print('carga insert ProduccionDetalleLote', df_INSProduccionDetalleLote.count())   
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleLote"
}
df_INSProduccionDetalleLote.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleLote")
print('se inserta la data a ProduccionDetalleLote', df_INSProduccionDetalleLote.count())
#Se muestra ProduccionDetalleMensual
df_ProduccionDetalleMensual = spark.sql(f"""
WITH PobInicialPorMes AS (SELECT pk_mes,pk_division,SUM(PobInicial) AS TotalPobInicial FROM {database_name_tmp}.ProduccionDetalleLote WHERE pk_estado IN (2,3) AND FlagArtAtipico = 2 GROUP BY pk_mes,pk_division)
select  a.pk_mes
,a.pk_empresa
,a.pk_division
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0))as int) as pk_diasvida
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,max(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(AreaGalpon) as AreaGalpon
,sum(PobInicial) as PobInicial
,sum(AvesRendidas) as AvesRendidas
,sum(KilosRendidos) as KilosRendidos
,sum(MortDia) as MortDia
,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE ((cast(SUM(MortDia) as integer) / SUM(PobInicial*1.0) )*100) END AS PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,sum(MortSem8) AS MortSem8
,sum(MortSem9) AS MortSem9
,sum(MortSem10) AS MortSem10
,sum(MortSem11) AS MortSem11
,sum(MortSem12) AS MortSem12
,sum(MortSem13) AS MortSem13
,sum(MortSem14) AS MortSem14
,sum(MortSem15) AS MortSem15
,sum(MortSem16) AS MortSem16
,sum(MortSem17) AS MortSem17
,sum(MortSem18) AS MortSem18
,sum(MortSem19) AS MortSem19
,sum(MortSem20) AS MortSem20
,sum(MortSemAcum1) as MortSemAcum1 
,sum(MortSemAcum2) as MortSemAcum2 
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4 
,sum(MortSemAcum5) as MortSemAcum5 
,sum(MortSemAcum6) as MortSemAcum6 
,sum(MortSemAcum7) as MortSemAcum7
,sum(MortSemAcum8) AS MortSemAcum8 
,sum(MortSemAcum9) AS MortSemAcum9 
,sum(MortSemAcum10) AS MortSemAcum10 
,sum(MortSemAcum11) AS MortSemAcum11 
,sum(MortSemAcum12) AS MortSemAcum12 
,sum(MortSemAcum13) AS MortSemAcum13 
,sum(MortSemAcum14) AS MortSemAcum14 
,sum(MortSemAcum15) AS MortSemAcum15 
,sum(MortSemAcum16) AS MortSemAcum16 
,sum(MortSemAcum17) AS MortSemAcum17 
,sum(MortSemAcum18) AS MortSemAcum18 
,sum(MortSemAcum19) AS MortSemAcum19 
,sum(MortSemAcum20) AS MortSemAcum20 
,max(Peso) as Peso
,avg(PesoAlo) as PesoAlo
,CASE WHEN sum(AvesRendidas) = 0 THEN 0.0 ELSE floor((sum(KilosRendidos)/sum(AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
,avg(STDPeso) as STDPeso
,sum(UnidSeleccion) as UnidSeleccion
,sum(ConsDia) as ConsDia
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,CASE WHEN sum(KilosRendidos) = 0 THEN 0.0 ELSE floor((cast(SUM(ConsDia) as integer)/sum(KilosRendidos)) * 1000000) / 1000000 END AS ICA
,sum(CantInicioSaca) as CantInicioSaca
,min(EdadInicioSaca) as EdadInicioSaca
,avg(EdadGranjaLote) as EdadGranjaLote
,avg(Gas) as Gas
,avg(Cama) as Cama
,avg(Agua) as Agua
,sum(CantMacho) as CantMacho
,avg(Pigmentacion) as Pigmentacion
,max(FlagAtipico) FlagAtipico
,max(FlagArtAtipico) FlagArtAtipico
,MAX(PavosBBMortIncub) AS PavosBBMortIncub
,SUM(A.PesoSem1XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem1
,SUM(A.PesoSem2XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem2
,SUM(A.PesoSem3XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem3
,SUM(A.PesoSem4XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem4
,SUM(A.PesoSem5XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem5
,SUM(A.PesoSem6XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem6
,SUM(A.PesoSem7XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem7
,SUM(A.PesoSem8XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem8
,SUM(A.PesoSem9XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem9
,SUM(A.PesoSem10XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem10
,SUM(A.PesoSem11XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem11
,SUM(A.PesoSem12XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem12
,SUM(A.PesoSem13XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem13
,SUM(A.PesoSem14XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem14
,SUM(A.PesoSem15XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem15
,SUM(A.PesoSem16XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem16
,SUM(A.PesoSem17XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem17
,SUM(A.PesoSem18XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem18
,SUM(A.PesoSem19XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem19
,SUM(A.PesoSem20XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem20
,SUM(A.Peso5DiasXPobInicial) / COALESCE(P.TotalPobInicial, 1) AS Peso5Dias		
from {database_name_tmp}.ProduccionDetalleLote A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN PobInicialPorMes P ON A.pk_mes = P.pk_mes AND A.pk_division = P.pk_division
where pk_estado in (3,4) and FlagArtAtipico = 2
group by a.pk_mes,a.pk_empresa,a.pk_division,P.TotalPobInicial
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleMensual"
}
df_ProduccionDetalleMensual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleMensual")
print('carga ProduccionDetalleMensual', df_ProduccionDetalleMensual.count())   
#Se muestra ProduccionDetalleMensualSinAtipico
df_ProduccionDetalleMensualSinAtipico = spark.sql(f"""
WITH PobInicialPorMes AS (SELECT pk_mes,pk_division,SUM(PobInicial) AS TotalPobInicial FROM {database_name_tmp}.ProduccionDetalleLote WHERE pk_estado IN (2,3) AND FlagArtAtipico = 1 AND FlagAtipico = 1 GROUP BY pk_mes,pk_division)

select A.pk_mes
,a.pk_empresa
,A.pk_division
,cast((round((case when sum(PobInicial) = 0 then 0 else sum(pk_diasvidaXPobInicial)/sum(PobInicial*1.0) end),0)) as int) as pk_diasvida
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,max(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(AreaGalpon) as AreaGalpon
,sum(PobInicial) as PobInicial
,sum(AvesRendidas) as AvesRendidas
,sum(KilosRendidos) as KilosRendidos
,sum(MortDia) as MortDia
,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE ((cast(SUM(MortDia) as integer) / SUM(PobInicial*1.0) )*100) END AS PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,sum(MortSem8) AS MortSem8
,sum(MortSem9) AS MortSem9
,sum(MortSem10) AS MortSem10
,sum(MortSem11) AS MortSem11
,sum(MortSem12) AS MortSem12
,sum(MortSem13) AS MortSem13
,sum(MortSem14) AS MortSem14
,sum(MortSem15) AS MortSem15
,sum(MortSem16) AS MortSem16
,sum(MortSem17) AS MortSem17
,sum(MortSem18) AS MortSem18
,sum(MortSem19) AS MortSem19
,sum(MortSem20) AS MortSem20
,sum(MortSemAcum1) as MortSemAcum1 
,sum(MortSemAcum2) as MortSemAcum2 
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4 
,sum(MortSemAcum5) as MortSemAcum5 
,sum(MortSemAcum6) as MortSemAcum6 
,sum(MortSemAcum7) as MortSemAcum7 
,sum(MortSemAcum8) AS MortSemAcum8 
,sum(MortSemAcum9) AS MortSemAcum9 
,sum(MortSemAcum10) AS MortSemAcum10 
,sum(MortSemAcum11) AS MortSemAcum11 
,sum(MortSemAcum12) AS MortSemAcum12 
,sum(MortSemAcum13) AS MortSemAcum13 
,sum(MortSemAcum14) AS MortSemAcum14 
,sum(MortSemAcum15) AS MortSemAcum15 
,sum(MortSemAcum16) AS MortSemAcum16 
,sum(MortSemAcum17) AS MortSemAcum17 
,sum(MortSemAcum18) AS MortSemAcum18 
,sum(MortSemAcum19) AS MortSemAcum19 
,sum(MortSemAcum20) AS MortSemAcum20 
,max(Peso) as Peso
,avg(PesoAlo) as PesoAlo
,CASE WHEN sum(AvesRendidas) = 0 THEN 0.0 ELSE floor((sum(KilosRendidos)/sum(AvesRendidas)) * 1000000) / 1000000 END AS PesoProm
,avg(STDPeso) as STDPeso
,sum(UnidSeleccion) as UnidSeleccion
,sum(ConsDia) as ConsDia
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,CASE WHEN sum(KilosRendidos) = 0 THEN 0.0 ELSE floor((cast(SUM(ConsDia) as integer)/sum(KilosRendidos)) * 1000000) / 1000000 END AS ICA
,sum(CantInicioSaca) as CantInicioSaca
,min(EdadInicioSaca) as EdadInicioSaca
,avg(EdadGranjaLote) as EdadGranjaLote
,avg(Gas) as Gas
,avg(Cama) as Cama
,avg(Agua) as Agua
,sum(CantMacho) as CantMacho
,avg(Pigmentacion) as Pigmentacion
,1 FlagAtipico
,max(FlagArtAtipico) FlagArtAtipico
,MAX(PavosBBMortIncub) AS PavosBBMortIncub
,SUM(A.PesoSem1XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem1
,SUM(A.PesoSem2XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem2
,SUM(A.PesoSem3XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem3
,SUM(A.PesoSem4XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem4
,SUM(A.PesoSem5XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem5
,SUM(A.PesoSem6XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem6
,SUM(A.PesoSem7XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem7
,SUM(A.PesoSem8XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem8
,SUM(A.PesoSem9XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem9
,SUM(A.PesoSem10XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem10
,SUM(A.PesoSem11XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem11
,SUM(A.PesoSem12XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem12
,SUM(A.PesoSem13XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem13
,SUM(A.PesoSem14XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem14
,SUM(A.PesoSem15XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem15
,SUM(A.PesoSem16XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem16
,SUM(A.PesoSem17XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem17
,SUM(A.PesoSem18XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem18
,SUM(A.PesoSem19XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem19
,SUM(A.PesoSem20XPobInicial) / COALESCE(P.TotalPobInicial, 1) AS PesoSem20
,SUM(A.Peso5DiasXPobInicial) / COALESCE(P.TotalPobInicial, 1) AS Peso5Dias
from {database_name_tmp}.ProduccionDetalleLote A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN PobInicialPorMes P ON A.pk_mes = P.pk_mes AND A.pk_division = P.pk_division
where pk_estado in (3,4) and FlagAtipico = 1 and FlagArtAtipico = 1
group by A.pk_mes,a.pk_empresa,A.pk_division,P.TotalPobInicial
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleMensualSinAtipico"
}
df_ProduccionDetalleMensualSinAtipico.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleMensualSinAtipico")
print('carga ProduccionDetalleMensualSinAtipico', df_ProduccionDetalleMensualSinAtipico.count())   
#Se muestra ProduccionDetalleMensual
df_INSProduccionDetalleMensual = spark.sql(f"""
select * from {database_name_tmp}.ProduccionDetalleMensualSinAtipico
except
select * from {database_name_tmp}.ProduccionDetalleMensual
""")
print('carga INSERT ProduccionDetalleMensual', df_INSProduccionDetalleMensual.count())   
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleMensual"
}
df_INSProduccionDetalleMensual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_tmp}.ProduccionDetalleMensual")
print('se inserta la data a ProduccionDetalleMensual', df_INSProduccionDetalleMensual.count())
#Se muestra ft_consolidado_Diario
df_ft_consolidado_Diario = spark.sql(f"""
WITH ProduccionAnterior AS (SELECT ComplexEntityNo,pk_semanavida,MAX(PesoSem) AS PesoSemAnterior FROM {database_name_tmp}.Produccion GROUP BY ComplexEntityNo,pk_semanavida),
STD_Mortalidad AS (SELECT ComplexEntityNo,pk_semanavida,ROUND(SUM(STDMortDia), 2) AS STDPorcMortSem,ROUND((SUM(STDMortDia) / 100) * AVG(PobInicial), 0) AS STDMortSem FROM {database_name_tmp}.ProduccionDetalleEdad GROUP BY ComplexEntityNo,pk_semanavida)

select
 max(pk_tiempo) as pk_tiempo
,b.pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_lote
,pk_galpon
,pk_sexo
,pk_standard
,pk_producto
,pk_grupoconsumo
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,B.pk_diasvida
,b.pk_semanavida
,B.ComplexEntityNo
,FechaNacimiento as Nacimiento
,max(Edad) as Edad
,FechaCierre
,FechaAlojamiento
,avg(PobInicial) as PobInicial
,avg(PobInicial) - max(COALESCE(MortAcum,0)) as Inventario
,avg(AvesRendidas) as AvesRendidas
,avg(AvesRendidasAcum) as AvesRendidasAcum
,avg(KilosRendidos) as KilosRendidos
,avg(KilosRendidosAcum) as KilosRendidosAcum
,max(MortDia) as MortDia
,max(MortAcum) as MortAcum
,max(MortSem) as MortSem
,round(((max(STDMortDia)/100)*avg(PobInicial)),0) as STDMortDia
,round(((max(STDMortAcum)/100)*avg(PobInicial)),0) as STDMortAcum
,sm.STDMortSem as STDMortSem
,round((max(PorcMortDia)),2) as PorcMortDia 
,round((max(PorcMortAcum)),2) as PorcMortAcum
,round((max(PorcMortSem)),2) as PorcMortSem
,round((max(STDMortDia)),2) as STDPorcMortDia
,round((max(STDMortAcum)),2) as STDPorcMortAcum
,sm.STDPorcMortSem STDPorcMortSem
,round((round((max(PorcMortDia)),2) - max(STDMortDia)),2) as DifPorcMort_STD
,round((round((max(PorcMortAcum)),2) - max(STDMortAcum)),2) as DifPorcMortAcum_STDAcum
,ROUND(ROUND(MAX(b.PorcMortSem), 2) - sm.STDPorcMortSem, 2) as DifPorcMortSem_STDSem
,avg(AreaGalpon) as AreaGalpon
,sum(ConsDia) as Consumo
,max(ConsAcum) as ConsAcum
,case when avg(PobInicial) = 0 then 0.0
	  when (avg(PobInicial) - max(MortAcum)) = 0 then 0.0
	  else round((sum(ConsDia) / COALESCE((avg(PobInicial) - max(MortAcum)),0)),3) end as PorcConsDia
,case when avg(PobInicial) = 0 then 0.0
	  when (avg(PobInicial) - max(MortAcum)) = 0 then 0.0
	  else round((max(ConsAcum) / COALESCE((avg(PobInicial) - max(MortAcum)),0)),3) end as PorcConsAcum
,round((max(STDConsDia)),3) as STDConsDia
,round((max(STDConsAcum)),3) as STDConsAcum
,round((((case when avg(PobInicial) = 0 then 0.0 when (avg(PobInicial) - max(MortAcum)) = 0 then 0.0 else sum(ConsDia) / COALESCE((avg(PobInicial) - max(MortAcum)),0) end) - max(STDConsDia)) * 1000),3) as DifPorcCons_STD
,round((((case when avg(PobInicial) = 0 then 0.0 when (avg(PobInicial) - max(MortAcum)) = 0 then 0.0 else max(ConsAcum) / COALESCE((avg(PobInicial) - max(MortAcum)),0) end) - max(STDConsAcum)) * 1000),3) as DifPorcConsAcum_STDAcum
,max(PreInicio) as PreInicio
,max(Inicio) as Inicio
,max(Acabado) as Acabado
,max(Terminado) as Terminado
,max(Finalizador) as Finalizador
,max(Peso_STD) as PesoMasSTD
,max(Peso) as Peso
,max(PesoSem) as PesoSem
,max(STDPeso) as STDPeso
,case when max(Peso) = 0 then 0.0 else (max(Peso) - max(STDPeso)) * 1000 end as DifPeso_STD
,avg(PesoAlo) as PesoAlo
,case when avg(PesoAlo) = 0 then 0 when B.pk_diasvida in (4,7,14,21,28,35,42,49,56) then max(PesoSem) / COALESCE((avg(PesoAlo)),0) end as TasaCre
,case when max(edad) = 0 then 0 else round(((max(Peso)/COALESCE(max(Edad),0))*1000),2) end as GananciaDia
,max(PesoSem)-COALESCE(pa.PesoSemAnterior, 0) as GananciaSem
,round((max(STDGanancia)),3) as STDGanancia
,(case when max(STDConsDia) = 0.0 then 0.0 else max(STDGanancia)/max(STDConsDia) end ) * (case when avg(PobInicial) = 0 then 0.0
																								when (avg(PobInicial) - max(MortAcum)) = 0 then 0.0
																								else ((sum(ConsDia) / COALESCE((avg(PobInicial) - max(MortAcum)),0))) end ) as GananciaEsp
,MAX(DifPesoActPesoAnt) DifPesoActPesoAnt
,MAX(CantDia) CantDia
,MAX(GananciaPesoDia) GananciaPesoDia
,case when avg(PobInicial) - max(COALESCE(MortAcum,0)) = 0 then 0
		when max(PesoDia) = 0 then 0.0
		else (max(ConsAcum) / COALESCE((avg(PobInicial) - max(COALESCE(MortAcum,0))),0)) / COALESCE(max(PesoDia),0) end as ICA
,max(STDICA) as STDICA
,case when B.complexentityno like 'v%' then ((8.5-max(PesoDia))/COALESCE((12.22+ (case when avg(PobInicial) - max(COALESCE(MortAcum,0)) = 0 then 0
																		when max(PesoDia) = 0 then 0.0
																		else (max(ConsAcum) / COALESCE((avg(PobInicial) - max(COALESCE(MortAcum,0))),0)) /COALESCE( max(PesoDia),0) end)),0))
else
(case when pk_sexo = 1 then
((2.5 - max(PesoDia))/3) + 
							(case when avg(PobInicial) - max(COALESCE(MortAcum,0)) = 0 then 0
									when max(PesoDia) = 0 then 0.0
									else (max(ConsAcum) / COALESCE((avg(PobInicial) - max(COALESCE(MortAcum,0))),0)) / COALESCE(max(PesoDia),0) end) 
 else 
 ((2.5 - max(PesoDia))/5.2) + 
							(case when avg(PobInicial) - max(COALESCE(MortAcum,0)) = 0 then 0
									when max(PesoDia) = 0 then 0.0
									else (max(ConsAcum) / COALESCE((avg(PobInicial) - max(COALESCE(MortAcum,0))),0)) / COALESCE(max(PesoDia),0) end) 
 end) end  as ICAAjustado
,case when avg(KilosRendidosAcum)  = 0 then 0.0 else max(ConsAcum) / COALESCE(avg(KilosRendidosAcum),0) end as ICAVenta
,sum(U_PEAccidentados) as U_PEAccidentados
,sum(U_PEHigadoGraso) as U_PEHigadoGraso
,sum(U_PEHepatomegalia) as U_PEHepatomegalia
,sum(U_PEHigadoHemorragico) as U_PEHigadoHemorragico
,sum(U_PEInanicion) as U_PEInanicion
,sum(U_PEProblemaRespiratorio) as U_PEProblemaRespiratorio
,sum(U_PESCH) as U_PESCH
,sum(U_PEEnteritis) as U_PEEnteritis
,sum(U_PEAscitis) as U_PEAscitis
,sum(U_PEMuerteSubita) as U_PEMuerteSubita
,sum(U_PEEstresPorCalor) as U_PEEstresPorCalor
,sum(U_PEHidropericardio) as U_PEHidropericardio
,sum(U_PEHemopericardio) as U_PEHemopericardio
,sum(U_PEUratosis) as U_PEUratosis
,sum(U_PEMaterialCaseoso) as U_PEMaterialCaseoso
,sum(U_PEOnfalitis) as U_PEOnfalitis
,sum(U_PERetencionDeYema) as U_PERetencionDeYema
,sum(U_PEErosionDeMolleja) as U_PEErosionDeMolleja
,sum(U_PEHemorragiaMusculos) as U_PEHemorragiaMusculos
,sum(U_PESangreEnCiego) as U_PESangreEnCiego
,sum(U_PEPericarditis) as U_PEPericarditis
,sum(U_PEPeritonitis) as U_PEPeritonitis
,sum(U_PEProlapso) as U_PEProlapso
,sum(U_PEPicaje) as U_PEPicaje
,sum(U_PERupturaAortica) as U_PERupturaAortica
,sum(U_PEBazoMoteado) as U_PEBazoMoteado
,sum(U_PENoViable) as U_PENoViable
,'' as U_PECaja
,'' as U_PEGota
,'' as U_PEIntoxicacion
,'' as U_PERetrazos
,'' as U_PEEliminados
,'' as U_PEAhogados
,'' as U_PEEColi
,'' as U_PEDescarte
,'' as U_PEOtros
,'' as U_PECoccidia
,'' as U_PEDeshidratados
,'' as U_PEHepatitis
,'' as U_PETraumatismo
,avg(Pigmentacion) as Pigmentacion
,categoria
,PadreMayor
,Flagatipico
,max(PesoHvo) PesoHvo
,sum(U_PEAerosaculitisG2) as U_PEAerosaculitisG2
,sum(U_PECojera) as U_PECojera
,sum(U_PEHigadoIcterico) as U_PEHigadoIcterico
,sum(U_PEMaterialCaseoso_po1ra) as U_PEMaterialCaseoso_po1ra
,sum(U_PEMaterialCaseosoMedRetr) as U_PEMaterialCaseosoMedRetr
,sum(U_PENecrosisHepatica) as U_PENecrosisHepatica
,sum(U_PENeumonia) as U_PENeumonia
,sum(U_PESepticemia) as U_PESepticemia
,sum(U_PEVomitoNegro) as U_PEVomitoNegro
,sum(U_PEAsperguillius) as U_PEAsperguillius
,sum(U_PEBazoGrandeMot) as U_PEBazoGrandeMot
,sum(U_PECorazonGrande) as U_PECorazonGrande
,sum(U_PECuadroToxico) as U_PECuadroToxico
,max(PavosBBMortIncub) as PavosBBMortIncub
,max(PavoIni) as PavoIni
,max(Pavo1) as Pavo1
,max(Pavo2) as Pavo2
,max(Pavo3) as Pavo3
,max(Pavo4) as Pavo4
,max(Pavo5) as Pavo5
,max(Pavo6) as Pavo6
,max(Pavo7) as Pavo7
,EdadPadreCorralDescrip
,U_CausaPesoBajo
,U_AccionPesoBajo
,round((max(STDConsDia)*(avg(PobInicial) - max(COALESCE(MortAcum,0)))),3) as STDConsDiaKg
,round((max(STDConsAcum)*(avg(PobInicial) - max(COALESCE(MortAcum,0)))),3) as STDConsAcumKg
,(sum(ConsDia) - round((max(STDConsDia)*(avg(PobInicial) - max(COALESCE(MortAcum,0)))),3)) as DifCons_STD
,(max(ConsAcum) - round((max(STDConsAcum)*(avg(PobInicial) - max(COALESCE(MortAcum,0)))),3)) as DifConsAcum_STDAcum
,CASE WHEN max(Edad) >= 1 AND max(Edad)<=8 AND pk_division = 3 THEN 'PREINICIO'
	  WHEN max(Edad) >= 9 AND max(Edad)<=18 AND pk_division = 3 THEN 'INICIO'
	  WHEN max(Edad) >= 19 AND max(Edad)<=24 AND pk_division = 3 THEN 'ACABADO'
	  WHEN max(Edad) >= 25 AND max(Edad)<=31 AND pk_division = 3 THEN 'TERMINADOR'
	  WHEN max(Edad) >= 32 AND pk_division = 3 THEN 'FINALIZADOR'
 END TipoAlimento
 ,MAX(PreinicioAcum) / COALESCE((max(PobInicial) - max(COALESCE(MortAcum,0))),0) PorcPreinicioAcum
 ,MAX(InicioAcum) / COALESCE((max(PobInicial) - max(COALESCE(MortAcum,0))),0) PorcInicioAcum
 ,MAX(AcabadoAcum) / COALESCE((max(PobInicial) - max(COALESCE(MortAcum,0))),0) PorcAcabadoAcum
 ,MAX(TerminadoAcum) / COALESCE((max(PobInicial) - max(COALESCE(MortAcum,0))),0) PorcTerminadoAcum
 ,MAX(FinalizadorAcum) / COALESCE((max(PobInicial) - max(COALESCE(MortAcum,0))),0) PorcFinalizadorAcum
 ,'' STDConsAcumPreinicio
 ,'' STDConsAcumInicio
 ,'' STDConsAcumAcabado
 ,'' STDConsAcumTerminado
 ,'' STDConsAcumFinalizador
 ,MAX(U_RuidosTotales) RuidosRespiratorios
 ,ProductoConsumo
 ,MAX(STDConsSem) STDConsSem
 ,MAX(STDPorcConsSem) STDPorcConsSem
 ,MAX(ConsSem) ConsSem
 ,round((max(ConsSem) / COALESCE((avg(PobInicial) - max(MortSem)),0)),3)as PorcConsSem
 ,MAX(ConsSem) - MAX(STDConsSem) DifConsSem_STDConsSem
 ,(round((max(ConsSem) / COALESCE((avg(PobInicial) - max(MortSem)),0)),3)) - MAX(STDPorcConsSem) DifPorcConsSem_STDPorcConsSem
 ,MAX(U_ConsumoGasInvierno) STDPorcConsGasInvierno
 ,MAX(U_ConsumoGasVerano) STDPorcConsGasVerano
 ,'' as CantMortSemLesionUno
 ,'' as PorcMortSemLesionUno
 ,'' as DescripMortSemLesionUno
 ,'' as CantMortSemLesionDos
 ,'' as PorcMortSemLesionDos
 ,'' as DescripMortSemLesionDos
 ,'' as CantMortSemLesionTres
 ,'' as PorcMortSemLesionTres
 ,'' as DescripMortSemLesionTres
 ,'' as CantMortSemLesionOtros
 ,'' as PorcMortSemLesionOtros
 ,'' as DescripMortSemLesionOtros
 ,'' as CantMortSemLesionAcumUno
 ,'' as PorcMortSemLesionAcumUno
 ,'' as DescripMortSemLesionAcumUno
 ,'' as CantMortSemLesionAcumDos
 ,'' as PorcMortSemLesionAcumDos
 ,'' as DescripMortSemLesionAcumDos
 ,'' as CantMortSemLesionAcumTres
 ,'' as PorcMortSemLesionAcumTres
 ,'' as DescripMortSemLesionAcumTres
 ,'' as CantMortSemLesionAcumOtros
 ,'' as PorcMortSemLesionAcumOtros
 ,'' as DescripMortSemLesionAcumOtros
 ,avg(PobInicial) - max(COALESCE(MortAcum,0)) - avg(AvesRendidasAcum) as SaldoAves
 ,round((max(STDConsDia)*(CASE WHEN AVG(SaldoAvesRendidasAnterior) IS NULL THEN avg(PobInicial) - max(COALESCE(MortAcum,0)) ELSE AVG(SaldoAvesRendidasAnterior) - max(COALESCE(MortAcum,0)) END)),3) as STDConsDiaKgSaldoAves
 ,round((sum(ConsDia) / COALESCE((CASE WHEN AVG(SaldoAvesRendidasAnterior) IS NULL THEN avg(PobInicial) - max(COALESCE(MortAcum,0)) ELSE AVG(SaldoAvesRendidasAnterior) - max(COALESCE(MortAcum,0)) END),0)),3) as PorcConsDiaSaldoAves
 ,(sum(ConsDia) - round((max(STDConsDia)*(CASE WHEN AVG(SaldoAvesRendidasAnterior) IS NULL THEN avg(PobInicial) - max(COALESCE(MortAcum,0)) ELSE AVG(SaldoAvesRendidasAnterior) - max(COALESCE(MortAcum,0)) END)),3)) as DifCons_STDKgSaldoAves
 ,((((sum(ConsDia) / COALESCE((CASE WHEN AVG(SaldoAvesRendidasAnterior) IS NULL THEN avg(PobInicial) - max(COALESCE(MortAcum,0)) ELSE AVG(SaldoAvesRendidasAnterior) - max(COALESCE(MortAcum,0)) END),0)) - max(STDConsDia)) * 1000)) as DifPorcCons_STDSaldoAves
 ,CASE WHEN AVG(SaldoAvesRendidasAnterior) IS NULL THEN avg(PobInicial) - max(COALESCE(MortAcum,0)) ELSE AVG(SaldoAvesRendidasAnterior) - max(COALESCE(MortAcum,0)) END as SaldoAvesConsumo
,MAX(PorcMortDiaAcum) PorcMortDiaAcum
,MAX(DifPorcMortDiaAcum_STDDiaAcum) DifPorcMortDiaAcum_STDDiaAcum
,MAX(TasaNoViable) TasaNoViable
,MAX(SemPorcPriLesion) SemPorcPriLesion
,MAX(SemPorcSegLesion) SemPorcSegLesion
,MAX(SemPorcTerLesion) SemPorcTerLesion
,DescripTipoAlimentoXTipoProducto TipoAlimentoXTipoProducto
,MAX(STDConsDiaXTipoAlimento) STDConsDiaXTipoAlimentoXTipoProducto
,(avg(PobInicial) - max(COALESCE(MortAcum,0))) * MAX(STDConsDiaXTipoAlimento) STDConsDiaKgXTipoAlimentoXTipoProducto
,ListaFormulaNo
,ListaFormulaName
,TipoOrigen
,MortConcatCorral
,MAX(MortSemAntCorral) MortSemAntCorral
,MortConcatLote
,MAX(MortSemAntLote) MortSemAntLote
,MortConcatSemAnioLote
,PesoConcatCorral
,MAX(PesoSemAntCorral) PesoSemAntCorral
,PesoConcatLote
,MAX(PesoSemAntLote) PesoSemAntLote
,PesoConcatSemAnioLote
,'' MortConcatSemAnioCorral
,'' PesoConcatSemAnioCorral
,MAX(NoViableSem1) NoViableSem1
,MAX(NoViableSem2) NoViableSem2
,MAX(NoViableSem3) NoViableSem3
,MAX(NoViableSem4) NoViableSem4
,MAX(NoViableSem5) NoViableSem5
,MAX(NoViableSem6) NoViableSem6
,MAX(NoViableSem7) NoViableSem7
,MAX(PorcNoViableSem1) PorcNoViableSem1
,MAX(PorcNoViableSem2) PorcNoViableSem2
,MAX(PorcNoViableSem3) PorcNoViableSem3
,MAX(PorcNoViableSem4) PorcNoViableSem4
,MAX(PorcNoViableSem5) PorcNoViableSem5
,MAX(PorcNoViableSem6) PorcNoViableSem6
,MAX(PorcNoViableSem7) PorcNoViableSem7
,MAX(NoViableSem8) NoViableSem8
,MAX(PorcNoViableSem8) PorcNoViableSem8
,pk_tipogranja
,'' U_PENoEspecificado
,'' U_PEAnalisis_Laboratorio
,MAX(GananciaPesoSem) GananciaPesoSem
,MAX(CV) CV
,max(fecha) DescripFecha
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
,'' DescripGrupoConsumo
,'' DescripEspecie
,'' DescripEstado
,'' DescripAdministrador
,'' DescripProveedor
,'' DescripDiaVida
,'' DescripSemanaVida
from {database_name_tmp}.Produccion b
left join (select distinct complexentityno, pk_diasvida, ((PobInicial) - (AvesRendidasAcum)) SaldoAvesRendidasAnterior from {database_name_tmp}.Produccion) C on B.ComplexEntityNo = C.ComplexEntityNo and B.pk_diasvida = C.pk_diasvida +1
LEFT JOIN ProduccionAnterior pa ON b.ComplexEntityNo = pa.ComplexEntityNo AND b.pk_semanavida = pa.pk_semanavida - 1
LEFT JOIN STD_Mortalidad sm ON b.ComplexEntityNo = sm.ComplexEntityNo AND b.pk_semanavida = sm.pk_semanavida
where date_format(CAST(b.fecha AS timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
group by b.pk_empresa,b.pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_grupoconsumo,pk_especie,pk_estado
,pk_administrador,pk_proveedor,B.pk_diasvida,b.pk_semanavida,B.ComplexEntityNo,FechaNacimiento,FechaCierre,FechaAlojamiento,categoria,PadreMayor,FlagAtipico
,EdadPadreCorralDescrip,U_CausaPesoBajo,U_AccionPesoBajo,ProductoConsumo,DescripTipoAlimentoXTipoProducto,ListaFormulaNo,ListaFormulaName,TipoOrigen
,MortConcatCorral,MortConcatLote,MortConcatSemAnioLote,PesoConcatCorral,PesoConcatLote,PesoConcatSemAnioLote,pk_tipogranja
,pa.PesoSemAnterior,sm.STDPorcMortSem,sm.STDMortSem 
""")
print('carga ft_consolidado_diario', df_ft_consolidado_Diario.count())   
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target31 = f"{bucket_name_prdmtech}ft_consolidado_diario/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consolidado_diario cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consolidado_diario")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_consolidado_Diario.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_diarioTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consolidado_diarioTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_diarioTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_diario"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_diario")
            
    print(f"agrega registros nuevos a la tabla ft_consolidado_diario : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consolidado_diario : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_diarioTemporal')
    print(f"Tabla ft_consolidado_diarioTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_diario"
    }
    df_ft_consolidado_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_diario")
#Se muestra ft_consolidado_Corral
df_ft_consolidado_CorralT = spark.sql(f"""
select 
		 max(b.pk_tiempo) as pk_tiempo
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
		,max(pk_proveedor) as pk_proveedor
		,max(pk_diasvida) as pk_diasvida
		,A.ComplexEntityNo
		,FechaNacimiento
		,max(fechacierre) as FechaCierre
		,MAX(FechaCierreLote) FechaCierreLote
		,FechaAlojamiento
        ,FechaCrianza
        ,FechaInicioGranja
        ,FechaInicioSaca
        ,FechaFinSaca
		,PadreMayor
		,RazaMayor
		,IncubadoraMayor
		,MAX(PorcPadreMayor) PorcPadreMayor
		,MAX(PorcRazaMayor) PorCodigoRaza
		,MAX(PorcIncMayor) PorcIncMayor
		,avg(PobInicial) as PobInicial
		,avg(PobInicial) - sum(MortDia) as AvesLogradas
		,avg(AvesRendidas) as AvesRendidas
		,avg(AvesRendidas) - (avg(PobInicial) - sum(MortDia)) as SobranFaltan
		,round((avg(KilosRendidos)),2) as KilosRendidos
		,sum(MortDia) as MortDia
		,case when avg(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / avg(PobInicial*1.0))*100)),2) end as PorMort
		,MAX(STDMortAcum) STDPorMort
		,case when avg(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / avg(PobInicial*1.0))*100)),2) end - MAX(STDMortAcum) DifPorMort_STDPorMort
		,avg(MortSem1) as MortSem1
		,avg(MortSem2) as MortSem2
		,avg(MortSem3) as MortSem3
		,avg(MortSem4) as MortSem4
		,avg(MortSem5) as MortSem5
		,avg(MortSem6) as MortSem6
		,avg(MortSem7) as MortSem7
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem1) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem1
		,MAX(STDPorcMortSem1) STDPorcMortSem1
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem1) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem1) DifPorcMortSem1_STDPorcMortSem1
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem2) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem2
		,MAX(STDPorcMortSem2) STDPorcMortSem2
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem2) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem2) DifPorcMortSem2_STDPorcMortSem2
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem3) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem3
		,MAX(STDPorcMortSem3) STDPorcMortSem3
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem3) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem3) DifPorcMortSem3_STDPorcMortSem3
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem4) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem4
		,MAX(STDPorcMortSem4) STDPorcMortSem4
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem4) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem4) DifPorcMortSem4_STDPorcMortSem4
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem5) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem5
		,MAX(STDPorcMortSem5) STDPorcMortSem5
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem5) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem5) DifPorcMortSem5_STDPorcMortSem5
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem6) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem6
		,MAX(STDPorcMortSem6) STDPorcMortSem6
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem6) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem6) DifPorcMortSem6_STDPorcMortSem6
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem7) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem7
		,MAX(STDPorcMortSem7) STDPorcMortSem7
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem7) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem7) DifPorcMortSem7_STDPorcMortSem7
		,avg(MortSemAcum1) as MortSemAcum1
		,avg(MortSemAcum2) as MortSemAcum2
		,avg(MortSemAcum3) as MortSemAcum3
		,avg(MortSemAcum4) as MortSemAcum4
		,avg(MortSemAcum5) as MortSemAcum5
		,avg(MortSemAcum6) as MortSemAcum6
		,avg(MortSemAcum7) as MortSemAcum7
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum1)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum1
		,MAX(STDPorcMortSemAcum1) STDPorcMortSemAcum1
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum1)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum1) DifPorcMortSemAcum1_STDPorcMortSemAcum1
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum2) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum2
		,MAX(STDPorcMortSemAcum2) STDPorcMortSemAcum2
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum2)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum2) DifPorcMortSemAcum2_STDPorcMortSemAcum2
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum3) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum3
		,MAX(STDPorcMortSemAcum3) STDPorcMortSemAcum3
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum3)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum3) DifPorcMortSemAcum3_STDPorcMortSemAcum3
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum4) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum4
		,MAX(STDPorcMortSemAcum4) STDPorcMortSemAcum4
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum4)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum4) DifPorcMortSemAcum4_STDPorcMortSemAcum4
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum5) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum5
		,MAX(STDPorcMortSemAcum5) STDPorcMortSemAcum5
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum5)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum5) DifPorcMortSemAcum5_STDPorcMortSemAcum5
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum6) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum6
		,MAX(STDPorcMortSemAcum6) STDPorcMortSemAcum6
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum6)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum6) DifPorcMortSemAcum6_STDPorcMortSemAcum6
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum7) / avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum7
		,MAX(STDPorcMortSemAcum7) STDPorcMortSemAcum7
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum7)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum7) DifPorcMortSemAcum7_STDPorcMortSemAcum7
		,sum(PreInicio) as PreInicio
		,sum(Inicio) as Inicio
		,sum(Acabado) as Acabado
		,sum(Terminado) as Terminado
		,sum(Finalizador) as Finalizador
		,sum(ConsDia) as ConsDia
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(PreInicio)/(avg(AvesRendidas))),3) end PorcPreIni
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Inicio)/(avg(AvesRendidas))),3) end PorcIni
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Acabado)/(avg(AvesRendidas))),3) end PorcAcab
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Terminado)/(avg(AvesRendidas))),3) end PorcTerm
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Finalizador)/(avg(AvesRendidas))),3) end PorcFin
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(avg(AvesRendidas))),3) end PorcConsumo
		,MAX(STDConsAcumC) STDPorcConsumo
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(avg(AvesRendidas))),3) end - MAX(STDConsAcumC) DifPorcConsumo_STDPorcConsumo
		,avg(UnidSeleccion) as Seleccion
		,case when AVG(PobInicial) = 0 then 0.0 else round((avg(UnidSeleccion)/AVG(PobInicial*1.0))*100,2) end as PorcSeleccion
		,avg(PesoSem1) as PesoSem1
		,MAX(STDPesoSem1) STDPesoSem1
		,avg(PesoSem1) - MAX(STDPesoSem1) DifPesoSem1_STDPesoSem1
		,avg(PesoSem2) as PesoSem2
		,MAX(STDPesoSem2) STDPesoSem2
		,avg(PesoSem2) - MAX(STDPesoSem2) DifPesoSem2_STDPesoSem2
		,avg(PesoSem3) as PesoSem3
		,MAX(STDPesoSem3) STDPesoSem3
		,avg(PesoSem3) - MAX(STDPesoSem3) DifPesoSem3_STDPesoSem3
		,avg(PesoSem4) as PesoSem4
		,MAX(STDPesoSem4) STDPesoSem4
		,avg(PesoSem4) - MAX(STDPesoSem4) DifPesoSem4_STDPesoSem4
		,avg(PesoSem5) as PesoSem5
		,MAX(STDPesoSem5) STDPesoSem5
		,avg(PesoSem5) - MAX(STDPesoSem5) DifPesoSem5_STDPesoSem5
		,avg(PesoSem6) as PesoSem6
		,MAX(STDPesoSem6) STDPesoSem6
		,avg(PesoSem6) - MAX(STDPesoSem6) DifPesoSem6_STDPesoSem6
		,avg(PesoSem7) as PesoSem7
		,MAX(STDPesoSem7) STDPesoSem7
		,avg(PesoSem7) - MAX(STDPesoSem7) DifPesoSem7_STDPesoSem7
		,case when AVG(AvesRendidas) = 0 then 0.0 else round((avg(KilosRendidos)/avg(AvesRendidas)),3) end as PesoProm
		,MAX(STDPesoC) STDPesoProm
		,case when AVG(AvesRendidas) = 0 then 0.0 else round((avg(KilosRendidos)/avg(AvesRendidas)),3) end - MAX(STDPesoC) DifPesoProm_STDPesoProm
		,case when avg(EdadGranjaCorral) = 0 then 0.0 else round(((case when AVG(AvesRendidas) = 0 then 0.0 else avg(KilosRendidos)/avg(AvesRendidas) end / (avg(EdadGranjaCorral)))*1000),2) end as GananciaDiaVenta
		,case when avg(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/avg(KilosRendidos)),3) end as ICA
		,MAX(STDICAC) STDICA
		,(case when avg(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/avg(KilosRendidos)),3) end) - MAX(STDICAC) as DifICA_STDICA
		,case when pk_division = 4 then ((8.5-avg(PesoProm))/12.22)+avg(ICA) else (case when pk_sexo = 1 then round((((2.500-avg(PesoProm))/3)+avg(ICA)),3) else round((((2.500-avg(PesoProm))/5.2)+avg(ICA)),3) end) end as ICAAjustado
		,case when avg(AreaGalpon) = 0 then 0.0 else round((avg(PobInicial) / avg(AreaGalpon)),2) end as AvesXm2
		,case when avg(AreaGalpon) = 0 then 0.0 else round((avg(KilosRendidos) / avg(AreaGalpon)),2) end as KgXm2
		,case when COALESCE(avg(EdadGranjaCorral),0) = 0 then 0.0 when avg(ica) = 0 then 0.0 else (((100-avg(PorMort))*(avg(PesoProm)))/(avg(EdadGranjaCorral)*avg(ica)))*100 end as IEP
		,case when FechaInicioGranja is null then 19
			  when DATEDIFF(FechaInicioGranja,FechaCrianza) between 50 and 100 then 19
			  when DATEDIFF(FechaInicioGranja,FechaCrianza) >=101 then 50
			  else DATEDIFF(FechaInicioGranja,FechaCrianza) end as DiasLimpieza
		,DATEDIFF(FechaCrianza,FechaFinSaca) as DiasCrianza
		,case when FechaInicioGranja is null then DATEDIFF(FechaCrianza,FechaFinSaca)
			  when DATEDIFF(FechaInicioGranja,FechaCrianza) between 50 and 100 then 19 + DATEDIFF(FechaCrianza,FechaFinSaca)
			  when DATEDIFF(FechaInicioGranja,FechaCrianza) >=101 then 50 + DATEDIFF(FechaCrianza,FechaFinSaca)
			  else DATEDIFF(FechaInicioGranja,FechaCrianza) + DATEDIFF(FechaCrianza,FechaFinSaca) end as TotalCampana
		,datediff(FechaInicioSaca,FechaFinSaca) as DiasSaca
		,min(EdadInicioSaca) as EdadInicioSaca
		,avg(CantInicioSaca) as CantInicioSaca
		,round(avg(EdadGranjaCorral),2) as EdadGranja
		,avg(round(Gas,2)) as Gas
		,avg(round(Cama,2)) as Cama
		,avg(round(Agua,2)) as Agua
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(CantMacho)/avg(PobInicial*1.0))*100,2) end as PorcMacho
		,avg(Pigmentacion) as Pigmentacion 
		,categoria
		,avg(PesoAlo) as PesoAlo
		,max(FlagAtipico) FlagAtipico
		,max(edadpadrecorral) EdadPadreCorral
        ,'' GasGalpon
		,max(PesoHvo) PesoHvo
		,avg(MortSem8) as MortSem8
		,avg(MortSem9) as MortSem9
		,avg(MortSem10) as MortSem10
		,avg(MortSem11) as MortSem11
		,avg(MortSem12) as MortSem12
		,avg(MortSem13) as MortSem13
		,avg(MortSem14) as MortSem14
		,avg(MortSem15) as MortSem15
		,avg(MortSem16) as MortSem16
		,avg(MortSem17) as MortSem17
		,avg(MortSem18) as MortSem18
		,avg(MortSem19) as MortSem19
		,avg(MortSem20) as MortSem20
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem8) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem8
		,MAX(STDPorcMortSem8) STDPorcMortSem8
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem8) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem8) DifPorcMortSem8_STDPorcMortSem8
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem9) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem9
		,MAX(STDPorcMortSem9) STDPorcMortSem9
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem9) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem9) DifPorcMortSem9_STDPorcMortSem9
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem10) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem10
		,MAX(STDPorcMortSem10) STDPorcMortSem10
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem10) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem10) DifPorcMortSem10_STDPorcMortSem10
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem11) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem11
		,MAX(STDPorcMortSem11) STDPorcMortSem11
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem11) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem11) DifPorcMortSem11_STDPorcMortSem11
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem12) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem12
		,MAX(STDPorcMortSem12) STDPorcMortSem12
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem12) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem12) DifPorcMortSem12_STDPorcMortSem12
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem13) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem13
		,MAX(STDPorcMortSem13) STDPorcMortSem13
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem13) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem13) DifPorcMortSem13_STDPorcMortSem13
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem14) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem14
		,MAX(STDPorcMortSem14) STDPorcMortSem14
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem14) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem14) DifPorcMortSem14_STDPorcMortSem14
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem15) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem15
		,MAX(STDPorcMortSem15) STDPorcMortSem15
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem15) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem15) DifPorcMortSem15_STDPorcMortSem15
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem16) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem16
		,MAX(STDPorcMortSem16) STDPorcMortSem16
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem16) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem16) DifPorcMortSem16_STDPorcMortSem16
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem17) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem17
		,MAX(STDPorcMortSem17) STDPorcMortSem17
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem17) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem17) DifPorcMortSem17_STDPorcMortSem17
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem18) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem18
		,MAX(STDPorcMortSem18) STDPorcMortSem18
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem18) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem18) DifPorcMortSem18_STDPorcMortSem18
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem19) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem19
		,MAX(STDPorcMortSem19) STDPorcMortSem19
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem19) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem19) DifPorcMortSem19_STDPorcMortSem19
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem20) / avg(PobInicial*1.0))*100),2) end AS PorcMortSem20
		,MAX(STDPorcMortSem20) STDPorcMortSem20
		,case when avg(PobInicial) = 0 then 0.0 else round(((avg(MortSem20) / avg(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem20) DifPorcMortSem20_STDPorcMortSem20
		,avg(MortSemAcum8) as MortSemAcum8
		,avg(MortSemAcum9) as MortSemAcum9
		,avg(MortSemAcum10) as MortSemAcum10
		,avg(MortSemAcum11) as MortSemAcum11
		,avg(MortSemAcum12) as MortSemAcum12
		,avg(MortSemAcum13) as MortSemAcum13
		,avg(MortSemAcum14) as MortSemAcum14
		,avg(MortSemAcum15) as MortSemAcum15
		,avg(MortSemAcum16) as MortSemAcum16
		,avg(MortSemAcum17) as MortSemAcum17
		,avg(MortSemAcum18) as MortSemAcum18
		,avg(MortSemAcum19) as MortSemAcum19
		,avg(MortSemAcum20) as MortSemAcum20
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum8)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum8
		,MAX(STDPorcMortSemAcum8) STDPorcMortSemAcum8
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum8)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum8) DifPorcMortSemAcum8_STDPorcMortSemAcum8
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum9)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum9
		,MAX(STDPorcMortSemAcum9) STDPorcMortSemAcum9
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum9)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum9) DifPorcMortSemAcum9_STDPorcMortSemAcum9
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum10)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum10
		,MAX(STDPorcMortSemAcum10) STDPorcMortSemAcum10
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum10)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum10) DifPorcMortSemAcum10_STDPorcMortSemAcum10
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum11)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum11
		,MAX(STDPorcMortSemAcum11) STDPorcMortSemAcum11
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum11)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum11) DifPorcMortSemAcum11_STDPorcMortSemAcum11
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum12)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum12
		,MAX(STDPorcMortSemAcum12) STDPorcMortSemAcum12
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum12)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum12) DifPorcMortSemAcum12_STDPorcMortSemAcum12
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum13)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum13
		,MAX(STDPorcMortSemAcum13) STDPorcMortSemAcum13
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum13)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum13) DifPorcMortSemAcum13_STDPorcMortSemAcum13
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum14)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum14
		,MAX(STDPorcMortSemAcum14) STDPorcMortSemAcum14
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum14)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum14) DifPorcMortSemAcum14_STDPorcMortSemAcum14
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum15)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum15
		,MAX(STDPorcMortSemAcum15) STDPorcMortSemAcum15
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum15)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum15) DifPorcMortSemAcum15_STDPorcMortSemAcum15
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum16)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum16
		,MAX(STDPorcMortSemAcum16) STDPorcMortSemAcum16
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum16)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum16) DifPorcMortSemAcum16_STDPorcMortSemAcum16
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum17)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum17
		,MAX(STDPorcMortSemAcum17) STDPorcMortSemAcum17
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum17)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum17) DifPorcMortSemAcum17_STDPorcMortSemAcum17
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum18)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum18
		,MAX(STDPorcMortSemAcum18) STDPorcMortSemAcum18
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum18)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum18) DifPorcMortSemAcum18_STDPorcMortSemAcum18
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum19)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum19
		,MAX(STDPorcMortSemAcum19) STDPorcMortSemAcum19
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum19)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum19) DifPorcMortSemAcum19_STDPorcMortSemAcum19
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum20)/ avg(PobInicial*1.0))*100,2) end AS PorcMortSemAcum20
		,MAX(STDPorcMortSemAcum20) STDPorcMortSemAcum20
		,case when avg(PobInicial) = 0 then 0.0 else round((avg(MortSemAcum20)/ avg(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum20) DifPorcMortSemAcum20_STDPorcMortSemAcum20
		,sum(PavoIni) as PavoIni
		,sum(Pavo1) as Pavo1
		,sum(Pavo2) as Pavo2
		,sum(Pavo3) as Pavo3
		,sum(Pavo4) as Pavo4
		,sum(Pavo5) as Pavo5
		,sum(Pavo6) as Pavo6
		,sum(Pavo7) as Pavo7
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(PavoIni)/(avg(AvesRendidas))),3) end PorcPavoIni
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo1)/(avg(AvesRendidas))),3) end PorcPavo1
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo2)/(avg(AvesRendidas))),3) end PorcPavo2
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo3)/(avg(AvesRendidas))),3) end PorcPavo3
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo4)/(avg(AvesRendidas))),3) end PorcPavo4
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo5)/(avg(AvesRendidas))),3) end PorcPavo5
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo6)/(avg(AvesRendidas))),3) end PorcPavo6
		,case when avg(AvesRendidas) = 0 then 0.0 else round((sum(Pavo7)/(avg(AvesRendidas))),3) end PorcPavo7
		,avg(PesoSem8) as PesoSem8
		,MAX(STDPesoSem8) STDPesoSem8
		,avg(PesoSem8) - MAX(STDPesoSem8) DifPesoSem8_STDPesoSem8
		,avg(PesoSem9) as PesoSem9
		,MAX(STDPesoSem9) STDPesoSem9
		,avg(PesoSem9) - MAX(STDPesoSem9) DifPesoSem9_STDPesoSem9
		,avg(PesoSem10) as PesoSem10
		,MAX(STDPesoSem10) STDPesoSem10
		,avg(PesoSem10) - MAX(STDPesoSem10) DifPesoSem10_STDPesoSem10
		,avg(PesoSem11) as PesoSem11
		,MAX(STDPesoSem11) STDPesoSem11
		,avg(PesoSem11) - MAX(STDPesoSem11) DifPesoSem11_STDPesoSem11
		,avg(PesoSem12) as PesoSem12
		,MAX(STDPesoSem12) STDPesoSem12
		,avg(PesoSem12) - MAX(STDPesoSem12) DifPesoSem12_STDPesoSem12
		,avg(PesoSem13) as PesoSem13
		,MAX(STDPesoSem13) STDPesoSem13
		,avg(PesoSem13) - MAX(STDPesoSem13) DifPesoSem13_STDPesoSem13
		,avg(PesoSem14) as PesoSem14
		,MAX(STDPesoSem14) STDPesoSem14
		,avg(PesoSem14) - MAX(STDPesoSem14) DifPesoSem14_STDPesoSem14
		,avg(PesoSem15) as PesoSem15
		,MAX(STDPesoSem15) STDPesoSem15
		,avg(PesoSem15) - MAX(STDPesoSem15) DifPesoSem15_STDPesoSem15
		,avg(PesoSem16) as PesoSem16
		,MAX(STDPesoSem16) STDPesoSem16
		,avg(PesoSem16) - MAX(STDPesoSem16) DifPesoSem16_STDPesoSem16
		,avg(PesoSem17) as PesoSem17
		,MAX(STDPesoSem17) STDPesoSem17
		,avg(PesoSem17) - MAX(STDPesoSem17) DifPesoSem17_STDPesoSem17
		,avg(PesoSem18) as PesoSem18
		,MAX(STDPesoSem18) STDPesoSem18
		,avg(PesoSem18) - MAX(STDPesoSem18) DifPesoSem18_STDPesoSem18
		,avg(PesoSem19) as PesoSem19
		,MAX(STDPesoSem19) STDPesoSem19
		,avg(PesoSem19) - MAX(STDPesoSem19) DifPesoSem19_STDPesoSem19
		,avg(PesoSem20) as PesoSem20
		,MAX(STDPesoSem20) STDPesoSem20
		,avg(PesoSem20) - MAX(STDPesoSem20) DifPesoSem20_STDPesoSem20
		,sum(PavosBBMortIncub) as PavosBBMortIncub
		,EdadPadreCorralDescrip 
		,MAX(DiasAloj) DiasAloj
		,'' FechaDesinfeccion
		,'' TipoCama
		,'' FormaReutilizacion
		,'' NReuso
		,'' PDE
		,'' PDT
		,'' PromAmoniaco
		,'' PromTemperatura        
		,MAX(U_RuidosTotales) / MAX(COALESCE(CountRuidosTotales*1.0,0)) RuidosRespiratorios
        ,'' nProductoMediano
        ,'' AvesRendidasMediano
        ,'' KilosRendidosMediano
        ,'' kilosRendidosPromMediano        
		,MAX(ConsSem1) ConsSem1
		,MAX(ConsSem2) ConsSem2
		,MAX(ConsSem3) ConsSem3
		,MAX(ConsSem4) ConsSem4
		,MAX(ConsSem5) ConsSem5
		,MAX(ConsSem6) ConsSem6
		,MAX(ConsSem7) ConsSem7
		,round((MAX(ConsSem1)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem1
		,round((MAX(ConsSem2)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem2
		,round((MAX(ConsSem3)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem3
		,round((MAX(ConsSem4)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem4
		,round((MAX(ConsSem5)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem5
		,round((MAX(ConsSem6)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem6
		,round((MAX(ConsSem7)/COALESCE((MAX(AvesRendidas)),0)),3) PorcConsSem7
		,ROUND((MAX(ConsSem1)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem1) DifPorcConsSem1_STDPorcConsSem1
		,ROUND((MAX(ConsSem2)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem2) DifPorcConsSem2_STDPorcConsSem2
		,ROUND((MAX(ConsSem3)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem3) DifPorcConsSem3_STDPorcConsSem3
		,ROUND((MAX(ConsSem4)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem4) DifPorcConsSem4_STDPorcConsSem4
		,ROUND((MAX(ConsSem5)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem5) DifPorcConsSem5_STDPorcConsSem5
		,ROUND((MAX(ConsSem6)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem6) DifPorcConsSem6_STDPorcConsSem6
		,ROUND((MAX(ConsSem7)/COALESCE((MAX(AvesRendidas)),0)),3) - MAX(STDPorcConsSem7) DifPorcConsSem7_STDPorcConsSem7
		,MAX(STDPorcConsSem1) STDPorcConsSem1
		,MAX(STDPorcConsSem2) STDPorcConsSem2
		,MAX(STDPorcConsSem3) STDPorcConsSem3
		,MAX(STDPorcConsSem4) STDPorcConsSem4
		,MAX(STDPorcConsSem5) STDPorcConsSem5
		,MAX(STDPorcConsSem6) STDPorcConsSem6
		,MAX(STDPorcConsSem7) STDPorcConsSem7
		,MAX(STDConsSem1) STDConsSem1
		,MAX(STDConsSem2) STDConsSem2
		,MAX(STDConsSem3) STDConsSem3
		,MAX(STDConsSem4) STDConsSem4
		,MAX(STDConsSem5) STDConsSem5
		,MAX(STDConsSem6) STDConsSem6
		,MAX(STDConsSem7) STDConsSem7
		,MAX(ConsSem1) - MAX(STDConsSem1) DifConsSem1_STDConsSem1
		,MAX(ConsSem2) - MAX(STDConsSem2) DifConsSem2_STDConsSem2
		,MAX(ConsSem3) - MAX(STDConsSem3) DifConsSem3_STDConsSem3
		,MAX(ConsSem4) - MAX(STDConsSem4) DifConsSem4_STDConsSem4
		,MAX(ConsSem5) - MAX(STDConsSem5) DifConsSem5_STDConsSem5
		,MAX(ConsSem6) - MAX(STDConsSem6) DifConsSem6_STDConsSem6
		,MAX(ConsSem7) - MAX(STDConsSem7) DifConsSem7_STDConsSem7
		,MAX(CantPrimLesion) CantPrimLesion
		,MAX(CantSegLesion) CantSegLesion
		,MAX(CantTerLesion) CantTerLesion
		,MAX(PorcPrimLesion) PorcPrimLesion
		,MAX(PorcSegLesion) PorcSegLesion
		,MAX(PorcTerLesion) PorcTerLesion
		,NomPrimLesion
		,NomSegLesion
		,NomTerLesion
		,MAX(DiasSacaEfectivo) DiasSacaEfectivo
		,MAX(U_PEAccidentados) U_PEAccidentados
		,MAX(U_PEHigadoGraso) U_PEHigadoGraso
		,MAX(U_PEHepatomegalia) U_PEHepatomegalia
		,MAX(U_PEHigadoHemorragico) U_PEHigadoHemorragico
		,MAX(U_PEInanicion) U_PEInanicion
		,MAX(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio
		,MAX(U_PESCH) U_PESCH
		,MAX(U_PEEnteritis) U_PEEnteritis
		,MAX(U_PEAscitis) U_PEAscitis
		,MAX(U_PEMuerteSubita) U_PEMuerteSubita
		,MAX(U_PEEstresPorCalor) U_PEEstresPorCalor
		,MAX(U_PEHidropericardio) U_PEHidropericardio
		,MAX(U_PEHemopericardio) U_PEHemopericardio
		,MAX(U_PEUratosis) U_PEUratosis
		,MAX(U_PEMaterialCaseoso) U_PEMaterialCaseoso
		,MAX(U_PEOnfalitis) U_PEOnfalitis
		,MAX(U_PERetencionDeYema) U_PERetencionDeYema
		,MAX(U_PEErosionDeMolleja) U_PEErosionDeMolleja
		,MAX(U_PEHemorragiaMusculos)U_PEHemorragiaMusculos
		,MAX(U_PESangreEnCiego) U_PESangreEnCiego
		,MAX(U_PEPericarditis) U_PEPericarditis
		,MAX(U_PEPeritonitis) U_PEPeritonitis
		,MAX(U_PEProlapso)U_PEProlapso
		,MAX(U_PEPicaje) U_PEPicaje
		,MAX(U_PERupturaAortica) U_PERupturaAortica
		,MAX(U_PEBazoMoteado) U_PEBazoMoteado
		,MAX(U_PENoViable) U_PENoViable
		,MAX(U_PEAerosaculitisG2) U_PEAerosaculitisG2
		,MAX(U_PECojera) U_PECojera
		,MAX(U_PEHigadoIcterico)U_PEHigadoIcterico
		,MAX(U_PEMaterialCaseoso_po1ra) U_PEMaterialCaseoso_po1ra
		,MAX(U_PEMaterialCaseosoMedRetr) U_PEMaterialCaseosoMedRetr
		,MAX(U_PENecrosisHepatica) U_PENecrosisHepatica
		,MAX(U_PENeumonia) U_PENeumonia
		,MAX(U_PESepticemia) U_PESepticemia
		,MAX(U_PEVomitoNegro) U_PEVomitoNegro
		,MAX(PorcAccidentados) PorcAccidentados
		,MAX(PorcHigadoGraso) PorcHigadoGraso
		,MAX(PorcHepatomegalia) PorcHepatomegalia
		,MAX(PorcHigadoHemorragico) PorcHigadoHemorragico
		,MAX(PorcInanicion) PorcInanicion
		,MAX(PorcProblemaRespiratorio) PorcProblemaRespiratorio
		,MAX(PorcSCH) PorcSCH
		,MAX(PorcEnteritis) PorcEnteritis
		,MAX(PorcAscitis) PorcAscitis
		,MAX(PorcMuerteSubita) PorcMuerteSubita
		,MAX(PorcEstresPorCalor) PorcEstresPorCalor
		,MAX(PorcHidropericardio) PorcHidropericardio
		,MAX(PorcHemopericardio) PorcHemopericardio
		,MAX(PorcUratosis) PorcUratosis
		,MAX(PorcMaterialCaseoso) PorcMaterialCaseoso
		,MAX(PorcOnfalitis) PorcOnfalitis
		,MAX(PorcRetencionDeYema) PorcRetencionDeYema
		,MAX(PorcErosionDeMolleja) PorcErosionDeMolleja
		,MAX(PorcHemorragiaMusculos) PorcHemorragiaMusculos
		,MAX(PorcSangreEnCiego) PorcSangreEnCiego
		,MAX(PorcPericarditis)PorcPericarditis
		,MAX(PorcPeritonitis) PorcPeritonitis
		,MAX(PorcProlapso) PorcProlapso
		,MAX(PorcPicaje) PorcPicaje
		,MAX(PorcRupturaAortica) PorcRupturaAortica
		,MAX(PorcBazoMoteado) PorcBazoMoteado
		,MAX(PorcNoViable) PorcNoViable
		,MAX(PorcAerosaculitisG2) PorcAerosaculitisG2
		,MAX(PorcCojera) PorcCojera
		,MAX(PorcHigadoIcterico) PorcHigadoIcterico
		,MAX(PorcMaterialCaseoso_po1ra) PorcMaterialCaseoso_po1ra
		,MAX(PorcMaterialCaseosoMedRetr) PorcMaterialCaseosoMedRetr
		,MAX(PorcNecrosisHepatica) PorcNecrosisHepatica
		,MAX(PorcNeumonia) PorcNeumonia
		,MAX(PorcSepticemia) PorcSepticemia
		,MAX(PorcVomitoNegro) PorcVomitoNegro
		,MAX(STDPorcConsGasInviernoC) STDPorcConsGasInvierno
		,MAX(STDPorcConsGasVeranoC) STDPorcConsGasVerano
        ,'' PorcPolloMediano
		,MedSem1
		,MedSem2
		,MedSem3
		,MedSem4
		,MedSem5
		,MedSem6
		,MedSem7
		,MAX(DiasMedSem1) DiasMedSem1
		,MAX(DiasMedSem2) DiasMedSem2
		,MAX(DiasMedSem3) DiasMedSem3
		,MAX(DiasMedSem4) DiasMedSem4
		,MAX(DiasMedSem5) DiasMedSem5
		,MAX(DiasMedSem6) DiasMedSem6
		,MAX(DiasMedSem7) DiasMedSem7
        ,max(a.fecha) EventDate
		,MAX(PorcAlojamientoXEdadPadre) PorcAlojamientoXEdadPadre
		,A.TipoOrigen
		,avg(Peso5Dias) as Peso5Dias
		,MAX(STDPeso5Dias) STDPeso5Dias
		,avg(Peso5Dias) - MAX(STDPeso5Dias) DifPeso5Dias_STDPeso5Dias
		,MAX(NoViableSem1) NoViableSem1
		,MAX(NoViableSem2) NoViableSem2
		,MAX(NoViableSem3) NoViableSem3
		,MAX(NoViableSem4) NoViableSem4
		,MAX(NoViableSem5) NoViableSem5
		,MAX(NoViableSem6) NoViableSem6
		,MAX(NoViableSem7) NoViableSem7
		,MAX(PorcNoViableSem1) PorcNoViableSem1
		,MAX(PorcNoViableSem2) PorcNoViableSem2
		,MAX(PorcNoViableSem3) PorcNoViableSem3
		,MAX(PorcNoViableSem4) PorcNoViableSem4
		,MAX(PorcNoViableSem5) PorcNoViableSem5
		,MAX(PorcNoViableSem6) PorcNoViableSem6
		,MAX(PorcNoViableSem7) PorcNoViableSem7 
		,MAX(NoViableSem8) NoViableSem8
		,MAX(PorcNoViableSem8) PorcNoViableSem8
		,pk_tipogranja
        ,max(to_date(a.FechaCierre, 'yyyyMMdd')) DescripFecha
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
        ,'' DescripDiaVida
        ,'' DescripTipoGranja
        ,'' U_PECaja
        ,'' U_PEGota
        ,'' U_PEIntoxicacion
        ,'' U_PERetrazos
        ,'' U_PEEliminados
        ,'' U_PEAhogados
        ,'' U_PEEColi
        ,'' U_PEOtros
        ,'' U_PECoccidia
        ,'' U_PEDeshidratados
        ,'' U_PEHepatitis
        ,'' U_PETraumatismo
from {database_name_tmp}.ProduccionDetalleCorral A
left join {database_name_gl}.lk_tiempo B on to_date(a.FechaCierre, 'yyyyMMdd') = b.fecha
--where date_format(CAST(a.fecha AS timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
group by pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_tipoproducto,
pk_estado,pk_especie,A.ComplexEntityNo,FechaNacimiento,FechaAlojamiento,FechaCrianza,FechaInicioGranja,FechaInicioSaca,FechaFinSaca,PadreMayor,RazaMayor,
IncubadoraMayor,categoria,EdadPadreCorralDescrip,NomPrimLesion,NomSegLesion,NomTerLesion,MedSem1,MedSem2,MedSem3,MedSem4,MedSem5,MedSem6,MedSem7,A.TipoOrigen
,pk_tipogranja
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_CorralT"
}
df_ft_consolidado_CorralT.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_consolidado_CorralT")
print('carga ft_consolidado_CorralT', df_ft_consolidado_CorralT.count())
df_ft_consolidado_Corral = spark.sql(f"""SELECT * from {database_name_tmp}.ft_consolidado_CorralT
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_consolidado_Corral', df_ft_consolidado_Corral.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target2 = f"{bucket_name_prdmtech}ft_consolidado_corral/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consolidado_corral cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consolidado_corral")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_consolidado_Corral.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_corralTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consolidado_corralTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_corralTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_corral"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_corral")
            
    print(f"agrega registros nuevos a la tabla ft_consolidado_corral : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consolidado_corral : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_corralTemporal')
    print(f"Tabla ft_consolidado_corralTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_corral"
    }
    df_ft_consolidado_Corral.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_corral")
#Se muestra UPD ft_consolidado_Galpon
df_ft_consolidado_GalponT = spark.sql(f"""
select 
 max(D.pk_tiempo) as pk_tiempo
,a.pk_empresa
,pk_division
,pk_zona
,pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_galpon
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,max(pk_proveedor) pk_proveedor
,max(pk_diasvida) as pk_diasvida
,concat(clote , '-' , nogalpon) as ComplexEntityNo
,'' as fechanacimiento
,max(fechacierre) as FechaCierre
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,min(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(PobInicial) as PobInicial
,sum(PobInicial) - sum(MortDia) as AvesLogradas
,sum(AvesRendidas) as AvesRendidas
,sum(AvesRendidas) - (sum(PobInicial) - sum(MortDia)) as SobranFaltan
,round((sum(KilosRendidos)),2) as KilosRendidos
,sum(MortDia) as MortDia
,case when sum(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(PobInicial*1.0))*100)),2) end as PorMort
,MAX(STDMortAcumG) STDPorMort
,case when sum(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(PobInicial*1.0))*100)),2) end - MAX(STDMortAcumG) DifPorMort_STDPorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem1
,MAX(STDPorcMortSem1G) STDPorcMortSem1
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem1G) DifPorcMortSem1_STDPorcMortSem1
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem2
,MAX(STDPorcMortSem2G) STDPorcMortSem2
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem2G) DifPorcMortSem2_STDPorcMortSem2
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem3
,MAX(STDPorcMortSem3G) STDPorcMortSem3
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem3G) DifPorcMortSem3_STDPorcMortSem3
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem4
,MAX(STDPorcMortSem4G) STDPorcMortSem4
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem4G) DifPorcMortSem4_STDPorcMortSem4
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem5
,MAX(STDPorcMortSem5G) STDPorcMortSem5
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem5G) DifPorcMortSem5_STDPorcMortSem5
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem6
,MAX(STDPorcMortSem6G) STDPorcMortSem6
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem6G) DifPorcMortSem6_STDPorcMortSem6
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem7
,MAX(STDPorcMortSem7G) STDPorcMortSem7
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem7G) DifPorcMortSem7_STDPorcMortSem7
,sum(MortSemAcum1) as MortSemAcum1
,sum(MortSemAcum2) as MortSemAcum2
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4
,sum(MortSemAcum5) as MortSemAcum5
,sum(MortSemAcum6) as MortSemAcum6
,sum(MortSemAcum7) as MortSemAcum7
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum1
,MAX(STDPorcMortSemAcum1G) STDPorcMortSemAcum1
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum1G) DifPorcMortSemAcum1_STDPorcMortSemAcum1
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum2
,MAX(STDPorcMortSemAcum2G) STDPorcMortSemAcum2
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum2G) DifPorcMortSemAcum2_STDPorcMortSemAcum2
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum3
,MAX(STDPorcMortSemAcum3G) STDPorcMortSemAcum3
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum3G) DifPorcMortSemAcum3_STDPorcMortSemAcum3
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum4
,MAX(STDPorcMortSemAcum4G) STDPorcMortSemAcum4
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum4G) DifPorcMortSemAcum4_STDPorcMortSemAcum4
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum5
,MAX(STDPorcMortSemAcum5G) STDPorcMortSemAcum5
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum5G) DifPorcMortSemAcum5_STDPorcMortSemAcum5
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum6
,MAX(STDPorcMortSemAcum6G) STDPorcMortSemAcum6
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum6G) DifPorcMortSemAcum6_STDPorcMortSemAcum6
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum7
,MAX(STDPorcMortSemAcum7G) STDPorcMortSemAcum7
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum7G) DifPorcMortSemAcum7_STDPorcMortSemAcum7
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(ConsDia) as ConsDia
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PreInicio)/(sum(AvesRendidas))),3) end PorcPreIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Inicio)/(sum(AvesRendidas))),3) end PorcIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Acabado)/(sum(AvesRendidas))),3) end PorcAcab
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Terminado)/(sum(AvesRendidas))),3) end PorcTerm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Finalizador)/(sum(AvesRendidas))),3) end PorcFin
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),3) end PorcConsumo
,MAX(STDConsAcumG) STDPorcConsumo
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),3) end - MAX(STDConsAcumG) DifPorcConsumo_STDPorcConsumo
,sum(UnidSeleccion) as Seleccion
,case when sum(PobInicial) = 0 then 0.0 else round((sum(UnidSeleccion)/sum(PobInicial*1.0))*100,2) end as PorcSeleccion
,sum(PesoSem1) as PesoSem1
,MAX(STDPorcPesoSem1G) STDPesoSem1
,sum(PesoSem1) - MAX(STDPorcPesoSem1G) DifPesoSem1_STDPesoSem1
,sum(PesoSem2) as PesoSem2
,MAX(STDPorcPesoSem2G) STDPesoSem2
,sum(PesoSem2) - MAX(STDPorcPesoSem2G) DifPesoSem2_STDPesoSem2
,sum(PesoSem3) as PesoSem3
,MAX(STDPorcPesoSem3G) STDPesoSem3
,sum(PesoSem3) - MAX(STDPorcPesoSem3G) DifPesoSem3_STDPesoSem3
,sum(PesoSem4) as PesoSem4
,MAX(STDPorcPesoSem4G) STDPesoSem4
,sum(PesoSem4) - MAX(STDPorcPesoSem4G) DifPesoSem4_STDPesoSem4
,sum(PesoSem5) as PesoSem5
,MAX(STDPorcPesoSem5G) STDPesoSem5
,sum(PesoSem5) - MAX(STDPorcPesoSem5G) DifPesoSem5_STDPesoSem5
,sum(PesoSem6) as PesoSem6
,MAX(STDPorcPesoSem6G) STDPesoSem6
,sum(PesoSem6) - MAX(STDPorcPesoSem6G) DifPesoSem6_STDPesoSem6
,sum(PesoSem7) as PesoSem7
,MAX(STDPorcPesoSem7G) STDPesoSem7
,sum(PesoSem7) - MAX(STDPorcPesoSem7G) DifPesoSem7_STDPesoSem7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),3) end as PesoProm
,MAX(STDPesoG) STDPesoProm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),3) end - MAX(STDPesoG) DifPesoProm_STDPesoProm
,case when avg(EdadGranjaGalpon) = 0 then 0.0 else round(((case when AVG(AvesRendidas) = 0 then 0.0 else avg(KilosRendidos)/avg(AvesRendidas) end / (avg(EdadGranjaGalpon)))*1000),2) end as GananciaDiaVenta
,case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),3) end as ICA
,MAX(STDICAG) STDICA
,(case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),3) end) - MAX(STDICAG) DifICA_STDICA
,case when pk_division = 4 then ((8.5-avg(PesoProm))/12.22)+avg(ICA)	else (round((((2.500-avg(PesoProm))/3.8)+avg(ICA)),3)) end as ICAAjustado
,case when avg(AreaGalpon) = 0 then 0.0 else round((sum(PobInicial) / avg(AreaGalpon)),2) end as AvesXm2
,case when avg(AreaGalpon) = 0 then 0.0 else round((sum(KilosRendidos) / avg(AreaGalpon)),2) end as KgXm2
,case when COALESCE(avg(EdadGranjaGalpon),0) = 0 then 0.0 when avg(ica) = 0 then 0.0 else (((100-avg(PorMort))*(avg(PesoProm)))/(avg(EdadGranjaGalpon)*avg(ica)))*100 end as IEP
,case when min(FechaInicioGranja) is null then 19
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) between 50 and 100 then 19
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) >= 101 then 50
	  else DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) end as DiasLimpieza
,DATEDIFF(min(FechaCrianza),max(FechaFinSaca)) as DiasCrianza
,case when min(FechaInicioGranja) is null then DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) between 50 and 100 then 19 + DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) >= 101 then 50 + DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
	  else DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) + DATEDIFF(min(FechaCrianza),max(FechaFinSaca)) end as TotalCampana
,datediff(min(FechaInicioSaca),max(FechaFinSaca)) as DiasSaca
,min(EdadInicioSaca) as EdadInicioSaca
,sum(CantInicioSaca) as CantInicioSaca
,round(avg(EdadGranjaGalpon),2) as EdadGranja
,avg(round(Gas,2)) as Gas
,avg(round(Cama,2)) as Cama
,avg(round(Agua,2)) as Agua
,case when sum(PobInicial) = 0 then 0.0 else round((sum(CantMacho) /sum(PobInicial*1.0))*100,2) end as PorcMacho
,avg(Pigmentacion) as Pigmentacion
,date_format(max(a.fecha),'yyyyMMdd') as EventDate
,categoria
,max(FlagAtipico) FlagAtipico
,max(EdadPadreGalpon) EdadPadreGalpon
,sum(MortSem8) as MortSem8
,sum(MortSem9) as MortSem9
,sum(MortSem10) as MortSem10
,sum(MortSem11) as MortSem11
,sum(MortSem12) as MortSem12
,sum(MortSem13) as MortSem13
,sum(MortSem14) as MortSem14
,sum(MortSem15) as MortSem15
,sum(MortSem16) as MortSem16
,sum(MortSem17) as MortSem17
,sum(MortSem18) as MortSem18
,sum(MortSem19) as MortSem19
,sum(MortSem20) as MortSem20
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem8
,MAX(STDPorcMortSem8G) STDPorcMortSem8
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem8G) DifPorcMortSem8_STDPorcMortSem8
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem9
,MAX(STDPorcMortSem9G) STDPorcMortSem9
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem9G) DifPorcMortSem9_STDPorcMortSem9
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem10
,MAX(STDPorcMortSem10G) STDPorcMortSem10
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem10G) DifPorcMortSem10_STDPorcMortSem10
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem11
,MAX(STDPorcMortSem11G) STDPorcMortSem11
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem11G) DifPorcMortSem11_STDPorcMortSem11
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem12
,MAX(STDPorcMortSem12G) STDPorcMortSem12
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem12G) DifPorcMortSem12_STDPorcMortSem12
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem13
,MAX(STDPorcMortSem13G) STDPorcMortSem13
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem13G) DifPorcMortSem13_STDPorcMortSem13
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem14
,MAX(STDPorcMortSem14G) STDPorcMortSem14
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem14G) DifPorcMortSem14_STDPorcMortSem14
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem15
,MAX(STDPorcMortSem15G) STDPorcMortSem15
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem15G) DifPorcMortSem15_STDPorcMortSem15
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem16
,MAX(STDPorcMortSem16G) STDPorcMortSem16
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem16G) DifPorcMortSem16_STDPorcMortSem16
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem17
,MAX(STDPorcMortSem17G) STDPorcMortSem17
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem17G) DifPorcMortSem17_STDPorcMortSem17
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem18
,MAX(STDPorcMortSem18G) STDPorcMortSem18
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem18G) DifPorcMortSem18_STDPorcMortSem18
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem19
,MAX(STDPorcMortSem19G) STDPorcMortSem19
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem19G) DifPorcMortSem19_STDPorcMortSem19
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(PobInicial*1.0))*100),2) end AS PorcMortSem20
,MAX(STDPorcMortSem20G) STDPorcMortSem20
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(PobInicial*1.0))*100),2) end - MAX(STDPorcMortSem20G) DifPorcMortSem20_STDPorcMortSem20
,sum(MortSemAcum8) as MortSemAcum8
,sum(MortSemAcum9) as MortSemAcum9
,sum(MortSemAcum10) as MortSemAcum10
,sum(MortSemAcum11) as MortSemAcum11
,sum(MortSemAcum12) as MortSemAcum12
,sum(MortSemAcum13) as MortSemAcum13
,sum(MortSemAcum14) as MortSemAcum14
,sum(MortSemAcum15) as MortSemAcum15
,sum(MortSemAcum16) as MortSemAcum16
,sum(MortSemAcum17) as MortSemAcum17
,sum(MortSemAcum18) as MortSemAcum18
,sum(MortSemAcum19) as MortSemAcum19
,sum(MortSemAcum20) as MortSemAcum20
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum8
,MAX(STDPorcMortSemAcum8G) STDPorcMortSemAcum8
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum8G) DifPorcMortSemAcum8_STDPorcMortSemAcum8
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum9
,MAX(STDPorcMortSemAcum9G) STDPorcMortSemAcum9
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum9G) DifPorcMortSemAcum9_STDPorcMortSemAcum9
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum10
,MAX(STDPorcMortSemAcum10G) STDPorcMortSemAcum10
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum10G) DifPorcMortSemAcum10_STDPorcMortSemAcum10
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum11
,MAX(STDPorcMortSemAcum11G) STDPorcMortSemAcum11
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum11G) DifPorcMortSemAcum11_STDPorcMortSemAcum11
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum12
,MAX(STDPorcMortSemAcum12G) STDPorcMortSemAcum12
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum12G) DifPorcMortSemAcum12_STDPorcMortSemAcum12
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum13
,MAX(STDPorcMortSemAcum13G) STDPorcMortSemAcum13
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum13G) DifPorcMortSemAcum13_STDPorcMortSemAcum13
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum14
,MAX(STDPorcMortSemAcum14G) STDPorcMortSemAcum14
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum14G) DifPorcMortSemAcum14_STDPorcMortSemAcum14
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum15
,MAX(STDPorcMortSemAcum15G) STDPorcMortSemAcum15
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum15G) DifPorcMortSemAcum15_STDPorcMortSemAcum15
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum16
,MAX(STDPorcMortSemAcum16G) STDPorcMortSemAcum16
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum16G) DifPorcMortSemAcum16_STDPorcMortSemAcum16
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum17
,MAX(STDPorcMortSemAcum17G) STDPorcMortSemAcum17
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum17G) DifPorcMortSemAcum17_STDPorcMortSemAcum17
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum18
,MAX(STDPorcMortSemAcum18G) STDPorcMortSemAcum18
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum18G) DifPorcMortSemAcum18_STDPorcMortSemAcum18
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum19
,MAX(STDPorcMortSemAcum19G) STDPorcMortSemAcum19
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum19G) DifPorcMortSemAcum19_STDPorcMortSemAcum19
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20) / sum(PobInicial*1.0))*100,2) end AS PorcMortSemAcum20
,MAX(STDPorcMortSemAcum20G) STDPorcMortSemAcum20
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20)/ sum(PobInicial*1.0))*100,2) end - MAX(STDPorcMortSemAcum20G) DifPorcMortSemAcum20_STDPorcMortSemAcum20
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PavoIni)/(sum(AvesRendidas))),3) end PorcPavoIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo1)/(sum(AvesRendidas))),3) end PorcPavo1
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo2)/(sum(AvesRendidas))),3) end PorcPavo2
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo3)/(sum(AvesRendidas))),3) end PorcPavo3
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo4)/(sum(AvesRendidas))),3) end PorcPavo4
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo5)/(sum(AvesRendidas))),3) end PorcPavo5
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo6)/(sum(AvesRendidas))),3) end PorcPavo6
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo7)/(sum(AvesRendidas))),3) end PorcPavo7
,sum(PesoSem8) as PesoSem8
,MAX(STDPorcPesoSem8G) STDPesoSem8
,sum(PesoSem8) - MAX(STDPorcPesoSem8G) DifPesoSem8_STDPesoSem8
,sum(PesoSem9) as PesoSem9
,MAX(STDPorcPesoSem9G) STDPesoSem9
,sum(PesoSem9) - MAX(STDPorcPesoSem9G) DifPesoSem9_STDPesoSem9
,sum(PesoSem10) as PesoSem10
,MAX(STDPorcPesoSem10G) STDPesoSem10
,sum(PesoSem10) - MAX(STDPorcPesoSem10G) DifPesoSem10_STDPesoSem10
,sum(PesoSem11) as PesoSem11
,MAX(STDPorcPesoSem11G) STDPesoSem11
,sum(PesoSem11) - MAX(STDPorcPesoSem11G) DifPesoSem11_STDPesoSem11
,sum(PesoSem12) as PesoSem12
,MAX(STDPorcPesoSem12G) STDPesoSem12
,sum(PesoSem12) - MAX(STDPorcPesoSem12G) DifPesoSem12_STDPesoSem12
,sum(PesoSem13) as PesoSem13
,MAX(STDPorcPesoSem13G) STDPesoSem13
,sum(PesoSem13) - MAX(STDPorcPesoSem13G) DifPesoSem13_STDPesoSem13
,sum(PesoSem14) as PesoSem14
,MAX(STDPorcPesoSem14G) STDPesoSem14
,sum(PesoSem14) - MAX(STDPorcPesoSem14G) DifPesoSem14_STDPesoSem14
,sum(PesoSem15) as PesoSem15
,MAX(STDPorcPesoSem15G) STDPesoSem15
,sum(PesoSem15) - MAX(STDPorcPesoSem15G) DifPesoSem15_STDPesoSem15
,sum(PesoSem16) as PesoSem16
,MAX(STDPorcPesoSem16G) STDPesoSem16
,sum(PesoSem16) - MAX(STDPorcPesoSem16G) DifPesoSem16_STDPesoSem16
,sum(PesoSem17) as PesoSem17
,MAX(STDPorcPesoSem17G) STDPesoSem17
,sum(PesoSem17) - MAX(STDPorcPesoSem17G) DifPesoSem17_STDPesoSem17
,sum(PesoSem18) as PesoSem18
,MAX(STDPorcPesoSem18G) STDPesoSem18
,sum(PesoSem18) - MAX(STDPorcPesoSem18G) DifPesoSem18_STDPesoSem18
,sum(PesoSem19) as PesoSem19
,MAX(STDPorcPesoSem19G) STDPesoSem19
,sum(PesoSem19) - MAX(STDPorcPesoSem19G) DifPesoSem19_STDPesoSem19
,sum(PesoSem20) as PesoSem20
,MAX(STDPorcPesoSem20G) STDPesoSem20
,sum(PesoSem20) - MAX(STDPorcPesoSem20G) DifPesoSem20_STDPesoSem20
,sum(PavosBBMortIncub) as PavosBBMortIncub
,MAX(STDPorcConsGasInviernoG) STDPorcConsGasInvierno
,MAX(STDPorcConsGasVeranoG) STDPorcConsGasVerano
,pk_tipogranja
,max(to_date(a.FechaCierre, 'yyyyMMdd')) DescripFecha
,'' descripempresa      
,'' descripdivision     
,'' descripzona         
,'' descripsubzona      
,'' plantel             
,'' lote                
,'' galpon              
,'' descriptipoproducto 
,'' descripespecie      
,'' descripestado       
,'' descripadministrador
,'' descripproveedor    
,'' descripdiavida      
,'' descriptipogranja   
from {database_name_tmp}.ProduccionDetalleGalpon A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name_gl}.lk_galpon C on A.pk_galpon = C.pk_galpon
left join {database_name_gl}.lk_tiempo D on to_date(a.FechaCierre, 'yyyyMMdd') = D.fecha
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,A.pk_plantel,A.pk_lote,A.pk_galpon,pk_tipoproducto,pk_estado,pk_especie,nogalpon,
clote,categoria,pk_tipogranja
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_GalponT"
}
df_ft_consolidado_GalponT.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_consolidado_GalponT")
print('carga ft_consolidado_GalponT', df_ft_consolidado_GalponT.count())
df_ft_consolidado_Galpon = spark.sql(f"""SELECT * from {database_name_tmp}.ft_consolidado_GalponT
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga df_ft_consolidado_Galpon', df_ft_consolidado_Galpon.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target2 = f"{bucket_name_prdmtech}ft_consolidado_galpon/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consolidado_galpon cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consolidado_galpon")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_consolidado_Galpon.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_galponTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consolidado_galponTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_galponTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_galpon"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_galpon")
            
    print(f"agrega registros nuevos a la tabla ft_consolidado_galpon : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consolidado_galpon : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_galponTemporal')
    print(f"Tabla ft_consolidado_galponTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_galpon"
    }
    df_ft_consolidado_Galpon.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_galpon")
df_ft_consolidado_LoteT = spark.sql(f"""
select 
 max(D.pk_tiempo) as pk_tiempo
,max(pk_mes) as pk_mes
,a.pk_empresa
,pk_division
,pk_zona
,pk_subzona
,A.pk_plantel
,A.pk_lote
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,max(pk_proveedor) pk_proveedor
,case when COALESCE(pk_diasvida,0) = 0 then 120 else pk_diasvida end pk_diasvida
,clote as ComplexEntityNo
,max(fechacierre) as FechaCierre
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,min(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(PobInicial) as PobInicial
,sum(PobInicial) - sum(MortDia) as AvesLogradas
,sum(AvesRendidas) as AvesRendidas
,sum(AvesRendidas) - (sum(PobInicial) - sum(MortDia)) as SobranFaltan
,round((sum(KilosRendidos)),14) as KilosRendidos
,sum(MortDia) as MortDia
,case when sum(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(PobInicial*1.0))*100)),2) end as PorMort
,MAX(STDMortAcumL) STDPorMort
,case when sum(PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(PobInicial*1.0))*100)),2) end - MAX(STDMortAcumL) DifPorMort_STDPorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem1
,MAX(STDPorcMortSem1L) STDPorcMortSem1
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem1L) DifPorcMortSem1_STDPorcMortSem1
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem2
,MAX(STDPorcMortSem2L) STDPorcMortSem2
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem2L) DifPorcMortSem2_STDPorcMortSem2
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem3
,MAX(STDPorcMortSem3L) STDPorcMortSem3
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem3L) DifPorcMortSem3_STDPorcMortSem3
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem4
,MAX(STDPorcMortSem4L) STDPorcMortSem4
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem4L) DifPorcMortSem4_STDPorcMortSem4
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem5
,MAX(STDPorcMortSem5L) STDPorcMortSem5
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem5L) DifPorcMortSem5_STDPorcMortSem5
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem6
,MAX(STDPorcMortSem6L) STDPorcMortSem6
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem6L) DifPorcMortSem6_STDPorcMortSem6
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem7
,MAX(STDPorcMortSem7L) STDPorcMortSem7
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem7L) DifPorcMortSem7_STDPorcMortSem7
,sum(MortSemAcum1) as MortSemAcum1
,sum(MortSemAcum2) as MortSemAcum2
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4
,sum(MortSemAcum5) as MortSemAcum5
,sum(MortSemAcum6) as MortSemAcum6
,sum(MortSemAcum7) as MortSemAcum7
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum1
,MAX(STDPorcMortSemAcum1L) STDPorcMortSemAcum1
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum1L) DifPorcMortSemAcum1_STDPorcMortSemAcum1
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum2
,MAX(STDPorcMortSemAcum2L) STDPorcMortSemAcum2
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum2L) DifPorcMortSemAcum2_STDPorcMortSemAcum2
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum3
,MAX(STDPorcMortSemAcum3L) STDPorcMortSemAcum3
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum3L) DifPorcMortSemAcum3_STDPorcMortSemAcum3
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum4
,MAX(STDPorcMortSemAcum4L) STDPorcMortSemAcum4
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum4L) DifPorcMortSemAcum4_STDPorcMortSemAcum4
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum5
,MAX(STDPorcMortSemAcum5L) STDPorcMortSemAcum5
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum5L) DifPorcMortSemAcum5_STDPorcMortSemAcum5
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum6
,MAX(STDPorcMortSemAcum6L) STDPorcMortSemAcum6
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum6L) DifPorcMortSemAcum6_STDPorcMortSemAcum6
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum7
,MAX(STDPorcMortSemAcum7L) STDPorcMortSemAcum7
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum7L) DifPorcMortSemAcum7_STDPorcMortSemAcum7
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(ConsDia) as ConsDia
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PreInicio)/(sum(AvesRendidas))),14) end PorcPreIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Inicio)/(sum(AvesRendidas))),14) end PorcIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Acabado)/(sum(AvesRendidas))),14) end PorcAcab
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Terminado)/(sum(AvesRendidas))),14) end PorcTerm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Finalizador)/(sum(AvesRendidas))),14) end PorcFin
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),14) end PorcConsumo
,MAX(STDConsAcumL) STDPorcConsumo
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),14) end - MAX(STDConsAcumL) DifPorcConsumo_STDPorcConsumo
,sum(UnidSeleccion) as Seleccion
,case when sum(PobInicial) = 0 then 0.0 else round((sum(UnidSeleccion)/sum(PobInicial*1.0))*100,14) end as PorcSeleccion
,sum(PesoSem1) as PesoSem1
,MAX(STDPorcPesoSem1L) STDPesoSem1
,sum(PesoSem1) - MAX(STDPorcPesoSem1L) DifPesoSem1_STDPesoSem1
,sum(PesoSem2) as PesoSem2
,MAX(STDPorcPesoSem2L) STDPesoSem2
,sum(PesoSem2) - MAX(STDPorcPesoSem2L) DifPesoSem2_STDPesoSem2
,sum(PesoSem3) as PesoSem3
,MAX(STDPorcPesoSem3L) STDPesoSem3
,sum(PesoSem3) - MAX(STDPorcPesoSem3L) DifPesoSem3_STDPesoSem3
,sum(PesoSem4) as PesoSem4
,MAX(STDPorcPesoSem4L) STDPesoSem4
,sum(PesoSem4) - MAX(STDPorcPesoSem4L) DifPesoSem4_STDPesoSem4
,sum(PesoSem5) as PesoSem5
,MAX(STDPorcPesoSem5L) STDPesoSem5
,sum(PesoSem5) - MAX(STDPorcPesoSem5L) DifPesoSem5_STDPesoSem5
,sum(PesoSem6) as PesoSem6
,MAX(STDPorcPesoSem6L) STDPesoSem6
,sum(PesoSem6) - MAX(STDPorcPesoSem6L) DifPesoSem6_STDPesoSem6
,sum(PesoSem7) as PesoSem7
,MAX(STDPorcPesoSem7L) STDPesoSem7
,sum(PesoSem7) - MAX(STDPorcPesoSem7L) DifPesoSem7_STDPesoSem7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),14) end as PesoProm
,MAX(STDPesoL) STDPesoProm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),14) end - MAX(STDPesoL) DifPesoProm_STDPesoProm
,case when avg(EdadGranjaLote) = 0 then 0.0 else round(((case when AVG(AvesRendidas) = 0 then 0.0 else avg(KilosRendidos)/avg(AvesRendidas) end / (avg(EdadGranjaLote)))*1000),2) end as GananciaDiaVenta
,CASE WHEN SUM(KilosRendidos) = 0 THEN 0.0 ELSE  floor((cast(SUM(ConsDia) as decimal(30,10))/cast(SUM(KilosRendidos) as decimal(30,10))) * 1000000) / 1000000 END AS ICA
,MAX(STDICAL) STDICA
,(case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),14) end) - MAX(STDICAL) DifICA_STDICA
,case when pk_division = 4 then ((8.5-avg(PesoProm))/12.22)+avg(ICA) else round((((2.500-avg(PesoProm))/3.8)+avg(ICA)),3) end as ICAAjustado
,case when avg(AreaGalpon) = 0 then 0.0 else round((sum(PobInicial) / sum(AreaGalpon)),14) end as AvesXm2
,case when avg(AreaGalpon) = 0 then 0.0 else round((sum(KilosRendidos) / sum(AreaGalpon)),14) end as KgXm2
,case when COALESCE(avg(EdadGranjaLote),0) = 0 then 0.0 when avg(ica) = 0 then 0.0 else (((100-avg(PorMort))*(avg(PesoProm)))/(avg(EdadGranjaLote)*avg(ica)))*100 end as IEP
,case when min(FechaInicioGranja) is null then 19
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) between 50 and 100 then 19
	  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) >=101 then 50
	  else DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) end as DiasLimpieza
,DATEDIFF(min(FechaCrianza),max(FechaFinSaca)) as DiasCrianza
,case when min(FechaInicioGranja) is null then DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
			  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) between 50 and 100 then 19 + DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
			  when DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) >=101 then 50 + DATEDIFF(min(FechaCrianza),max(FechaFinSaca))
			  else DATEDIFF(min(FechaInicioGranja),min(FechaCrianza)) + DATEDIFF(min(FechaCrianza),max(FechaFinSaca)) end as TotalCampana
,case when DATEDIFF(min(FechaInicioSaca),max(FechaFinSaca))>=1000 then 0 else DATEDIFF(min(FechaInicioSaca),max(FechaFinSaca)) end as DiasSaca
,min(EdadInicioSaca) as EdadInicioSaca
,sum(CantInicioSaca) as CantInicioSaca
,round(avg(EdadGranjaLote),14) as EdadGranja
,avg(round(Gas,2)) as Gas
,avg(round(Cama,2)) as Cama
,avg(round(Agua,2)) as Agua
,case when sum(PobInicial) = 0 then 0 else round((sum(CantMacho) /sum(PobInicial*1.0))*100,2) end as PorcMacho
,avg(Pigmentacion) as Pigmentacion
,date_format(max(a.fecha),'yyyyMMdd') as EventDate
,categoria
,FlagAtipico
,FlagArtAtipico
,max(EdadPadreLote) EdadPadreLote
,round(max(PorcPolloJoven),3) PorcPolloJoven
,round(max(PorcPesoAloMenor),3) PorcPesoAloMenor
,round(max(COALESCE(PorcGalponesMixtos,0)),3) PorcGalponesMixtos
,max(PesoAlo) PesoAlo
,Max(PesoHvo) PesoHvo
,sum(MortSem8) as MortSem8
,sum(MortSem9) as MortSem9
,sum(MortSem10) as MortSem10
,sum(MortSem11) as MortSem11
,sum(MortSem12) as MortSem12
,sum(MortSem13) as MortSem13
,sum(MortSem14) as MortSem14
,sum(MortSem15) as MortSem15
,sum(MortSem16) as MortSem16
,sum(MortSem17) as MortSem17
,sum(MortSem18) as MortSem18
,sum(MortSem19) as MortSem19
,sum(MortSem20) as MortSem20
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem8
,MAX(STDPorcMortSem8L) STDPorcMortSem8
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem8L) DifPorcMortSem8_STDPorcMortSem8
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem9
,MAX(STDPorcMortSem9L) STDPorcMortSem9
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem9L) DifPorcMortSem9_STDPorcMortSem9
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem10
,MAX(STDPorcMortSem10L) STDPorcMortSem10
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem10L) DifPorcMortSem10_STDPorcMortSem10
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem11
,MAX(STDPorcMortSem11L) STDPorcMortSem11
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem11L) DifPorcMortSem11_STDPorcMortSem11
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem12
,MAX(STDPorcMortSem12L) STDPorcMortSem12
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem12L) DifPorcMortSem12_STDPorcMortSem12
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem13
,MAX(STDPorcMortSem13L) STDPorcMortSem13
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem13L) DifPorcMortSem13_STDPorcMortSem13
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem14
,MAX(STDPorcMortSem14L) STDPorcMortSem14
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem14L) DifPorcMortSem14_STDPorcMortSem14
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem15
,MAX(STDPorcMortSem15L) STDPorcMortSem15
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem15L) DifPorcMortSem15_STDPorcMortSem15
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem16
,MAX(STDPorcMortSem16L) STDPorcMortSem16
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem16L) DifPorcMortSem16_STDPorcMortSem16
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem17
,MAX(STDPorcMortSem17L) STDPorcMortSem17
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem17L) DifPorcMortSem17_STDPorcMortSem17
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem18
,MAX(STDPorcMortSem18L) STDPorcMortSem18
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem18L) DifPorcMortSem18_STDPorcMortSem18
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem19
,MAX(STDPorcMortSem19L) STDPorcMortSem19
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem19L) DifPorcMortSem19_STDPorcMortSem19
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(PobInicial*1.0))*100),14) end AS PorcMortSem20
,MAX(STDPorcMortSem20L) STDPorcMortSem20
,case when sum(PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(PobInicial*1.0))*100),14) end - MAX(STDPorcMortSem20L) DifPorcMortSem20_STDPorcMortSem20
,sum(MortSemAcum8) as MortSemAcum8
,sum(MortSemAcum9) as MortSemAcum9
,sum(MortSemAcum10) as MortSemAcum10
,sum(MortSemAcum11) as MortSemAcum11
,sum(MortSemAcum12) as MortSemAcum12
,sum(MortSemAcum13) as MortSemAcum13
,sum(MortSemAcum14) as MortSemAcum14
,sum(MortSemAcum15) as MortSemAcum15
,sum(MortSemAcum16) as MortSemAcum16
,sum(MortSemAcum17) as MortSemAcum17
,sum(MortSemAcum18) as MortSemAcum18
,sum(MortSemAcum19) as MortSemAcum19
,sum(MortSemAcum20) as MortSemAcum20
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum8
,MAX(STDPorcMortSemAcum8L) STDPorcMortSemAcum8
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum8L) DifPorcMortSemAcum8_STDPorcMortSemAcum8
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum9
,MAX(STDPorcMortSemAcum9L) STDPorcMortSemAcum9
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum9L) DifPorcMortSemAcum9_STDPorcMortSemAcum9
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum10
,MAX(STDPorcMortSemAcum10L) STDPorcMortSemAcum10
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum10L) DifPorcMortSemAcum10_STDPorcMortSemAcum10
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum11
,MAX(STDPorcMortSemAcum11L) STDPorcMortSemAcum11
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum11L) DifPorcMortSemAcum11_STDPorcMortSemAcum11
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum12
,MAX(STDPorcMortSemAcum12L) STDPorcMortSemAcum12
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum12L) DifPorcMortSemAcum12_STDPorcMortSemAcum12
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum13
,MAX(STDPorcMortSemAcum13L) STDPorcMortSemAcum13
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum13L) DifPorcMortSemAcum13_STDPorcMortSemAcum13
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum14
,MAX(STDPorcMortSemAcum14L) STDPorcMortSemAcum14
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum14L) DifPorcMortSemAcum14_STDPorcMortSemAcum14
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum15
,MAX(STDPorcMortSemAcum15L) STDPorcMortSemAcum15
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum15L) DifPorcMortSemAcum15_STDPorcMortSemAcum15
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum16
,MAX(STDPorcMortSemAcum16L) STDPorcMortSemAcum16
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum16L) DifPorcMortSemAcum16_STDPorcMortSemAcum16
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum17
,MAX(STDPorcMortSemAcum17L) STDPorcMortSemAcum17
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum17L) DifPorcMortSemAcum17_STDPorcMortSemAcum17
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum18
,MAX(STDPorcMortSemAcum18L) STDPorcMortSemAcum18
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum18L) DifPorcMortSemAcum18_STDPorcMortSemAcum18
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum19
,MAX(STDPorcMortSemAcum19L) STDPorcMortSemAcum19
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum19L) DifPorcMortSemAcum19_STDPorcMortSemAcum19
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20) / sum(PobInicial*1.0))*100,14) end AS PorcMortSemAcum20
,MAX(STDPorcMortSemAcum20L) STDPorcMortSemAcum20
,case when sum(PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20)/ sum(PobInicial*1.0))*100,14) end - MAX(STDPorcMortSemAcum20L) DifPorcMortSemAcum20_STDPorcMortSemAcum20
,sum(PavoIni) as PavoIni
,sum(Pavo1) as Pavo1
,sum(Pavo2) as Pavo2
,sum(Pavo3) as Pavo3
,sum(Pavo4) as Pavo4
,sum(Pavo5) as Pavo5
,sum(Pavo6) as Pavo6
,sum(Pavo7) as Pavo7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PavoIni)/(sum(AvesRendidas))),14) end PorcPavoIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo1)/(sum(AvesRendidas))),14) end PorcPavo1
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo2)/(sum(AvesRendidas))),14) end PorcPavo2
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo3)/(sum(AvesRendidas))),14) end PorcPavo3
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo4)/(sum(AvesRendidas))),14) end PorcPavo4
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo5)/(sum(AvesRendidas))),14) end PorcPavo5
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo6)/(sum(AvesRendidas))),14) end PorcPavo6
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo7)/(sum(AvesRendidas))),14) end PorcPavo7
,sum(PesoSem8) as PesoSem8
,MAX(STDPorcPesoSem8L) STDPesoSem8
,sum(PesoSem8) - MAX(STDPorcPesoSem8L) DifPesoSem8_STDPesoSem8
,sum(PesoSem9) as PesoSem9
,MAX(STDPorcPesoSem9L) STDPesoSem9
,sum(PesoSem9) - MAX(STDPorcPesoSem9L) DifPesoSem9_STDPesoSem9
,sum(PesoSem10) as PesoSem10
,MAX(STDPorcPesoSem10L) STDPesoSem10
,sum(PesoSem10) - MAX(STDPorcPesoSem10L) DifPesoSem10_STDPesoSem10
,sum(PesoSem11) as PesoSem11
,MAX(STDPorcPesoSem11L) STDPesoSem11
,sum(PesoSem11) - MAX(STDPorcPesoSem11L) DifPesoSem11_STDPesoSem11
,sum(PesoSem12) as PesoSem12
,MAX(STDPorcPesoSem12L) STDPesoSem12
,sum(PesoSem12) - MAX(STDPorcPesoSem12L) DifPesoSem12_STDPesoSem12
,sum(PesoSem13) as PesoSem13
,MAX(STDPorcPesoSem13L) STDPesoSem13
,sum(PesoSem13) - MAX(STDPorcPesoSem13L) DifPesoSem13_STDPesoSem13
,sum(PesoSem14) as PesoSem14
,MAX(STDPorcPesoSem14L) STDPesoSem14
,sum(PesoSem14) - MAX(STDPorcPesoSem14L) DifPesoSem14_STDPesoSem14
,sum(PesoSem15) as PesoSem15
,MAX(STDPorcPesoSem15L) STDPesoSem15
,sum(PesoSem15) - MAX(STDPorcPesoSem15L) DifPesoSem15_STDPesoSem15
,sum(PesoSem16) as PesoSem16
,MAX(STDPorcPesoSem16L) STDPesoSem16
,sum(PesoSem16) - MAX(STDPorcPesoSem16L) DifPesoSem16_STDPesoSem16
,sum(PesoSem17) as PesoSem17
,MAX(STDPorcPesoSem17L) STDPesoSem17
,sum(PesoSem17) - MAX(STDPorcPesoSem17L) DifPesoSem17_STDPesoSem17
,sum(PesoSem18) as PesoSem18
,MAX(STDPorcPesoSem18L) STDPesoSem18
,sum(PesoSem18) - MAX(STDPorcPesoSem18L) DifPesoSem18_STDPesoSem18
,sum(PesoSem19) as PesoSem19
,MAX(STDPorcPesoSem19L) STDPesoSem19
,sum(PesoSem19) - MAX(STDPorcPesoSem19L) DifPesoSem19_STDPesoSem19
,sum(PesoSem20) as PesoSem20
,MAX(STDPorcPesoSem20L) STDPesoSem20
,sum(PesoSem20) - MAX(STDPorcPesoSem20L) DifPesoSem20_STDPesoSem20
,sum(PavosBBMortIncub) as PavosBBMortIncub
,MAX(STDPorcConsGasInviernoL) STDPorcConsGasInvierno
,MAX(STDPorcConsGasVeranoL) STDPorcConsGasVerano
,'' avesrendidasmediano 
,'' porcavesrendidasmediano
,MAX(NoViableSem1) NoViableSem1
,MAX(NoViableSem2) NoViableSem2
,MAX(NoViableSem3) NoViableSem3
,MAX(NoViableSem4) NoViableSem4
,MAX(NoViableSem5) NoViableSem5
,MAX(NoViableSem6) NoViableSem6
,MAX(NoViableSem7) NoViableSem7
,MAX(PorcNoViableSem1) PorcNoViableSem1
,MAX(PorcNoViableSem2) PorcNoViableSem2
,MAX(PorcNoViableSem3) PorcNoViableSem3
,MAX(PorcNoViableSem4) PorcNoViableSem4
,MAX(PorcNoViableSem5) PorcNoViableSem5
,MAX(PorcNoViableSem6) PorcNoViableSem6
,MAX(PorcNoViableSem7) PorcNoViableSem7
,MAX(NoViableSem8) NoViableSem8
,MAX(PorcNoViableSem8) PorcNoViableSem8
,max(to_date(a.FechaCierre, 'yyyyMMdd')) DescripFecha
,'' descripmes          
,'' descripempresa      
,'' descripdivision     
,'' descripzona         
,'' descripsubzona      
,'' plantel             
,'' lote                
,'' descriptipoproducto 
,'' descripespecie      
,'' descripestado       
,'' descripadministrador
,'' descripproveedor    
,'' descripdiavida  
from {database_name_tmp}.ProduccionDetalleLote A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name_gl}.lk_tiempo D on to_date(a.FechaCierre, 'yyyyMMdd') = D.fecha
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,pk_diasvida,A.pk_plantel,A.pk_lote,pk_tipoproducto,
pk_estado,pk_especie,clote,categoria,FlagAtipico,FlagArtAtipico,clote
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_LoteT"
}
df_ft_consolidado_LoteT.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_consolidado_LoteT")
print('carga ft_consolidado_LoteT', df_ft_consolidado_LoteT.count())
df_ft_consolidado_Lote = spark.sql(f"""SELECT * from {database_name_tmp}.ft_consolidado_LoteT
WHERE date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('carga ft_consolidado_Lote', df_ft_consolidado_Lote.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target2 = f"{bucket_name_prdmtech}ft_consolidado_lote/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consolidado_lote cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consolidado_lote")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_consolidado_Lote.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_loteTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consolidado_loteTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_loteTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_lote"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_lote")
            
    print(f"agrega registros nuevos a la tabla ft_consolidado_lote : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consolidado_lote : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_loteTemporal')
    print(f"Tabla ft_consolidado_loteTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_lote"
    }
    df_ft_consolidado_Lote.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_lote")
df_dias = spark.sql(f"""
with flag as (select ComplexEntityNo,
pk_division,
date_format(DescripFecha,'yyyyMM') pk_mes,
PobInicial,
DiasLimpieza,
DiasCrianza,
TotalCampana,
DiasSaca,
PobInicial * AvesXm2 as PobInicialXAvesXm2,
PobInicial * KgXm2 as PobInicialXKgXm2,
PobInicial * Gas as PobInicialXGas,
PobInicial * Cama as PobInicialXCama,
PobInicial * Agua as PobInicialXAgua,
PobInicial * EdadGranja as PobInicialXEdadGranja,
PobInicial * DiasLimpieza as PobInicialXDiasLimpieza,
PobInicial * DiasCrianza as PobInicialXDiasCrianza,
PobInicial * TotalCampana as PobInicialXTotalCampana,
PobInicial * DiasSaca as PobInicialXDiasSaca
from {database_name_gl}.ft_consolidado_Lote
where pk_estado in (3,4) and pk_empresa = 1 and FlagArtAtipico = 2 )

select	 pk_mes
		,pk_division
		,sum(PobInicialXAvesXm2) PobInicialXAvesXm2
		,sum(PobInicialXKgXm2) PobInicialXKgXm2
		,sum(PobInicialXGas) PobInicialXGas
		,sum(PobInicialXCama) PobInicialXCama
		,sum(PobInicialXAgua) PobInicialXAgua
		,sum(PobInicialXEdadGranja) PobInicialXEdadGranja
		,sum(COALESCE(PobInicialXDiasLimpieza,0)*1.0) as PobInicialXDiasLimpieza
		,sum(COALESCE(PobInicialXDiasCrianza,0)*1.0) as PobInicialXDiasCrianza
		,sum(COALESCE(PobInicialXTotalCampana,0)*1.0) as PobInicialXTotalCampana
		,sum(COALESCE(PobInicialXDiasSaca,0)*1.0) as PobInicialXDiasSaca
from flag
group by pk_mes,pk_division
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/dias"
}
df_dias.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.dias")
print('carga dias', df_dias.count())
df_diasSinAtipico = spark.sql(f"""
with diasSinAtipico as (select ComplexEntityNo,
				pk_division,
				date_format(DescripFecha,'yyyyMM') pk_mes,
				PobInicial,
				DiasLimpieza,
				DiasCrianza,
				TotalCampana,
				DiasSaca,
				PobInicial * AvesXm2 as PobInicialXAvesXm2,
				PobInicial * KgXm2 as PobInicialXKgXm2,
				PobInicial * Gas as PobInicialXGas,
				PobInicial * Cama as PobInicialXCama,
				PobInicial * Agua as PobInicialXAgua,
				PobInicial * EdadGranja as PobInicialXEdadGranja,
				PobInicial * DiasLimpieza as PobInicialXDiasLimpieza,
				PobInicial * DiasCrianza as PobInicialXDiasCrianza,
				PobInicial * TotalCampana as PobInicialXTotalCampana,
				PobInicial * DiasSaca as PobInicialXDiasSaca
		from {database_name_gl}.ft_consolidado_Lote
		where pk_estado in (3,4) and pk_empresa = 1 and FlagAtipico = 1 and FlagArtAtipico = 1)
select	 
		 pk_mes
		,pk_division
		,sum(PobInicialXAvesXm2) PobInicialXAvesXm2
		,sum(PobInicialXKgXm2) PobInicialXKgXm2
		,sum(PobInicialXGas) PobInicialXGas
		,sum(PobInicialXCama) PobInicialXCama
		,sum(PobInicialXAgua) PobInicialXAgua
		,sum(PobInicialXEdadGranja) PobInicialXEdadGranja
		,sum(COALESCE(PobInicialXDiasLimpieza,0)*1.0) as PobInicialXDiasLimpieza
		,sum(COALESCE(PobInicialXDiasCrianza,0)*1.0) as PobInicialXDiasCrianza
		,sum(COALESCE(PobInicialXTotalCampana,0)*1.0) as PobInicialXTotalCampana
		,sum(COALESCE(PobInicialXDiasSaca,0)*1.0) as PobInicialXDiasSaca
from diasSinAtipico
group by pk_mes,pk_division
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/diasSinAtipico"
}
df_diasSinAtipico.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.diasSinAtipico")
print('carga diasSinAtipico', df_diasSinAtipico.count())
df_ft_consolidado_mensual = spark.sql(f"""
select a.pk_mes
,pk_empresa
,case when COALESCE(pk_diasvida,0) = 0 then 121 else pk_diasvida end pk_diasvida
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,min(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(a.PobInicial) as PobInicial
,sum(a.PobInicial) - sum(MortDia) as AvesLogradas
,sum(AvesRendidas) as AvesRendidas
,sum(AvesRendidas) - (sum(a.PobInicial) - sum(MortDia)) as SobranFaltan
,round((sum(KilosRendidos)),14) as KilosRendidos
,sum(MortDia) as MortDia
,case when sum(a.PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(a.PobInicial*1.0))*100)),2) end as PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem1
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem2
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem3
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem4
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem5
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem6
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem7
,sum(MortSemAcum1) as MortSemAcum1
,sum(MortSemAcum2) as MortSemAcum2
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4
,sum(MortSemAcum5) as MortSemAcum5
,sum(MortSemAcum6) as MortSemAcum6
,sum(MortSemAcum7) as MortSemAcum7
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum1
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum2
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum3
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum4
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum5
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum6
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum7
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(ConsDia) as ConsDia
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PreInicio)/(sum(AvesRendidas))),14) end PorcPreIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Inicio)/(sum(AvesRendidas))),14) end PorcIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Acabado)/(sum(AvesRendidas))),14) end PorcAcab
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Terminado)/(sum(AvesRendidas))),14) end PorcTerm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Finalizador)/(sum(AvesRendidas))),14) end PorcFin
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),14) end PorcConsumo
,sum(UnidSeleccion) as Seleccion
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(UnidSeleccion)/sum(a.PobInicial*1.0))*100,14) end as PorcSeleccion
,sum(PesoSem1) as PesoSem1
,sum(PesoSem2) as PesoSem2
,sum(PesoSem3) as PesoSem3
,sum(PesoSem4) as PesoSem4
,sum(PesoSem5) as PesoSem5
,sum(PesoSem6) as PesoSem6
,sum(PesoSem7) as PesoSem7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),14) end as PesoProm
,case when avg(EdadGranjaLote) = 0 then 0.0 else round(((case when AVG(AvesRendidas) = 0 then 0.0 else avg(KilosRendidos)/avg(AvesRendidas) end / (avg(EdadGranjaLote)))*1000),2) end as GananciaDiaVenta
,case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),14) end as ICA
,case when a.pk_division = 4 then ((8.5-avg(PesoProm))/12.22)+avg(ICA) else round((((2.500-avg(PesoProm))/3.8)+avg(ICA)),3) end as ICAAjustado
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXAvesXm2)/sum(a.PobInicial) end AvesXm2
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXKgXm2)/sum(a.PobInicial) end KgXm2
,case when COALESCE(avg(EdadGranjaLote),0) = 0 then 0.0 when avg(ica) = 0 then 0.0 else (((100-avg(PorMort))*(avg(PesoProm)))/(avg(EdadGranjaLote)*avg(ica)))*100 end as IEP
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasLimpieza) /sum(a.PobInicial*1.0) end DiasLimpieza
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasCrianza) /sum(a.PobInicial*1.0) end DiasCrianza
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXTotalCampana) /sum(a.PobInicial*1.0) end TotalCampana
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasSaca) /sum(a.PobInicial*1.0) end DiasSaca
,COALESCE((min(EdadInicioSaca)),0) as EdadInicioSaca
,COALESCE((sum(CantInicioSaca)),0) as CantInicioSaca
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXEdadGranja)/sum(a.PobInicial) end EdadGranja
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXGas)/sum(a.PobInicial) end Gas
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXCama)/sum(a.PobInicial) end Cama
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXAgua)/sum(a.PobInicial) end Agua
,case when sum(PobInicial) = 0 then 0 else round((sum(CantMacho) /sum(PobInicial*1.0))*100,2) end as PorcMacho
,avg(Pigmentacion) as Pigmentacion
,FlagAtipico
,FlagArtAtipico
,sum(MortSem8) as MortSem8
,sum(MortSem9) as MortSem9
,sum(MortSem10) as MortSem10
,sum(MortSem11) as MortSem11
,sum(MortSem12) as MortSem12
,sum(MortSem13) as MortSem13
,sum(MortSem14) as MortSem14
,sum(MortSem15) as MortSem15
,sum(MortSem16) as MortSem16
,sum(MortSem17) as MortSem17
,sum(MortSem18) as MortSem18
,sum(MortSem19) as MortSem19
,sum(MortSem20) as MortSem20
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem8
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem9
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem10
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem11
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem12
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem13
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem14
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem15
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem16
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem17
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem18
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem19
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem20
,sum(MortSemAcum8) as MortSemAcum8
,sum(MortSemAcum9) as MortSemAcum9
,sum(MortSemAcum10) as MortSemAcum10
,sum(MortSemAcum11) as MortSemAcum11
,sum(MortSemAcum12) as MortSemAcum12
,sum(MortSemAcum13) as MortSemAcum13
,sum(MortSemAcum14) as MortSemAcum14
,sum(MortSemAcum15) as MortSemAcum15
,sum(MortSemAcum16) as MortSemAcum16
,sum(MortSemAcum17) as MortSemAcum17
,sum(MortSemAcum18) as MortSemAcum18
,sum(MortSemAcum19) as MortSemAcum19
,sum(MortSemAcum20) as MortSemAcum20
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum8
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum9
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum10
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum11
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum12
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum13
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum14
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum15
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum16
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum17
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum18
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum19
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum20
,SUM(PavoIni) AS PavoIni
,SUM(Pavo1) AS Pavo1
,SUM(Pavo2) AS Pavo2
,SUM(Pavo3) AS Pavo3
,SUM(Pavo4) AS Pavo4
,SUM(Pavo5) AS Pavo5
,SUM(Pavo6) AS Pavo6
,SUM(Pavo7) AS Pavo7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PavoIni)/(sum(AvesRendidas))),14) end PorcPavoIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo1)/(sum(AvesRendidas))),14) end PorcPavo1
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo2)/(sum(AvesRendidas))),14) end PorcPavo2
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo3)/(sum(AvesRendidas))),14) end PorcPavo3
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo4)/(sum(AvesRendidas))),14) end PorcPavo4
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo5)/(sum(AvesRendidas))),14) end PorcPavo5
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo6)/(sum(AvesRendidas))),14) end PorcPavo6
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo7)/(sum(AvesRendidas))),14) end PorcPavo7
,sum(PesoSem8) as PesoSem8
,sum(PesoSem9) as PesoSem9
,sum(PesoSem10) as PesoSem10
,sum(PesoSem11) as PesoSem11
,sum(PesoSem12) as PesoSem12
,sum(PesoSem13) as PesoSem13
,sum(PesoSem14) as PesoSem14
,sum(PesoSem15) as PesoSem15
,sum(PesoSem16) as PesoSem16
,sum(PesoSem17) as PesoSem17
,sum(PesoSem18) as PesoSem18
,sum(PesoSem19) as PesoSem19
,sum(PesoSem20) as PesoSem20
,sum(PavosBBMortIncub) as PavosBBMortIncub
from {database_name_tmp}.ProduccionDetalleMensual A
left join {database_name_tmp}.dias b on b.pk_mes = a.pk_mes and a.pk_division = b.pk_division
WHERE A.pk_mes >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
and FlagArtAtipico = 2 and a.pk_division = 4
group by a.pk_mes,a.pk_empresa,a.pk_diasvida,a.pk_division,FlagAtipico,FlagArtAtipico
union
select a.pk_mes
,pk_empresa
,case when COALESCE(pk_diasvida,0) = 0 then 121 else pk_diasvida end pk_diasvida
,min(FechaAlojamiento) as FechaAlojamiento
,min(FechaCrianza) as FechaCrianza
,min(FechaInicioGranja) as FechaInicioGranja
,min(FechaInicioSaca) as FechaInicioSaca 
,max(FechaFinSaca) as FechaFinSaca
,sum(a.PobInicial) as PobInicial
,sum(a.PobInicial) - sum(MortDia) as AvesLogradas
,sum(AvesRendidas) as AvesRendidas
,sum(AvesRendidas) - (sum(a.PobInicial) - sum(MortDia)) as SobranFaltan
,round((sum(KilosRendidos)),14) as KilosRendidos
,sum(MortDia) as MortDia
,case when sum(a.PobInicial) = 0 then 0.0 else round((((sum(MortDia) / sum(a.PobInicial*1.0))*100)),2) end as PorMort
,sum(MortSem1) as MortSem1
,sum(MortSem2) as MortSem2
,sum(MortSem3) as MortSem3
,sum(MortSem4) as MortSem4
,sum(MortSem5) as MortSem5
,sum(MortSem6) as MortSem6
,sum(MortSem7) as MortSem7
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem1) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem1
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem2) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem2
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem3) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem3
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem4) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem4
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem5) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem5
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem6) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem6
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem7) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem7
,sum(MortSemAcum1) as MortSemAcum1
,sum(MortSemAcum2) as MortSemAcum2
,sum(MortSemAcum3) as MortSemAcum3
,sum(MortSemAcum4) as MortSemAcum4
,sum(MortSemAcum5) as MortSemAcum5
,sum(MortSemAcum6) as MortSemAcum6
,sum(MortSemAcum7) as MortSemAcum7
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum1)/ sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum1
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum2) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum2
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum3) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum3
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum4) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum4
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum5) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum5
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum6) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum6
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum7) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum7
,sum(PreInicio) as PreInicio
,sum(Inicio) as Inicio
,sum(Acabado) as Acabado
,sum(Terminado) as Terminado
,sum(Finalizador) as Finalizador
,sum(ConsDia) as ConsDia
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PreInicio)/(sum(AvesRendidas))),14) end PorcPreIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Inicio)/(sum(AvesRendidas))),14) end PorcIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Acabado)/(sum(AvesRendidas))),14) end PorcAcab
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Terminado)/(sum(AvesRendidas))),14) end PorcTerm
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Finalizador)/(sum(AvesRendidas))),14) end PorcFin
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(ConsDia)/(sum(AvesRendidas))),14) end PorcConsumo
,sum(UnidSeleccion) as Seleccion
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(UnidSeleccion)/sum(a.PobInicial*1.0))*100,14) end as PorcSeleccion
,sum(PesoSem1) as PesoSem1
,sum(PesoSem2) as PesoSem2
,sum(PesoSem3) as PesoSem3
,sum(PesoSem4) as PesoSem4
,sum(PesoSem5) as PesoSem5
,sum(PesoSem6) as PesoSem6
,sum(PesoSem7) as PesoSem7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(KilosRendidos)/sum(AvesRendidas)),14) end as PesoProm
,case when avg(EdadGranjaLote) = 0 then 0.0 else round(((case when AVG(AvesRendidas) = 0 then 0.0 else avg(KilosRendidos)/avg(AvesRendidas) end / (avg(EdadGranjaLote)))*1000),2) end as GananciaDiaVenta
,case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),14) end as ICA
,case when a.pk_division = 4 then ((8.5-avg(PesoProm))/12.22)+avg(ICA) else round((((2.500-avg(PesoProm))/3.8)+avg(ICA)),3) end as ICAAjustado
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXAvesXm2)/sum(a.PobInicial) end AvesXm2
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXKgXm2)/sum(a.PobInicial) end KgXm2
,case when COALESCE(avg(EdadGranjaLote),0) = 0 then 0.0 when avg(ica) = 0 then 0.0 else (((100-avg(PorMort))*(avg(PesoProm)))/(avg(EdadGranjaLote)*avg(ica)))*100 end as IEP
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasLimpieza) /sum(a.PobInicial*1.0) end DiasLimpieza
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasCrianza) /sum(a.PobInicial*1.0) end DiasCrianza
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXTotalCampana) /sum(a.PobInicial*1.0) end TotalCampana
,case when sum(a.PobInicial) = 0 then 0.0 else sum(b.PobInicialXDiasSaca) /sum(a.PobInicial*1.0) end DiasSaca
,COALESCE((min(EdadInicioSaca)),0) as EdadInicioSaca
,COALESCE((sum(CantInicioSaca)),0) as CantInicioSaca
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXEdadGranja)/sum(a.PobInicial) end EdadGranja
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXGas)/sum(a.PobInicial) end Gas
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXCama)/sum(a.PobInicial) end Cama
,case when sum(a.PobInicial) = 0 then 0 else max(b.PobInicialXAgua)/sum(a.PobInicial) end Agua
,case when sum(PobInicial) = 0 then 0 else round((sum(CantMacho) /sum(PobInicial*1.0))*100,2) end as PorcMacho
,avg(Pigmentacion) as Pigmentacion
,FlagAtipico
,FlagArtAtipico
,sum(MortSem8) as MortSem8
,sum(MortSem9) as MortSem9
,sum(MortSem10) as MortSem10
,sum(MortSem11) as MortSem11
,sum(MortSem12) as MortSem12
,sum(MortSem13) as MortSem13
,sum(MortSem14) as MortSem14
,sum(MortSem15) as MortSem15
,sum(MortSem16) as MortSem16
,sum(MortSem17) as MortSem17
,sum(MortSem18) as MortSem18
,sum(MortSem19) as MortSem19
,sum(MortSem20) as MortSem20
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem8) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem8
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem9) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem9
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem10) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem10
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem11) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem11
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem12) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem12
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem13) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem13
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem14) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem14
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem15) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem15
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem16) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem16
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem17) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem17
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem18) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem18
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem19) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem19
,case when sum(a.PobInicial) = 0 then 0.0 else round(((sum(MortSem20) / sum(a.PobInicial*1.0))*100),14) end AS PorcMortSem20
,sum(MortSemAcum8) as MortSemAcum8
,sum(MortSemAcum9) as MortSemAcum9
,sum(MortSemAcum10) as MortSemAcum10
,sum(MortSemAcum11) as MortSemAcum11
,sum(MortSemAcum12) as MortSemAcum12
,sum(MortSemAcum13) as MortSemAcum13
,sum(MortSemAcum14) as MortSemAcum14
,sum(MortSemAcum15) as MortSemAcum15
,sum(MortSemAcum16) as MortSemAcum16
,sum(MortSemAcum17) as MortSemAcum17
,sum(MortSemAcum18) as MortSemAcum18
,sum(MortSemAcum19) as MortSemAcum19
,sum(MortSemAcum20) as MortSemAcum20
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum8) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum8
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum9) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum9
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum10) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum10
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum11) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum11
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum12) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum12
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum13) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum13
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum14) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum14
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum15) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum15
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum16) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum16
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum17) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum17
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum18) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum18
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum19) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum19
,case when sum(a.PobInicial) = 0 then 0.0 else round((sum(MortSemAcum20) / sum(a.PobInicial*1.0))*100,14) end AS PorcMortSemAcum20
,SUM(PavoIni) AS PavoIni
,SUM(Pavo1) AS Pavo1
,SUM(Pavo2) AS Pavo2
,SUM(Pavo3) AS Pavo3
,SUM(Pavo4) AS Pavo4
,SUM(Pavo5) AS Pavo5
,SUM(Pavo6) AS Pavo6
,SUM(Pavo7) AS Pavo7
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(PavoIni)/(sum(AvesRendidas))),14) end PorcPavoIni
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo1)/(sum(AvesRendidas))),14) end PorcPavo1
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo2)/(sum(AvesRendidas))),14) end PorcPavo2
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo3)/(sum(AvesRendidas))),14) end PorcPavo3
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo4)/(sum(AvesRendidas))),14) end PorcPavo4
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo5)/(sum(AvesRendidas))),14) end PorcPavo5
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo6)/(sum(AvesRendidas))),14) end PorcPavo6
,case when sum(AvesRendidas) = 0 then 0.0 else round((sum(Pavo7)/(sum(AvesRendidas))),14) end PorcPavo7
,sum(PesoSem8) as PesoSem8
,sum(PesoSem9) as PesoSem9
,sum(PesoSem10) as PesoSem10
,sum(PesoSem11) as PesoSem11
,sum(PesoSem12) as PesoSem12
,sum(PesoSem13) as PesoSem13
,sum(PesoSem14) as PesoSem14
,sum(PesoSem15) as PesoSem15
,sum(PesoSem16) as PesoSem16
,sum(PesoSem17) as PesoSem17
,sum(PesoSem18) as PesoSem18
,sum(PesoSem19) as PesoSem19
,sum(PesoSem20) as PesoSem20
,sum(PavosBBMortIncub) as PavosBBMortIncub
from {database_name_tmp}.ProduccionDetalleMensual A
left join {database_name_tmp}.diasSinAtipico b on b.pk_mes = a.pk_mes and a.pk_division = b.pk_division
WHERE A.pk_mes >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
and FlagArtAtipico = 1 and a.pk_division = 4
group by a.pk_mes,a.pk_empresa,a.pk_diasvida,a.pk_division,FlagAtipico,FlagArtAtipico
""")
print('df_ft_consolidado_mensual' ,df_ft_consolidado_mensual.count() )
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y%m")
file_name_target2 = f"{bucket_name_prdmtech}ft_consolidado_mensual/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consolidado_mensual cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consolidado_mensual")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((col("pk_mes") >= fecha_str)))
    filtered_new_data = df_ft_consolidado_mensual.filter((col("pk_mes") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_mensualTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consolidado_mensualTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_mensualTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_mensual"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_mensual")
            
    print(f"agrega registros nuevos a la tabla ft_consolidado_mensual : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consolidado_mensual : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_mensualTemporal')
    print(f"Tabla ft_consolidado_mensualTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_mensual"
    }
    df_ft_consolidado_mensual.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consolidado_mensual")
df_UPDft_consolidado_Corral2 = spark.sql(f"""
with GasGalpon as (select pk_plantel,pk_lote,pk_galpon,max(gas) gas from {database_name_gl}.ft_consolidado_galpon where pk_empresa = 1 group by pk_plantel,pk_lote,pk_galpon)
select 
 A.pk_tiempo           
,A.pk_empresa          
,A.pk_division         
,A.pk_zona             
,A.pk_subzona          
,A.pk_plantel          
,A.pk_lote             
,A.pk_galpon           
,A.pk_sexo             
,A.pk_standard         
,A.pk_producto         
,A.pk_tipoproducto     
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.pk_diasvida         
,A.complexentityno     
,A.fechanacimiento     
,A.fechacierre         
,A.fechacierrelote     
,A.fechaalojamiento    
,A.fechacrianza        
,A.fechainiciogranja   
,A.fechainiciosaca     
,A.fechafinsaca        
,A.padremayor          
,A.razamayor           
,A.incubadoramayor     
,A.porcpadremayor      
,A.porcodigoraza       
,A.porcincmayor        
,A.pobinicial          
,A.aveslogradas        
,A.avesrendidas        
,A.sobranfaltan        
,A.kilosrendidos       
,A.mortdia             
,A.pormort             
,A.stdpormort          
,A.difpormort_stdpormort
,A.mortsem1            
,A.mortsem2            
,A.mortsem3            
,A.mortsem4            
,A.mortsem5            
,A.mortsem6            
,A.mortsem7            
,A.porcmortsem1        
,A.stdporcmortsem1     
,A.difporcmortsem1_stdporcmortsem1
,A.porcmortsem2        
,A.stdporcmortsem2     
,A.difporcmortsem2_stdporcmortsem2
,A.porcmortsem3        
,A.stdporcmortsem3     
,A.difporcmortsem3_stdporcmortsem3
,A.porcmortsem4        
,A.stdporcmortsem4     
,A.difporcmortsem4_stdporcmortsem4
,A.porcmortsem5        
,A.stdporcmortsem5     
,A.difporcmortsem5_stdporcmortsem5
,A.porcmortsem6        
,A.stdporcmortsem6     
,A.difporcmortsem6_stdporcmortsem6
,A.porcmortsem7        
,A.stdporcmortsem7     
,A.difporcmortsem7_stdporcmortsem7
,A.mortsemacum1        
,A.mortsemacum2        
,A.mortsemacum3        
,A.mortsemacum4        
,A.mortsemacum5        
,A.mortsemacum6        
,A.mortsemacum7        
,A.porcmortsemacum1    
,A.stdporcmortsemacum1 
,A.difporcmortsemacum1_stdporcmortsemacum1
,A.porcmortsemacum2    
,A.stdporcmortsemacum2 
,A.difporcmortsemacum2_stdporcmortsemacum2
,A.porcmortsemacum3    
,A.stdporcmortsemacum3 
,A.difporcmortsemacum3_stdporcmortsemacum3
,A.porcmortsemacum4    
,A.stdporcmortsemacum4 
,A.difporcmortsemacum4_stdporcmortsemacum4
,A.porcmortsemacum5    
,A.stdporcmortsemacum5 
,A.difporcmortsemacum5_stdporcmortsemacum5
,A.porcmortsemacum6    
,A.stdporcmortsemacum6 
,A.difporcmortsemacum6_stdporcmortsemacum6
,A.porcmortsemacum7    
,A.stdporcmortsemacum7 
,A.difporcmortsemacum7_stdporcmortsemacum7
,A.preinicio           
,A.inicio              
,A.acabado             
,A.terminado           
,A.finalizador         
,A.consdia             
,A.porcpreini          
,A.porcini             
,A.porcacab            
,A.porcterm            
,A.porcfin             
,A.porcconsumo         
,A.stdporcconsumo      
,A.difporcconsumo_stdporcconsumo
,A.seleccion           
,A.porcseleccion       
,A.pesosem1            
,A.stdpesosem1         
,A.difpesosem1_stdpesosem1
,A.pesosem2            
,A.stdpesosem2         
,A.difpesosem2_stdpesosem2
,A.pesosem3            
,A.stdpesosem3         
,A.difpesosem3_stdpesosem3
,A.pesosem4            
,A.stdpesosem4         
,A.difpesosem4_stdpesosem4
,A.pesosem5            
,A.stdpesosem5         
,A.difpesosem5_stdpesosem5
,A.pesosem6            
,A.stdpesosem6         
,A.difpesosem6_stdpesosem6
,A.pesosem7            
,A.stdpesosem7         
,A.difpesosem7_stdpesosem7
,A.pesoprom            
,A.stdpesoprom         
,A.difpesoprom_stdpesoprom
,A.gananciadiaventa    
,A.ica                 
,A.stdica              
,A.difica_stdica       
,A.icaajustado         
,A.avesxm2             
,A.kgxm2               
,A.iep                 
,A.diaslimpieza        
,A.diascrianza         
,A.totalcampana        
,A.diassaca            
,A.edadiniciosaca      
,A.cantiniciosaca      
,A.edadgranja          
,A.gas                 
,A.cama                
,A.agua                
,A.porcmacho           
,A.pigmentacion        
,A.categoria           
,A.pesoalo             
,A.flagatipico         
,A.edadpadrecorral     
,B.gas GasGalpon
,A.pesohvo             
,A.mortsem8            
,A.mortsem9            
,A.mortsem10           
,A.mortsem11           
,A.mortsem12           
,A.mortsem13           
,A.mortsem14           
,A.mortsem15           
,A.mortsem16           
,A.mortsem17           
,A.mortsem18           
,A.mortsem19           
,A.mortsem20           
,A.porcmortsem8        
,A.stdporcmortsem8     
,A.difporcmortsem8_stdporcmortsem8
,A.porcmortsem9        
,A.stdporcmortsem9     
,A.difporcmortsem9_stdporcmortsem9
,A.porcmortsem10       
,A.stdporcmortsem10    
,A.difporcmortsem10_stdporcmortsem10
,A.porcmortsem11       
,A.stdporcmortsem11    
,A.difporcmortsem11_stdporcmortsem11
,A.porcmortsem12       
,A.stdporcmortsem12    
,A.difporcmortsem12_stdporcmortsem12
,A.porcmortsem13       
,A.stdporcmortsem13    
,A.difporcmortsem13_stdporcmortsem13
,A.porcmortsem14       
,A.stdporcmortsem14    
,A.difporcmortsem14_stdporcmortsem14
,A.porcmortsem15       
,A.stdporcmortsem15    
,A.difporcmortsem15_stdporcmortsem15
,A.porcmortsem16       
,A.stdporcmortsem16    
,A.difporcmortsem16_stdporcmortsem16
,A.porcmortsem17       
,A.stdporcmortsem17    
,A.difporcmortsem17_stdporcmortsem17
,A.porcmortsem18       
,A.stdporcmortsem18    
,A.difporcmortsem18_stdporcmortsem18
,A.porcmortsem19       
,A.stdporcmortsem19    
,A.difporcmortsem19_stdporcmortsem19
,A.porcmortsem20       
,A.stdporcmortsem20    
,A.difporcmortsem20_stdporcmortsem20
,A.mortsemacum8        
,A.mortsemacum9        
,A.mortsemacum10       
,A.mortsemacum11       
,A.mortsemacum12       
,A.mortsemacum13       
,A.mortsemacum14       
,A.mortsemacum15       
,A.mortsemacum16       
,A.mortsemacum17       
,A.mortsemacum18       
,A.mortsemacum19       
,A.mortsemacum20       
,A.porcmortsemacum8    
,A.stdporcmortsemacum8 
,A.difporcmortsemacum8_stdporcmortsemacum8
,A.porcmortsemacum9    
,A.stdporcmortsemacum9 
,A.difporcmortsemacum9_stdporcmortsemacum9
,A.porcmortsemacum10   
,A.stdporcmortsemacum10
,A.difporcmortsemacum10_stdporcmortsemacum10
,A.porcmortsemacum11   
,A.stdporcmortsemacum11
,A.difporcmortsemacum11_stdporcmortsemacum11
,A.porcmortsemacum12   
,A.stdporcmortsemacum12
,A.difporcmortsemacum12_stdporcmortsemacum12
,A.porcmortsemacum13   
,A.stdporcmortsemacum13
,A.difporcmortsemacum13_stdporcmortsemacum13
,A.porcmortsemacum14   
,A.stdporcmortsemacum14
,A.difporcmortsemacum14_stdporcmortsemacum14
,A.porcmortsemacum15   
,A.stdporcmortsemacum15
,A.difporcmortsemacum15_stdporcmortsemacum15
,A.porcmortsemacum16   
,A.stdporcmortsemacum16
,A.difporcmortsemacum16_stdporcmortsemacum16
,A.porcmortsemacum17   
,A.stdporcmortsemacum17
,A.difporcmortsemacum17_stdporcmortsemacum17
,A.porcmortsemacum18   
,A.stdporcmortsemacum18
,A.difporcmortsemacum18_stdporcmortsemacum18
,A.porcmortsemacum19   
,A.stdporcmortsemacum19
,A.difporcmortsemacum19_stdporcmortsemacum19
,A.porcmortsemacum20   
,A.stdporcmortsemacum20
,A.difporcmortsemacum20_stdporcmortsemacum20
,A.pavoini             
,A.pavo1               
,A.pavo2               
,A.pavo3               
,A.pavo4               
,A.pavo5               
,A.pavo6               
,A.pavo7               
,A.porcpavoini         
,A.porcpavo1           
,A.porcpavo2           
,A.porcpavo3           
,A.porcpavo4           
,A.porcpavo5           
,A.porcpavo6           
,A.porcpavo7           
,A.pesosem8            
,A.stdpesosem8         
,A.difpesosem8_stdpesosem8
,A.pesosem9            
,A.stdpesosem9         
,A.difpesosem9_stdpesosem9
,A.pesosem10           
,A.stdpesosem10        
,A.difpesosem10_stdpesosem10
,A.pesosem11           
,A.stdpesosem11        
,A.difpesosem11_stdpesosem11
,A.pesosem12           
,A.stdpesosem12        
,A.difpesosem12_stdpesosem12
,A.pesosem13           
,A.stdpesosem13        
,A.difpesosem13_stdpesosem13
,A.pesosem14           
,A.stdpesosem14        
,A.difpesosem14_stdpesosem14
,A.pesosem15           
,A.stdpesosem15        
,A.difpesosem15_stdpesosem15
,A.pesosem16           
,A.stdpesosem16        
,A.difpesosem16_stdpesosem16
,A.pesosem17           
,A.stdpesosem17        
,A.difpesosem17_stdpesosem17
,A.pesosem18           
,A.stdpesosem18        
,A.difpesosem18_stdpesosem18
,A.pesosem19           
,A.stdpesosem19        
,A.difpesosem19_stdpesosem19
,A.pesosem20           
,A.stdpesosem20        
,A.difpesosem20_stdpesosem20
,A.pavosbbmortincub    
,A.edadpadrecorraldescrip
,A.diasaloj            
,A.fechadesinfeccion   
,A.tipocama            
,A.formareutilizacion  
,A.nreuso              
,A.pde                 
,A.pdt                 
,A.promamoniaco        
,A.promtemperatura     
,A.ruidosrespiratorios 
,A.nproductomediano    
,A.avesrendidasmediano 
,A.kilosrendidosmediano
,A.kilosrendidosprommediano
,A.conssem1            
,A.conssem2            
,A.conssem3            
,A.conssem4            
,A.conssem5            
,A.conssem6            
,A.conssem7            
,A.porcconssem1        
,A.porcconssem2        
,A.porcconssem3        
,A.porcconssem4        
,A.porcconssem5        
,A.porcconssem6        
,A.porcconssem7        
,A.difporcconssem1_stdporcconssem1
,A.difporcconssem2_stdporcconssem2
,A.difporcconssem3_stdporcconssem3
,A.difporcconssem4_stdporcconssem4
,A.difporcconssem5_stdporcconssem5
,A.difporcconssem6_stdporcconssem6
,A.difporcconssem7_stdporcconssem7
,A.stdporcconssem1     
,A.stdporcconssem2     
,A.stdporcconssem3     
,A.stdporcconssem4     
,A.stdporcconssem5     
,A.stdporcconssem6     
,A.stdporcconssem7     
,A.stdconssem1         
,A.stdconssem2         
,A.stdconssem3         
,A.stdconssem4         
,A.stdconssem5         
,A.stdconssem6         
,A.stdconssem7         
,A.difconssem1_stdconssem1
,A.difconssem2_stdconssem2
,A.difconssem3_stdconssem3
,A.difconssem4_stdconssem4
,A.difconssem5_stdconssem5
,A.difconssem6_stdconssem6
,A.difconssem7_stdconssem7
,A.cantprimlesion      
,A.cantseglesion       
,A.cantterlesion       
,A.porcprimlesion      
,A.porcseglesion       
,A.porcterlesion       
,A.nomprimlesion       
,A.nomseglesion        
,A.nomterlesion        
,A.diassacaefectivo    
,A.u_peaccidentados    
,A.u_pehigadograso     
,A.u_pehepatomegalia   
,A.u_pehigadohemorragico
,A.u_peinanicion       
,A.u_peproblemarespiratorio
,A.u_pesch             
,A.u_peenteritis       
,A.u_peascitis         
,A.u_pemuertesubita    
,A.u_peestresporcalor  
,A.u_pehidropericardio 
,A.u_pehemopericardio  
,A.u_peuratosis        
,A.u_pematerialcaseoso 
,A.u_peonfalitis       
,A.u_peretenciondeyema 
,A.u_peerosiondemolleja
,A.u_pehemorragiamusculos
,A.u_pesangreenciego   
,A.u_pepericarditis    
,A.u_peperitonitis     
,A.u_peprolapso        
,A.u_pepicaje          
,A.u_perupturaaortica  
,A.u_pebazomoteado     
,A.u_penoviable        
,A.u_peaerosaculitisg2 
,A.u_pecojera          
,A.u_pehigadoicterico  
,A.u_pematerialcaseoso_po1ra
,A.u_pematerialcaseosomedretr
,A.u_penecrosishepatica
,A.u_peneumonia        
,A.u_pesepticemia      
,A.u_pevomitonegro     
,A.porcaccidentados    
,A.porchigadograso     
,A.porchepatomegalia   
,A.porchigadohemorragico
,A.porcinanicion       
,A.porcproblemarespiratorio
,A.porcsch             
,A.porcenteritis       
,A.porcascitis         
,A.porcmuertesubita    
,A.porcestresporcalor  
,A.porchidropericardio 
,A.porchemopericardio  
,A.porcuratosis        
,A.porcmaterialcaseoso 
,A.porconfalitis       
,A.porcretenciondeyema 
,A.porcerosiondemolleja
,A.porchemorragiamusculos
,A.porcsangreenciego   
,A.porcpericarditis    
,A.porcperitonitis     
,A.porcprolapso        
,A.porcpicaje          
,A.porcrupturaaortica  
,A.porcbazomoteado     
,A.porcnoviable        
,A.porcaerosaculitisg2 
,A.porccojera          
,A.porchigadoicterico  
,A.porcmaterialcaseoso_po1ra
,A.porcmaterialcaseosomedretr
,A.porcnecrosishepatica
,A.porcneumonia        
,A.porcsepticemia      
,A.porcvomitonegro     
,A.stdporcconsgasinvierno
,A.stdporcconsgasverano
,A.porcpollomediano    
,A.medsem1             
,A.medsem2             
,A.medsem3             
,A.medsem4             
,A.medsem5             
,A.medsem6             
,A.medsem7             
,A.diasmedsem1         
,A.diasmedsem2         
,A.diasmedsem3         
,A.diasmedsem4         
,A.diasmedsem5         
,A.diasmedsem6         
,A.diasmedsem7         
,A.eventdate           
,A.porcalojamientoxedadpadre
,A.tipoorigen          
,A.peso5dias           
,A.stdpeso5dias        
,A.difpeso5dias_stdpeso5dias
,A.noviablesem1        
,A.noviablesem2        
,A.noviablesem3        
,A.noviablesem4        
,A.noviablesem5        
,A.noviablesem6        
,A.noviablesem7        
,A.porcnoviablesem1    
,A.porcnoviablesem2    
,A.porcnoviablesem3    
,A.porcnoviablesem4    
,A.porcnoviablesem5    
,A.porcnoviablesem6    
,A.porcnoviablesem7    
,A.noviablesem8        
,A.porcnoviablesem8    
,A.pk_tipogranja       
,A.descripfecha        
,A.descripempresa      
,A.descripdivision     
,A.descripzona         
,A.descripsubzona      
,A.plantel             
,A.lote                
,A.galpon              
,A.descripsexo         
,A.descripstandard     
,A.descripproducto     
,A.descriptipoproducto 
,A.descripespecie      
,A.descripestado       
,A.descripadministrador
,A.descripproveedor    
,A.descripdiavida      
,A.descriptipogranja   
,A.u_pecaja            
,A.u_pegota            
,A.u_peintoxicacion    
,A.u_peretrazos        
,A.u_peeliminados      
,A.u_peahogados        
,A.u_peecoli           
,A.u_peotros           
,A.u_pecoccidia        
,A.u_pedeshidratados   
,A.u_pehepatitis       
,A.u_petraumatismo
from {database_name_gl}.ft_consolidado_corral A
left join GasGalpon B on A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_galpon = B.pk_galpon
where A.pk_empresa = 1
""")
print('df_UPDft_consolidado_Corral2' ,df_UPDft_consolidado_Corral2.count() )
path_target1 = f"s3://{bucket_name_target}/{bucket_name_prdmtech}/ft_consolidado_corral"
existing_data = spark.read.format("parquet").load(path_target1)
data_after_delete = existing_data.filter(~(col("pk_empresa")== 1))
filtered_new_data = df_UPDft_consolidado_Corral2.filter(col("pk_empresa")== 1)
final_data = filtered_new_data.union(data_after_delete)     
cant_total = final_data.count()

print('data_after_delete', data_after_delete.count())
print('filtered_new_data', filtered_new_data.count())

# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_corralTemporal"
}
final_data.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_consolidado_corralTemporal")

final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consolidado_corralTemporal")         
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consolidado_corral"
}
final_data2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_gl}.ft_consolidado_corral")

print(f"Total de registros en la tabla ft_consolidado_corral : {cant_total}")
 #Limpia la ubicación temporal
glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consolidado_corralTemporal')
print(f"Tabla ft_consolidado_corralTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
df_LimpiezaDesinfeccion = spark.sql(f"""
SELECT
RTRIM(VB.ComplexEntityNo) ComplexEntityNo,
RTRIM(VB.FarmNo) FarmNo,
RTRIM(VB.EntityNo) EntityNo,
RTRIM(VB.HouseNo) HouseNo,
COALESCE(BFT.Eventdate,'1899-11-30 00:00:00.000') EventDate,
VB.FirstHatchDate,
BE.U_1ra_desinfeccion,
COALESCE(BFT.U_NH3_1,0) U_NH3_1,
COALESCE(BFT.U_NH3_2,0) U_NH3_2,
COALESCE(BFT.U_TempRuma1,0) U_TempRuma1,
COALESCE(BFT.U_TempRuma2,0) U_TempRuma2,
COALESCE(BFT.U_TempRuma3,0) U_TempRuma3,
COALESCE(BFT.U_TempRuma4,0) U_TempRuma4,
VB.ProteinFarmsIRN,
VB.ProteinEntitiesIRN,
VB.ProteinCostCentersIRN,
RTRIM(MUF1.Code) Code,
CASE WHEN LENGTH(RTRIM(MUF2.Name)) = 0 THEN 'Cama Nueva' ELSE RTRIM(MUF2.Name) END Name,
COALESCE(BFT.U_NH3_1,0) + COALESCE(BFT.U_NH3_2,0) + COALESCE(BFT.U_TempRuma1,0) + COALESCE(BFT.U_TempRuma2,0) + COALESCE(BFT.U_TempRuma3,0) + COALESCE(BFT.U_TempRuma4,0) TotalEvaluacion,
CASE WHEN (COALESCE(BFT.U_NH3_1,0) + COALESCE(BFT.U_NH3_2,0) + COALESCE(BFT.U_TempRuma1,0) + COALESCE(BFT.U_TempRuma2,0) + COALESCE(BFT.U_TempRuma3,0) + COALESCE(BFT.U_TempRuma4,0)) = 0 THEN 0 ELSE 
DENSE_RANK() OVER(PARTITION BY VB.ComplexEntityNo ORDER BY BFT.Eventdate ASC) END NumeroFechaEvento
FROM {database_name_si}.si_mvBrimEntities VB
INNER JOIN {database_name_si}.si_BrimEntities AS BE ON BE.IRN = VB.BrimEntitiesIRN
LEFT JOIN {database_name_si}.si_BrimFieldTrans AS BFT ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN 
LEFT JOIN {database_name_si}.si_MtSysUserFieldCodes AS MUF1 ON BE.U_Uso_de_cama =  MUF1.IRN
LEFT JOIN {database_name_si}.si_MtSysUserFieldCodes AS MUF2 ON BE.U_Forma_reutilizacion =  MUF2.IRN
WHERE GRN = 'H' AND SUBSTRING(ComplexEntityNo,1,1) = 'P' 
AND RTRIM(HouseNo) IN ('01','02','03','04','05','06','07','08','09','10')
AND date_format(BE.U_1ra_desinfeccion,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LimpiezaDesinfeccion"
}
df_LimpiezaDesinfeccion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.LimpiezaDesinfeccion")
print('carga LimpiezaDesinfeccion', df_LimpiezaDesinfeccion.count())
df_ft_Temperatura = spark.sql(f"""
WITH 
 RegistrosTotal AS (SELECT MIN(EventDate) EventDate, ComplexEntityNo, SUM(TotalEvaluacion) TotalEvaluacion FROM {database_name_tmp}.LimpiezaDesinfeccion GROUP BY ComplexEntityNo)
,RegistrosTotalCeros as (select * from RegistrosTotal WHERE TotalEvaluacion = 0)
select
LT.pk_tiempo,
LP.pk_plantel,
LL.pk_lote,
LG.pk_galpon,
MO.ComplexEntityNo,
FirstHatchDate FechaInicial,
U_1ra_desinfeccion FechaDesinfeccion,
COALESCE(U_NH3_1,0) AmoniacoMuestra1,
U_NH3_2 AmoniacoMuestra2,
U_TempRuma1 TempRuma1,
U_TempRuma2 TempRuma2,
U_TempRuma3 TempRuma3,
U_TempRuma4 TempRuma4,
Code NReuso,
COALESCE(Name,'Cama Nueva') FormaReutilizacion,
'' pobinicial,    
'' fechainiciogranja,
'' pdt,
(DATEDIFF(U_1ra_desinfeccion, FirstHatchDate )-1) PDE,
CG.pk_empresa,
CG.pk_division,
CG.pk_zona,
CG.pk_subzona,
CG.pk_tipoproducto,
CG.pk_especie,
CG.pk_estado,
CG.pk_administrador,
CG.pk_proveedor,
CG.categoria,
MO.EventDate FechaEvento,
MO.NumeroFechaEvento,
GREATEST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MaxTemp,
LEAST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MinTemp,
(U_TempRuma1 + U_TempRuma2 + U_TempRuma3 + U_TempRuma4) / 4 PromTemp,
LT.fecha descripfecha
from {database_name_tmp}.LimpiezaDesinfeccion MO
LEFT JOIN (select *,EntityNo EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) not in ('C1','C2','C3') or notes is null
           union all
           select *, concat(substring(EntityNo,1,2) , substring(Notes,2,1) , substring(EntityNo,3,3)) EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('C1','C2','C3') and length(EntityNo) <> 7
           union all
           select *, EntityNo EntityNoCat
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('C1','C2','C3') and length(EntityNo) = 7
           ) PE ON PE.IRN = MO.ProteinEntitiesIRN
LEFT JOIN {database_name_gl}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50))
LEFT JOIN {database_name_si}.si_ProteinCostCenters PCC  ON cast(PCC.IRN AS varchar(50)) =LP.ProteinCostCentersIRN
LEFT JOIN {database_name_gl}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1)
LEFT JOIN {database_name_gl}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name_gl}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50))
LEFT JOIN {database_name_gl}.lk_tiempo LT ON LT.fecha =MO.EventDate
LEFT JOIN {database_name_gl}.ft_consolidado_galpon CG ON MO.ComplexEntityNo = CG.ComplexEntityNo
WHERE LD.pk_division = 4 and MO.U_NH3_1 + MO.U_NH3_2 + MO.U_TempRuma1 + MO.U_TempRuma2 + MO.U_TempRuma3 + MO.U_TempRuma4 <> 0
AND date_format(U_1ra_desinfeccion,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
UNION
SELECT
LT.pk_tiempo,
LP.pk_plantel,
LL.pk_lote,
LG.pk_galpon,
MO.ComplexEntityNo,
FirstHatchDate FechaInicial,
U_1ra_desinfeccion FechaDesinfeccion,
U_NH3_1 AmoniacoMuestra1,
U_NH3_2 AmoniacoMuestra2,
U_TempRuma1 TempRuma1,
U_TempRuma2 TempRuma2,
U_TempRuma3 TempRuma3,
U_TempRuma4 TempRuma4,
Code NReuso,
COALESCE(Name,'Cama Nueva') FormaReutilizacion,
'' pobinicial,    
'' fechainiciogranja,
'' pdt,
(DATEDIFF(U_1ra_desinfeccion, FirstHatchDate )-1) PDE,
CG.PK_empresa,
CG.PK_division,
CG.PK_zona,
CG.PK_subzona,
CG.PK_tipoproducto,
CG.PK_especie,
CG.PK_estado,
CG.PK_administrador,
CG.pk_proveedor,
CG.categoria,
MO.EventDate FechaEvento,
MO.NumeroFechaEvento,
GREATEST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MaxTemp,
LEAST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MinTemp,
(U_TempRuma1 + U_TempRuma2 + U_TempRuma3 + U_TempRuma4) / 4 PromTemp,
LT.fecha descripfecha
FROM {database_name_tmp}.LimpiezaDesinfeccion MO
LEFT JOIN (select *,EntityNo EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) not in ('C1','C2','C3') or notes is null
           union all
           select *, concat(substring(EntityNo,1,2) , substring(Notes,2,1) , substring(EntityNo,3,3)) EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('C1','C2','C3') and length(EntityNo) <> 7
           union all
           select *, EntityNo EntityNoCat
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('C1','C2','C3') and length(EntityNo) = 7
           ) PE ON PE.IRN = MO.ProteinEntitiesIRN
LEFT JOIN {database_name_gl}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50))
LEFT JOIN {database_name_si}.si_ProteinCostCenters PCC  ON cast(PCC.IRN AS varchar(50)) =LP.ProteinCostCentersIRN
LEFT JOIN {database_name_gl}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1)
LEFT JOIN {database_name_gl}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name_gl}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50))
LEFT JOIN {database_name_gl}.lk_tiempo LT ON LT.fecha =MO.EventDate
LEFT JOIN {database_name_gl}.ft_consolidado_galpon CG ON MO.ComplexEntityNo = CG.ComplexEntityNo
LEFT JOIN RegistrosTotalCeros RTC ON RTC.ComplexEntityNo = MO.ComplexEntityNo AND RTC.Eventdate = MO.Eventdate
WHERE LD.pk_division = 4 AND RTC.TotalEvaluacion = 0
AND date_format(U_1ra_desinfeccion,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
""")
print('df_ft_Temperatura' , df_ft_Temperatura.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target2 = f"{bucket_name_prdmtech}ft_temperatura/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"

try:
    df_existentes = spark.read.format("parquet").load(path_target2)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_temperatura cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_temperatura")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target2)
    data_after_delete = existing_data.filter(~((date_format(col("fechadesinfeccion"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_Temperatura.filter((date_format(col("fechadesinfeccion"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_temperaturaTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_temperaturaTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_temperaturaTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_temperatura"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_temperatura")
            
    print(f"agrega registros nuevos a la tabla ft_temperatura : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_temperatura : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_temperaturaTemporal')
    print(f"Tabla ft_temperaturaTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_temperatura"
    }
    df_ft_Temperatura.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_temperatura")
df_UPDft_Temperatura = spark.sql(f"""
with temp as (select max(Pobinicial) Pobinicial,ComplexEntityNo, FechaInicioGranja, max(DiasLimpieza) DiasLimpieza from {database_name_gl}.ft_consolidado_galpon group by ComplexEntityNo, FechaInicioGranja)
SELECT 
 A.pk_tiempo               
,A.pk_plantel          
,A.pk_lote             
,A.pk_galpon           
,A.complexentityno     
,A.fechainicial        
,A.fechadesinfeccion   
,A.amoniacomuestra1    
,A.amoniacomuestra2    
,A.tempruma1           
,A.tempruma2           
,A.tempruma3           
,A.tempruma4           
,A.nreuso              
,A.formareutilizacion 
,B.PobInicial as PobInicial
,B.FechaInicioGranja as FechaInicioGranja
,B.DiasLimpieza-1 as PDT 
,A.pde                 
,A.pk_empresa          
,A.pk_division         
,A.pk_zona             
,A.pk_subzona          
,A.pk_tipoproducto     
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.categoria           
,A.fechaevento         
,A.numerofechaevento   
,A.maxtemp             
,A.mintemp             
,A.promtemp 
,A.descripfecha
FROM {database_name_gl}.ft_Temperatura A
LEFT JOIN temp B on A.ComplexEntityNo = B.ComplexEntityNo
""")
print('df_UPDft_Temperatura' , df_UPDft_Temperatura.count())
final_data = df_UPDft_Temperatura
cant_total = final_data.count()

# Escribir los resultados en ruta temporal
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_temperaturaTemporal"
}
final_data.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_temperaturaTemporal")

final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_temperaturaTemporal")         
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_temperatura"
}
final_data2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_gl}.ft_temperatura")

print(f"Total de registros en la tabla ft_temperatura : {cant_total}")
 #Limpia la ubicación temporal
glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_temperaturaTemporal')
print(f"Tabla ft_temperaturaTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()