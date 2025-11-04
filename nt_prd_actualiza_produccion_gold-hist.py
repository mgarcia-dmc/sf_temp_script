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
JOB_NAME = "nt_prd_actualiza_produccion_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, trunc, add_months, date_format,date_add
from datetime import datetime

print("inicia spark")
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

file_name_target2 = f"{bucket_name_prdmtech}ft_actualiza_produccion/"
path_target2 = f"s3://{bucket_name_target}/{file_name_target2}"
print('cargando rutas')
database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
##_OrdenarFechas
df_OrdenarFechas1= spark.sql(f"SELECT ComplexEntityNo,ProteinEntitiesIRN,FirstDatePlaced,LastDateSold,GRN,FarmNo,EntityNo,HouseNo,PenNo, \
                                DENSE_RANK() OVER (PARTITION BY FarmNo,HouseNo ORDER BY FarmNo,EntityNo ASC) AS fila \
                                FROM {database_name}.si_mvbrimentities \
                                WHERE GRN = 'P' and FarmType = 1 and SpeciesType = 1")

df_OrdenarFechas2= spark.sql(f"SELECT ComplexEntityNo,ProteinEntitiesIRN,FirstDatePlaced,LastDateSold,GRN,FarmNo,nlote2 EntityNo,HouseNo,PenNo, \
                                DENSE_RANK() OVER (PARTITION BY FarmNo,HouseNo ORDER BY FarmNo,PlantelCampana ASC) AS fila \
                                FROM {database_name}.si_mvbrimentities A \
                                left join {database_name}.lk_lote B on A.FarmNo = B.noplantel and A.EntityNo = B.nlote \
                                WHERE GRN = 'P' and FarmType = 7 and SpeciesType = 2")

df_OrdenarFechas = df_OrdenarFechas1.union(df_OrdenarFechas2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarFechas"
}
df_OrdenarFechas.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarFechas")
print('carga OrdenarFechas')
df_DiasCampana= spark.sql(f"SELECT ProteinEntitiesIRN,ComplexEntityNo,(SELECT MIN(FirstDatePlaced) FROM {database_name}.OrdenarFechas A \
                            WHERE A.FarmNo = BE.FarmNo AND A.fila = BE.fila AND A.HouseNo = BE.HouseNo AND FirstDatePlaced <> '1899-11-30 00:00:00.000') AS FechaCrianza, \
                            (SELECT MAX(LastDateSold) FROM {database_name}.OrdenarFechas A WHERE A.FarmNo = BE.FarmNo AND A.fila = BE.fila-1  AND A.HouseNo = BE.HouseNo) AS FechaInicioGranja \
                            FROM {database_name}.OrdenarFechas BE \
                            WHERE GRN = 'P'")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DiasCampana"
}
df_DiasCampana.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DiasCampana")
print('carga DiasCampana')
df_AlojamientoMayor =spark.sql(f"select ComplexEntityNo,PadreMayor,RazaMayor,IncubadoraMayor,EdadPadreCorralDescrip,MAX(PorcAlojPadreMayor) PorcAlojPadreMayor, \
                                MAX(PorcRazaMayor) PorcRazaMayor,MAX(PorcIncMayor) PorcIncMayor,MAX(DiasAloj) DiasAloj,MAX(PorcAlojamientoXEdadPadre) PorcAlojamientoXEdadPadre,TipoOrigen \
                                from {database_name}.ft_alojamiento \
                                where pk_empresa = 1 \
                                group by ComplexEntityNo,PadreMayor,RazaMayor,IncubadoraMayor,EdadPadreCorralDescrip,TipoOrigen order by 1")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AlojamientoMayor"
}
df_AlojamientoMayor.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AlojamientoMayor")
print('carga AlojamientoMayor')
df_EdadPadre = spark.sql(f"select ComplexEntityNo,max(EdadPadreCorral) EdadPadreCorral,max(EdadPadreGalpon) EdadPadreGalpon,max(EdadPadreLote) EdadPadreLote\
                            from {database_name}.ft_alojamiento \
                            where pk_empresa = 1 group by ComplexEntityNo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadPadre"
}
df_EdadPadre.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadPadre")
print('carga EdadPadre')
#Realiza el cálculo de SacaYSTD
df_SacaYSTD= spark.sql(f"select fecha pk_fecha,pk_plantel,pk_lote,pk_galpon,pk_sexo,ComplexEntityNo,EdadDiaSaca,AvesGranja,STDPorcMortAcum \
                            ,AvesGranja * STDPorcMortAcum STDPorcMortAcumXAvesGranja,STDPeso,AvesGranja * STDPeso STDPesoXAvesGranja \
                            ,STDConsAcum,AvesGranja * STDConsAcum STDConsAcumXAvesGranja,STDICA,AvesGranja * STDICA STDICAXAvesGranja,STDPorcConsGasInvierno \
                            ,AvesGranja * STDPorcConsGasInvierno STDPorcConsGasInviernoXAvesGranja,STDPorcConsGasVerano,AvesGranja * STDPorcConsGasVerano STDPorcConsGasVeranoXAvesGranja \
                        from (select A.fecha,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,A.EdadDiaSaca,SUM(AvesGranja) AvesGranja \
                       ,0 STDPorcMortAcum,0 STDPeso,0 STDConsAcum,0 STDICA,0 STDPorcConsGasInvierno,0 STDPorcConsGasVerano \
                            from {database_name}.ft_ventas_CD A \
                            where A.pk_empresa = 1 \
                            group by A.fecha,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,A.EdadDiaSaca)A \
                        WHERE fecha >= date_format(date_add(date_trunc('YEAR', current_date), -365), 'yyyy-MM-dd')")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaYSTD"
}
df_SacaYSTD.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaYSTD")
print('carga SacaYSTD')
#Realiza el cálculo de GasGranel
df_STDPond = spark.sql(f"""
    WITH aggregated_mortality AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDPorcMortAcumXAvesGranja)/SUM(AvesGranja) AS STDPorcMortAcum FROM {database_name}.SacaYSTD WHERE STDPorcMortAcumXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo),
     aggregated_weight AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDPesoXAvesGranja)/SUM(AvesGranja) AS STDPeso FROM {database_name}.SacaYSTD WHERE STDPesoXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo),
     aggregated_consumption AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDConsAcumXAvesGranja)/SUM(AvesGranja) AS STDConsAcum FROM {database_name}.SacaYSTD WHERE STDConsAcumXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo),
     aggregated_ica AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDICAXAvesGranja)/SUM(AvesGranja) AS STDICA FROM {database_name}.SacaYSTD WHERE STDICAXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo),
     aggregated_porcongasinvierno AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDPorcConsGasInviernoXAvesGranja)/SUM(AvesGranja) AS STDPorcConsGasInvierno FROM {database_name}.SacaYSTD WHERE STDPorcConsGasInviernoXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo),
     aggregated_porcongasverano AS (SELECT pk_plantel, pk_lote, pk_galpon, pk_sexo,SUM(STDPorcConsGasVeranoXAvesGranja)/SUM(AvesGranja) AS STDPorcConsGasVerano FROM {database_name}.SacaYSTD WHERE STDPorcConsGasVeranoXAvesGranja <> 0 GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo)

    SELECT MAX(A.pk_fecha) AS pk_fecha, 
           A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo,
           B.STDPorcMortAcum AS STDPorcMortAcumC,C.STDPorcMortAcum AS STDPorcMortAcumG,D.STDPorcMortAcum AS STDPorcMortAcumL,
           E.STDPeso AS STDPesoC, F.STDPeso AS STDPesoG, G.STDPeso AS STDPesoL,
           H.STDConsAcum STDConsAcumC, I.STDConsAcum STDConsAcumG, J.STDConsAcum STDConsAcumL,
           K.STDICA STDICAC, L.STDICA STDICAG, M.STDICA STDICAL,
           N.STDPorcConsGasInvierno STDPorcConsGasInviernoC,O.STDPorcConsGasInvierno STDPorcConsGasInviernoG, P.STDPorcConsGasInvierno STDPorcConsGasInviernoL,
           Q.STDPorcConsGasVerano STDPorcConsGasVeranoC, R.STDPorcConsGasVerano STDPorcConsGasVeranoG, S.STDPorcConsGasVerano STDPorcConsGasVeranoL
    FROM {database_name}.SacaYSTD A 
    LEFT JOIN aggregated_mortality B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon AND A.pk_sexo = B.pk_sexo
    LEFT JOIN aggregated_mortality C ON A.pk_plantel = C.pk_plantel AND A.pk_lote = C.pk_lote AND A.pk_galpon = C.pk_galpon 
    LEFT JOIN aggregated_mortality D ON A.pk_plantel = D.pk_plantel AND A.pk_lote = D.pk_lote
    LEFT JOIN aggregated_weight E ON A.pk_plantel = E.pk_plantel AND A.pk_lote = E.pk_lote AND A.pk_galpon = E.pk_galpon AND A.pk_sexo = E.pk_sexo
    LEFT JOIN aggregated_weight F ON A.pk_plantel = F.pk_plantel AND A.pk_lote = F.pk_lote AND A.pk_galpon = F.pk_galpon 
    LEFT JOIN aggregated_weight G ON A.pk_plantel = G.pk_plantel AND A.pk_lote = G.pk_lote
    LEFT JOIN aggregated_consumption H ON A.pk_plantel = H.pk_plantel AND A.pk_lote = H.pk_lote AND A.pk_galpon = H.pk_galpon AND A.pk_sexo = H.pk_sexo
    LEFT JOIN aggregated_consumption I ON A.pk_plantel = I.pk_plantel AND A.pk_lote = I.pk_lote AND A.pk_galpon = I.pk_galpon 
    LEFT JOIN aggregated_consumption J ON A.pk_plantel = J.pk_plantel AND A.pk_lote = J.pk_lote
    LEFT JOIN aggregated_ica K ON A.pk_plantel = K.pk_plantel AND A.pk_lote = K.pk_lote AND A.pk_galpon = K.pk_galpon AND A.pk_sexo = K.pk_sexo
    LEFT JOIN aggregated_ica L ON A.pk_plantel = L.pk_plantel AND A.pk_lote = L.pk_lote AND A.pk_galpon = L.pk_galpon 
    LEFT JOIN aggregated_ica M ON A.pk_plantel = M.pk_plantel AND A.pk_lote = M.pk_lote
    LEFT JOIN aggregated_porcongasinvierno N ON A.pk_plantel = N.pk_plantel AND A.pk_lote = N.pk_lote AND A.pk_galpon = N.pk_galpon AND A.pk_sexo = N.pk_sexo
    LEFT JOIN aggregated_porcongasinvierno O ON A.pk_plantel = O.pk_plantel AND A.pk_lote = O.pk_lote AND A.pk_galpon = O.pk_galpon 
    LEFT JOIN aggregated_porcongasinvierno P ON A.pk_plantel = P.pk_plantel AND A.pk_lote = P.pk_lote
    LEFT JOIN aggregated_porcongasverano Q ON A.pk_plantel = Q.pk_plantel AND A.pk_lote = Q.pk_lote AND A.pk_galpon = Q.pk_galpon AND A.pk_sexo = Q.pk_sexo
    LEFT JOIN aggregated_porcongasverano R ON A.pk_plantel = R.pk_plantel AND A.pk_lote = R.pk_lote AND A.pk_galpon = R.pk_galpon 
    LEFT JOIN aggregated_porcongasverano S ON A.pk_plantel = S.pk_plantel AND A.pk_lote = S.pk_lote    
    GROUP BY A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo,B.STDPorcMortAcum,C.STDPorcMortAcum,D.STDPorcMortAcum,E.STDPeso, F.STDPeso,G.STDPeso,H.STDConsAcum, I.STDConsAcum, J.STDConsAcum
    ,K.STDICA, L.STDICA, M.STDICA,N.STDPorcConsGasInvierno,O.STDPorcConsGasInvierno, P.STDPorcConsGasInvierno,Q.STDPorcConsGasVerano, R.STDPorcConsGasVerano, S.STDPorcConsGasVerano
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPond"
}
df_STDPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPond")
print('carga STDPond')
#Realiza el cálculo de acumuladoSaca
df_acumuladoSaca = spark.sql(f"""
    WITH ventas_aggregated AS (
        SELECT ComplexEntityNo, pk_tiempo,MAX(COALESCE(avesgranjaacum, 0)) AS AvesRendidas,MAX(COALESCE(PesoGranjaAcum, 0)) AS kilosRendidos
        FROM {database_name}.ft_ventas_CD
        GROUP BY ComplexEntityNo, pk_tiempo)

    SELECT 
        pd.fecha, pd.ComplexEntityNo,
        COALESCE(v.AvesRendidas, 0) AS AvesRendidas,
        COALESCE(SUM(v.AvesRendidas) OVER (PARTITION BY pd.ComplexEntityNo ORDER BY pd.pk_tiempo), 0) AS AvesRendidasAcum,
        COALESCE(v.kilosRendidos, 0) AS kilosRendidos,
        COALESCE(SUM(v.kilosRendidos) OVER (PARTITION BY pd.ComplexEntityNo ORDER BY pd.pk_tiempo), 0) AS kilosRendidosAcum
    FROM {database_name}.ProduccionDetalle pd
    LEFT JOIN ventas_aggregated v ON pd.ComplexEntityNo = v.ComplexEntityNo AND pd.pk_tiempo = v.pk_tiempo
    WHERE pd.fecha >= date_format(date_add(date_trunc('YEAR', current_date), -365), 'yyyy-MM-dd')
    ORDER BY pd.fecha """)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/acumuladoSaca"
}
df_acumuladoSaca.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.acumuladoSaca")
print('carga acumuladoSaca')
#Realiza el cálculo de STDPorcMort
df_STDPorcMort = spark.sql(f"""select A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,MAX(STDPorcMortSem)STDPorcMortSem,MAX(STDPorcMortSemAcum)STDPorcMortSemAcum
                            from {database_name}.ft_mortalidad_diario A
                            where A.pk_empresa = 1 and A.fecha >= date_format(date_add(date_trunc('YEAR', current_date), -365), 'yyyy-MM-dd')
                            group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcMort"
}
df_STDPorcMort.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPorcMort")
print('carga STDPorcMort')
#Realiza el cálculo de STDPorcMortXPobInicial
df_STDPorcMortXPobInicial = spark.sql(f"""
WITH aggregated_data AS (
    SELECT 
        ComplexEntityNo,pk_semanavida,MAX(STDPorcMortSem) AS STDPorcMortSem,MAX(STDPorcMortSemAcum) AS STDPorcMortSemAcum
    FROM {database_name}.STDPorcMort
    GROUP BY ComplexEntityNo,pk_semanavida
)

select 
A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,
    -- Pivot Mortality Percentage by Week
    MAX(CASE WHEN B.pk_semanavida = 1 THEN B.STDPorcMortSem END) AS STDPorcMortSem1,
    MAX(CASE WHEN B.pk_semanavida = 2 THEN B.STDPorcMortSem END) AS STDPorcMortSem2,
    MAX(CASE WHEN B.pk_semanavida = 3 THEN B.STDPorcMortSem END) AS STDPorcMortSem3,
    MAX(CASE WHEN B.pk_semanavida = 4 THEN B.STDPorcMortSem END) AS STDPorcMortSem4,
    MAX(CASE WHEN B.pk_semanavida = 5 THEN B.STDPorcMortSem END) AS STDPorcMortSem5,
    MAX(CASE WHEN B.pk_semanavida = 6 THEN B.STDPorcMortSem END) AS STDPorcMortSem6,
    MAX(CASE WHEN B.pk_semanavida = 7 THEN B.STDPorcMortSem END) AS STDPorcMortSem7,
    MAX(CASE WHEN B.pk_semanavida = 8 THEN B.STDPorcMortSem END) AS STDPorcMortSem8,
    MAX(CASE WHEN B.pk_semanavida = 9 THEN B.STDPorcMortSem END) AS STDPorcMortSem9,
    MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSem END) AS STDPorcMortSem10,
    MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSem END) AS STDPorcMortSem11,
    MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSem END) AS STDPorcMortSem12,
    MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSem END) AS STDPorcMortSem13,
    MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSem END) AS STDPorcMortSem14,
    MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSem END) AS STDPorcMortSem15,
    MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSem END) AS STDPorcMortSem16,
    MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSem END) AS STDPorcMortSem17,
    MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSem END) AS STDPorcMortSem18,
    MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSem END) AS STDPorcMortSem19,
    MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSem END) AS STDPorcMortSem20,
    -- Pivot Accumulated Mortality Percentage by Week
    MAX(CASE WHEN B.pk_semanavida = 1 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum1,
    MAX(CASE WHEN B.pk_semanavida = 2 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum2,
    MAX(CASE WHEN B.pk_semanavida = 3 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum3,
    MAX(CASE WHEN B.pk_semanavida = 4 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum4,
    MAX(CASE WHEN B.pk_semanavida = 5 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum5,
    MAX(CASE WHEN B.pk_semanavida = 6 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum6,
    MAX(CASE WHEN B.pk_semanavida = 7 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum7,
    MAX(CASE WHEN B.pk_semanavida = 8 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum8,
    MAX(CASE WHEN B.pk_semanavida = 9 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum9,
    MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum10,
    MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum11,
    MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum12,
    MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum13,
    MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum14,
    MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum15,
    MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum16,
    MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum17,
    MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum18,
    MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum19,
    MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum20,
    -- Pivot PobInicial x Mortality Percentage by Week
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 1 THEN B.STDPorcMortSem END) AS STDPorcMortSem1XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 2 THEN B.STDPorcMortSem END) AS STDPorcMortSem2XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 3 THEN B.STDPorcMortSem END) AS STDPorcMortSem3XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 4 THEN B.STDPorcMortSem END) AS STDPorcMortSem4XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 5 THEN B.STDPorcMortSem END) AS STDPorcMortSem5XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 6 THEN B.STDPorcMortSem END) AS STDPorcMortSem6XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 7 THEN B.STDPorcMortSem END) AS STDPorcMortSem7XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 8 THEN B.STDPorcMortSem END) AS STDPorcMortSem8XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 9 THEN B.STDPorcMortSem END) AS STDPorcMortSem9XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSem END) AS STDPorcMortSem10XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSem END) AS STDPorcMortSem11XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSem END) AS STDPorcMortSem12XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSem END) AS STDPorcMortSem13XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSem END) AS STDPorcMortSem14XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSem END) AS STDPorcMortSem15XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSem END) AS STDPorcMortSem16XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSem END) AS STDPorcMortSem17XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSem END) AS STDPorcMortSem18XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSem END) AS STDPorcMortSem19XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSem END) AS STDPorcMortSem20XPobInicial,
    -- Pivot PobInicial x Accumulated Mortality Percentage by Week
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 1 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum1XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 2 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum2XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 3 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum3XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 4 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum4XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 5 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum5XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 6 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum6XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 7 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum7XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 8 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum8XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 9 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum9XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 10 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum10XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 11 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum11XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 12 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum12XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 13 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum13XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 14 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum14XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 15 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum15XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 16 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum16XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 17 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum17XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 18 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum18XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 19 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum19XPobInicial,
    MAX(PobInicial)*MAX(CASE WHEN B.pk_semanavida = 20 THEN B.STDPorcMortSemAcum END) AS STDPorcMortSemAcum20XPobInicial
FROM {database_name}.STDPorcMort A
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
    .saveAsTable(f"{database_name}.STDPorcMortXPobInicial")
print('carga STDPorcMortXPobInicial')
#Realiza el cálculo de STDMortPond
df_STDMortPond = spark.sql(f"""
WITH aggregated_mortality AS (
    SELECT 
        pk_plantel, 
        pk_lote, 
        pk_galpon, 
        pk_sexo,
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
    FROM {database_name}.STDPorcMortXPobInicial
    WHERE pobinicial <> 0
    GROUP BY pk_plantel, pk_lote, pk_galpon, pk_sexo
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
FROM {database_name}.stdporcmortxpobinicial A
LEFT JOIN aggregated_mortality B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon
LEFT JOIN aggregated_mortality C ON A.pk_plantel = C.pk_plantel AND A.pk_lote = C.pk_lote
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
    .saveAsTable(f"{database_name}.STDMortPond")
print('carga STDMortPond')
#Realiza el cálculo de STDPorcPeso
df_STDPorcPeso1 = spark.sql(f"select A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,MAX(STDPeso) STDPeso \
                            from {database_name}.ft_peso_diario A where A.pk_empresa = 1 group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo")

df_STDPorcPeso2 = spark.sql(f"select 0 pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo,MAX(PobInicial) PobInicial,MAX(STDPeso) STDPeso \
                            from {database_name}.ft_peso_diario A where A.pk_empresa = 1 and A.pk_diasvida = 5 group by A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo")

df_STDPorcPeso= df_STDPorcPeso1.union(df_STDPorcPeso2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcPeso"
}
df_STDPorcPeso.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPorcPeso")
print('carga STDPorcPeso')
#Realiza el cálculo de STDPorcPesoXPobInicial
df_STDPorcPesoXPobInicial = spark.sql(f"""WITH aggregated_weights AS (
    SELECT 
        ComplexEntityNo,
        MAX(CASE WHEN pk_semanavida = 1 THEN STDPeso ELSE NULL END) AS STDPesoSem1,
        MAX(CASE WHEN pk_semanavida = 2 THEN STDPeso ELSE NULL END) AS STDPesoSem2,
        MAX(CASE WHEN pk_semanavida = 3 THEN STDPeso ELSE NULL END) AS STDPesoSem3,
        MAX(CASE WHEN pk_semanavida = 4 THEN STDPeso ELSE NULL END) AS STDPesoSem4,
        MAX(CASE WHEN pk_semanavida = 5 THEN STDPeso ELSE NULL END) AS STDPesoSem5,
        MAX(CASE WHEN pk_semanavida = 6 THEN STDPeso ELSE NULL END) AS STDPesoSem6,
        MAX(CASE WHEN pk_semanavida = 7 THEN STDPeso ELSE NULL END) AS STDPesoSem7,
        MAX(CASE WHEN pk_semanavida = 8 THEN STDPeso ELSE NULL END) AS STDPesoSem8,
        MAX(CASE WHEN pk_semanavida = 9 THEN STDPeso ELSE NULL END) AS STDPesoSem9,
        MAX(CASE WHEN pk_semanavida = 10 THEN STDPeso ELSE NULL END) AS STDPesoSem10,
        MAX(CASE WHEN pk_semanavida = 11 THEN STDPeso ELSE NULL END) AS STDPesoSem11,
        MAX(CASE WHEN pk_semanavida = 12 THEN STDPeso ELSE NULL END) AS STDPesoSem12,
        MAX(CASE WHEN pk_semanavida = 13 THEN STDPeso ELSE NULL END) AS STDPesoSem13,
        MAX(CASE WHEN pk_semanavida = 14 THEN STDPeso ELSE NULL END) AS STDPesoSem14,
        MAX(CASE WHEN pk_semanavida = 15 THEN STDPeso ELSE NULL END) AS STDPesoSem15,
        MAX(CASE WHEN pk_semanavida = 16 THEN STDPeso ELSE NULL END) AS STDPesoSem16,
        MAX(CASE WHEN pk_semanavida = 17 THEN STDPeso ELSE NULL END) AS STDPesoSem17,
        MAX(CASE WHEN pk_semanavida = 18 THEN STDPeso ELSE NULL END) AS STDPesoSem18,
        MAX(CASE WHEN pk_semanavida = 19 THEN STDPeso ELSE NULL END) AS STDPesoSem19,
        MAX(CASE WHEN pk_semanavida = 20 THEN STDPeso ELSE NULL END) AS STDPesoSem20,
        MAX(CASE WHEN pk_semanavida = 0 THEN STDPeso ELSE NULL END) AS STDPeso5Dias
    FROM {database_name}.STDPorcPeso
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
FROM {database_name}.STDPorcPeso A
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
    .saveAsTable(f"{database_name}.STDPorcPesoXPobInicial")
print('carga STDPorcPesoXPobInicial')
#Realiza el cálculo de STDPesoPond
df_STDPesoPond = spark.sql(f"""WITH aggregated_weights AS (
    SELECT 
        pk_plantel, pk_lote, pk_galpon,
        SUM(STDPesoSem1XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem1,
        SUM(STDPesoSem2XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem2,
        SUM(STDPesoSem3XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem3,
        SUM(STDPesoSem4XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem4,
        SUM(STDPesoSem5XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem5,
        SUM(STDPesoSem6XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem6,
        SUM(STDPesoSem7XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem7,
        SUM(STDPesoSem8XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem8,
        SUM(STDPesoSem9XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem9,
        SUM(STDPesoSem10XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem10,
        SUM(STDPesoSem11XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem11,
        SUM(STDPesoSem12XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem12,
        SUM(STDPesoSem13XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem13,
        SUM(STDPesoSem14XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem14,
        SUM(STDPesoSem15XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem15,
        SUM(STDPesoSem16XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem16,
        SUM(STDPesoSem17XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem17,
        SUM(STDPesoSem18XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem18,
        SUM(STDPesoSem19XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem19,
        SUM(STDPesoSem20XPobInicial) / SUM(PobInicial) AS STDPorcPesoSem20,
        SUM(STDPeso5DiasXPobInicial) / SUM(PobInicial) AS STDPorcPeso5Dias
    FROM {database_name}.STDPorcPesoXPobInicial
    WHERE PobInicial > 0
    GROUP BY pk_plantel, pk_lote, pk_galpon)

SELECT 
    A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo,
    STDPesoSem1, STDPesoSem2, STDPesoSem3, STDPesoSem4, STDPesoSem5,
    STDPesoSem6, STDPesoSem7, STDPesoSem8, STDPesoSem9, STDPesoSem10,
    STDPesoSem11, STDPesoSem12, STDPesoSem13, STDPesoSem14, STDPesoSem15,
    STDPesoSem16, STDPesoSem17, STDPesoSem18, STDPesoSem19, STDPesoSem20,
    STDPeso5Dias,
    -- Datos agregados
    COALESCE(B.STDPorcPesoSem1, 0) AS STDPorcPesoSem1G,
    COALESCE(B.STDPorcPesoSem2, 0) AS STDPorcPesoSem2G,
    COALESCE(B.STDPorcPesoSem3, 0) AS STDPorcPesoSem3G,
    COALESCE(B.STDPorcPesoSem4, 0) AS STDPorcPesoSem4G,
    COALESCE(B.STDPorcPesoSem5, 0) AS STDPorcPesoSem5G,
    COALESCE(B.STDPorcPesoSem6, 0) AS STDPorcPesoSem6G,
    COALESCE(B.STDPorcPesoSem7, 0) AS STDPorcPesoSem7G,
    COALESCE(B.STDPorcPesoSem8, 0) AS STDPorcPesoSem8G,
    COALESCE(B.STDPorcPesoSem9, 0) AS STDPorcPesoSem9G,
    COALESCE(B.STDPorcPesoSem10, 0) AS STDPorcPesoSem10G,
    COALESCE(B.STDPorcPesoSem11, 0) AS STDPorcPesoSem11G,
    COALESCE(B.STDPorcPesoSem12, 0) AS STDPorcPesoSem12G,
    COALESCE(B.STDPorcPesoSem13, 0) AS STDPorcPesoSem13G,
    COALESCE(B.STDPorcPesoSem14, 0) AS STDPorcPesoSem14G,
    COALESCE(B.STDPorcPesoSem15, 0) AS STDPorcPesoSem15G,
    COALESCE(B.STDPorcPesoSem16, 0) AS STDPorcPesoSem16G,
    COALESCE(B.STDPorcPesoSem17, 0) AS STDPorcPesoSem17G,
    COALESCE(B.STDPorcPesoSem18, 0) AS STDPorcPesoSem18G,
    COALESCE(B.STDPorcPesoSem19, 0) AS STDPorcPesoSem19G,
    COALESCE(B.STDPorcPesoSem20, 0) AS STDPorcPesoSem20G,
    COALESCE(B.STDPorcPeso5Dias, 0) AS STDPorcPeso5DiasG, 
    COALESCE(C.STDPorcPesoSem1, 0) AS STDPorcPesoSem1L,
    COALESCE(C.STDPorcPesoSem2, 0) AS STDPorcPesoSem2L,
    COALESCE(C.STDPorcPesoSem3, 0) AS STDPorcPesoSem3L,
    COALESCE(C.STDPorcPesoSem4, 0) AS STDPorcPesoSem4L,
    COALESCE(C.STDPorcPesoSem5, 0) AS STDPorcPesoSem5L,
    COALESCE(C.STDPorcPesoSem6, 0) AS STDPorcPesoSem6L,
    COALESCE(C.STDPorcPesoSem7, 0) AS STDPorcPesoSem7L,
    COALESCE(C.STDPorcPesoSem8, 0) AS STDPorcPesoSem8L,
    COALESCE(C.STDPorcPesoSem9, 0) AS STDPorcPesoSem9L,
    COALESCE(C.STDPorcPesoSem10, 0) AS STDPorcPesoSem10L,
    COALESCE(C.STDPorcPesoSem11, 0) AS STDPorcPesoSem11L,
    COALESCE(C.STDPorcPesoSem12, 0) AS STDPorcPesoSem12L,
    COALESCE(C.STDPorcPesoSem13, 0) AS STDPorcPesoSem13L,
    COALESCE(C.STDPorcPesoSem14, 0) AS STDPorcPesoSem14L,
    COALESCE(C.STDPorcPesoSem15, 0) AS STDPorcPesoSem15L,
    COALESCE(C.STDPorcPesoSem16, 0) AS STDPorcPesoSem16L,
    COALESCE(C.STDPorcPesoSem17, 0) AS STDPorcPesoSem17L,
    COALESCE(C.STDPorcPesoSem18, 0) AS STDPorcPesoSem18L,
    COALESCE(C.STDPorcPesoSem19, 0) AS STDPorcPesoSem19L,
    COALESCE(C.STDPorcPesoSem20, 0) AS STDPorcPesoSem20L,
    COALESCE(C.STDPorcPeso5Dias, 0) AS STDPorcPeso5DiasL
FROM {database_name}.STDPorcPesoXPobInicial A
LEFT JOIN aggregated_weights B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon
LEFT JOIN aggregated_weights C ON A.pk_plantel = C.pk_plantel AND A.pk_lote = C.pk_lote
ORDER BY A.ComplexEntityNo """
)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPesoPond"
}
df_STDPesoPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPesoPond")
print('carga STDPesoPond')
#Se muestra el DescripTipoAlimentoXTipoProducto
df_DescripTipoAlimentoXTipoProducto = spark.sql(f"select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, B.grupoproducto, \
CASE WHEN A.pk_diasvida <= 8 THEN 'PREINICIO' \
WHEN (A.pk_diasvida >= 9 AND  A.pk_diasvida <= 18) THEN 'INICIO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 32) AND B.grupoproducto = 'VIVO' THEN 'ACABADO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 28) AND B.grupoproducto = 'BENEFICIO' THEN 'ACABADO' \
WHEN  A.pk_diasvida >= 33 AND B.grupoproducto = 'VIVO' THEN 'FINALIZADOR' \
WHEN  A.pk_diasvida >= 29 AND B.grupoproducto = 'BENEFICIO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto \
from {database_name}.ProduccionDetalle A \
left join {database_name}.lk_producto B on A.pk_producto = B.pk_producto \
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard \
where A.pk_empresa = 1 and A.pk_division = 3 and A.GRN = 'P' and C.nstandard not in ('DINUT - VERANO', 'DINUT - INVIERNO') \
and (date_format(CAST(A.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(A.fecha AS timestamp),'yyyyMM') <= {AnioMesFin}) \
union \
select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, B.grupoproducto, \
CASE WHEN A.pk_diasvida <= 8 THEN 'PREINICIO' \
WHEN (A.pk_diasvida >= 9  AND A.pk_diasvida <= 18) THEN 'INICIO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 34) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - VERANO' THEN 'ACABADO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 32) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'ACABADO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 30) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - VERANO' THEN 'ACABADO' \
WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 29) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'ACABADO' \
WHEN (A.pk_diasvida >= 35 AND A.pk_diasvida <= 42) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - VERANO' THEN 'FINALIZADOR' \
WHEN (A.pk_diasvida >= 33 AND A.pk_diasvida <= 40) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'FINALIZADOR' \
WHEN (A.pk_diasvida >= 31 AND A.pk_diasvida <= 38) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - VERANO' THEN 'FINALIZADOR' \
WHEN (A.pk_diasvida >= 30 AND A.pk_diasvida <= 37) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto \
from {database_name}.ProduccionDetalle A \
left join {database_name}.lk_producto B on A.pk_producto = B.pk_producto \
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard \
where A.pk_empresa = 1 and A.pk_division = 3 and A.GRN = 'P' and C.nstandard in ('DINUT - VERANO', 'DINUT - INVIERNO') and C.SpeciesType = '1' \
and (date_format(CAST(A.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(A.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DescripTipoAlimentoXTipoProducto"
}
df_DescripTipoAlimentoXTipoProducto.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DescripTipoAlimentoXTipoProducto")
print('carga DescripTipoAlimentoXTipoProducto')
#Se muestra el STDConsDiaNuevo
df_STDConsDiaNuevo = spark.sql(f"select A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto, SUM(A.STDConsDia) STDConsDia,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3)) ComplexEntityNoGalpon \
from ( select A.ComplexEntityNo, A.pk_diasvida, MAX(A.STDConsDia) STDConsDia \
from {database_name}.ProduccionDetalle A \
where A.ComplexEntityNo in (select ComplexEntityNo from {database_name}.ft_alojamiento where participacionhm <> 0) and pk_empresa = 1 and pk_division = 3 and GRN = 'P' \
and (date_format(CAST(A.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(A.fecha AS timestamp),'yyyyMM') <= {AnioMesFin}) \
group by A.ComplexEntityNo, A.pk_diasvida) A \
left join {database_name}.DescripTipoAlimentoXTipoProducto B on A.ComplexEntityNo = B.ComplexEntityNo and A.pk_diasvida = B.pk_diasvida \
group by A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3))")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDConsDiaNuevo"
}
df_STDConsDiaNuevo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDConsDiaNuevo")
print('carga STDConsDiaNuevo')
#Inserta los datos en la tabla _Produccion
df_Produccion = spark.sql(f" SELECT PD.fecha \
		,PD.pk_empresa  \
		,COALESCE(PD.pk_division,6) pk_division \
		,COALESCE(PD.pk_zona,1) pk_zona \
		,COALESCE(PD.pk_subzona,75) pk_subzona \
		,COALESCE(PD.pk_plantel,391) pk_plantel \
		,COALESCE(PD.pk_lote,0) pk_lote \
		,COALESCE(PD.pk_galpon,2640) pk_galpon \
		,COALESCE(PD.pk_sexo,4) pk_sexo \
		,COALESCE(PD.pk_standard,105) pk_standard \
		,COALESCE(PD.pk_producto,2401) pk_producto \
		,PD.pk_tipoproducto \
		,COALESCE(PD.pk_grupoconsumo,12) pk_grupoconsumo \
		,COALESCE(LEP.pk_especie,29)pk_especie \
		,PD.pk_estado \
		,COALESCE(PD.pk_administrador,70) pk_administrador \
		,COALESCE(PD.pk_proveedor,1) pk_proveedor \
		,PD.pk_semanavida \
		,PD.pk_diasvida \
		,COALESCE(PD.pk_alimento,77) pk_alimento \
		,PD.ComplexEntityNo \
		,PD.FechaNacimiento \
		,PD.Edad \
		,PD.FechaCierre \
		,IC.Fecha FechaAlojamiento \
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
		,PED.Peso  \
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
		,PED.pesohvopond PesoHvo \
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
		,MD.PorcMortDiaAcum  \
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
FROM {database_name}.ProduccionDetalle PD \
LEFT JOIN {database_name}.ft_ingresocons IC ON PD.complexentityno = IC.complexentityno \
LEFT JOIN {database_name}.ft_mortalidad_diario MD ON PD.ComplexEntityNo = MD.ComplexEntityNo AND PD.pk_diasvida = MD.pk_diasvida AND MD.flagartificio = 1 \
LEFT JOIN {database_name}.ft_peso_diario PED ON PD.ComplexEntityNo = PED.ComplexEntityNo AND PD.pk_diasvida = PED.pk_diasvida \
LEFT JOIN {database_name}.lk_alimento FAL ON PD.pk_alimento = FAL.pk_alimento \
LEFT JOIN (SELECT complexentityno, MIN(fecha) fecha, MIN(avesgranjaacum)avesgranjaacum, MIN(edadgranja)edadgranja,MIN(edadgalpon)edadgalpon, MIN(edadlote) edadlote, MIN(edadiniciosaca) edadiniciosaca,COUNT(distinct fecha) DiasSacaEfectivo FROM {database_name}.ft_ventas_CD GROUP BY complexentityno) VE ON PD.complexentityno = VE.complexentityno  \
LEFT JOIN {database_name}.ft_consumogascamaagua CS ON CS.ComplexEntityNo = SUBSTRING(PD.ComplexEntityNo,1,9) \
LEFT JOIN {database_name}.DiasCampana DC ON CAST(DC.ProteinEntitiesIRN AS VARCHAR(50)) = CAST(PD.ProteinEntitiesIRN AS VARCHAR(50)) AND DC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name}.AlojamientoMayor AM ON PD.complexentityno = AM.complexentityno \
LEFT JOIN {database_name}.EdadPadre EP ON PD.complexentityno = EP.complexentityno \
LEFT JOIN {database_name}.STDPond STDP ON PD.pk_plantel = STDP.pk_plantel AND PD.pk_lote = STDP.pk_lote AND PD.pk_galpon = STDP.pk_galpon AND PD.pk_sexo =STDP.pk_sexo \
LEFT JOIN {database_name}.STDMortPond STDMP ON PD.pk_plantel = STDMP.pk_plantel AND PD.pk_lote = STDMP.pk_lote AND PD.pk_galpon = STDMP.pk_galpon AND PD.pk_sexo =STDMP.pk_sexo \
LEFT JOIN {database_name}.STDPesoPond STDPP ON PD.pk_plantel = STDPP.pk_plantel AND PD.pk_lote = STDPP.pk_lote AND PD.pk_galpon = STDPP.pk_galpon AND PD.pk_sexo =STDPP.pk_sexo \
LEFT JOIN {database_name}.acumuladoSaca ASA ON PD.ComplexEntityNo = ASA.ComplexEntityNo and PD.fecha = ASA.fecha \
LEFT JOIN {database_name}.lk_especie LEP ON LEP.cespecie = AM.RazaMayor \
LEFT JOIN {database_name}.DescripTipoAlimentoXTipoProducto DTA on PD.ComplexEntityNo = DTA.ComplexEntityNo and PD.pk_diasvida = DTA.pk_diasvida \
LEFT JOIN {database_name}.STDConsDiaNuevo STDCN on substring(DTA.complexentityno,1,(LENGTH(DTA.complexentityno)-3)) = STDCN.ComplexEntityNoGalpon and DTA.DescripTipoAlimentoXTipoProducto = STDCN.DescripTipoAlimentoXTipoProducto \
LEFT JOIN {database_name}.ConsumosXTipoAlimento CXTA ON PD.complexEntityNo = CXTA.ComplexEntityNo and PD.pk_diasvida = CXTA.pk_diasvida and PD.pk_alimento = CXTA.pk_alimento \
WHERE (date_format(CAST(PD.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(PD.fecha AS timestamp),'yyyyMM') <= {AnioMesFin}) \
AND PD.pk_diasvida >= 0  \
AND PD.GRN = 'p' AND PD.pk_division = 3 \
AND PD.pk_plantel NOT IN (72,82)  \
AND SUBSTRING(PD.ComplexEntityNo,8,2) IN ('01','02','03','04','05','06','07','08','09','10') \
union all \
SELECT PD.fecha \
		,PD.pk_empresa \
		,COALESCE(PD.pk_division,6) pk_division \
		,COALESCE(PD.pk_zona,1) pk_zona \
		,COALESCE(PD.pk_subzona,75) pk_subzona \
		,COALESCE(PD.pk_plantel,391) pk_plantel \
		,COALESCE(PD.pk_lote,0) pk_lote \
		,COALESCE(PD.pk_galpon,2640) pk_galpon \
		,COALESCE(PD.pk_sexo,4) pk_sexo \
		,COALESCE(PD.pk_standard,105) pk_standard \
		,COALESCE(PD.pk_producto,2401) pk_producto \
		,PD.pk_tipoproducto \
		,COALESCE(PD.pk_grupoconsumo,12) pk_grupoconsumo \
		,COALESCE(PED.pk_especie,29) pk_especie \
		,PD.pk_estado \
		,COALESCE(PD.pk_administrador,70) pk_administrador \
		,COALESCE(PD.pk_proveedor,1) pk_proveedor \
		,PD.pk_semanavida \
		,PD.pk_diasvida \
		,COALESCE(PD.pk_alimento,77) pk_alimento \
		,PD.ComplexEntityNo \
		,PD.FechaNacimiento \
		,PD.Edad \
		,PD.FechaCierre \
		,IC.Fecha FechaAlojamiento \
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
		,PED.pesohvopond PesoHvo \
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
FROM {database_name}.ProduccionDetalle PD \
LEFT JOIN {database_name}.ft_ingresocons IC ON PD.complexentityno = IC.complexentityno \
LEFT JOIN {database_name}.ft_mortalidad_Diario MD ON PD.ComplexEntityNo = MD.ComplexEntityNo AND PD.pk_diasvida = MD.pk_diasvida AND MD.flagartificio = 1 \
LEFT JOIN {database_name}.ft_peso_Diario PED ON PD.ComplexEntityNo = PED.ComplexEntityNo AND PD.pk_diasvida = PED.pk_diasvida \
LEFT JOIN {database_name}.lk_alimento FAL ON PD.pk_alimento = FAL.pk_alimento \
LEFT JOIN (SELECT complexentityno, MIN(fecha)fecha, MIN(avesgranjaacum)avesgranjaacum, MIN(edadgranja)edadgranja,MIN(edadgalpon)edadgalpon, MIN(edadlote) edadlote, MIN(edadiniciosaca) edadiniciosaca,COUNT(distinct fecha) DiasSacaEfectivo FROM {database_name}.ft_ventas_CD GROUP BY complexentityno) VE ON PD.complexentityno = VE.complexentityno  \
LEFT JOIN {database_name}.ft_consumogascamaagua CS ON CS.ComplexEntityNo = substring(PD.ComplexEntityNo,1,(LENGTH(PD.ComplexEntityNo)-6)) \
LEFT JOIN {database_name}.DiasCampana DC ON CAST(DC.ProteinEntitiesIRN AS VARCHAR(50)) = CAST(PD.ProteinEntitiesIRN AS VARCHAR(50)) AND DC.ComplexEntityNo = PD.ComplexEntityNo \
LEFT JOIN {database_name}.AlojamientoMayor AM ON PD.complexentityno = AM.complexentityno \
LEFT JOIN {database_name}.EdadPadre EP ON PD.complexentityno = EP.complexentityno \
LEFT JOIN {database_name}.STDPond STDP ON PD.pk_plantel = STDP.pk_plantel AND PD.pk_lote = STDP.pk_lote AND PD.pk_galpon = STDP.pk_galpon AND PD.pk_sexo =STDP.pk_sexo \
LEFT JOIN {database_name}.STDMortPond STDMP ON PD.pk_plantel = STDMP.pk_plantel AND PD.pk_lote = STDMP.pk_lote AND PD.pk_galpon = STDMP.pk_galpon AND PD.pk_sexo =STDMP.pk_sexo \
LEFT JOIN {database_name}.STDPesoPond STDPP ON PD.pk_plantel = STDPP.pk_plantel AND PD.pk_lote = STDPP.pk_lote AND PD.pk_galpon = STDPP.pk_galpon AND PD.pk_sexo =STDPP.pk_sexo \
left join {database_name}.acumuladoSaca ASA ON PD.ComplexEntityNo = ASA.ComplexEntityNo and PD.fecha = ASA.fecha \
LEFT JOIN {database_name}.ConsumosXTipoAlimento CXTA ON PD.complexEntityNo = CXTA.ComplexEntityNo and PD.pk_diasvida = CXTA.pk_diasvida and PD.pk_alimento = CXTA.pk_alimento \
WHERE (date_format(CAST(PD.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(PD.fecha AS timestamp),'yyyyMM') <= {AnioMesFin}) \
AND PD.pk_diasvida >= 0  \
AND PD.GRN = 'p'  \
AND PD.pk_division = 4 \
")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Produccion"
}
df_Produccion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Produccion")
print('carga Produccion')                           
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
    ,SUM(ConsDia) ConsDia,CASE WHEN A.pk_diasvida<= 42 THEN MAX(STDConsDia) ELSE 0 END STDPorcConsDia
    ,((CASE WHEN A.pk_diasvida<= 42 THEN MAX(STDConsDia) ELSE 0 END)*MAX(AvesRendidas))STDConsDia
    from {database_name}.produccion A where A.pk_empresa = 1 
    group by A.pk_semanavida,A.pk_diasvida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo) A
group by A.pk_semanavida,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.ComplexEntityNo""" )

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcConsDia"
}
df_STDPorcConsDia.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPorcConsDia")
print('carga STDPorcConsDia')
#Se muestra el STDPorcConsDiaHorizontal
df_STDPorcConsDiaHorizontal = spark.sql(f"""
WITH aggregated_data AS (
    SELECT
        ComplexEntityNo,
        MAX(CASE WHEN pk_semanavida = 1 THEN STDPorcConsSem END) AS STDPorcConsSem1,
        MAX(CASE WHEN pk_semanavida = 2 THEN STDPorcConsSem END) AS STDPorcConsSem2,
        MAX(CASE WHEN pk_semanavida = 3 THEN STDPorcConsSem END) AS STDPorcConsSem3,
        MAX(CASE WHEN pk_semanavida = 4 THEN STDPorcConsSem END) AS STDPorcConsSem4,
        MAX(CASE WHEN pk_semanavida = 5 THEN STDPorcConsSem END) AS STDPorcConsSem5,
        MAX(CASE WHEN pk_semanavida = 6 THEN STDPorcConsSem END) AS STDPorcConsSem6,
        MAX(CASE WHEN pk_semanavida = 7 THEN STDPorcConsSem END) AS STDPorcConsSem7,
        MAX(CASE WHEN pk_semanavida = 1 THEN STDConsSem END) AS STDConsSem1,
        MAX(CASE WHEN pk_semanavida = 2 THEN STDConsSem END) AS STDConsSem2,
        MAX(CASE WHEN pk_semanavida = 3 THEN STDConsSem END) AS STDConsSem3,
        MAX(CASE WHEN pk_semanavida = 4 THEN STDConsSem END) AS STDConsSem4,
        MAX(CASE WHEN pk_semanavida = 5 THEN STDConsSem END) AS STDConsSem5,
        MAX(CASE WHEN pk_semanavida = 6 THEN STDConsSem END) AS STDConsSem6,
        MAX(CASE WHEN pk_semanavida = 7 THEN STDConsSem END) AS STDConsSem7
    FROM {database_name}.STDPorcConsDia
    GROUP BY ComplexEntityNo
)

SELECT 
    A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo, 
    MAX(A.AvesRendidas) AS AvesRendidas,
    COALESCE(B.STDPorcConsSem1, 0) AS STDPorcConsSem1,
    COALESCE(B.STDPorcConsSem2, 0) AS STDPorcConsSem2,
    COALESCE(B.STDPorcConsSem3, 0) AS STDPorcConsSem3,
    COALESCE(B.STDPorcConsSem4, 0) AS STDPorcConsSem4,
    COALESCE(B.STDPorcConsSem5, 0) AS STDPorcConsSem5,
    COALESCE(B.STDPorcConsSem6, 0) AS STDPorcConsSem6,
    COALESCE(B.STDPorcConsSem7, 0) AS STDPorcConsSem7,
    COALESCE(B.STDConsSem1, 0) AS STDConsSem1,
    COALESCE(B.STDConsSem2, 0) AS STDConsSem2,
    COALESCE(B.STDConsSem3, 0) AS STDConsSem3,
    COALESCE(B.STDConsSem4, 0) AS STDConsSem4,
    COALESCE(B.STDConsSem5, 0) AS STDConsSem5,
    COALESCE(B.STDConsSem6, 0) AS STDConsSem6,
    COALESCE(B.STDConsSem7, 0) AS STDConsSem7
FROM {database_name}.STDPorcConsDia A
LEFT JOIN aggregated_data B ON A.ComplexEntityNo = B.ComplexEntityNo
GROUP BY A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.ComplexEntityNo, 
B.STDPorcConsSem1,B.STDPorcConsSem2,B.STDPorcConsSem3,B.STDPorcConsSem4,B.STDPorcConsSem5,B.STDPorcConsSem6,B.STDPorcConsSem7,
B.STDConsSem1,B.STDConsSem2,B.STDConsSem3,B.STDConsSem4,B.STDConsSem5,B.STDConsSem6,B.STDConsSem7
""" )

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDPorcConsDiaHorizontal"
}
df_STDPorcConsDiaHorizontal.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDPorcConsDiaHorizontal")
print('carga STDPorcConsDiaHorizontal')
#Se muestra produccion
df_ALTERTABLEproduccion = spark.sql(f"""
alter table {database_name}.produccion
ADD COLUMNS ( STDPorcConsSem1 numeric(12,3),STDPorcConsSem2 numeric(12,3),STDPorcConsSem3 numeric(12,3),STDPorcConsSem4 numeric(12,3),STDPorcConsSem5 numeric(12,3),STDPorcConsSem6 numeric(12,3),STDPorcConsSem7 numeric(12,3)
    ,STDConsSem1 numeric(12,3),STDConsSem2 numeric(12,3),STDConsSem3 numeric(12,3),STDConsSem4 numeric(12,3),STDConsSem5 numeric(12,3),STDConsSem6 numeric(12,3),STDConsSem7 numeric(12,3)
    ,STDConsSem numeric(12,3),STDPorcConsSem numeric(12,3),ConsSem numeric(12,3))""" )
print('carga df_ALTERTABLEproduccion')

# 1️⃣ Crear DataFrame con datos actualizados
df_UPDATEproduccion = spark.sql(f"""
SELECT A.fecha
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
,COALESCE(B.STDPorcConsSem1, A.STDPorcConsSem1) AS STDPorcConsSem1
,COALESCE(B.STDPorcConsSem2, A.STDPorcConsSem2) AS STDPorcConsSem2
,COALESCE(B.STDPorcConsSem3, A.STDPorcConsSem3) AS STDPorcConsSem3
,COALESCE(B.STDPorcConsSem4, A.STDPorcConsSem4) AS STDPorcConsSem4
,COALESCE(B.STDPorcConsSem5, A.STDPorcConsSem5) AS STDPorcConsSem5
,COALESCE(B.STDPorcConsSem6, A.STDPorcConsSem6) AS STDPorcConsSem6
,COALESCE(B.STDPorcConsSem7, A.STDPorcConsSem7) AS STDPorcConsSem7
,COALESCE(B.STDConsSem1, A.STDConsSem1) AS STDConsSem1
,COALESCE(B.STDConsSem2, A.STDConsSem2) AS STDConsSem2
,COALESCE(B.STDConsSem3, A.STDConsSem3) AS STDConsSem3
,COALESCE(B.STDConsSem4, A.STDConsSem4) AS STDConsSem4
,COALESCE(B.STDConsSem5, A.STDConsSem5) AS STDConsSem5
,COALESCE(B.STDConsSem6, A.STDConsSem6) AS STDConsSem6
,COALESCE(B.STDConsSem7, A.STDConsSem7) AS STDConsSem7
,'' stdconssem
,'' stdporcconssem
,'' conssem
FROM {database_name}.produccion A
LEFT JOIN {database_name}.STDPorcConsDiaHorizontal B 
ON A.pk_plantel = B.pk_plantel 
AND A.pk_lote = B.pk_lote 
AND A.pk_galpon = B.pk_galpon 
AND A.pk_sexo = B.pk_sexo
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/produccion_nueva"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDATEproduccion.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.produccion_nueva")

df_produccion_nueva = spark.sql(f"""SELECT * from {database_name}.produccion_nueva """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.produccion")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
#spark.sql(f"""CREATE TABLE {database_name}.produccion AS SELECT * FROM {database_name}.produccion_nueva""")
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/produccion"
}
df_produccion_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.produccion")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.produccion_nueva")

print('carga produccion')
# 1️⃣ Crear DataFrame con datos actualizados
df_UPDATEproduccion = spark.sql(f"""
SELECT A.fecha
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
FROM {database_name}.produccion A
LEFT JOIN {database_name}.STDPorcConsDia B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote AND A.pk_galpon = B.pk_galpon AND A.pk_sexo =B.pk_sexo AND A.pk_semanavida = B.pk_semanavida
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/produccion_nueva"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDATEproduccion.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.produccion_nueva")

df_produccion_nueva = spark.sql(f"""SELECT * from {database_name}.produccion_nueva """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.produccion")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
#spark.sql(f"""CREATE TABLE {database_name}.produccion AS SELECT * FROM {database_name}.produccion_nueva""")
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/produccion"
}
df_produccion_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.produccion")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es necesaria
spark.sql(f"DROP TABLE IF EXISTS {database_name}.produccion_nueva")

print('carga produccion')
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
FROM {database_name}.Produccion A
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
    .saveAsTable(f"{database_name}.ProduccionDetalleEdad")
print('carga ProduccionDetalleEdad')   
#Se muestra OrdenarMortalidadesCorral
df_OrdenarMortalidadesCorral = spark.sql(f"""
    SELECT 
        fecha, pk_empresa, pk_division, ComplexEntityNo, 
        causa, cmortalidad, 
        ROUND((cmortalidad / NULLIF(PobInicial * 1.0, 0) * 100), 5) AS pcmortalidad,
        ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) AS orden,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'CantPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'CantSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'CantTerLesion'
        END AS OrdenCantidad,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'NomPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'NomSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'NomTerLesion'
        END AS OrdenNombre,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'PorcPrimLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'PorcSegLesion'
            WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'PorcTerLesion'
        END AS OrdenPorcentaje
    FROM {database_name}.ft_mortalidad_corral
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
    AND (date_format(CAST(fecha AS timestamp), 'yyyyMM') >= {AnioMes} 
    AND date_format(CAST(fecha AS timestamp), 'yyyyMM') <= {AnioMesFin})
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesCorral"
}
df_OrdenarMortalidadesCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidadesCorral")
print('carga OrdenarMortalidadesCorral')   
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
FROM {database_name}.OrdenarMortalidadesCorral
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
    .saveAsTable(f"{database_name}.OrdenarMortalidadesCorral2")
print('carga OrdenarMortalidadesCorral2')   
#Se muestra OrdenarMortalidadesCorral
df_OrdenarMortalidadesCorral = spark.sql(f"""
select A.ComplexEntityNo,A.pk_semanavida,A.nproductoconsumo,MAX(ROW_NUMBER) ROW_NUMBER
from (select A.fecha,A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo,ROW_NUMBER() OVER (PARTITION BY A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo ORDER BY A.fecha) ROW_NUMBER
		from {database_name}.ProduccionDetalleEdad A
		left join {database_name}.ft_consumos B on A.complexentityno = b. complexentityno and a.pk_diasvida = b.pk_diasvida
		left join {database_name}.lk_productoconsumo c on b.pk_productoconsumo = c.pk_productoconsumo
		where C.pk_grupoconsumo = 10 and A.pk_division = 3 and A.pk_empresa = 1 ) A
GROUP BY A.ComplexEntityNo,A.pk_semanavida,A.nproductoconsumo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesCorral"
}
df_OrdenarMortalidadesCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidadesCorral")
print('carga OrdenarMortalidadesCorral')   
#Se muestra ProductoConsumo1
df_ProductoConsumo1 = spark.sql(f"""
select A.ComplexEntityNo,A.pk_semanavida,A.nproductoconsumo,MAX(ROW_NUMBER) ROW_NUMBER
from (select A.fecha,A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo,ROW_NUMBER() OVER (PARTITION BY A.ComplexEntityNo,A.pk_semanavida,C.nproductoconsumo ORDER BY A.fecha) ROW_NUMBER
		from {database_name}.ProduccionDetalleEdad A
		left join {database_name}.ft_consumos B on A.complexentityno = b. complexentityno and a.pk_diasvida = b.pk_diasvida
		left join {database_name}.lk_productoconsumo c on b.pk_productoconsumo = c.pk_productoconsumo
		where C.pk_grupoconsumo = 10 and A.pk_division = 3 and A.pk_empresa = 1 ) A
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
    .saveAsTable(f"{database_name}.ProductoConsumo1")
print('carga ProductoConsumo1')   
#Se muestra ProductoConsumo2
df_ProductoConsumo2 = spark.sql(f"""
WITH RankedData AS (
    SELECT ComplexEntityNo,pk_semanavida,nproductoconsumo,ROW_NUMBER() OVER (PARTITION BY ComplexEntityNo, pk_semanavida ORDER BY nproductoconsumo) AS row_num
    FROM {database_name}.ProductoConsumo1
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
    .saveAsTable(f"{database_name}.ProductoConsumo2")
print('carga ProductoConsumo2')   
#Se muestra Medicaciones
df_Medicaciones = spark.sql(f"""
WITH ProductoConsumo AS (SELECT ComplexEntityNo,pk_semanavida,MedSem,DiasMedSem FROM {database_name}.ProductoConsumo2)
SELECT A.ComplexEntityNo,
    MAX(CASE WHEN A.pk_semanavida = 1 THEN A.MedSem END) AS MedSem1,
    MAX(CASE WHEN A.pk_semanavida = 2 THEN A.MedSem END) AS MedSem2,
    MAX(CASE WHEN A.pk_semanavida = 3 THEN A.MedSem END) AS MedSem3,
    MAX(CASE WHEN A.pk_semanavida = 4 THEN A.MedSem END) AS MedSem4,
    MAX(CASE WHEN A.pk_semanavida = 5 THEN A.MedSem END) AS MedSem5,
    MAX(CASE WHEN A.pk_semanavida = 6 THEN A.MedSem END) AS MedSem6,
    MAX(CASE WHEN A.pk_semanavida = 7 THEN A.MedSem END) AS MedSem7,
    MAX(CASE WHEN B.pk_semanavida = 1 THEN B.DiasMedSem END) AS DiasMedSem1,
    MAX(CASE WHEN B.pk_semanavida = 2 THEN B.DiasMedSem END) AS DiasMedSem2,
    MAX(CASE WHEN B.pk_semanavida = 3 THEN B.DiasMedSem END) AS DiasMedSem3,
    MAX(CASE WHEN B.pk_semanavida = 4 THEN B.DiasMedSem END) AS DiasMedSem4,
    MAX(CASE WHEN B.pk_semanavida = 5 THEN B.DiasMedSem END) AS DiasMedSem5,
    MAX(CASE WHEN B.pk_semanavida = 6 THEN B.DiasMedSem END) AS DiasMedSem6,
    MAX(CASE WHEN B.pk_semanavida = 7 THEN B.DiasMedSem END) AS DiasMedSem7
FROM {database_name}.ProductoConsumo2 A
LEFT JOIN {database_name}.ProductoConsumo2 B 
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
    .saveAsTable(f"{database_name}.Medicaciones")
print('carga Medicaciones')   
#Se muestra ProduccionDetalleCorral
df_ProduccionDetalleCorral = spark.sql(f"""
WITH ProduccionCierre AS (SELECT pk_lote,MAX(FechaCierre) AS FechaCierreLote FROM {database_name}.ProduccionDetalleEdad GROUP BY pk_lote),
RuidosTotales AS (SELECT ComplexEntityNo,COUNT(U_RuidosTotales) AS CountRuidosTotales FROM {database_name}.ProduccionDetalleEdad WHERE U_RuidosTotales <> 0 GROUP BY ComplexEntityNo)

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
		,CASE WHEN AVG(AvesRendidas) = 0 THEN 0.0 ELSE (AVG(KilosRendidos)/AVG(AvesRendidas)) END AS PesoProm
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
		,CASE WHEN AVG(KilosRendidos) = 0 THEN 0.0 ELSE (SUM(ConsDia)/AVG(KilosRendidos)) END AS ICA
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
FROM {database_name}.ProduccionDetalleEdad B
left join {database_name}.OrdenarMortalidadesCorral2 C on B.ComplexEntityNo = C.ComplexEntityNo
LEFT JOIN {database_name}.ft_mortalidad_Corral D on B.ComplexEntityNo = D.ComplexEntityNo and B.FlagAtipico = D.FlagAtipico 
left join {database_name}.Medicaciones E on B.ComplexEntityNo = E.ComplexEntityNo
LEFT JOIN ProduccionCierre P ON B.pk_lote = P.pk_lote
LEFT JOIN RuidosTotales R ON B.ComplexEntityNo = R.ComplexEntityNo
GROUP BY B.pk_empresa,B.pk_division,B.pk_zona,B.pk_subzona,B.pk_plantel,B.pk_lote,B.pk_galpon,B.pk_sexo,B.pk_standard,B.pk_producto,B.pk_tipoproducto
,B.pk_especie,B.pk_estado,B.pk_administrador,B.ComplexEntityNo,B.FechaNacimiento
,B.FechaCierre,B.FechaAlojamiento,B.FechaCrianza,B.FechaInicioGranja,B.FechaInicioSaca
,B.FechaFinSaca,B.PadreMayor,B.RazaMayor,B.IncubadoraMayor,B.categoria,B.EdadPadreCorralDescrip,R.CountRuidosTotales,P.FechaCierreLote
,NomPrimLesion,NomSegLesion,NomTerLesion,MedSem1,MedSem2,MedSem3,MedSem4,MedSem5,MedSem6,MedSem7,B.TipoOrigen,B.pk_tipogranja
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleCorral"
}
df_ProduccionDetalleCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ProduccionDetalleCorral")
print('carga ProduccionDetalleCorral')   
#Se muestra ProduccionDetalleGalpon
df_ProduccionDetalleGalpon = spark.sql(f"""
WITH PobInicialSuma AS (SELECT pk_lote,pk_galpon,SUM(PobInicial) AS PobInicialTotal FROM {database_name}.ProduccionDetalleCorral GROUP BY pk_lote, pk_galpon),
PobotroSuma AS (SELECT pk_lote,pk_galpon,SUM(PesoAloXPobInicial) AS PesoAloXPobInicial,SUM(PesoHvoXPobInicial) AS PesoHvoXPobInicial,SUM(PobInicialXSTDMortAcumC) AS PobInicialXSTDMortAcumC FROM {database_name}.ProduccionDetalleCorral GROUP BY pk_lote, pk_galpon),
PorcNoViable AS (SELECT pk_lote,pk_galpon, 
        SUM(PorcNoViableSem1XPobInicial) AS PorcNoViableSem1XPobInicial,
        SUM(PorcNoViableSem2XPobInicial) AS PorcNoViableSem2XPobInicial,
        SUM(PorcNoViableSem3XPobInicial) AS PorcNoViableSem3XPobInicial,
        SUM(PorcNoViableSem4XPobInicial) AS PorcNoViableSem4XPobInicial,
        SUM(PorcNoViableSem5XPobInicial) AS PorcNoViableSem5XPobInicial,
        SUM(PorcNoViableSem6XPobInicial) AS PorcNoViableSem6XPobInicial,
        SUM(PorcNoViableSem7XPobInicial) AS PorcNoViableSem7XPobInicial,
        SUM(PorcNoViableSem8XPobInicial) AS PorcNoViableSem8XPobInicial
    FROM {database_name}.ProduccionDetalleCorral
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
    FROM {database_name}.ProduccionDetalleCorral
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
		,clote + '-' + nogalpon AS ComplexEntityNoGalpon
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
		,CASE WHEN SUM(PobInicial) = 0 THEN 0.0 ELSE (((SUM(MortDia) / SUM(PobInicial*1.0))*100)) END AS PorMort
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
		,case when sum(AvesRendidas) = 0 then 0.0 else (sum(KilosRendidos)/sum(AvesRendidas)) end as PesoProm
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
		,case when sum(KilosRendidos) = 0 then 0.0 else (sum(ConsDia)/sum(KilosRendidos)) end as ICA
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
from {database_name}.ProduccionDetalleCorral A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name}.lk_galpon C on A.pk_galpon = C.pk_galpon
left join {database_name}.ft_consumogascamaagua D on B.clote + '-' + c.nogalpon = D.ComplexEntityNo
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

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleGalpon"
}
df_ProduccionDetalleGalpon.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ProduccionDetalleGalpon")
print('carga ProduccionDetalleGalpon')   
#Se muestra OrdenId
df_OrdenId = spark.sql(f"""
select clote,pk_especie,pk_tipoproducto,sum(AvesRendidas) AvesRendidas,DENSE_RANK() OVER (PARTITION BY clote ORDER BY sum(AvesRendidas) asc) as orden
from {database_name}.ProduccionDetalleGalpon A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
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
    .saveAsTable(f"{database_name}.OrdenId")
print('carga OrdenId')   
#Se muestra PorcPoblacion
df_PorcPoblacion = spark.sql(f"""
select A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo,A.pk_standard,C.lstandard,A.ComplexEntityNo,MAX(A.PobInicial)PobInicial,MAX(B.PesoAlo) PesoAlo
from {database_name}.ft_consolidado_Corral A
left join {database_name}.ft_peso_diario B on A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_galpon = B.pk_galpon and A.pk_sexo = B.pk_sexo
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard
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
    .saveAsTable(f"{database_name}.PorcPoblacion")
print('carga PorcPoblacion')   
#Se muestra CalculosPorcPoblacion
df_CalculosPorcPoblacion = spark.sql(f"""
WITH PobLote AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialLote FROM {database_name}.PorcPoblacion GROUP BY pk_plantel, pk_lote),
PobJoven AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialJoven FROM {database_name}.PorcPoblacion WHERE lstandard = 'Joven' GROUP BY pk_plantel, pk_lote),
PobPesoMenor AS (SELECT pk_plantel,pk_lote,SUM(PobInicial) AS PobInicialPesoMenor FROM {database_name}.PorcPoblacion WHERE PesoAlo <= 0.033 GROUP BY pk_plantel, pk_lote),
CantidadGalpon AS (SELECT pk_plantel,pk_lote,COUNT(DISTINCT pk_galpon) AS CantidadGalpon FROM {database_name}.PorcPoblacion GROUP BY pk_plantel, pk_lote),
GalponMixtoD AS (SELECT pk_plantel,pk_lote,pk_galpon,SUM(pk_sexo) AS TotalSexo FROM {database_name}.PorcPoblacion GROUP BY pk_plantel, pk_lote, pk_galpon),
GalponMixtoFinal AS (SELECT pk_plantel, pk_lote, pk_galpon,CASE WHEN TotalSexo = 3 THEN 1 ELSE 0 END AS TotalGalponMixto FROM GalponMixtoD)

SELECT A.pk_empresa,A.pk_division, A.pk_plantel, A.pk_lote
    ,COALESCE(PJ.PobInicialJoven * 1.0 / PL.PobInicialLote, 0) AS PorcPolloJoven
    ,COALESCE(PPM.PobInicialPesoMenor * 1.0 / PL.PobInicialLote, 0) AS PorcPesoAloMenor
    ,COALESCE((GM.TotalGalponMixto * 1.0 / 2) / CG.CantidadGalpon, 0) AS PorcGalponesMixtos
FROM {database_name}.PorcPoblacion A
LEFT JOIN PobLote PL ON A.pk_plantel = PL.pk_plantel AND A.pk_lote = PL.pk_lote
LEFT JOIN PobJoven PJ ON A.pk_plantel = PJ.pk_plantel AND A.pk_lote = PJ.pk_lote
LEFT JOIN PobPesoMenor PPM ON A.pk_plantel = PPM.pk_plantel AND A.pk_lote = PPM.pk_lote
LEFT JOIN CantidadGalpon CG ON A.pk_plantel = CG.pk_plantel AND A.pk_lote = CG.pk_lote
LEFT JOIN GalponMixtoFinal GM ON A.pk_plantel = GM.pk_plantel AND A.pk_lote = GM.pk_lote and A.pk_galpon = GM.pk_galpon 
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
    .saveAsTable(f"{database_name}.CalculosPorcPoblacion")
print('carga CalculosPorcPoblacion')   
#Se muestra ProduccionDetalleLote
df_ProduccionDetalleLote = spark.sql(f"""
WITH MaxOrden AS (SELECT clote, MAX(orden) AS max_orden FROM {database_name}.OrdenId GROUP BY clote),
Categorias AS (SELECT pk_lote,CONCAT_WS(',', COLLECT_SET(categoria)) AS categoria FROM {database_name}.ProduccionDetalleGalpon GROUP BY pk_lote),
PobInicialLote AS (SELECT pk_lote, SUM(PobInicial) AS TotalPobInicial FROM {database_name}.ProduccionDetalleGalpon GROUP BY pk_lote),
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
    FROM {database_name}.ProduccionDetalleGalpon
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
    FROM {database_name}.ProduccionDetalleGalpon
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
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorMort
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
,case when sum(a.AvesRendidas) = 0 then 0.0 else (sum(KilosRendidos)/sum(a.AvesRendidas)) end as PesoProm
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
,case when sum(KilosRendidos) = 0 then 0.0 else (sum(ConsDia)/sum(KilosRendidos)) end as ICA
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
from {database_name}.ProduccionDetalleGalpon A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN MaxOrden X on X.clote = B.clote
LEFT JOIN {database_name}.OrdenId C ON B.clote = C.clote AND C.orden = X.max_orden
left join {database_name}.ft_consumogascamaagua D on b.clote = D.ComplexEntityNo
left join {database_name}.CalculosPorcPoblacion E on A.pk_empresa = E.pk_empresa and A.pk_division = E.pk_division and A.pk_plantel = E.pk_plantel and A.pk_lote = E.pk_lote
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
    .saveAsTable(f"{database_name}.ProduccionDetalleLote")
print('carga ProduccionDetalleLote')   
#Se muestra ProduccionDetalleLoteSinAtipico
df_ProduccionDetalleLoteSinAtipico = spark.sql(f"""
WITH MaxOrden AS (SELECT clote, MAX(orden) AS max_orden FROM {database_name}.OrdenId GROUP BY clote),
Categorias AS (SELECT pk_lote,CONCAT_WS(',', COLLECT_SET(categoria)) AS categoria FROM {database_name}.ProduccionDetalleGalpon GROUP BY pk_lote),
PobInicialLote AS (SELECT pk_lote, SUM(PobInicial) AS TotalPobInicial FROM {database_name}.ProduccionDetalleGalpon GROUP BY pk_lote),
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
    FROM {database_name}.ProduccionDetalleGalpon
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
    FROM {database_name}.ProduccionDetalleGalpon
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
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorMort
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
,case when sum(KilosRendidos) = 0 then 0.0 else (sum(ConsDia)/sum(KilosRendidos)) end as ICA
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
,case when sum(a.AvesRendidas) = 0 then 0.0 else (sum(KilosRendidos)/sum(a.AvesRendidas)) end as PesoProm        
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
from {database_name}.ProduccionDetalleGalpon A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN MaxOrden X on X.clote = B.clote
LEFT JOIN {database_name}.OrdenId C ON B.clote = C.clote AND C.orden = X.max_orden
left join {database_name}.ft_consumogascamaagua D on b.clote = D.ComplexEntityNo
left join {database_name}.CalculosPorcPoblacion E on A.pk_empresa = E.pk_empresa and A.pk_division = E.pk_division and A.pk_plantel = E.pk_plantel and A.pk_lote = E.pk_lote
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
    .saveAsTable(f"{database_name}.ProduccionDetalleLoteSinAtipico")
print('carga ProduccionDetalleLoteSinAtipico')   
#Se muestra ProduccionDetalleLote
df_ProduccionDetalleLote = spark.sql(f"""
insert into {database_name}.ProduccionDetalleLote
select * from {database_name}.ProduccionDetalleLoteSinAtipico
except
select * from {database_name}.ProduccionDetalleLote
""")
print('carga insert ProduccionDetalleLote')   
#Se muestra ProduccionDetalleMensual
df_ProduccionDetalleMensual = spark.sql(f"""
WITH PobInicialPorMes AS (SELECT pk_mes,pk_division,SUM(PobInicial) AS TotalPobInicial FROM {database_name}.ProduccionDetalleLote WHERE pk_estado IN (2,3) AND FlagArtAtipico = 2 GROUP BY pk_mes,pk_division)
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
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorMort
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
,case when sum(AvesRendidas) = 0 then 0.0 else (sum(KilosRendidos)/sum(AvesRendidas)) end as PesoProm
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
,case when sum(KilosRendidos) = 0 then 0.0 else (sum(ConsDia)/sum(KilosRendidos)) end as ICA
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
from {database_name}.ProduccionDetalleLote A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN PobInicialPorMes P ON A.pk_mes = P.pk_mes AND A.pk_division = P.pk_division
where pk_estado in (2,3) and FlagArtAtipico = 2
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
    .saveAsTable(f"{database_name}.ProduccionDetalleMensual")
print('carga ProduccionDetalleMensual')   
#Se muestra ProduccionDetalleMensualSinAtipico
df_ProduccionDetalleMensualSinAtipico = spark.sql(f"""
WITH PobInicialPorMes AS (SELECT pk_mes,pk_division,SUM(PobInicial) AS TotalPobInicial FROM {database_name}.ProduccionDetalleLote WHERE pk_estado IN (2,3) AND FlagArtAtipico = 1 AND FlagAtipico = 1 GROUP BY pk_mes,pk_division)

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
,case when sum(PobInicial) = 0 then 0.0 else (((sum(MortDia) / sum(PobInicial*1.0))*100)) end as PorMort
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
,case when sum(AvesRendidas) = 0 then 0.0 else (sum(KilosRendidos)/sum(AvesRendidas)) end as PesoProm
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
,case when sum(KilosRendidos) = 0 then 0.0 else (sum(ConsDia)/sum(KilosRendidos)) end as ICA
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
from {database_name}.ProduccionDetalleLote A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
LEFT JOIN PobInicialPorMes P ON A.pk_mes = P.pk_mes AND A.pk_division = P.pk_division
where pk_estado in (2,3) and FlagAtipico = 1 and FlagArtAtipico = 1
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
    .saveAsTable(f"{database_name}.ProduccionDetalleMensualSinAtipico")
print('carga ProduccionDetalleMensualSinAtipico')   
#Se muestra ProduccionDetalleMensual
df_ProduccionDetalleMensual = spark.sql(f"""
insert into {database_name}.ProduccionDetalleMensual
select * from {database_name}.ProduccionDetalleMensualSinAtipico
except
select * from {database_name}.ProduccionDetalleMensual
""")
print('carga INSERT ProduccionDetalleMensual')   
#Se muestra ft_consolidado_Diario
df_ft_consolidado_Diario = spark.sql(f"""
WITH ProduccionAnterior AS (SELECT ComplexEntityNo,pk_semanavida,MAX(PesoSem) AS PesoSemAnterior FROM {database_name}.Produccion GROUP BY ComplexEntityNo,pk_semanavida),
STD_Mortalidad AS (SELECT ComplexEntityNo,pk_semanavida,ROUND(SUM(STDMortDia), 2) AS STDPorcMortSem,ROUND((SUM(STDMortDia) / 100) * AVG(PobInicial), 0) AS STDMortSem FROM {database_name}.ProduccionDetalleEdad GROUP BY ComplexEntityNo,pk_semanavida)

select
 max(fecha) as fecha
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
 ,MAX(U_RuidosTotales) RuidosRespiratorios
 ,ProductoConsumo
 ,MAX(STDConsSem) STDConsSem
 ,MAX(STDPorcConsSem) STDPorcConsSem
 ,MAX(ConsSem) ConsSem
 ,round((max(ConsSem) / COALESCE((avg(PobInicial) - max(MortSem)),0)),3)as PorcConsSem
 ,MAX(ConsSem) - MAX(STDConsSem) DifConsSem_STDConsSem
 ,(round((max(ConsSem) / COALESCE((avg(PobInicial) - max(MortSem)),0)),3)) - MAX(STDPorcConsSem) DifPorcConsSem_STDPorcConsSem
 ,MAX(U_ConsumoGasInvierno) U_ConsumoGasInvierno
 ,MAX(U_ConsumoGasVerano) U_ConsumoGasVerano
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
,MAX(GananciaPesoSem) GananciaPesoSem
,MAX(CV) CV
from {database_name}.Produccion b
left join (select distinct complexentityno, pk_diasvida, ((PobInicial) - (AvesRendidasAcum)) SaldoAvesRendidasAnterior from {database_name}.Produccion) C on B.ComplexEntityNo = C.ComplexEntityNo and B.pk_diasvida = C.pk_diasvida +1
LEFT JOIN ProduccionAnterior pa ON b.ComplexEntityNo = pa.ComplexEntityNo AND b.pk_semanavida = pa.pk_semanavida - 1
LEFT JOIN STD_Mortalidad sm ON b.ComplexEntityNo = sm.ComplexEntityNo AND b.pk_semanavida = sm.pk_semanavida
where (date_format(CAST(b.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(b.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
group by b.pk_empresa,b.pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_grupoconsumo,pk_especie,pk_estado
,pk_administrador,pk_proveedor,B.pk_diasvida,b.pk_semanavida,B.ComplexEntityNo,FechaNacimiento,FechaCierre,FechaAlojamiento,categoria,PadreMayor,FlagAtipico
,EdadPadreCorralDescrip,U_CausaPesoBajo,U_AccionPesoBajo,ProductoConsumo,DescripTipoAlimentoXTipoProducto,ListaFormulaNo,ListaFormulaName,TipoOrigen
,MortConcatCorral,MortConcatLote,MortConcatSemAnioLote,PesoConcatCorral,PesoConcatLote,PesoConcatSemAnioLote,pk_tipogranja
,pa.PesoSemAnterior,sm.STDPorcMortSem,sm.STDMortSem 
""")

#Se muestra ft_consolidado_Diario
try:
    df = spark.table("{database_name}.ft_consolidado_Diario")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_consolidado_Diario_new = df_ft_consolidado_Diario.union(df_filtered)
except Exception as e:
    df_ft_consolidado_Diario_new = df_ft_consolidado_Diario

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_Diario_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_new")

df_ft_consolidado_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
#spark.sql(f"""CREATE TABLE {database_name}.produccion AS SELECT * FROM {database_name}.produccion_nueva""")
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario"
}
df_ft_consolidado_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario_new")

print('carga ft_consolidado_Diario')   
#Se muestra ft_consolidado_Corral
df_ft_consolidado_Corral = spark.sql(f"""
select 
		 max(FechaCierre) as fecha
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
        ,max(fecha) EventDate
		,categoria
		,avg(PesoAlo) as PesoAlo
		,max(FlagAtipico) FlagAtipico
		,max(edadpadrecorral) EdadPadreCorral
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
		,MAX(U_RuidosTotales) / MAX(COALESCE(CountRuidosTotales*1.0,0)) RuidosRespiratorios
		,'' FechaDesinfeccion
		,'' TipoCama
		,'' FormaReutilizacion
		,'' NReuso
		,'' PDE
		,'' PDT
		,'' PromAmoniaco
		,'' PromTemperatura
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
from {database_name}.ProduccionDetalleCorral A
where (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
group by pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_tipoproducto,
pk_estado,pk_especie,A.ComplexEntityNo,FechaNacimiento,FechaAlojamiento,FechaCrianza,FechaInicioGranja,FechaInicioSaca,FechaFinSaca,PadreMayor,RazaMayor,
IncubadoraMayor,categoria,EdadPadreCorralDescrip,NomPrimLesion,NomSegLesion,NomTerLesion,MedSem1,MedSem2,MedSem3,MedSem4,MedSem5,MedSem6,MedSem7,A.TipoOrigen
,pk_tipogranja
""")

#Se muestra ft_consolidado_Corral
try:
    df = spark.table("{database_name}.ft_consolidado_Corral")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_consolidado_Corral_new = df_ft_consolidado_Corral.union(df_filtered)
    print("✅ Tabla cargada correctamente")
except Exception as e:
    df_ft_consolidado_Corral_new = df_ft_consolidado_Corral
    print(f"⚠️ Error al cargar la tabla: {str(e)}")
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_Corral_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral_new")

df_ft_consolidado_Corral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Corral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral"
}
df_ft_consolidado_Corral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral_new")

print('carga ft_consolidado_Corral')
#Se muestra UPD ft_consolidado_Corral
df_UPDft_consolidado_Corral = spark.sql(f"""
   WITH temp_TemperaturaCorral AS (
    SELECT 
        ComplexEntityNo,
        FechaDesinfeccion,
        TipoCama,
        FormaReutilizacion,
        NReuso,
        MAX(PDE) AS PDE,
        MAX(PDT) AS PDT,
        MAX(AmoniacoMuestra1) AS AmoniacoMuestra1,
        MAX(AmoniacoMuestra2) AS AmoniacoMuestra2,
        MAX(TempRuma1) AS TempRuma1,
        MAX(TempRuma2) AS TempRuma2,
        MAX(TempRuma3) AS TempRuma3,
        MAX(TempRuma4) AS TempRuma4
    FROM {database_name}.ft_TemperaturaCorral
    GROUP BY ComplexEntityNo, FechaDesinfeccion, TipoCama, FormaReutilizacion, NReuso)

SELECT 
 A.fecha
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
,A.ComplexEntityNo
,A.FechaNacimiento
,A.FechaCierre
,A.FechaCierreLote
,A.FechaAlojamiento
,A.FechaCrianza
,A.FechaInicioGranja
,A.FechaInicioSaca
,A.FechaFinSaca
,A.PadreMayor
,A.RazaMayor
,A.IncubadoraMayor
,A.PorcPadreMayor
,A.PorCodigoRaza
,A.PorcIncMayor
,A.PobInicial
,A.AvesLogradas
,A.AvesRendidas
,A.SobranFaltan
,A.KilosRendidos
,A.MortDia
,A.PorMort
,A.STDPorMort
,A.DifPorMort_STDPorMort
,A.MortSem1
,A.MortSem2
,A.MortSem3
,A.MortSem4
,A.MortSem5
,A.MortSem6
,A.MortSem7
,A.PorcMortSem1
,A.STDPorcMortSem1
,A.DifPorcMortSem1_STDPorcMortSem1
,A.PorcMortSem2
,A.STDPorcMortSem2
,A.DifPorcMortSem2_STDPorcMortSem2
,A.PorcMortSem3
,A.STDPorcMortSem3
,A.DifPorcMortSem3_STDPorcMortSem3
,A.PorcMortSem4
,A.STDPorcMortSem4
,A.DifPorcMortSem4_STDPorcMortSem4
,A.PorcMortSem5
,A.STDPorcMortSem5
,A.DifPorcMortSem5_STDPorcMortSem5
,A.PorcMortSem6
,A.STDPorcMortSem6
,A.DifPorcMortSem6_STDPorcMortSem6
,A.PorcMortSem7
,A.STDPorcMortSem7
,A.DifPorcMortSem7_STDPorcMortSem7
,A.MortSemAcum1
,A.MortSemAcum2
,A.MortSemAcum3
,A.MortSemAcum4
,A.MortSemAcum5
,A.MortSemAcum6
,A.MortSemAcum7
,A.PorcMortSemAcum1
,A.STDPorcMortSemAcum1
,A.DifPorcMortSemAcum1_STDPorcMortSemAcum1
,A.PorcMortSemAcum2
,A.STDPorcMortSemAcum2
,A.DifPorcMortSemAcum2_STDPorcMortSemAcum2
,A.PorcMortSemAcum3
,A.STDPorcMortSemAcum3
,A.DifPorcMortSemAcum3_STDPorcMortSemAcum3
,A.PorcMortSemAcum4
,A.STDPorcMortSemAcum4
,A.DifPorcMortSemAcum4_STDPorcMortSemAcum4
,A.PorcMortSemAcum5
,A.STDPorcMortSemAcum5
,A.DifPorcMortSemAcum5_STDPorcMortSemAcum5
,A.PorcMortSemAcum6
,A.STDPorcMortSemAcum6
,A.DifPorcMortSemAcum6_STDPorcMortSemAcum6
,A.PorcMortSemAcum7
,A.STDPorcMortSemAcum7
,A.DifPorcMortSemAcum7_STDPorcMortSemAcum7
,A.PreInicio
,A.Inicio
,A.Acabado
,A.Terminado
,A.Finalizador
,A.ConsDia
,A.PorcPreIni
,A.PorcIni
,A.PorcAcab
,A.PorcTerm
,A.PorcFin
,A.PorcConsumo
,A.STDPorcConsumo
,A.DifPorcConsumo_STDPorcConsumo
,A.Seleccion
,A.PorcSeleccion
,A.PesoSem1
,A.STDPesoSem1
,A.DifPesoSem1_STDPesoSem1
,A.PesoSem2
,A.STDPesoSem2
,A.DifPesoSem2_STDPesoSem2
,A.PesoSem3
,A.STDPesoSem3
,A.DifPesoSem3_STDPesoSem3
,A.PesoSem4
,A.STDPesoSem4
,A.DifPesoSem4_STDPesoSem4
,A.PesoSem5
,A.STDPesoSem5
,A.DifPesoSem5_STDPesoSem5
,A.PesoSem6
,A.STDPesoSem6
,A.DifPesoSem6_STDPesoSem6
,A.PesoSem7
,A.STDPesoSem7
,A.DifPesoSem7_STDPesoSem7
,A.PesoProm
,A.STDPesoProm
,A.DifPesoProm_STDPesoProm
,A.GananciaDiaVenta
,A.ICA
,A.STDICA
,A.DifICA_STDICA
,A.ICAAjustado
,A.AvesXm2
,A.KgXm2
,A.IEP
,A.DiasLimpieza
,A.DiasCrianza
,A.TotalCampana
,A.DiasSaca
,A.EdadInicioSaca
,A.CantInicioSaca
,A.EdadGranja
,A.Gas
,A.Cama
,A.Agua
,A.PorcMacho
,A.Pigmentacion
,A.EventDate
,A.categoria
,A.PesoAlo
,A.FlagAtipico
,A.EdadPadreCorral
,A.PesoHvo
,A.MortSem8
,A.MortSem9
,A.MortSem10
,A.MortSem11
,A.MortSem12
,A.MortSem13
,A.MortSem14
,A.MortSem15
,A.MortSem16
,A.MortSem17
,A.MortSem18
,A.MortSem19
,A.MortSem20
,A.PorcMortSem8
,A.STDPorcMortSem8
,A.DifPorcMortSem8_STDPorcMortSem8
,A.PorcMortSem9
,A.STDPorcMortSem9
,A.DifPorcMortSem9_STDPorcMortSem9
,A.PorcMortSem10
,A.STDPorcMortSem10
,A.DifPorcMortSem10_STDPorcMortSem10
,A.PorcMortSem11
,A.STDPorcMortSem11
,A.DifPorcMortSem11_STDPorcMortSem11
,A.PorcMortSem12
,A.STDPorcMortSem12
,A.DifPorcMortSem12_STDPorcMortSem12
,A.PorcMortSem13
,A.STDPorcMortSem13
,A.DifPorcMortSem13_STDPorcMortSem13
,A.PorcMortSem14
,A.STDPorcMortSem14
,A.DifPorcMortSem14_STDPorcMortSem14
,A.PorcMortSem15
,A.STDPorcMortSem15
,A.DifPorcMortSem15_STDPorcMortSem15
,A.PorcMortSem16
,A.STDPorcMortSem16
,A.DifPorcMortSem16_STDPorcMortSem16
,A.PorcMortSem17
,A.STDPorcMortSem17
,A.DifPorcMortSem17_STDPorcMortSem17
,A.PorcMortSem18
,A.STDPorcMortSem18
,A.DifPorcMortSem18_STDPorcMortSem18
,A.PorcMortSem19
,A.STDPorcMortSem19
,A.DifPorcMortSem19_STDPorcMortSem19
,A.PorcMortSem20
,A.STDPorcMortSem20
,A.DifPorcMortSem20_STDPorcMortSem20
,A.MortSemAcum8 
,A.MortSemAcum9 
,A.MortSemAcum10 
,A.MortSemAcum11 
,A.MortSemAcum12 
,A.MortSemAcum13 
,A.MortSemAcum14 
,A.MortSemAcum15 
,A.MortSemAcum16 
,A.MortSemAcum17 
,A.MortSemAcum18 
,A.MortSemAcum19 
,A.MortSemAcum20
,A.PorcMortSemAcum8
,A.STDPorcMortSemAcum8
,A.DifPorcMortSemAcum8_STDPorcMortSemAcum8
,A.PorcMortSemAcum9
,A.STDPorcMortSemAcum9
,A.DifPorcMortSemAcum9_STDPorcMortSemAcum9
,A.PorcMortSemAcum10
,A.STDPorcMortSemAcum10
,A.DifPorcMortSemAcum10_STDPorcMortSemAcum10
,A.PorcMortSemAcum11
,A.STDPorcMortSemAcum11
,A.DifPorcMortSemAcum11_STDPorcMortSemAcum11
,A.PorcMortSemAcum12
,A.STDPorcMortSemAcum12
,A.DifPorcMortSemAcum12_STDPorcMortSemAcum12
,A.PorcMortSemAcum13
,A.STDPorcMortSemAcum13
,A.DifPorcMortSemAcum13_STDPorcMortSemAcum13
,A.PorcMortSemAcum14
,A.STDPorcMortSemAcum14
,A.DifPorcMortSemAcum14_STDPorcMortSemAcum14
,A.PorcMortSemAcum15
,A.STDPorcMortSemAcum15
,A.DifPorcMortSemAcum15_STDPorcMortSemAcum15
,A.PorcMortSemAcum16
,A.STDPorcMortSemAcum16
,A.DifPorcMortSemAcum16_STDPorcMortSemAcum16
,A.PorcMortSemAcum17
,A.STDPorcMortSemAcum17
,A.DifPorcMortSemAcum17_STDPorcMortSemAcum17
,A.PorcMortSemAcum18
,A.STDPorcMortSemAcum18
,A.DifPorcMortSemAcum18_STDPorcMortSemAcum18
,A.PorcMortSemAcum19
,A.STDPorcMortSemAcum19
,A.DifPorcMortSemAcum19_STDPorcMortSemAcum19
,A.PorcMortSemAcum20
,A.STDPorcMortSemAcum20
,A.DifPorcMortSemAcum20_STDPorcMortSemAcum20
,A.PavoIni
,A.Pavo1
,A.Pavo2
,A.Pavo3
,A.Pavo4
,A.Pavo5
,A.Pavo6
,A.Pavo7
,A.PorcPavoIni
,A.PorcPavo1
,A.PorcPavo2
,A.PorcPavo3
,A.PorcPavo4
,A.PorcPavo5
,A.PorcPavo6
,A.PorcPavo7
,A.PesoSem8
,A.STDPesoSem8
,A.DifPesoSem8_STDPesoSem8
,A.PesoSem9
,A.STDPesoSem9
,A.DifPesoSem9_STDPesoSem9
,A.PesoSem10
,A.STDPesoSem10
,A.DifPesoSem10_STDPesoSem10
,A.PesoSem11
,A.STDPesoSem11
,A.DifPesoSem11_STDPesoSem11
,A.PesoSem12
,A.STDPesoSem12
,A.DifPesoSem12_STDPesoSem12
,A.PesoSem13
,A.STDPesoSem13
,A.DifPesoSem13_STDPesoSem13
,A.PesoSem14
,A.STDPesoSem14
,A.DifPesoSem14_STDPesoSem14
,A.PesoSem15
,A.STDPesoSem15
,A.DifPesoSem15_STDPesoSem15
,A.PesoSem16
,A.STDPesoSem16
,A.DifPesoSem16_STDPesoSem16
,A.PesoSem17
,A.STDPesoSem17
,A.DifPesoSem17_STDPesoSem17
,A.PesoSem18
,A.STDPesoSem18
,A.DifPesoSem18_STDPesoSem18
,A.PesoSem19
,A.STDPesoSem19
,A.DifPesoSem19_STDPesoSem19
,A.PesoSem20
,A.STDPesoSem20
,A.DifPesoSem20_STDPesoSem20
,A.PavosBBMortIncub
,A.EdadPadreCorralDescrip
,A.DiasAloj
,A.RuidosRespiratorios
,B.FechaDesinfeccion
,B.TipoCama
,B.FormaReutilizacion
,B.NReuso
,B.PDE
,B.PDT
,((B.AmoniacoMuestra1 + B.AmoniacoMuestra2)/2) PromAmoniaco
,((B.TempRuma1 + B.TempRuma2 + B.TempRuma3 + B.TempRuma4)/4) PromTemperatura
,A.ConsSem1
,A.ConsSem2
,A.ConsSem3
,A.ConsSem4
,A.ConsSem5
,A.ConsSem6
,A.ConsSem7
,A.PorcConsSem1
,A.PorcConsSem2
,A.PorcConsSem3
,A.PorcConsSem4
,A.PorcConsSem5
,A.PorcConsSem6
,A.PorcConsSem7
,A.DifPorcConsSem1_STDPorcConsSem1
,A.DifPorcConsSem2_STDPorcConsSem2
,A.DifPorcConsSem3_STDPorcConsSem3
,A.DifPorcConsSem4_STDPorcConsSem4
,A.DifPorcConsSem5_STDPorcConsSem5
,A.DifPorcConsSem6_STDPorcConsSem6
,A.DifPorcConsSem7_STDPorcConsSem7
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
,A.DifConsSem1_STDConsSem1
,A.DifConsSem2_STDConsSem2
,A.DifConsSem3_STDConsSem3
,A.DifConsSem4_STDConsSem4
,A.DifConsSem5_STDConsSem5
,A.DifConsSem6_STDConsSem6
,A.DifConsSem7_STDConsSem7
,A.CantPrimLesion
,A.CantSegLesion
,A.CantTerLesion
,A.PorcPrimLesion
,A.PorcSegLesion
,A.PorcTerLesion
,A.NomPrimLesion
,A.NomSegLesion
,A.NomTerLesion
,A.DiasSacaEfectivo
,A.U_PEAccidentados
,A.U_PEHigadoGraso
,A.U_PEHepatomegalia
,A.U_PEHigadoHemorragico
,A.U_PEInanicion
,A.U_PEProblemaRespiratorio
,A.U_PESCH
,A.U_PEEnteritis
,A.U_PEAscitis
,A.U_PEMuerteSubita
,A.U_PEEstresPorCalor
,A.U_PEHidropericardio
,A.U_PEHemopericardio
,A.U_PEUratosis
,A.U_PEMaterialCaseoso
,A.U_PEOnfalitis
,A.U_PERetencionDeYema
,A.U_PEErosionDeMolleja
,A.U_PEHemorragiaMusculos
,A.U_PESangreEnCiego
,A.U_PEPericarditis
,A.U_PEPeritonitis
,A.U_PEProlapso
,A.U_PEPicaje
,A.U_PERupturaAortica
,A.U_PEBazoMoteado
,A.U_PENoViable
,A.U_PEAerosaculitisG2
,A.U_PECojera
,A.U_PEHigadoIcterico
,A.U_PEMaterialCaseoso_po1ra
,A.U_PEMaterialCaseosoMedRetr
,A.U_PENecrosisHepatica
,A.U_PENeumonia
,A.U_PESepticemia
,A.U_PEVomitoNegro
,A.PorcAccidentados
,A.PorcHigadoGraso
,A.PorcHepatomegalia
,A.PorcHigadoHemorragico
,A.PorcInanicion
,A.PorcProblemaRespiratorio
,A.PorcSCH
,A.PorcEnteritis
,A.PorcAscitis
,A.PorcMuerteSubita
,A.PorcEstresPorCalor
,A.PorcHidropericardio
,A.PorcHemopericardio
,A.PorcUratosis
,A.PorcMaterialCaseoso
,A.PorcOnfalitis
,A.PorcRetencionDeYema
,A.PorcErosionDeMolleja
,A.PorcHemorragiaMusculos
,A.PorcSangreEnCiego
,A.PorcPericarditis
,A.PorcPeritonitis
,A.PorcProlapso
,A.PorcPicaje
,A.PorcRupturaAortica
,A.PorcBazoMoteado
,A.PorcNoViable
,A.PorcAerosaculitisG2
,A.PorcCojera
,A.PorcHigadoIcterico
,A.PorcMaterialCaseoso_po1ra
,A.PorcMaterialCaseosoMedRetr
,A.PorcNecrosisHepatica
,A.PorcNeumonia
,A.PorcSepticemia
,A.PorcVomitoNegro
,A.STDPorcConsGasInvierno
,A.STDPorcConsGasVerano
,A.MedSem1
,A.MedSem2
,A.MedSem3
,A.MedSem4
,A.MedSem5
,A.MedSem6
,A.MedSem7
,A.DiasMedSem1
,A.DiasMedSem2
,A.DiasMedSem3
,A.DiasMedSem4
,A.DiasMedSem5
,A.DiasMedSem6
,A.DiasMedSem7
,A.PorcAlojamientoXEdadPadre
,A.TipoOrigen
,A.Peso5Dias
,A.STDPeso5Dias
,A.DifPeso5Dias_STDPeso5Dias
,A.NoViableSem1
,A.NoViableSem2
,A.NoViableSem3
,A.NoViableSem4
,A.NoViableSem5
,A.NoViableSem6
,A.NoViableSem7
,A.PorcNoViableSem1
,A.PorcNoViableSem2
,A.PorcNoViableSem3
,A.PorcNoViableSem4
,A.PorcNoViableSem5
,A.PorcNoViableSem6
,A.PorcNoViableSem7
,A.NoViableSem8
,A.PorcNoViableSem8
,A.pk_tipogranja
FROM {database_name}.ft_consolidado_Corral A
LEFT JOIN temp_TemperaturaCorral B ON A.ComplexEntityNo = B.ComplexEntityNo
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Corral.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral_new")

df_ft_consolidado_Corral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Corral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral"
}
df_ft_consolidado_Corral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral_new")

print('carga UPD ft_consolidado_Corral')
#Se muestra UPD ft_consolidado_Galpon
df_ft_consolidado_Galpon = spark.sql(f"""
select max(FechaCierre) as fecha
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
,clote + '-' + nogalpon as ComplexEntityNo
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
,max(fecha) as EventDate
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
from {database_name}.ProduccionDetalleGalpon A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name}.lk_galpon C on A.pk_galpon = C.pk_galpon
where (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,A.pk_plantel,A.pk_lote,A.pk_galpon,pk_tipoproducto,pk_estado,pk_especie,nogalpon,
clote,categoria,pk_tipogranja
""")

#Se muestra ft_consolidado_Galpon
try:
    df = spark.table(f"{database_name}.ft_consolidado_Galpon")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_consolidado_Galpon_new = df_ft_consolidado_Galpon.union(df_filtered)
except Exception as e:
    df_ft_consolidado_Galpon_new = df_ft_consolidado_Galpon
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Galpon_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_Galpon_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Galpon_new")

df_ft_consolidado_Galpon_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Galpon_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Galpon")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Galpon"
}
df_ft_consolidado_Galpon_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Galpon")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Galpon_new")
print('carga UPD ft_consolidado_Galpon')
#Se muestra UPD ft_consolidado_Corral
df_ft_consolidado_Lote = spark.sql(f"""
select 
 max(fechacierre) as fecha
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
,case when sum(KilosRendidos) = 0 then 0.0 else round((sum(ConsDia)/sum(KilosRendidos)),14) end as ICA
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
,max(fecha) as EventDate
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
from {database_name}.ProduccionDetalleLote A
left join {database_name}.lk_lote B on A.pk_lote = B.pk_lote
where (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
group by a.pk_empresa,pk_division,pk_zona,pk_subzona,pk_administrador,pk_diasvida,A.pk_plantel,A.pk_lote,pk_tipoproducto,
pk_estado,pk_especie,clote,categoria,FlagAtipico,FlagArtAtipico,clote
""")

#Se muestra ft_consolidado_Lote
try:
    df = spark.table(f"{database_name}.ft_consolidado_Lote")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_consolidado_Lote_new = df_ft_consolidado_Lote.union(df_filtered)
except Exception as e:
    df_ft_consolidado_Lote_new = df_ft_consolidado_Lote
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_Lote_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote_new")

df_ft_consolidado_Lote_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Lote_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote"
}
df_ft_consolidado_Lote_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote_new")

print('carga ft_consolidado_Lote')
#Se muestra dias
df_dias = spark.sql(f"""
with flag as (select ComplexEntityNo,
pk_division,
substring(cast(fecha as varchar(8)),1,6) pk_mes,
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
from {database_name}.ft_consolidado_Lote
where pk_estado in (2,3) and pk_empresa = 1 and FlagArtAtipico = 2 )

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
    .saveAsTable(f"{database_name}.dias")
print('carga dias')
#Se muestra diasSinAtipico
df_diasSinAtipico = spark.sql(f"""
with diasSinAtipico as (select ComplexEntityNo,
				pk_division,
				substring(cast(fecha as varchar(8)),1,6) pk_mes,
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
		from {database_name}.ft_consolidado_Lote
		where pk_estado in (2,3) and pk_empresa = 1 and FlagAtipico = 1 and FlagArtAtipico = 1)
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
    .saveAsTable(f"{database_name}.diasSinAtipico")
print('carga diasSinAtipico')
#Se muestra ft_consolidado_mensual
df_ft_consolidado_mensual = spark.sql(f"""
select a.pk_mes
,pk_empresa
,case when COALESCE(pk_diasvida,0) = 0 then 120 else pk_diasvida end pk_diasvida
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
from {database_name}.ProduccionDetalleMensual A
left join {database_name}.dias b on b.pk_mes = a.pk_mes and a.pk_division = b.pk_division
where (A.pk_mes >= {AnioMes} AND A.pk_mes <= {AnioMesFin})
and FlagArtAtipico = 2 and a.pk_division = 3
group by a.pk_mes,a.pk_empresa,a.pk_diasvida,a.pk_division,FlagAtipico,FlagArtAtipico
union
select a.pk_mes
,pk_empresa
,case when COALESCE(pk_diasvida,0) = 0 then 120 else pk_diasvida end pk_diasvida
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
from {database_name}.ProduccionDetalleMensual A
left join {database_name}.diasSinAtipico b on b.pk_mes = a.pk_mes and a.pk_division = b.pk_division
where (A.pk_mes >= {AnioMes} AND A.pk_mes <= {AnioMesFin})
and FlagArtAtipico = 1 and a.pk_division = 3
group by a.pk_mes,a.pk_empresa,a.pk_diasvida,a.pk_division,FlagAtipico,FlagArtAtipico
""")

#Se muestra ft_consolidado_mensual
try:
    df = spark.table(f"{database_name}.ft_consolidado_mensual")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.col("pk_mes") < F.lit(AnioMes)) |
        (F.col("pk_mes") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_consolidado_mensual_new = df_ft_consolidado_mensual.union(df_filtered)
except Exception as e:
    df_ft_consolidado_mensual_new = df_ft_consolidado_mensual
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_mensual_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_mensual_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_mensual_new")

df_ft_consolidado_mensual_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_mensual_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_mensual")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_mensual"
}
df_ft_consolidado_mensual_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_mensual")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_mensual_new")

print('carga ft_consolidado_mensual')
#Se muestra UPDft_consolidado_Corral2
df_UPDft_consolidado_Corral2 = spark.sql(f"""
with GasGalpon as (select pk_plantel,pk_lote,pk_galpon,max(gas) gas from {database_name}.ft_consolidado_galpon where pk_empresa = 1 group by pk_plantel,pk_lote,pk_galpon)
select 
 A.fecha
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
,A.ComplexEntityNo
,A.FechaNacimiento
,A.FechaCierre
,A.FechaCierreLote
,A.FechaAlojamiento
,A.FechaCrianza
,A.FechaInicioGranja
,A.FechaInicioSaca
,A.FechaFinSaca
,A.PadreMayor
,A.RazaMayor
,A.IncubadoraMayor
,A.PorcPadreMayor
,A.PorCodigoRaza
,A.PorcIncMayor
,A.PobInicial
,A.AvesLogradas
,A.AvesRendidas
,A.SobranFaltan
,A.KilosRendidos
,A.MortDia
,A.PorMort
,A.STDPorMort
,A.DifPorMort_STDPorMort
,A.MortSem1
,A.MortSem2
,A.MortSem3
,A.MortSem4
,A.MortSem5
,A.MortSem6
,A.MortSem7
,A.PorcMortSem1
,A.STDPorcMortSem1
,A.DifPorcMortSem1_STDPorcMortSem1
,A.PorcMortSem2
,A.STDPorcMortSem2
,A.DifPorcMortSem2_STDPorcMortSem2
,A.PorcMortSem3
,A.STDPorcMortSem3
,A.DifPorcMortSem3_STDPorcMortSem3
,A.PorcMortSem4
,A.STDPorcMortSem4
,A.DifPorcMortSem4_STDPorcMortSem4
,A.PorcMortSem5
,A.STDPorcMortSem5
,A.DifPorcMortSem5_STDPorcMortSem5
,A.PorcMortSem6
,A.STDPorcMortSem6
,A.DifPorcMortSem6_STDPorcMortSem6
,A.PorcMortSem7
,A.STDPorcMortSem7
,A.DifPorcMortSem7_STDPorcMortSem7
,A.MortSemAcum1
,A.MortSemAcum2
,A.MortSemAcum3
,A.MortSemAcum4
,A.MortSemAcum5
,A.MortSemAcum6
,A.MortSemAcum7
,A.PorcMortSemAcum1
,A.STDPorcMortSemAcum1
,A.DifPorcMortSemAcum1_STDPorcMortSemAcum1
,A.PorcMortSemAcum2
,A.STDPorcMortSemAcum2
,A.DifPorcMortSemAcum2_STDPorcMortSemAcum2
,A.PorcMortSemAcum3
,A.STDPorcMortSemAcum3
,A.DifPorcMortSemAcum3_STDPorcMortSemAcum3
,A.PorcMortSemAcum4
,A.STDPorcMortSemAcum4
,A.DifPorcMortSemAcum4_STDPorcMortSemAcum4
,A.PorcMortSemAcum5
,A.STDPorcMortSemAcum5
,A.DifPorcMortSemAcum5_STDPorcMortSemAcum5
,A.PorcMortSemAcum6
,A.STDPorcMortSemAcum6
,A.DifPorcMortSemAcum6_STDPorcMortSemAcum6
,A.PorcMortSemAcum7
,A.STDPorcMortSemAcum7
,A.DifPorcMortSemAcum7_STDPorcMortSemAcum7
,A.PreInicio
,A.Inicio
,A.Acabado
,A.Terminado
,A.Finalizador
,A.ConsDia
,A.PorcPreIni
,A.PorcIni
,A.PorcAcab
,A.PorcTerm
,A.PorcFin
,A.PorcConsumo
,A.STDPorcConsumo
,A.DifPorcConsumo_STDPorcConsumo
,A.Seleccion
,A.PorcSeleccion
,A.PesoSem1
,A.STDPesoSem1
,A.DifPesoSem1_STDPesoSem1
,A.PesoSem2
,A.STDPesoSem2
,A.DifPesoSem2_STDPesoSem2
,A.PesoSem3
,A.STDPesoSem3
,A.DifPesoSem3_STDPesoSem3
,A.PesoSem4
,A.STDPesoSem4
,A.DifPesoSem4_STDPesoSem4
,A.PesoSem5
,A.STDPesoSem5
,A.DifPesoSem5_STDPesoSem5
,A.PesoSem6
,A.STDPesoSem6
,A.DifPesoSem6_STDPesoSem6
,A.PesoSem7
,A.STDPesoSem7
,A.DifPesoSem7_STDPesoSem7
,A.PesoProm
,A.STDPesoProm
,A.DifPesoProm_STDPesoProm
,A.GananciaDiaVenta
,A.ICA
,A.STDICA
,A.DifICA_STDICA
,A.ICAAjustado
,A.AvesXm2
,A.KgXm2
,A.IEP
,A.DiasLimpieza
,A.DiasCrianza
,A.TotalCampana
,A.DiasSaca
,A.EdadInicioSaca
,A.CantInicioSaca
,A.EdadGranja
,A.Gas
,A.Cama
,A.Agua
,A.PorcMacho
,A.Pigmentacion
,A.EventDate
,A.categoria
,A.PesoAlo
,A.FlagAtipico
,A.EdadPadreCorral
,B.gas as GasGalpon
,A.PesoHvo
,A.MortSem8
,A.MortSem9
,A.MortSem10
,A.MortSem11
,A.MortSem12
,A.MortSem13
,A.MortSem14
,A.MortSem15
,A.MortSem16
,A.MortSem17
,A.MortSem18
,A.MortSem19
,A.MortSem20
,A.PorcMortSem8
,A.STDPorcMortSem8
,A.DifPorcMortSem8_STDPorcMortSem8
,A.PorcMortSem9
,A.STDPorcMortSem9
,A.DifPorcMortSem9_STDPorcMortSem9
,A.PorcMortSem10
,A.STDPorcMortSem10
,A.DifPorcMortSem10_STDPorcMortSem10
,A.PorcMortSem11
,A.STDPorcMortSem11
,A.DifPorcMortSem11_STDPorcMortSem11
,A.PorcMortSem12
,A.STDPorcMortSem12
,A.DifPorcMortSem12_STDPorcMortSem12
,A.PorcMortSem13
,A.STDPorcMortSem13
,A.DifPorcMortSem13_STDPorcMortSem13
,A.PorcMortSem14
,A.STDPorcMortSem14
,A.DifPorcMortSem14_STDPorcMortSem14
,A.PorcMortSem15
,A.STDPorcMortSem15
,A.DifPorcMortSem15_STDPorcMortSem15
,A.PorcMortSem16
,A.STDPorcMortSem16
,A.DifPorcMortSem16_STDPorcMortSem16
,A.PorcMortSem17
,A.STDPorcMortSem17
,A.DifPorcMortSem17_STDPorcMortSem17
,A.PorcMortSem18
,A.STDPorcMortSem18
,A.DifPorcMortSem18_STDPorcMortSem18
,A.PorcMortSem19
,A.STDPorcMortSem19
,A.DifPorcMortSem19_STDPorcMortSem19
,A.PorcMortSem20
,A.STDPorcMortSem20
,A.DifPorcMortSem20_STDPorcMortSem20
,A.MortSemAcum8 
,A.MortSemAcum9 
,A.MortSemAcum10 
,A.MortSemAcum11 
,A.MortSemAcum12 
,A.MortSemAcum13 
,A.MortSemAcum14 
,A.MortSemAcum15 
,A.MortSemAcum16 
,A.MortSemAcum17 
,A.MortSemAcum18 
,A.MortSemAcum19 
,A.MortSemAcum20
,A.PorcMortSemAcum8
,A.STDPorcMortSemAcum8
,A.DifPorcMortSemAcum8_STDPorcMortSemAcum8
,A.PorcMortSemAcum9
,A.STDPorcMortSemAcum9
,A.DifPorcMortSemAcum9_STDPorcMortSemAcum9
,A.PorcMortSemAcum10
,A.STDPorcMortSemAcum10
,A.DifPorcMortSemAcum10_STDPorcMortSemAcum10
,A.PorcMortSemAcum11
,A.STDPorcMortSemAcum11
,A.DifPorcMortSemAcum11_STDPorcMortSemAcum11
,A.PorcMortSemAcum12
,A.STDPorcMortSemAcum12
,A.DifPorcMortSemAcum12_STDPorcMortSemAcum12
,A.PorcMortSemAcum13
,A.STDPorcMortSemAcum13
,A.DifPorcMortSemAcum13_STDPorcMortSemAcum13
,A.PorcMortSemAcum14
,A.STDPorcMortSemAcum14
,A.DifPorcMortSemAcum14_STDPorcMortSemAcum14
,A.PorcMortSemAcum15
,A.STDPorcMortSemAcum15
,A.DifPorcMortSemAcum15_STDPorcMortSemAcum15
,A.PorcMortSemAcum16
,A.STDPorcMortSemAcum16
,A.DifPorcMortSemAcum16_STDPorcMortSemAcum16
,A.PorcMortSemAcum17
,A.STDPorcMortSemAcum17
,A.DifPorcMortSemAcum17_STDPorcMortSemAcum17
,A.PorcMortSemAcum18
,A.STDPorcMortSemAcum18
,A.DifPorcMortSemAcum18_STDPorcMortSemAcum18
,A.PorcMortSemAcum19
,A.STDPorcMortSemAcum19
,A.DifPorcMortSemAcum19_STDPorcMortSemAcum19
,A.PorcMortSemAcum20
,A.STDPorcMortSemAcum20
,A.DifPorcMortSemAcum20_STDPorcMortSemAcum20
,A.PavoIni
,A.Pavo1
,A.Pavo2
,A.Pavo3
,A.Pavo4
,A.Pavo5
,A.Pavo6
,A.Pavo7
,A.PorcPavoIni
,A.PorcPavo1
,A.PorcPavo2
,A.PorcPavo3
,A.PorcPavo4
,A.PorcPavo5
,A.PorcPavo6
,A.PorcPavo7
,A.PesoSem8
,A.STDPesoSem8
,A.DifPesoSem8_STDPesoSem8
,A.PesoSem9
,A.STDPesoSem9
,A.DifPesoSem9_STDPesoSem9
,A.PesoSem10
,A.STDPesoSem10
,A.DifPesoSem10_STDPesoSem10
,A.PesoSem11
,A.STDPesoSem11
,A.DifPesoSem11_STDPesoSem11
,A.PesoSem12
,A.STDPesoSem12
,A.DifPesoSem12_STDPesoSem12
,A.PesoSem13
,A.STDPesoSem13
,A.DifPesoSem13_STDPesoSem13
,A.PesoSem14
,A.STDPesoSem14
,A.DifPesoSem14_STDPesoSem14
,A.PesoSem15
,A.STDPesoSem15
,A.DifPesoSem15_STDPesoSem15
,A.PesoSem16
,A.STDPesoSem16
,A.DifPesoSem16_STDPesoSem16
,A.PesoSem17
,A.STDPesoSem17
,A.DifPesoSem17_STDPesoSem17
,A.PesoSem18
,A.STDPesoSem18
,A.DifPesoSem18_STDPesoSem18
,A.PesoSem19
,A.STDPesoSem19
,A.DifPesoSem19_STDPesoSem19
,A.PesoSem20
,A.STDPesoSem20
,A.DifPesoSem20_STDPesoSem20
,A.PavosBBMortIncub
,A.EdadPadreCorralDescrip
,A.DiasAloj
,A.RuidosRespiratorios
,A.FechaDesinfeccion
,A.TipoCama
,A.FormaReutilizacion
,A.NReuso
,A.PDE
,A.PDT
,A.PromAmoniaco
,A.PromTemperatura
,A.ConsSem1
,A.ConsSem2
,A.ConsSem3
,A.ConsSem4
,A.ConsSem5
,A.ConsSem6
,A.ConsSem7
,A.PorcConsSem1
,A.PorcConsSem2
,A.PorcConsSem3
,A.PorcConsSem4
,A.PorcConsSem5
,A.PorcConsSem6
,A.PorcConsSem7
,A.DifPorcConsSem1_STDPorcConsSem1
,A.DifPorcConsSem2_STDPorcConsSem2
,A.DifPorcConsSem3_STDPorcConsSem3
,A.DifPorcConsSem4_STDPorcConsSem4
,A.DifPorcConsSem5_STDPorcConsSem5
,A.DifPorcConsSem6_STDPorcConsSem6
,A.DifPorcConsSem7_STDPorcConsSem7
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
,A.DifConsSem1_STDConsSem1
,A.DifConsSem2_STDConsSem2
,A.DifConsSem3_STDConsSem3
,A.DifConsSem4_STDConsSem4
,A.DifConsSem5_STDConsSem5
,A.DifConsSem6_STDConsSem6
,A.DifConsSem7_STDConsSem7
,A.CantPrimLesion
,A.CantSegLesion
,A.CantTerLesion
,A.PorcPrimLesion
,A.PorcSegLesion
,A.PorcTerLesion
,A.NomPrimLesion
,A.NomSegLesion
,A.NomTerLesion
,A.DiasSacaEfectivo
,A.U_PEAccidentados
,A.U_PEHigadoGraso
,A.U_PEHepatomegalia
,A.U_PEHigadoHemorragico
,A.U_PEInanicion
,A.U_PEProblemaRespiratorio
,A.U_PESCH
,A.U_PEEnteritis
,A.U_PEAscitis
,A.U_PEMuerteSubita
,A.U_PEEstresPorCalor
,A.U_PEHidropericardio
,A.U_PEHemopericardio
,A.U_PEUratosis
,A.U_PEMaterialCaseoso
,A.U_PEOnfalitis
,A.U_PERetencionDeYema
,A.U_PEErosionDeMolleja
,A.U_PEHemorragiaMusculos
,A.U_PESangreEnCiego
,A.U_PEPericarditis
,A.U_PEPeritonitis
,A.U_PEProlapso
,A.U_PEPicaje
,A.U_PERupturaAortica
,A.U_PEBazoMoteado
,A.U_PENoViable
,A.U_PEAerosaculitisG2
,A.U_PECojera
,A.U_PEHigadoIcterico
,A.U_PEMaterialCaseoso_po1ra
,A.U_PEMaterialCaseosoMedRetr
,A.U_PENecrosisHepatica
,A.U_PENeumonia
,A.U_PESepticemia
,A.U_PEVomitoNegro
,A.PorcAccidentados
,A.PorcHigadoGraso
,A.PorcHepatomegalia
,A.PorcHigadoHemorragico
,A.PorcInanicion
,A.PorcProblemaRespiratorio
,A.PorcSCH
,A.PorcEnteritis
,A.PorcAscitis
,A.PorcMuerteSubita
,A.PorcEstresPorCalor
,A.PorcHidropericardio
,A.PorcHemopericardio
,A.PorcUratosis
,A.PorcMaterialCaseoso
,A.PorcOnfalitis
,A.PorcRetencionDeYema
,A.PorcErosionDeMolleja
,A.PorcHemorragiaMusculos
,A.PorcSangreEnCiego
,A.PorcPericarditis
,A.PorcPeritonitis
,A.PorcProlapso
,A.PorcPicaje
,A.PorcRupturaAortica
,A.PorcBazoMoteado
,A.PorcNoViable
,A.PorcAerosaculitisG2
,A.PorcCojera
,A.PorcHigadoIcterico
,A.PorcMaterialCaseoso_po1ra
,A.PorcMaterialCaseosoMedRetr
,A.PorcNecrosisHepatica
,A.PorcNeumonia
,A.PorcSepticemia
,A.PorcVomitoNegro
,A.STDPorcConsGasInvierno
,A.STDPorcConsGasVerano
,A.MedSem1
,A.MedSem2
,A.MedSem3
,A.MedSem4
,A.MedSem5
,A.MedSem6
,A.MedSem7
,A.DiasMedSem1
,A.DiasMedSem2
,A.DiasMedSem3
,A.DiasMedSem4
,A.DiasMedSem5
,A.DiasMedSem6
,A.DiasMedSem7
,A.PorcAlojamientoXEdadPadre
,A.TipoOrigen
,A.Peso5Dias
,A.STDPeso5Dias
,A.DifPeso5Dias_STDPeso5Dias
,A.NoViableSem1
,A.NoViableSem2
,A.NoViableSem3
,A.NoViableSem4
,A.NoViableSem5
,A.NoViableSem6
,A.NoViableSem7
,A.PorcNoViableSem1
,A.PorcNoViableSem2
,A.PorcNoViableSem3
,A.PorcNoViableSem4
,A.PorcNoViableSem5
,A.PorcNoViableSem6
,A.PorcNoViableSem7
,A.NoViableSem8
,A.PorcNoViableSem8
,A.pk_tipogranja
from {database_name}.ft_consolidado_corral A
left join GasGalpon B on A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_galpon = B.pk_galpon
where A.pk_empresa = 1
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Corral2.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral_new")

df_ft_consolidado_Corral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Corral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral"
}
df_ft_consolidado_Corral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral_new")

print('carga UPD ft_consolidado_Corral 2')
#Se muestra ft_dinuts
df_ft_dinuts = spark.sql(f"""
select  LST.idempresa as pk_empresa,
		case when Age = 0 then 141 else Age end as pk_diasvida,
		CASE WHEN nstandard like '%hem%' THEN 1 WHEN nstandard like '%mac%' THEN 2 ELSE 4 END AS pk_sexo,
		LST.pk_standard,
		LST.cstandard,
		substring(LST.cstandard,7,15) dinuts,
		LST.lstandard
		,COALESCE(BSD.U_PesoVivo,0) as STDPeso
from {database_name}.br_BrimStandardsData BSD
LEFT JOIN {database_name}.br_ProteinStandardVersions PSV on BSD.ProteinStandardVersionsIRN = PSV.IRN
LEFT JOIN {database_name}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50))
where idempresa = 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_dinuts"
}
df_ft_dinuts.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_dinuts")
print('carga ft_dinuts')
#Se muestra UPDft_dinuts
df_UPDft_dinuts = spark.sql(f"""
WITH pruebaDinuts AS (select * from {database_name}.ft_dinuts where pk_standard in (295,296,297,298))
select a.pk_empresa
,a.pk_diasvida
,a.pk_sexo
,a.pk_standard
,a.cstandard
,a.dinuts
,a.lstandard
,a.STDPeso
,case when a.pk_standard = 399 then B.STDPeso when a.pk_standard = 398 then C.STDPeso when a.pk_standard = 400 then d.STDPeso when a.pk_standard = 401 then e.STDPeso else 0 end STDPesoAnt 
from {database_name}.ft_dinuts A
left join pruebaDinuts b on a.pk_empresa = B.pk_empresa and a.pk_diasvida = b.pk_diasvida and a.pk_sexo = b.pk_sexo and b.pk_standard = 297
left join pruebaDinuts c on a.pk_empresa = c.pk_empresa and a.pk_diasvida = c.pk_diasvida and a.pk_sexo = c.pk_sexo and c.pk_standard = 295
left join pruebaDinuts d on a.pk_empresa = d.pk_empresa and a.pk_diasvida = d.pk_diasvida and a.pk_sexo = d.pk_sexo and d.pk_standard = 296
left join pruebaDinuts e on a.pk_empresa = e.pk_empresa and a.pk_diasvida = e.pk_diasvida and a.pk_sexo = e.pk_sexo and e.pk_standard = 298
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_dinuts_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_dinuts.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_dinuts_new")

df_ft_dinuts_nueva = spark.sql(f"""SELECT * from {database_name}.ft_dinuts_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_dinuts")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_dinuts"
}
df_ft_dinuts_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_dinuts")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_dinuts_new")

print('carga UPD ft_dinuts')
#Se muestra UPDft_consolidado_Lote
df_UPDft_consolidado_Lote = spark.sql(f"""
select A.fecha               
,A.pk_mes              
,A.pk_empresa          
,A.pk_division         
,A.pk_zona             
,A.pk_subzona          
,A.pk_plantel          
,A.pk_lote             
,A.pk_tipoproducto     
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.pk_diasvida         
,A.complexentityno     
,A.fechacierre         
,A.fechaalojamiento    
,A.fechacrianza        
,A.fechainiciogranja   
,A.fechainiciosaca     
,A.fechafinsaca        
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
,A.eventdate           
,COALESCE(B.categoria,'-')categoria           
,A.flagatipico         
,A.flagartatipico      
,A.edadpadrelote       
,A.porcpollojoven      
,A.porcpesoalomenor    
,A.porcgalponesmixtos  
,A.pesoalo             
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
,A.stdporcconsgasinvierno
,A.stdporcconsgasverano
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
from {database_name}.ft_consolidado_Lote A
left join {database_name}.lk_ft_Excel_categoria B on A.pk_lote = B.pk_lote
where pk_empresa =1 and (a.categoria is null or a.categoria ='-')
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Lote.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote_new")

df_ft_consolidado_Lote_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Lote_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote"
}
df_ft_consolidado_Lote_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote_new")

print('carga UPD ft_consolidado_Lote')
#Se muestra ft_consolidado_galpon
df_UPDft_consolidado_galpon = spark.sql(f"""
select 
A.fecha               
,A.pk_empresa          
,A.pk_division         
,A.pk_zona             
,A.pk_subzona          
,A.pk_plantel          
,A.pk_lote             
,A.pk_galpon           
,A.pk_tipoproducto     
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.pk_diasvida         
,A.complexentityno     
,A.fechacierre         
,A.fechaalojamiento    
,A.fechacrianza        
,A.fechainiciogranja   
,A.fechainiciosaca     
,A.fechafinsaca        
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
,A.eventdate           
,COALESCE(B.categoria,'-') categoria
,A.flagatipico         
,A.edadpadregalpon     
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
,A.stdporcconsgasinvierno
,A.stdporcconsgasverano
,A.pk_tipogranja
from {database_name}.ft_consolidado_galpon A
left join {database_name}.lk_ft_Excel_categoria B on A.pk_lote = B.pk_lote
where pk_empresa =1 and (a.categoria is null or a.categoria ='-')
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_galpon_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_galpon.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_galpon_new")

df_ft_consolidado_galpon_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_galpon_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_galpon")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_galpon"
}
df_ft_consolidado_galpon_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_galpon")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_galpon_new")

print('carga UPD ft_consolidado_galpon')
#Se muestra dias
df_UPDft_consolidado_corral = spark.sql(f"""
select 
 A.fecha               
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
,A.eventdate           
,COALESCE(B.categoria,'-') categoria
,A.pesoalo             
,A.flagatipico         
,A.edadpadrecorral     
,A.gasgalpon           
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
,A.ruidosrespiratorios 
,A.fechadesinfeccion   
,A.tipocama            
,A.formareutilizacion  
,A.nreuso              
,A.pde                 
,A.pdt                 
,A.promamoniaco        
,A.promtemperatura     
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
from {database_name}.ft_consolidado_Corral A
left join {database_name}.lk_ft_Excel_categoria B on A.pk_lote = B.pk_lote
where pk_empresa =1 and (a.categoria is null or a.categoria ='-')
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_corral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_corral.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_corral_new")

df_ft_consolidado_corral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_corral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_corral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_corral"
}
df_ft_consolidado_corral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_corral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_corral_new")

print('carga UPD ft_consolidado_corral')
#Se muestra dias
df_UPDft_consolidado_Diario = spark.sql(f"""
select 
 A.fecha               
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
,A.pk_grupoconsumo     
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.pk_diasvida         
,A.pk_semanavida       
,A.complexentityno     
,A.nacimiento          
,A.edad                
,A.fechacierre         
,A.fechaalojamiento    
,A.pobinicial          
,A.inventario          
,A.avesrendidas        
,A.avesrendidasacum    
,A.kilosrendidos       
,A.kilosrendidosacum   
,A.mortdia             
,A.mortacum            
,A.mortsem             
,A.stdmortdia          
,A.stdmortacum         
,A.stdmortsem          
,A.porcmortdia         
,A.porcmortacum        
,A.porcmortsem         
,A.stdporcmortdia      
,A.stdporcmortacum     
,A.stdporcmortsem      
,A.difporcmort_std     
,A.difporcmortacum_stdacum
,A.difporcmortsem_stdsem
,A.areagalpon          
,A.consumo             
,A.consacum            
,A.porcconsdia         
,A.porcconsacum        
,A.stdconsdia          
,A.stdconsacum         
,A.difporccons_std     
,A.difporcconsacum_stdacum
,A.preinicio           
,A.inicio              
,A.acabado             
,A.terminado           
,A.finalizador         
,A.pesomasstd          
,A.peso                
,A.pesosem             
,A.stdpeso             
,A.difpeso_std         
,A.pesoalo             
,A.tasacre             
,A.gananciadia         
,A.gananciasem         
,A.stdganancia         
,A.gananciaesp         
,A.difpesoactpesoant   
,A.cantdia             
,A.gananciapesodia     
,A.ica                 
,A.stdica              
,A.icaajustado         
,A.icaventa            
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
,A.u_pecaja            
,A.u_pegota            
,A.u_peintoxicacion    
,A.u_peretrazos        
,A.u_peeliminados      
,A.u_peahogados        
,A.u_peecoli           
,A.u_pedescarte        
,A.u_peotros           
,A.u_pecoccidia        
,A.u_pedeshidratados   
,A.u_pehepatitis       
,A.u_petraumatismo     
,A.pigmentacion        
,COALESCE(B.categoria,'-') categoria
,A.padremayor          
,A.flagatipico         
,A.pesohvo             
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
,A.pavoini             
,A.pavo1               
,A.pavo2               
,A.pavo3               
,A.pavo4               
,A.pavo5               
,A.pavo6               
,A.pavo7               
,A.edadpadrecorraldescrip
,A.u_causapesobajo     
,A.u_accionpesobajo    
,A.stdconsdiakg        
,A.stdconsacumkg       
,A.difcons_std         
,A.difconsacum_stdacum 
,A.tipoalimento        
,A.porcpreinicioacum   
,A.porcinicioacum      
,A.porcacabadoacum     
,A.porcterminadoacum   
,A.porcfinalizadoracum 
,A.ruidosrespiratorios 
,A.productoconsumo     
,A.stdconssem          
,A.stdporcconssem      
,A.conssem             
,A.porcconssem         
,A.difconssem_stdconssem
,A.difporcconssem_stdporcconssem
,A.u_consumogasinvierno
,A.u_consumogasverano  
,A.saldoaves           
,A.stdconsdiakgsaldoaves
,A.porcconsdiasaldoaves
,A.difcons_stdkgsaldoaves
,A.difporccons_stdsaldoaves
,A.saldoavesconsumo    
,A.porcmortdiaacum     
,A.difporcmortdiaacum_stddiaacum
,A.tasanoviable        
,A.semporcprilesion    
,A.semporcseglesion    
,A.semporcterlesion    
,A.tipoalimentoxtipoproducto
,A.stdconsdiaxtipoalimentoxtipoproducto
,A.stdconsdiakgxtipoalimentoxtipoproducto
,A.listaformulano      
,A.listaformulaname    
,A.tipoorigen          
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
from {database_name}.ft_consolidado_Diario A
left join {database_name}.lk_ft_Excel_categoria B on A.pk_lote = B.pk_lote
where pk_empresa =1 and (a.categoria is null or a.categoria ='-')
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Diario.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_new")

df_ft_consolidado_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario"
}
df_ft_consolidado_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario_new")

print('carga UPD ft_consolidado_Diario')
#Se muestra PlantillaFechaEdadAgregarMax
df_PlantillaFechaEdadAgregarMax = spark.sql(f"""
with MaxFechaVentas as (select max(fecha) fechaMax, ComplexEntityNo,pk_empresa,pk_division from {database_name}.ft_ventas_cd where pk_empresa = 1 group by ComplexEntityNo,pk_empresa,pk_division), 
PlantillaFechaEdadMax as (select max(fecha) fecha, max(pk_diasvida) pk_diasvida,complexentityno,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_grupoconsumo,pk_especie,max(pk_estado) pk_estado,pk_administrador,pk_proveedor,FechaCierre,FechaAlojamiento
from {database_name}.ft_consolidado_diario where pk_empresa = 1
group by complexentityno,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_grupoconsumo,pk_especie,pk_administrador,pk_proveedor,FechaCierre,FechaAlojamiento
order by fecha)

select c.fecha,a.pk_diasvida + (ROW_NUMBER() OVER(PARTITION BY a.complexentityno ORDER BY c.fecha ASC)) pk_diasvida,a.complexentityno,a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote,a.pk_galpon,a.pk_sexo,a.pk_standard,a.pk_producto,a.pk_grupoconsumo,a.pk_especie,a.pk_estado,a.pk_administrador,a.pk_proveedor,a.FechaCierre,a.FechaAlojamiento
from PlantillaFechaEdadMax a
left join MaxFechaVentas b on a.complexentityno = B.complexentityno
cross join {database_name}.lk_tiempo c
where c.fecha > a.fecha and c.fecha <= b.fechaMax order by pk_diasvida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PlantillaFechaEdadAgregarMax"
}
df_PlantillaFechaEdadAgregarMax.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PlantillaFechaEdadAgregarMax")
print('carga PlantillaFechaEdadAgregarMax')
#Se muestra ft_PlantillaFechaEdadTotal
df_ft_PlantillaFechaEdadTotal = spark.sql(f"""
select fecha, pk_diasvida,complexentityno,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_grupoconsumo,pk_especie,pk_estado,pk_administrador,pk_proveedor,FechaCierre,FechaAlojamiento
from {database_name}.ft_consolidado_diario where pk_empresa = 1 
union
select * from {database_name}.PlantillaFechaEdadAgregarMax
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_PlantillaFechaEdadTotal"
}
df_ft_PlantillaFechaEdadTotal.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_PlantillaFechaEdadTotal")
print('carga ft_PlantillaFechaEdadTotal')
#Se muestra ft_consolidado_Diario_hist
df_ft_consolidado_Diario_hist = spark.sql(f"""
select * from {database_name}.ft_consolidado_Diario where pk_empresa = 1 and (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_hist"
}
df_ft_consolidado_Diario_hist.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_hist")
print('carga ft_consolidado_Diario_hist')
#Se muestra PesoSemanal
df_PesoSemanal = spark.sql(f"""
with 
ft_peso_d as (select ComplexEntityNo,pk_semanavida,pk_diasvida,STDPeso from {database_name}.ft_peso_diario)
,temp_PesoSemanal as (
select a.ComplexEntityNo,a.pk_semanavida,min(a.pk_diasvida) pk_diasvida,min(a.Peso) Peso,min(COALESCE(b.STDPeso,0)) STDPeso,c.PlantelCampana
from {database_name}.ft_peso_diario a
left join ft_peso_d b on a.ComplexEntityNo = b.ComplexEntityNo and a.pk_diasvida-1 = b.pk_diasvida 
left join {database_name}.lk_lote c on a.pk_lote = c.pk_lote
where a.Peso <> 0 and a.pk_empresa = 1 and (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
group by a.ComplexEntityNo,a.pk_semanavida,c.PlantelCampana)
,temp_PesoSemanal2 as (select PlantelCampana, max(pk_semanavida) pk_semanavida from temp_PesoSemanal group by PlantelCampana)
,artificioPeso as (
select ComplexEntityNo,A.PlantelCampana,max(A.pk_semanavida) pk_semanavida,max(XA.pk_semanavida) as pk_semanavidacampana
from temp_PesoSemanal A 
left join temp_PesoSemanal2 XA on XA.PlantelCampana = A.PlantelCampana group by ComplexEntityNo,A.PlantelCampana)

select * from temp_PesoSemanal
union all
select C.ComplexEntityNo,
		B.pk_semanavida,
		D.pk_diasvida,
		C.Peso, 
		C.STDPeso, 
		C.PlantelCampana
from artificioPeso A
left join temp_PesoSemanal C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_semanavida = C.pk_semanavida
cross join {database_name}.lk_semanavida B
left join (select min(pk_diasvida) pk_diasvida, pk_semanavida from {database_name}.lk_diasvida group by pk_semanavida) D on B.pk_semanavida = D.pk_semanavida
where b.pk_semanavida > A.pk_semanavida and b.pk_semanavida <= A.pk_semanavidacampana
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PesoSemanal"
}
df_PesoSemanal.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PesoSemanal")
print('carga PesoSemanal')
#Se muestra Preventa1
df_Preventa1_1 = spark.sql(f"""
with AvesGranjaPrimera as (select ComplexEntityNo, sum(AvesGranja) AvesGranjaPrimera from {database_name}.ft_ventas_cd
where pk_productos in (150,483,536,539,556,753,798,1184,1300,1415,1417,1638,1658,1719,2429,2485,2499,2520,2564,2581,2623,2673) group by ComplexEntityNo) 
select 
a.fecha, 
a.PK_empresa, 
a.PK_division, 
a.PK_zona, 
a.PK_subzona, 
a.PK_plantel, 
a.PK_lote, 
a.PK_galpon, 
a.PK_sexo, 
a.PK_standard,
a.PK_producto,
a.PK_grupoconsumo, 
a.PK_especie, 
a.PK_estado, 
a.PK_administrador, 
a.PK_proveedor, 
a.PK_diasvida, 
a.PK_semanavida, 
a.ComplexEntityNo, 
a.Nacimiento FechaIngBB, 
a.PobInicial,
c.PorcMortSem1, 
a.MortDia, 
a.PorcMortDia, 
a.MortAcum, 
a.PorcMortAcum, 
COALESCE(d.Peso,0) Peso, 
COALESCE(d.STDPeso,0) STDPeso, 
a.ICA, 
a.Inventario,
a.AvesRendidas, 
a.AvesRendidasAcum,
V.AvesGranjaPrimera,
c.Seleccion,
1 FlagTipoRegistro, 
b.PlantelCampana,
e.gas GasGalpon,
e.cama CamaGalpon,
c.gas GasLote,
c.cama CamaLote
from {database_name}.ft_consolidado_diario a
left join {database_name}.lk_lote b on a.pk_lote = b.pk_lote
left join {database_name}.ft_consolidado_corral c  on a.ComplexEntityNo = c.ComplexEntityNo
left join {database_name}.PesoSemanal d on a.ComplexEntityNo = d.ComplexEntityNo and a.pk_semanavida = d.pk_semanavida
left join {database_name}.ft_consumogascamaagua e on substring(a.ComplexEntityNo,1,(LENGTH(a.ComplexEntityNo)-3)) = e.ComplexEntityNo
left join AvesGranjaPrimera V on V.ComplexEntityNo = A.ComplexEntityNo 
where a.pk_empresa = 1 and (date_format(CAST(a.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(a.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Preventa1_1"
}
df_Preventa1_1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Preventa1_1")
print('carga Preventa1_1')
#Se muestra RegistrosVentasFaltantes
df_RegistrosVentasFaltantes = spark.sql(f"""
with 
 ar as (SELECT ComplexEntityNo, MAX(COALESCE(avesgranjaacum,0)) AvesRendidas FROM {database_name}.ft_ventas_CD SS group by ComplexEntityNo)
,ag as (SELECT fecha,ComplexEntityNo, MAX(COALESCE(avesgranjaacum,0)) avesgranjaacum FROM {database_name}.ft_ventas_CD SS group by ComplexEntityNo,fecha)
,temp as (select a.fecha,a.pk_diasvida,b.pk_semanavida,a.ComplexEntityNo,
        COALESCE(SS.AvesRendidas,0.0) AS AvesRendidas,
        COALESCE(SA.avesgranjaacum,0.0) AS AvesRendidasAcum
      from {database_name}.ft_PlantillaFechaEdadTotal A
      left join ar SS on A.ComplexEntityNo = SS.ComplexEntityNo
      left join ag SA on A.ComplexEntityNo = SA.ComplexEntityNo AND A.fecha >= SA.fecha
      left join {database_name}.lk_diasvida B on A.pk_diasvida = B.pk_diasvida)
select A.*
from temp A
left join {database_name}.Preventa1 B on B.fecha = A.fecha and B.ComplexEntityNo = A.ComplexEntityNo
where B.ComplexEntityNo is null and (date_format(CAST(a.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(a.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})  
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/RegistrosVentasFaltantes"
}
df_RegistrosVentasFaltantes.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.RegistrosVentasFaltantes")
print('carga RegistrosVentasFaltantes')
#Se muestra Preventa1_2
df_Preventa1_2 = spark.sql(f"""
with MaxPreventa1 as (select ComplexEntityNo,max(fecha) fecha,max(pk_diasvida) pk_diasvida,max(pk_semanavida) pk_semanavida from {database_name}.Preventa1_1 group by ComplexEntityNo)
select 
C.fecha,
A.pk_empresa,
A.pk_division,
A.pk_zona,
A.pk_subzona,
A.pk_plantel,
A.pk_lote,
A.pk_galpon,
A.pk_sexo,
A.pk_standard,
A.pk_producto,
A.pk_grupoconsumo,
A.pk_especie,
A.pk_estado,
A.pk_administrador,
A.pk_proveedor,
C.pk_diasvida,
C.pk_semanavida,
A.ComplexEntityNo,
A.FechaIngBB,
A.PobInicial,
A.PorcMortSem1,
A.MortDia,
A.PorcMortDia,
A.MortAcum,
A.PorcMortAcum,
A.Peso,
A.STDPeso,
A.ICA,
A.Inventario,
C.AvesRendidas,
C.AvesRendidasAcum,
COALESCE(A.AvesGranjaPrimera,0) AvesGranjaPrimera,
A.Seleccion,
2 FlagTipoRegistro,
A.PlantelCampana,
A.GasGalpon,
A.CamaGalpon,
A.GasLote,
A.CamaLote
from {database_name}.Preventa1_1 A
left join MaxPreventa1 B ON A.ComplexEntityNo = B.ComplexEntityNo and A.fecha = B.fecha
left join {database_name}.RegistrosVentasFaltantes C ON A.ComplexEntityNo = C.ComplexEntityNo
where B.ComplexEntityNo is not null and C.ComplexEntityNo is not null and pk_empresa = 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Preventa1_2"
}
df_Preventa1_2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Preventa1_2")
print('carga Preventa1_2')
#Se muestra Preventa1
df_Preventa1 = spark.sql(f"""
with 
 preventa as (select * from {database_name}.Preventa1_1 union all select * from {database_name}.Preventa1_2)
,fechacampana as (select PlantelCampana, max(fecha) fecha from preventa group by PlantelCampana)
,artificio as (select ComplexEntityNo,A.PlantelCampana,max(A.fecha) fecha, max(XA.fecha) as pk_fechacampana
                from Preventa A 
                left join fechacampana XA on XA.PlantelCampana = A.PlantelCampana
                where pk_empresa = 1 group by ComplexEntityNo,A.PlantelCampana)
select 
B.fecha,
C.pk_empresa,
C.pk_division,
C.pk_zona,
C.pk_subzona,
C.pk_plantel,
C.pk_lote,
C.pk_galpon,
C.pk_sexo,
C.pk_standard,
C.pk_producto,
C.pk_grupoconsumo,
C.pk_especie,
C.pk_estado,
C.pk_administrador,
C.pk_proveedor,
C.pk_diasvida,
C.pk_semanavida,
C.ComplexEntityNo,
C.FechaIngBB,
C.PobInicial,
C.PorcMortSem1,
C.MortDia,
C.PorcMortDia,
C.MortAcum,
C.PorcMortAcum,
C.Peso,
C.STDPeso,
C.ICA,
C.Inventario,
C.AvesRendidas,
C.AvesRendidasAcum,
C.AvesGranjaPrimera,
C.Seleccion,
3 FlagTipoRegistro,
C.PlantelCampana,
C.GasGalpon,
C.CamaGalpon,
C.GasLote,
C.CamaLote
from artificio A
left join preventa C on A.ComplexEntityNo = C.ComplexEntityNo and A.fecha = C.fecha
cross join {database_name}.lk_tiempo B
where b.fecha > A.fecha and b.fecha <= A.pk_fechacampana and pk_empresa = 1
union all 
select * from preventa
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Preventa1"
}
df_Preventa1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Preventa1")
print('carga Preventa1')
#Se muestra Preventa2
df_Preventa2 = spark.sql(f"""
with 
 aPreventa1 as (select fecha,PlantelCampana,sum(PobInicial) PobInicial from {database_name}.Preventa1 group by fecha,PlantelCampana)
,aPreventa2 as (select fecha,PlantelCampana,sum(MortDia) MortDia from {database_name}.Preventa1 group by fecha,PlantelCampana)
,aPreventa3 as (select fecha,PlantelCampana,sum(MortAcum) MortAcum from {database_name}.Preventa1 group by fecha,PlantelCampana)
select
B.fecha,
B.PK_empresa,
B.PK_division,
B.PK_zona,
B.PK_subzona,
B.PK_plantel,
B.PK_lote,
B.PK_galpon,
B.PK_sexo,
B.PK_standard,
B.PK_producto,
B.PK_grupoconsumo,
B.PK_especie,
B.PK_estado,
B.PK_administrador,
B.PK_proveedor,
B.PK_diasvida,
B.PK_semanavida,
B.ComplexEntityNo,
B.FechaIngBB,
B.PobInicial,
B.PorcMortSem1,
B.MortDia,
B.PorcMortDia,
B.MortAcum,
B.PorcMortAcum,
B.Peso,
B.STDPeso,
B.ICA,
B.Inventario,
B.AvesRendidas,
B.AvesRendidasAcum,
B.AvesGranjaPrimera,
B.Seleccion,
B.FlagTipoRegistro,
B.PlantelCampana,
B.GasGalpon,
B.CamaGalpon,
B.GasLote,
B.CamaLote,
C.PobInicial PobInicialXFecha,
D.MortDia MortDiaXFecha,
E.MortAcum MortAcumXFecha
from {database_name}.Preventa1 B
left join aPreventa1 C on B.PlantelCampana = C.PlantelCampana and B.fecha=C.fecha
left join aPreventa2 D on B.PlantelCampana = D.PlantelCampana and B.fecha=D.fecha
left join aPreventa3 E on B.PlantelCampana = E.PlantelCampana and B.fecha=E.fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Preventa2"
}
df_Preventa2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Preventa2")
print('carga Preventa2')
#Se muestra ft_Preventa
df_ft_Preventa = spark.sql(f"""
select
B.fecha,
B.pk_empresa,
B.pk_division,
B.pk_zona,
B.pk_subzona,
B.pk_plantel,
B.pk_lote,
B.pk_galpon,
B.pk_sexo,
B.pk_standard,
B.pk_producto,
B.pk_grupoconsumo,
B.pk_especie,
B.pk_estado,
B.pk_administrador,
B.pk_proveedor,
B.pk_diasvida,
B.pk_semanavida,
B.ComplexEntityNo,
round((B.pk_diasvida*1.0)/7,1) + ((B.pk_diasvida*1.0)%7)/10 as Edad,
B.FechaIngBB,
B.PobInicial,
ROUND(B.PorcMortSem1,2) PorcMortSem1,
B.MortDia,
B.PorcMortDia,
(B.MortDiaXFecha/COALESCE(B.PobInicialXFecha,0))*100 PorcMortDiaTotal,
B.MortAcum,
B.PorcMortAcum,
(B.MortAcumXFecha/COALESCE(B.PobInicialXFecha,0))*100 PorcMortAcumTotal,
B.Peso,
B.STDPeso,
B.Peso-B.STDPeso DifPesoRealSTD,
B.ICA,
COALESCE(B.AvesGranjaPrimera,0) PavosPrimera,
B.Seleccion,
ROUND(((B.Seleccion / COALESCE(B.PobInicial,0))*100),2) PorcDespacho,
B.Inventario - B.AvesRendidasAcum Saldo,
B.FlagTipoRegistro,
B.PlantelCampana,
B.GasGalpon,
B.CamaGalpon,
B.GasLote,
B.CamaLote
from {database_name}.Preventa2 B
where (date_format(CAST(fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Preventa"
}
df_ft_Preventa.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_Preventa")
print('carga ft_Preventa')
#Se muestra AgrupadoLoteMensual
df_AgrupadoLoteMensual = spark.sql(f"""
select
 a.fecha
,a.pk_mes
,b.anio
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_tipoproducto
,a.pk_especie
,a.pk_estado
,a.pk_administrador
,a.pk_proveedor
,a.ComplexEntityNo
,a.flagartatipico
,a.PobInicial
,a.ICA 
,a.ICA * a.PobInicial ICAXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.ICA ASC) AS TerciosIca
,a.PorMort
,a.PorMort * a.PobInicial PorMortXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico  ORDER BY a.PorMort ASC) AS TerciosPorMort
,a.PorcMortSem1
,a.PorcMortSem1 * a.PobInicial PorcMortSem1XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1
,a.PorcMortSem2
,a.PorcMortSem2 * a.PobInicial PorcMortSem2XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2
,a.PorcMortSem3
,a.PorcMortSem3 * a.PobInicial PorcMortSem3XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3
,a.PorcMortSem4
,a.PorcMortSem4 * a.PobInicial PorcMortSem4XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4
,a.PorcMortSem5
,a.PorcMortSem5 * a.PobInicial PorcMortSem5XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5
,a.IEP
,a.IEP * a.PobInicial IEPXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.IEP DESC) AS TerciosIEP
,a.PesoProm
,a.PesoProm * a.PobInicial PesoPromXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoProm DESC) AS TerciosPesoProm
,a.PesoSem1
,a.PesoSem1 * a.PobInicial PesoSem1XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1
,a.PesoSem2
,a.PesoSem2 * a.PobInicial PesoSem2XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2
,a.PesoSem3
,a.PesoSem3 * a.PobInicial PesoSem3XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3
,a.PesoSem4
,a.PesoSem4 * a.PobInicial PesoSem4XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4
,a.PesoSem5
,a.PesoSem5 * a.PobInicial PesoSem5XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5
,a.Gas
,a.Gas * a.PobInicial GasXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.Gas ASC) AS TerciosGas
,a.Cama
,a.Cama * a.PobInicial CamaXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.Cama ASC) AS TerciosCama
,a.Agua
,a.Agua * a.PobInicial AguaXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.Agua ASC) AS TerciosAgua
,a.AvesXm2
,a.AvesXm2 * a.PobInicial AvesXm2XPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2
,a.GananciaDiaVenta
,a.GananciaDiaVenta * a.PobInicial GananciaDiaVentaXPobInicial
,NTILE(3) OVER(PARTITION BY a.pk_mes, a.pk_tipoproducto, a.flagartatipico ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVenta
from {database_name}.ft_consolidado_Lote a
left join {database_name}.lk_tiempo B on A.fecha = B.fecha
where a.pk_division = 3 and a.pk_empresa = 1 and a.pk_estado in (2,3)
and (date_format(CAST(a.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(a.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
order by a.pk_mes
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AgrupadoLoteMensual"
}
df_AgrupadoLoteMensual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AgrupadoLoteMensual")
print('carga AgrupadoLoteMensual')
#Se muestra AgrupadoPlantelAnual
df_AgrupadoPlantelAnual = spark.sql(f"""
select
 max(A.fecha) fecha
,max(A.pk_mes) pk_mes
,A.anio
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_tipoproducto
,A.pk_especie
,A.pk_estado
,70 pk_administrador
,A.pk_proveedor
,A.FlagArtAtipico
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-5)) ComplexEntityNo
,COALESCE(SUM(PobInicial),0) PobInicial
,COALESCE(SUM(ICAXPobInicial),0) ICAXPobInicial
,COALESCE(SUM(ICAXPobInicial) / SUM(PobInicial),0) ICA
,COALESCE(SUM(PorMortXPobInicial),0) PorMortXPobInicial
,COALESCE(SUM(PorMortXPobInicial) / SUM(PobInicial),0) PorMort
,COALESCE(SUM(PorcMortSem1XPobInicial),0) PorcMortSem1XPobInicial
,COALESCE(SUM(PorcMortSem1XPobInicial) / SUM(PobInicial),0) PorcMortSem1
,COALESCE(SUM(PorcMortSem2XPobInicial),0) PorcMortSem2XPobInicial
,COALESCE(SUM(PorcMortSem2XPobInicial) / SUM(PobInicial),0) PorcMortSem2
,COALESCE(SUM(PorcMortSem3XPobInicial),0) PorcMortSem3XPobInicial
,COALESCE(SUM(PorcMortSem3XPobInicial) / SUM(PobInicial),0) PorcMortSem3
,COALESCE(SUM(PorcMortSem4XPobInicial),0) PorcMortSem4XPobInicial
,COALESCE(SUM(PorcMortSem4XPobInicial) / SUM(PobInicial),0) PorcMortSem4
,COALESCE(SUM(PorcMortSem5XPobInicial),0) PorcMortSem5XPobInicial
,COALESCE(SUM(PorcMortSem5XPobInicial) / SUM(PobInicial),0) PorcMortSem5
,COALESCE(SUM(IEPXPobInicial),0) IEPXPobInicial
,COALESCE(SUM(IEPXPobInicial) / SUM(PobInicial),0) IEP
,COALESCE(SUM(PesoPromXPobInicial),0) PesoPromXPobInicial
,COALESCE(SUM(PesoPromXPobInicial) / SUM(PobInicial),0) PesoProm
,COALESCE(SUM(PesoSem1XPobInicial),0) PesoSem1XPobInicial
,COALESCE(SUM(PesoSem1XPobInicial) / SUM(PobInicial),0) PesoSem1
,COALESCE(SUM(PesoSem2XPobInicial),0) PesoSem2XPobInicial
,COALESCE(SUM(PesoSem2XPobInicial) / SUM(PobInicial),0) PesoSem2
,COALESCE(SUM(PesoSem3XPobInicial),0) PesoSem3XPobInicial
,COALESCE(SUM(PesoSem3XPobInicial) / SUM(PobInicial),0) PesoSem3
,COALESCE(SUM(PesoSem4XPobInicial),0) PesoSem4XPobInicial
,COALESCE(SUM(PesoSem4XPobInicial) / SUM(PobInicial),0) PesoSem4
,COALESCE(SUM(PesoSem5XPobInicial),0) PesoSem5XPobInicial
,COALESCE(SUM(PesoSem5XPobInicial) / SUM(PobInicial),0) PesoSem5
,COALESCE(SUM(GasXPobInicial),0) GasXPobInicial
,COALESCE(SUM(GasXPobInicial) / SUM(PobInicial),0) Gas
,COALESCE(SUM(CamaXPobInicial),0) CamaXPobInicial
,COALESCE(SUM(CamaXPobInicial) / SUM(PobInicial),0) Cama
,COALESCE(SUM(AguaXPobInicial),0) AguaXPobInicial
,COALESCE(SUM(AguaXPobInicial) / SUM(PobInicial),0) Agua
,COALESCE(SUM(AvesXm2XPobInicial),0) AvesXm2XPobInicial
,COALESCE(SUM(AvesXm2XPobInicial) / SUM(PobInicial),0) AvesXm2
,COALESCE(SUM(GananciaDiaVentaXPobInicial),0) GananciaDiaVentaXPobInicial
,COALESCE(SUM(GananciaDiaVentaXPobInicial) / SUM(PobInicial),0) GananciaDiaVenta
from {database_name}.AgrupadoLoteMensual A
group by A.anio,A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_tipoproducto,A.pk_especie,A.pk_estado,A.pk_proveedor
,A.FlagArtAtipico,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-5))
order by substring(A.complexentityno,1,(LENGTH(A.complexentityno)-5)),A.anio
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AgrupadoPlantelAnual"
}
df_AgrupadoPlantelAnual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AgrupadoPlantelAnual")
print('carga AgrupadoPlantelAnual')
#Se muestra ft_Tercios_Lote_Plantel
df_ft_Tercios_Lote_Plantel = spark.sql(f"""
with AgrupadoPlantelAnual2 as (
select
 a.fecha
,a.pk_mes
,a.anio
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,0 pk_lote
,a.pk_tipoproducto
,a.pk_especie
,a.pk_estado
,a.pk_administrador
,a.pk_proveedor
,a.FlagArtAtipico
,a.ComplexEntityNo
,a.PobInicial
,a.ICA 
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico ORDER BY a.ICA ASC) AS TerciosIca
,a.PorMort
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorMort ASC) AS TerciosPorMort
,a.PorcMortSem1
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1
,a.PorcMortSem2
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2
,a.PorcMortSem3
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3
,a.PorcMortSem4
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4
,a.PorcMortSem5
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5
,a.IEP
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.IEP DESC) AS TerciosIEP
,a.PesoProm
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoProm DESC) AS TerciosPesoProm
,a.PesoSem1
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1
,a.PesoSem2
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2
,a.PesoSem3
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3
,a.PesoSem4
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4
,a.PesoSem5
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5
,a.Gas
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.Gas ASC) AS TerciosGas
,a.Cama
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.Cama ASC) AS TerciosCama
,a.Agua
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.Agua ASC) AS TerciosAgua
,a.AvesXm2
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2
,a.GananciaDiaVenta
,NTILE(3) OVER(PARTITION BY a.anio, a.pk_tipoproducto, a.FlagArtAtipico  ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVenta
from {database_name}.AgrupadoPlantelAnual a)

select
'MENSUAL' frecuencia
,fecha
,pk_mes
,anio
,pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_lote
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,FlagArtAtipico
,ComplexEntityNo
,PobInicial
,ICA
,TerciosIca
,PorMort
,TerciosPorMort
,PorcMortSem1
,TerciosPorcMortSem1
,PorcMortSem2
,TerciosPorcMortSem2
,PorcMortSem3
,TerciosPorcMortSem3
,PorcMortSem4
,TerciosPorcMortSem4
,PorcMortSem5
,TerciosPorcMortSem5
,IEP
,TerciosIEP
,PesoProm
,TerciosPesoProm
,PesoSem1
,TerciosPesoSem1
,PesoSem2
,TerciosPesoSem2
,PesoSem3
,TerciosPesoSem3
,PesoSem4
,TerciosPesoSem4
,PesoSem5
,TerciosPesoSem5
,Gas
,TerciosGas
,Cama
,TerciosCama
,Agua
,TerciosAgua
,AvesXm2
,TerciosAvesXm2
,GananciaDiaVenta
,TerciosGananciaDiaVenta
from {database_name}.AgrupadoLoteMensual
union
select 'ANUAL',* from AgrupadoPlantelAnual2
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Tercios_Lote_Plantel"
}
df_ft_Tercios_Lote_Plantel.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_Tercios_Lote_Plantel")
print('carga ft_Tercios_Lote_Plantel')
#Se muestra AgrupadoCorralMensual
df_AgrupadoCorralMensual = spark.sql(f"""
select
 a.fecha
,b.idmes pk_mes
,b.anio
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_tipoproducto
,a.pk_especie
,a.pk_estado
,a.pk_administrador
,a.pk_proveedor
,a.ComplexEntityNo
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-11)) ComplexEntityNoPlantel
,c.lstandard
,a.PobInicial
,a.ICA 
,a.ICA * a.PobInicial ICAXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.ICA ASC) AS TerciosIcaXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.ICA ASC) AS TerciosIcaXTPStandard
,a.PorMort
,a.PorMort * a.PobInicial PorMortXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorMort ASC) AS TerciosPorMortXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorMort ASC) AS TerciosPorMortXTPStandard
,a.PorcMortSem1
,a.PorcMortSem1 * a.PobInicial PorcMortSem1XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1XTPStandard
,a.PorcMortSem2
,a.PorcMortSem2 * a.PobInicial PorcMortSem2XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2XTPStandard
,a.PorcMortSem3
,a.PorcMortSem3 * a.PobInicial PorcMortSem3XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3XTPStandard
,a.PorcMortSem4
,a.PorcMortSem4 * a.PobInicial PorcMortSem4XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4XTPStandard
,a.PorcMortSem5
,a.PorcMortSem5 * a.PobInicial PorcMortSem5XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5XTPStandard
,a.IEP
,a.IEP * a.PobInicial IEPXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.IEP DESC) AS TerciosIEPXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.IEP DESC) AS TerciosIEPXTPStandard
,a.PesoProm
,a.PesoProm * a.PobInicial PesoPromXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoProm DESC) AS TerciosPesoPromXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoProm DESC) AS TerciosPesoPromXTPStandard
,a.PesoSem1
,a.PesoSem1 * a.PobInicial PesoSem1XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1XTPStandard
,a.PesoSem2
,a.PesoSem2 * a.PobInicial PesoSem2XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2XTPStandard
,a.PesoSem3
,a.PesoSem3 * a.PobInicial PesoSem3XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3XTPStandard
,a.PesoSem4
,a.PesoSem4 * a.PobInicial PesoSem4XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4XTPStandard
,a.PesoSem5
,a.PesoSem5 * a.PobInicial PesoSem5XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5XTPStandard
,a.Gas
,a.Gas * a.PobInicial GasXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.Gas ASC) AS TerciosGasXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.Gas ASC) AS TerciosGasXTPStandard
,a.Cama
,a.Cama * a.PobInicial CamaXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.Cama ASC) AS TerciosCamaXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.Cama ASC) AS TerciosCamaXTPStandard
,a.Agua
,a.Agua * a.PobInicial AguaXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.Agua ASC) AS TerciosAguaXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.Agua ASC) AS TerciosAguaXTPStandard
,a.AvesXm2
,a.AvesXm2 * a.PobInicial AvesXm2XPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2XTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2XTPStandard
,a.GananciaDiaVenta
,a.GananciaDiaVenta * a.PobInicial GananciaDiaVentaXPobInicial
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, a.pk_especie ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVentaXTPEspecie
,NTILE(3) OVER(PARTITION BY b.idmes, a.pk_tipoproducto, c.lstandard ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVentaXTPStandard
from {database_name}.ft_consolidado_corral a
left join {database_name}.lk_tiempo B on A.fecha = B.fecha
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard
where a.pk_division = 3 and a.pk_empresa = 1 and a.flagatipico = 1 and a.pk_estado in (2,3) and a.pk_especie in (16,40,9)
order by b.idmes
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AgrupadoCorralMensual"
}
df_AgrupadoCorralMensual.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AgrupadoCorralMensual")
print('carga AgrupadoCorralMensual')
#Se muestra ft_Tercios_Corral_Plantel
df_ft_Tercios_Corral_Plantel = spark.sql(f"""
with AgrupadoCorralMensual2 as 
(select
 max(fecha) fecha
,max(pk_mes) pk_mes
,max(anio) anio
,pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,ComplexEntityNoPlantel ComplexEntityNo
,lstandard
,SUM(PobInicial) PobInicial
,SUM(ICAXPobInicial) ICAXPobInicial
,SUM(ICAXPobInicial) / SUM(PobInicial) ICA
,SUM(PorMortXPobInicial) PorMortXPobInicial
,SUM(PorMortXPobInicial) / SUM(PobInicial) PorMort
,SUM(PorcMortSem1XPobInicial) PorcMortSem1XPobInicial
,SUM(PorcMortSem1XPobInicial) / SUM(PobInicial) PorcMortSem1
,SUM(PorcMortSem2XPobInicial) PorcMortSem2XPobInicial
,SUM(PorcMortSem2XPobInicial) / SUM(PobInicial)  PorcMortSem2
,SUM(PorcMortSem3XPobInicial) PorcMortSem3XPobInicial
,SUM(PorcMortSem3XPobInicial) / SUM(PobInicial) PorcMortSem3
,SUM(PorcMortSem4XPobInicial) PorcMortSem4XPobInicial
,SUM(PorcMortSem4XPobInicial) / SUM(PobInicial) PorcMortSem4
,SUM(PorcMortSem5XPobInicial) PorcMortSem5XPobInicial
,SUM(PorcMortSem5XPobInicial) / SUM(PobInicial) PorcMortSem5
,SUM(IEPXPobInicial) IEPXPobInicial
,SUM(IEPXPobInicial) / SUM(PobInicial) IEP
,SUM(PesoPromXPobInicial) PesoPromXPobInicial
,SUM(PesoPromXPobInicial) / SUM(PobInicial) PesoProm
,SUM(PesoSem1XPobInicial) PesoSem1XPobInicial
,SUM(PesoSem1XPobInicial) / SUM(PobInicial) PesoSem1
,SUM(PesoSem2XPobInicial) PesoSem2XPobInicial
,SUM(PesoSem2XPobInicial) / SUM(PobInicial) PesoSem2
,SUM(PesoSem3XPobInicial) PesoSem3XPobInicial
,SUM(PesoSem3XPobInicial) / SUM(PobInicial) PesoSem3
,SUM(PesoSem4XPobInicial) PesoSem4XPobInicial
,SUM(PesoSem4XPobInicial) / SUM(PobInicial) PesoSem4
,SUM(PesoSem5XPobInicial) PesoSem5XPobInicial
,SUM(PesoSem5XPobInicial) / SUM(PobInicial) PesoSem5
,SUM(GasXPobInicial) GasXPobInicial
,SUM(GasXPobInicial) / SUM(PobInicial) Gas
,SUM(CamaXPobInicial) CamaXPobInicial
,SUM(CamaXPobInicial) / SUM(PobInicial) Cama
,SUM(AguaXPobInicial) AguaXPobInicial
,SUM(AguaXPobInicial) / SUM(PobInicial) Agua
,SUM(AvesXm2XPobInicial) AvesXm2XPobInicial
,SUM(AvesXm2XPobInicial) / SUM(PobInicial) AvesXm2
,SUM(GananciaDiaVentaXPobInicial) GananciaDiaVentaXPobInicial
,SUM(GananciaDiaVentaXPobInicial) / SUM(PobInicial) GananciaDiaVenta
from {database_name}.AgrupadoCorralMensual
group by 
 pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,ComplexEntityNoPlantel
,lstandard)

select 
'MENSUAL' frecuencia
,a.fecha
,a.pk_mes
,a.anio
,a.pk_empresa
,a.pk_division
,a.pk_zona
,a.pk_subzona
,a.pk_plantel
,a.pk_lote
,a.pk_tipoproducto
,a.pk_especie
,a.pk_estado
,a.pk_administrador
,a.pk_proveedor
,a.ComplexEntityNo
,a.lstandard
,a.PobInicial
,a.ICA 
,a.TerciosIcaXTPEspecie
,a.TerciosIcaXTPStandard
,a.PorMort
,a.TerciosPorMortXTPEspecie
,a.TerciosPorMortXTPStandard
,a.PorcMortSem1
,a.TerciosPorcMortSem1XTPEspecie
,a.TerciosPorcMortSem1XTPStandard
,a.PorcMortSem2
,a.TerciosPorcMortSem2XTPEspecie
,a.TerciosPorcMortSem2XTPStandard
,a.PorcMortSem3
,a.TerciosPorcMortSem3XTPEspecie
,a.TerciosPorcMortSem3XTPStandard
,a.PorcMortSem4
,a.TerciosPorcMortSem4XTPEspecie
,a.TerciosPorcMortSem4XTPStandard
,a.PorcMortSem5
,a.TerciosPorcMortSem5XTPEspecie
,a.TerciosPorcMortSem5XTPStandard
,a.IEP
,a.TerciosIEPXTPEspecie
,a.TerciosIEPXTPStandard
,a.PesoProm
,a.TerciosPesoPromXTPEspecie
,a.TerciosPesoPromXTPStandard
,a.PesoSem1
,a.TerciosPesoSem1XTPEspecie
,a.TerciosPesoSem1XTPStandard
,a.PesoSem2
,a.TerciosPesoSem2XTPEspecie
,a.TerciosPesoSem2XTPStandard
,a.PesoSem3
,a.TerciosPesoSem3XTPEspecie
,a.TerciosPesoSem3XTPStandard
,a.PesoSem4
,a.TerciosPesoSem4XTPEspecie
,a.TerciosPesoSem4XTPStandard
,a.PesoSem5
,a.TerciosPesoSem5XTPEspecie
,a.TerciosPesoSem5XTPStandard
,a.Gas
,a.TerciosGasXTPEspecie
,a.TerciosGasXTPStandard
,a.Cama
,a.TerciosCamaXTPEspecie
,a.TerciosCamaXTPStandard
,a.Agua
,a.TerciosAguaXTPEspecie
,a.TerciosAguaXTPStandard
,a.AvesXm2
,a.TerciosAvesXm2XTPEspecie
,a.TerciosAvesXm2XTPStandard
,a.GananciaDiaVenta
,a.TerciosGananciaDiaVentaXTPEspecie
,a.TerciosGananciaDiaVentaXTPStandard
from {database_name}.AgrupadoCorralMensual a
where (pk_mes >= {AnioMes} AND pk_mes <= {AnioMesFin})
UNION
select 
'ANUAL' frecuencia
,fecha
,pk_mes
,anio
,pk_empresa
,pk_division
,pk_zona
,pk_subzona
,pk_plantel
,0 pk_lote
,pk_tipoproducto
,pk_especie
,pk_estado
,pk_administrador
,pk_proveedor
,ComplexEntityNo
,lstandard
,PobInicial
,ICA
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.ICA ASC) AS TerciosIcaXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.ICA ASC) AS TerciosIcaXTPStandard
,a.PorMort
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorMort ASC) AS TerciosPorMortXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorMort ASC) AS TerciosPorMortXTPStandard
,a.PorcMortSem1
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorcMortSem1 ASC) AS TerciosPorcMortSem1XTPStandard
,a.PorcMortSem2
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorcMortSem2 ASC) AS TerciosPorcMortSem2XTPStandard
,a.PorcMortSem3
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorcMortSem3 ASC) AS TerciosPorcMortSem3XTPStandard
,a.PorcMortSem4
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorcMortSem4 ASC) AS TerciosPorcMortSem4XTPStandard
,a.PorcMortSem5
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PorcMortSem5 ASC) AS TerciosPorcMortSem5XTPStandard
,a.IEP
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.IEP DESC) AS TerciosIEPXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.IEP DESC) AS TerciosIEPXTPStandard
,a.PesoProm
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoProm DESC) AS TerciosPesoPromXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoProm DESC) AS TerciosPesoPromXTPStandard
,a.PesoSem1
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoSem1 DESC) AS TerciosPesoSem1XTPStandard
,a.PesoSem2
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoSem2 DESC) AS TerciosPesoSem2XTPStandard
,a.PesoSem3
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoSem3 DESC) AS TerciosPesoSem3XTPStandard
,a.PesoSem4
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoSem4 DESC) AS TerciosPesoSem4XTPStandard
,a.PesoSem5
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.PesoSem5 DESC) AS TerciosPesoSem5XTPStandard
,a.Gas
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.Gas ASC) AS TerciosGasXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.Gas ASC) AS TerciosGasXTPStandard
,a.Cama
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.Cama ASC) AS TerciosCamaXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.Cama ASC) AS TerciosCamaXTPStandard
,a.Agua
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.Agua ASC) AS TerciosAguaXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.Agua ASC) AS TerciosAguaXTPStandard
,a.AvesXm2
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2XTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.AvesXm2 DESC) AS TerciosAvesXm2XTPStandard
,a.GananciaDiaVenta
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.pk_especie ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVentaXTPEspecie
,NTILE(3) OVER(PARTITION BY anio, a.pk_tipoproducto, a.lstandard ORDER BY a.GananciaDiaVenta DESC) AS TerciosGananciaDiaVentaXTPStandard
from AgrupadoCorralMensual2 a
where (pk_mes >= {AnioMes} AND pk_mes <= {AnioMesFin})
""")

#Se muestra ft_consolidado_Diario
try:
    df = spark.table("{database_name}.ft_Tercios_Corral_Plantel")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin)) |
        (F.col("pk_empresa") != 1)
    )
    df_ft_Tercios_Corral_Plantel_new = df_ft_Tercios_Corral_Plantel.union(df_filtered)
except Exception as e:
    df_ft_Tercios_Corral_Plantel_new = df_ft_Tercios_Corral_Plantel
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Tercios_Corral_Plantel_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_Tercios_Corral_Plantel_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_Tercios_Corral_Plantel_new")

df_ft_Tercios_Corral_Plantel_nueva = spark.sql(f"""SELECT * from {database_name}.ft_Tercios_Corral_Plantel_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_Tercios_Corral_Plantel")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Tercios_Corral_Plantel"
}
df_ft_Tercios_Corral_Plantel_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_Tercios_Corral_Plantel")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_Tercios_Corral_Plantel_new")

print('carga ft_Tercios_Corral_Plantel')
#Se muestra UPD ft_consolidado_Diario 2
df_UPDft_consolidado_Diario = spark.sql(f"""
with c1 as (select complexentityno,pk_diasvida,STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'PREINICIO')
,c2 as (select complexentityno,MAX(STDConsAcum)STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'PREINICIO' group by complexentityno)
,c3 as (select complexentityno,pk_diasvida,STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'INICIO')
,c4 as (select complexentityno,MAX(STDConsAcum)STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'INICIO' group by complexentityno)
,c5 as (select complexentityno,pk_diasvida,STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'ACABADO')
,c6 as (select complexentityno,MAX(STDConsAcum)STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'ACABADO' group by complexentityno)
,c7 as (select complexentityno,pk_diasvida,STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'TERMINADOR')
,c8 as (select complexentityno,MAX(STDConsAcum)STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'TERMINADOR' group by complexentityno)
,c9 as (select complexentityno,pk_diasvida,STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'FINALIZADOR')
,c10 as (select complexentityno,MAX(STDConsAcum)STDConsAcum from {database_name}.ft_consolidado_Diario where TipoAlimentoXTipoProducto = 'FINALIZADOR' group by complexentityno)
,ConsumoXTipoAlimento1 as (
select A.fecha,A.pk_empresa,A.pk_division,A.pk_diasvida,A.ComplexEntityNo
,c1.STDConsAcum STDConsAcumPreinicio
,c2.STDConsAcum STDConsAcumPreinicioMax
,c3.STDConsAcum STDConsAcumInicio
,c4.STDConsAcum STDConsAcumInicioMax
,c5.STDConsAcum STDConsAcumAcabado
,c6.STDConsAcum STDConsAcumAcabadoMax
,c7.STDConsAcum STDConsAcumTerminador
,c8.STDConsAcum STDConsAcumTerminadorMax
,c9.STDConsAcum STDConsAcumFinalizador
,c10.STDConsAcum STDConsAcumFinalizadorMax
from {database_name}.ft_consolidado_Diario A
left join c1 on c1.complexentityno = A.complexentityno and c1.pk_diasvida = A.pk_diasvida
left join c2 on c2.complexentityno = A.complexentityno
left join c3 on c3.complexentityno = A.complexentityno and c3.pk_diasvida = A.pk_diasvida
left join c4 on c4.complexentityno = A.complexentityno
left join c5 on c5.complexentityno = A.complexentityno and c5.pk_diasvida = A.pk_diasvida
left join c6 on c6.complexentityno = A.complexentityno
left join c7 on c7.complexentityno = A.complexentityno and c7.pk_diasvida = A.pk_diasvida
left join c8 on c8.complexentityno = A.complexentityno
left join c9 on c9.complexentityno = A.complexentityno and c9.pk_diasvida = A.pk_diasvida
left join c10 on c10.complexentityno = A.complexentityno
where pk_empresa = 1 and pk_division = 3)
,ConsumoXTipoAlimento2 as (
select 
 fecha
,pk_empresa
,pk_division
,pk_diasvida
,ComplexEntityNo
,CASE WHEN pk_diasvida >= 1 AND (STDConsAcumPreinicio IS NULL OR STDConsAcumPreinicio = 0) THEN STDConsAcumPreinicioMax ELSE STDConsAcumPreinicio END STDConsAcumPreinicio
,CASE WHEN pk_diasvida >= 9 AND (STDConsAcumInicio IS NULL OR STDConsAcumInicio = 0) THEN STDConsAcumInicioMax ELSE STDConsAcumInicio END STDConsAcumInicio
,CASE WHEN pk_diasvida >= 19 AND (STDConsAcumAcabado IS NULL OR STDConsAcumAcabado = 0) THEN STDConsAcumAcabadoMax ELSE STDConsAcumAcabado END STDConsAcumAcabado
,CASE WHEN pk_diasvida >= 25 AND (STDConsAcumTerminador IS NULL OR STDConsAcumTerminador = 0) THEN STDConsAcumTerminadorMax ELSE STDConsAcumTerminador END STDConsAcumTerminador
,CASE WHEN pk_diasvida >= 35 AND (STDConsAcumFinalizador IS NULL OR STDConsAcumFinalizador=0) THEN STDConsAcumFinalizadorMax ELSE STDConsAcumFinalizador END STDConsAcumFinalizador
from ConsumoXTipoAlimento1
order by fecha)

select 
 a.fecha               
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
,a.pk_grupoconsumo     
,a.pk_especie          
,a.pk_estado           
,a.pk_administrador    
,a.pk_proveedor        
,a.pk_diasvida         
,a.pk_semanavida       
,a.complexentityno     
,a.nacimiento          
,a.edad                
,a.fechacierre         
,a.fechaalojamiento    
,a.pobinicial          
,a.inventario          
,a.avesrendidas        
,a.avesrendidasacum    
,a.kilosrendidos       
,a.kilosrendidosacum   
,a.mortdia             
,a.mortacum            
,a.mortsem             
,a.stdmortdia          
,a.stdmortacum         
,a.stdmortsem          
,a.porcmortdia         
,a.porcmortacum        
,a.porcmortsem         
,a.stdporcmortdia      
,a.stdporcmortacum     
,a.stdporcmortsem      
,a.difporcmort_std     
,a.difporcmortacum_stdacum
,a.difporcmortsem_stdsem
,a.areagalpon          
,a.consumo             
,a.consacum            
,a.porcconsdia         
,a.porcconsacum        
,a.stdconsdia          
,a.stdconsacum         
,a.difporccons_std     
,a.difporcconsacum_stdacum
,a.preinicio           
,a.inicio              
,a.acabado             
,a.terminado           
,a.finalizador         
,a.pesomasstd          
,a.peso                
,a.pesosem             
,a.stdpeso             
,a.difpeso_std         
,a.pesoalo             
,a.tasacre             
,a.gananciadia         
,a.gananciasem         
,a.stdganancia         
,a.gananciaesp         
,a.difpesoactpesoant   
,a.cantdia             
,a.gananciapesodia     
,a.ica                 
,a.stdica              
,a.icaajustado         
,a.icaventa            
,a.u_peaccidentados    
,a.u_pehigadograso     
,a.u_pehepatomegalia   
,a.u_pehigadohemorragico
,a.u_peinanicion       
,a.u_peproblemarespiratorio
,a.u_pesch             
,a.u_peenteritis       
,a.u_peascitis         
,a.u_pemuertesubita    
,a.u_peestresporcalor  
,a.u_pehidropericardio 
,a.u_pehemopericardio  
,a.u_peuratosis        
,a.u_pematerialcaseoso 
,a.u_peonfalitis       
,a.u_peretenciondeyema 
,a.u_peerosiondemolleja
,a.u_pehemorragiamusculos
,a.u_pesangreenciego   
,a.u_pepericarditis    
,a.u_peperitonitis     
,a.u_peprolapso        
,a.u_pepicaje          
,a.u_perupturaaortica  
,a.u_pebazomoteado     
,a.u_penoviable        
,a.u_pecaja            
,a.u_pegota            
,a.u_peintoxicacion    
,a.u_peretrazos        
,a.u_peeliminados      
,a.u_peahogados        
,a.u_peecoli           
,a.u_pedescarte        
,a.u_peotros           
,a.u_pecoccidia        
,a.u_pedeshidratados   
,a.u_pehepatitis       
,a.u_petraumatismo     
,a.pigmentacion        
,a.categoria           
,a.padremayor          
,a.flagatipico         
,a.pesohvo             
,a.u_peaerosaculitisg2 
,a.u_pecojera          
,a.u_pehigadoicterico  
,a.u_pematerialcaseoso_po1ra
,a.u_pematerialcaseosomedretr
,a.u_penecrosishepatica
,a.u_peneumonia        
,a.u_pesepticemia      
,a.u_pevomitonegro     
,a.u_peasperguillius   
,a.u_pebazograndemot   
,a.u_pecorazongrande   
,a.u_pecuadrotoxico    
,a.pavosbbmortincub    
,a.pavoini             
,a.pavo1               
,a.pavo2               
,a.pavo3               
,a.pavo4               
,a.pavo5               
,a.pavo6               
,a.pavo7               
,a.edadpadrecorraldescrip
,a.u_causapesobajo     
,a.u_accionpesobajo    
,a.stdconsdiakg        
,a.stdconsacumkg       
,a.difcons_std         
,a.difconsacum_stdacum 
,a.tipoalimento        
,a.porcpreinicioacum   
,a.porcinicioacum      
,a.porcacabadoacum     
,a.porcterminadoacum   
,a.porcfinalizadoracum
,b.STDConsAcumPreinicio
,b.STDConsAcumInicio
,b.STDConsAcumAcabado
,b.STDConsAcumTerminador STDConsAcumTerminado
,b.STDConsAcumFinalizador
,a.ruidosrespiratorios 
,a.productoconsumo     
,a.stdconssem          
,a.stdporcconssem      
,a.conssem             
,a.porcconssem         
,a.difconssem_stdconssem
,a.difporcconssem_stdporcconssem
,a.u_consumogasinvierno
,a.u_consumogasverano  
,a.saldoaves           
,a.stdconsdiakgsaldoaves
,a.porcconsdiasaldoaves
,a.difcons_stdkgsaldoaves
,a.difporccons_stdsaldoaves
,a.saldoavesconsumo    
,a.porcmortdiaacum     
,a.difporcmortdiaacum_stddiaacum
,a.tasanoviable        
,a.semporcprilesion    
,a.semporcseglesion    
,a.semporcterlesion    
,a.tipoalimentoxtipoproducto
,a.stdconsdiaxtipoalimentoxtipoproducto
,a.stdconsdiakgxtipoalimentoxtipoproducto
,a.listaformulano      
,a.listaformulaname    
,a.tipoorigen          
,a.mortconcatcorral    
,a.mortsemantcorral    
,a.mortconcatlote      
,a.mortsemantlote      
,a.mortconcatsemaniolote
,a.pesoconcatcorral    
,a.pesosemantcorral    
,a.pesoconcatlote      
,a.pesosemantlote      
,a.pesoconcatsemaniolote
,a.noviablesem1        
,a.noviablesem2        
,a.noviablesem3        
,a.noviablesem4        
,a.noviablesem5        
,a.noviablesem6        
,a.noviablesem7        
,a.porcnoviablesem1    
,a.porcnoviablesem2    
,a.porcnoviablesem3    
,a.porcnoviablesem4    
,a.porcnoviablesem5    
,a.porcnoviablesem6    
,a.porcnoviablesem7    
,a.noviablesem8        
,a.porcnoviablesem8    
,a.pk_tipogranja       
,a.gananciapesosem     
,a.cv           
from {database_name}.ft_consolidado_Diario A
left join ConsumoXTipoAlimento2 B ON A.ComplexEntityNo = B.ComplexEntityNo AND A.pk_diasvida = B.pk_diasvida
where A.pk_empresa = 1 and A.pk_division = 3
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Diario.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_new")

df_ft_consolidado_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario"
}
df_ft_consolidado_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario_new")

print('carga UPD ft_consolidado_Diario 2')
#Se muestra LimpiezaDesinfeccion
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
FROM {database_name}.si_mvBrimEntities VB
LEFT JOIN {database_name}.si_BrimEntities AS BE ON BE.IRN = VB.BrimEntitiesIRN
LEFT JOIN {database_name}.si_BrimFieldTrans AS BFT ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN 
LEFT JOIN {database_name}.si_MtSysUserFieldCodes AS MUF1 ON BE.U_Uso_de_cama =  MUF1.IRN
LEFT JOIN {database_name}.si_MtSysUserFieldCodes AS MUF2 ON BE.U_Forma_reutilizacion =  MUF2.IRN
WHERE GRN = 'H' AND SUBSTRING(ComplexEntityNo,1,1) = 'P' 
AND RTRIM(HouseNo) IN ('01','02','03','04','05','06','07','08','09','10')
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LimpiezaDesinfeccion"
}
df_LimpiezaDesinfeccion.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.LimpiezaDesinfeccion")
print('carga LimpiezaDesinfeccion')
#Se muestra ft_Temperatura
df_ft_Temperatura = spark.sql(f"""
WITH 
 RegistrosTotal AS (SELECT MIN(EventDate) EventDate, ComplexEntityNo, SUM(TotalEvaluacion) TotalEvaluacion FROM {database_name}.LimpiezaDesinfeccion GROUP BY ComplexEntityNo)
,RegistrosTotalCeros as (select * from RegistrosTotal WHERE TotalEvaluacion = 0)
select
LT.fecha,
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
(U_TempRuma1 + U_TempRuma2 + U_TempRuma3 + U_TempRuma4) / 4 PromTemp
from {database_name}.LimpiezaDesinfeccion MO
LEFT JOIN {database_name}.si_ProteinEntities PE ON PE.IRN = MO.ProteinEntitiesIRN
LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50))
LEFT JOIN {database_name}.si_ProteinCostCenters PCC  ON cast(PCC.IRN AS varchar(50)) =LP.ProteinCostCentersIRN
LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1)
LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50))
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha =MO.EventDate
LEFT JOIN {database_name}.ft_consolidado_galpon CG ON MO.ComplexEntityNo = CG.ComplexEntityNo
WHERE LD.pk_division = 3 and MO.U_NH3_1 + MO.U_NH3_2 + MO.U_TempRuma1 + MO.U_TempRuma2 + MO.U_TempRuma3 + MO.U_TempRuma4 <> 0
UNION
SELECT
LT.fecha,
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
(DATEDIFF(U_1ra_desinfeccion, FirstHatchDate )-1) PDE,
CG.PK_empresa,
CG.PK_division,
CG.PK_zona,
CG.PK_subzona,
CG.PK_tipoproducto,
CG.PK_especie,
CG.PK_estado,
CG.PK_administrador,
CG.PK_proveedor,
CG.categoria,
MO.EventDate FechaEvento,
MO.NumeroFechaEvento,
GREATEST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MaxTemp,
LEAST(U_TempRuma1, U_TempRuma2, U_TempRuma3, U_TempRuma4) AS MinTemp,
(U_TempRuma1 + U_TempRuma2 + U_TempRuma3 + U_TempRuma4) / 4 PromTemp
FROM {database_name}.LimpiezaDesinfeccion MO
LEFT JOIN {database_name}.si_ProteinEntities PE ON PE.IRN = MO.ProteinEntitiesIRN
LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50))
LEFT JOIN {database_name}.si_ProteinCostCenters PCC  ON cast(PCC.IRN AS varchar(50)) =LP.ProteinCostCentersIRN
LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1)
LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50))
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha =MO.EventDate
LEFT JOIN {database_name}.ft_consolidado_galpon CG ON MO.ComplexEntityNo = CG.ComplexEntityNo
LEFT JOIN RegistrosTotalCeros RTC ON RTC.ComplexEntityNo = MO.ComplexEntityNo AND RTC.Eventdate = MO.Eventdate
WHERE LD.pk_division = 3 AND RTC.TotalEvaluacion = 0
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Temperatura"
}
df_ft_Temperatura.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_Temperatura")
print('carga ft_Temperatura')
#Se muestra UPD ft_Temperatura
df_UPDft_Temperatura = spark.sql(f"""
with temp as (select max(Pobinicial) Pobinicial,ComplexEntityNo, FechaInicioGranja, max(DiasLimpieza) DiasLimpieza from {database_name}.ft_consolidado_galpon group by ComplexEntityNo, FechaInicioGranja)
SELECT 
 A.fecha               
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
FROM {database_name}.ft_Temperatura A
LEFT JOIN temp B on A.ComplexEntityNo = B.ComplexEntityNo
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Temperatura_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_Temperatura.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_Temperatura_new")

df_ft_Temperatura_nueva = spark.sql(f"""SELECT * from {database_name}.ft_Temperatura_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_Temperatura")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_Temperatura"
}
df_ft_Temperatura_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_Temperatura")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_Temperatura_new")

print('carga UPD ft_Temperatura')
#Se muestra LimpiezaDesinfeccionCorral
df_LimpiezaDesinfeccionCorral = spark.sql(f"""
select 
VB.ComplexEntityNo,
VB.FarmNo,
VB.EntityNo,
VB.HouseNo,
VB.PenNo,
BFT.Eventdate,
VB.FirstHatchDate,
BE.U_1ra_desinfeccion,
VB.ProteinFarmsIRN,
BFT.ProteinEntitiesIRN,
VB.ProteinCostCentersIRN,
RTRIM(MUF1.Code) Code,
CASE WHEN LENGTH(RTRIM(MUF2.Name)) = 0 THEN 'Cama Nueva' ELSE RTRIM(MUF2.Name) END Name
from {database_name}.si_BrimFieldTrans AS BFT 
LEFT JOIN {database_name}.si_mvBrimEntities AS VB ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN
LEFT JOIN {database_name}.si_BrimEntities AS BE ON VB.BrimEntitiesIRN = BE.IRN 
LEFT JOIN {database_name}.si_MtSysUserFieldCodes AS MUF1 ON BE.U_Uso_de_cama =  MUF1.IRN
LEFT JOIN {database_name}.si_MtSysUserFieldCodes AS MUF2 ON BE.U_Forma_reutilizacion =  MUF2.IRN
where GRN = 'P'
group by
VB.ComplexEntityNo,
VB.FarmNo,
VB.EntityNo,
VB.HouseNo,
VB.PenNo,
BFT.Eventdate,
VB.FirstHatchDate,
BE.U_1ra_desinfeccion,
VB.ProteinFarmsIRN,
BFT.ProteinEntitiesIRN,
VB.ProteinCostCentersIRN,
MUF1.Code,
MUF2.Name
order by ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LimpiezaDesinfeccionCorral"
}
df_LimpiezaDesinfeccionCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.LimpiezaDesinfeccionCorral")
print('carga LimpiezaDesinfeccionCorral')
#Se muestra ft_TemperaturaCorral
df_ft_TemperaturaCorral = spark.sql(f"""
WITH 
CalculoTemperaturaPromSimple as (select	pk_plantel,pk_lote,pk_galpon,ComplexEntityNo,avg(AmoniacoMuestra1) AmoniacoMuestra1,avg(AmoniacoMuestra2) AmoniacoMuestra2,avg(TempRuma1) TempRuma1,avg(TempRuma2) TempRuma2,avg(TempRuma3) TempRuma3,avg(TempRuma4) TempRuma4
                                from {database_name}.ft_Temperatura
                                group by pk_plantel,pk_lote,pk_galpon,ComplexEntityNo)
select
LT.fecha,
LP.pk_plantel,
LL.pk_lote,
LG.pk_galpon,
COALESCE(LS.pk_sexo,4) pk_sexo,
MO.ComplexEntityNo,
FirstHatchDate FechaInicial,
U_1ra_desinfeccion FechaDesinfeccion,
AmoniacoMuestra1,
AmoniacoMuestra2,
TempRuma1,
TempRuma2,
TempRuma3,
TempRuma4,
Code NReuso,
Name FormaReutilizacion,
case when Name = 'Cama Nueva' then 'Cama Nueva' else 'Reutilización' end TipoCama,
(DATEDIFF(U_1ra_desinfeccion, FirstHatchDate )-1) PDE
from {database_name}.LimpiezaDesinfeccionCorral MO
LEFT JOIN {database_name}.si_ProteinEntities PE ON PE.IRN = MO.ProteinEntitiesIRN
LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50))
LEFT JOIN {database_name}.si_ProteinCostCenters PCC  ON cast(PCC.IRN AS varchar(50)) =LP.ProteinCostCentersIRN
LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1)
LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50))
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha =MO.EventDate
LEFT JOIN {database_name}.lk_sexo LS ON LS.csexo = rtrim(MO.PenNo) 
LEFT JOIN CalculoTemperaturaPromSimple CT ON CT.pk_plantel = LP.pk_plantel AND CT.pk_lote = LL.pk_lote AND CT.pk_galpon = LG.pk_galpon
where LD.pk_division = 3
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_TemperaturaCorral"
}
df_ft_TemperaturaCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_TemperaturaCorral")
print('carga ft_TemperaturaCorral')
#Se muestra UPD ft_TemperaturaCorral
df_UPDft_TemperaturaCorral = spark.sql(f"""
with temp as (select max(Pobinicial) Pobinicial,ComplexEntityNo,FechaInicioGranja,max(DiasLimpieza) DiasLimpieza from {database_name}.ft_consolidado_corral group by ComplexEntityNo, FechaInicioGranja)
select
 A.fecha               
,A.pk_plantel          
,A.pk_lote             
,A.pk_galpon           
,A.pk_sexo             
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
,A.tipocama
,B.PobInicial PobInicial
,B.FechaInicioGranja FechaInicioGranja
,B.DiasLimpieza-1 PDT      
,A.pde          
from {database_name}.ft_TemperaturaCorral A
left join temp B on A.ComplexEntityNo = B.ComplexEntityNo
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_TemperaturaCorral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_TemperaturaCorral.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_TemperaturaCorral_new")

df_ft_TemperaturaCorral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_TemperaturaCorral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_TemperaturaCorral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_TemperaturaCorral"
}
df_ft_TemperaturaCorral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_TemperaturaCorral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_TemperaturaCorral_new")

print('carga UPD ft_TemperaturaCorral')
#Se muestra ft_consolidado_ReferenciaCorral
df_ft_consolidado_ReferenciaCorral = spark.sql(f"""
WITH GuiaRemision as (select ComplexEntityNo,guiaremision Referencia from {database_name}.ft_ventas_cd VCD where pk_empresa = 1 and pk_division = 3 group by complexentityno,guiaremision order by ComplexEntityNo)
SELECT fecha
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
,pk_diasvida
,A.ComplexEntityNo
,FechaNacimiento
,FechaCierre
,FechaCierreLote
,FechaAlojamiento
,FechaCrianza
,FechaInicioGranja
,FechaInicioSaca
,FechaFinSaca
,PadreMayor
,RazaMayor
,IncubadoraMayor
,PorcPadreMayor
,PorCodigoRaza
,PorcIncMayor
,PobInicial
,AvesLogradas
,AvesRendidas
,SobranFaltan
,KilosRendidos
,MortDia
,PorMort
,STDPorMort
,DifPorMort_STDPorMort
,MortSem1
,MortSem2
,MortSem3
,MortSem4
,MortSem5
,MortSem6
,MortSem7
,PorcMortSem1
,STDPorcMortSem1
,DifPorcMortSem1_STDPorcMortSem1
,PorcMortSem2
,STDPorcMortSem2
,DifPorcMortSem2_STDPorcMortSem2
,PorcMortSem3
,STDPorcMortSem3
,DifPorcMortSem3_STDPorcMortSem3
,PorcMortSem4
,STDPorcMortSem4
,DifPorcMortSem4_STDPorcMortSem4
,PorcMortSem5
,STDPorcMortSem5
,DifPorcMortSem5_STDPorcMortSem5
,PorcMortSem6
,STDPorcMortSem6
,DifPorcMortSem6_STDPorcMortSem6
,PorcMortSem7
,STDPorcMortSem7
,DifPorcMortSem7_STDPorcMortSem7
,MortSemAcum1
,MortSemAcum2
,MortSemAcum3
,MortSemAcum4
,MortSemAcum5
,MortSemAcum6
,MortSemAcum7
,PorcMortSemAcum1
,STDPorcMortSemAcum1
,DifPorcMortSemAcum1_STDPorcMortSemAcum1
,PorcMortSemAcum2
,STDPorcMortSemAcum2
,DifPorcMortSemAcum2_STDPorcMortSemAcum2
,PorcMortSemAcum3
,STDPorcMortSemAcum3
,DifPorcMortSemAcum3_STDPorcMortSemAcum3
,PorcMortSemAcum4
,STDPorcMortSemAcum4
,DifPorcMortSemAcum4_STDPorcMortSemAcum4
,PorcMortSemAcum5
,STDPorcMortSemAcum5
,DifPorcMortSemAcum5_STDPorcMortSemAcum5
,PorcMortSemAcum6
,STDPorcMortSemAcum6
,DifPorcMortSemAcum6_STDPorcMortSemAcum6
,PorcMortSemAcum7
,STDPorcMortSemAcum7
,DifPorcMortSemAcum7_STDPorcMortSemAcum7
,PreInicio
,Inicio
,Acabado
,Terminado
,Finalizador
,ConsDia
,PorcPreIni
,PorcIni
,PorcAcab
,PorcTerm
,PorcFin
,PorcConsumo
,STDPorcConsumo
,DifPorcConsumo_STDPorcConsumo
,Seleccion
,PorcSeleccion
,PesoSem1
,STDPesoSem1
,DifPesoSem1_STDPesoSem1
,PesoSem2
,STDPesoSem2
,DifPesoSem2_STDPesoSem2
,PesoSem3
,STDPesoSem3
,DifPesoSem3_STDPesoSem3
,PesoSem4
,STDPesoSem4
,DifPesoSem4_STDPesoSem4
,PesoSem5
,STDPesoSem5
,DifPesoSem5_STDPesoSem5
,PesoSem6
,STDPesoSem6
,DifPesoSem6_STDPesoSem6
,PesoSem7
,STDPesoSem7
,DifPesoSem7_STDPesoSem7
,PesoProm
,STDPesoProm
,DifPesoProm_STDPesoProm
,GananciaDiaVenta
,ICA
,STDICA
,DifICA_STDICA
,ICAAjustado
,AvesXm2
,KgXm2
,IEP
,DiasLimpieza
,DiasCrianza
,TotalCampana
,DiasSaca
,EdadInicioSaca
,CantInicioSaca
,EdadGranja
,Gas
,Cama
,Agua
,PorcMacho
,Pigmentacion
,EventDate
,categoria
,PesoAlo
,FlagAtipico
,EdadPadreCorral
,GasGalpon
,PesoHvo
,EdadPadreCorralDescrip
,DiasAloj
,FechaDesinfeccion
,TipoCama
,FormaReutilizacion
,NReuso
,PDE
,PDT
,PromAmoniaco
,PromTemperatura
,Referencia
FROM {database_name}.ft_consolidado_Corral A
LEFT JOIN GuiaRemision B ON A.ComplexEntityNo = B.ComplexEntityNo
WHERE pk_empresa = 1 AND pk_division = 3 
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_ReferenciaCorral"
}
df_ft_consolidado_ReferenciaCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_ReferenciaCorral")
print('carga ft_consolidado_ReferenciaCorral')
#Se muestra ft_Tercios_Lote_Plantel
df_UPDft_consolidado_Corral3 = spark.sql(f"""
with PolloMediano as (
select ComplexEntityNo,nproducto,SUM(AvesGranja) AvesGranja,SUM(PesoGranja) PesoGranja,SUM(PesoGranja)/SUM(AvesGranja) PesoPromGranja
from {database_name}.ft_ventas_cd A
left join {database_name}.lk_producto B on A.pk_productos = B.pk_producto
where A.pk_productos in (3902,3903) and ComplexEntityNo not in ('P211-2205-04-02','P294-2205-04-01') group by ComplexEntityNo,nproducto)

select 
 A.fecha               
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
,A.eventdate           
,A.categoria           
,A.pesoalo             
,A.flagatipico         
,A.edadpadrecorral     
,A.gasgalpon           
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
,A.ruidosrespiratorios 
,A.fechadesinfeccion   
,A.tipocama            
,A.formareutilizacion  
,A.nreuso              
,A.pde                 
,A.pdt                 
,A.promamoniaco        
,A.promtemperatura     
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
,B.nproducto nProductoMediano
,B.AvesGranja AvesRendidasMediano
,B.PesoGranja KilosRendidosMediano
,B.PesoPromGranja kilosRendidosPromMediano
,(B.AvesGranja*1.0)/A.AvesRendidas PorcPolloMediano
from {database_name}.ft_consolidado_Corral A
left join PolloMediano B on A.ComplexEntityNo = B.ComplexEntityNo
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Corral3.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral_new")

df_ft_consolidado_Corral_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Corral_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Corral"
}
df_ft_consolidado_Corral_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Corral")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Corral_new")

print('carga UPD ft_consolidado_Corral')
#Se muestra UPD ft_consolidado_Lote
df_UPDft_consolidado_Lote = spark.sql(f"""
with 
 PolloMediano as (select ComplexEntityNo,nproducto,SUM(AvesGranja) AvesGranja,SUM(PesoGranja) PesoGranja,SUM(PesoGranja)/SUM(AvesGranja) PesoPromGranja
                    from {database_name}.ft_ventas_cd A
                    left join {database_name}.lk_producto B on A.pk_productos = B.pk_producto
                    where A.pk_productos in (3902,3903) and ComplexEntityNo not in ('P211-2205-04-02','P294-2205-04-01') group by ComplexEntityNo,nproducto)
,PolloMedianoLote as (select substring(complexentityno,1,(LENGTH(complexentityno)-6)) ComplexEntityNoLote,SUM(A.AvesGranja) AvesRendidasMediano, MAX(B.AvesGranja) AvesRendidasTotal
                        ,SUM(A.AvesGranja*1.0)/COALESCE(MAX(B.AvesGranja),0) PorcAvesRendidasMediano
                    from PolloMediano A
                    left join (select substring(complexentityno,1,(LENGTH(complexentityno)-6)) ComplexEntityNoLote,SUM(AvesGranja) AvesGranja from {database_name}.ft_ventas_cd group by substring(complexentityno,1,(LENGTH(complexentityno)-6))) B on substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) = B.ComplexEntityNoLote
                    group by substring(complexentityno,1,(LENGTH(complexentityno)-6)) )
select A.*,
B.AvesRendidasMediano,
B.PorcAvesRendidasMediano
from {database_name}.ft_consolidado_Lote A
left join PolloMedianoLote B on A.ComplexEntityNo = B.ComplexEntityNoLote
where a.pk_empresa = 1 and A.pk_division = 3
UNION 
select A.*,
'' AvesRendidasMediano,
'' PorcAvesRendidasMediano
from {database_name}.ft_consolidado_Lote A
left join PolloMedianoLote B on A.ComplexEntityNo = B.ComplexEntityNoLote
where a.pk_empresa = 1 and A.pk_division <> 3
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Lote.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote_new")

df_ft_consolidado_Lote_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Lote_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote"
}
df_ft_consolidado_Lote_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote_new")

print('carga UPD ft_consolidado_Lote')
#Se muestra PonderadoLoteDiario
df_PonderadoLoteDiario = spark.sql(f"""
with temp as (select fecha
,pk_empresa
,pk_division
,pk_plantel
,pk_lote
,substring(complexentityno,1,(LENGTH(complexentityno)-6)) ComplexEntityNo
,PobInicial
,PorcMortAcum
,pk_diasvida*PobInicial pk_diasvidaXPobInicial
,PorcMortDia*PobInicial PorcMortDiaXPobInicial
,PorcMortAcum*PobInicial PorcMortAcumXPobInicial
,Peso*PobInicial PesoXPobInicial
,PesoDia*PobInicial PesoDiaXPobInicial
,PesoAlo*PobInicial PesoAloXPobInicial
,PesoHvo*PobInicial PesoHvoXPobInicial
,STDMortDia*PobInicial STDMortDiaXPobInicial
,STDMortAcum*PobInicial STDMortAcumXPobInicial
,STDConsDia*PobInicial STDConsDiaXPobInicial
,STDConsAcum*PobInicial STDConsAcumXPobInicial
,STDPeso*PobInicial STDPesoXPobInicial
,STDICA*PobInicial STDICAXPobInicial
from {database_name}.ProduccionDetalleEdad where ComplexEntityNo like 'P%' )
select fecha
,pk_empresa
,pk_division
,pk_plantel
,pk_lote
,ComplexEntityNo
,SUM(pk_diasvidaXPobInicial) pk_diasvidaXPobInicial
,SUM(PorcMortDiaXPobInicial) PorcMortDiaXPobInicial
,SUM(PorcMortAcumXPobInicial) PorcMortAcumXPobInicial
,SUM(PesoXPobInicial) PesoXPobInicial
,SUM(PesoDiaXPobInicial) PesoDiaXPobInicial
,SUM(PesoAloXPobInicial) PesoAloXPobInicial
,SUM(PesoHvoXPobInicial) PesoHvoXPobInicial
,SUM(STDMortDiaXPobInicial) STDMortDiaXPobInicial
,SUM(STDMortAcumXPobInicial) STDMortAcumXPobInicial
,SUM(STDConsDiaXPobInicial) STDConsDiaXPobInicial
,SUM(STDConsAcumXPobInicial) STDConsAcumXPobInicial
,SUM(STDPesoXPobInicial) STDPesoXPobInicial
,SUM(STDICAXPobInicial) STDICAXPobInicial
from temp A
group by fecha
,pk_empresa
,pk_division
,pk_plantel
,pk_lote
,ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoLoteDiario"
}
df_PonderadoLoteDiario.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoLoteDiario")
print('carga PonderadoLoteDiario')
#Se muestra PonderadoLoteDiario2
df_PonderadoLoteDiario2 = spark.sql(f"""
with 
 c1 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where pk_diasvida<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c2 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where PorcMortDia<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6))) 
,c3 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where PorcMortAcum<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c4 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where PesoDia<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c5 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where Peso<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c6 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where PesoAlo<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c7 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where PesoHvo<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c8 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDMortDia<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c9 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDMortAcum<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c10 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDConsDia<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c11 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDConsAcum<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c12 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDPeso<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
,c13 as (select fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) co,sum(PobInicial) PobInicial from {database_name}.ProduccionDetalleEdad where STDICA<>0 group by fecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)))
select
 A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_plantel
,A.pk_lote
,MAX(pk_diasvidaXPobInicial)/COALESCE(c1.PobInicial,0) pk_diasvida
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) ComplexEntityNo
,MAX(PorcMortDiaXPobInicial) / COALESCE(c2.PobInicial,0) PorcMortDia
,MAX(PorcMortAcumXPobInicial) / COALESCE(c3.PobInicial,0) PorcMortAcum
,MAX(PesoDiaXPobInicial)/ COALESCE(c4.PobInicial,0) PesoDia
,MAX(PesoXPobInicial) / COALESCE(c5.PobInicial,0) Peso
,MAX(PesoAloXPobInicial) / COALESCE(c6.PobInicial,0) PesoAlo		
,MAX(PesoHvoXPobInicial) / COALESCE(c7.PobInicial,0) PesoHvo		
,MAX(STDMortDiaXPobInicial)/COALESCE(c8.PobInicial,0) STDMortDia
,MAX(STDMortAcumXPobInicial)/COALESCE(c9.PobInicial,0) STDMortAcum
,MAX(STDConsDiaXPobInicial)/COALESCE(c10.PobInicial,0) STDConsDia
,MAX(STDConsAcumXPobInicial)/COALESCE(c11.PobInicial,0) STDConsAcum
,MAX(STDPesoXPobInicial)/COALESCE(c12.PobInicial,0) STDPeso
,MAX(STDICAXPobInicial)/COALESCE(c13.PobInicial,0) STDICA
from {database_name}.ProduccionDetalleEdad A
left join {database_name}.PonderadoLoteDiario B on A.fecha = B.fecha and A.pk_division = B.pk_division and A.pk_plantel = B.pk_plantel and substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) = B.ComplexEntityNo
LEFT JOIN c1 on c1.fecha = A.fecha and c1.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c2 on c2.fecha = A.fecha and c2.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c3 on c3.fecha = A.fecha and c3.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c4 on c4.fecha = A.fecha and c4.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c5 on c5.fecha = A.fecha and c5.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c6 on c6.fecha = A.fecha and c6.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c7 on c7.fecha = A.fecha and c7.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c8 on c8.fecha = A.fecha and c8.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c9 on c9.fecha = A.fecha and c9.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c10 on c10.fecha = A.fecha and c10.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c11 on c11.fecha = A.fecha and c11.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c12 on c12.fecha = A.fecha and c12.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
LEFT JOIN c13 on c13.fecha = A.fecha and c13.co = substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))  
where A.ComplexEntityNo like 'P%'
group by A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_plantel
,A.pk_lote
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))
,c1.PobInicial,c2.PobInicial,c3.PobInicial,c4.PobInicial,c5.PobInicial,c6.PobInicial,c7.PobInicial,c8.PobInicial,c9.PobInicial,c10.PobInicial,c11.PobInicial,c12.PobInicial,c13.PobInicial
order by A.fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoLoteDiario2"
}
df_PonderadoLoteDiario2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoLoteDiario2")
print('carga PonderadoLoteDiario2')
#Se muestra ProduccionDetalleLoteDiario
df_ProduccionDetalleLoteDiario = spark.sql(f"""
select
 A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_proveedor
,MIN(A.pk_estado) pk_estado
,MAX(B.pk_diasvida) pk_diasvida
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) ComplexEntityNo
,min(FechaNacimiento) FechaNacimiento
,max(FechaCierre) FechaCierre
,min(FechaAlojamiento) FechaAlojamiento
,SUM(AreaGalpon) AS AreaGalpon
,SUM(A.PobInicial) AS PobInicial
,SUM(AvesRendidas) AS AvesRendidas
,SUM(KilosRendidos) AS KilosRendidos
,SUM(MortDia) AS MortDia
,SUM(MortAcum) AS MortAcum
,MAX(B.PorcMortDia) PorcMortDia
,MAX(B.PorcMortAcum) PorcMortAcum
,MAX(B.PesoDia) PesoDia
,MAX(B.Peso) Peso
,MAX(B.PesoAlo) PesoAlo	
,MAX(B.PesoHvo) PesoHvo	
,SUM(UnidSeleccion) as UnidSeleccion
,SUM(ConsDia) as ConsDia
,SUM(ConsAcum) as ConsAcum
,SUM(PreInicio) as PreInicio
,SUM(Inicio) as Inicio
,SUM(Acabado) as Acabado
,SUM(Terminado) as Terminado
,SUM(Finalizador) as Finalizador
,SUM(Ganancia) Ganancia
,MAX(B.STDMortDia) STDMortDia
,MAX(B.STDMortAcum) STDMortAcum
,MAX(B.STDConsDia) STDConsDia
,MAX(B.STDConsAcum) STDConsAcum
,MAX(B.STDPeso) STDPeso
,MAX(B.STDICA) STDICA
,AVG(Gas) as Gas
,AVG(Cama) as Cama
,AVG(Agua) as Agua
,SUM(CantMacho) as CantMacho
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
,SUM(Pigmentacion) AS Pigmentacion
from {database_name}.ProduccionDetalleEdad A
left join {database_name}.PonderadoLoteDiario2 B on A.fecha = B.fecha and A.pk_division = B.pk_division and A.pk_plantel = B.pk_plantel and substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) = B.ComplexEntityNo
where A.ComplexEntityNo like 'P%'
group by A.fecha
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_proveedor
,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6))
order by A.fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ProduccionDetalleLoteDiario"
}
df_ProduccionDetalleLoteDiario.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ProduccionDetalleLoteDiario")
print('carga ProduccionDetalleLoteDiario')
#Se muestra Acumulados
df_Acumulados = spark.sql(f"""
with 
 c1  as (select ComplexEntityNo,fecha,max(PobInicial) PobInicial from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c2  as (select ComplexEntityNo,fecha,sum(U_PEAccidentados) U_PEAccidentados from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c3  as (select ComplexEntityNo,fecha,sum(U_PEHigadoGraso) U_PEHigadoGraso from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c4  as (select ComplexEntityNo,fecha,sum(U_PEHepatomegalia) U_PEHepatomegalia from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c5  as (select ComplexEntityNo,fecha,sum(U_PEHigadoHemorragico) U_PEHigadoHemorragico from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c6  as (select ComplexEntityNo,fecha,sum(U_PEInanicion) U_PEInanicion from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c7  as (select ComplexEntityNo,fecha,sum(U_PEProblemaRespiratorio) U_PEProblemaRespiratorio from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c8  as (select ComplexEntityNo,fecha,sum(U_PESCH) U_PESCH from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c9  as (select ComplexEntityNo,fecha,sum(U_PEEnteritis) U_PEEnteritis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c10 as (select ComplexEntityNo,fecha,sum(U_PEMuerteSubita) U_PEMuerteSubita from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha) 
,c11 as (select ComplexEntityNo,fecha,sum(U_PEEstresPorCalor) U_PEEstresPorCalor from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c12 as (select ComplexEntityNo,fecha,sum(U_PEHidropericardio) U_PEHidropericardio from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c13 as (select ComplexEntityNo,fecha,sum(U_PEHemopericardio) U_PEHemopericardio from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c14 as (select ComplexEntityNo,fecha,sum(U_PEUratosis) U_PEUratosis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c15 as (select ComplexEntityNo,fecha,sum(U_PEMaterialCaseoso) U_PEMaterialCaseoso from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c16 as (select ComplexEntityNo,fecha,sum(U_PEOnfalitis) U_PEOnfalitis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c17 as (select ComplexEntityNo,fecha,sum(U_PERetencionDeYema) U_PERetencionDeYema from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c18 as (select ComplexEntityNo,fecha,sum(U_PEErosionDeMolleja) U_PEErosionDeMolleja from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c19 as (select ComplexEntityNo,fecha,sum(U_PEHemorragiaMusculos) U_PEHemorragiaMusculos from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c20 as (select ComplexEntityNo,fecha,sum(U_PESangreEnCiego) U_PESangreEnCiego from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c21 as (select ComplexEntityNo,fecha,sum(U_PEPericarditis) U_PEPericarditis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c22 as (select ComplexEntityNo,fecha,sum(U_PEPeritonitis) U_PEPeritonitis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c23 as (select ComplexEntityNo,fecha,sum(U_PEProlapso) U_PEProlapso from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c24 as (select ComplexEntityNo,fecha,sum(U_PEPicaje) U_PEPicaje from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c25 as (select ComplexEntityNo,fecha,sum(U_PERupturaAortica) U_PERupturaAortica from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c26 as (select ComplexEntityNo,fecha,sum(U_PEBazoMoteado) U_PEBazoMoteado from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c27 as (select ComplexEntityNo,fecha,sum(U_PENoViable) U_PENoViable from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
,c28 as (select ComplexEntityNo,fecha,sum(U_PEAscitis) U_PEAscitis from {database_name}.ProduccionDetalleLoteDiario group by ComplexEntityNo,fecha)
select a.fecha, a.ComplexEntityNo
,max(c1.PobInicial) PobInicial
,sum(c2.U_PEAccidentados) U_PEAccidentadosAcum
,sum(c3.U_PEHigadoGraso)          U_PEHigadoGrasoAcum
,sum(c4.U_PEHepatomegalia)        U_PEHepatomegaliaAcum
,sum(c5.U_PEHigadoHemorragico)    U_PEHigadoHemorragicoAcum
,sum(c6.U_PEInanicion)            U_PEInanicionAcum
,sum(c7.U_PEProblemaRespiratorio) U_PEProblemaRespiratorioAcum
,sum(c8.U_PESCH)                  U_PESCHAcum
,sum(c9.U_PEEnteritis)            U_PEEnteritisAcum
,sum(c28.U_PEAscitis)              U_PEAscitisAcum
,sum(c10.U_PEMuerteSubita)         U_PEMuerteSubitaAcum
,sum(c11.U_PEEstresPorCalor)       U_PEEstresPorCalorAcum
,sum(c12.U_PEHidropericardio)      U_PEHidropericardioAcum
,sum(c13.U_PEHemopericardio)       U_PEHemopericardioAcum
,sum(c14.U_PEUratosis)             U_PEUratosisAcum
,sum(c15.U_PEMaterialCaseoso)      U_PEMaterialCaseosoAcum
,sum(c16.U_PEOnfalitis)            U_PEOnfalitisAcum
,sum(c17.U_PERetencionDeYema)      U_PERetencionDeYemaAcum
,sum(c18.U_PEErosionDeMolleja)     U_PEErosionDeMollejaAcum
,sum(c19.U_PEHemorragiaMusculos)   U_PEHemorragiaMusculosAcum
,sum(c20.U_PESangreEnCiego)        U_PESangreEnCiegoAcum
,sum(c21.U_PEPericarditis)         U_PEPericarditisAcum
,sum(c22.U_PEPeritonitis)          U_PEPeritonitisAcum
,sum(c23.U_PEProlapso)             U_PEProlapsoAcum
,sum(c24.U_PEPicaje)               U_PEPicajeAcum
,sum(c25.U_PERupturaAortica)       U_PERupturaAorticaAcum
,sum(c26.U_PEBazoMoteado)          U_PEBazoMoteadoAcum
,sum(c27.U_PENoViable)             U_PENoViableAcum
from {database_name}.ProduccionDetalleLoteDiario A
left join c1 on c1.ComplexEntityNo = a.ComplexEntityNo and c1.fecha = a.fecha
left join c2 on  c2.ComplexEntityNo = a.ComplexEntityNo and c2.fecha <= a.fecha
left join c3 on  c3.ComplexEntityNo = a.ComplexEntityNo and c3.fecha <= a.fecha
left join c4 on  c4.ComplexEntityNo = a.ComplexEntityNo and c4.fecha <= a.fecha
left join c5 on  c5.ComplexEntityNo = a.ComplexEntityNo and c5.fecha <= a.fecha
left join c6 on  c6.ComplexEntityNo = a.ComplexEntityNo and c6.fecha <= a.fecha
left join c7 on  c7.ComplexEntityNo = a.ComplexEntityNo and c7.fecha <= a.fecha
left join c8 on  c8.ComplexEntityNo = a.ComplexEntityNo and c8.fecha <= a.fecha
left join c9 on  c9.ComplexEntityNo = a.ComplexEntityNo and c9.fecha <= a.fecha
left join c10 on c10.ComplexEntityNo = a.ComplexEntityNo and c10.fecha <= a.fecha
left join c11 on c11.ComplexEntityNo = a.ComplexEntityNo and c11.fecha <= a.fecha
left join c12 on c12.ComplexEntityNo = a.ComplexEntityNo and c12.fecha <= a.fecha
left join c13 on c13.ComplexEntityNo = a.ComplexEntityNo and c13.fecha <= a.fecha
left join c14 on c14.ComplexEntityNo = a.ComplexEntityNo and c14.fecha <= a.fecha
left join c15 on c15.ComplexEntityNo = a.ComplexEntityNo and c15.fecha <= a.fecha
left join c16 on c16.ComplexEntityNo = a.ComplexEntityNo and c16.fecha <= a.fecha
left join c17 on c17.ComplexEntityNo = a.ComplexEntityNo and c17.fecha <= a.fecha
left join c18 on c18.ComplexEntityNo = a.ComplexEntityNo and c18.fecha <= a.fecha
left join c19 on c19.ComplexEntityNo = a.ComplexEntityNo and c19.fecha <= a.fecha
left join c20 on c20.ComplexEntityNo = a.ComplexEntityNo and c20.fecha <= a.fecha
left join c21 on c21.ComplexEntityNo = a.ComplexEntityNo and c21.fecha <= a.fecha
left join c22 on c22.ComplexEntityNo = a.ComplexEntityNo and c22.fecha <= a.fecha
left join c23 on c23.ComplexEntityNo = a.ComplexEntityNo and c23.fecha <= a.fecha
left join c24 on c24.ComplexEntityNo = a.ComplexEntityNo and c24.fecha <= a.fecha
left join c25 on c25.ComplexEntityNo = a.ComplexEntityNo and c25.fecha <= a.fecha
left join c26 on c26.ComplexEntityNo = a.ComplexEntityNo and c26.fecha <= a.fecha
left join c27 on c27.ComplexEntityNo = a.ComplexEntityNo and c27.fecha <= a.fecha
left join c28 on c28.ComplexEntityNo = a.ComplexEntityNo and c28.fecha <= a.fecha
group by a.fecha, a.ComplexEntityNo
order by a.fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Acumulados"
}
df_Acumulados.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Acumulados")
print('carga Acumulados')
#Se muestra OrdenarMortalidades
df_OrdenarMortalidades = spark.sql(f"""
SELECT *,
    ROUND((cmortalidad / COALESCE(PobInicial * 1.0, 1) * 100), 5) AS pcmortalidad,
    ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) AS orden,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'CantPrimLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'CantSegLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'CantTerLesion'
    END AS OrdenCantidad,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'NomPrimLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'NomSegLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'NomTerLesion'
    END AS OrdenNombre,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 1 THEN 'PorcPrimLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 2 THEN 'PorcSegLesion'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidad DESC) = 3 THEN 'PorcTerLesion'
    END AS OrdenPorcentaje
FROM {database_name}.ProduccionDetalleLoteDiario 
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
    'U_PENoViable', U_PENoViable
)) AS Causa, cmortalidad
WHERE cmortalidad <> 0
ORDER BY fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidades"
}
df_OrdenarMortalidades.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidades")
print('carga OrdenarMortalidades')
#Se muestra OrdenarMortalidades2
df_OrdenarMortalidades2 = spark.sql(f"""
WITH Mortalidades AS (
    SELECT 
        fecha,
        complexentityno,
        cmortalidad,
        ordenCantidad,
        pcmortalidad,
        ordenPorcentaje,
        causa,
        ordenNombre
    FROM {database_name}.OrdenarMortalidades
),

Cantidades AS (
    SELECT 
        fecha,
        complexentityno,
        MAX(CASE WHEN ordenCantidad = 'CantPrimLesion' THEN cmortalidad END) AS CantPrimLesion,
        MAX(CASE WHEN ordenCantidad = 'CantSegLesion' THEN cmortalidad END) AS CantSegLesion,
        MAX(CASE WHEN ordenCantidad = 'CantTerLesion' THEN cmortalidad END) AS CantTerLesion
    FROM Mortalidades
    GROUP BY fecha, complexentityno
),

Porcentajes AS (
    SELECT 
        fecha,
        complexentityno,
        MAX(CASE WHEN ordenPorcentaje = 'PorcPrimLesion' THEN pcmortalidad END) AS PorcPrimLesion,
        MAX(CASE WHEN ordenPorcentaje = 'PorcSegLesion' THEN pcmortalidad END) AS PorcSegLesion,
        MAX(CASE WHEN ordenPorcentaje = 'PorcTerLesion' THEN pcmortalidad END) AS PorcTerLesion
    FROM Mortalidades
    GROUP BY fecha, complexentityno
),

Nombres AS (
    SELECT 
        fecha,
        complexentityno,
        MAX(CASE WHEN ordenNombre = 'NomPrimLesion' THEN causa END) AS NomPrimLesion,
        MAX(CASE WHEN ordenNombre = 'NomSegLesion' THEN causa END) AS NomSegLesion,
        MAX(CASE WHEN ordenNombre = 'NomTerLesion' THEN causa END) AS NomTerLesion
    FROM Mortalidades
    GROUP BY fecha, complexentityno
)

SELECT 
    A.fecha,
    A.complexentityno,
    A.CantPrimLesion, A.CantSegLesion, A.CantTerLesion,
    B.PorcPrimLesion, B.PorcSegLesion, B.PorcTerLesion,
    C.NomPrimLesion, C.NomSegLesion, C.NomTerLesion
FROM Cantidades A
LEFT JOIN Porcentajes B ON A.complexentityno = B.complexentityno AND A.fecha = B.fecha
LEFT JOIN Nombres C ON A.complexentityno = C.complexentityno AND A.fecha = C.fecha
ORDER BY A.fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidades2"
}
df_OrdenarMortalidades2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidades2")
print('carga OrdenarMortalidades2')
#Se muestra OrdenarMortalidadesAcum
df_OrdenarMortalidadesAcum = spark.sql(f"""
WITH AcumuladosUnpivot AS (
    SELECT 
        fecha,
        complexentityno,
        PobInicial,
        exploded_causa.key AS causa,
        exploded_causa.value AS cmortalidadAcum
    FROM {database_name}.Acumulados
    LATERAL VIEW EXPLODE(
        MAP(
            'U_PEAccidentadosAcum', U_PEAccidentadosAcum,
            'U_PEHigadoGrasoAcum', U_PEHigadoGrasoAcum,
            'U_PEHepatomegaliaAcum', U_PEHepatomegaliaAcum,
            'U_PEHigadoHemorragicoAcum', U_PEHigadoHemorragicoAcum,
            'U_PEInanicionAcum', U_PEInanicionAcum,
            'U_PEProblemaRespiratorioAcum', U_PEProblemaRespiratorioAcum,
            'U_PESCHAcum', U_PESCHAcum,
            'U_PEEnteritisAcum', U_PEEnteritisAcum,
            'U_PEAscitisAcum', U_PEAscitisAcum,
            'U_PEMuerteSubitaAcum', U_PEMuerteSubitaAcum,
            'U_PEEstresPorCalorAcum', U_PEEstresPorCalorAcum,
            'U_PEHidropericardioAcum', U_PEHidropericardioAcum,
            'U_PEHemopericardioAcum', U_PEHemopericardioAcum,
            'U_PEUratosisAcum', U_PEUratosisAcum,
            'U_PEMaterialCaseosoAcum', U_PEMaterialCaseosoAcum,
            'U_PEOnfalitisAcum', U_PEOnfalitisAcum,
            'U_PERetencionDeYemaAcum', U_PERetencionDeYemaAcum,
            'U_PEErosionDeMollejaAcum', U_PEErosionDeMollejaAcum,
            'U_PEHemorragiaMusculosAcum', U_PEHemorragiaMusculosAcum,
            'U_PESangreEnCiegoAcum', U_PESangreEnCiegoAcum,
            'U_PEPericarditisAcum', U_PEPericarditisAcum,
            'U_PEPeritonitisAcum', U_PEPeritonitisAcum,
            'U_PEProlapsoAcum', U_PEProlapsoAcum,
            'U_PEPicajeAcum', U_PEPicajeAcum,
            'U_PERupturaAorticaAcum', U_PERupturaAorticaAcum,
            'U_PEBazoMoteadoAcum', U_PEBazoMoteadoAcum,
            'U_PENoViableAcum', U_PENoViableAcum
        )
    ) exploded_causa
)

SELECT *,
    ROUND((cmortalidadAcum / COALESCE(PobInicial * 1.0, 0) * 100), 5) AS pcmortalidadAcum,
    ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) AS ordenAcum,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 1 THEN 'CantPrimLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 2 THEN 'CantSegLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 3 THEN 'CantTerLesionAcum'
    END AS OrdenCantidadAcum,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 1 THEN 'NomPrimLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 2 THEN 'NomSegLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 3 THEN 'NomTerLesionAcum'
    END AS OrdenNombreAcum,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 1 THEN 'PorcPrimLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 2 THEN 'PorcSegLesionAcum'
        WHEN ROW_NUMBER() OVER (PARTITION BY complexentityno, fecha ORDER BY cmortalidadAcum DESC) = 3 THEN 'PorcTerLesionAcum'
    END AS OrdenPorcentajeAcum
FROM AcumuladosUnpivot
WHERE cmortalidadAcum <> 0
ORDER BY fecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesAcum"
}
df_OrdenarMortalidadesAcum.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidadesAcum")
print('carga OrdenarMortalidadesAcum')
#Se muestra OrdenarMortalidadesAcum2
df_OrdenarMortalidadesAcum2 = spark.sql(f"""
SELECT 
    A.fecha, 
    A.ComplexEntityNo,
    MAX(CASE WHEN A.ordenCantidadAcum = 'CantPrimLesionAcum' THEN A.cmortalidadAcum END) AS CantPrimLesionAcum,
    MAX(CASE WHEN A.ordenCantidadAcum = 'CantSegLesionAcum' THEN A.cmortalidadAcum END) AS CantSegLesionAcum,
    MAX(CASE WHEN A.ordenCantidadAcum = 'CantTerLesionAcum' THEN A.cmortalidadAcum END) AS CantTerLesionAcum,
    MAX(CASE WHEN B.ordenPorcentajeAcum = 'PorcPrimLesionAcum' THEN B.pcmortalidadAcum END) AS PorcPrimLesionAcum,
    MAX(CASE WHEN B.ordenPorcentajeAcum = 'PorcSegLesionAcum' THEN B.pcmortalidadAcum END) AS PorcSegLesionAcum,
    MAX(CASE WHEN B.ordenPorcentajeAcum = 'PorcTerLesionAcum' THEN B.pcmortalidadAcum END) AS PorcTerLesionAcum,
    MAX(CASE WHEN C.ordenNombreAcum = 'NomPrimLesionAcum' THEN C.causa END) AS NomPrimLesionAcum,
    MAX(CASE WHEN C.ordenNombreAcum = 'NomSegLesionAcum' THEN C.causa END) AS NomSegLesionAcum,
    MAX(CASE WHEN C.ordenNombreAcum = 'NomTerLesionAcum' THEN C.causa END) AS NomTerLesionAcum
FROM {database_name}.OrdenarMortalidadesAcum A
LEFT JOIN {database_name}.OrdenarMortalidadesAcum B 
    ON A.ComplexEntityNo = B.ComplexEntityNo AND A.fecha = B.fecha
LEFT JOIN {database_name}.OrdenarMortalidadesAcum C 
    ON A.ComplexEntityNo = C.ComplexEntityNo AND A.fecha = C.fecha
GROUP BY A.fecha, A.ComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/OrdenarMortalidadesAcum2"
}
df_OrdenarMortalidadesAcum2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.OrdenarMortalidadesAcum2")
print('carga OrdenarMortalidadesAcum2')
#Se muestra ft_consolidado_Lote_Diario
df_ft_consolidado_Lote_Diario = spark.sql(f"""
select
 A.fecha
,PK_empresa
,PK_division
,PK_zona
,PK_subzona
,PK_plantel
,PK_lote
,PK_proveedor
,PK_estado
,PK_diasvida
,A.ComplexEntityNo
,FechaNacimiento
,FechaCierre
,FechaAlojamiento
,AreaGalpon
,A.PobInicial
,A.PobInicial - COALESCE(MortAcum,0) as Inventario
,AvesRendidas
,KilosRendidos
,MortDia
,MortAcum
,round((((STDMortDia)/100)*(A.PobInicial)),0) as STDMortDia
,round((((STDMortAcum)/100)*(A.PobInicial)),0) as STDMortAcum
,round(((PorcMortDia)),2) as PorcMortDia --
,round(((PorcMortAcum)),2) as PorcMortAcum
,round(((STDMortDia)),2) as STDPorcMortDia
,round(((STDMortAcum)),2) as STDPorcMortAcum
,round((round(((PorcMortDia)),2) - (STDMortDia)),2) as DifPorcMort_STD
,round((round(((PorcMortAcum)),2) - (STDMortAcum)),2) as DifPorcMortAcum_STDAcum
,(ConsDia) as Consumo
,(ConsAcum) as ConsAcum
,round(((ConsDia) / COALESCE(((A.PobInicial) - (MortAcum)),0)),3) as PorcConsDia
,round(((ConsAcum) / COALESCE(((A.PobInicial) - (MortAcum)),0)),3) as PorcConsAcum
,round(((STDConsDia)),3) as STDConsDia
,round(((STDConsAcum)),3) as STDConsAcum
,round(((STDConsDia)*((A.PobInicial) - (COALESCE(MortAcum,0)))),3) as STDConsDiaKg
,round(((STDConsAcum)*((A.PobInicial) - (COALESCE(MortAcum,0)))),3) as STDConsAcumKg
,((ConsDia) - round(((STDConsDia)*((A.PobInicial) - (COALESCE(MortAcum,0)))),3)) as DifCons_STD
,((ConsAcum) - round(((STDConsAcum)*((A.PobInicial) - (COALESCE(MortAcum,0)))),3)) as DifConsAcum_STDAcum
,round(((((ConsDia) / COALESCE(((A.PobInicial) - (MortAcum)),0)) - (STDConsDia)) * 1000),3) as DifPorcCons_STD
,round(((((ConsAcum) / COALESCE(((A.PobInicial) - (MortAcum)),0)) - (STDConsAcum)) * 1000),3) as DifPorcConsAcum_STDAcum
,PreInicio
,Inicio
,Acabado
,Terminado
,Finalizador
,Peso
,STDPeso
,((Peso) - (STDPeso)) * 1000 as DifPeso_STD
,PesoAlo
,((ConsAcum) / COALESCE(((A.PobInicial) - (COALESCE(MortAcum,0))),0)) / COALESCE((PesoDia),0) as ICA
,STDICA
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
,Pigmentacion
,PesoHvo
,B.CantPrimLesion,B.CantSegLesion,B.CantTerLesion,B.PorcPrimLesion,B.PorcSegLesion,B.PorcTerLesion,B.NomPrimLesion,B.NomSegLesion,B.NomTerLesion
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
,D.CantPrimLesionAcum,D.CantSegLesionAcum,D.CantTerLesionAcum,D.PorcPrimLesionAcum,D.PorcSegLesionAcum,D.PorcTerLesionAcum,D.NomPrimLesionAcum,D.NomSegLesionAcum,D.NomTerLesionAcum
from {database_name}.ProduccionDetalleLoteDiario A
left join {database_name}.OrdenarMortalidades2 B on A.fecha = B.fecha and A.ComplexEntityNo = B.ComplexEntityNo
left join {database_name}.Acumulados C on A.fecha = C.fecha and A.ComplexEntityNo = C.ComplexEntityNo
left join {database_name}.OrdenarMortalidadesAcum2 D on A.fecha = D.fecha and A.ComplexEntityNo = D.ComplexEntityNo
where (date_format(CAST(a.fecha AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(a.fecha AS timestamp),'yyyyMM') <= {AnioMesFin})
""")

try:
    df = spark.table("{database_name}.ft_consolidado_Lote_Diario")
    # Crear una tabla temporal con los datos filtrados
    df_filtered = df.where(
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") < F.lit(AnioMes)) |
        (F.date_format(F.col("fecha").cast("timestamp"), "yyyyMM") > F.lit(AnioMesFin))
    )
    df_ft_consolidado_Lote_Diario_new = df_ft_consolidado_Lote_Diario.union(df_filtered)
except Exception as e:
    df_ft_consolidado_Lote_Diario_new = df_ft_consolidado_Lote_Diario
    
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_ft_consolidado_Lote_Diario_new.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote_Diario_new")

df_ft_consolidado_Lote_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Lote_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Lote_Diario"
}
df_ft_consolidado_Lote_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Lote_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Lote_Diario_new")

print('carga ft_consolidado_Lote_Diario')
#Se muestra ConsolidadoDiarioProyectado
df_ConsolidadoDiarioProyectado = spark.sql(f"""
WITH temp1 as (select ProteinStandardVersionsIRN,Age,SUM(U_MortPercent) U_MortPercent  from {database_name}.si_BrimStandardsData group by ProteinStandardVersionsIRN,Age)
,si_mvBrimStandardsData as (select BSD.*, temp1.U_MortPercent U_MortAcumPercent from {database_name}.si_BrimStandardsData BSD left join temp1 on temp1.ProteinStandardVersionsIRN = BSD.ProteinStandardVersionsIRN AND temp1.Age <= BSD.Age order by BSD.ProteinStandardVersionsIRN, BSD.Age)
,STD42DIAS as (
SELECT ProteinStandardVersionsIRN,Age,U_MortPercent,U_MortAcumPercent,U_MortPorcSem,U_FeedConsumed,U_ConsAlimAcum,U_PesoVivo,U_WeightGainDay,U_FeedConversionBC
FROM si_mvBrimStandardsData WHERE Age = 42) 
,DiaMax as (SELECT MAX(pk_diasvida) pk_diasvida, ComplexEntityNo FROM {database_name}.ft_consolidado_Diario WHERE pk_empresa = 1 AND pk_division = 3 GROUP BY ComplexEntityNo)
,temp2 as (select ComplexEntityno, ProteinStandardVersionsIRN from {database_name}.ProduccionDetalle group by ComplexEntityno,ProteinStandardVersionsIRN)
SELECT
	'18991130' fecha
	,DATEADD(DAY,ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo ORDER BY C.pk_diasvida ASC) ,cast(fecha as date)) FechaProyectada
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
	,A.pk_grupoconsumo
	,A.pk_especie
	,A.pk_estado
	,A.pk_administrador
	,A.pk_proveedor
	,C.pk_diasvida
	,C.pk_semanavida
	,A.ComplexEntityNo
	,A.Nacimiento
	,C.pk_diasvida Edad
	,A.FechaCierre
	,A.FechaAlojamiento
	,A.PobInicial
	,A.AvesRendidasAcum
	,A.KilosRendidosAcum
	,A.MortAcum
	,A.PorcMortSem
	,U_MortPercent STDPorcMortDia
	,U_MortAcumPercent STDPorcMortAcum
	,U_MortPorcSem STDPorcMortSem
	,A.AreaGalpon
	,A.ConsAcum
	,A.PorcConsAcum
	,U_FeedConsumed STDConsDia
	,U_ConsAlimAcum STDConsAcum
	,U_PesoVivo STDPeso
	,A.PesoAlo
	,U_WeightGainDay STDGanancia
	,U_FeedConversionBC STDICA
	,A.categoria
	,A.PadreMayor
	,A.EdadPadreCorralDescrip
	,D.ProteinStandardVersionsIRN
	,F.grupoproducto GrupoProducto
	,G.nsexo Sexo
FROM {database_name}.ft_consolidado_diario A
LEFT JOIN DiaMax B on A.ComplexEntityNo = B.ComplexEntityNo and A.pk_diasvida = B.pk_diasvida
CROSS JOIN {database_name}.lk_diasvida C
LEFT JOIN temp2 D on A.ComplexEntityNo = D.ComplexEntityNo  
LEFT JOIN si_mvBrimStandardsData E on D.ProteinStandardVersionsIRN = E.ProteinStandardVersionsIRN and C.pk_diasvida = E.Age
LEFT JOIN {database_name}.lk_producto F on A.pk_producto = F.pk_producto
LEFT JOIN {database_name}.lk_sexo G on A.pk_sexo = G.pk_sexo
WHERE A.pk_empresa = 1 AND pk_division = 3 AND B.ComplexEntityNo is not null AND c.pk_diasvida > a.pk_diasvida AND c.pk_diasvida <= 50 AND A.FechaCierre = '18991130'
ORDER BY C.pk_diasvida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsolidadoDiarioProyectado"
}
df_ConsolidadoDiarioProyectado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsolidadoDiarioProyectado")
print('carga ConsolidadoDiarioProyectado')
#Se muestra UPDConsolidadoDiarioProyectado
df_UPDConsolidadoDiarioProyectado = spark.sql(f"""
WITH temp1 as (select ProteinStandardVersionsIRN,Age,SUM(U_MortPercent) U_MortPercent  from {database_name}.si_BrimStandardsData group by ProteinStandardVersionsIRN,Age)
,si_mvBrimStandardsData as (select BSD.*, temp1.U_MortPercent U_MortAcumPercent from {database_name}.si_BrimStandardsData BSD left join temp1 on temp1.ProteinStandardVersionsIRN = BSD.ProteinStandardVersionsIRN AND temp1.Age <= BSD.Age order by BSD.ProteinStandardVersionsIRN, BSD.Age)
,STD42DIAS as (
SELECT ProteinStandardVersionsIRN,Age,U_MortPercent,U_MortAcumPercent,U_MortPorcSem,U_FeedConsumed,U_ConsAlimAcum,U_PesoVivo,U_WeightGainDay,U_FeedConversionBC
FROM si_mvBrimStandardsData WHERE Age = 42)
select 
 A.fecha
,A.FechaProyectada
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
,A.pk_grupoconsumo
,A.pk_especie
,A.pk_estado
,A.pk_administrador
,A.pk_proveedor
,A.pk_diasvida
,A.pk_semanavida
,A.ComplexEntityNo
,A.Nacimiento
,A.Edad
,A.FechaCierre
,A.FechaAlojamiento
,A.PobInicial
,A.AvesRendidasAcum
,A.KilosRendidosAcum
,A.MortAcum
,A.PorcMortSem
,B.U_MortPercent     STDPorcMortDia 
,B.U_MortAcumPercent STDPorcMortAcum
,B.U_MortPorcSem     STDPorcMortSem 
,A.AreaGalpon
,A.ConsAcum
,A.PorcConsAcum
,B.U_FeedConsumed STDConsDia
,B.U_ConsAlimAcum STDConsAcum
,B.U_PesoVivo STDPeso
,A.PesoAlo
,B.U_WeightGainDay STDGanancia
,B.U_FeedConversionBC STDICA
,A.categoria
,A.PadreMayor
,A.EdadPadreCorralDescrip
,A.ProteinStandardVersionsIRN
,A.GrupoProducto
,A.Sexo
from {database_name}.ConsolidadoDiarioProyectado A
LEFT JOIN STD42DIAS B ON A.ProteinStandardVersionsIRN = B.ProteinStandardVersionsIRN 
WHERE A.pk_diasvida >42
union 
select *
from {database_name}.ConsolidadoDiarioProyectado
WHERE pk_diasvida <=42
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsolidadoDiarioProyectado_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDConsolidadoDiarioProyectado.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ConsolidadoDiarioProyectado_new")

df_ConsolidadoDiarioProyectado_nueva = spark.sql(f"""SELECT * from {database_name}.ConsolidadoDiarioProyectado_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ConsolidadoDiarioProyectado")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsolidadoDiarioProyectado"
}
df_ConsolidadoDiarioProyectado_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsolidadoDiarioProyectado")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ConsolidadoDiarioProyectado_new")

print('carga UPD ConsolidadoDiarioProyectado')
#Se muestra ft_Tercios_Lote_Plantel
df_UPDConsolidadoDiarioProyectado2 = spark.sql(f"""
with temp as (SELECT ComplexEntityno, ProteinStandardVersionsIRN FROM {database_name}.ProduccionDetalle GROUP BY ComplexEntityno,ProteinStandardVersionsIRN)
SELECT  A.fecha
,A.fecha FechaProyectada
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
,A.pk_grupoconsumo
,A.pk_especie
,A.pk_estado
,A.pk_administrador
,A.pk_proveedor
,A.pk_diasvida
,A.pk_semanavida
,A.ComplexEntityNo
,A.Nacimiento
,A.pk_diasvida Edad
,A.FechaCierre
,A.FechaAlojamiento
,A.PobInicial
,A.AvesRendidasAcum
,A.KilosRendidosAcum
,A.MortAcum
,A.PorcMortSem
,A.STDPorcMortDia
,A.STDPorcMortAcum
,A.STDPorcMortSem
,A.AreaGalpon
,A.ConsAcum
,A.PorcConsAcum
,A.STDConsDia
,A.STDConsAcum
,A.STDPeso
,A.PesoAlo
,A.STDGanancia
,A.STDICA
,A.categoria
,A.PadreMayor
,A.EdadPadreCorralDescrip
,D.ProteinStandardVersionsIRN
,B.grupoproducto GrupoProducto
,C.nsexo Sexo
FROM {database_name}.ft_consolidado_diario A
LEFT JOIN {database_name}.lk_producto B ON A.pk_producto = B.pk_producto
LEFT JOIN {database_name}.lk_Sexo C ON A.pk_sexo = C.pk_sexo
LEFT JOIN temp D on A.ComplexEntityNo = D.ComplexEntityNo  
WHERE A.pk_empresa = 1 AND pk_division = 3 AND A.FechaCierre = '18991130'
union
select *
FROM {database_name}.ConsolidadoDiarioProyectado 
WHERE pk_empresa = 1 AND pk_division <> 3 AND FechaCierre <> '18991130'
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsolidadoDiarioProyectado_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDConsolidadoDiarioProyectado2.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ConsolidadoDiarioProyectado_new")

df_ConsolidadoDiarioProyectado_nueva = spark.sql(f"""SELECT * from {database_name}.ConsolidadoDiarioProyectado_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ConsolidadoDiarioProyectado")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsolidadoDiarioProyectado"
}
df_ConsolidadoDiarioProyectado_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsolidadoDiarioProyectado")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ConsolidadoDiarioProyectado_new")

print('carga UPD ConsolidadoDiarioProyectado 2')
#Se muestra DescripTipoAlimentoXTipoProductoProyectado
df_DescripTipoAlimentoXTipoProductoProyectado = spark.sql(f"""
select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, B.grupoproducto,
		CASE WHEN A.pk_diasvida <= 8 THEN 'PREINICIO'
		WHEN (A.pk_diasvida >= 9 AND A.pk_diasvida <= 18) THEN 'INICIO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 32) AND B.grupoproducto = 'VIVO' THEN 'ACABADO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 28) AND B.grupoproducto = 'BENEFICIO' THEN 'ACABADO'
		WHEN A.pk_diasvida >= 33 AND B.grupoproducto = 'VIVO' THEN 'FINALIZADOR'
		WHEN A.pk_diasvida >= 29 AND B.grupoproducto = 'BENEFICIO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto
from {database_name}.ConsolidadoDiarioProyectado A
left join {database_name}.lk_producto B on A.pk_producto = B.pk_producto
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard
where A.pk_empresa = 1 and A.pk_division = 3
and C.nstandard not in ('DINUT - VERANO', 'DINUT - INVIERNO')
union
select distinct A.ComplexEntityNo, A.pk_diasvida, A.pk_producto, B.grupoproducto,
		CASE WHEN A.pk_diasvida <= 8 THEN 'PREINICIO'
		WHEN (A.pk_diasvida >= 9 AND A.pk_diasvida <= 18) THEN 'INICIO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 34) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - VERANO' THEN 'ACABADO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 32) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'ACABADO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 30) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - VERANO' THEN 'ACABADO'
		WHEN (A.pk_diasvida >= 19 AND A.pk_diasvida <= 29) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'ACABADO'
		WHEN (A.pk_diasvida >= 35 AND A.pk_diasvida <= 42) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - VERANO' THEN 'FINALIZADOR'
		WHEN (A.pk_diasvida >= 33 AND A.pk_diasvida <= 40) AND B.grupoproducto = 'VIVO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'FINALIZADOR'
		WHEN (A.pk_diasvida >= 31 AND A.pk_diasvida <= 38) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - VERANO' THEN 'FINALIZADOR'
		WHEN (A.pk_diasvida >= 30 AND A.pk_diasvida <= 37) AND B.grupoproducto = 'BENEFICIO' AND C.nstandard = 'DINUT - INVIERNO' THEN 'FINALIZADOR' END DescripTipoAlimentoXTipoProducto
from {database_name}.ConsolidadoDiarioProyectado A
left join {database_name}.lk_producto B on A.pk_producto = B.pk_producto
left join {database_name}.lk_standard C on A.pk_standard = C.pk_standard
where A.pk_empresa = 1 and A.pk_division = 3
and C.nstandard in ('DINUT - VERANO', 'DINUT - INVIERNO') and C.SpeciesType = '1'
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DescripTipoAlimentoXTipoProductoProyectado"
}
df_DescripTipoAlimentoXTipoProductoProyectado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DescripTipoAlimentoXTipoProductoProyectado")
print('carga DescripTipoAlimentoXTipoProductoProyectado')
#Se muestra STDConsDiaNuevoProyectado
df_STDConsDiaNuevoProyectado = spark.sql(f"""
with temp as (
select A.ComplexEntityNo, A.pk_diasvida, MAX(A.STDConsDia) STDConsDia
from {database_name}.ConsolidadoDiarioProyectado A
where A.ComplexEntityNo in (select ComplexEntityNo from {database_name}.ft_alojamiento where participacionhm <> 0)
and pk_empresa = 1 and pk_division = 3 
group by A.ComplexEntityNo, A.pk_diasvida)
select A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto, SUM(A.STDConsDia) STDConsDia,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3)) ComplexEntityNoGalpon
from temp A
left join {database_name}.DescripTipoAlimentoXTipoProductoProyectado B on A.ComplexEntityNo = B.ComplexEntityNo and A.pk_diasvida = B.pk_diasvida
group by A.ComplexEntityNo, B.DescripTipoAlimentoXTipoProducto,substring(A.complexentityno,1,(LENGTH(A.complexentityno)-3))
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDConsDiaNuevoProyectado"
}
df_STDConsDiaNuevoProyectado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDConsDiaNuevoProyectado")
print('carga STDConsDiaNuevoProyectado')
#Se muestra ft_consolidado_Diario_Proyectado
df_ft_consolidado_Diario_Proyectado = spark.sql(f"""
with 
 c1 as (select ComplexEntityNo,STDConsDia from {database_name}.STDConsDiaNuevoProyectado STD where STD.DescripTipoAlimentoXTipoProducto = 'PREINICIO') 
,c2 as (select ComplexEntityNo,STDConsDia from {database_name}.STDConsDiaNuevoProyectado STD where STD.DescripTipoAlimentoXTipoProducto = 'INICIO') 
,c3 as (select ComplexEntityNo,STDConsDia from {database_name}.STDConsDiaNuevoProyectado STD where STD.DescripTipoAlimentoXTipoProducto = 'ACABADO') 
,c4 as (select ComplexEntityNo,STDConsDia from {database_name}.STDConsDiaNuevoProyectado STD where STD.DescripTipoAlimentoXTipoProducto = 'FINALIZADOR') 
select
	 PD.fecha
	,FechaProyectada
	,PD.pk_empresa
	,PD.pk_division
	,PD.pk_zona
	,PD.pk_subzona
	,PD.pk_plantel
	,PD.pk_lote
	,PD.pk_galpon
	,PD.pk_sexo
	,PD.pk_standard
	,PD.pk_producto
	,PD.pk_grupoconsumo
	,PD.pk_especie
	,PD.pk_estado
	,PD.pk_administrador
	,PD.pk_proveedor
	,PD.pk_diasvida
	,PD.pk_semanavida
	,PD.ComplexEntityNo
	,PD.Nacimiento
	,PD.pk_diasvida Edad
	,PD.FechaCierre
	,PD.FechaAlojamiento
	,PD.PobInicial
	,PD.AvesRendidasAcum
	,PD.KilosRendidosAcum
	,PD.MortAcum
	,PD.PorcMortSem
	,STDPorcMortDia
	,STDPorcMortAcum
	,STDPorcMortSem
	,PD.AreaGalpon
	,PD.ConsAcum
	,PD.PorcConsAcum
	,PD.STDConsDia
	,PD.STDConsAcum
	,PD.STDPeso
	,PD.PesoAlo
	,PD.STDGanancia
	,PD.STDICA
	,PD.categoria
	,PD.PadreMayor
	,PD.EdadPadreCorralDescrip
	,PD.ProteinStandardVersionsIRN
	,PD.grupoproducto GrupoProducto
	,PD.Sexo
	,c1.STDConsDia STDConsAcumPreinicio
	,c2.STDConsDia STDConsAcumInicio
	,c3.STDConsDia STDConsAcumAcabado
	,c4.STDConsDia STDConsAcumFinalizador
from {database_name}.ConsolidadoDiarioProyectado PD 
LEFT JOIN {database_name}.DescripTipoAlimentoXTipoProductoProyectado DTA on PD.ComplexEntityNo = DTA.ComplexEntityNo and PD.pk_diasvida = DTA.pk_diasvida
LEFT JOIN {database_name}.STDConsDiaNuevoProyectado STDCN on substring(DTA.complexentityno,1,(LENGTH(DTA.complexentityno)-3)) = STDCN.ComplexEntityNoGalpon and DTA.DescripTipoAlimentoXTipoProducto = STDCN.DescripTipoAlimentoXTipoProducto
LEFT JOIN c1 on c1.ComplexEntityNo = STDCN.ComplexEntityNo
LEFT JOIN c2 on c2.ComplexEntityNo = STDCN.ComplexEntityNo
LEFT JOIN c3 on c3.ComplexEntityNo = STDCN.ComplexEntityNo
LEFT JOIN c4 on c4.ComplexEntityNo = STDCN.ComplexEntityNo
order by FechaProyectada
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_Proyectado"
}
df_ft_consolidado_Diario_Proyectado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_Proyectado")
print('carga ft_consolidado_Diario_Proyectado')
#Se muestra UPD ft_consolidado_Diario
df_UPDft_consolidado_Diario = spark.sql(f"""
with PrincipalesLesionesSemanalesXLote as (
SELECT	 pk_empresa,pk_plantel,pk_lote,pk_semanavida,pk_especie,ComplexEntityNo
		,CantMortSemLesionUno,PorcMortSemLesionUno,DescripMortSemLesionUno
		,CantMortSemLesionDos,PorcMortSemLesionDos,DescripMortSemLesionDos
		,CantMortSemLesionTres,PorcMortSemLesionTres,DescripMortSemLesionTres
		,CantMortSemLesionOtros,PorcMortSemLesionOtros,DescripMortSemLesionOtros
		,CantMortSemLesionAcumUno,PorcMortSemLesionAcumUno,DescripMortSemLesionAcumUno
		,CantMortSemLesionAcumDos,PorcMortSemLesionAcumDos,DescripMortSemLesionAcumDos
		,CantMortSemLesionAcumTres,PorcMortSemLesionAcumTres,DescripMortSemLesionAcumTres
		,CantMortSemLesionAcumOtros,PorcMortSemLesionAcumOtros,DescripMortSemLesionAcumOtros
FROM {database_name}.ft_mortalidad_Lote_semanal)
select A.*
,B.CantMortSemLesionUno CantMortSemLesionUno           
,B.PorcMortSemLesionUno PorcMortSemLesionUno           
,B.DescripMortSemLesionUno DescripMortSemLesionUno     
,B.CantMortSemLesionDos CantMortSemLesionDos           
,B.PorcMortSemLesionDos PorcMortSemLesionDos           
,B.DescripMortSemLesionDos DescripMortSemLesionDos     
,B.CantMortSemLesionTres CantMortSemLesionTres         
,B.PorcMortSemLesionTres PorcMortSemLesionTres         
,B.DescripMortSemLesionTres DescripMortSemLesionTres   
,B.CantMortSemLesionOtros CantMortSemLesionOtros       
,B.PorcMortSemLesionOtros PorcMortSemLesionOtros       
,B.DescripMortSemLesionOtros DescripMortSemLesionOtros 
,B.CantMortSemLesionAcumUno CantMortSemLesionAcumUno   
,B.PorcMortSemLesionAcumUno PorcMortSemLesionAcumUno   
,B.DescripMortSemLesionAcumUno DescripMortSemLesionAcumUno 
,B.CantMortSemLesionAcumDos CantMortSemLesionAcumDos       
,B.PorcMortSemLesionAcumDos PorcMortSemLesionAcumDos       
,B.DescripMortSemLesionAcumDos DescripMortSemLesionAcumDos 
,B.CantMortSemLesionAcumTres CantMortSemLesionAcumTres     
,B.PorcMortSemLesionAcumTres PorcMortSemLesionAcumTres     
,B.DescripMortSemLesionAcumTres DescripMortSemLesionAcumTres 
,B.CantMortSemLesionAcumOtros CantMortSemLesionAcumOtros     
,B.PorcMortSemLesionAcumOtros PorcMortSemLesionAcumOtros     
,B.DescripMortSemLesionAcumOtros DescripMortSemLesionAcumOtros
from {database_name}.ft_consolidado_Diario A
left join PrincipalesLesionesSemanalesXLote B on substring(A.ComplexEntityNo,1,(LENGTH(A.ComplexEntityNo)-6)) = B.ComplexEntityNo and A.pk_plantel = B.pk_plantel and A.pk_lote = B.pk_lote and A.pk_semanavida = B.pk_semanavida and A.pk_especie = B.pk_especie
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_consolidado_Diario.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario_new")

df_ft_consolidado_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_consolidado_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consolidado_Diario"
}
df_ft_consolidado_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_consolidado_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_consolidado_Diario_new")

print('carga UPD ft_consolidado_Diario')
#Se muestra UPD ft_mortalidad_Diario
df_UPDft_mortalidad_Diario = spark.sql(f"""
select  A.pk_tiempo           
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
,A.pk_especie          
,A.pk_estado           
,A.pk_administrador    
,A.pk_proveedor        
,A.pk_semanavida       
,A.pk_diasvida         
,A.complexentityno     
,A.fechanacimiento     
,A.incubadoramayor     
,A.listaincubadora     
,A.listapadre          
,A.pobinicial          
,A.mortdia             
,A.mortdiaacum         
,A.mortsem             
,A.mortsemacum         
,A.stdmortdia          
,A.stdmortdiaacum      
,A.stdmortsem          
,A.stdmortsemacum      
,A.porcmortdia         
,A.porcmortdiaacum     
,A.porcmortsem         
,A.porcmortsemacum     
,A.stdporcmortdia      
,A.stdporcmortdiaacum  
,A.stdporcmortsem      
,A.stdporcmortsemacum  
,A.difporcmortdia_stddia
,A.difporcmortdiaacum_stddiaacum
,A.difporcmortsem_stdsem
,A.difporcmortsemacum_stdsemacum
,A.mortsem1            
,A.mortsem2            
,A.mortsem3            
,A.mortsem4            
,A.mortsem5            
,A.mortsem6            
,A.mortsem7            
,A.mortsemacum1        
,A.mortsemacum2        
,A.mortsemacum3        
,A.mortsemacum4        
,A.mortsemacum5        
,A.mortsemacum6        
,A.mortsemacum7        
,A.tasanoviable        
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
,A.porcacumaccidentados
,A.porcacumhigadograso 
,A.porcacumhepatomegalia
,A.porcacumhigadohemorragico
,A.porcacuminanicion   
,A.porcacumproblemarespiratorio
,A.porcacumsch         
,A.porcacumenteritis   
,A.porcacumascitis     
,A.porcacummuertesubita
,A.porcacumestresporcalor
,A.porcacumhidropericardio
,A.porcacumhemopericardio
,A.porcacumuratosis    
,A.porcacummaterialcaseoso
,A.porcacumonfalitis   
,A.porcacumretenciondeyema
,A.porcacumerosiondemolleja
,A.porcacumhemorragiamusculos
,A.porcacumsangreenciego
,A.porcacumpericarditis
,A.porcacumperitonitis 
,A.porcacumprolapso    
,A.porcacumpicaje      
,A.porcacumrupturaaortica
,A.porcacumbazomoteado 
,A.porcacumnoviable    
,A.prilesion           
,A.porcprilesion       
,A.prilesionnom        
,A.seglesion           
,A.porcseglesion       
,A.seglesionnom        
,A.terlesion           
,A.porcterlesion       
,A.terlesionnom        
,A.porcprilesionacum   
,A.prilesionacumnom    
,A.porcseglesionacum   
,A.seglesionacumnom    
,A.porcterlesionacum   
,A.terlesionacumnom    
,A.toplesion           
,A.flagartificio       
,A.categoria           
,A.flagatipico         
,A.u_peaerosaculitisg2 
,A.u_pecojera          
,A.u_pehigadoicterico  
,A.u_pematerialcaseoso_po1ra
,A.u_pematerialcaseosomedretr
,A.u_penecrosishepatica
,A.u_peneumonia        
,A.u_pesepticemia      
,A.u_pevomitonegro     
,A.porcaerosaculitisg2 
,A.porccojera          
,A.porchigadoicterico  
,A.porcmaterialcaseoso_po1ra
,A.porcmaterialcaseosomedretr
,A.porcnecrosishepatica
,A.porcneumonia        
,A.porcsepticemia      
,A.porcvomitonegro     
,A.porcacumaerosaculitisg2
,A.porcacumcojera      
,A.porcacumhigadoicterico
,A.porcacummaterialcaseoso_po1ra
,A.porcacummaterialcaseosomedretr
,A.porcacumnecrosishepatica
,A.porcacumneumonia    
,A.porcacumsepticemia  
,A.porcacumvomitonegro 
,A.u_peasperguillius   
,A.u_pebazograndemot   
,A.u_pecorazongrande   
,A.porcasperguillius   
,A.porcbazograndemot   
,A.porccorazongrande   
,A.porcacumasperguillius
,A.porcacumbazograndemot
,A.porcacumcorazongrande
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
,A.u_pecuadrotoxico    
,A.porccuadrotoxico    
,A.porcacumcuadrotoxico
,A.pavosbbmortincub    
,A.flagtransfpavos     
,A.sourcecomplexentityno
,A.destinationcomplexentityno
,A.semaccidentados     
,A.semascitis          
,A.sembazomoteado      
,A.sementeritis        
,A.semerosiondemolleja 
,A.semestresporcalor   
,A.semhemopericardio   
,A.semhemorragiamusculos
,A.semhepatomegalia    
,A.semhidropericardio  
,A.semhigadograso      
,A.semhigadohemorragico
,A.seminanicion        
,A.semmaterialcaseoso  
,A.semmuertesubita     
,A.semnoviable         
,A.semonfalitis        
,A.sempericarditis     
,A.semperitonitis      
,A.sempicaje           
,A.semproblemarespiratorio
,A.semprolapso         
,A.semretenciondeyema  
,A.semrupturaaortica   
,A.semsangreenciego    
,A.semsch              
,A.semuratosis         
,A.semaerosaculitisg2  
,A.semcojera           
,A.semhigadoicterico   
,A.semmaterialcaseoso_po1ra
,A.semmaterialcaseosomedretr
,A.semnecrosishepatica 
,A.semneumonia         
,A.semsepticemia       
,A.semvomitonegro      
,A.semasperguillius    
,A.sembazograndemot    
,A.semcorazongrande    
,A.semcuadrotoxico     
,A.semporcaccidentados 
,A.semporcascitis      
,A.semporcbazomoteado  
,A.semporcenteritis    
,A.semporcerosiondemolleja
,A.semporcestresporcalor
,A.semporchemopericardio
,A.semporchemorragiamusculos
,A.semporchepatomegalia
,A.semporchidropericardio
,A.semporchigadograso  
,A.semporchigadohemorragico
,A.semporcinanicion    
,A.semporcmaterialcaseoso
,A.semporcmuertesubita 
,A.semporcnoviable     
,A.semporconfalitis    
,A.semporcpericarditis 
,A.semporcperitonitis  
,A.semporcpicaje       
,A.semporcproblemarespiratorio
,A.semporcprolapso     
,A.semporcretenciondeyema
,A.semporcrupturaaortica
,A.semporcsangreenciego
,A.semporcsch          
,A.semporcuratosis     
,A.semporcaerosaculitisg2
,A.semporccojera       
,A.semporchigadoicterico
,A.semporcmaterialcaseoso_po1ra
,A.semporcmaterialcaseosomedretr
,A.semporcnecrosishepatica
,A.semporcneumonia     
,A.semporcsepticemia   
,A.semporcvomitonegro  
,A.semporcasperguillius
,A.semporcbazograndemot
,A.semporccorazongrande
,A.semporccuadrotoxico 
,A.semprilesion        
,A.semporcprilesion    
,A.semprilesionnom     
,A.semseglesion        
,A.semporcseglesion    
,A.semseglesionnom     
,A.semterlesion        
,A.semporcterlesion    
,A.semterlesionnom     
,A.semotroslesion      
,A.semporcotroslesion  
,A.semotroslesionnom   
,A.padremayor          
,A.ruidosrespiratorios 
,A.porcincmayor        
,A.porcalojpadremayor  
,A.razamayor           
,A.porcrazamayor       
,A.edadpadrecorral     
,A.edadpadrecorraldescrip
,A.listaformulano      
,A.listaformulaname    
,A.tipoorigen          
,A.mortconcatcorral    
,A.mortsemantcorral    
,A.mortconcatlote      
,A.mortsemantlote      
,A.mortconcatsemaniolote
,A.mortconcatsemaniocorral
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
,B.SaldoAves
from {database_name}.ft_mortalidad_Diario A
left join {database_name}.ft_consolidado_diario B on A.ComplexEntityNo = B.ComplexEntityNo and A.fecha = B.fecha
""")

additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_Diario_new"
}
# 1️⃣ Crear DataFrame intermedio
df_UPDft_mortalidad_Diario.write \
    .mode("overwrite") \
    .format("parquet") \
    .options(**additional_options) \
    .saveAsTable(f"{database_name}.ft_mortalidad_Diario_new")

df_ft_mortalidad_Diario_nueva = spark.sql(f"""SELECT * from {database_name}.ft_mortalidad_Diario_new """)

# 2️⃣ Eliminar la tabla original (opcional, si se permite)
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_mortalidad_Diario")

# 4️⃣ Volver a crear la tabla original con los datos actualizados
additional_options2 = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_mortalidad_Diario"
}
df_ft_mortalidad_Diario_nueva.write \
    .format("parquet") \
    .options(**additional_options2) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ft_mortalidad_Diario")  

# 5️⃣ (Opcional) Eliminar la tabla temporal si ya no es neces
spark.sql(f"DROP TABLE IF EXISTS {database_name}.ft_mortalidad_Diario_new")

print('carga UPD ft_mortalidad_Diario')
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()