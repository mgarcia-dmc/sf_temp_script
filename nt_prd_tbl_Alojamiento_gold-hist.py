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
JOB_NAME = "nt_prd_tbl_Alojamiento_gold"

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

file_name_target18 = f"{bucket_name_prdmtech}ft_Alojamiento/"
path_target18 = f"s3://{bucket_name_target}/{file_name_target18}"
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
#Alojamiento
#1 Inserta los datos de alojamiento en la tabla temporal #Alojamiento (Pollo)
df_AlojamientoTemp1 = spark.sql(f"SELECT \
 A.ProteinProductsAnimalsIRN \
,A.ProteinEntitiesIRN \
,A.ProteinVehiclesIRN \
,A.ProteinDriversIRN \
,A.ProteinFarmsIRN_FF \
,C.ProteinBreedCodesIRN AS ProteinBreedCodesIRNP \
,F.ProteinBreedCodesIRN AS ProteinBreedCodesIRNH \
,CASE WHEN C.ProteinBreedCodesIRN IS NULL THEN F.ProteinBreedCodesIRN ELSE C.ProteinBreedCodesIRN END ProteinBreedCodesIRN \
,ProteinCostCentersIRN_HatchHatchery \
,G.IRN \
,A.ComplexEntityNo \
,substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNoGalpon \
,substring(A.complexentityno,1,(length(A.complexentityno)-6)) ComplexEntityNoLote \
,D.ComplexEntityNo AS ComplexEntityNoPadre \
,concat(RTRIM(D.FarmNo) ,'-', RTRIM(D.EntityNo)) AS LotePadre \
,RTRIM(D.FarmNo) AS FarmNoPadre \
,RTRIM(D.EntityNo) AS EntityNoPadre \
,RTRIM(D.HouseNo) AS HouseNoPadre \
,E.FirstHatchDate AS FechaNacimiento \
,A.TransDate AS FechaTransaccion \
,A.EventDate AS FechaRecepcion \
,A.TransDate AS FechaAlojamiento \
,(SELECT MIN(cast(TransDate as timestamp)) FROM {database_name}.si_mvhimchicktranshouses WHERE proteinentitiesirn = A.proteinentitiesirn) AS FechaIniAlojamiento \
,(SELECT MAX(cast(TransDate as timestamp)) FROM {database_name}.si_mvhimchicktranshouses WHERE proteinentitiesirn = A.proteinentitiesirn) AS FechaFinAlojamiento \
,E.LastDateSold AS FechaCierre \
,A.FarmNo \
,A.EntityNo \
,A.HouseNo \
,A.PenNo \
,A.Sex AS Sexo \
,A.RefNo AS Guia \
,A.VoidFlag AS Anular \
,RTRIM(HatchHatcheryName) AS PlantaIncubacion \
,B.U_PesoAlojamiento AS PesoAlojamiento \
,placementmortality AS MortalidadAlojamiento \
,CASE WHEN C.HeadPlaced IS NULL THEN B.HeadPlaced ELSE C.HeadPlaced END CantAlojamientoDet \
,B.headplaced AS CantAlojamientoXGuia \
,RelativeHeadPlaced AS PesoBebe \
,B.U_PesoAlojamiento*(CASE WHEN C.HeadPlaced IS NULL THEN B.HeadPlaced ELSE C.HeadPlaced END) AS PesoAlojXCantAlojDet \
,G.ProdDate \
,H.DateCap \
,(CASE WHEN isnull(H.DateCap) = true  THEN 0 WHEN cast(cast(H.DateCap as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(H.DateCap as timestamp) as date)) END) + 162 AS DiasPadreFechaCap \
,I.FirstHatchDate FechaNacimientoReprod \
,(CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END) AS DiasPadreFechaNacReprod \
,(round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
       (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) as EdadPadre \
,CASE WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                 (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 24.0 and \
           (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                 (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 35.0 THEN 'Joven' \
      WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                 (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 35.1 and \
           (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                 (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 50.0 THEN 'Adulto' \
      WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                 (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 50.1 THEN 'Viejo' \
 END EdadPadreDescrip \
 ,CASE WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                  (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 24.0 and \
            (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                  (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 35.0 THEN 'Joven' \
       WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
                  (((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 35.1 THEN 'Adulto' \
 END EdadPadreDescrip2 \
,0 FlagTransPavos \
,A.ComplexEntityNo SourceComplexEntityNo \
,J.GrowerType \
,A.VendorSKUNo \
,CASE WHEN J.GrowerType = 5 THEN 'HUEVO INCUBABLE COMPRADO' \
      WHEN A.VendorSKUNo IS NOT NULL THEN 'POLLITO BEBE COMPRADO' \
      WHEN J.GrowerType <> 5 AND A.VendorSKUNo IS NULL THEN 'PROPIO' \
 END Origen \
,CASE WHEN J.GrowerType = 5 THEN 'COMPRADO' \
      WHEN A.VendorSKUNo IS NOT NULL THEN 'COMPRADO' \
      WHEN J.GrowerType <> 5 AND A.VendorSKUNo IS NULL THEN 'PROPIO' \
 END TipoOrigen \
FROM {database_name}.si_mvhimchicktranshouses A \
LEFT JOIN {database_name}.si_himchicktranshouses B ON A.proteinentitiesirn = B.proteinentitiesirn and A.himchicktranshousesIRN = B.irn \
LEFT JOIN {database_name}.si_mvHimchicktransparentdetail C ON A.ProteinEntitiesIRN = C.ProteinEntitiesIRN and C.HimChickTransHousesIRN = A.HimChickTransHousesIRN \
LEFT JOIN {database_name}.si_mvproteinentities D ON C.ProteinEntitiesIRN_Parent = D.IRN \
LEFT JOIN {database_name}.si_brimentities E ON CAST(E.ProteinEntitiesIRN  AS VARCHAR(50))= CAST(A.ProteinEntitiesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.si_mvproteinentities F ON A.ProteinEntitiesIRN = F.IRN \
LEFT JOIN {database_name}.si_himchicktransparentdetail G ON C.IRN = G.IRN \
LEFT JOIN {database_name}.si_bimcapitalizationtrans H ON C.ProteinEntitiesIRN_Parent = H.ProteinEntitiesIRN \
LEFT JOIN {database_name}.si_mvbimentities I ON D.IRN = I.IRN \
LEFT JOIN {database_name}.si_mvbimfarms J ON J.FarmNo = D.FarmNo \
WHERE A.TransCode <> 3 and a.FarmType = 1 and a.SpeciesType = 1")
print('carga df_AlojamientoTemp1')
#1 Inserta los datos de alojamiento en la tabla temporal #Alojamiento (Pavo)
df_mvHimChickTransHouses = spark.sql(f"select * \
                                      FROM {database_name}.si_mvhimchicktranshouses A \
                                      WHERE A.TransCode <> 3 and a.FarmType = 7 and a.SpeciesType = 2")
#df_mvHimChickTransHouses.createOrReplaceTempView("mvHimChickTransHousesTemp")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mvHimChickTransHousesTemp"
}
df_mvHimChickTransHouses.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mvHimChickTransHousesTemp")
print('carga mvHimChickTransHousesTemp')

#INSERT INTO #Alojamiento --drop table #Alojamiento --66115	
df_AlojamientoTemp2 = spark.sql(f"SELECT A.ProteinProductsAnimalsIRN \
,A.ProteinEntitiesIRN \
,A.ProteinVehiclesIRN \
,A.ProteinDriversIRN \
,A.ProteinFarmsIRN_FF \
,C.ProteinBreedCodesIRN AS ProteinBreedCodesIRNP \
,F.ProteinBreedCodesIRN AS ProteinBreedCodesIRNH \
,CASE WHEN C.ProteinBreedCodesIRN IS NULL THEN F.ProteinBreedCodesIRN ELSE C.ProteinBreedCodesIRN END ProteinBreedCodesIRN \
,ProteinCostCentersIRN_HatchHatchery \
,G.IRN \
,A.ComplexEntityNo \
,substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNoGalpon \
,substring(A.complexentityno,1,(length(A.complexentityno)-6)) ComplexEntityNoLote \
,D.ComplexEntityNo AS ComplexEntityNoPadre \
,RTRIM(D.FarmNo) +'-'+ RTRIM(D.EntityNo) AS LotePadre \
,RTRIM(D.FarmNo) AS FarmNoPadre \
,RTRIM(D.EntityNo) AS EntityNoPadre \
,RTRIM(D.HouseNo) AS HouseNoPadre \
,E.FirstHatchDate AS FechaNacimiento \
,A.TransDate AS FechaTransaccion \
,A.EventDate AS FechaRecepcion \
,A.TransDate AS FechaAlojamiento \
,(SELECT MIN(cast(TransDate as timestamp)) FROM {database_name}.si_mvhimchicktranshouses WHERE proteinentitiesirn = A.proteinentitiesirn) AS FechaIniAlojamiento \
,(SELECT MAX(cast(TransDate as timestamp)) FROM {database_name}.si_mvhimchicktranshouses WHERE proteinentitiesirn = A.proteinentitiesirn) AS FechaFinAlojamiento \
,E.LastDateSold AS FechaCierre \
,A.FarmNo \
,A.EntityNo \
,A.HouseNo \
,A.PenNo \
,A.Sex AS Sexo \
,A.RefNo AS Guia \
,A.VoidFlag AS Anular \
,RTRIM(HatchHatcheryName) AS PlantaIncubacion \
,B.U_PesoAlojamiento AS PesoAlojamiento \
,placementmortality AS MortalidadAlojamiento \
,C.HeadPlaced AS CantAlojamientoDet \
,B.headplaced AS CantAlojamientoXGuia \
,RelativeHeadPlaced AS PesoBebe \
,B.U_PesoAlojamiento*C.HeadPlaced AS PesoAlojXCantAlojDet \
,G.ProdDate \
,H.DateCap \
,(CASE WHEN H.DateCap IS NULL THEN 0 WHEN cast(cast(H.DateCap as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(H.DateCap as timestamp) as date)) END) + 162 AS DiasPadreFechaCap \
,I.FirstHatchDate FechaNacimientoReprod \
,(CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END) AS DiasPadreFechaNacReprod \
,(round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) as EdadPadre \
,CASE WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 24.0 and \
(round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 35.0 THEN 'Joven' \
WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 35.1 and \
(round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 50.0 THEN 'Adulto' \
WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 50.1 THEN 'Viejo' \
END EdadPadreDescrip \
,CASE WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 24.0 and \
(round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) <= 35.0 THEN 'Joven' \
WHEN (round(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)/7,0) + \
(((CASE WHEN I.FirstHatchDate IS NULL THEN 0 WHEN cast(cast(I.FirstHatchDate as timestamp) as date) = cast('1899-11-30' as date) THEN 0 ELSE DateDiff(cast(cast(ProdDate as timestamp) as date), cast(cast(I.FirstHatchDate as timestamp) as date)) END)*1.0)%7)/10) >= 35.1 THEN 'Adulto' \
END EdadPadreDescrip2 \
,0 FlagTransPavos \
,A.ComplexEntityNo SourceComplexEntityNo \
,J.GrowerType \
,A.VendorSKUNo \
,CASE WHEN J.GrowerType = 5 THEN 'HUEVO INCUBABLE COMPRADO' \
WHEN A.VendorSKUNo IS NOT NULL THEN 'POLLITO BEBE COMPRADO' \
WHEN J.GrowerType <> 5 AND A.VendorSKUNo IS NULL THEN 'PROPIO' \
END Origen \
,CASE WHEN J.GrowerType = 5 THEN 'COMPRADO' \
WHEN A.VendorSKUNo IS NOT NULL THEN 'COMPRADO' \
WHEN J.GrowerType <> 5 AND A.VendorSKUNo IS NULL THEN 'PROPIO' \
END TipoOrigen \
FROM {database_name}.mvHimChickTransHousesTemp A \
LEFT JOIN {database_name}.si_himchicktranshouses B ON A.proteinentitiesirn = B.proteinentitiesirn and A.himchicktranshousesIRN = B.irn \
LEFT JOIN {database_name}.si_mvhimchicktransparentdetail C ON A.ProteinEntitiesIRN = C.ProteinEntitiesIRN and C.HimChickTransHousesIRN = A.HimChickTransHousesIRN \
LEFT JOIN {database_name}.si_mvproteinentities D ON C.ProteinEntitiesIRN_Parent = D.IRN \
LEFT JOIN {database_name}.si_brimentities E ON CAST(E.ProteinEntitiesIRN  AS VARCHAR(50))= CAST(A.ProteinEntitiesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.si_mvproteinentities F ON A.ProteinEntitiesIRN = F.IRN \
LEFT JOIN {database_name}.si_himchicktransparentdetail G ON C.IRN = G.IRN \
LEFT JOIN {database_name}.si_bimcapitalizationtrans H ON C.ProteinEntitiesIRN_Parent = H.ProteinEntitiesIRN \
LEFT JOIN {database_name}.si_mvbimentities I ON D.IRN = I.IRN \
LEFT JOIN {database_name}.si_mvbimfarms J ON J.FarmNo = D.FarmNo")

df_AlojamientoTemp = df_AlojamientoTemp1.union(df_AlojamientoTemp2)
#df_AlojamientoTemp.createOrReplaceTempView("AlojamientoTemp")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AlojamientoTemp"
}
df_AlojamientoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AlojamientoTemp")
print('carga AlojamientoTemp')

df_SumatoriaAlojamiento1 = spark.sql(f"SELECT ComplexEntityNo, EdadPadreDescrip, SUM(CantAlojamientoDet) CantAlojamientoXEdadPadre \
FROM {database_name}.AlojamientoTemp  group by ComplexEntityNo, EdadPadreDescrip") 
#df_SumatoriaAlojamiento1.createOrReplaceTempView("SumatoriaAlojamiento1")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento1"
}
df_SumatoriaAlojamiento1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento1")
print('carga SumatoriaAlojamiento1')

df_SumatoriaAlojamiento2 = spark.sql(f"SELECT ComplexEntityNo, EdadPadreDescrip2,SUM(CantAlojamientoDet) CantAlojamientoXEdadPadre2 \
FROM {database_name}.AlojamientoTemp group by ComplexEntityNo, EdadPadreDescrip2")
#df_SumatoriaAlojamiento2.createOrReplaceTempView("SumatoriaAlojamiento2")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento2"
}
df_SumatoriaAlojamiento2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento2")
print('carga SumatoriaAlojamiento2')

df_SumatoriaAlojamiento3 = spark.sql(f"SELECT ComplexEntityNo,SUM(CantAlojamientoDet) CantAlojamientoTotal FROM {database_name}.AlojamientoTemp group by ComplexEntityNo")
#df_SumatoriaAlojamiento3.createOrReplaceTempView("SumatoriaAlojamiento3")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento3"
}
df_SumatoriaAlojamiento3.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento3")
print('carga SumatoriaAlojamiento3')

df_SumatoriaAlojamiento4 = spark.sql(f"SELECT ComplexEntityNo,ProteinBreedCodesIRN,SUM(CantAlojamientoDet) CantAlojamientoTotal \
FROM {database_name}.AlojamientoTemp group by ComplexEntityNo,ProteinBreedCodesIRN")
#df_SumatoriaAlojamiento4.createOrReplaceTempView("SumatoriaAlojamiento4")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento4"
}
df_SumatoriaAlojamiento4.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento4")
print('carga SumatoriaAlojamiento4')

df_SumatoriaAlojamiento5 = spark.sql(f"SELECT ComplexEntityNo,FarmNoPadre,SUM(CantAlojamientoDet) CantAlojamientoTotal \
FROM {database_name}.AlojamientoTemp group by ComplexEntityNo,FarmNoPadre")
#df_SumatoriaAlojamiento5.createOrReplaceTempView("SumatoriaAlojamiento5")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento5"
}
df_SumatoriaAlojamiento5.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento5")
print('carga SumatoriaAlojamiento5')

df_SumatoriaAlojamiento6 = spark.sql(f"SELECT ComplexEntityNo,SUM(PesoAlojXCantAlojDet) PesoAlojXCantAlojDetTotal,SUM(CantAlojamientoDet) CantAlojamientoTotal \
FROM {database_name}.AlojamientoTemp \
WHERE PesoAlojXCantAlojDet > 0 group by ComplexEntityNo")
#df_SumatoriaAlojamiento6.createOrReplaceTempView("SumatoriaAlojamiento6")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento6"
}
df_SumatoriaAlojamiento6.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento6")
print('carga SumatoriaAlojamiento6')

df_SumatoriaAlojamiento7 = spark.sql(f"SELECT ComplexEntityNoGalpon,SUM(CantAlojamientoDet) CantAlojamientoTotal \
FROM {database_name}.AlojamientoTemp group by ComplexEntityNoGalpon")
#df_SumatoriaAlojamiento7.createOrReplaceTempView("SumatoriaAlojamiento7")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaAlojamiento7"
}
df_SumatoriaAlojamiento7.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SumatoriaAlojamiento7")
print('carga SumatoriaAlojamiento7')

print('cargas df_SumatoriaAlojamiento')
#3 Crea la tabla GSF_DMPecuario.STGPECUARIO.Alojamiento e inserta los datos de la tabla temporal #Alojamiento
df_Alojamiento = spark.sql(f"SELECT \
AL.ProteinProductsAnimalsIRN \
,AL.ProteinEntitiesIRN \
,AL.ProteinVehiclesIRN \
,AL.ProteinDriversIRN \
,AL.ProteinFarmsIRN_FF \
,AL.ProteinBreedCodesIRN \
,AL.ProteinCostCentersIRN_HatchHatchery \
,AL.IRN \
,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha=cast('1899-11-30' as date))) pk_tiempo \
,nvl(LT.fecha,cast('1899-11-30' as date)) fecha \
,(select distinct pk_empresa from {database_name}.lk_empresa where cempresa=1) as pk_empresa \
,nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) AS pk_division \
,nvl(LP.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) AS pk_zona \
,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) AS pk_subzona \
,nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) AS pk_plantel \
,nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) AS pk_lote \
,nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) AS pk_galpon \
,nvl(LS.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) AS pk_sexo \
,nvl(LST.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) AS pk_standard \
,nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) AS pk_producto \
,nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) AS pk_tipoproducto \
,nvl(LEP.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) AS pk_especie \
,nvl(LES.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) AS pk_estado \
,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) AS pk_administrador \
,nvl(LCO.pk_conductor,(select pk_conductor from {database_name}.lk_conductor where cconductor='0')) AS pk_conductor \
,nvl(VH.pk_vehiculo,(select pk_vehiculo from {database_name}.lk_vehiculo where cvehiculo='0')) AS pk_vehiculo \
,nvl(INC.pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) AS pk_incubadora \
,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
,AL.ComplexEntityNo \
,ComplexEntityNoPadre \
,LotePadre \
,AL.FarmNoPadre as PlantelPadre \
,Guia AS NroGuia \
,nvl(LEP.cespecie,'-') Raza \
,AL.FechaNacimiento \
,FechaTransaccion \
,FechaAlojamiento \
,FechaIniAlojamiento \
,FechaFinAlojamiento \
,FechaRecepcion \
,FechaCierre \
,'BRIM' AS TipoGranja \
,(SELECT concat_ws(',' , collect_list( DISTINCT ComplexEntityNoPadre)) ComplexEntityNoPadre FROM {database_name}.AlojamientoTemp F WHERE F.ComplexEntityNo = AL.ComplexEntityNo) ListaPadre \
,(SELECT concat_ws(',' , collect_list( DISTINCT PlantaIncubacion)) PlantaIncubacion FROM {database_name}.AlojamientoTemp F WHERE F.ComplexEntityNo = AL.ComplexEntityNo) ListaIncubadora \
,PlantaIncubacion \
,PesoBebe \
,PesoAlojamiento AS PesoAloj \
,CantAlojamientoDet AS CantAloj \
,CantAlojamientoXGuia AS TotalAloj \
,MortalidadAlojamiento AS MortAloj \
,SA3.CantAlojamientoTotal \
,nvl(SA4.CantAlojamientoTotal,0) AS CantAlojamientoXRaza \
,nvl(SA41.CantAlojamientoTotal,0) AS CantAlojamientoXROSS \
,nvl(SA42.CantAlojamientoTotal,0) AS CantAlojamientoXTodosLosROSS \
,SA5.CantAlojamientoTotal CantAlojamientoPadre \
,CASE WHEN SA3.CantAlojamientoTotal = 0 THEN 0 ELSE (SA6.PesoAlojXCantAlojDetTotal / SA6.CantAlojamientoTotal) END AS PesoAlojamientoPond \
,CASE WHEN SA3.CantAlojamientoTotal = 0 THEN 0 ELSE (nvl(SA4.CantAlojamientoTotal,0) * 1.0 / SA3.CantAlojamientoTotal) END AS PorcCodigoRaza \
,DATEDIFF(cast(cast(FechaFinAlojamiento as timestamp) as date),cast(cast(FechaIniAlojamiento as timestamp) as date)) + 1 AS DiasAlojamiento \
,CAT.categoria \
,nvl(AT.FlagAtipico,1) FlagAtipico \
,(round((DiasPadreFechaNacReprod*1.0)/7,0) + ((DiasPadreFechaNacReprod*1.0)%7)/10) as EdadPadre \
,EdadPadre AS EdadPadreTablaTemporal \
,AL.EdadPadreDescrip \
,AL.EdadPadreDescrip2 \
,SA1.CantAlojamientoXEdadPadre \
,SA2.CantAlojamientoXEdadPadre2 \
,RGD.PesoHvo \
,(RGD.PesoHvo*CantAlojamientoDet) PesoHvoXCantAloj \
,FlagTransPavos \
,SourceComplexEntityNo \
,SA7.CantAlojamientoTotal AS CantAlojamientoXGalpon \
,AL.Origen \
,(SELECT concat_ws(',' , collect_list( DISTINCT Origen)) Origen FROM {database_name}.AlojamientoTemp F WHERE F.ComplexEntityNo = AL.ComplexEntityNo) ListaOrigen \
,AL.TipoOrigen \
,(SELECT concat_ws(',' , collect_list( DISTINCT TipoOrigen)) TipoOrigen FROM {database_name}.AlojamientoTemp F WHERE F.ComplexEntityNo = AL.ComplexEntityNo) ListaTipoOrigen \
FROM {database_name}.AlojamientoTemp AL \
LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd') = date_format(AL.FechaAlojamiento,'yyyyMMdd') \
LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(AL.ProteinFarmsIRN_FF AS VARCHAR (50)) \
LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = AL.EntityNo AND LL.activeflag IN (0,1) \
LEFT JOIN {database_name}.lk_galpon LG ON LG.noplantel = LP.noplantel AND LG.nogalpon=AL.HouseNo AND LG.activeflag IN (false,true) \
LEFT JOIN {database_name}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LP.ProteinCostCentersIRN \
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_subzona LSZ ON LP.pk_subzona = LSZ.pk_subzona \
LEFT JOIN {database_name}.lk_sexo LS ON LS.csexo = AL.Sexo \
LEFT JOIN {database_name}.si_proteinentities PE ON PE.IRN = CAST(AL.ProteinEntitiesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.si_proteinstandardversions PSV ON CAST(PE.ProteinStandardVersionsIRN AS VARCHAR(50)) = PSV.IRN \
LEFT JOIN {database_name}.lk_standard LST ON LST.IRN = CAST(PSV.ProteinStandardsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(AL.ProteinProductsAnimalsIRN AS varchar(50)) \
LEFT JOIN {database_name}.lk_estado LES ON LES.cestado=PE.Status \
LEFT JOIN {database_name}.lk_especie LEP ON LEP.IRN = CAST(AL.ProteinBreedCodesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_tipoproducto TP ON TP.ntipoproducto=lpr.grupoproducto \
LEFT JOIN {database_name}.LK_conductor LCO   ON CAST(LCO.IRN AS VARCHAR(50)) = CAST(AL.ProteinDriversIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_vehiculo VH     ON CAST(VH.IRN AS VARCHAR(50))  = CAST(AL.ProteinVehiclesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_incubadora INC  ON INC.ProteinCostCentersIRN    = CAST(AL.ProteinCostCentersIRN_HatchHatchery as varchar (50)) \
LEFT JOIN {database_name}.si_mvbrimfarms MB  ON CAST(MB.ProteinFarmsIRN AS VARCHAR (50)) = LP.IRN \
LEFT JOIN {database_name}.lk_proveedor PRO   ON MB.VendorNo = PRO.cproveedor \
LEFT JOIN {database_name}.categoria CAT ON LP.pk_plantel = CAT.pk_plantel and LL.pk_lote = CAT.pk_lote and LG.pk_galpon = CAT.pk_galpon \
LEFT JOIN {database_name}.atipicos AT ON AL.ComplexEntityNo = AT.ComplexEntityNo \
LEFT JOIN {database_name}.ft_Reprod_Galpon_Semana RGD ON AL.ComplexEntityNoPadre = RGD.complexentityno and ceiling((round((DiasPadreFechaNacReprod*1.0)/7,0) + ((DiasPadreFechaNacReprod*1.0)%7)/10)) = RGD.edad and RGD.pk_etapa = 2 \
LEFT JOIN {database_name}.SumatoriaAlojamiento1  SA1 ON SA1.ComplexEntityNo   = AL.ComplexEntityNo AND SA1.EdadPadreDescrip = AL.EdadPadreDescrip \
LEFT JOIN {database_name}.SumatoriaAlojamiento2  SA2 ON SA2.ComplexEntityNo   = AL.ComplexEntityNo AND SA2.EdadPadreDescrip2 = AL.EdadPadreDescrip2 \
LEFT JOIN {database_name}.SumatoriaAlojamiento3  SA3 ON SA3.ComplexEntityNo   = AL.ComplexEntityNo \
LEFT JOIN {database_name}.SumatoriaAlojamiento4  SA4 ON SA4.ComplexEntityNo   = AL.ComplexEntityNo AND SA4.ProteinBreedCodesIRN = AL.ProteinBreedCodesIRN \
LEFT JOIN {database_name}.SumatoriaAlojamiento4  SA41 ON SA41.ComplexEntityNo = AL.ComplexEntityNo AND SA41.ProteinBreedCodesIRN = '143F98F4-1292-4533-AB3D-674CC21D15DF' \
LEFT JOIN {database_name}.SumatoriaAlojamiento4  SA42 ON SA42.ComplexEntityNo = AL.ComplexEntityNo AND (SA42.ProteinBreedCodesIRN = '143F98F4-1292-4533-AB3D-674CC21D15DF' OR \
SA42.ProteinBreedCodesIRN = 'FA43CCAB-D1FC-46E1-BD14-6433E437F2F3' OR SA42.ProteinBreedCodesIRN = '8B742404-5DFE-49C4-A894-78FE52D095E1') \
LEFT JOIN {database_name}.SumatoriaAlojamiento5  SA5 ON SA5.ComplexEntityNo = AL.ComplexEntityNo AND SA5.FarmNoPadre = AL.FarmNoPadre \
LEFT JOIN {database_name}.SumatoriaAlojamiento6  SA6 ON SA6.ComplexEntityNo = AL.ComplexEntityNo \
LEFT JOIN {database_name}.SumatoriaAlojamiento7  SA7 ON SA7.ComplexEntityNoGalpon = AL.ComplexEntityNoGalpon")

#df_Alojamiento.createorreplaceview("alojamiento")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamiento"
}
df_Alojamiento.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.alojamiento")
print('carga alojamiento')
#4 Tabla temporal para identificar cual es la planta de incubación mayoritaria
df_PlantaMayorTemp = spark.sql(f"SELECT * \
                                FROM ( \
                                     SELECT \
                                     CAST(ComplexEntityNo AS VARCHAR(50)) ComplexEntityNo, \
                                     pk_incubadora, \
                                     ListaPadre, \
                                     ListaIncubadora, \
                                     PlantaIncubacion, \
                                     ProteinEntitiesIRN, \
                                     AVG(TotalAloj)TotalAloj, \
                                     MAX(CantAlojamientoTotal) CantAlojamientoTotal, \
                                     ROUND(AVG(TotalAloj)/MAX(CantAlojamientoTotal*1.0),3) PorcIncMayor, \
                                     ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo ORDER BY A.pk_incubadora) AS Orden \
                                     FROM {database_name}.alojamiento A \
                                     WHERE TotalAloj = (SELECT MAX(TotalAloj) FROM {database_name}.alojamiento X WHERE a.ComplexEntityNo = x.ComplexEntityNo) \
                                     GROUP BY ComplexEntityNo,pk_incubadora,ListaPadre,ListaIncubadora,PlantaIncubacion,ProteinEntitiesIRN \
                                     ) A \
                                WHERE Orden = 1")
#df_PlantaMayorTemp.createorreplaceview("PlantaMayor")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PlantaMayor"
}
df_PlantaMayorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PlantaMayor")
print('carga PlantaMayor')
#4 Tabla temporal para identificar cual es la especie o raza mayoritaria
df_RazaMayorTemp = spark.sql(f"SELECT * \
                              FROM ( \
                                    SELECT \
                                    ComplexEntityNo, \
                                    ProteinEntitiesIRN, \
                                    ProteinBreedCodesIRN, \
                                    Raza, \
                                    AVG(PesoAlojamientoPond) AS PesoAlojamientoPond, \
                                    AVG(PorcCodigoRaza) AS PorcCodigoRaza, \
                                    ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo, A.ProteinEntitiesIRN ORDER BY ProteinBreedCodesIRN) AS Orden \
                                    FROM {database_name}.alojamiento A \
                                    WHERE CantAlojamientoXRaza = (SELECT MAX(CantAlojamientoXRaza) FROM {database_name}.alojamiento B WHERE A.ComplexEntityNo = B.ComplexEntityNo) \
                                    GROUP BY ComplexEntityNo, ProteinEntitiesIRN, ProteinBreedCodesIRN, Raza \
                                    )A \
                                WHERE Orden = 1")
#df_RazaMayorTemp.createorreplaceview("RazaMayor")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/RazaMayor"
}
df_RazaMayorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.RazaMayor")
print('carga RazaMayor')
#4 Tabla temporal para identificar cual es el alojamiento de padre mayoritario
df_PadreMayorTemp = spark.sql(f"SELECT * \
                               FROM ( \
                                     SELECT \
                                     ComplexEntityNo, \
                                     PlantelPadre, \
                                     MAX(CantAlojamientoPadre) AS CantAlojamientoPadre, \
                                     ROUND(MAX(CantAlojamientoPadre)/MAX(CantAlojamientoTotal*1.0),3) PorcAlojPadre, \
                                     ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo ORDER BY PlantelPadre) AS Orden \
                                     FROM {database_name}.alojamiento A \
                                     WHERE CantAlojamientoPadre = (SELECT MAX(CantAlojamientoPadre) FROM {database_name}.alojamiento B WHERE A.ComplexEntityNo = B.ComplexEntityNo) \
                                     GROUP BY ComplexEntityNo, PlantelPadre \
                                     )A \
                                WHERE Orden = 1")
#df_PadreMayorTemp.createorreplaceview("PadreMayor")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PadreMayor"
}
df_PadreMayorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PadreMayor")
print('carga PadreMayor')
df_EdadPadreMayorTemp = spark.sql(f"SELECT * \
                                   FROM ( \
                                         SELECT \
                                         ComplexEntityNo, \
                                         EdadPadreDescrip, \
                                         MAX(CantAlojamientoXEdadPadre) AS CantAlojamientoXEdadPadre, \
                                         ROUND(MAX(CantAlojamientoXEdadPadre)/MAX(CantAlojamientoTotal*1.0),3) PorcAlojamientoXEdadPadre, \
                                         ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo ORDER BY EdadPadreDescrip) AS Orden \
                                         FROM {database_name}.alojamiento A \
                                         WHERE CantAlojamientoXEdadPadre = (SELECT MAX(CantAlojamientoXEdadPadre) FROM {database_name}.alojamiento B WHERE A.ComplexEntityNo = B.ComplexEntityNo) \
                                         GROUP BY ComplexEntityNo, EdadPadreDescrip \
                                         )A \
                                    WHERE Orden = 1")
#df_EdadPadreMayorTemp.createorreplaceview("EdadPadreMayor")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadPadreMayor"
}
df_EdadPadreMayorTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadPadreMayor")
print('carga EdadPadreMayor')
df_EdadPadreMayor2Temp = spark.sql(f"SELECT * \
                                    FROM ( \
                                          SELECT \
                                          ComplexEntityNo, \
                                          EdadPadreDescrip2, \
                                          MAX(CantAlojamientoXEdadPadre2) AS CantAlojamientoXEdadPadre2, \
                                          ROUND(MAX(CantAlojamientoXEdadPadre2)/MAX(CantAlojamientoTotal*1.0),3) PorcAlojamientoXEdadPadre2, \
                                          ROW_NUMBER() OVER(PARTITION BY A.ComplexEntityNo ORDER BY EdadPadreDescrip2) AS Orden \
                                          FROM {database_name}.alojamiento A \
                                          WHERE CantAlojamientoXEdadPadre2 = (SELECT MAX(CantAlojamientoXEdadPadre2) FROM {database_name}.alojamiento B WHERE A.ComplexEntityNo = B.ComplexEntityNo) \
                                          GROUP BY ComplexEntityNo, EdadPadreDescrip2 \
                                          )A \
                                    WHERE Orden = 1")
#df_EdadPadreMayor2Temp.createorreplaceview("EdadPadreMayor2")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadPadreMayor2"
}
df_EdadPadreMayor2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadPadreMayor2")
print('carga EdadPadreMayor2')
df_sumEdadPadreCorral =spark.sql(f"SELECT ComplexEntityNo, (SUM(EdadPadre*CantAloj*1.0) / SUM(CantAloj)) sumEdadPadreCantAloj \
FROM {database_name}.alojamiento group by ComplexEntityNo")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/sumEdadPadreCorral"
}
df_sumEdadPadreCorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.sumEdadPadreCorral")
print('carga sumEdadPadreCorral')

df_sumEdadPadreCorral1 =spark.sql(f"SELECT pk_plantel,pk_lote,pk_galpon, (SUM(EdadPadre*CantAloj*1.0) / SUM(CantAloj)) sumEdadPadreCantAloj2 \
FROM {database_name}.alojamiento group by pk_plantel,pk_lote,pk_galpon")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/sumEdadPadreCorral1"
}
df_sumEdadPadreCorral1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.sumEdadPadreCorral1")
print('carga sumEdadPadreCorral1')

df_sumEdadPadreCorral2 =spark.sql(f"SELECT pk_plantel,pk_lote,(SUM(EdadPadre*CantAloj*1.0) / SUM(CantAloj)) sumEdadPadreCantAloj3 \
FROM {database_name}.alojamiento group by pk_plantel,pk_lote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/sumEdadPadreCorral2"
}
df_sumEdadPadreCorral2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.sumEdadPadreCorral2")
print('carga sumEdadPadreCorral2')
#Tabla temporal para hallar la edad padre a nivel corral, galpon, lote
df_EdadPadreTemp = spark.sql(f"SELECT IRN,a.ComplexEntityNo,ComplexEntityNoPadre,EdadPadre \
                             ,CASE WHEN SUM(CantAloj) = 0 THEN 0 ELSE sum( b.sumEdadPadreCantAloj) END AS EdadPadreCorral \
                             ,CASE WHEN SUM(CantAloj) = 0 THEN 0 ELSE sum(c.sumEdadPadreCantAloj2) END AS EdadPadreGalpon \
                             ,CASE WHEN SUM(CantAloj) = 0 THEN 0 ELSE sum(d.sumEdadPadreCantAloj3) END AS EdadPadreLote \
                              FROM {database_name}.alojamiento A \
                              left join {database_name}.sumEdadPadreCorral b on B.ComplexEntityNo = A.ComplexEntityNo \
                              left join {database_name}.sumEdadPadreCorral1 c on c.pk_plantel = A.pk_plantel and c.pk_lote = a.pk_lote and c.pk_galpon = a.pk_galpon \
                              left join {database_name}.sumEdadPadreCorral2 d on d.pk_plantel = A.pk_plantel and d.pk_lote = a.pk_lote \
                              GROUP BY irn,ProteinEntitiesIRN,a.ComplexEntityNo,ComplexEntityNoPadre,EdadPadre,a.pk_plantel,a.pk_lote,a.pk_galpon")
#df_EdadPadreTemp.createorreplaceview("EdadPadre")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadPadre"
}
df_EdadPadreTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadPadre")
print('carga EdadPadre')
df_ft_Alojamiento = spark.sql(f"SELECT \
 AL.pk_tiempo \
,AL.fecha \
,AL.pk_empresa \
,AL.pk_division \
,AL.pk_zona \
,AL.pk_subzona \
,AL.pk_plantel \
,AL.pk_lote \
,AL.pk_galpon \
,AL.pk_sexo \
,AL.pk_standard \
,AL.pk_producto \
,AL.pk_tipoproducto \
,AL.pk_especie \
,AL.pk_estado \
,AL.pk_administrador \
,AL.pk_conductor \
,AL.pk_vehiculo \
,AL.pk_incubadora \
,AL.pk_proveedor \
,AL.ComplexEntityNo \
,AL.ComplexEntityNoPadre \
,AL.LotePadre \
,AL.PlantelPadre \
,AL.NroGuia \
,AL.FechaNacimiento \
,(SELECT MIN(FechaAlojamiento) FROM {database_name}.alojamiento A WHERE a.pk_lote = al.pk_lote) FechaAlojamiento \
,AL.FechaIniAlojamiento \
,AL.FechaFinAlojamiento \
,AL.FechaRecepcion \
,AL.FechaCierre \
,AL.TipoGranja \
,PAM.PlantelPadre PadreMayor \
,nvl(AL.ListaPadre,'-') ListaPadre \
,RM.Raza RazaMayor \
,PM.PlantaIncubacion AS IncubadoraMayor \
,PM.ListaIncubadora \
,AL.PesoBebe \
,AL.PesoAloj \
,AL.PesoAlojamientoPond PesoAlojPond \
,AL.CantAloj \
,AL.CantAlojamientoXRaza CantAlojXRaza \
,AL.CantAlojamientoPadre CantAlojXPadre \
,AL.CantAlojamientoTotal CantAlojTotal \
,AL.TotalAloj \
,AL.MortAloj \
,CASE WHEN AL.TotalAloj = 0 THEN 0 ELSE ROUND(((AL.CantAloj / (AL.TotalAloj*1.0))*100),2) END PorcBbAloj \
,(SELECT SUM(CantAloj) \
FROM {database_name}.alojamiento A \
WHERE A.complexentityno = AL.complexentityno and A.plantelpadre = AL.plantelpadre \
GROUP BY complexentityno,plantelpadre)/(AL.CantAlojamientoTotal*1.0) AS PorcAlojPadre \
,PAM.PorcAlojPadre PorcAlojPadreMayor \
,AL.PorcCodigoRaza PorcRaza \
,RM.PorcCodigoRaza PorcRazaMayor \
,PM.PorcIncMayor \
,AL.DiasAlojamiento as DiasAloj \
,AL.categoria \
,AL.FlagAtipico \
,AL.EdadPadre \
,ROUND(EP.EdadPadreCorral,0) EdadPadreCorral \
,ROUND(EP.EdadPadreGalpon,0) EdadPadreGalpon \
,ROUND(EP.EdadPadreLote,0) EdadPadreLote \
,PesoHvo \
,CASE WHEN (SELECT SUM(CantAloj) FROM {database_name}.alojamiento D WHERE D.ComplexEntityNo = AL.ComplexEntityNo) = 0 THEN 0 \
ELSE (SELECT SUM(PesoHvoXCantAloj) FROM {database_name}.alojamiento D WHERE D.ComplexEntityNo = AL.ComplexEntityNo AND CantAloj>0) / \
(SELECT sum(CantAloj) FROM {database_name}.alojamiento D WHERE D.ComplexEntityNo = AL.ComplexEntityNo AND PesoHvoXCantAloj>0) END AS PesoHvoPond \
,AL.CantAlojamientoXROSS CantAlojXROSS \
,AL.CantAlojamientoXROSS / nullif(AL.CantAlojamientoTotal*1.0,0) PorcAlojXROSS \
,AL.CantAlojamientoXTodosLosROSS CantAlojXTROSS \
,AL.CantAlojamientoXTodosLosROSS / nullif(AL.CantAlojamientoTotal*1.0,0) PorcAlojXTROSS \
,EPM2.EdadPadreDescrip2 as EdadPadreCorralDescrip \
,AL.FlagTransPavos \
,AL.SourceComplexEntityNo \
,AL.CantAloj - AL.MortAloj CantAlojMort \
,AL.CantAlojamientoXEdadPadre \
,AL.EdadPadreDescrip EdadPadreDescripXCantAloj \
,EPM.EdadPadreDescrip EdadPadreDescripXCantAlojMayor \
,EPM2.PorcAlojamientoXEdadPadre2 as PorcAlojamientoXEdadPadre \
,AL.CantAlojamientoTotal / nullif(CantAlojamientoXGalpon*1.0,0) PorcentajeHM \
,nvl(CASE WHEN pk_sexo = 1 and AL.CantAlojamientoTotal / nullif(CantAlojamientoXGalpon*1.0,0) >= 0.6 THEN 1 \
WHEN pk_sexo = 1 and AL.CantAlojamientoTotal / nullif(CantAlojamientoXGalpon*1.0,0) <= 0.6 THEN 0 END,0) + \
nvl(CASE WHEN pk_sexo = 2 and AL.CantAlojamientoTotal / nullif(CantAlojamientoXGalpon*1.0,0) <= 0.4 THEN 0 \
WHEN pk_sexo = 2 and AL.CantAlojamientoTotal / nullif(CantAlojamientoXGalpon*1.0,0) >= 0.4 THEN 1 END,0) ParticipacionHM \
,AL.Origen \
,CASE WHEN AL.ListaTipoOrigen = 'PROPIO' THEN 'PROPIO' \
WHEN AL.ListaTipoOrigen = 'COMPRADO' THEN 'COMPRADO' \
ELSE 'MIXTO' END TipoOrigen \
FROM {database_name}.alojamiento AL \
LEFT JOIN {database_name}.PlantaMayor PM ON AL.ComplexEntityNo = PM.ComplexEntityNo \
LEFT JOIN {database_name}.RazaMayor RM ON AL.ComplexEntityNo = RM.ComplexEntityNo \
LEFT JOIN {database_name}.PadreMayor PAM ON AL.ComplexEntityNo = PAM.ComplexEntityNo \
LEFT JOIN {database_name}.EdadPadreMayor EPM ON AL.ComplexEntityNo = EPM.ComplexEntityNo \
LEFT JOIN {database_name}.EdadPadreMayor2 EPM2 ON AL.ComplexEntityNo = EPM2.ComplexEntityNo \
LEFT JOIN {database_name}.EdadPadre EP ON AL.ComplexEntityNo = EP.ComplexEntityNo AND AL.ComplexEntityNoPadre = EP.ComplexEntityNoPadre AND AL.IRN = EP.IRN")
print('carga df_ft_Alojamiento')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target18) 
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target18)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_Alojamiento cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_Alojamiento")


if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target18)
    data_after_delete = existing_data.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_Alojamiento.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_AlojamientoTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_AlojamientoTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_AlojamientoTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Alojamiento"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Alojamiento")
            
    print(f"agrega registros nuevos a la tabla ft_Alojamiento : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_Alojamiento : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_AlojamientoTemporal')
    print(f"Tabla ft_AlojamientoTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Alojamiento"
    }
    df_ft_Alojamiento.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Alojamiento")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()