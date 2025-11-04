# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')
# Define el nombre del trabajo
JOB_NAME = "ntb_prd_tbl_saca_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, current_timestamp
# Parámetros de entrada global
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"

file_name_target31 = f"{bucket_name_prdmtech}ft_ventas_CD/"

path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

print('cargando ruta')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
#PAVOS
df_SacaPavo0Temp = spark.sql(f"SELECT PPT.CreationDate,PPT.LastModDate,PPT.xDate,PPT.EventDate,PPT.IRN,PPT.ProteinDriversIRN, \
PPT.ProteinEntitiesIRN,PPT.ProteinVehiclesIRN,PPT.PostTransactionId,PPT.ProteinFacilityPlantsIRN,PPTD.IRN AS IRND, \
PPTD.ProteinEntitiesIRN AS ProteinEntitiesIRND, PPTD.ProteinFarmsIRN,PPTD.ProteinGrowoutCodesIRN,PPTD.ProteinCostCentersIRN_Farm, \
PPTD.ProteinCostCentersIRN,PPTD.ProteinProductsAnimalsIRN_Ovr,PPTD.PmtsProcRecvTransIRN,PPTD.complexentityno, \
substring(PPTD.complexentityno,1,(length(PPTD.complexentityno)-3)) ComplexEntityNoGalpon,PPT.U_GRemision,PPT.LoadNo,PPTD.PlantNo, \
PPTD.DriverNo,PPTD.VoidFlag,PPTD.PostStatus,PPTD.VehicleNo,PPTD.ProductNo,PPTD.PlantName,PPTD.EntityNo,PPTD.FarmNo,PPTD.BreedNo, \
PPTD.HouseNo,PPTD.PenNo,PPTD.HouseNoPenNo,PPT.UserId,PPT.FarmHdCount,PPT.FarmWtGross,PPT.FarmWtNet,PPT.FarmWtRefNo,PPT.FarmWtTare, \
PPTD.FarmHdCount AS FarmHdCountD,PPTD.FarmWtNet AS FarmWtNetD,PPTD.FarmWtGross AS FarmWtGrossD,PPTD.FarmWtTare AS FarmWtTareD, \
CASE WHEN isnull(PTS.SourceComplexEntityNo) = true AND isnull(PTD.DestinationComplexEntityNo) = true THEN 0 ELSE 1 END FlagTransfPavos, \
CASE WHEN isnull(PTS.SourceComplexEntityNo) = true THEN PTD.SourceComplexEntityNo ELSE PTS.SourceComplexEntityNo END SourceComplexEntityNo, \
CASE WHEN isnull(PTD.DestinationComplexEntityNo) = true THEN PTS.DestinationComplexEntityNo ELSE PTD.DestinationComplexEntityNo END DestinationComplexEntityNo \
FROM {database_name}.si_pmtsprocrecvtrans PPT \
LEFT JOIN {database_name}.si_mvpmtsprocrecvtranshousedetail PPTD ON PPT.IRN = PPTD.PmtsProcRecvTransIRN \
LEFT JOIN {database_name}.si_mvpmtstransferdestdetails AS PTS ON PPTD.ComplexEntityNo = PTS.SourceComplexEntityNo \
LEFT JOIN {database_name}.si_mvpmtstransferdestdetails AS PTD ON PPTD.ComplexEntityNo = PTD.DestinationComplexEntityNo \
WHERE PPTD.PostTransactionId is not null \
AND PPTD.FarmType = 7 and PPTD.SpeciesType = 2")
#date_format(cast(cast(PPT.EventDate as timestamp) as date),'yyyyMM') >= CONVERT(VARCHAR(8),DATEADD(MM, DATEDIFF(MM,0,DATEADD(MONTH,-8,GETDATE())), 0),112)
#df_SacaPavo0Temp.createOrReplaceTempView("SacaPavo0")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaPavo0"
}
df_SacaPavo0Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaPavo0")
print('carga SacaPavo0')
df_DestinationComplexEntityNoTemp1 = spark.sql(f"SELECT ProteinEntitiesIRN,ProteinFacilityPlantsIRN,ProteinEntitiesIRND, \
ProteinFarmsIRN,ProteinGrowoutCodesIRN,ProteinCostCentersIRN_Farm,ProteinCostCentersIRN,ComplexEntityNo,ComplexEntityNoGalpon, \
PlantNo,PlantName,EntityNo,FarmNo,HouseNo,PenNo,HouseNoPenNo \
from {database_name}.SacaPavo0 \
where ComplexEntityNo in (select DestinationComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where DEstinationComplexEntityNo like 'v%') \
group by ProteinEntitiesIRN,ProteinFacilityPlantsIRN,ProteinEntitiesIRND,ProteinFarmsIRN,ProteinGrowoutCodesIRN,ProteinCostCentersIRN_Farm, \
ProteinCostCentersIRN,ComplexEntityNo,ComplexEntityNoGalpon,PlantNo,PlantName,EntityNo,FarmNo,HouseNo,PenNo,HouseNoPenNo")

#df_DestinationComplexEntityNoTemp1.createOrReplaceTempView("DestinationComplexEntityNoTemp1")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DestinationComplexEntityNoTemp1"
}
df_DestinationComplexEntityNoTemp1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DestinationComplexEntityNoTemp1")
print('carga DestinationComplexEntityNoTemp1')
df_SacaPavo0_upd = spark.sql(f"SELECT CreationDate,LastModDate,xDate,EventDate,IRN,ProteinDriversIRN, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinEntitiesIRN ELSE A.ProteinEntitiesIRN END ProteinEntitiesIRN, \
ProteinVehiclesIRN,PostTransactionId, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinFacilityPlantsIRN ELSE A.ProteinFacilityPlantsIRN END ProteinFacilityPlantsIRN, IRND, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinEntitiesIRND ELSE A.ProteinEntitiesIRND END ProteinEntitiesIRND, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinFarmsIRN ELSE A.ProteinFarmsIRN END ProteinFarmsIRN, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinGrowoutCodesIRN ELSE A.ProteinGrowoutCodesIRN END ProteinGrowoutCodesIRN, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinCostCentersIRN_Farm ELSE A.ProteinCostCentersIRN_Farm END ProteinCostCentersIRN_Farm, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ProteinCostCentersIRN ELSE A.ProteinCostCentersIRN END ProteinCostCentersIRN, \
ProteinProductsAnimalsIRN_Ovr,PmtsProcRecvTransIRN, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.complexentityno ELSE A.complexentityno END complexentityno, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.ComplexEntityNoGalpon ELSE A.ComplexEntityNoGalpon END ComplexEntityNoGalpon,U_GRemision,LoadNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.PlantNo ELSE A.PlantNo END PlantNo, \
DriverNo,VoidFlag,PostStatus,VehicleNo,ProductNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.PlantName ELSE A.PlantName END PlantName, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.EntityNo ELSE A.EntityNo END EntityNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.FarmNo ELSE A.FarmNo END FarmNo,BreedNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.HouseNo ELSE A.HouseNo END HouseNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.PenNo ELSE A.PenNo END PenNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') and \
isnotnull(B.ProteinEntitiesIRN) = true THEN B.HouseNoPenNo ELSE A.HouseNoPenNo END HouseNoPenNo,UserId,FarmHdCount,FarmWtGross,FarmWtNet, \
FarmWtRefNo,FarmWtTare,FarmHdCountD,FarmWtNetD,FarmWtGrossD,FarmWtTareD,FlagTransfPavos,SourceComplexEntityNo,DestinationComplexEntityNo \
from {database_name}.SacaPavo0 A \
LEFT JOIN {database_name}.DestinationComplexEntityNoTemp1 B on A.DestinationComplexEntityNo = B.ComplexEntityNo")
#df_SacaPavo0_upd.createOrReplaceTempView("SacaPavo0_upd")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaPavo0_upd"
}
df_SacaPavo0_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaPavo0_upd")
print('carga SacaPavo0_upd')
df_Saca1Temp1 = spark.sql(f"SELECT \
PPT.CreationDate,PPT.LastModDate,PPT.xDate,PPT.EventDate,PPT.IRN,PPT.ProteinDriversIRN,PPT.ProteinEntitiesIRN,PPT.ProteinVehiclesIRN, \
PPT.PostTransactionId,PPT.ProteinFacilityPlantsIRN,PPTD.IRN AS IRND,PPTD.ProteinEntitiesIRN AS ProteinEntitiesIRND, \
PPTD.ProteinFarmsIRN,PPTD.ProteinGrowoutCodesIRN,PPTD.ProteinCostCentersIRN_Farm,PPTD.ProteinCostCentersIRN,AX.pk_especie, \
PPTD.ProteinProductsAnimalsIRN_Ovr,PPTD.PmtsProcRecvTransIRN,PPTD.complexentityno, \
substring(PPTD.complexentityno,1,(length(PPTD.complexentityno)-3)) ComplexEntityNoGalpon, \
PPT.U_GRemision,PPT.LoadNo,PPTD.PlantNo,PPTD.DriverNo,PPTD.VoidFlag,PPTD.PostStatus,PPTD.VehicleNo,PPTD.ProductNo,PPTD.PlantName, \
PPTD.EntityNo,PPTD.FarmNo,PPTD.BreedNo,PPTD.HouseNo,PPTD.PenNo,PPTD.HouseNoPenNo,PPT.UserId,PPT.FarmHdCount,PPT.FarmWtGross, \
PPT.FarmWtNet,PPT.FarmWtRefNo,PPT.FarmWtTare,PPTD.FarmHdCount AS FarmHdCountD,PPTD.FarmWtNet AS FarmWtNetD, \
PPTD.FarmWtGross AS FarmWtGrossD,PPTD.FarmWtTare AS FarmWtTareD, \
(select nvl(sum(MT.FarmHdCount),0) from {database_name}.si_mvpmtsprocrecvtranshousedetail MT WHERE MT.ComplexEntityNo= PPTD.ComplexEntityNo AND MT.PostTransactionId is not null) FarmHdCountDTot, \
(select nvl(sum(MT.FarmHdCount),0) from {database_name}.si_mvpmtsprocrecvtranshousedetail MT WHERE MT.ComplexEntityNo= PPTD.ComplexEntityNo AND cast(MT.EventDate as timestamp) <= cast(PPTD.EventDate as timestamp) AND MT.PostTransactionId is not null) FarmHdCountDAcum, \
(select nvl(sum(MT.FarmWtNet),0) from   {database_name}.si_mvpmtsprocrecvtranshousedetail MT WHERE MT.ComplexEntityNo= PPTD.ComplexEntityNo AND cast(MT.EventDate as timestamp) <= cast(PPTD.EventDate as timestamp) AND MT.PostTransactionId is not null) FarmWtNetDAcum \
FROM {database_name}.si_pmtsprocrecvtrans PPT \
LEFT JOIN {database_name}.si_mvpmtsprocrecvtranshousedetail PPTD ON PPT.IRN = PPTD.PmtsProcRecvTransIRN \
LEFT JOIN (select * from ( \
SELECT ComplexEntityNo,pk_especie, \
ROW_NUMBER() OVER(PARTITION BY ComplexEntityNo ORDER BY pk_especie) as Rn \
from {database_name}.ft_alojamiento A \
where CantAlojXRaza = (select max(CantAlojXRaza) from {database_name}.ft_alojamiento  B where B.ComplexEntityNo = A.ComplexEntityNo) \
group by ComplexEntityNo,pk_especie \
) B \
where rn=1) AX ON  AX.ComplexEntityNo = PPTD.ComplexEntityNo \
WHERE PPTD.PostTransactionId is not null AND FarmType = 1 and SpeciesType = 1")
#df_Saca1Temp.createOrReplaceTempView("Saca1Temp1")

print('carga Saca1Temp1')
df_Saca1Temp2 = spark.sql(f"SELECT SP.CreationDate,SP.LastModDate,SP.xDate,SP.EventDate,SP.IRN,SP.ProteinDriversIRN,SP.ProteinEntitiesIRN, \
SP.ProteinVehiclesIRN,SP.PostTransactionId,SP.ProteinFacilityPlantsIRN,SP.IRND,SP.ProteinEntitiesIRND, SP.ProteinFarmsIRN, \
SP.ProteinGrowoutCodesIRN,SP.ProteinCostCentersIRN_Farm,SP.ProteinCostCentersIRN,AX.pk_especie,SP.ProteinProductsAnimalsIRN_Ovr, \
SP.PmtsProcRecvTransIRN,SP.complexentityno, \
substring(SP.complexentityno,1,(length(SP.complexentityno)-3)) ComplexEntityNoGalpon, \
SP.U_GRemision,SP.LoadNo,SP.PlantNo,SP.DriverNo,SP.VoidFlag,SP.PostStatus,SP.VehicleNo,SP.ProductNo,SP.PlantName,SP.EntityNo, \
SP.FarmNo,SP.BreedNo,SP.HouseNo,SP.PenNo,SP.HouseNoPenNo,SP.UserId,SP.FarmHdCount,SP.FarmWtGross,SP.FarmWtNet,SP.FarmWtRefNo, \
SP.FarmWtTare,SP.FarmHdCount AS FarmHdCountD,SP.FarmWtNet AS FarmWtNetD,SP.FarmWtGross AS FarmWtGrossD,SP.FarmWtTare AS FarmWtTareD, \
(select nvl(sum(MT.FarmHdCountD),0) from SacaPavo0 MT WHERE MT.ComplexEntityNo= SP.ComplexEntityNo AND MT.PostTransactionId is not null) FarmHdCountDTot, \
(select nvl(sum(MT.FarmHdCountD),0) from SacaPavo0 MT WHERE MT.ComplexEntityNo= SP.ComplexEntityNo AND cast(MT.EventDate as timestamp) <= cast(SP.EventDate as timestamp) AND MT.PostTransactionId is not null) FarmHdCountDAcum, \
(select nvl(sum(MT.FarmWtNetD),0) from SacaPavo0 MT WHERE MT.ComplexEntityNo= SP.ComplexEntityNo AND cast(MT.EventDate as timestamp) <= cast(SP.EventDate as timestamp) AND MT.PostTransactionId is not null) FarmWtNetDAcum \
FROM {database_name}.SacaPavo0 SP \
LEFT JOIN (select * from( \
SELECT ComplexEntityNo,pk_especie, \
ROW_NUMBER() OVER(PARTITION BY ComplexEntityNo ORDER BY pk_especie) as Rn \
from {database_name}.ft_alojamiento A \
where CantAlojXRaza = (select max(CantAlojXRaza) from {database_name}.ft_alojamiento  B where B.ComplexEntityNo = A.ComplexEntityNo) \
group by ComplexEntityNo,pk_especie \
) B \
where rn=1) AX ON  AX.ComplexEntityNo = SP.ComplexEntityNo")
                          
#df_Saca1Temp2.createOrReplaceTempView("Saca1Temp2")
print('Saca1Temp2')
df_Saca1Temp = df_Saca1Temp1.union(df_Saca1Temp2)
#df_Saca1Temp.createOrReplaceTempView("Saca1")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca1"
}
df_Saca1Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca1")
print('carga Saca1')
df_Saca2Temp1 = spark.sql(f"SELECT C.IRN,C.ProteinDriversIRN,C.ProteinEntitiesIRN,C.ProteinVehiclesIRN,C.PostTransactionId, \
C.ProteinFacilityPlantsIRN,C.IRND,C.ProteinEntitiesIRND,C.ProteinFarmsIRN,C.ProteinGrowoutCodesIRN,C.ProteinCostCentersIRN_Farm, \
C.ProteinCostCentersIRN,C.pk_especie,C.ProteinProductsAnimalsIRN_Ovr,C.PmtsProcRecvTransIRN,A.ProteinEntitiesIRN AS ProteinEntitiesIRNS, \
B.ProteinFarmsIRN AS ProteinFarmsIRNS,B.ProteinCostCentersIRN AS ProteinCostCentersIRNS,A.TransactionIRN,B.ComplexEntityNo, \
C.ComplexEntityNoGalpon,A.CreationDate as CreationDateS,C.CreationDate,A.EventDate,A.xDate,unix_timestamp(cast(A.xDate as timestamp)) NumXDate, \
B.FirstHatchDate,B.FirstDateSold,B.LastDateSold,D.FirstHatchDate as FirstHatchDateLote,D.FirstDateSold as FirstDateSoldLote, \
D.LastDateSold as LastDateSoldLote,E.FirstHatchDate as FirstHatchDateGalpon,E.FirstDateSold as FirstDateSoldGalpon, \
E.LastDateSold as LastDateSoldGalpon,B.FarmNo,B.EntityNo,B.HouseNo,B.PenNo,C.LoadNo, \
DATEDIFF(cast(A.EventDate as timestamp),    cast(B.FirstHatchDate as timestamp)) as EdadDiaSaca, \
DATEDIFF(cast(A.xDate as timestamp),        cast(B.FirstHatchDate as timestamp)) as EdadDiaSacaxDate, \
DATEDIFF(cast(B.FirstDateSold as timestamp),cast(B.FirstHatchDate as timestamp))  AS EdadInicioSaca, \
DATEDIFF(cast(B.LastDateSold as timestamp), cast(B.FirstHatchDate as timestamp)) AS EdadFinSaca, \
DATEDIFF(cast(B.LastDateSold as timestamp), cast(B.FirstDateSold as timestamp)) AS DiasSaca, \
DATEDIFF(cast(A.EventDate as timestamp),    cast(D.FirstHatchDate as timestamp)) as EdadDiaSacaLote, \
DATEDIFF(cast(D.FirstDateSold as timestamp),cast(D.FirstHatchDate as timestamp))  AS EdadInicioSacaLote, \
DATEDIFF(cast(D.LastDateSold as timestamp), cast(D.FirstHatchDate as timestamp)) AS EdadFinSacaLote, \
DATEDIFF(cast(D.LastDateSold as timestamp), cast(D.FirstDateSold as timestamp))+1 AS DiasSacaLote, \
DATEDIFF(cast(E.LastDateSold as timestamp), cast(E.FirstDateSold as timestamp))+1 AS DiasSacaGalpon, \
ABS(A.Quantity) AS SacaDia,B.GrowoutNo,B.GrowoutName,B.ProductNo,B.ProductName,B.BreedNo,B.BreedName,B.Status,C.FarmWtRefNo, \
B.TechSupervisorNo,'Brim' AS TipoGranja,C.UserId,C.FarmHdCountD,C.FarmHdCountDAcum,C.FarmHdCountDTot,C.FarmHdCount, \
C.FarmWtNetD,C.FarmWtNetDAcum,C.FarmWtNet, \
(SELECT SUM(FarmHdCountd) AS FarmHdCountd \
FROM {database_name}.Saca1 X \
WHERE (X.ProteinEntitiesIRN = C.ProteinEntitiesIRN) ) AS FarmHdCountTotal, \
CASE WHEN C.FarmHdCount = 0 THEN 0 ELSE ROUND((C.FarmWtNetD/C.FarmHdCountD),2) END AS PesoPromGranja, \
(SELECT SUM(abs(Quantity)) AS Expr1 \
FROM {database_name}.si_brimentityinventory AS DI \
WHERE (ProteinEntitiesIRN = A.ProteinEntitiesIRN) AND (SourceCode = 'BrimFieldTrans') AND (xDate between B.FirstDateSold AND B.LastDateSold)) AS SacaMort \
FROM {database_name}.si_brimentityinventory A \
LEFT JOIN {database_name}.si_mvbrimentities B ON A.ProteinEntitiesIRN  = B.IRN \
LEFT JOIN {database_name}.Saca1 C ON A.ProteinEntitiesIRN = C.ProteinEntitiesIRND and A.TransactionIRN = C.irnd \
LEFT JOIN {database_name}.si_mvbrimentities D ON D.IRN = C.ProteinEntitiesIRN \
LEFT JOIN {database_name}.si_mvbrimentities E ON C.ComplexEntityNoGalpon = E.ComplexEntityNo and E.GRN = 'H' \
WHERE SourceCode = 'PmtsProcRecvTrans' AND B.FarmType = 1 and B.SpeciesType = 1")

#df_Saca2Temp1.createOrReplaceTempView("Saca2Temp1")
print('carga Saca2Temp1')

df_SACAPAVO01Temp = spark.sql(f"SELECT A.ProteinEntitiesIRN AS ProteinEntitiesIRNS,B.ProteinFarmsIRN AS ProteinFarmsIRNS, \
B.ProteinCostCentersIRN AS ProteinCostCentersIRNS,A.TransactionIRN,B.ComplexEntityNo,A.CreationDate as CreationDateS, \
A.EventDate,A.xDate,unix_timestamp(cast(A.xDate as timestamp)) NumXDate, B.FirstHatchDate,B.FirstDateSold,B.LastDateSold, \
B.FarmNo,B.EntityNo,B.HouseNo,B.PenNo, \
DATEDIFF(cast(A.EventDate     as timestamp),cast(B.FirstHatchDate as timestamp)) as EdadDiaSaca, \
DATEDIFF(cast(A.xDate         as timestamp),cast(B.FirstHatchDate as timestamp)) as EdadDiaSacaxDate, \
DATEDIFF(cast(B.FirstDateSold as timestamp),cast(B.FirstHatchDate as timestamp)) as EdadInicioSaca, \
DATEDIFF(cast(B.LastDateSold  as timestamp),cast(B.FirstHatchDate as timestamp)) as EdadFinSaca, \
DATEDIFF(cast(B.LastDateSold  as timestamp),cast(B.FirstDateSold  as timestamp)) as DiasSaca, \
ABS(A.Quantity) AS SacaDia,B.GrowoutNo,B.GrowoutName,B.ProductNo,B.ProductName,B.BreedNo,B.BreedName,B.Status,B.TechSupervisorNo, \
'Brim' AS TipoGranja, \
CASE WHEN isnull(PTS.SourceComplexEntityNo) = true AND isnull(PTD.DestinationComplexEntityNo) = true THEN 0 ELSE 1 END FlagTransfPavos, \
CASE WHEN isnull(PTS.SourceComplexEntityNo) = true THEN PTD.SourceComplexEntityNo ELSE PTS.SourceComplexEntityNo END SourceComplexEntityNo, \
CASE WHEN isnull(PTD.DestinationComplexEntityNo) = true THEN PTS.DestinationComplexEntityNo ELSE PTD.DestinationComplexEntityNo END DestinationComplexEntityNo \
FROM {database_name}.si_brimentityinventory A \
LEFT JOIN {database_name}.si_mvbrimentities B ON A.ProteinEntitiesIRN  = B.IRN \
LEFT JOIN {database_name}.si_mvpmtstransferdestdetails AS PTS ON B.ComplexEntityNo = PTS.SourceComplexEntityNo \
LEFT JOIN {database_name}.si_mvpmtstransferdestdetails AS PTD ON B.ComplexEntityNo = PTD.DestinationComplexEntityNo \
WHERE a.SourceCode = 'PmtsProcRecvTrans' AND b.FarmType = 7 and b.SpeciesType = 2")

#df_SACAPAVO01Temp.createOrReplaceTempView("SACAPAVO01")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SACAPAVO01"
}
df_SACAPAVO01Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SACAPAVO01")
print('carga SACAPAVO01')


df_DestinationComplexEntityNo01Temp = spark.sql(f"SELECT ProteinEntitiesIRNS,ProteinFarmsIRNS,ProteinCostCentersIRNS,ComplexEntityNo, \
FirstHatchDate,FirstDateSold,LastDateSold,FarmNo,EntityNo,HouseNo,PenNo,EdadInicioSaca,EdadFinSaca,DiasSaca \
from {database_name}.SACAPAVO01 \
where ComplexEntityNo in (select DestinationComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where DEstinationComplexEntityNo like 'v%') \
group by ProteinEntitiesIRNS,ProteinFarmsIRNS,ProteinCostCentersIRNS,ComplexEntityNo,FirstHatchDate,FirstDateSold,LastDateSold,FarmNo,EntityNo, \
HouseNo,PenNo,EdadInicioSaca,EdadFinSaca,DiasSaca")
#df_DestinationComplexEntityNo01Temp.createOrReplaceTempView("DestinationComplexEntityNo01")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DestinationComplexEntityNo01"
}
df_DestinationComplexEntityNo01Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DestinationComplexEntityNo01")
print('carga DestinationComplexEntityNo01')

df_SACAPAVO01_upd = spark.sql(f"SELECT \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.ProteinEntitiesIRNS ELSE A.ProteinEntitiesIRNS END ProteinEntitiesIRNS, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.ProteinFarmsIRNS ELSE A.ProteinFarmsIRNS END ProteinFarmsIRNS, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.ProteinCostCentersIRNS ELSE A.ProteinCostCentersIRNS END ProteinCostCentersIRNS, \
A.TransactionIRN, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.ComplexEntityNo ELSE A.ComplexEntityNo END ComplexEntityNo, A.CreationDateS, \
A.EventDate,A.xDate,A.NumXDate, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.FirstHatchDate ELSE A.FirstHatchDate END FirstHatchDate, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.FirstDateSold ELSE A.FirstDateSold END FirstDateSold, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.LastDateSold ELSE A.LastDateSold END LastDateSold, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.FarmNo ELSE A.FarmNo END FarmNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.EntityNo ELSE A.EntityNo END EntityNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.HouseNo ELSE A.HouseNo END HouseNo, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.PenNo ELSE A.PenNo END PenNo, \
A.EdadDiaSaca, A.EdadDiaSacaxDate, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.EdadInicioSaca ELSE A.EdadInicioSaca END EdadInicioSaca, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.EdadFinSaca ELSE A.EdadFinSaca END EdadFinSaca, \
CASE WHEN A.ComplexEntityNo in (select SourceComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where SourceComplexEntityNo like 'v%') \
and isnotnull(B.ComplexEntityNo) = true THEN B.DiasSaca ELSE A.DiasSaca END DiasSaca, \
A.SacaDia,A.GrowoutNo,A.GrowoutName,A.ProductNo,A.ProductName,A.BreedNo,A.BreedName,A.Status,A.TechSupervisorNo, \
A.TipoGranja,A.FlagTransfPavos,A.SourceComplexEntityNo,A.DestinationComplexEntityNo \
from {database_name}.SACAPAVO01 A \
left join {database_name}.DestinationComplexEntityNo01 B on A.DestinationComplexEntityNo = B.ComplexEntityNo ")
#df_SACAPAVO01_upd.createOrReplaceTempView("SACAPAVO01_upd")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SACAPAVO01_upd"
}
df_SACAPAVO01_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SACAPAVO01_upd")
print('carga SACAPAVO01_upd')

df_Saca2Temp2 = spark.sql(f"SELECT C.IRN,C.ProteinDriversIRN,C.ProteinEntitiesIRN,C.ProteinVehiclesIRN,C.PostTransactionId, \
C.ProteinFacilityPlantsIRN,C.IRND,C.ProteinEntitiesIRND,C.ProteinFarmsIRN,C.ProteinGrowoutCodesIRN,C.ProteinCostCentersIRN_Farm, \
C.ProteinCostCentersIRN,C.pk_especie,C.ProteinProductsAnimalsIRN_Ovr,C.PmtsProcRecvTransIRN,A.ProteinEntitiesIRNS,A.ProteinFarmsIRNS, \
A.ProteinCostCentersIRNS,A.TransactionIRN,A.ComplexEntityNo,C.ComplexEntityNoGalpon,A.CreationDateS,C.CreationDate,A.EventDate, \
A.xDate,A.NumXDate,A.FirstHatchDate,A.FirstDateSold,A.LastDateSold,D.FirstHatchDate as FirstHatchDateLote,D.FirstDateSold as FirstDateSoldLote, \
D.LastDateSold as LastDateSoldLote,E.FirstHatchDate as FirstHatchDateGalpon,E.FirstDateSold as FirstDateSoldGalpon,E.LastDateSold as LastDateSoldGalpon, \
A.FarmNo,A.EntityNo,A.HouseNo,A.PenNo,C.LoadNo,A.EdadDiaSaca,A.EdadDiaSacaxDate, \
A.EdadInicioSaca,A.EdadFinSaca,A.DiasSaca, \
DATEDIFF(cast(A.EventDate     as timestamp),cast(D.FirstHatchDate as timestamp)) as EdadDiaSacaLote, \
DATEDIFF(cast(D.FirstDateSold as timestamp),cast(D.FirstHatchDate as timestamp))  AS EdadInicioSacaLote, \
DATEDIFF(cast(D.LastDateSold  as timestamp),cast(D.FirstHatchDate as timestamp)) AS EdadFinSacaLote, \
DATEDIFF(cast(D.LastDateSold  as timestamp),cast(D.FirstDateSold  as timestamp))+1 AS DiasSacaLote, \
DATEDIFF(cast(E.LastDateSold  as timestamp),cast(E.FirstDateSold  as timestamp))+1 AS DiasSacaGalpon, \
A.SacaDia,A.GrowoutNo,A.GrowoutName,A.ProductNo,A.ProductName,A.BreedNo,A.BreedName,A.Status,C.FarmWtRefNo,A.TechSupervisorNo,'Brim' AS TipoGranja, \
C.UserId,C.FarmHdCountD,C.FarmHdCountDAcum,C.FarmHdCountDTot,C.FarmHdCount,C.FarmWtNetD,C.FarmWtNetDAcum,C.FarmWtNet, \
(SELECT SUM(FarmHdCountd) AS FarmHdCountd \
FROM {database_name}.Saca1 X \
WHERE (X.ProteinEntitiesIRN = C.ProteinEntitiesIRN) ) AS FarmHdCountTotal, \
CASE WHEN C.FarmHdCount = 0 THEN 0 \
ELSE ROUND((C.FarmWtNetD/C.FarmHdCountD),2) END AS PesoPromGranja, \
(SELECT SUM(abs(Quantity)) AS Expr1 \
FROM {database_name}.si_brimentityinventory AS DI \
WHERE (ProteinEntitiesIRN = A.ProteinEntitiesIRNS) AND (SourceCode = 'BrimFieldTrans') AND (xDate between A.FirstDateSold AND A.LastDateSold)) AS SacaMort \
FROM {database_name}.SACAPAVO01 A \
LEFT JOIN {database_name}.Saca1 C ON A.ProteinEntitiesIRNS = C.ProteinEntitiesIRND and A.TransactionIRN = C.irnd \
LEFT JOIN {database_name}.si_mvbrimentities D ON D.IRN = C.ProteinEntitiesIRN \
LEFT JOIN {database_name}.si_mvbrimentities E ON C.ComplexEntityNoGalpon = E.ComplexEntityNo and E.GRN = 'H'")
df_Saca2Temp = df_Saca2Temp1.union(df_Saca2Temp2)
#df_Saca2Temp.createOrReplaceTempView("Saca2")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca2"
}
df_Saca2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca2")
print('carga Saca2')
df_EdadTemp = spark.sql(f"SELECT a.ProteinEntitiesIRN,a.ProteinEntitiesIRNS,a.ComplexEntityNo, cast(a.EventDate as timestamp) as EventDate \
,a.EdadDiaSaca, SUM(a.FarmHdCountD) AS AvesGranja \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM(b.EdadDiaSaca*b.FarmHdCountD*1.0) / SUM(b.FarmHdCountD) ) END AS EdadCorral \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM(c.EdadDiaSaca*c.FarmHdCountD*1.0) / SUM(c.FarmHdCountD) ) END AS EdadGalpon \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM(d.EdadDiaSaca*d.FarmHdCountD*1.0) / SUM(d.FarmHdCountD) ) END AS EdadLote \
FROM {database_name}.Saca2 A left join {database_name}.Saca2 b on B.ComplexEntityNo = A.ComplexEntityNo \
left join {database_name}.Saca2 c on  c.FarmNo = A.FarmNo and c.EntityNo = a.EntityNo and c.HouseNo = a.HouseNo \
left join {database_name}.Saca2 d on  d.FarmNo = A.FarmNo and d.EntityNo = a.EntityNo \
WHERE isnotnull(a.FarmHdCountD) = true \
GROUP BY a.ProteinEntitiesIRN,a.ProteinEntitiesIRNS,a.ComplexEntityNo, cast(a.EventDate as timestamp),cast(a.xdate as timestamp),a.EdadDiaSaca, a.FarmNo,a.EntityNo,a.HouseNo")
#df_EdadTemp.createOrReplaceTempView("EdadTemp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadTemp"
}
df_EdadTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadTemp")
print('carga EdadTemp')

#Tabla temporal para ponderar xdate y cantidad saca por corral, galpon y lote
df_EdadxDateTemp = spark.sql(f"SELECT a.ProteinEntitiesIRN,a.ProteinEntitiesIRNS,a.ComplexEntityNo, cast(a.EventDate as timestamp) as EventDate, \
cast(a.xdate as timestamp) as xdate,a.EdadDiaSaca,a.EdadDiaSacaxDate,avg((a.NumXDate+1)*1.0)NumXDate,SUM(a.FarmHdCountD) AS AvesGranja \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM((b.NumXDate+1)*b.FarmHdCountD*1.0) / SUM(b.FarmHdCountD) ) END AS EdadNumCorral \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM((c.NumXDate+1)*c.FarmHdCountD*1.0) / SUM(c.FarmHdCountD) ) END AS EdadNumGalpon \
,CASE WHEN SUM(a.FarmHdCountD) = 0 THEN 0 ELSE ( SUM((d.NumXDate+1)*d.FarmHdCountD*1.0) / SUM(d.FarmHdCountD) ) END AS EdadNumLote \
FROM {database_name}.Saca2 A left join {database_name}.Saca2 b on B.ComplexEntityNo = A.ComplexEntityNo \
left join {database_name}.Saca2 c on  c.FarmNo = A.FarmNo and c.EntityNo = a.EntityNo and c.HouseNo = a.HouseNo \
left join {database_name}.Saca2 d on  d.FarmNo = A.FarmNo and d.EntityNo = a.EntityNo \
WHERE isnotnull(a.FarmHdCountD) = true \
GROUP BY a.ProteinEntitiesIRN,a.ProteinEntitiesIRNS,a.ComplexEntityNo,cast(a.EventDate as timestamp),cast(a.xdate as timestamp),a.EdadDiaSaca,a.EdadDiaSacaxdate,a.FarmNo,a.EntityNo,a.HouseNo")
#df_EdadxDateTemp.createOrReplaceTempView("EdadxDateTemp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadxDateTemp"
}
df_EdadxDateTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadxDateTemp")
print('carga EdadxDateTemp')

#Tabla temporal para insertar la cantidad y fecha de alojamiento
df_alojamientoTemp = spark.sql(f"select ComplexentityNo, \
substring(complexentityno,1,(length(complexentityno)-3)) ComplexEntityNoGalpon, \
substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNoLote, \
pk_tiempo,unix_timestamp(cast(fecha as timestamp))+2 NumIdfecha,Inventario \
from {database_name}.ft_ingreso")
#df_alojamientoTemp.createOrReplaceTempView("alojamientoTemp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamientoTemp2"
}
df_alojamientoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.alojamientoTemp2")
print('carga alojamientoTemp2')
# Tabla temporal para ponderar la fecha y cantidad de alojamiento
df_alojamiento2Temp = spark.sql(f"select a.ComplexentityNo,a.pk_tiempo,SUM(a.Inventario) Inventario \
,CASE WHEN SUM(a.Inventario) = 0 THEN 0 ELSE ( SUM(b.NumIdfecha*b.Inventario*1.0) / NULLIF(SUM(b.Inventario),0) ) END AS AlojNumCorral \
,CASE WHEN SUM(a.Inventario) = 0 THEN 0 ELSE ( SUM(c.NumIdfecha*c.Inventario*1.0) / NULLIF(SUM(c.Inventario),0) ) END AS AlojNumGalpon \
,CASE WHEN SUM(a.Inventario) = 0 THEN 0 ELSE ( SUM(d.NumIdfecha*d.Inventario*1.0) / NULLIF(SUM(d.Inventario),0) ) END AS AlojNumLote \
from {database_name}.alojamientoTemp2 A left join {database_name}.alojamientoTemp2 b on B.ComplexEntityNo = A.ComplexEntityNo \
left join {database_name}.alojamientoTemp2 c on c.ComplexEntityNoGalpon = A.ComplexEntityNoGalpon \
left join {database_name}.alojamientoTemp2 d on d.ComplexEntityNoLote = A.ComplexEntityNoLote \
group by a.ComplexentityNo,a.ComplexEntityNoGalpon,a.ComplexEntityNoLote,a.pk_tiempo")
#df_alojamiento2Temp.createOrReplaceTempView("alojamiento2Temp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamiento2Temp"
}
df_alojamiento2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.alojamiento2Temp")
print('carga alojamiento2Temp')
 # Tabla donde se resta el ponderado de saca y alojamiento para hablar la Edad(Granja)
df_EdadLiquidacionTemp = spark.sql(f"select A.ComplexEntityNo,MAX(EdadNumCorral)EdadNumCorral,MAX(AlojNumCorral)AlojNumCorral, \
MAX(EdadNumCorral)-MAX(AlojNumCorral) EdadCorral,MAX(EdadNumGalpon)EdadNumGalpon,MAX(AlojNumGalpon)AlojNumGalpon, \
MAX(EdadNumGalpon)-MAX(AlojNumGalpon) EdadGalpon,MAX(EdadNumLote)EdadNumLote,MAX(AlojNumLote)AlojNumLote, \
MAX(EdadNumLote)-MAX(AlojNumLote) EdadLote \
from {database_name}.EdadxdateTemp A \
left join (select ComplexEntityNo, MAX(AlojNumCorral) AlojNumCorral, MAX(AlojNumGalpon) AlojNumGalpon, MAX(AlojNumLote)AlojNumLote from {database_name}.alojamiento2Temp group by ComplexEntityNo ) B on A.ComplexEntityNo = B.ComplexEntityNo \
group by A.ComplexEntityNo")
#df_EdadLiquidacionTemp.createOrReplaceTempView("EdadLiquidacionTemp")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadLiquidacionTemp"
}
df_EdadLiquidacionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadLiquidacionTemp")
print('carga EdadLiquidacionTemp')
df_SacaTemp = spark.sql(f"SELECT SA.IRN,SA.ProteinDriversIRN,SA.ProteinEntitiesIRN,SA.ProteinVehiclesIRN,SA.PostTransactionId \
,SA.ProteinFacilityPlantsIRN,SA.IRND,SA.ProteinEntitiesIRND,SA.ProteinFarmsIRN,SA.ProteinGrowoutCodesIRN,SA.ProteinCostCentersIRN_Farm \
,SA.ProteinCostCentersIRN,SA.ProteinProductsAnimalsIRN_Ovr,SA.PmtsProcRecvTransIRN,SA.ProteinEntitiesIRNS,SA.ProteinFarmsIRNS \
,SA.ProteinCostCentersIRNS,SA.TransactionIRN,LT.pk_tiempo,LT.fecha,1 as pk_empresa \
,nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) AS pk_division \
,nvl(LP.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) AS pk_zona \
,nvl(LP.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) AS pk_subzona \
,nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) AS pk_plantel \
,nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) AS pk_lote \
,nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) AS pk_galpon \
,nvl(LS.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo \
,nvl(LST.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard \
,nvl(LPRS.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_productoS \
,nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(SA.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(LES.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador \
,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,PLA.pk_planta,VEH.pk_vehiculo,SA.ComplexEntityNo \
,cast(cast(SA.xdate as timestamp) as date) as FechaTransaccion \
,year(cast(cast(SA.xdate as timestamp) as date)) AS AnhoTransaccion \
,month(cast(cast(SA.xdate as timestamp) as date)) AS MesTransaccion \
,cast(cast(SA.EventDate          as timestamp) as date) AS FechaSaca \
,cast(cast(SA.FirstHatchDate     as timestamp) as date) AS FechaNacimiento \
,cast(cast(SA.FirstDateSold      as timestamp) as date) AS FechaInicioSaca \
,cast(cast(SA.LastDateSold       as timestamp) as date) AS FechaFinSaca \
,cast(cast(SA.FirstHatchDateLote as timestamp) as date) AS FechaNacimientoLote \
,cast(cast(SA.FirstDateSoldLote  as timestamp) as date) AS FechaInicioSacaLote \
,cast(cast(SA.LastDateSoldLote   as timestamp) as date) AS FechaFinSacaLote \
,SA.EdadDiaSaca,SA.EdadInicioSaca,SA.EdadFinSaca,SA.DiasSaca,SA.EdadDiaSacaLote,SA.EdadInicioSacaLote \
,SA.EdadFinSacaLote,SA.DiasSacaLote,SA.DiasSacaGalpon,LPR.nproducto AS ProductoInicial,LPRS.grupoproductosalida AS TipoProductoSalida \
,SA.FarmWtRefNo AS GuiaRemision,SA.TipoGranja,RTRIM(SA.UserId) AS usuario \
,CASE WHEN LEN(SA.Loadno) = 0 THEN '-' ELSE RTRIM(SA.Loadno) END AS carga \
,SA.FarmHdCountD AS AvesGranja,SA.FarmHdCountDAcum AS AvesGranjaAcum,SA.FarmHdCountDTot AS AvesGranjaTot,SA.FarmWtNetD AS PesoGranja \
,SA.FarmWtNetDAcum AS PesoGranjaAcum,SA.FarmHdCount AS TotalAvesGranja,SA.FarmWtNet AS TotalPesoGranja,SA.PesoPromGranja \
,SA.FarmHdCountTotal AS AvesPlantel,SA.SacaDia,SA.SacaMort,D.EdadCorral,D.EdadGalpon,D.EdadLote,E.EdadCorral EdadCorralLiq \
,E.EdadGalpon EdadGalponLiq,E.EdadLote EdadLoteLiq,CAT.Categoria \
,CASE WHEN PLA.pk_planta IN (1,7,20) THEN 2 ELSE 1 END FlagAtipico \
,(select count(distinct x.EventDate) from {database_name}.Saca2 x where x.ComplexEntityNoGalpon = SA.ComplexEntityNoGalpon) DiasSacaEfectivoGalpon \
from {database_name}.Saca2 SA \
LEFT JOIN {database_name}.si_proteincostcenters PCC ON SA.ProteinCostCentersIRN = PCC.IRN \
LEFT JOIN {database_name}.si_proteinentities PE ON PE.IRN = SA.ProteinEntitiesIRND \
LEFT JOIN {database_name}.si_proteinstandardversions PSV ON PSV.IRN = PE.ProteinStandardVersionsIRN \
left join {database_name}.si_proteinproductsanimals PPA ON SA.ProteinProductsAnimalsIRN_Ovr = PPA.IRN \
left join {database_name}.si_proteinproducts PP on PPA.ProteinProductsIRN = PP.IRN \
LEFT JOIN {database_name}.lk_tiempo LT ON LT.fecha = cast(cast(SA.EventDate as timestamp) as date) \
LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(SA.ProteinFarmsIRN AS VARCHAR (50)) \
LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(SA.EntityNo) and ll.activeflag in (0,1) \
LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and lg.activeflag in (false,true) \
LEFT JOIN {database_name}.lk_division LD ON LD.IRN = CAST(PCC.ProteinDivisionsIRN as varchar(50)) \
LEFT JOIN {database_name}.lk_sexo LS ON LS.csexo = RTRIM(SA.PenNo) \
LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50)) \
LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50)) \
LEFT JOIN {database_name}.lk_tipoproducto TP ON TP.ntipoproducto=LPR.grupoproducto \
LEFT JOIN {database_name}.lk_producto LPRS ON LPRS.cproducto = PP.ProductNo \
LEFT JOIN {database_name}.lk_tipoproducto TPS ON TPS.ntipoproducto=LPRS.grupoproducto \
LEFT JOIN {database_name}.lk_estado LES ON LES.cestado=PE.Status \
LEFT JOIN {database_name}.lk_planta PLA ON CAST(SA.ProteinCostCentersIRN AS varchar(50)) = PLA.ProteinCostCentersIRN \
LEFT JOIN {database_name}.lk_vehiculo VEH ON CAST(SA.ProteinVehiclesIRN AS VARCHAR(50)) = CAST(VEH.IRN as VARCHAR(50)) \
LEFT JOIN {database_name}.si_mvbrimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LP.IRN \
LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
LEFT JOIN {database_name}.Categoria CAT ON LP.pk_plantel = CAT.pk_plantel and LL.pk_lote = CAT.pk_lote and LG.pk_galpon = CAT.pk_galpon \
LEFT JOIN (select ComplexEntityNo,EdadCorral,EdadGalpon,EdadLote \
from edadTemp \
group by ComplexEntityNo,EdadCorral,EdadGalpon,EdadLote \
) D on SA.ComplexEntityNo = D.ComplexEntityNo and isnotnull(SA.FarmHdCountD) =true \
LEFT JOIN {database_name}.EdadLiquidacionTemp E ON E.ComplexEntityNo = SA.ComplexEntityNo")
#df_SacaTemp.createOrReplaceTempView("Saca")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca"
}
df_SacaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca")
print('carga Saca')
#CONVERT(VARCHAR(8),idfecha, 112) >= CONVERT(VARCHAR(8),DATEADD(MM, DATEDIFF(MM,0,DATEADD(MONTH,-4,GETDATE())), 0),112)
df_ft_ventas_CDTemp = spark.sql(f"SELECT a.pk_tiempo,a.fecha,a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote, \
a.pk_galpon,a.pk_sexo,a.pk_standard,a.pk_productoS,a.pk_tipoproducto,a.pk_especie,a.pk_estado,a.pk_administrador,a.pk_proveedor, \
a.pk_planta,a.pk_vehiculo,a.ComplexEntityNo,a.FechaTransaccion,a.AnhoTransaccion,a.MesTransaccion,a.FechaSaca,a.FechaNacimiento, \
a.FechaInicioSaca,a.FechaFinSaca,a.FechaNacimientoLote,a.FechaInicioSacaLote,a.FechaFinSacaLote,a.EdadDiaSaca,a.EdadInicioSaca, \
a.EdadFinSaca,a.DiasSaca,a.EdadDiaSacaLote,a.EdadInicioSacaLote,a.EdadFinSacaLote,a.DiasSacaLote,a.ProductoInicial,a.TipoProductoSalida, \
a.GuiaRemision,a.TipoGranja,a.usuario,a.carga, a.AvesGranja,a.AvesGranjaAcum,a.PesoGranja,a.PesoGranjaAcum,a.TotalAvesGranja,a.TotalPesoGranja, \
a.PesoPromGranja,a.AvesPlantel,b.CantAloj,c.PobInicial,c.MortDia,c.PorcMortDiaAcum AS PorcMort,a.SacaMort as MortSaca, \
(a.SacaMort / nullif(c.PobInicial,0))*100 AS PorcMortSaca, \
(c.PobInicial - a.AvesGranjaTot - c.MortDia) as Saldo, \
a.EdadCorralLiq AS EdadGranja,a.EdadGalponLiq EdadGalpon,a.EdadLoteLiq EdadLote,b.DiasAloj,1 as FlagArtificio,a.categoria,a.FlagAtipico,a.DiasSacaGalpon, \
a.DiasSacaEfectivoGalpon,B.TipoOrigen \
FROM {database_name}.Saca A \
left join ( \
select \
max(pk_tiempo) as pk_tiempo, \
max(fecha) as fecha, \
max(diasaloj) as DiasAloj, \
ComplexEntityNo, \
sum(CantAloj) as CantAloj, \
TipoOrigen \
from {database_name}.ft_alojamiento \
group by ComplexEntityNo,TipoOrigen \
) B on A.ComplexEntityNo = b.ComplexEntityNo \
left join ( \
select \
ComplexEntityNo, \
avg(PobInicial) as PobInicial, \
sum(MortDia) as MortDia, \
max(PorcMortDiaAcum) as PorcMortDiaAcum \
from {database_name}.ft_mortalidad_Diario \
where flagartificio = 1 \
group by ComplexEntityNo,categoria \
) C on A.ComplexEntityNo = C.ComplexEntityNo ")
print('df_ft_ventas_CDTemp')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target31)  
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")



try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ventas_CD cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ventas_CD")



if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_ventas_CDTemp.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_CDTemporal")
    
    
    #schema = existing_data.schema
    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_CD"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_CD")
            
    print(f"agrega registros nuevos a la tabla ft_ventas_CD : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ventas_CD : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_temporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ventas_CDTemporal')
    print(f"Tabla ft_ventas_CDTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_CD"
    }
    df_ft_ventas_CDTemp.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_CD")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()