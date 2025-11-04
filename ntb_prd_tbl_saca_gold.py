from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import date_format, current_date, add_months,expr, col, lit
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Definir el nombre del trabajo
JOB_NAME = "ntb_prd_tbl_saca_gold"

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
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
database_name = "default"

file_name_target31 = f"{bucket_name_prdmtech}ft_ventas_cd/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"
print('cargando ruta')
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
AND (date_format(cast(cast(PPT.EventDate as timestamp) as date),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
AND PPTD.FarmType = 7 and PPTD.SpeciesType = 2")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaPavo0"
}
df_SacaPavo0Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaPavo0")
print('carga SacaPavo0', df_SacaPavo0Temp.count())
df_DestinationComplexEntityNoTemp1 = spark.sql(f"SELECT ProteinEntitiesIRN,ProteinFacilityPlantsIRN,ProteinEntitiesIRND, \
ProteinFarmsIRN,ProteinGrowoutCodesIRN,ProteinCostCentersIRN_Farm,ProteinCostCentersIRN,ComplexEntityNo,ComplexEntityNoGalpon, \
PlantNo,PlantName,EntityNo,FarmNo,HouseNo,PenNo,HouseNoPenNo \
from {database_name}.SacaPavo0 \
where ComplexEntityNo in (select DestinationComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where DEstinationComplexEntityNo like 'v%') \
group by ProteinEntitiesIRN,ProteinFacilityPlantsIRN,ProteinEntitiesIRND,ProteinFarmsIRN,ProteinGrowoutCodesIRN,ProteinCostCentersIRN_Farm, \
ProteinCostCentersIRN,ComplexEntityNo,ComplexEntityNoGalpon,PlantNo,PlantName,EntityNo,FarmNo,HouseNo,PenNo,HouseNoPenNo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DestinationComplexEntityNoTemp1"
}
df_DestinationComplexEntityNoTemp1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DestinationComplexEntityNoTemp1")
print('carga DestinationComplexEntityNoTemp1' , df_DestinationComplexEntityNoTemp1.count())
df_SacaPavo0_upd = spark.sql(f"""
SELECT
    A.CreationDate,
    A.LastModDate,
    A.xDate,
    A.EventDate,
    A.IRN,
    A.ProteinDriversIRN,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinEntitiesIRN
        ELSE A.ProteinEntitiesIRN
    END ProteinEntitiesIRN,
    A.ProteinVehiclesIRN,
    A.PostTransactionId,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinFacilityPlantsIRN
        ELSE A.ProteinFacilityPlantsIRN
    END ProteinFacilityPlantsIRN,
    A.IRND,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinEntitiesIRND
        ELSE A.ProteinEntitiesIRND
    END ProteinEntitiesIRND,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinFarmsIRN
        ELSE A.ProteinFarmsIRN
    END ProteinFarmsIRN,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinGrowoutCodesIRN
        ELSE A.ProteinGrowoutCodesIRN
    END ProteinGrowoutCodesIRN,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinCostCentersIRN_Farm
        ELSE A.ProteinCostCentersIRN_Farm
    END ProteinCostCentersIRN_Farm,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ProteinCostCentersIRN
        ELSE A.ProteinCostCentersIRN
    END ProteinCostCentersIRN,
    A.ProteinProductsAnimalsIRN_Ovr,
    A.PmtsProcRecvTransIRN,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.complexentityno
        ELSE A.complexentityno
    END complexentityno,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.ComplexEntityNoGalpon
        ELSE A.ComplexEntityNoGalpon
    END ComplexEntityNoGalpon,
    A.U_GRemision,
    A.LoadNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.PlantNo
        ELSE A.PlantNo
    END PlantNo,
    A.DriverNo,
    A.VoidFlag,
    A.PostStatus,
    A.VehicleNo,
    A.ProductNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.PlantName
        ELSE A.PlantName
    END PlantName,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.EntityNo
        ELSE A.EntityNo
    END EntityNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.FarmNo
        ELSE A.FarmNo
    END FarmNo,
    A.BreedNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.HouseNo
        ELSE A.HouseNo
    END HouseNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.PenNo
        ELSE A.PenNo
    END PenNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ProteinEntitiesIRN IS NOT NULL THEN B.HouseNoPenNo
        ELSE A.HouseNoPenNo
    END HouseNoPenNo,
    A.UserId,
    A.FarmHdCount,
    A.FarmWtGross,
    A.FarmWtNet,
    A.FarmWtRefNo,
    A.FarmWtTare,
    A.FarmHdCountD,
    A.FarmWtNetD,
    A.FarmWtGrossD,
    A.FarmWtTareD,
    A.FlagTransfPavos,
    A.SourceComplexEntityNo,
    A.DestinationComplexEntityNo
FROM
    {database_name}.SacaPavo0 A
LEFT JOIN
    {database_name}.DestinationComplexEntityNoTemp1 B ON A.DestinationComplexEntityNo = B.ComplexEntityNo
LEFT JOIN
    (SELECT DISTINCT SourceComplexEntityNo FROM {database_name}.si_mvpmtstransferdestdetails WHERE SourceComplexEntityNo LIKE 'v%') AS EXISTS_V
    ON A.ComplexEntityNo = EXISTS_V.SourceComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaPavo0_upd"
}
df_SacaPavo0_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaPavo0_upd")
print('carga SacaPavo0_upd', df_SacaPavo0_upd.count())
df_Saca1Temp1 = spark.sql(f"""
WITH FarmHdCountTotals AS (
    SELECT ComplexEntityNo,SUM(FarmHdCount) AS FarmHdCountDTot
    FROM {database_name}.si_mvpmtsprocrecvtranshousedetail
    WHERE PostTransactionId IS NOT NULL
    GROUP BY ComplexEntityNo
),
FarmHdCountAccumulated AS (select ComplexEntityNo,EventDate, max(FarmHdCountDAcum) FarmHdCountDAcum from (
    SELECT
        ComplexEntityNo,
        EventDate,
        SUM(FarmHdCount) OVER (PARTITION BY ComplexEntityNo ORDER BY EventDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FarmHdCountDAcum
    FROM
        {database_name}.si_mvpmtsprocrecvtranshousedetail
    WHERE
        PostTransactionId IS NOT NULL ) a group by ComplexEntityNo,EventDate
),
FarmWtNetAccumulated AS (select ComplexEntityNo,EventDate, max(FarmWtNetDAcum) FarmWtNetDAcum from (
    SELECT
        ComplexEntityNo,
        EventDate,
        SUM(FarmWtNet) OVER (PARTITION BY ComplexEntityNo ORDER BY EventDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FarmWtNetDAcum
    FROM
        {database_name}.si_mvpmtsprocrecvtranshousedetail
    WHERE
        PostTransactionId IS NOT NULL ) a group by ComplexEntityNo,EventDate
)
SELECT
    PPT.CreationDate,
    PPT.LastModDate,
    PPT.xDate,
    PPT.EventDate,
    PPT.IRN,
    PPT.ProteinDriversIRN,
    PPT.ProteinEntitiesIRN,
    PPT.ProteinVehiclesIRN,
    PPT.PostTransactionId,
    PPT.ProteinFacilityPlantsIRN,
    PPTD.IRN AS IRND,
    PPTD.ProteinEntitiesIRN AS ProteinEntitiesIRND,
    PPTD.ProteinFarmsIRN,
    PPTD.ProteinGrowoutCodesIRN,
    PPTD.ProteinCostCentersIRN_Farm,
    PPTD.ProteinCostCentersIRN,
    AX.pk_especie,
    PPTD.ProteinProductsAnimalsIRN_Ovr,
    PPTD.PmtsProcRecvTransIRN,
    PPTD.complexentityno,
    substring(PPTD.complexentityno, 1, (length(PPTD.complexentityno) - 3)) AS ComplexEntityNoGalpon,
    PPT.U_GRemision,
    PPT.LoadNo,
    PPTD.PlantNo,
    PPTD.DriverNo,
    PPTD.VoidFlag,
    PPTD.PostStatus,
    PPTD.VehicleNo,
    PPTD.ProductNo,
    PPTD.PlantName,
    PPTD.EntityNo,
    PPTD.FarmNo,
    PPTD.BreedNo,
    PPTD.HouseNo,
    PPTD.PenNo,
    PPTD.HouseNoPenNo,
    PPT.UserId,
    PPT.FarmHdCount,
    PPT.FarmWtGross,
    PPT.FarmWtNet,
    PPT.FarmWtRefNo,
    PPT.FarmWtTare,
    PPTD.FarmHdCount AS FarmHdCountD,
    PPTD.FarmWtNet AS FarmWtNetD,
    PPTD.FarmWtGross AS FarmWtGrossD,
    PPTD.FarmWtTare AS FarmWtTareD,
    FHT.FarmHdCountDTot,
    FHA.FarmHdCountDAcum,
    FWNA.FarmWtNetDAcum
FROM
    {database_name}.si_pmtsprocrecvtrans PPT
LEFT JOIN
    {database_name}.si_mvpmtsprocrecvtranshousedetail PPTD ON PPT.IRN = PPTD.PmtsProcRecvTransIRN
LEFT JOIN
    (SELECT * FROM (
        SELECT
            ComplexEntityNo,
            pk_especie,
            ROW_NUMBER() OVER(PARTITION BY ComplexEntityNo ORDER BY pk_especie) AS Rn
        FROM
            {database_name}.ft_alojamiento A
        WHERE
            CantAlojXRaza = (
                SELECT MAX(CantAlojXRaza)
                FROM {database_name}.ft_alojamiento B
                WHERE B.ComplexEntityNo = A.ComplexEntityNo
            )
        GROUP BY
            ComplexEntityNo, pk_especie
    ) B
    WHERE Rn = 1) AX ON AX.ComplexEntityNo = PPTD.ComplexEntityNo
LEFT JOIN
    FarmHdCountTotals FHT ON PPTD.ComplexEntityNo = FHT.ComplexEntityNo
LEFT JOIN
    FarmHdCountAccumulated FHA ON PPTD.ComplexEntityNo = FHA.ComplexEntityNo AND PPTD.EventDate = FHA.EventDate
LEFT JOIN
    FarmWtNetAccumulated FWNA ON PPTD.ComplexEntityNo = FWNA.ComplexEntityNo AND PPTD.EventDate = FWNA.EventDate
WHERE
    PPTD.PostTransactionId IS NOT NULL
    AND FarmType = 1
    AND SpeciesType = 1
    AND date_format(CAST(PPT.EventDate AS DATE), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'), -9), 'yyyyMM')
""")
print('carga Saca1Temp1', df_Saca1Temp1.count())
df_Saca1Temp2 = spark.sql(f"""
WITH FarmHdCountDTotals AS (
    SELECT
        ComplexEntityNo,
        SUM(FarmHdCountD) AS FarmHdCountDTot
    FROM
        {database_name}.SacaPavo0
    WHERE
        PostTransactionId IS NOT NULL
    GROUP BY
        ComplexEntityNo
),
FarmHdCountDAccumulated AS (select ComplexEntityNo,EventDate, max(FarmHdCountDAcum) FarmHdCountDAcum from (
    SELECT
        ComplexEntityNo,
        EventDate,
        SUM(FarmHdCountD) OVER (PARTITION BY ComplexEntityNo ORDER BY EventDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FarmHdCountDAcum
    FROM
        {database_name}.SacaPavo0
    WHERE
        PostTransactionId IS NOT NULL ) a group by ComplexEntityNo,EventDate
),
FarmWtNetDAccumulated AS (select ComplexEntityNo,EventDate, max(FarmWtNetDAcum) FarmWtNetDAcum from (
    SELECT
        ComplexEntityNo,
        EventDate,
        SUM(FarmWtNetD) OVER (PARTITION BY ComplexEntityNo ORDER BY EventDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FarmWtNetDAcum
    FROM
        {database_name}.SacaPavo0
    WHERE
        PostTransactionId IS NOT NULL ) a group by ComplexEntityNo,EventDate
)
SELECT
    SP.CreationDate,
    SP.LastModDate,
    SP.xDate,
    SP.EventDate,
    SP.IRN,
    SP.ProteinDriversIRN,
    SP.ProteinEntitiesIRN,
    SP.ProteinVehiclesIRN,
    SP.PostTransactionId,
    SP.ProteinFacilityPlantsIRN,
    SP.IRND,
    SP.ProteinEntitiesIRND,
    SP.ProteinFarmsIRN,
    SP.ProteinGrowoutCodesIRN,
    SP.ProteinCostCentersIRN_Farm,
    SP.ProteinCostCentersIRN,
    AX.pk_especie,
    SP.ProteinProductsAnimalsIRN_Ovr,
    SP.PmtsProcRecvTransIRN,
    SP.complexentityno,
    substring(SP.complexentityno, 1, (length(SP.complexentityno) - 3)) AS ComplexEntityNoGalpon,
    SP.U_GRemision,
    SP.LoadNo,
    SP.PlantNo,
    SP.DriverNo,
    SP.VoidFlag,
    SP.PostStatus,
    SP.VehicleNo,
    SP.ProductNo,
    SP.PlantName,
    SP.EntityNo,
    SP.FarmNo,
    SP.BreedNo,
    SP.HouseNo,
    SP.PenNo,
    SP.HouseNoPenNo,
    SP.UserId,
    SP.FarmHdCount,
    SP.FarmWtGross,
    SP.FarmWtNet,
    SP.FarmWtRefNo,
    SP.FarmWtTare,
    SP.FarmHdCount AS FarmHdCountD,
    SP.FarmWtNet AS FarmWtNetD,
    SP.FarmWtGross AS FarmWtGrossD,
    SP.FarmWtTare AS FarmWtTareD,
    FHT.FarmHdCountDTot,
    FHA.FarmHdCountDAcum,
    FWNA.FarmWtNetDAcum
FROM
    {database_name}.SacaPavo0 SP
LEFT JOIN
    (SELECT * FROM(
        SELECT
            ComplexEntityNo,
            pk_especie,
            ROW_NUMBER() OVER(PARTITION BY ComplexEntityNo ORDER BY pk_especie) AS Rn
        FROM
            {database_name}.ft_alojamiento A
        WHERE
            CantAlojXRaza = (
                SELECT MAX(CantAlojXRaza)
                FROM {database_name}.ft_alojamiento B
                WHERE B.ComplexEntityNo = A.ComplexEntityNo
            )
        GROUP BY
            ComplexEntityNo, pk_especie
    ) B
    WHERE Rn = 1) AX ON AX.ComplexEntityNo = SP.ComplexEntityNo
LEFT JOIN
    FarmHdCountDTotals FHT ON SP.ComplexEntityNo = FHT.ComplexEntityNo
LEFT JOIN
    FarmHdCountDAccumulated FHA ON SP.ComplexEntityNo = FHA.ComplexEntityNo AND SP.EventDate = FHA.EventDate
LEFT JOIN
    FarmWtNetDAccumulated FWNA ON SP.ComplexEntityNo = FWNA.ComplexEntityNo AND SP.EventDate = FWNA.EventDate
WHERE
    date_format(CAST(SP.EventDate AS DATE), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'), -9), 'yyyyMM')
""")

print('Saca1Temp2', df_Saca1Temp2.count())
df_Saca1Temp = df_Saca1Temp1.union(df_Saca1Temp2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca1"
}
df_Saca1Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca1")
print('carga Saca1', df_Saca1Temp.count())
df_Saca2Temp1 = spark.sql(f"""
WITH SacaMortData AS (
    SELECT
        a.ProteinEntitiesIRN,
        SUM(abs(Quantity)) AS SacaMort
    FROM
        si_brimentityinventory A
    INNER JOIN si_mvbrimentities B ON A.ProteinEntitiesIRN = B.IRN AND (xDate between B.FirstDateSold AND B.LastDateSold)
    WHERE
        SourceCode = 'BrimFieldTrans'
    GROUP BY
        a.ProteinEntitiesIRN
)
SELECT
    C.IRN,
    C.ProteinDriversIRN,
    C.ProteinEntitiesIRN,
    C.ProteinVehiclesIRN,
    C.PostTransactionId,
    C.ProteinFacilityPlantsIRN,
    C.IRND,
    C.ProteinEntitiesIRND,
    C.ProteinFarmsIRN,
    C.ProteinGrowoutCodesIRN,
    C.ProteinCostCentersIRN_Farm,
    C.ProteinCostCentersIRN,
    C.pk_especie,
    C.ProteinProductsAnimalsIRN_Ovr,
    C.PmtsProcRecvTransIRN,
    A.ProteinEntitiesIRN AS ProteinEntitiesIRNS,
    B.ProteinFarmsIRN AS ProteinFarmsIRNS,
    B.ProteinCostCentersIRN AS ProteinCostCentersIRNS,
    A.TransactionIRN,
    B.ComplexEntityNo,
    C.ComplexEntityNoGalpon,
    A.CreationDate AS CreationDateS,
    C.CreationDate,
    A.EventDate,
    A.xDate,
    datediff(CAST(A.xDate AS DATE), DATE '1900-01-01') + 
    CASE WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 59 AND second(A.xDate) = 59 THEN 0 
    WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 59 AND second(A.xDate) = 00 THEN 0 
    WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 29 AND second(A.xDate) = 00 THEN 0 
    WHEN hour(A.xDate) > 0 OR minute(A.xDate) > 0 OR second(A.xDate) > 0 THEN 1 ELSE 0 END AS NumXDate,
    B.FirstHatchDate,
    B.FirstDateSold,
    B.LastDateSold,
    D.FirstHatchDate AS FirstHatchDateLote,
    D.FirstDateSold AS FirstDateSoldLote,
    D.LastDateSold AS LastDateSoldLote,
    E.FirstHatchDate AS FirstHatchDateGalpon,
    E.FirstDateSold AS FirstDateSoldGalpon,
    E.LastDateSold AS LastDateSoldGalpon,
    B.FarmNo,
    B.EntityNo,
    B.HouseNo,
    B.PenNo,
    C.LoadNo,
    DATEDIFF(cast(A.EventDate as timestamp), cast(B.FirstHatchDate as timestamp)) AS EdadDiaSaca,
    DATEDIFF(cast(A.xDate as timestamp), cast(B.FirstHatchDate as timestamp)) AS EdadDiaSacaxDate,
    DATEDIFF(cast(B.FirstDateSold as timestamp), cast(B.FirstHatchDate as timestamp)) AS EdadInicioSaca,
    DATEDIFF(cast(B.LastDateSold as timestamp), cast(B.FirstHatchDate as timestamp)) AS EdadFinSaca,
    DATEDIFF(cast(B.LastDateSold as timestamp), cast(B.FirstDateSold as timestamp)) AS DiasSaca,
    DATEDIFF(cast(A.EventDate as timestamp), cast(D.FirstHatchDate as timestamp)) AS EdadDiaSacaLote,
    DATEDIFF(cast(D.FirstDateSold as timestamp), cast(D.FirstHatchDate as timestamp)) AS EdadInicioSacaLote,
    DATEDIFF(cast(D.LastDateSold as timestamp), cast(D.FirstHatchDate as timestamp)) AS EdadFinSacaLote,
    DATEDIFF(cast(D.LastDateSold as timestamp), cast(D.FirstDateSold as timestamp)) + 1 AS DiasSacaLote,
    DATEDIFF(cast(E.LastDateSold as timestamp), cast(E.FirstDateSold as timestamp)) + 1 AS DiasSacaGalpon,
    ABS(A.Quantity) AS SacaDia,
    B.GrowoutNo,
    B.GrowoutName,
    B.ProductNo,
    B.ProductName,
    B.BreedNo,
    B.BreedName,
    B.Status,
    C.FarmWtRefNo,
    B.TechSupervisorNo,
    'Brim' AS TipoGranja,
    C.UserId,
    C.FarmHdCountD,
    C.FarmHdCountDAcum,
    C.FarmHdCountDTot,
    C.FarmHdCount,
    C.FarmWtNetD,
    C.FarmWtNetDAcum,
    C.FarmWtNet,
    (SELECT SUM(FarmHdCountd) AS FarmHdCountd
     FROM {database_name}.Saca1 X
     WHERE (X.ProteinEntitiesIRN = C.ProteinEntitiesIRN)) AS FarmHdCountTotal,
    CASE WHEN C.FarmHdCount = 0 THEN 0 ELSE ROUND((C.FarmWtNetD / C.FarmHdCountD), 2) END AS PesoPromGranja,
    SMD.SacaMort
FROM
    {database_name}.si_brimentityinventory A
LEFT JOIN
    {database_name}.si_mvbrimentities B ON A.ProteinEntitiesIRN = B.IRN
LEFT JOIN
    {database_name}.Saca1 C ON A.ProteinEntitiesIRN = C.ProteinEntitiesIRND AND A.TransactionIRN = C.irnd
LEFT JOIN
    {database_name}.si_mvbrimentities D ON D.IRN = C.ProteinEntitiesIRN
LEFT JOIN
    {database_name}.si_mvbrimentities E ON C.ComplexEntityNoGalpon = E.ComplexEntityNo AND E.GRN = 'H'
LEFT JOIN SacaMortData SMD ON A.ProteinEntitiesIRN = SMD.ProteinEntitiesIRN
WHERE
    A.SourceCode = 'PmtsProcRecvTrans'
    AND B.FarmType = 1
    AND B.SpeciesType = 1
    AND date_format(CAST(A.EventDate AS DATE), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'), -9), 'yyyyMM')
""")

print('carga Saca2Temp1', df_Saca2Temp1.count())
df_SACAPAVO01Temp = spark.sql(f"SELECT A.ProteinEntitiesIRN AS ProteinEntitiesIRNS,B.ProteinFarmsIRN AS ProteinFarmsIRNS, \
B.ProteinCostCentersIRN AS ProteinCostCentersIRNS,A.TransactionIRN,B.ComplexEntityNo,A.CreationDate as CreationDateS, \
A.EventDate,A.xDate, datediff(CAST(A.xDate AS DATE), DATE '1900-01-01') + \
CASE WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 59 AND second(A.xDate) = 59 THEN 0 \
WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 59 AND second(A.xDate) = 00 THEN 0 \
WHEN hour(A.xDate) = 11 AND minute(A.xDate) = 29 AND second(A.xDate) = 00 THEN 0 \
WHEN hour(A.xDate) > 0 OR minute(A.xDate) > 0 OR second(A.xDate) > 0 THEN 1 ELSE 0 END AS NumXDate \
, B.FirstHatchDate,B.FirstDateSold,B.LastDateSold, \
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
WHERE a.SourceCode = 'PmtsProcRecvTrans' AND b.FarmType = 7 and b.SpeciesType = 2 \
AND (date_format(cast(cast(A.EventDate as timestamp) as date),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM'))")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SACAPAVO01"
}
df_SACAPAVO01Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SACAPAVO01")
print('carga SACAPAVO01', df_SACAPAVO01Temp.count())
df_DestinationComplexEntityNo01Temp = spark.sql(f"SELECT ProteinEntitiesIRNS,ProteinFarmsIRNS,ProteinCostCentersIRNS,ComplexEntityNo, \
FirstHatchDate,FirstDateSold,LastDateSold,FarmNo,EntityNo,HouseNo,PenNo,EdadInicioSaca,EdadFinSaca,DiasSaca \
from {database_name}.SACAPAVO01 \
where ComplexEntityNo in (select DestinationComplexEntityNo from {database_name}.si_mvpmtstransferdestdetails where DEstinationComplexEntityNo like 'v%') \
group by ProteinEntitiesIRNS,ProteinFarmsIRNS,ProteinCostCentersIRNS,ComplexEntityNo,FirstHatchDate,FirstDateSold,LastDateSold,FarmNo,EntityNo, \
HouseNo,PenNo,EdadInicioSaca,EdadFinSaca,DiasSaca")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/DestinationComplexEntityNo01"
}
df_DestinationComplexEntityNo01Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.DestinationComplexEntityNo01")
print('carga DestinationComplexEntityNo01',df_DestinationComplexEntityNo01Temp.count())
df_SACAPAVO01_upd = spark.sql(f"""
SELECT
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.ProteinEntitiesIRNS
        ELSE A.ProteinEntitiesIRNS
    END ProteinEntitiesIRNS,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.ProteinFarmsIRNS
        ELSE A.ProteinFarmsIRNS
    END ProteinFarmsIRNS,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.ProteinCostCentersIRNS
        ELSE A.ProteinCostCentersIRNS
    END ProteinCostCentersIRNS,
    A.TransactionIRN,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.ComplexEntityNo
        ELSE A.ComplexEntityNo
    END ComplexEntityNo,
    A.CreationDateS,
    A.EventDate,
    A.xDate,
    A.NumXDate,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.FirstHatchDate
        ELSE A.FirstHatchDate
    END FirstHatchDate,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.FirstDateSold
        ELSE A.FirstDateSold
    END FirstDateSold,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.LastDateSold
        ELSE A.LastDateSold
    END LastDateSold,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.FarmNo
        ELSE A.FarmNo
    END FarmNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.EntityNo
        ELSE A.EntityNo
    END EntityNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.HouseNo
        ELSE A.HouseNo
    END HouseNo,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.PenNo
        ELSE A.PenNo
    END PenNo,
    A.EdadDiaSaca,
    A.EdadDiaSacaxDate,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.EdadInicioSaca
        ELSE A.EdadInicioSaca
    END EdadInicioSaca,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.EdadFinSaca
        ELSE A.EdadFinSaca
    END EdadFinSaca,
    CASE
        WHEN EXISTS_V.SourceComplexEntityNo IS NOT NULL AND B.ComplexEntityNo IS NOT NULL THEN B.DiasSaca
        ELSE A.DiasSaca
    END DiasSaca,
    A.SacaDia,
    A.GrowoutNo,
    A.GrowoutName,
    A.ProductNo,
    A.ProductName,
    A.BreedNo,
    A.BreedName,
    A.Status,
    A.TechSupervisorNo,
    A.TipoGranja,
    A.FlagTransfPavos,
    A.SourceComplexEntityNo,
    A.DestinationComplexEntityNo
FROM
    {database_name}.SACAPAVO01 A
LEFT JOIN
    {database_name}.DestinationComplexEntityNo01 B ON A.DestinationComplexEntityNo = B.ComplexEntityNo
LEFT JOIN
    (SELECT DISTINCT SourceComplexEntityNo FROM {database_name}.si_mvpmtstransferdestdetails WHERE SourceComplexEntityNo LIKE 'v%') AS EXISTS_V
    ON A.ComplexEntityNo = EXISTS_V.SourceComplexEntityNo
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SACAPAVO01_upd"
}
df_SACAPAVO01_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SACAPAVO01_upd")
print('carga SACAPAVO01_upd', df_SACAPAVO01_upd.count())
df_Saca2Temp2 = spark.sql(f"""
WITH SacaMortData AS (
SELECT  
        a.ProteinEntitiesIRNS,
        SUM(abs(Quantity)) AS SacaMort
    FROM
        (select FirstDateSold,LastDateSold,ProteinEntitiesIRNS from SACAPAVO01_upd group by FirstDateSold,LastDateSold,ProteinEntitiesIRNS)  A
    INNER JOIN si_brimentityinventory B ON B.ProteinEntitiesIRN = A.ProteinEntitiesIRNS AND (B.xDate between A.FirstDateSold AND A.LastDateSold) and b.SourceCode = 'BrimFieldTrans' 
    GROUP BY
        a.ProteinEntitiesIRNS
)
SELECT
    C.IRN,
    C.ProteinDriversIRN,
    C.ProteinEntitiesIRN,
    C.ProteinVehiclesIRN,
    C.PostTransactionId,
    C.ProteinFacilityPlantsIRN,
    C.IRND,
    C.ProteinEntitiesIRND,
    C.ProteinFarmsIRN,
    C.ProteinGrowoutCodesIRN,
    C.ProteinCostCentersIRN_Farm,
    C.ProteinCostCentersIRN,
    C.pk_especie,
    C.ProteinProductsAnimalsIRN_Ovr,
    C.PmtsProcRecvTransIRN,
    A.ProteinEntitiesIRNS,
    A.ProteinFarmsIRNS,
    A.ProteinCostCentersIRNS,
    A.TransactionIRN,
    A.ComplexEntityNo,
    C.ComplexEntityNoGalpon,
    A.CreationDateS,
    C.CreationDate,
    A.EventDate,
    A.xDate,
    A.NumXDate,
    A.FirstHatchDate,
    A.FirstDateSold,
    A.LastDateSold,
    D.FirstHatchDate AS FirstHatchDateLote,
    D.FirstDateSold AS FirstDateSoldLote,
    D.LastDateSold AS LastDateSoldLote,
    E.FirstHatchDate AS FirstHatchDateGalpon,
    E.FirstDateSold AS FirstDateSoldGalpon,
    E.LastDateSold AS LastDateSoldGalpon,
    A.FarmNo,
    A.EntityNo,
    A.HouseNo,
    A.PenNo,
    C.LoadNo,
    A.EdadDiaSaca,
    A.EdadDiaSacaxDate,
    A.EdadInicioSaca,
    A.EdadFinSaca,
    A.DiasSaca,
    DATEDIFF(CAST(A.EventDate AS TIMESTAMP), CAST(D.FirstHatchDate AS TIMESTAMP)) AS EdadDiaSacaLote,
    DATEDIFF(CAST(D.FirstDateSold AS TIMESTAMP), CAST(D.FirstHatchDate AS TIMESTAMP)) AS EdadInicioSacaLote,
    DATEDIFF(CAST(D.LastDateSold AS TIMESTAMP), CAST(D.FirstHatchDate AS TIMESTAMP)) AS EdadFinSacaLote,
    DATEDIFF(CAST(D.LastDateSold AS TIMESTAMP), CAST(D.FirstDateSold AS TIMESTAMP)) + 1 AS DiasSacaLote,
    DATEDIFF(CAST(E.LastDateSold AS TIMESTAMP), CAST(E.FirstDateSold AS TIMESTAMP)) + 1 AS DiasSacaGalpon,
    A.SacaDia,
    A.GrowoutNo,
    A.GrowoutName,
    A.ProductNo,
    A.ProductName,
    A.BreedNo,
    A.BreedName,
    A.Status,
    C.FarmWtRefNo,
    A.TechSupervisorNo,
    'Brim' AS TipoGranja,
    C.UserId,
    C.FarmHdCountD,
    C.FarmHdCountDAcum,
    C.FarmHdCountDTot,
    C.FarmHdCount,
    C.FarmWtNetD,
    C.FarmWtNetDAcum,
    C.FarmWtNet,
    (SELECT SUM(FarmHdCountd) AS FarmHdCountd
     FROM {database_name}.Saca1 X
     WHERE (X.ProteinEntitiesIRN = C.ProteinEntitiesIRN)) AS FarmHdCountTotal,
    CASE
        WHEN C.FarmHdCount = 0 THEN 0
        ELSE ROUND((C.FarmWtNetD / C.FarmHdCountD), 2)
    END AS PesoPromGranja,
    SMD.SacaMort
FROM
    {database_name}.SACAPAVO01_upd A
LEFT JOIN
    {database_name}.Saca1 C ON A.ProteinEntitiesIRNS = C.ProteinEntitiesIRND AND A.TransactionIRN = C.irnd
LEFT JOIN
    {database_name}.si_mvbrimentities D ON D.IRN = C.ProteinEntitiesIRN
LEFT JOIN
    {database_name}.si_mvbrimentities E ON C.ComplexEntityNoGalpon = E.ComplexEntityNo AND E.GRN = 'H'
LEFT JOIN
    SacaMortData SMD ON A.ProteinEntitiesIRNS = SMD.ProteinEntitiesIRNS
WHERE
    (date_format(CAST(A.EventDate AS TIMESTAMP), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'), -9), 'yyyyMM'))
""")

df_Saca2Temp = df_Saca2Temp1.union(df_Saca2Temp2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca2"
}
df_Saca2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca2")
print('carga Saca2', df_Saca2Temp.count())
df_EdadTemp = spark.sql(f"""
SELECT 
    A.ProteinEntitiesIRN,
    A.ProteinEntitiesIRNS,
    A.ComplexEntityNo,
    A.EventDate,
    A.EdadDiaSaca,
    A.AvesGranja,
    C.EdadCorral,
    G.EdadGalpon,
    L.EdadLote
FROM (
    SELECT 
        ProteinEntitiesIRN,
        ProteinEntitiesIRNS,
        ComplexEntityNo,
        CAST(EventDate AS timestamp) AS EventDate,
        EdadDiaSaca,
        FarmNo,
        EntityNo,
        HouseNo,
        SUM(FarmHdCountD) AS AvesGranja
    FROM {database_name}.Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY ProteinEntitiesIRN, ProteinEntitiesIRNS, ComplexEntityNo, 
             CAST(EventDate AS timestamp), EdadDiaSaca, FarmNo, EntityNo, HouseNo
) A
LEFT JOIN (
    SELECT 
        ComplexEntityNo, 
        SUM(EdadDiaSaca * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadCorral
    FROM {database_name}.Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY ComplexEntityNo
) C ON A.ComplexEntityNo = C.ComplexEntityNo
LEFT JOIN (
    SELECT 
        FarmNo, EntityNo, HouseNo,
        SUM(EdadDiaSaca * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadGalpon
    FROM {database_name}.Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY FarmNo, EntityNo, HouseNo
) G ON A.FarmNo = G.FarmNo AND A.EntityNo = G.EntityNo AND A.HouseNo = G.HouseNo
LEFT JOIN (
    SELECT 
        FarmNo, EntityNo,
        SUM(EdadDiaSaca * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadLote
    FROM {database_name}.Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY FarmNo, EntityNo
) L ON A.FarmNo = L.FarmNo AND A.EntityNo = L.EntityNo

""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadTemp"
}
df_EdadTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadTemp")
print('carga EdadTemp', df_EdadTemp.count())
#Tabla temporal para ponderar xdate y cantidad saca por corral, galpon y lote
df_EdadxDateTemp = spark.sql(f"""
WITH 
EdadNumCorral AS (
    SELECT 
        ComplexEntityNo,
        SUM((NumXDate + 1) * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadNumCorral
    FROM Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY ComplexEntityNo
),
EdadNumGalpon AS (
    SELECT 
        FarmNo, EntityNo, HouseNo,
        SUM((NumXDate + 1) * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadNumGalpon
    FROM Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY FarmNo, EntityNo, HouseNo
),
EdadNumLote AS (
    SELECT 
        FarmNo, EntityNo,
        SUM((NumXDate + 1) * FarmHdCountD * 1.0) / SUM(FarmHdCountD) AS EdadNumLote
    FROM Saca2
    WHERE FarmHdCountD IS NOT NULL
    GROUP BY FarmNo, EntityNo
)

SELECT 
    A.ProteinEntitiesIRN,
    A.ProteinEntitiesIRNS,
    A.ComplexEntityNo,
    CAST(A.EventDate AS timestamp) AS EventDate,
    CAST(A.xdate AS timestamp) AS xdate,
    A.EdadDiaSaca,
    A.EdadDiaSacaxDate,
    AVG((A.NumXDate + 1) * 1.0) AS NumXDate,
    SUM(A.FarmHdCountD) AS AvesGranja,
    C.EdadNumCorral,
    G.EdadNumGalpon,
    L.EdadNumLote
FROM Saca2 A
LEFT JOIN EdadNumCorral C ON A.ComplexEntityNo = C.ComplexEntityNo
LEFT JOIN EdadNumGalpon G ON A.FarmNo = G.FarmNo AND A.EntityNo = G.EntityNo AND A.HouseNo = G.HouseNo
LEFT JOIN EdadNumLote L ON A.FarmNo = L.FarmNo AND A.EntityNo = L.EntityNo
WHERE A.FarmHdCountD IS NOT NULL
GROUP BY 
    A.ProteinEntitiesIRN, A.ProteinEntitiesIRNS, A.ComplexEntityNo, 
    CAST(A.EventDate AS timestamp), CAST(A.xdate AS timestamp), 
    A.EdadDiaSaca, A.EdadDiaSacaxDate, A.FarmNo, A.EntityNo, A.HouseNo,
    C.EdadNumCorral,G.EdadNumGalpon,L.EdadNumLote
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadxDateTemp"
}
df_EdadxDateTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadxDateTemp")
print('carga EdadxDateTemp', df_EdadxDateTemp.count())
#Tabla temporal para insertar la cantidad y fecha de alojamiento
df_alojamientoTemp = spark.sql(f"select ComplexentityNo, \
substring(complexentityno,1,(length(complexentityno)-3)) ComplexEntityNoGalpon, \
substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNoLote, \
pk_tiempo,datediff(CAST(descripfecha AS DATE), DATE '1900-01-01') + CASE WHEN hour(descripfecha) > 0 OR minute(descripfecha) > 0 OR second(descripfecha) > 0 THEN 1 ELSE 0 END +2 NumIdfecha,Inventario \
from {database_name}.ft_ingreso")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamientoTemp"
}
df_alojamientoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.alojamientoTemp")
print('carga alojamientoTemp', df_alojamientoTemp.count())
# Tabla temporal para ponderar la fecha y cantidad de alojamiento
df_alojamiento2Temp = spark.sql(f"""
WITH base AS (
    SELECT 
        ComplexEntityNo,
        ComplexEntityNoGalpon,
        ComplexEntityNoLote,
        pk_tiempo,
        SUM(Inventario) AS Inventario
    FROM {database_name}.alojamientoTemp
    GROUP BY ComplexEntityNo, ComplexEntityNoGalpon, ComplexEntityNoLote, pk_tiempo
),
calc_corral AS (
    SELECT 
        ComplexEntityNo,
        SUM(NumIdfecha * Inventario) AS sum_numid_inv,
        SUM(Inventario) AS sum_inv
    FROM {database_name}.alojamientoTemp
    GROUP BY ComplexEntityNo
),
calc_galpon AS (
    SELECT 
        ComplexEntityNoGalpon,
        SUM(NumIdfecha * Inventario) AS sum_numid_inv,
        SUM(Inventario) AS sum_inv
    FROM {database_name}.alojamientoTemp
    GROUP BY ComplexEntityNoGalpon
),
calc_lote AS (
    SELECT 
        ComplexEntityNoLote,
        SUM(NumIdfecha * Inventario) AS sum_numid_inv,
        SUM(Inventario) AS sum_inv
    FROM {database_name}.alojamientoTemp
    GROUP BY ComplexEntityNoLote
)

SELECT 
    a.ComplexEntityNo,
    a.pk_tiempo,
    a.Inventario,
    CASE WHEN b.sum_inv = 0 THEN 0 ELSE b.sum_numid_inv / b.sum_inv END AS AlojNumCorral,
    CASE WHEN c.sum_inv = 0 THEN 0 ELSE c.sum_numid_inv / c.sum_inv END AS AlojNumGalpon,
    CASE WHEN d.sum_inv = 0 THEN 0 ELSE d.sum_numid_inv / d.sum_inv END AS AlojNumLote
FROM base a
LEFT JOIN calc_corral b ON a.ComplexEntityNo = b.ComplexEntityNo
LEFT JOIN calc_galpon c ON a.ComplexEntityNoGalpon = c.ComplexEntityNoGalpon
LEFT JOIN calc_lote d ON a.ComplexEntityNoLote = d.ComplexEntityNoLote
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojamiento2Temp"
}
df_alojamiento2Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.alojamiento2Temp")
print('carga alojamiento2Temp', df_alojamiento2Temp.count())
 # Tabla donde se resta el ponderado de saca y alojamiento para hablar la Edad(Granja)
df_EdadLiquidacionTemp = spark.sql(f"select A.ComplexEntityNo,MAX(EdadNumCorral)EdadNumCorral,MAX(AlojNumCorral)AlojNumCorral, \
MAX(EdadNumCorral)-MAX(AlojNumCorral) EdadCorral,MAX(EdadNumGalpon)EdadNumGalpon,MAX(AlojNumGalpon)AlojNumGalpon, \
MAX(EdadNumGalpon)-MAX(AlojNumGalpon) EdadGalpon,MAX(EdadNumLote)EdadNumLote,MAX(AlojNumLote)AlojNumLote, \
MAX(EdadNumLote)-MAX(AlojNumLote) EdadLote \
from {database_name}.EdadxdateTemp A \
left join (select ComplexEntityNo, MAX(AlojNumCorral) AlojNumCorral, MAX(AlojNumGalpon) AlojNumGalpon, MAX(AlojNumLote)AlojNumLote from {database_name}.alojamiento2Temp group by ComplexEntityNo ) B on A.ComplexEntityNo = B.ComplexEntityNo \
group by A.ComplexEntityNo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/EdadLiquidacionTemp"
}
df_EdadLiquidacionTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.EdadLiquidacionTemp")
print('carga EdadLiquidacionTemp', df_EdadLiquidacionTemp.count())
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
,nvl(LPRS.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(SA.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(LES.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador \
,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,PLA.pk_planta,VEH.pk_vehiculo,SA.ComplexEntityNo \
,date_format(SA.xdate,'yyyyMMdd') as FechaTransaccion \
,year(cast(cast(SA.xdate as timestamp) as date)) AS AnoTransaccion \
,case when date_format(SA.xdate, 'MMM') = 'Jan' then 'Ene' \
	when date_format(SA.xdate, 'MMM') = 'Apr' then 'Abr' \
	when date_format(SA.xdate, 'MMM') = 'Aug' then 'Ago'  \
	when date_format(SA.xdate, 'MMM') = 'Dec' then 'Dic'  \
	else date_format(SA.xdate, 'MMM') end AS MesTransaccion \
,date_format(SA.EventDate          ,'yyyyMMdd') AS FechaSaca \
,date_format(SA.FirstHatchDate     ,'yyyyMMdd') AS FechaNacimiento \
,date_format(SA.FirstDateSold      ,'yyyyMMdd') AS FechaInicioSaca \
,date_format(SA.LastDateSold       ,'yyyyMMdd') AS FechaFinSaca \
,date_format(SA.FirstHatchDateLote ,'yyyyMMdd') AS FechaNacimientoLote \
,date_format(SA.FirstDateSoldLote  ,'yyyyMMdd') AS FechaInicioSacaLote \
,date_format(SA.LastDateSoldLote   ,'yyyyMMdd') AS FechaFinSacaLote \
,SA.EdadDiaSaca,SA.EdadInicioSaca,SA.EdadFinSaca,SA.DiasSaca,SA.EdadDiaSacaLote,SA.EdadInicioSacaLote \
,SA.EdadFinSacaLote,SA.DiasSacaLote,SA.DiasSacaGalpon,LPR.nproducto AS nomproductoinicial,LPRS.grupoproductosalida AS TipoProductoSalida \
,SA.FarmWtRefNo AS GuiaRemision,SA.TipoGranja,RTRIM(SA.UserId) AS usuario \
,CASE WHEN LENGTH(SA.Loadno) = 0 THEN '-' ELSE RTRIM(SA.Loadno) END AS carga \
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
LEFT JOIN {database_name}.EdadLiquidacionTemp E ON E.ComplexEntityNo = SA.ComplexEntityNo \
WHERE (date_format(SA.EventDate,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM'))")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Saca"
}
df_SacaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Saca")
print('carga Saca', df_SacaTemp.count())
df_ft_ventas_CDTemp = spark.sql(f"""SELECT a.pk_tiempo,a.pk_empresa,a.pk_division,a.pk_zona,a.pk_subzona,a.pk_plantel,a.pk_lote, 
a.pk_galpon,a.pk_sexo,a.pk_standard,a.pk_producto,a.pk_tipoproducto,a.pk_especie,a.pk_estado,a.pk_administrador,a.pk_proveedor, 
a.pk_planta,a.pk_vehiculo,a.ComplexEntityNo,a.FechaTransaccion,a.AnoTransaccion,a.MesTransaccion,a.FechaSaca,a.FechaNacimiento, 
a.FechaInicioSaca,a.FechaFinSaca,a.FechaNacimientoLote,a.FechaInicioSacaLote,a.FechaFinSacaLote,a.EdadDiaSaca,a.EdadInicioSaca, 
a.EdadFinSaca,a.DiasSaca,a.EdadDiaSacaLote,a.EdadInicioSacaLote,a.EdadFinSacaLote,a.DiasSacaLote,a.nomproductoinicial,a.TipoProductoSalida, 
a.GuiaRemision,a.TipoGranja,a.usuario,a.carga, a.AvesGranja,a.AvesGranjaAcum,a.PesoGranja,a.PesoGranjaAcum,a.TotalAvesGranja,a.TotalPesoGranja, 
a.PesoPromGranja,a.AvesPlantel,b.CantAloj,c.PobInicial,c.MortDia,c.PorcMortDiaAcum AS PorcMort,a.SacaMort as MortSaca, 
round((a.SacaMort / nullif(c.PobInicial,0))*100, 3) AS PorcMortSaca, 
(c.PobInicial - a.AvesGranjaTot - c.MortDia) as Saldo, 
round(a.EdadCorralLiq,3) AS EdadGranja,round(a.EdadGalponLiq,3) EdadGalpon,round(a.EdadLoteLiq,3) EdadLote,b.DiasAloj,1 as FlagArtificio,a.categoria,a.FlagAtipico,a.DiasSacaGalpon, 
a.DiasSacaEfectivoGalpon,B.TipoOrigen 
,'' plantno             
,a.fecha descripfecha        
,'' descripempresa      
,'' descripdivision     
,'' descripzona         
,'' descripsubzona      
,'' plantel             
,'' lote                
,'' galpon              
,'' descripsexo         
,'' descripstandard     
,'' descripproducto     
,'' descriptipoproducto 
,'' descripespecie      
,'' descripestado       
,'' descripadministrador
,'' descripproveedor    
,'' descripsemanavida   
,'' descripdiavida      
,'' descripplanta       
,'' descripvehiculo   
FROM {database_name}.Saca A 
left join ( 
select 
max(pk_tiempo) as pk_tiempo, 
max(descripfecha) as fecha, 
max(diasaloj) as DiasAloj, 
ComplexEntityNo, 
sum(CantAloj) as CantAloj, 
TipoOrigen 
from {database_name}.ft_alojamiento 
group by ComplexEntityNo,TipoOrigen 
) B on A.ComplexEntityNo = b.ComplexEntityNo 
left join ( 
select 
ComplexEntityNo, 
avg(PobInicial) as PobInicial, 
sum(MortDia) as MortDia, 
max(PorcMortDiaAcum) as PorcMortDiaAcum 
from {database_name}.ft_mortalidad_Diario 
where flagartificio = 1 
group by ComplexEntityNo,categoria 
) C on A.ComplexEntityNo = C.ComplexEntityNo """)
print('df_ft_ventas_CDTemp', df_ft_ventas_CDTemp.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ventas_CD cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ventas_CD")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_ventas_CDTemp.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
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

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
            
    print(f"agrega registros nuevos a la tabla ft_ventas_CD : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ventas_CD : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ventas_CDTemporal')
    print(f"Tabla ft_ventas_CDTemporal eliminada correctamente de la base de datos '{database_name}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    df_ft_ventas_CDTemp.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
df_UPD_ft_ventas_CD = spark.sql(f"""
select 
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
,a.pk_planta           
,a.pk_vehiculo         
,a.complexentityno     
,a.fechatransaccion    
,a.anotransaccion     
,a.mestransaccion      
,a.fechasaca           
,a.fechanacimiento     
,a.fechainiciosaca     
,a.fechafinsaca        
,a.fechanacimientolote 
,a.fechainiciosacalote 
,a.fechafinsacalote    
,a.edaddiasaca         
,a.edadiniciosaca      
,a.edadfinsaca         
,a.diassaca            
,a.edaddiasacalote     
,a.edadiniciosacalote  
,a.edadfinsacalote     
,a.diassacalote        
,a.nomproductoinicial  
,a.tipoproductosalida  
,a.guiaremision        
,a.tipogranja          
,a.usuario             
,a.carga               
,a.avesgranja          
,a.avesgranjaacum      
,a.pesogranja          
,a.pesogranjaacum      
,a.totalavesgranja     
,a.totalpesogranja     
,a.pesopromgranja      
,a.avesplantel         
,a.cantaloj            
,a.pobinicial          
,a.mortdia             
,a.porcmort            
,a.mortsaca            
,a.porcmortsaca        
,a.saldo               
,a.edadgranja          
,a.edadgalpon          
,a.edadlote            
,a.diasaloj            
,a.flagartificio       
,case when (a.categoria is null or a.categoria ='-') then coalesce(B.categoria,'-') else a.categoria end categoria
,a.flagatipico         
,a.diassacagalpon      
,a.diassacaefectivogalpon
,a.tipoorigen          
,a.plantno             
,a.descripfecha        
,a.descripempresa      
,a.descripdivision     
,a.descripzona         
,a.descripsubzona      
,a.plantel             
,a.lote                
,a.galpon              
,a.descripsexo         
,a.descripstandard     
,a.descripproducto     
,a.descriptipoproducto 
,a.descripespecie      
,a.descripestado       
,a.descripadministrador
,a.descripproveedor    
,a.descripsemanavida   
,a.descripdiavida      
,a.descripplanta       
,a.descripvehiculo   
from default.ft_ventas_CD A
left join default.lk_ft_Excel_categoria B on A.pk_lote = B.pk_lote 
where pk_empresa =1 """)
print('df_UPD_ft_ventas_CD', df_UPD_ft_ventas_CD.count())
try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ventas_CD cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ventas_CD")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(col("pk_empresa")== 1))
    filtered_new_data = df_UPD_ft_ventas_CD.filter(col("pk_empresa")== 1)
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

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
            
    print(f"agrega registros nuevos a la tabla ft_ventas_CD : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ventas_CD : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ventas_CDTemporal')
    print(f"Tabla ft_ventas_CDTemporal eliminada correctamente de la base de datos '{database_name}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    df_UPD_ft_ventas_CD.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
#df_INS_ft_ventas_CD2 = spark.sql(f"""
#select 
# A.pk_tiempo
#,A.pk_empresa
#,A.pk_division
#,A.pk_zona
#,A.pk_subzona
#,A.pk_plantel
#,A.pk_lote
#,A.pk_galpon
#,A.pk_sexo
#,A.pk_standard
#,A.pk_producto
#,A.pk_tipoproducto
#,A.pk_especie
#,A.pk_estado
#,A.pk_administrador
#,A.pk_proveedor
#,1 as pk_Planta
#,1 as pk_vehiculo
#,A.ComplexEntityNo
#,'' fechatransaccion    
#,'' anotransaccion     
#,'' mestransaccion      
#,'' fechasaca           
#,'' fechanacimiento     
#,A.FechaInicioSaca
#,A.FechaFinSaca
#,'' fechanacimientolote 
#,'' fechainiciosacalote 
#,'' fechafinsacalote    
#,0 EdadDiaSaca
#,0 EdadInicioSaca
#,0 EdadFinSaca
#,0 DiasSaca
#,0 EdadDiaSacaLote
#,0 EdadInicioSacaLote
#,0 EdadFinSacaLote
#,0 DiasSacaLote
#,'-' NomProductoInicial
#,'-' TipoProductoSalida
#,'-' GuiaRemision
#,'Brim' TipoGranja
#,'-' usuario
#,'-' carga
#,0 AvesGranja
#,0 AvesGranjaAcum
#,0 PesoGranja
#,0 PesoGranjaAcum
#,0 TotalAvesGranja
#,0 TotalPesoGranja
#,0 PesoPromGranja
#,0 AvesPlantel
#,0 CantAloj
#,A.PobInicial
#,A.MortDia
#,A.PorMort
#,0 MortSaca
#,0 PorcMortSaca
#,0 Saldo
#,0 EdadGranja
#,0 EdadGalpon
#,0 EdadLote
#,0 DiasAloj
#,2 flagartificio
#,A.categoria    
#,'' flagatipico         
#,'' diassacagalpon      
#,'' diassacaefectivogalpon
#,'' tipoorigen          
#,'' plantno             
#,a.descripfecha        
#,'' descripempresa      
#,'' descripdivision     
#,'' descripzona         
#,'' descripsubzona      
#,'' plantel             
#,'' lote                
#,'' galpon              
#,'' descripsexo         
#,'' descripstandard     
#,'' descripproducto     
#,'' descriptipoproducto 
#,'' descripespecie      
#,'' descripestado       
#,'' descripadministrador
#,'' descripproveedor    
#,'' descripsemanavida   
#,'' descripdiavida      
#,'' descripplanta       
#,'' descripvehiculo   
#from {database_name}.ft_consolidado_Corral A
#left join {database_name}.ft_ventas_cd B on A.ComplexEntityNo = B.ComplexEntityNo
#where B.ComplexEntityNo is null 
#and A.pk_empresa = 1 
#and a.pk_lote in (select distinct pk_lote from {database_name}.ft_ventas_cd where flagartificio = 1)
#and A.FechaAlojamiento >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
#""")
#print('df_INS_ft_ventas_CD2', df_INS_ft_ventas_CD2.count())
#try:
#    df_existentes = spark.read.format("parquet").load(path_target31)
#    datos_existentes = True
#    logger.info(f"Datos existentes de ft_ventas_CD cargados: {df_existentes.count()} registros")
#except:
#    datos_existentes = False
#    logger.info("No se encontraron datos existentes en ft_ventas_CD")
#
#if datos_existentes:
#    existing_data = spark.read.format("parquet").load(path_target31)
#    data_after_delete = existing_data.filter(~((col("pk_empresa") == 1) & (col("flagartificio") == 2)))
#    filtered_new_data = df_INS_ft_ventas_CD2.filter((col("pk_empresa") == 1) & (col("flagartificio") == 2))
#    final_data = filtered_new_data.union(data_after_delete)                             
#   
#    cant_ingresonuevo = filtered_new_data.count()
#    cant_total = final_data.count()
#    
#    # Escribir los resultados en ruta temporal
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal"
#    }
#    final_data.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.ft_ventas_CDTemporal")
#
#    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal")         
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
#    }
#    final_data2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.ft_ventas_cd")
#            
#    print(f"agrega registros nuevos a la tabla ft_ventas_CD : {cant_ingresonuevo}")
#    print(f"Total de registros en la tabla ft_ventas_CD : {cant_total}")
#     #Limpia la ubicación temporal
#    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
#    glue_client.delete_table(DatabaseName=database_name, Name='ft_ventas_CDTemporal')
#    print(f"Tabla ft_ventas_CDTemporal eliminada correctamente de la base de datos '{database_name}'.")
#else:
#    additional_options = {
#        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
#    }
#    df_INS_ft_ventas_CD2.write \
#        .format("parquet") \
#        .options(**additional_options) \
#        .mode("overwrite") \
#        .saveAsTable(f"{database_name}.ft_ventas_cd")
df_UPD_ft_ventas_CD2 = spark.sql(f"""
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
,a.pk_Planta
,a.pk_vehiculo
,A.ComplexEntityNo
,a.fechatransaccion    
,a.anotransaccion     
,a.mestransaccion      
,a.fechasaca           
,a.fechanacimiento     
,A.FechaInicioSaca
,A.FechaFinSaca
,b.fechanacimientolote 
,a.fechainiciosacalote 
,a.fechafinsacalote    
,a.EdadDiaSaca
,a.EdadInicioSaca
,a.EdadFinSaca
,a.DiasSaca
,a.EdadDiaSacaLote
,a.EdadInicioSacaLote
,a.EdadFinSacaLote
,a.DiasSacaLote
,a.NomProductoInicial
,a.TipoProductoSalida
,a.GuiaRemision
,a.TipoGranja
,a.usuario
,a.carga
,a.AvesGranja
,a.AvesGranjaAcum
,a.PesoGranja
,a.PesoGranjaAcum
,a.TotalAvesGranja
,a.TotalPesoGranja
,a.PesoPromGranja
,a.AvesPlantel
,a.CantAloj
,a.PobInicial
,a.MortDia
,a.PorcMort
,a.MortSaca
,a.PorcMortSaca
,a.Saldo
,a.EdadGranja
,a.EdadGalpon
,a.EdadLote
,a.DiasAloj
,a.flagartificio
,A.categoria    
,a.flagatipico         
,a.diassacagalpon      
,a.diassacaefectivogalpon
,a.tipoorigen          
,a.plantno             
,a.descripfecha        
,a.descripempresa      
,a.descripdivision     
,a.descripzona         
,a.descripsubzona      
,a.plantel             
,a.lote                
,a.galpon              
,a.descripsexo         
,a.descripstandard     
,a.descripproducto     
,a.descriptipoproducto 
,a.descripespecie      
,a.descripestado       
,a.descripadministrador
,a.descripproveedor    
,a.descripsemanavida   
,a.descripdiavida      
,a.descripplanta       
,a.descripvehiculo   
from {database_name}.ft_ventas_CD A
left join 
(select pk_plantel,pk_lote,fechanacimientolote from {database_name}.ft_ventas_cd 
where fechanacimientolote is not null
group by pk_plantel,pk_lote,fechanacimientolote) B ON A.pk_plantel = B.pk_plantel AND A.pk_lote = B.pk_lote
where A.flagartificio = 2
and A.fechanacimientolote is null
""")
print('df_UPD_ft_ventas_CD2', df_UPD_ft_ventas_CD2.count())
try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_ventas_CD cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_ventas_CD")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~((col("flagartificio") == 2) & (col("fechanacimientolote").isNull())))
    filtered_new_data = df_UPD_ft_ventas_CD2.filter((col("flagartificio") == 2) & (col("fechanacimientolote").isNull()))
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

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_ventas_CDTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
            
    print(f"agrega registros nuevos a la tabla ft_ventas_CD : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_ventas_CD : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_ventas_CDTemporal')
    print(f"Tabla ft_ventas_CDTemporal eliminada correctamente de la base de datos '{database_name}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_ventas_cd"
    }
    df_UPD_ft_ventas_CD2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_ventas_cd")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()