from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_prd_tbl_mortdia_gold"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
## Parametros Globales 
database_name = "default"
bucket_name_target = "ue1stgtestas3dtl005-gold"
bucket_name_source = "ue1stgtestas3dtl005-silver"
bucket_name_prdmtech = "UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
df_mortdia1 = spark.sql(f"SELECT a.IRN, a.LastModDate, a.xDate,a.xDateTime,a.EventDate,(CASE c.ActiveFlag WHEN 1 THEN 'Active' ELSE 'Inactive' END) AS Status, a.CreationDate, \
a.CreationUserId, a.PostStatus, a.UserId,a.RefNo,a.VoidFlag, a.EntityHistoryFlag, b.EntityNo,b.FarmName, b.FarmNo,a.ProteinEntitiesIRN, \
CASE WHEN a.Mortality = 0 and cast(a.creationdate as timestamp) >= current_timestamp() and current_timestamp() <= \
(current_timestamp()  + interval 9 hours 35 minutes) then a.U_Muertes else a.Mortality end Mortality, \
a.Culled,a.Weight,a.FeedConsumed,a.FeedInventory,b.ComplexEntityNo,b.ProteinGrowoutCodesIRN,b.ProteinFarmsIRN,b.GrowoutNo,b.ProteinCostCentersIRN,b.Stage, \
(CASE WHEN b.FirstDatePlaced IS NULL THEN 0 WHEN date_format(cast(cast(b.FirstDatePlaced as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.FirstDatePlaced as timestamp) as date)) END) AS FirstDatePlacedAge, \
(CASE WHEN b.FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(cast(b.FirstHatchDate  as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.FirstHatchDate  as timestamp) as date)) END) AS FirstHatchDateAge, \
(CASE WHEN b.AvgDatePlaced   IS NULL THEN 0 WHEN date_format(cast(cast(b.AvgDatePlaced   as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.AvgDatePlaced   as timestamp) as date)) END) AS AvgDatePlacedAge, \
(CASE WHEN b.AvgHatchDate    IS NULL THEN 0 WHEN date_format(cast(cast(b.AvgHatchDate    as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.AvgHatchDate    as timestamp) as date)) END) AS AvgHatchDateAge, \
b.FirstDatePlaced,b.FirstHatchDate,b.AvgDatePlaced,b.AvgHatchDate,b.HouseNo,b.PenNo,e.FeedTypeNo,a.FmimFeedTypesIRN,e.FeedTypeName,d.FormulaNo,d.FormulaName, \
b.SpeciesType,c.FarmType,d.ProteinProductsIRN,a.U_PEAccidentados,a.U_PEHigadoGraso,a.U_PEHepatomegalia,a.U_PEHigadoHemorragico,a.U_PEInanicion, \
a.U_PEProblemaRespiratorio,a.U_PESCH,a.U_PEEnteritis,a.U_PEAscitis,a.U_PEMuerteSubita,a.U_PEEstresPorCalor,a.U_PEHidropericardio, \
a.U_PEHemopericardio, a.U_PEUratosis, a.U_PEMaterialCaseoso, a.U_PEOnfalitis, a.U_PERetencionDeYema, a.U_PEErosionDeMolleja, a.U_PEHemorragiaMusculos, \
a.U_PESangreEnCiego, a.U_PEPericarditis, a.U_PEPeritonitis, a.U_PEProlapso, a.U_PEPicaje, a.U_PERupturaAortica, a.U_PEBazoMoteado, a.U_PENoViable, \
a.U_AerosaculitisG2 U_PEAerosaculitisG2, a.U_Cojera U_PECojera, a.`U_HígadoIcterico` as `U_PEHigadoIcterico`, a.U_MaterialCaseoso_po1ra U_PEMaterialCaseoso_po1ra, \
a.U_MaterialCaseosoMedRetr U_PEMaterialCaseosoMedRetr, a.U_NecrosisHepatica U_PENecrosisHepatica, a.U_Neumonia U_PENeumonia, a.U_Septicemia U_PESepticemia, \
a.U_VomitoNegro U_PEVomitoNegro, a.U_PVEAsperguillius U_PEAsperguillius, a.U_PVEBazoGrandeMot U_PEBazoGrandeMot, a.U_PVECorazonGrande U_PECorazonGrande, \
a.U_PVECuadToxi U_PECuadroToxico, a.U_PavosBB, a.U_Pigmentacion, 0 FlagTransfPavos, b.ComplexEntityNo SourceComplexEntityNo, \
b.ComplexEntityNo DestinationComplexEntityNo, a.U_CausaPesoBajo, a.U_AccionPesoBajo, a.U_RuidosTotales, g.Price, a.Uniformity, a.U_RuidoRespiratorio, a.CV \
FROM {database_name}.si_brimfieldtrans a \
INNER JOIN {database_name}.si_mvbrimentities b ON a.ProteinEntitiesIRN=b.ProteinEntitiesIRN \
INNER JOIN {database_name}.si_proteinfarms c ON b.ProteinFarmsIRN=c.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas d ON a.FmimFeedFormulasIRN=d.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes e ON a.FmimFeedTypesIRN=e.IRN \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtrans where \
                  (date_format(cast(EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                ) f ON a.U_TrazSAP=F.RefNo \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtransdetails where \
                  (date_format(cast(CreationDate as timestamp), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                ) g ON f.IRN=g.FmimFeedTransIRN \
WHERE c.FarmType = 1 and b.SpeciesType = 1 and a.EntityHistoryFlag in (false) and \
(date_format(cast(a.EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) ")
print('CARGA df_mortdia1', df_mortdia1.count())
df_mortdia2 = spark.sql(f"SELECT a.IRN, a.LastModDate, a.xDate, a.xDateTime,a.EventDate, (CASE c.ActiveFlag WHEN 1 THEN 'Active' ELSE 'Inactive' END) AS Status, a.CreationDate, \
a.CreationUserId, a.PostStatus,a.UserId, a.RefNo, a.VoidFlag, a.EntityHistoryFlag, b.EntityNo, b.FarmName, b.FarmNo, a.ProteinEntitiesIRN, \
CASE WHEN a.Mortality = 0 and cast(a.creationdate as timestamp) >= current_timestamp() and current_timestamp()  <= \
(current_timestamp()  + interval 9 hours 35 minutes) then a.U_Muertes else a.Mortality end Mortality, \
a.Culled, a.Weight, a.FeedConsumed, a.FeedInventory,b.ComplexEntityNo, b.ProteinGrowoutCodesIRN, b.ProteinFarmsIRN, b.GrowoutNo, b.ProteinCostCentersIRN, b.Stage, \
(CASE WHEN b.FirstDatePlaced IS NULL THEN 0 WHEN date_format(cast(cast(b.FirstDatePlaced as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.FirstDatePlaced as timestamp) as date)) END) AS FirstDatePlacedAge, \
(CASE WHEN b.FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(cast(b.FirstHatchDate  as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.FirstHatchDate  as timestamp) as date)) END) AS FirstHatchDateAge, \
(CASE WHEN b.AvgDatePlaced   IS NULL THEN 0 WHEN date_format(cast(cast(b.AvgDatePlaced   as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.AvgDatePlaced   as timestamp) as date)) END) AS AvgDatePlacedAge, \
(CASE WHEN b.AvgHatchDate    IS NULL THEN 0 WHEN date_format(cast(cast(b.AvgHatchDate    as timestamp) as date),'yyyyMMdd') = date_format(cast('1899-11-30' as date),'yyyyMMdd') THEN 0 ELSE DateDiff(cast(cast(a.xdate as timestamp) as date),cast(cast(b.AvgHatchDate    as timestamp) as date)) END) AS AvgHatchDateAge, \
b.FirstDatePlaced, b.FirstHatchDate, b.AvgDatePlaced, b.AvgHatchDate, b.HouseNo, b.PenNo, e.FeedTypeNo, a.FmimFeedTypesIRN, e.FeedTypeName, d.FormulaNo, \
d.FormulaName, b.SpeciesType, c.FarmType, d.ProteinProductsIRN,a.U_PVEAccidentados U_PEAccidentados, a.U_PEHigadoGraso, a.U_PEHepatomegalia, a.U_PVEHigadoHemorragico U_PEHigadoHemorragico, \
a.U_PEInanicion, a.U_PVEProbResp U_PEProblemaRespiratorio, a.U_PESCH, a.U_PVEEnteritis U_PEEnteritis, a.U_PVEAscitis U_PEAscitis, a.U_PVEMuerteSubita U_PEMuerteSubita, a.U_PEEstresPorCalor, \
a.U_PEHidropericardio, a.U_PEHemopericardio, a.U_PVEUratosis U_PEUratosis, a.U_PVEMaterialCaseoso U_PEMaterialCaseoso, a.U_PVEOnfalitis U_PEOnfalitis, a.U_PVERetYema U_PERetencionDeYema, \
a.U_PVEErosionMolleja U_PEErosionDeMolleja, a.U_PEHemorragiaMusculos, a.U_PESangreEnCiego, a.U_PEPericarditis, a.U_PVEPeritonitis U_PEPeritonitis, a.U_PVEProlapsado U_PEProlapso, \
a.U_PVEPicaje U_PEPicaje, a.U_PVERupturaAortica U_PERupturaAortica, a.U_PEBazoMoteado, a.U_PVENoViable U_PENoViable, a.U_AerosaculitisG2 U_PEAerosaculitisG2, a.U_Cojera U_PECojera, \
a.`U_HígadoIcterico` U_PEHigadoIcterico, a.U_MaterialCaseoso_po1ra U_PEMaterialCaseoso_po1ra, a.U_MaterialCaseosoMedRetr U_PEMaterialCaseosoMedRetr, a.U_NecrosisHepatica U_PENecrosisHepatica, \
a.U_Neumonia U_PENeumonia,a.U_Septicemia U_PESepticemia,a.U_VomitoNegro U_PEVomitoNegro,a.U_PVEAsperguillius U_PEAsperguillius,a.U_PVEBazoGrandeMot U_PEBazoGrandeMot, \
a.U_PVECorazonGrande U_PECorazonGrande,a.U_PVECuadToxi U_PECuadroToxico,a.U_PavosBB,a.U_Pigmentacion,CASE WHEN isnull(f.SourceComplexEntityNo) = true THEN 0 ELSE 1 END FlagTransfPavos, \
f.SourceComplexEntityNo SourceComplexEntityNo,f.DestinationComplexEntityNo DestinationComplexEntityNo,a.U_CausaPesoBajo,a.U_AccionPesoBajo,a.U_RuidosTotales,h.Price,a.Uniformity,a.U_RuidoRespiratorio,a.CV \
FROM {database_name}.si_brimfieldtrans a \
INNER JOIN {database_name}.si_mvbrimentities b ON a.ProteinEntitiesIRN=b.ProteinEntitiesIRN \
INNER JOIN {database_name}.si_proteinfarms c ON b.ProteinFarmsIRN=c.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas d ON a.FmimFeedFormulasIRN=d.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes e ON a.FmimFeedTypesIRN=e.IRN \
LEFT OUTER JOIN (select MIN(cast(cast(xdate as timestamp) as date)) xdate,SourceComplexEntityNo,DestinationComplexEntityNo \
                 from {database_name}.si_mvpmtstransferdestdetails \
                 GROUP BY SourceComplexEntityNo,DestinationComplexEntityNo \
                 ) f ON  b.ComplexEntityNo = f.SourceComplexEntityNo AND cast(cast(a.EventDate as timestamp) as date) <= cast(cast(f.xdate as timestamp) as date) \
LEFT OUTER JOIN ( select * from {database_name}.si_fmimfeedtrans where \
                  (date_format(cast(EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                ) g ON a.U_TrazSAP=g.RefNo \
LEFT OUTER JOIN ( select * from {database_name}.si_fmimfeedtransdetails where \
                  (date_format(cast(CreationDate as timestamp), 'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
                ) h ON g.IRN=h.FmimFeedTransIRN \
WHERE c.FarmType = 7 and b.SpeciesType = 2 and a.EntityHistoryFlag in (false) and a.PostStatus <> 1 and \
(date_format(cast(a.EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) \
order by  a.xdate")

df_Mortdia = df_mortdia1.union(df_mortdia2)

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/mortdia"
}
df_Mortdia.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.mortdia")
print('carga df_mortdia2', df_mortdia2.count())
print('carga mortdia', df_Mortdia.count())
df_Atipicos = spark.sql(f"""SELECT ComplexEntityNo,PlantNo, 2 as FlagAtipico 
FROM default.si_mvPmtsProcRecvTransHouseDetail
WHERE (date_format(cast(EventDate as timestamp),'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyyMM')) and PlantNo like '%SIN_ELIM%'
GROUP BY ComplexEntityNo,PlantNo
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Atipicos"
}
df_Atipicos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Atipicos")
print('carga Atipicos', df_Atipicos.count())
job.commit()  # Llama a commit al final del trabajo
print("termina notebook") 
job.commit()