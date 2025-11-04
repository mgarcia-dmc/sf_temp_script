# Usa el contexto de Spark existente
from pyspark.context import SparkContext  # Importa SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job  # Asegúrate de importar Job
import boto3
from botocore.exceptions import ClientError

# Obtén el contexto de Spark activo proporcionado por Glue
sc = SparkContext.getOrCreate()
sc._conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
sc._conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
sc._conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
sc._conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
#sc._conf.set("spark.executor.memory", "16g")
#sc._conf.set("spark.driver.memory", "4g")
#sc._conf.set("spark.dynamicAllocation.enabled", "true")
#sc._conf.set("spark.executor.memoryOverhead", "2g")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

# Define el nombre del trabajo
JOB_NAME = "nt_prd_tbl_gold"

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

file_name_target6 = f"{bucket_name_prdmtech}ft_Reprod_Ingreso_Total/"
file_name_target7 = f"{bucket_name_prdmtech}ft_Reprod_Mort_Diario/"
file_name_target8 = f"{bucket_name_prdmtech}ft_Reprod_Diario/"
file_name_target9 = f"{bucket_name_prdmtech}ft_Reprod_Galpon_Diario/"
file_name_target10 = f"{bucket_name_prdmtech}ft_Reprod_Galpon_Semana/"
file_name_target11 = f"{bucket_name_prdmtech}ft_Reprod_Lote_Diario/"
file_name_target12 = f"{bucket_name_prdmtech}ft_Reprod_Lote_Semana/"
file_name_target13 = f"{bucket_name_prdmtech}ft_Reprod_Galpon_Mensual/"
file_name_target14 = f"{bucket_name_prdmtech}ft_Reprod_Lote_Mensual/"
file_name_target15 = f"{bucket_name_prdmtech}ft_Reprod_Liquid_Diario/"
file_name_target16 = f"{bucket_name_prdmtech}ft_Reprod_Liquid_Galpon/"
file_name_target17 = f"{bucket_name_prdmtech}ft_Reprod_ConsumoAlimento/"

path_target6 = f"s3://{bucket_name_target}/{file_name_target6}"
path_target7 = f"s3://{bucket_name_target}/{file_name_target7}"
path_target8 = f"s3://{bucket_name_target}/{file_name_target8}"
path_target9 = f"s3://{bucket_name_target}/{file_name_target9}"
path_target10 = f"s3://{bucket_name_target}/{file_name_target10}"
path_target11 = f"s3://{bucket_name_target}/{file_name_target11}"
path_target12 = f"s3://{bucket_name_target}/{file_name_target12}"
path_target13 = f"s3://{bucket_name_target}/{file_name_target13}"
path_target14 = f"s3://{bucket_name_target}/{file_name_target14}"
path_target15 = f"s3://{bucket_name_target}/{file_name_target15}"
path_target16 = f"s3://{bucket_name_target}/{file_name_target16}"
path_target17 = f"s3://{bucket_name_target}/{file_name_target17}"

print('cargando rutas')
from pyspark.sql.functions import *

database_name = "default"
df_parametros = spark.sql(f"select * from {database_name}.Parametros")

AnioMes = df_parametros.collect()[0][0]
AnioMesFin = df_parametros.collect()[0][1]

print('cargando parametros fechas')
print(f'parametros : desde  {AnioMes} hasta {AnioMesFin}')
# tabla Reproductora

#Se crea la tabla intermedia Reprod_Trans para insertar los datos de las tablas principales de MTECH (relacionar y unificar las tablas principales)
#Transfermode <> 3
df_Reprod_Trans1 = spark.sql(f"SELECT \
BFT.CreationDate,BFT.CullEggsInv ,BFT.CullEggsProd ,BFT.EggWeightSamples ,BFT.EggWeightsFlag ,BFT.FeedConsumedF ,BFT.FeedConsumedM ,BFT.FeedInventoryF \
,BFT.FeedInventoryM ,BFT.FmimFeedFormulasIRN_F,BFT.FmimFeedFormulasIRN_M ,BFT.FmimFeedTypesIRN_Female,BFT.FmimFeedTypesIRN_Male,BFT.HatchEggsInv \
,BFT.HatchEggsProd,BFT.IRN IRN_BFT,BFT.MortalityF,BFT.MortalityM ,BFT.ProteinEntitiesIRN  ProteinEntitiesIRN_BFT ,BFT.WeightFSamples \
,BFT.WeightMSamples,BFT.xDate,BFT.UniformityM ,BFT.UniformityF \
,CASE WHEN BFT.WeightF <= 0 THEN 0 ELSE BFT.WeightF END WeightF \
,CASE WHEN BFT.WeightM <= 0 THEN 0 ELSE BFT.WeightM END WeightM \
,BFT.EggWeight ,BFT.EventDate,BFT.U_Ruptura_Aortica ,BFT.U_Ruptura_AorticaMale,BFT.U_Bazo_Moteado ,BFT.U_Bazo_MoteadoMale \
,(BFT.U_Accidentados + BFT.U_AccidentadosPollo) U_AccidentadosPollo \
,(BFT.U_AccidentadosMale + BFT.U_AccidentadosPolloMale) U_AccidentadosPolloMale \
,(BFT.U_Descarte + BFT.U_DescartePollo) U_DescartePollo \
,(BFT.U_DescarteMale + BFT.U_DescartePolloMale) U_DescartePolloMale \
,(BFT.U_ProblemaRespiratorio + BFT.U_Pro_RespiratorioPL) U_Pro_RespiratorioPL \
,(BFT.U_ProblemaRespiratorioMale + BFT.U_Pro_RespiratorioPLMale) U_Pro_RespiratorioPLMale \
,(BFT.U_Enteritis + BFT.U_EnteritisPollo) U_EnteritisPollo \
,(BFT.U_EnteritisMale + BFT.U_EnteritisPolloMale) U_EnteritisPolloMale \
,(BFT.U_Material_caseoso + BFT.U_Mat_caseosoPL) U_Mat_caseosoPL \
,(BFT.U_Material_caseosoMale + BFT.U_Mat_caseosoPLMale) U_Mat_caseosoPLMale \
,(BFT.U_Picaje + BFT.U_PicajePollo) U_PicajePollo \
,(BFT.U_PicajeMale + BFT.U_PicajePolloMale) U_PicajePolloMale \
,(BFT.U_Prolapso + BFT.U_ProlapsoPollo) U_ProlapsoPollo \
,(BFT.U_ProlapsoMale + BFT.U_ProlapsoPolloMale) U_ProlapsoPolloMale \
,(BFT.U_Peritonitis + BFT.U_PeritonitisPollo) U_PeritonitisPollo \
,(BFT.U_PeritonitisMale + BFT.U_PeritonitisPolloMale) U_PeritonitisPolloMale \
,BFT.U_MaltratoPollo ,BFT.U_MaltratoPolloMale ,BFT.U_Erosion_de_molleja ,BFT.U_Erosion_de_mollejaMale \
,(BFT.U_Onfalitis + BFT.U_OnfalitisPollo) U_OnfalitisPollo \
,(BFT.U_OnfalitisMale + BFT.U_OnfalitisPolloMale) U_OnfalitisPolloMale \
,BFT.U_Error_de_sexo ,BFT.U_Error_de_sexoMale,BFT.U_Postrado ,BFT.U_PostradoMale,BFT.U_Bajo_peso,BFT.U_Bajo_pesoMale \
,BFT.U_PicajeCull,BFT.U_PicajeCullMale,BFT.U_CojosCull ,BFT.U_CojosCullMale,BFT.U_PatasAbiertas,BFT.U_PatasAbiertasMale \
,BFT.U_ProlapsoCull,BFT.U_ProlapsoCullMale,BFT.U_Fuer_de_post ,BFT.U_Fuer_de_postMale,BFT.U_otrosCull,BFT.U_otrosCullMale \
,BFT.U_Prolapso_PV,BFT.U_Prolapso_PVMale,BFT.U_Postrado_PV ,BFT.U_Postrado_PVMale,BFT.U_PatasAbiertas_Pavo,BFT.U_PatasAbiertas_PavoMale \
,BFT.U_Otros_PV,BFT.U_Otros_PVMale,BFT.U_FueraPostura_PV ,BFT.U_FueraPostura_PVMale,BFT.U_BajoPeso_PV,BFT.U_BajoPeso_PVMale \
,BFT.U_DescartePicaje_PV,BFT.U_DescartePicaje_PVMale,BFT.U_DescarteCojo_PV ,BFT.U_DescarteCojo_PVMale,BFT.U_SexoErrado_PV,BFT.U_SexoErrado_PVMale \
,BFT.U_ErrorSexo_PV,BFT.U_Error_Sexo,BFT.U_Error_SexoMale ,BFT.U_Cojo,BFT.U_CojoMale,BFT.U_Uratosis,BFT.U_UratosisMale \
,BFT.U_Ascitis,BFT.U_AscitisMale,BFT.U_Retencion_Yema,BFT.U_Retencion_YemaMale,BFT.U_Sangre_en_Ciego,BFT.U_Sangre_en_CiegoMale,BFT.U_Hidropericardio \
,BFT.U_HidropericardioMale,BFT.U_Higado_Hemorragico,BFT.U_Higado_HemorragicoMale,BFT.U_SCH,BFT.U_SCHMale,BFT.U_Muerte_Subita,BFT.U_Muerte_SubitaMale \
,(BFT.`U_Estrés_Calor` + BFT.U_Estres_Calor) as U_Estres_Calor \
,(BFT.`U_Estrés_CalorMale` + BFT.U_Estres_CalorMale) as U_Estres_CalorMale \
,BFT.U_Pericarditis,BFT.U_PericarditisMale,BFT.U_GalloCorrido,BFT.U_GalloCorridoMale,BFT.U_Conformacion,BFT.U_ConformacionMale,BFT.U_Higado_Graso,BFT.U_Higado_GrasoMale \
,BFT.U_Tumor,BFT.U_TumorMale,BFT.U_DobleYema,BFT.U_DobleYemaProd,BFT.U_Poroso,BFT.U_PorosoProd,BFT.U_Chico,BFT.U_ChicoProd,BFT.U_Sucio,BFT.U_SucioProd \
,BFT.U_Deforme,BFT.U_DeformeProd,BFT.U_CascaraDebil,BFT.U_CascaraDebilProd ,BFT.U_Palido,BFT.U_PalidoProd,BFT.U_Quinado,BFT.U_QuinadoProd,BFT.U_Farfara \
,BFT.U_FarfaraProd,BFT.U_HuevoMarron,BFT.U_HuevoMarronProd,BFT.U_Roto,BFT.U_RotoProd,BFT.U_Invendible,BFT.U_InvendibleProd,BFT.U_Sangre \
,BFT.U_SangreProd,BFT.U_Incubable,BFT.U_IncubableProd,BFT.U_Comercial ,BFT.U_ComercialProd,BFT.U_Oviducto_Poliquistico,BFT.U_Oviducto_PoliquisticoMale \
,BFT.U_No_Viable,BFT.U_No_ViableMale,BFT.U_SuperJumbo,BFT.U_SuperJumboProd,BFT.U_Despatarrados,BFT.U_DespatarradosMale,BFT.TotalEggsProd,BFT.TotalEggsInv \
,BFT.U_HI_de_piso,VB.ProteinEntitiesIRN ProteinEntitiesIRN_VB ,VB.ProteinProductsAnimalsIRN_OS,VB.ProteinProductsIRN_OS,VB.ProductNo_OS,VB.AnimalType_OS \
,(CASE WHEN VB.FirstDatePlaced IS NULL THEN 0 WHEN date_format(VB.FirstDatePlaced,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstDatePlaced as timestamp)))+6 END)/7 AS FirstDatePlacedAge \
,(CASE WHEN VB.FirstHatchDate  IS NULL THEN 0 WHEN date_format(VB.FirstHatchDate,'yyyy-MM-dd')  = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+6 END)/7 AS FirstHatchDateAge \
,(CASE WHEN VB.AvgDatePlaced   IS NULL THEN 0 WHEN date_format(VB.AvgDatePlaced,'yyyy-MM-dd')   = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgDatePlaced   as timestamp)))+6 END)/7 AS AvgDatePlacedAge \
,(CASE WHEN VB.AvgHatchDate    IS NULL THEN 0 WHEN date_format(VB.AvgHatchDate,'yyyy-MM-dd')    = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgHatchDate    as timestamp)))+6 END)/7 AS AvgHatchDateAge \
,(CASE WHEN VB.FirstDatePlaced IS NULL THEN 0 WHEN date_format(VB.FirstDatePlaced,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstDatePlaced as timestamp)) END) AS FirstDatePlacedAgesin7 \
,(CASE WHEN VB.FirstHatchDate  IS NULL THEN 0 WHEN date_format(VB.FirstHatchDate,'yyyy-MM-dd')  = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)) END) AS FirstHatchDateAgesin7 \
,(CASE WHEN VB.AvgDatePlaced   IS NULL THEN 0 WHEN date_format(VB.AvgDatePlaced,'yyyy-MM-dd')   = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgDatePlaced   as timestamp)) END) AS AvgDatePlacedAgesin7 \
,(CASE WHEN VB.AvgHatchDate    IS NULL THEN 0 WHEN date_format(VB.AvgHatchDate,'yyyy-MM-dd')    = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgHatchDate    as timestamp)) END) AS AvgHatchDateAgesin7 \
,(CASE WHEN VB.FirstHatchDate  IS NULL THEN 0 WHEN date_format(VB.FirstHatchDate,'yyyy-MM-dd')  = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+2 END)/7 AS FirstHatchDateAge1 \
,(CASE WHEN VB.FirstHatchDate  IS NULL THEN 0 WHEN date_format(VB.FirstHatchDate,'yyyy-MM-dd')  = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+2 END) AS FirstHatchDateAge2 \
,VB.AvgHatchDate ,VB.AvgDatePlaced ,VB.FirstHatchDate ,VB.FirstDatePlaced ,VB.FirstDateSold ,VB.LastDateSold \
,VB.AvgDateSold ,VB.PenNo,VB.EntityNo,VB.FarmNo FarmNo_VB,VB.HouseNo,VB.IRN IRN_VB,VB.ComplexEntityNo,VB.HouseNoPenNo \
,VB.FarmName FarmName_VB,VB.ProteinFarmsIRN,VB.FarmType FarmType_VB ,VB.GrowoutNo ,VB.ProteinGrowoutCodesIRN ProteinGrowoutCodesIRN_VB \
,VB.AnimalType,VB.ProductNo,VB.ProteinHousesIRN ProteinHousesIRN_VB ,VB.BreedNo,VB.ProteinBreedCodesIRN ProteinBreedCodesIRN_VB \
,VB.GRN,VB.ProteinCostCentersIRN ProteinCostCentersIRN_VB,VB.FarmStage FarmStage_VB,VB.Status Status_VB \
,VB.GrowoutName,VB.ProteinProductsIRN ProteinProductsIRN_VB,VB.BreedName,VB.ProteinProductsAnimalsIRN ProteinProductsAnimalsIRN_VB \
,VB.ProteinStandardVersionsIRN ProteinStandardVersionsIRN_VB,VB.FeedMillName,VB.ProteinFacilityFeedMillsIRN ProteinFacilityFeedMillsIRN_VB \
,VB.StandardName,VB.StandardNo,VB.ProductName,VB.ProductName_OS,VB.ProteinProductsAnimalsIRN_Progeny,VB.ProteinBreedCodesIRN_Progeny \
,VB.ProgenyProductNo,VB.ProgenyBreedNo,VB.MaleProductNo,VB.ProgenyProductName,VB.DateCap,VB.MalesCap,VB.HensCap,VB.GenerationCode \
,PF.FarmName FarmName_PF,PF.FarmNo FarmNo_PF,PF.FarmStage FarmStage_PF,PF.FarmType FarmType_PF,PF.IRN IRN_PF \
,PF.ProteinCostCentersIRN ProteinCostCentersIRN_PF,PF.ProteinCountiesIRN,PF.ProteinFacilityFeedMillsIRN ProteinFacilityFeedMillsIRN_PF \
,PF.ProteinFacilityPlantsIRN,PF.ProteinGrowoutCodesIRN ProteinGrowoutCodesIRN_PF,VFFF.IRN IRN_VFFF,VFFF.FmimFeedTypesIRN FmimFeedTypesIRN_VFFF \
,VFFF.FormulaNo FormulaNo_VFFF,VFFF.FormulaType FormulaType_VFFF,VFFF.FeedTypeNo FeedTypeNo_VFFF,VFFF.FeedTypeName FeedTypeName_VFFF \
,VFFF.ActiveFlag ActiveFlag_VFFF,VFFF.FormulaName FormulaName_VFFF,VFFF.FarmType FarmType_VFFF,VFFF.ProteinProductsIRN ProteinProductsIRN_VFFF \
,VFFF.UnitsPer UnitsPer_VFFF,VFFM.IRN IRN_VFFM,VFFM.FmimFeedTypesIRN FmimFeedTypesIRN_VFFM,VFFM.FormulaNo FormulaNo_VFFM \
,VFFM.FormulaType FormulaType_VFFM,VFFM.FeedTypeNo FeedTypeNo_VFFM,VFFM.FeedTypeName FeedTypeName_VFFM,VFFM.ActiveFlag ActiveFlag_VFFM \
,VFFM.FormulaName FormulaName_VFFM,VFFM.FarmType FarmType_VFFM,VFFM.ProteinProductsIRN ProteinProductsIRN_VFFM,VFFM.UnitsPer UnitsPer_VFFM \
,FTF.FeedTypeNo FeedTypeNo_FTF,FTF.FarmType FarmType_FTF,FTF.IRN IRN_FTF,FTF.FeedTypeName FeedTypeName_FTF,FTF.Sex Sex_FTF,FTM.FeedTypeNo FeedTypeNo_FTM \
,FTM.FarmType FarmType_FTM,FTM.IRN IRN_FTM,FTM.FeedTypeName FeedTypeName_FTM,FTM.Sex Sex_FTM,FirstDateMovedIn,BFT.Transfermode,FFTD.Price \
FROM {database_name}.si_bimfieldtrans AS BFT \
LEFT OUTER JOIN {database_name}.si_mvbimentities AS VB ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN \
LEFT OUTER JOIN {database_name}.si_proteinfarms AS PF ON VB.ProteinFarmsIRN = PF.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas AS VFFF ON BFT.FmimFeedFormulasIRN_F = VFFF.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas AS VFFM ON BFT.FmimFeedFormulasIRN_M = VFFM.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes AS FTF ON BFT.FmimFeedTypesIRN_Female = FTF.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes AS FTM ON BFT.FmimFeedTypesIRN_Male = FTM.IRN \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtrans \
                ) as FFT ON BFT.U_TrazSAP = FFT.RefNo \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtransdetails \
                ) FFTD on FFT.IRN = FFTD.FmimFeedTransIRN \
WHERE BFT.Transfermode <> 3 \
AND VB.SpeciesType in (1,2)")
#(date_format(CAST(BFT.EventDate AS timestamp),'yyyyMM') >= {AnioMes} AND date_format(CAST(BFT.EventDate AS timestamp),'yyyyMM') <= {AnioMesFin})
df_Reprod_Trans1.show(10)
print('df_Reprod_Trans1')
#INSERT INTO STGPECUARIO.Reprod_Trans
df_Reprod_Trans2 = spark.sql(f"SELECT BFT.CreationDate \
,CASE WHEN BFT.CullEggsInv                                                 < 0 THEN 0 ELSE BFT.CullEggsInv      END CullEggsInv \
,CASE WHEN BFT.CullEggsProd                                                < 0 THEN 0 ELSE BFT.CullEggsProd     END CullEggsProd \
,CASE WHEN BFT.EggWeightSamples                                            < 0 THEN 0 ELSE BFT.EggWeightSamples END EggWeightSamples,BFT.EggWeightsFlag \
,CASE WHEN BFT.FeedConsumedF                                               < 0 THEN 0 ELSE BFT.FeedConsumedF    END FeedConsumedF \
,CASE WHEN BFT.FeedConsumedM                                               < 0 THEN 0 ELSE BFT.FeedConsumedM    END FeedConsumedM \
,CASE WHEN BFT.FeedInventoryF                                              < 0 THEN 0 ELSE BFT.FeedInventoryF   END FeedInventoryF \
,CASE WHEN BFT.FeedInventoryM                                              < 0 THEN 0 ELSE BFT.FeedInventoryM   END FeedInventoryM \
,BFT.FmimFeedFormulasIRN_F,BFT.FmimFeedFormulasIRN_M,BFT.FmimFeedTypesIRN_Female,BFT.FmimFeedTypesIRN_Male \
,CASE WHEN BFT.HatchEggsInv                                                < 0 THEN 0 ELSE BFT.HatchEggsInv END HatchEggsInv \
,CASE WHEN BFT.HatchEggsProd                                               < 0 THEN 0 ELSE BFT.HatchEggsProd END HatchEggsProd,BFT.IRN IRN_BFT \
,CASE WHEN BFT.MortalityF                                                  < 0 THEN 0 ELSE BFT.MortalityF END MortalityF \
,CASE WHEN BFT.MortalityM                                                  < 0 THEN 0 ELSE BFT.MortalityM END MortalityM ,BFT.ProteinEntitiesIRN  ProteinEntitiesIRN_BFT \
,CASE WHEN BFT.WeightFSamples                                              < 0 THEN 0 ELSE BFT.WeightFSamples END WeightFSamples \
,CASE WHEN BFT.WeightMSamples                                              < 0 THEN 0 ELSE BFT.WeightMSamples END WeightMSamples,BFT.xDate \
,CASE WHEN BFT.UniformityM                                                 < 0 THEN 0 ELSE BFT.UniformityM END UniformityM \
,CASE WHEN BFT.UniformityF                                                 < 0 THEN 0 ELSE BFT.UniformityF END UniformityF \
,CASE WHEN BFT.WeightF                                                     < 0 THEN 0 ELSE BFT.WeightF END WeightF \
,CASE WHEN BFT.WeightM                                                     < 0 THEN 0 ELSE BFT.WeightM END WeightM \
,CASE WHEN BFT.EggWeight                                                   < 0 THEN 0 ELSE BFT.EggWeight                                                   END EggWeight,BFT.EventDate \
,CASE WHEN BFT.U_Ruptura_Aortica                                           < 0 THEN 0 ELSE BFT.U_Ruptura_Aortica                                           END U_Ruptura_Aortica \
,CASE WHEN BFT.U_Ruptura_AorticaMale                                       < 0 THEN 0 ELSE BFT.U_Ruptura_AorticaMale                                       END U_Ruptura_AorticaMale \
,CASE WHEN BFT.U_Bazo_Moteado                                              < 0 THEN 0 ELSE BFT.U_Bazo_Moteado                                              END U_Bazo_Moteado \
,CASE WHEN BFT.U_Bazo_MoteadoMale                                          < 0 THEN 0 ELSE BFT.U_Bazo_MoteadoMale                                          END U_Bazo_MoteadoMale \
,CASE WHEN (BFT.U_AccidentadosPollo + BFT.U_Accidentados)                  < 0 THEN 0 ELSE (BFT.U_AccidentadosPollo + BFT.U_Accidentados)                  END U_AccidentadosPollo \
,CASE WHEN (BFT.U_AccidentadosPolloMale + BFT.U_AccidentadosMale)          < 0 THEN 0 ELSE (BFT.U_AccidentadosPolloMale + BFT.U_AccidentadosMale)          END U_AccidentadosPolloMale \
,CASE WHEN (BFT.U_DescartePollo + BFT.U_Descarte)                          < 0 THEN 0 ELSE (BFT.U_DescartePollo + BFT.U_Descarte)                          END U_DescartePollo \
,CASE WHEN (BFT.U_DescartePolloMale + BFT.U_DescarteMale)                  < 0 THEN 0 ELSE (BFT.U_DescartePolloMale + BFT.U_DescarteMale)                  END U_DescartePolloMale \
,CASE WHEN (BFT.U_Pro_RespiratorioPL + BFT.U_ProblemaRespiratorio)         < 0 THEN 0 ELSE (BFT.U_Pro_RespiratorioPL + BFT.U_ProblemaRespiratorio)         END U_Pro_RespiratorioPL \
,CASE WHEN (BFT.U_Pro_RespiratorioPLMale + BFT.U_ProblemaRespiratorioMale) < 0 THEN 0 ELSE (BFT.U_Pro_RespiratorioPLMale + BFT.U_ProblemaRespiratorioMale) END U_Pro_RespiratorioPLMale \
,CASE WHEN (BFT.U_EnteritisPollo + BFT.U_Enteritis)                        < 0 THEN 0 ELSE (BFT.U_EnteritisPollo + BFT.U_Enteritis)                        END U_EnteritisPollo \
,CASE WHEN (BFT.U_EnteritisPolloMale + BFT.U_EnteritisMale)                < 0 THEN 0 ELSE (BFT.U_EnteritisPolloMale + BFT.U_EnteritisMale)                END U_EnteritisPolloMale \
,CASE WHEN (BFT.U_Mat_caseosoPL + BFT.U_Material_caseoso)                  < 0 THEN 0 ELSE (BFT.U_Mat_caseosoPL + BFT.U_Material_caseoso)                  END U_Mat_caseosoPL \
,CASE WHEN (BFT.U_Mat_caseosoPLMale + BFT.U_Material_caseosoMale)          < 0 THEN 0 ELSE (BFT.U_Mat_caseosoPLMale + BFT.U_Material_caseosoMale)          END U_Mat_caseosoPLMale \
,CASE WHEN (BFT.U_PicajePollo + BFT.U_Picaje)                              < 0 THEN 0 ELSE (BFT.U_PicajePollo + BFT.U_Picaje)                              END U_PicajePollo \
,CASE WHEN (BFT.U_PicajePolloMale + BFT.U_PicajeMale)                      < 0 THEN 0 ELSE (BFT.U_PicajePolloMale + BFT.U_PicajeMale)                      END U_PicajePolloMale \
,CASE WHEN (BFT.U_ProlapsoPollo + BFT.U_Prolapso)                          < 0 THEN 0 ELSE (BFT.U_ProlapsoPollo + BFT.U_Prolapso)                          END U_ProlapsoPollo \
,CASE WHEN (BFT.U_ProlapsoPolloMale + BFT.U_ProlapsoMale)                  < 0 THEN 0 ELSE (BFT.U_ProlapsoPolloMale + BFT.U_ProlapsoMale)                  END U_ProlapsoPolloMale \
,CASE WHEN (BFT.U_PeritonitisPollo + BFT.U_Peritonitis)                    < 0 THEN 0 ELSE (BFT.U_PeritonitisPollo + BFT.U_Peritonitis)                    END U_PeritonitisPollo \
,CASE WHEN (BFT.U_PeritonitisPolloMale + BFT.U_PeritonitisMale)            < 0 THEN 0 ELSE (BFT.U_PeritonitisPolloMale + BFT.U_PeritonitisMale)            END U_PeritonitisPolloMale \
,CASE WHEN BFT.U_MaltratoPollo                                             < 0 THEN 0 ELSE BFT.U_MaltratoPollo END U_MaltratoPollo \
,CASE WHEN BFT.U_MaltratoPolloMale                                         < 0 THEN 0 ELSE BFT.U_MaltratoPolloMale END U_MaltratoPolloMale \
,CASE WHEN BFT.U_Erosion_de_molleja                                        < 0 THEN 0 ELSE BFT.U_Erosion_de_molleja END U_Erosion_de_molleja \
,CASE WHEN BFT.U_Erosion_de_mollejaMale                                    < 0 THEN 0 ELSE BFT.U_Erosion_de_mollejaMale END U_Erosion_de_mollejaMale \
,CASE WHEN (BFT.U_OnfalitisPollo + BFT.U_Onfalitis)                        < 0 THEN 0 ELSE (BFT.U_OnfalitisPollo + BFT.U_Onfalitis) END U_OnfalitisPollo \
,CASE WHEN (BFT.U_OnfalitisPolloMale + BFT.U_OnfalitisMale)                < 0 THEN 0 ELSE (BFT.U_OnfalitisPolloMale + BFT.U_OnfalitisMale) END U_OnfalitisPolloMale \
,CASE WHEN BFT.U_Error_de_sexo                                             < 0 THEN 0 ELSE BFT.U_Error_de_sexo END U_Error_de_sexo \
,CASE WHEN BFT.U_Error_de_sexoMale                                         < 0 THEN 0 ELSE BFT.U_Error_de_sexoMale END U_Error_de_sexoMale \
,CASE WHEN BFT.U_Postrado                                                  < 0 THEN 0 ELSE BFT.U_Postrado END U_Postrado \
,CASE WHEN BFT.U_PostradoMale                                              < 0 THEN 0 ELSE BFT.U_PostradoMale END U_PostradoMale \
,CASE WHEN BFT.U_Bajo_peso                                                 < 0 THEN 0 ELSE BFT.U_Bajo_peso END U_Bajo_peso \
,CASE WHEN BFT.U_Bajo_pesoMale                                             < 0 THEN 0 ELSE BFT.U_Bajo_pesoMale END U_Bajo_pesoMale \
,CASE WHEN BFT.U_PicajeCull                                                < 0 THEN 0 ELSE BFT.U_PicajeCull END U_PicajeCull \
,CASE WHEN BFT.U_PicajeCullMale                                            < 0 THEN 0 ELSE BFT.U_PicajeCullMale END U_PicajeCullMale \
,CASE WHEN BFT.U_CojosCull                                                 < 0 THEN 0 ELSE BFT.U_CojosCull END U_CojosCull \
,CASE WHEN BFT.U_CojosCullMale                                             < 0 THEN 0 ELSE BFT.U_CojosCullMale END U_CojosCullMale \
,CASE WHEN BFT.U_PatasAbiertas                                             < 0 THEN 0 ELSE BFT.U_PatasAbiertas END U_PatasAbiertas \
,CASE WHEN BFT.U_PatasAbiertasMale                                         < 0 THEN 0 ELSE BFT.U_PatasAbiertasMale END U_PatasAbiertasMale \
,CASE WHEN BFT.U_ProlapsoCull                                              < 0 THEN 0 ELSE BFT.U_ProlapsoCull END U_ProlapsoCull \
,CASE WHEN BFT.U_ProlapsoCullMale                                          < 0 THEN 0 ELSE BFT.U_ProlapsoCullMale END U_ProlapsoCullMale \
,CASE WHEN BFT.U_Fuer_de_post                                              < 0 THEN 0 ELSE BFT.U_Fuer_de_post END U_Fuer_de_post \
,CASE WHEN BFT.U_Fuer_de_postMale                                          < 0 THEN 0 ELSE BFT.U_Fuer_de_postMale END U_Fuer_de_postMale \
,CASE WHEN BFT.U_otrosCull                                                 < 0 THEN 0 ELSE BFT.U_otrosCull END U_otrosCull \
,CASE WHEN BFT.U_otrosCullMale                                             < 0 THEN 0 ELSE BFT.U_otrosCullMale END U_otrosCullMale \
,CASE WHEN BFT.U_Prolapso_PV                                               < 0 THEN 0 ELSE BFT.U_Prolapso_PV END U_Prolapso_PV \
,CASE WHEN BFT.U_Prolapso_PVMale                                           < 0 THEN 0 ELSE BFT.U_Prolapso_PVMale END U_Prolapso_PVMale \
,CASE WHEN BFT.U_Postrado_PV                                               < 0 THEN 0 ELSE BFT.U_Postrado_PV END U_Postrado_PV \
,CASE WHEN BFT.U_Postrado_PVMale                                           < 0 THEN 0 ELSE BFT.U_Postrado_PVMale END U_Postrado_PVMale \
,CASE WHEN BFT.U_PatasAbiertas_Pavo                                        < 0 THEN 0 ELSE BFT.U_PatasAbiertas_Pavo END U_PatasAbiertas_Pavo \
,CASE WHEN BFT.U_PatasAbiertas_PavoMale                                    < 0 THEN 0 ELSE BFT.U_PatasAbiertas_PavoMale END U_PatasAbiertas_PavoMale \
,CASE WHEN BFT.U_Otros_PV                                                  < 0 THEN 0 ELSE BFT.U_Otros_PV END U_Otros_PV \
,CASE WHEN BFT.U_Otros_PVMale                                              < 0 THEN 0 ELSE BFT.U_Otros_PVMale END U_Otros_PVMale \
,CASE WHEN BFT.U_FueraPostura_PV                                           < 0 THEN 0 ELSE BFT.U_FueraPostura_PV END U_FueraPostura_PV \
,CASE WHEN BFT.U_FueraPostura_PVMale                                       < 0 THEN 0 ELSE BFT.U_FueraPostura_PVMale END U_FueraPostura_PVMale \
,CASE WHEN BFT.U_BajoPeso_PV                                               < 0 THEN 0 ELSE BFT.U_BajoPeso_PV END U_BajoPeso_PV \
,CASE WHEN BFT.U_BajoPeso_PVMale                                           < 0 THEN 0 ELSE BFT.U_BajoPeso_PVMale END U_BajoPeso_PVMale \
,CASE WHEN BFT.U_DescartePicaje_PV                                         < 0 THEN 0 ELSE BFT.U_DescartePicaje_PV END U_DescartePicaje_PV \
,CASE WHEN BFT.U_DescartePicaje_PVMale                                     < 0 THEN 0 ELSE BFT.U_DescartePicaje_PVMale END U_DescartePicaje_PVMale \
,CASE WHEN BFT.U_DescarteCojo_PV                                           < 0 THEN 0 ELSE BFT.U_DescarteCojo_PV END U_DescarteCojo_PV \
,CASE WHEN BFT.U_DescarteCojo_PVMale                                       < 0 THEN 0 ELSE BFT.U_DescarteCojo_PVMale END U_DescarteCojo_PVMale \
,CASE WHEN BFT.U_SexoErrado_PV                                             < 0 THEN 0 ELSE BFT.U_SexoErrado_PV END U_SexoErrado_PV \
,CASE WHEN BFT.U_SexoErrado_PVMale                                         < 0 THEN 0 ELSE BFT.U_SexoErrado_PVMale END U_SexoErrado_PVMale \
,CASE WHEN BFT.U_ErrorSexo_PV                                              < 0 THEN 0 ELSE BFT.U_ErrorSexo_PV END U_ErrorSexo_PV \
,CASE WHEN BFT.U_Error_Sexo                                                < 0 THEN 0 ELSE BFT.U_Error_Sexo END U_Error_Sexo \
,CASE WHEN BFT.U_Error_SexoMale                                            < 0 THEN 0 ELSE BFT.U_Error_SexoMale END U_Error_SexoMale \
,CASE WHEN BFT.U_Cojo                                                      < 0 THEN 0 ELSE BFT.U_Cojo                                          END U_Cojo \
,CASE WHEN BFT.U_CojoMale                                                  < 0 THEN 0 ELSE BFT.U_CojoMale                                      END U_CojoMale \
,CASE WHEN BFT.U_Uratosis                                                  < 0 THEN 0 ELSE BFT.U_Uratosis                                      END U_Uratosis \
,CASE WHEN BFT.U_UratosisMale                                              < 0 THEN 0 ELSE BFT.U_UratosisMale                                  END U_UratosisMale \
,CASE WHEN BFT.U_Ascitis                                                   < 0 THEN 0 ELSE BFT.U_Ascitis                                       END U_Ascitis \
,CASE WHEN BFT.U_AscitisMale                                               < 0 THEN 0 ELSE BFT.U_AscitisMale                                   END U_AscitisMale \
,CASE WHEN BFT.U_Retencion_Yema                                            < 0 THEN 0 ELSE BFT.U_Retencion_Yema                                END U_Retencion_Yema \
,CASE WHEN BFT.U_Retencion_YemaMale                                        < 0 THEN 0 ELSE BFT.U_Retencion_YemaMale                            END U_Retencion_YemaMale \
,CASE WHEN BFT.U_Sangre_en_Ciego                                           < 0 THEN 0 ELSE BFT.U_Sangre_en_Ciego                               END U_Sangre_en_Ciego \
,CASE WHEN BFT.U_Sangre_en_CiegoMale                                       < 0 THEN 0 ELSE BFT.U_Sangre_en_CiegoMale                           END U_Sangre_en_CiegoMale \
,CASE WHEN BFT.U_Hidropericardio                                           < 0 THEN 0 ELSE BFT.U_Hidropericardio                               END U_Hidropericardio \
,CASE WHEN BFT.U_HidropericardioMale                                       < 0 THEN 0 ELSE BFT.U_HidropericardioMale                           END U_HidropericardioMale \
,CASE WHEN BFT.U_Higado_Hemorragico                                        < 0 THEN 0 ELSE BFT.U_Higado_Hemorragico                            END U_Higado_Hemorragico \
,CASE WHEN BFT.U_Higado_HemorragicoMale                                    < 0 THEN 0 ELSE BFT.U_Higado_HemorragicoMale                        END U_Higado_HemorragicoMale \
,CASE WHEN BFT.U_SCH                                                       < 0 THEN 0 ELSE BFT.U_SCH                                           END U_SCH \
,CASE WHEN BFT.U_SCHMale                                                   < 0 THEN 0 ELSE BFT.U_SCHMale                                       END U_SCHMale \
,CASE WHEN BFT.U_Muerte_Subita                                             < 0 THEN 0 ELSE BFT.U_Muerte_Subita                                 END U_Muerte_Subita \
,CASE WHEN BFT.U_Muerte_SubitaMale                                         < 0 THEN 0 ELSE BFT.U_Muerte_SubitaMale                             END U_Muerte_SubitaMale \
,CASE WHEN (BFT.U_Estres_Calor + BFT.`U_Estrés_Calor`)                     < 0 THEN 0 ELSE (BFT.U_Estres_Calor + BFT.`U_Estrés_Calor`)         END U_Estres_Calor \
,CASE WHEN (BFT.U_Estres_CalorMale + BFT.`U_Estrés_CalorMale`)             < 0 THEN 0 ELSE (BFT.U_Estres_CalorMale + BFT.`U_Estrés_CalorMale`) END U_Estres_CalorMale \
,CASE WHEN BFT.U_Pericarditis                                              < 0 THEN 0 ELSE BFT.U_Pericarditis                                  END U_Pericarditis \
,CASE WHEN BFT.U_PericarditisMale                                          < 0 THEN 0 ELSE BFT.U_PericarditisMale                              END U_PericarditisMale \
,CASE WHEN BFT.U_GalloCorrido                                              < 0 THEN 0 ELSE BFT.U_GalloCorrido                                  END U_GalloCorrido \
,CASE WHEN BFT.U_GalloCorridoMale                                          < 0 THEN 0 ELSE BFT.U_GalloCorridoMale                              END U_GalloCorridoMale \
,CASE WHEN BFT.U_Conformacion                                              < 0 THEN 0 ELSE BFT.U_Conformacion                                  END U_Conformacion \
,CASE WHEN BFT.U_ConformacionMale                                          < 0 THEN 0 ELSE BFT.U_ConformacionMale                              END U_ConformacionMale \
,CASE WHEN BFT.U_Higado_Graso                                              < 0 THEN 0 ELSE BFT.U_Higado_Graso                                  END U_Higado_Graso \
,CASE WHEN BFT.U_Higado_GrasoMale                                          < 0 THEN 0 ELSE BFT.U_Higado_GrasoMale                              END U_Higado_GrasoMale \
,CASE WHEN BFT.U_Tumor                                                     < 0 THEN 0 ELSE BFT.U_Tumor END U_Tumor \
,CASE WHEN BFT.U_TumorMale                                                 < 0 THEN 0 ELSE BFT.U_TumorMale END U_TumorMale \
,CASE WHEN BFT.U_DobleYema                                                 < 0 THEN 0 ELSE BFT.U_DobleYema END U_DobleYema \
,CASE WHEN BFT.U_DobleYemaProd                                             < 0 THEN 0 ELSE BFT.U_DobleYemaProd END U_DobleYemaProd \
,CASE WHEN BFT.U_Poroso                                                    < 0 THEN 0 ELSE BFT.U_Poroso END U_Poroso \
,CASE WHEN BFT.U_PorosoProd                                                < 0 THEN 0 ELSE BFT.U_PorosoProd END U_PorosoProd \
,CASE WHEN BFT.U_Chico                                                     < 0 THEN 0 ELSE BFT.U_Chico END U_Chico \
,CASE WHEN BFT.U_ChicoProd                                                 < 0 THEN 0 ELSE BFT.U_ChicoProd END U_ChicoProd \
,CASE WHEN BFT.U_Sucio                                                     < 0 THEN 0 ELSE BFT.U_Sucio END U_Sucio \
,CASE WHEN BFT.U_SucioProd                                                 < 0 THEN 0 ELSE BFT.U_SucioProd END U_SucioProd \
,CASE WHEN BFT.U_Deforme                                                   < 0 THEN 0 ELSE BFT.U_Deforme END U_Deforme \
,CASE WHEN BFT.U_DeformeProd                                               < 0 THEN 0 ELSE BFT.U_DeformeProd END U_DeformeProd \
,CASE WHEN BFT.U_CascaraDebil                                              < 0 THEN 0 ELSE BFT.U_CascaraDebil END U_CascaraDebil \
,CASE WHEN BFT.U_CascaraDebilProd                                          < 0 THEN 0 ELSE BFT.U_CascaraDebilProd END U_CascaraDebilProd \
,CASE WHEN BFT.U_Palido                                                    < 0 THEN 0 ELSE BFT.U_Palido END U_Palido \
,CASE WHEN BFT.U_PalidoProd                                                < 0 THEN 0 ELSE BFT.U_PalidoProd END U_PalidoProd \
,CASE WHEN BFT.U_Quinado                                                   < 0 THEN 0 ELSE BFT.U_Quinado END U_Quinado \
,CASE WHEN BFT.U_QuinadoProd                                               < 0 THEN 0 ELSE BFT.U_QuinadoProd               END U_QuinadoProd \
,CASE WHEN BFT.U_Farfara                                                   < 0 THEN 0 ELSE BFT.U_Farfara                   END U_Farfara \
,CASE WHEN BFT.U_FarfaraProd                                               < 0 THEN 0 ELSE BFT.U_FarfaraProd               END U_FarfaraProd \
,CASE WHEN BFT.U_HuevoMarron                                               < 0 THEN 0 ELSE BFT.U_HuevoMarron               END U_HuevoMarron \
,CASE WHEN BFT.U_HuevoMarronProd                                           < 0 THEN 0 ELSE BFT.U_HuevoMarronProd           END U_HuevoMarronProd \
,CASE WHEN BFT.U_Roto                                                      < 0 THEN 0 ELSE BFT.U_Roto                      END U_Roto \
,CASE WHEN BFT.U_RotoProd                                                  < 0 THEN 0 ELSE BFT.U_RotoProd                  END U_RotoProd \
,CASE WHEN BFT.U_Invendible                                                < 0 THEN 0 ELSE BFT.U_Invendible                END U_Invendible \
,CASE WHEN BFT.U_InvendibleProd                                            < 0 THEN 0 ELSE BFT.U_InvendibleProd            END U_InvendibleProd \
,CASE WHEN BFT.U_Sangre                                                    < 0 THEN 0 ELSE BFT.U_Sangre                    END U_Sangre \
,CASE WHEN BFT.U_SangreProd                                                < 0 THEN 0 ELSE BFT.U_SangreProd                END U_SangreProd \
,CASE WHEN BFT.U_Incubable                                                 < 0 THEN 0 ELSE BFT.U_Incubable                 END U_Incubable \
,CASE WHEN BFT.U_IncubableProd                                             < 0 THEN 0 ELSE BFT.U_IncubableProd             END U_IncubableProd \
,CASE WHEN BFT.U_Comercial                                                 < 0 THEN 0 ELSE BFT.U_Comercial                 END U_Comercial \
,CASE WHEN BFT.U_ComercialProd                                             < 0 THEN 0 ELSE BFT.U_ComercialProd             END U_ComercialProd \
,CASE WHEN BFT.U_Oviducto_Poliquistico                                     < 0 THEN 0 ELSE BFT.U_Oviducto_Poliquistico     END U_Oviducto_Poliquistico \
,CASE WHEN BFT.U_Oviducto_PoliquisticoMale                                 < 0 THEN 0 ELSE BFT.U_Oviducto_PoliquisticoMale END U_Oviducto_PoliquisticoMale \
,CASE WHEN BFT.U_No_Viable                                                 < 0 THEN 0 ELSE BFT.U_No_Viable                 END U_No_Viable \
,CASE WHEN BFT.U_No_ViableMale                                             < 0 THEN 0 ELSE BFT.U_No_ViableMale             END U_No_ViableMale \
,CASE WHEN BFT.U_SuperJumbo                                                < 0 THEN 0 ELSE BFT.U_SuperJumbo                END U_SuperJumbo \
,CASE WHEN BFT.U_SuperJumboProd                                            < 0 THEN 0 ELSE BFT.U_SuperJumboProd            END U_SuperJumboProd \
,CASE WHEN BFT.U_Despatarrados                                             < 0 THEN 0 ELSE BFT.U_Despatarrados             END U_Despatarrados \
,CASE WHEN BFT.U_DespatarradosMale                                         < 0 THEN 0 ELSE BFT.U_DespatarradosMale         END U_DespatarradosMale \
,CASE WHEN BFT.TotalEggsProd                                               < 0 THEN 0 ELSE BFT.TotalEggsProd               END TotalEggsProd \
,CASE WHEN BFT.TotalEggsInv                                                < 0 THEN 0 ELSE BFT.TotalEggsInv                END TotalEggsInv \
,CASE WHEN BFT.U_HI_de_piso                                                < 0 THEN 0 ELSE BFT.U_HI_de_piso                END U_HI_de_piso \
,VB.ProteinEntitiesIRN ProteinEntitiesIRN_VB,VB.ProteinProductsAnimalsIRN_OS,VB.ProteinProductsIRN_OS,VB.ProductNo_OS,VB.AnimalType_OS \
,(CASE WHEN FirstDatePlaced IS NULL THEN 0 WHEN date_format(cast(VB.FirstDatePlaced as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstDatePlaced as timestamp)))+6 END)/7 AS FirstDatePlacedAge \
,(CASE WHEN FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(VB.FirstHatchDate as timestamp) ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+6 END)/7 AS FirstHatchDateAge \
,(CASE WHEN AvgDatePlaced   IS NULL THEN 0 WHEN date_format(cast(VB.AvgDatePlaced as timestamp)  ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgDatePlaced   as timestamp)))+6 END)/7 AS AvgDatePlacedAge \
,(CASE WHEN AvgHatchDate    IS NULL THEN 0 WHEN date_format(cast(VB.AvgHatchDate as timestamp)   ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgHatchDate    as timestamp)))+6 END)/7 AS AvgHatchDateAge \
,(CASE WHEN FirstDatePlaced IS NULL THEN 0 WHEN date_format(cast(VB.FirstDatePlaced as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstDatePlaced as timestamp))    END)   AS FirstDatePlacedAgesin7 \
,(CASE WHEN FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(VB.FirstHatchDate as timestamp) ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp))    END)   AS FirstHatchDateAgesin7 \
,(CASE WHEN AvgDatePlaced   IS NULL THEN 0 WHEN date_format(cast(VB.AvgDatePlaced as timestamp)  ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgDatePlaced   as timestamp))    END)   AS AvgDatePlacedAgesin7 \
,(CASE WHEN AvgHatchDate    IS NULL THEN 0 WHEN date_format(cast(VB.AvgHatchDate as timestamp)   ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE  datediff(cast(BFT.eventdate as timestamp),cast(VB.AvgHatchDate    as timestamp))    END)   AS AvgHatchDateAgesin7 \
,(CASE WHEN FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(VB.FirstHatchDate as timestamp) ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+2 END)/7 AS FirstHatchDateAge1 \
,(CASE WHEN FirstHatchDate  IS NULL THEN 0 WHEN date_format(cast(VB.FirstHatchDate as timestamp) ,'yyyy-MM-dd') = '1899-11-30' THEN 0 ELSE (datediff(cast(BFT.eventdate as timestamp),cast(VB.FirstHatchDate  as timestamp)))+2 END)   AS FirstHatchDateAge2 \
,VB.AvgHatchDate,VB.AvgDatePlaced,VB.FirstHatchDate,VB.FirstDatePlaced,VB.FirstDateSold,VB.LastDateSold,VB.AvgDateSold,VB.PenNo,VB.EntityNo,VB.FarmNo FarmNo_VB,VB.HouseNo,VB.IRN IRN_VB \
,VB.ComplexEntityNo,VB.HouseNoPenNo,VB.FarmName FarmName_VB,VB.ProteinFarmsIRN,VB.FarmType FarmType_VB,VB.GrowoutNo,VB.ProteinGrowoutCodesIRN ProteinGrowoutCodesIRN_VB,VB.AnimalType \
,VB.ProductNo,VB.ProteinHousesIRN ProteinHousesIRN_VB,VB.BreedNo,VB.ProteinBreedCodesIRN ProteinBreedCodesIRN_VB,VB.GRN,VB.ProteinCostCentersIRN ProteinCostCentersIRN_VB,VB.FarmStage FarmStage_VB \
,VB.Status Status_VB,VB.GrowoutName,VB.ProteinProductsIRN ProteinProductsIRN_VB,VB.BreedName,VB.ProteinProductsAnimalsIRN ProteinProductsAnimalsIRN_VB \
,VB.ProteinStandardVersionsIRN ProteinStandardVersionsIRN_VB,VB.FeedMillName,VB.ProteinFacilityFeedMillsIRN ProteinFacilityFeedMillsIRN_VB,VB.StandardName,VB.StandardNo,VB.ProductName,VB.ProductName_OS \
,VB.ProteinProductsAnimalsIRN_Progeny,VB.ProteinBreedCodesIRN_Progeny,VB.ProgenyProductNo,VB.ProgenyBreedNo,VB.MaleProductNo,VB.ProgenyProductName,VB.DateCap,VB.MalesCap,VB.HensCap,VB.GenerationCode \
,PF.FarmName FarmName_PF,PF.FarmNo FarmNo_PF,PF.FarmStage FarmStage_PF,PF.FarmType FarmType_PF,PF.IRN IRN_PF,PF.ProteinCostCentersIRN ProteinCostCentersIRN_PF,PF.ProteinCountiesIRN \
,PF.ProteinFacilityFeedMillsIRN ProteinFacilityFeedMillsIRN_PF,PF.ProteinFacilityPlantsIRN,PF.ProteinGrowoutCodesIRN ProteinGrowoutCodesIRN_PF,VFFF.IRN IRN_VFFF \
,VFFF.FmimFeedTypesIRN FmimFeedTypesIRN_VFFF,VFFF.FormulaNo FormulaNo_VFFF,VFFF.FormulaType FormulaType_VFFF,VFFF.FeedTypeNo FeedTypeNo_VFFF,VFFF.FeedTypeName FeedTypeName_VFFF \
,VFFF.ActiveFlag ActiveFlag_VFFF,VFFF.FormulaName FormulaName_VFFF,VFFF.FarmType FarmType_VFFF,VFFF.ProteinProductsIRN ProteinProductsIRN_VFFF,VFFF.UnitsPer UnitsPer_VFFF \
,VFFM.IRN IRN_VFFM,VFFM.FmimFeedTypesIRN FmimFeedTypesIRN_VFFM,VFFM.FormulaNo FormulaNo_VFFM,VFFM.FormulaType FormulaType_VFFM,VFFM.FeedTypeNo FeedTypeNo_VFFM \
,VFFM.FeedTypeName FeedTypeName_VFFM,VFFM.ActiveFlag ActiveFlag_VFFM,VFFM.FormulaName FormulaName_VFFM,VFFM.FarmType FarmType_VFFM,VFFM.ProteinProductsIRN ProteinProductsIRN_VFFM \
,VFFM.UnitsPer UnitsPer_VFFM,FTF.FeedTypeNo FeedTypeNo_FTF,FTF.FarmType FarmType_FTF,FTF.IRN IRN_FTF,FTF.FeedTypeName FeedTypeName_FTF,FTF.Sex Sex_FTF,FTM.FeedTypeNo FeedTypeNo_FTM \
,FTM.FarmType FarmType_FTM,FTM.IRN IRN_FTM,FTM.FeedTypeName FeedTypeName_FTM,FTM.Sex Sex_FTM,FirstDateMovedIn,BFT.Transfermode,FFTD.Price \
FROM {database_name}.si_bimfieldtrans AS BFT \
LEFT OUTER JOIN {database_name}.si_mvbimentities AS VB ON BFT.ProteinEntitiesIRN = VB.ProteinEntitiesIRN \
LEFT OUTER JOIN {database_name}.si_proteinfarms AS PF ON VB.ProteinFarmsIRN = PF.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas AS VFFF ON BFT.FmimFeedFormulasIRN_F = VFFF.IRN \
LEFT OUTER JOIN {database_name}.si_mvfmimfeedformulas AS VFFM ON BFT.FmimFeedFormulasIRN_M = VFFM.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes AS FTF ON BFT.FmimFeedTypesIRN_Female = FTF.IRN \
LEFT OUTER JOIN {database_name}.si_fmimfeedtypes AS FTM ON BFT.FmimFeedTypesIRN_Male = FTM.IRN \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtrans \
                ) as FFT ON BFT.U_TrazSAP = FFT.RefNo \
LEFT OUTER JOIN (select * from {database_name}.si_fmimfeedtransdetails \
                ) FFTD on FFT.IRN = FFTD.FmimFeedTransIRN \
WHERE BFT.Transfermode = 3 AND VB.SpeciesType in (1,2)")

#date_format(CAST(BFT.EventDate AS timestamp),'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 12 MONTH), 'yyyyMM')
print('carga df_Reprod_Trans2')
df_Reprod_Trans =df_Reprod_Trans1.union(df_Reprod_Trans2)
#df_Reprod_Trans.createOrReplaceTempView("Reprod_Trans")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Trans"
}
df_Reprod_Trans.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Trans")

print('inicia Reprod_Trans')
# Alojamiento Reproductora

# Lista de Padre
# Se crea la tabla temporal para insertar todos los Lotes padres que están relacionado a un Lote principal
df_ReproductoraPadreTemp = spark.sql(f"SELECT A.ComplexEntityNo,substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNoGalpon, D.ComplexEntityNo ComplexEntityNoPadre \
FROM (select * from {database_name}.si_mvhimchicktranshouses) A \
LEFT JOIN {database_name}.si_mvhimchicktransparentdetail C ON A.ProteinEntitiesIRN = C.ProteinEntitiesIRN and C.HimChickTransHousesIRN = A.HimChickTransHousesIRN \
LEFT JOIN {database_name}.si_mvproteinentities D ON C.ProteinEntitiesIRN_Parent = D.IRN \
WHERE A.ComplexEntityNo NOT LIKE 'P%' AND A.ComplexEntityNo NOT LIKE 'V%' ")
                                      
#df_ReproductoraPadreTemp.createOrReplaceTempView("ReproductoraPadre")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ReproductoraPadre"
}
df_ReproductoraPadreTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ReproductoraPadre")

print('inicia ReproductoraPadre')
df_ListaTemp = spark.sql(f"SELECT ComplexEntityNoGalpon, concat_ws(',' , collect_list( DISTINCT ComplexEntityNoPadre)) ComplexEntityNoPadre \
FROM {database_name}.ReproductoraPadre group by ComplexEntityNoGalpon")
#df_ListaTemp.createOrReplaceTempView("ListaTemp")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ListaTemp"
}
df_ListaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ListaTemp")

print('inicia ListaTemp')
#Se crea la tabla temporal para agrupar los lotes padres en una lista 
df_ListaPadreTemp = spark.sql(f"SELECT A.ComplexEntityNo, A.ComplexEntityNoGalpon, \
B.ComplexEntityNoPadre ListaPadre \
FROM {database_name}.ReproductoraPadre A \
LEFT JOIN {database_name}.ListaTemp B ON A.ComplexEntityNoGalpon = B.ComplexEntityNoGalpon \
GROUP BY A.ComplexEntityNo,A.ComplexEntityNoGalpon,B.ComplexEntityNoPadre")
#df_ListaPadreTemp.createOrReplaceTempView("ListaPadre")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ListaPadre"
}
df_ListaPadreTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ListaPadre")

print('inicia ListaPadre')
#Inserta en la tabla temporal los datos de alojamiento reproductora (Pollo y Huevo) 
df_IngresoReproductora = spark.sql(f"SELECT PE.ComplexEntityNo \
,(select distinct pk_empresa from {database_name}.lk_empresa where cempresa=1) as pk_empresa\
,nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0')) pk_administrador \
,nvl(LDV.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
,nvl(LPL.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
,nvl(LLT.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
,nvl(LGP.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
,nvl(LSX.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0))pk_sexo \
,nvl(LZN.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
,nvl(LSZ.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona \
,nvl(LES.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha = cast('1899-11-30' as date))) pk_tiempo \
,nvl(LT.fecha,cast('1899-11-30' as date)) fecha \
,BIN.CreationUserId AS usuario_creacion \
,BIN.Quantity * 1.000000 AS Inventario \
,nvl(LIC.pk_incubadora,(select pk_incubadora from {database_name}.lk_incubadora where cincubadora='0')) pk_incubadora \
,nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) AS pk_proveedor \
,LP.ListaPadre \
FROM (select * from {database_name}.si_bimentityinventory) BIN \
LEFT JOIN {database_name}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_plantel LPL ON LPL.IRN = CAST(PE.ProteinFarmsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.si_proteincostcenters PCC ON CAST(PCC.IRN AS VARCHAR(50)) = LPL.ProteinCostCentersIRN \
LEFT JOIN {database_name}.lk_division LDV ON LDV.IRN = CAST(PCC.ProteinDivisionsIRN AS VARCHAR (50)) \
LEFT JOIN {database_name}.lk_lote LLT ON LLT.pk_plantel=LPL.pk_plantel AND LLT.nlote = PE.EntityNo AND LLT.activeflag IN (0,1) \
LEFT JOIN {database_name}.lk_galpon LGP ON LGP.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) AND LGP.activeflag IN (false,true) \
LEFT JOIN {database_name}.lk_sexo LSX ON LSX.csexo = Pe.PenNo \
LEFT JOIN {database_name}.lk_zona LZN ON LZN.pk_zona = LPL.pk_zona \
LEFT JOIN {database_name}.lk_subzona LSZ ON LSZ.pk_subzona = LPL.pk_subzona \
LEFT JOIN {database_name}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRN = CAST(PE.ProteinProductsIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd') =date_format(BIN.xDate,'yyyyMMdd') \
LEFT JOIN {database_name}.si_mvhimchicktrans MHT ON MHT.ProteinEntitiesIRN = PE.IRN AND BIN.Quantity=MHT.HeadPlaced AND MHT.RefNo = BIN.RefNo \
LEFT JOIN {database_name}.lk_incubadora LIC ON LIC.IRN=CAST(MHT.ProteinFacilityHatcheriesIRN AS VARCHAR(50)) \
LEFT JOIN {database_name}.si_mvbimfarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN \
LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo \
LEFT JOIN {database_name}.ListaPadre LP ON PE.ComplexEntityNo = LP.ComplexEntityNo \
WHERE BIN.EntityHistoryFlag = true AND BIN.SourceCode in ('CHICK','BimEntities') AND BIN.TransferMode <> 3 ")

#df_IngresoReproductora.createOrReplaceTempView("IngresoReproductora")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/IngresoReproductora"
}
df_IngresoReproductora.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.IngresoReproductora")

#date_format(LT.fecha,'yyyyMM')  >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 12 MONTH), 'yyyyMM')
print('carga IngresoReproductora')
#Inserta los datos de la tabla temporal agrupados por ComplexEntityNo. Para que se muestre una línea por ComplexEntityNo.
#INSERT INTO DMPECUARIO.ft_Reprod_Ingreso_Total
df_ft_Reprod_Ingreso_Total = spark.sql(f"SELECT ComplexEntityNo,pk_empresa,pk_administrador,pk_division,pk_plantel,pk_lote,pk_galpon \
,pk_sexo,pk_zona,pk_subzona,pk_especie,pk_producto \
,MIN(pk_tiempo) pk_tiempo \
,MIN(fecha) fecha \
,pk_proveedor \
,SUM(Inventario * 1.000000)Inventario \
,ListaPadre \
FROM {database_name}.IngresoReproductora \
WHERE pk_empresa = 1 \
GROUP BY ComplexEntityNo,pk_empresa,pk_administrador,pk_division,pk_plantel,pk_lote,pk_galpon, \
pk_sexo,pk_zona, pk_subzona,pk_especie,pk_producto,pk_proveedor,ListaPadre")
#df_ft_Reprod_Ingreso_Total.createOrReplaceTempView("ft_Reprod_Ingreso_TotalTemp")
print('inicio de carga df_ft_Reprod_Ingreso_Total')
# Verificar si la tabla gold ya existe - ft_Reprod_Ingreso_Total
#gold_table = spark.read.format("parquet").load(path_target6)   
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes6 = spark.read.format("parquet").load(path_target6)
    datos_existentes6 = True
    logger.info(f"Datos existentes de ft_Reprod_Ingreso_Total cargados: {df_existentes6.count()} registros")
except:
    datos_existentes6 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Ingreso_Total")

if datos_existentes6:
    existing_data6 = spark.read.format("parquet").load(path_target6)
    data_after_delete6 = existing_data6.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data6 = df_ft_Reprod_Ingreso_Total.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data6 = filtered_new_data6.union(data_after_delete6)                             
   
    cant_ingresonuevo6 = filtered_new_data6.count()
    cant_total6 = final_data6.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Ingreso_TotalTemporal"
    }
    final_data6.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Ingreso_TotalTemporal")
    
    
    #schema = existing_data.schema
    final_data6_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Ingreso_TotalTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Ingreso_Total"
    }
    final_data6_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Ingreso_Total")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Ingreso_Total : {cant_ingresonuevo6}")
    print(f"Total de registros en la tabla ft_Reprod_Ingreso_Total : {cant_total6}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Ingreso_TotalTemporal')
    print(f"Tabla ft_Reprod_Ingreso_TotalTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Ingreso_Total"
    }
    df_ft_Reprod_Ingreso_Total.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Ingreso_Total")
#Mortalidad

#Inserta en la tabla temporal todos los consumos por tipo de alimento. Se incluye los consumos que tienen edad 0
df_Consumo = spark.sql(f"SELECT ComplexEntityNo,IRN_BFT,pk_diasvida,pk_semanavida,pk_alimentoF,pk_alimentoM,FmimFeedTypesIRN_VFFF,FmimFeedTypesIRN_VFFM,sum(FeedConsumed) Consumo \
                        FROM ( \
                              SELECT \
                               M.ComplexEntityNo \
                              ,IRN_BFT \
                              ,M.FirstHatchDateAgesin7 pk_diasvida \
                              ,S.pk_semanavida \
                              ,FALF.pk_alimento pk_alimentoF \
                              ,FALM.pk_alimento pk_alimentoM \
                              ,FeedConsumedF \
                              ,FeedConsumedM \
                              ,(FeedConsumedF + FeedConsumedM) FeedConsumed \
                              ,M.FmimFeedTypesIRN_VFFF \
                              ,M.FmimFeedTypesIRN_VFFM \
                              FROM {database_name}.Reprod_Trans M \
                              LEFT JOIN {database_name}.lk_alimento FALF ON CAST(FALF.IRN as varchar(50)) = CAST(M.FmimFeedTypesIRN_VFFF AS varchar (50)) \
                              LEFT JOIN {database_name}.lk_alimento FALM ON CAST(FALM.IRN as varchar(50)) = CAST(M.FmimFeedTypesIRN_VFFM AS varchar (50)) \
                              LEFT JOIN {database_name}.lk_diasvida D ON D.FirstHatchDateAge = cast(M.FirstHatchDateAge as int) \
                              LEFT JOIN {database_name}.lk_semanavida S ON S.pk_semanavida = D.pk_semanavida \
                              WHERE (FeedConsumedF + FeedConsumedM) <> 0.00 \
                              )A \
                        GROUP BY ComplexEntityNo,IRN_BFT,pk_diasvida,pk_semanavida,pk_alimentoF,pk_alimentoM,FmimFeedTypesIRN_VFFF,FmimFeedTypesIRN_VFFM")
#,M.FirstHatchDateAgesin7 pk_diasvida \


#df_Consumo.createOrReplaceTempView("consumo")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/consumo"
}
df_Consumo.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.consumo")

print('carga consumo')

df_FechaMaximaTemp = spark.sql(f"select max(cast(EventDate as timestamp)) FechaMax, ComplexEntityNo from {database_name}.Reprod_Trans group by ComplexEntityNo")
df_FechaMaximaTemp.createOrReplaceTempView("FechaMaxima")
df_FechaCapitalizacionMaximaTemp = spark.sql(f"select max(cast(DateCap as timestamp)) FechaCapitalizacionMax, ComplexEntityNo from {database_name}.Reprod_Trans \
                                               group by ComplexEntityNo")
df_FechaCapitalizacionMaximaTemp.createOrReplaceTempView("FechaCapitalizacionMaxima")

print('carga vistas')

##Se inserta las dimensiones (id) y métricas de reproductora.
#df_Reprod_Reproductora = spark.sql(f"SELECT MO.IRN_BFT as BrimFieldTransIRN,MO.ProteinEntitiesIRN_BFT as ProteinEntitiesIRN,MO.ProteinGrowoutCodesIRN_VB as ProteinGrowoutCodesIRN,MO.ProteinFarmsIRN, \
#                                    MO.ProteinCostCentersIRN_VB as ProteinCostCentersIRN,MO.FmimFeedTypesIRN_Female,MO.FmimFeedTypesIRN_Male,MO.FmimFeedTypesIRN_VFFF,MO.FmimFeedTypesIRN_VFFM, \
#                                    PCC.ProteinDivisionsIRN,PE.ProteinHousesIRN,PE.ProteinBreedCodesIRN,PE.ProteinStandardVersionsIRN,PE.ProteinTechSupervisorsIRN,PE.ProteinProductsAnimalsIRN, \
#                                    PSV.ProteinStandardsIRN, \
#                                    nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha = cast('1899-11-30' as date))) pk_tiempo, \
#                                    nvl(LT.fecha,cast('1899-11-30' as date)) fecha, \
#                                    nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) as pk_division, \
#                                    nvl(LP.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) as pk_zona, \
#                                    nvl(LP.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) as pk_subzona, \
#                                    nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) as pk_plantel, \
#                                    nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) as pk_lote, \
#                                    nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) as pk_galpon, \
#                                    nvl(LS.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo, \
#                                    CASE WHEN isnull(MO.DateCap) = true THEN 1 WHEN cast(MO.EventDate as timestamp) < cast(MO.DateCap as timestamp) THEN 1 ELSE 3 END pk_etapa, \
#                                    nvl(LST.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard, \
#                                    nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto, \
#                                    nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto, \
#                                    nvl(GCO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name}.lk_grupoconsumo where cgrupoconsumo=0)) as pk_grupoconsumo, \
#                                    nvl(LEP.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie, \
#                                    nvl(LES.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado, \
#                                    nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador, \
#                                    nvl(FALF.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimentof, \
#                                    nvl(FALM.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimentom, \
#                                    nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) as pk_proveedor, \
#                                    nvl(LGE.pk_generacion,(select pk_generacion from {database_name}.lk_generacion where cgeneracion=0)) as pk_generacion, \
#                                    DV.pk_semanavida pk_semanavida, \
#                                    DV.pk_diasvida pk_diasvida, \
#                                    MO.ComplexEntityNo,MO.FirstHatchDate FechaNacimiento, \
#                                    round((FirstHatchDateAgesin7*1.0)/7,0) + ((FirstHatchDateAgesin7*1.0)%7)/10 as Edad, \
#                                    date_format(MO.LastDateSold,'yyyyMMdd') as FechaCierre, \
#                                    date_format(MO.FirstDateSold,'yyyyMMdd') as FechaInicioSaca, \
#                                    date_format(MO.LastDateSold,'yyyyMMdd') as FechaFinSaca, \
#                                    MO.EventDate,MO.xDate,LG.Area as AreaGalpon,LT.semanaanioReprod semanaReprod,LT.semanaReprod semanaReprod2, \
#                                    MO.MortalityF,MO.MortalityM,MO.UniformityF,MO.UniformityM,MO.U_AccidentadosPollo,MO.U_AccidentadosPolloMale, \
#                                    U_No_Viable,U_No_ViableMale,U_Higado_Hemorragico,U_Higado_HemorragicoMale,U_Pro_RespiratorioPL,U_Pro_RespiratorioPLMale, \
#                                    U_SCH,U_SCHMale,U_EnteritisPollo,U_EnteritisPolloMale,U_Ascitis,U_AscitisMale,U_Estres_Calor,U_Estres_CalorMale, \
#                                    U_Muerte_Subita,U_Muerte_SubitaMale,U_Hidropericardio,U_HidropericardioMale,U_Uratosis,U_UratosisMale,U_Mat_caseosoPL, \
#                                    U_Mat_caseosoPLMale,U_OnfalitisPollo,U_OnfalitisPolloMale,U_Retencion_Yema,U_Retencion_YemaMale,U_Erosion_de_molleja, \
#                                    U_Erosion_de_mollejaMale,U_Sangre_en_Ciego,U_Sangre_en_CiegoMale,U_Pericarditis,U_PericarditisMale,U_PicajePollo, \
#                                    U_PicajePolloMale,U_ProlapsoPollo,U_ProlapsoPolloMale,U_PeritonitisPollo,U_PeritonitisPolloMale,U_MaltratoPollo, \
#                                    U_MaltratoPolloMale,U_Error_Sexo,U_Error_SexoMale,U_Cojo,U_CojoMale,U_Despatarrados,U_DespatarradosMale, \
#                                    MO.GRN,BSD.U_FeedPerHenWK,BSD.U_FeedPerMaleWK,BSD.U_PercentProdTEWK,BSD.U_PercentProdHEWK,BSD.U_HenWeight, \
#                                    BSD.U_MaleWeight,BSD.U_TEPerHHACM,BSD.U_HEPerHHACM,BSD.U_HenMortWK,BSD.U_MaleMortWK,BSD.U_HenMortACM, \
#                                    BSD.U_MaleMortACM,BSD.U_EggWeight,MO.FeedConsumedF,MO.FeedConsumedM,MO.TotalEggsProd ProdHT, \
#                                    MO.HatchEggsProd ProdHI,MO.EggWeight PesoHvo,MO.WeightF PesoH,MO.WeightM PesoM,MO.U_PorosoProd,MO.U_SucioProd, \
#                                    MO.U_SangreProd,MO.U_DeformeProd,MO.U_CascaraDebilProd,MO.U_RotoProd,MO.U_FarfaraProd,MO.U_InvendibleProd, \
#                                    MO.U_ChicoProd,MO.U_DobleYemaProd,MO.U_HI_de_piso,PE.U_UnidadesSeleccionadas,MO.DateCap,MO.MalesCap,MO.HensCap \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 30 OR A.pk_alimentoM = 30)),0) AS AlimInicio \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 15 OR A.pk_alimentoM = 15)),0) AS AlimCrecimiento \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 64 OR A.pk_alimentoM = 64)),0) AS AlimPreReprod \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 72 OR A.pk_alimentoM = 72)),0) AS AlimReprodI \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 73 OR A.pk_alimentoM = 73)),0) AS AlimReprodII \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 71 OR A.pk_alimentoM = 71)),0) AS AlimReprodMacho \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 10 OR A.pk_alimentoM = 10)),0) AS PosturaCrecIRP \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 63 OR A.pk_alimentoM = 63)),0) AS PosturaPreInicioRP \
#                                    ,MO.U_Ruptura_Aortica,MO.U_Ruptura_AorticaMale,MO.U_Bazo_Moteado,MO.U_Bazo_MoteadoMale \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 58 OR A.pk_alimentoM = 58)),0) AS PreInicialPavas \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 28 OR A.pk_alimentoM = 28)),0) AS InicialPavosRP \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF =  8 OR A.pk_alimentoM =  8)),0) AS CreIPavas \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 11 OR A.pk_alimentoM = 11)),0) AS CrecIIPavas \
#                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.FirstHatchDateAge = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 38 OR A.pk_alimentoM = 38)),0) AS MantPavas \
#                                    ,MO.ProteinProductsIRN_VFFF,MO.ProteinProductsIRN_VFFM,MO.Price,FM.FechaMax,FCM.FechaCapitalizacionMax \
#                                    from {database_name}.Reprod_Trans MO \
#                                    LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd') = date_format(cast(MO.EventDate as timestamp),'yyyyMMdd') \
#                                    LEFT JOIN {database_name}.si_proteincostcenters PCC ON MO.ProteinCostCentersIRN_VB = PCC.IRN \
#                                    LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50)) \
#                                    LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50)) \
#                                    LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1) \
#                                    LEFT JOIN {database_name}.si_proteinentities PE ON PE.IRN = MO.ProteinEntitiesIRN_BFT \
#                                    LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and lg.activeflag in (false,true) \
#                                    LEFT JOIN {database_name}.lk_sexo LS ON LS.csexo = rtrim(MO.PenNo) \
#                                    LEFT JOIN {database_name}.si_ProteinStandardVersions PSV ON PSV.IRN = PE.ProteinStandardVersionsIRN \
#                                    LEFT JOIN {database_name}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50)) \
#                                    LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50)) \
#                                    LEFT JOIN {database_name}.lk_tipoproducto TP ON tp.ntipoproducto=lpr.grupoproducto \
#                                    LEFT JOIN {database_name}.lk_grupoconsumo GCO ON LPR.pk_grupoconsumo = GCO.pk_grupoconsumo \
#                                    LEFT JOIN {database_name}.lk_especie LEP ON LEP.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
#                                    LEFT JOIN {database_name}.lk_estado LES ON LES.cestado=PE.Status \
#                                    LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
#                                    LEFT JOIN {database_name}.lk_alimento FALF ON FALF.IRN = CAST(MO.FmimFeedTypesIRN_VFFF AS varchar (50)) \
#                                    LEFT JOIN {database_name}.lk_alimento FALM ON FALM.IRN = CAST(MO.FmimFeedTypesIRN_VFFM AS varchar (50)) \
#                                    LEFT JOIN {database_name}.si_mvbimfarms MVBF ON MVBF.ProteinFarmsIRN = MO.ProteinFarmsIRN \
#                                    LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MVBF.VendorNo \
#                                    LEFT JOIN {database_name}.si_bimstandardsdata BSD ON BSD.ProteinStandardVersionsIRN = PSV.IRN and BSD.age = MO.FirstHatchDateAge \
#                                    LEFT JOIN {database_name}.lk_generacion LGE ON MO.GenerationCode = LGE.cgeneracion \
#                                    LEFT JOIN {database_name}.lk_diasvida DV on MO.FirstHatchDateAgesin7 = DV.FirstHatchDateAge \
#                                    LEFT JOIN FechaMaxima FM ON MO.ComplexEntityNo = FM.ComplexEntityNo \
#                                    LEFT JOIN FechaCapitalizacionMaxima FCM ON FCM.ComplexEntityNo = MO.ComplexEntityNo \
#                                    WHERE MO.FirstHatchDateAge >= 0 and (date_format(CAST(MO.EventDate AS timestamp),'yyyyMM') >= \
#                                          date_format(add_months(trunc(current_date, 'month'),-12),'yyyyMM'))")
##cast(MO.FirstHatchDateAge as int) pk_semanavida, \
## MO.FirstHatchDateAgesin7 pk_diasvida, \
##44,76,41,38,71,54,33,18,12,49,46,37,51
#
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Reproductora"
#}
#df_Reprod_Reproductora.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.Reprod_Reproductora")
#
#print('carga Reprod_Reproductora')
#Se inserta las dimensiones (id) y métricas de reproductora.
df_Reprod_Reproductora = spark.sql(f"SELECT MO.IRN_BFT as BrimFieldTransIRN,MO.ProteinEntitiesIRN_BFT as ProteinEntitiesIRN,MO.ProteinGrowoutCodesIRN_VB as ProteinGrowoutCodesIRN,MO.ProteinFarmsIRN, \
                                    MO.ProteinCostCentersIRN_VB as ProteinCostCentersIRN,MO.FmimFeedTypesIRN_Female,MO.FmimFeedTypesIRN_Male,MO.FmimFeedTypesIRN_VFFF,MO.FmimFeedTypesIRN_VFFM, \
                                    PCC.ProteinDivisionsIRN,PE.ProteinHousesIRN,PE.ProteinBreedCodesIRN,PE.ProteinStandardVersionsIRN,PE.ProteinTechSupervisorsIRN,PE.ProteinProductsAnimalsIRN, \
                                    PSV.ProteinStandardsIRN, \
                                    nvl(LT.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha = cast('1899-11-30' as date))) pk_tiempo, \
                                    nvl(LT.fecha,cast('1899-11-30' as date)) fecha, \
                                    nvl(LD.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) as pk_division, \
                                    nvl(LP.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) as pk_zona, \
                                    nvl(LP.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) as pk_subzona, \
                                    nvl(LP.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) as pk_plantel, \
                                    nvl(LL.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) as pk_lote, \
                                    nvl(LG.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) as pk_galpon, \
                                    nvl(LS.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo, \
                                    CASE WHEN isnull(MO.DateCap) = true THEN 1 WHEN cast(MO.EventDate as timestamp) < cast(MO.DateCap as timestamp) THEN 1 ELSE 3 END pk_etapa, \
                                    nvl(LST.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard, \
                                    nvl(LPR.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto, \
                                    nvl(TP.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto, \
                                    nvl(GCO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name}.lk_grupoconsumo where cgrupoconsumo=0)) as pk_grupoconsumo, \
                                    nvl(LEP.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie, \
                                    nvl(LES.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado, \
                                    nvl(LAD.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador, \
                                    nvl(FALF.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimentof, \
                                    nvl(FALM.pk_alimento,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) as pk_alimentom, \
                                    nvl(PRO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) as pk_proveedor, \
                                    nvl(LGE.pk_generacion,(select pk_generacion from {database_name}.lk_generacion where cgeneracion=0)) as pk_generacion, \
                                    cast(MO.FirstHatchDateAge as int) pk_semanavida, \
                                    MO.FirstHatchDateAgesin7 pk_diasvida, \
                                    MO.ComplexEntityNo,MO.FirstHatchDate FechaNacimiento, \
                                    round((FirstHatchDateAgesin7*1.0)/7,0) + ((FirstHatchDateAgesin7*1.0)%7)/10 as Edad, \
                                    date_format(MO.LastDateSold,'yyyyMMdd') as FechaCierre, \
                                    date_format(MO.FirstDateSold,'yyyyMMdd') as FechaInicioSaca, \
                                    date_format(MO.LastDateSold,'yyyyMMdd') as FechaFinSaca, \
                                    MO.EventDate,MO.xDate,LG.Area as AreaGalpon,LT.semanaanioReprod semanaReprod,LT.semanaReprod semanaReprod2, \
                                    MO.MortalityF,MO.MortalityM,MO.UniformityF,MO.UniformityM,MO.U_AccidentadosPollo,MO.U_AccidentadosPolloMale, \
                                    U_No_Viable,U_No_ViableMale,U_Higado_Hemorragico,U_Higado_HemorragicoMale,U_Pro_RespiratorioPL,U_Pro_RespiratorioPLMale, \
                                    U_SCH,U_SCHMale,U_EnteritisPollo,U_EnteritisPolloMale,U_Ascitis,U_AscitisMale,U_Estres_Calor,U_Estres_CalorMale, \
                                    U_Muerte_Subita,U_Muerte_SubitaMale,U_Hidropericardio,U_HidropericardioMale,U_Uratosis,U_UratosisMale,U_Mat_caseosoPL, \
                                    U_Mat_caseosoPLMale,U_OnfalitisPollo,U_OnfalitisPolloMale,U_Retencion_Yema,U_Retencion_YemaMale,U_Erosion_de_molleja, \
                                    U_Erosion_de_mollejaMale,U_Sangre_en_Ciego,U_Sangre_en_CiegoMale,U_Pericarditis,U_PericarditisMale,U_PicajePollo, \
                                    U_PicajePolloMale,U_ProlapsoPollo,U_ProlapsoPolloMale,U_PeritonitisPollo,U_PeritonitisPolloMale,U_MaltratoPollo, \
                                    U_MaltratoPolloMale,U_Error_Sexo,U_Error_SexoMale,U_Cojo,U_CojoMale,U_Despatarrados,U_DespatarradosMale, \
                                    MO.GRN,BSD.U_FeedPerHenWK,BSD.U_FeedPerMaleWK,BSD.U_PercentProdTEWK,BSD.U_PercentProdHEWK,BSD.U_HenWeight, \
                                    BSD.U_MaleWeight,BSD.U_TEPerHHACM,BSD.U_HEPerHHACM,BSD.U_HenMortWK,BSD.U_MaleMortWK,BSD.U_HenMortACM, \
                                    BSD.U_MaleMortACM,BSD.U_EggWeight,MO.FeedConsumedF,MO.FeedConsumedM,MO.TotalEggsProd ProdHT, \
                                    MO.HatchEggsProd ProdHI,MO.EggWeight PesoHvo,MO.WeightF PesoH,MO.WeightM PesoM,MO.U_PorosoProd,MO.U_SucioProd, \
                                    MO.U_SangreProd,MO.U_DeformeProd,MO.U_CascaraDebilProd,MO.U_RotoProd,MO.U_FarfaraProd,MO.U_InvendibleProd, \
                                    MO.U_ChicoProd,MO.U_DobleYemaProd,MO.U_HI_de_piso,PE.U_UnidadesSeleccionadas,MO.DateCap,MO.MalesCap,MO.HensCap \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 30 OR A.pk_alimentoM = 30)),0) AS AlimInicio \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 15 OR A.pk_alimentoM = 15)),0) AS AlimCrecimiento \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 64 OR A.pk_alimentoM = 64)),0) AS AlimPreReprod \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 72 OR A.pk_alimentoM = 72)),0) AS AlimReprodI \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 73 OR A.pk_alimentoM = 73)),0) AS AlimReprodII \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 71 OR A.pk_alimentoM = 71)),0) AS AlimReprodMacho \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 10 OR A.pk_alimentoM = 10)),0) AS PosturaCrecIRP \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 63 OR A.pk_alimentoM = 63)),0) AS PosturaPreInicioRP \
                                    ,MO.U_Ruptura_Aortica,MO.U_Ruptura_AorticaMale,MO.U_Bazo_Moteado,MO.U_Bazo_MoteadoMale \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 58 OR A.pk_alimentoM = 58)),0) AS PreInicialPavas \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 28 OR A.pk_alimentoM = 28)),0) AS InicialPavosRP \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF =  8 OR A.pk_alimentoM =  8)),0) AS CreIPavas \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 11 OR A.pk_alimentoM = 11)),0) AS CrecIIPavas \
                                    ,nvl((SELECT SUM(Consumo) FROM {database_name}.consumo A WHERE A.ComplexEntityNo = MO.ComplexEntityNo AND A.pk_diasvida = MO.FirstHatchDateAgesin7 AND A.IRN_BFT = MO.IRN_BFT AND (A.pk_alimentoF = 38 OR A.pk_alimentoM = 38)),0) AS MantPavas \
                                    ,MO.ProteinProductsIRN_VFFF,MO.ProteinProductsIRN_VFFM,MO.Price,FM.FechaMax,FCM.FechaCapitalizacionMax \
                                    from {database_name}.Reprod_Trans MO \
                                    LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyyMMdd') = date_format(cast(MO.EventDate as timestamp),'yyyyMMdd') \
                                    LEFT JOIN {database_name}.si_proteincostcenters PCC ON MO.ProteinCostCentersIRN_VB = PCC.IRN \
                                    LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50)) \
                                    LEFT JOIN {database_name}.lk_plantel LP ON LP.IRN = CAST(MO.ProteinFarmsIRN AS VARCHAR (50)) \
                                    LEFT JOIN {database_name}.lk_lote LL ON LL.pk_plantel=LP.pk_plantel AND LL.nlote = RTRIM(MO.EntityNo) and ll.activeflag in (0,1) \
                                    LEFT JOIN {database_name}.si_proteinentities PE ON PE.IRN = MO.ProteinEntitiesIRN_BFT \
                                    LEFT JOIN {database_name}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR (50)) and lg.activeflag in (false,true) \
                                    LEFT JOIN {database_name}.lk_sexo LS ON LS.csexo = rtrim(MO.PenNo) \
                                    LEFT JOIN {database_name}.si_ProteinStandardVersions PSV ON PSV.IRN = PE.ProteinStandardVersionsIRN \
                                    LEFT JOIN {database_name}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50)) \
                                    LEFT JOIN {database_name}.lk_producto LPR ON LPR.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50)) \
                                    LEFT JOIN {database_name}.lk_tipoproducto TP ON tp.ntipoproducto=lpr.grupoproducto \
                                    LEFT JOIN {database_name}.lk_grupoconsumo GCO ON LPR.pk_grupoconsumo = GCO.pk_grupoconsumo \
                                    LEFT JOIN {database_name}.lk_especie LEP ON LEP.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
                                    LEFT JOIN {database_name}.lk_estado LES ON LES.cestado=PE.Status \
                                    LEFT JOIN {database_name}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50)) \
                                    LEFT JOIN {database_name}.lk_alimento FALF ON FALF.IRN = CAST(MO.FmimFeedTypesIRN_VFFF AS varchar (50)) \
                                    LEFT JOIN {database_name}.lk_alimento FALM ON FALM.IRN = CAST(MO.FmimFeedTypesIRN_VFFM AS varchar (50)) \
                                    LEFT JOIN {database_name}.si_mvbimfarms MVBF ON MVBF.ProteinFarmsIRN = MO.ProteinFarmsIRN \
                                    LEFT JOIN {database_name}.lk_proveedor PRO ON PRO.cproveedor = MVBF.VendorNo \
                                    LEFT JOIN {database_name}.si_bimstandardsdata BSD ON BSD.ProteinStandardVersionsIRN = PSV.IRN and BSD.age = MO.FirstHatchDateAge \
                                    LEFT JOIN {database_name}.lk_generacion LGE ON MO.GenerationCode = LGE.cgeneracion \
                                    LEFT JOIN FechaMaxima FM ON MO.ComplexEntityNo = FM.ComplexEntityNo \
                                    LEFT JOIN FechaCapitalizacionMaxima FCM ON FCM.ComplexEntityNo = MO.ComplexEntityNo \
                                    WHERE MO.FirstHatchDateAge >= 0")

#df_Reprod_Reproductora.createOrReplaceTempView("Reprod_Reproductora")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Reproductora"
}
df_Reprod_Reproductora.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Reproductora")

print('carga Reprod_Reproductora')
#Se inserta en una tabla los STD de la raza COBB en la edad 64, para luego insertarlo en los STD mayores a 64.
df_stdcobbedad64Temp = spark.sql(f"select distinct ComplexEntityNo,Edad,U_FeedPerHenWK,U_FeedPerMaleWK,U_PercentProdTEWK,U_PercentProdHEWK,U_HenWeight,U_MaleWeight,U_TEPerHHACM,U_HEPerHHACM, \
                                    U_HenMortWK,U_MaleMortWK,U_HenMortACM,U_MaleMortACM,U_EggWeight \
                                  from {database_name}.Reprod_Reproductora \
                                  where pk_especie in (7,9,10,14,16,17) and edad = 64")
# where idespecie in (13,14,16,40,10,2) and edad = 64
#df_stdcobbedad64Temp.createOrReplaceTempView("stdcobbedad64")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/stdcobbedad64"
}
df_stdcobbedad64Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.stdcobbedad64")

print('carga stdcobbedad64')
#Se actualiza los STD mayores de 64 con los datos de la tabla anterior
df_Reprod_Reproductora_upd = spark.sql(f"SELECT BrimFieldTransIRN,ProteinEntitiesIRN,ProteinGrowoutCodesIRN,ProteinFarmsIRN, \
ProteinCostCentersIRN,FmimFeedTypesIRN_Female,FmimFeedTypesIRN_Male,FmimFeedTypesIRN_VFFF,FmimFeedTypesIRN_VFFM, \
ProteinDivisionsIRN,ProteinHousesIRN,ProteinBreedCodesIRN,ProteinStandardVersionsIRN,ProteinTechSupervisorsIRN,ProteinProductsAnimalsIRN, \
ProteinStandardsIRN, \
pk_tiempo,fecha, pk_division, pk_zona, pk_subzona, pk_plantel, pk_lote, pk_galpon, pk_sexo, pk_etapa, \
pk_standard, pk_producto, pk_tipoproducto, pk_grupoconsumo, pk_especie, pk_estado, pk_administrador, \
pk_alimentof, pk_alimentom, pk_proveedor, pk_generacion, pk_semanavida, pk_diasvida, \
A.ComplexEntityNo,FechaNacimiento, A.Edad, \
FechaCierre, FechaInicioSaca, FechaFinSaca, \
EventDate,xDate,AreaGalpon, semanaReprod, semanaReprod2, \
MortalityF,MortalityM,UniformityF,UniformityM,U_AccidentadosPollo,U_AccidentadosPolloMale, \
U_No_Viable,U_No_ViableMale,U_Higado_Hemorragico,U_Higado_HemorragicoMale,U_Pro_RespiratorioPL,U_Pro_RespiratorioPLMale, \
U_SCH,U_SCHMale,U_EnteritisPollo,U_EnteritisPolloMale,U_Ascitis,U_AscitisMale,U_Estres_Calor,U_Estres_CalorMale, \
U_Muerte_Subita,U_Muerte_SubitaMale,U_Hidropericardio,U_HidropericardioMale,U_Uratosis,U_UratosisMale,U_Mat_caseosoPL, \
U_Mat_caseosoPLMale,U_OnfalitisPollo,U_OnfalitisPolloMale,U_Retencion_Yema,U_Retencion_YemaMale,U_Erosion_de_molleja, \
U_Erosion_de_mollejaMale,U_Sangre_en_Ciego,U_Sangre_en_CiegoMale,U_Pericarditis,U_PericarditisMale,U_PicajePollo, \
U_PicajePolloMale,U_ProlapsoPollo,U_ProlapsoPolloMale,U_PeritonitisPollo,U_PeritonitisPolloMale,U_MaltratoPollo, \
U_MaltratoPolloMale,U_Error_Sexo,U_Error_SexoMale,U_Cojo,U_CojoMale,U_Despatarrados,U_DespatarradosMale, \
GRN, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_FeedPerHenWK ELSE A.U_FeedPerHenWK END U_FeedPerHenWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_FeedPerMaleWK ELSE A.U_FeedPerMaleWK END U_FeedPerMaleWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_PercentProdTEWK ELSE A.U_PercentProdTEWK END U_PercentProdTEWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_PercentProdHEWK ELSE A.U_PercentProdHEWK END U_PercentProdHEWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_HenWeight ELSE A.U_HenWeight END U_HenWeight, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_MaleWeight ELSE A.U_MaleWeight END U_MaleWeight, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_TEPerHHACM ELSE A.U_TEPerHHACM END U_TEPerHHACM, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_HEPerHHACM ELSE A.U_HEPerHHACM END U_HEPerHHACM, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_HenMortWK ELSE A.U_HenMortWK END U_HenMortWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_MaleMortWK ELSE A.U_MaleMortWK END U_MaleMortWK, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_HenMortACM ELSE A.U_HenMortACM END U_HenMortACM, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_MaleMortACM ELSE A.U_MaleMortACM END U_MaleMortACM, \
CASE WHEN A.pk_especie in (7,9,10,14,16,17) and A.edad > 64.0 THEN  B.U_EggWeight ELSE A.U_EggWeight END U_EggWeight, \
FeedConsumedF,FeedConsumedM,ProdHT, ProdHI,PesoHvo, PesoH, PesoM,U_PorosoProd,U_SucioProd, \
U_SangreProd,U_DeformeProd,U_CascaraDebilProd,U_RotoProd,U_FarfaraProd,U_InvendibleProd, \
U_ChicoProd,U_DobleYemaProd,U_HI_de_piso,U_UnidadesSeleccionadas,DateCap,MalesCap,HensCap \
,AlimInicio ,AlimCrecimiento ,AlimPreReprod ,AlimReprodI ,AlimReprodII,AlimReprodMacho \
,PosturaCrecIRP ,PosturaPreInicioRP ,U_Ruptura_Aortica,U_Ruptura_AorticaMale,U_Bazo_Moteado,U_Bazo_MoteadoMale \
,PreInicialPavas ,InicialPavosRP ,CreIPavas ,CrecIIPavas ,MantPavas \
,ProteinProductsIRN_VFFF,ProteinProductsIRN_VFFM,Price,FechaMax,FechaCapitalizacionMax \
from {database_name}.Reprod_Reproductora A \
LEFT JOIN \
{database_name}.stdcobbedad64 B ON A.complexentityno = B.complexentityno")
#df_Reprod_Reproductora_upd.createOrReplaceTempView("Reprod_Reproductora_upd")
#13,14,16,40,10,2

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Reproductora_upd"
}
df_Reprod_Reproductora_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Reproductora_upd")

print('carga Reprod_Reproductora_upd')
#Se inserta en una tabla los STD de la raza ROSS en la edad 65, para luego insertarlo en los STD mayores a 65.
df_stdrossedad65Temp = spark.sql(f"select distinct ComplexEntityNo,Edad,U_FeedPerHenWK,U_FeedPerMaleWK,U_PercentProdTEWK,U_PercentProdHEWK,U_HenWeight,U_MaleWeight,U_TEPerHHACM, \
U_HEPerHHACM,U_HenMortWK,U_MaleMortWK,U_HenMortACM,U_MaleMortACM,U_EggWeight \
from {database_name}.Reprod_Reproductora_upd \
where pk_especie in (41) and edad = 65")
#df_stdrossedad65Temp.createOrReplaceTempView("stdrossedad65")
#idespecie=9

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/stdrossedad65"
}
df_stdrossedad65Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.stdrossedad65")

print('carga stdrossedad65')
#Se actualiza los STD mayores de 65 con los datos de la tabla anterior
df_Reprod_Reproductora_upd2 = spark.sql(f"SELECT A.BrimFieldTransIRN,A.ProteinEntitiesIRN,A.ProteinGrowoutCodesIRN,A.ProteinFarmsIRN, \
A.ProteinCostCentersIRN,A.FmimFeedTypesIRN_Female,A.FmimFeedTypesIRN_Male,A.FmimFeedTypesIRN_VFFF,A.FmimFeedTypesIRN_VFFM, \
A.ProteinDivisionsIRN,A.ProteinHousesIRN,A.ProteinBreedCodesIRN,A.ProteinStandardVersionsIRN,A.ProteinTechSupervisorsIRN,A.ProteinProductsAnimalsIRN, \
A.ProteinStandardsIRN, \
A.pk_tiempo, fecha,A.pk_division, A.pk_zona, A.pk_subzona, A.pk_plantel, A.pk_lote, A.pk_galpon, A.pk_sexo, A.pk_etapa, \
A.pk_standard, A.pk_producto, A.pk_tipoproducto, A.pk_grupoconsumo, A.pk_especie, A.pk_estado, A.pk_administrador, \
A.pk_alimentof, A.pk_alimentom, A.pk_proveedor, A.pk_generacion, A.pk_semanavida, A.pk_diasvida, \
A.ComplexEntityNo,A.FechaNacimiento, A.Edad, \
A.FechaCierre, A.FechaInicioSaca, A.FechaFinSaca, \
A.EventDate,A.xDate,A.AreaGalpon, A.semanaReprod, A.semanaReprod2, \
A.MortalityF,A.MortalityM,A.UniformityF,A.UniformityM,A.U_AccidentadosPollo,A.U_AccidentadosPolloMale, \
A.U_No_Viable,A.U_No_ViableMale,A.U_Higado_Hemorragico,A.U_Higado_HemorragicoMale,A.U_Pro_RespiratorioPL,A.U_Pro_RespiratorioPLMale, \
A.U_SCH,U_SCHMale,A.U_EnteritisPollo,A.U_EnteritisPolloMale,A.U_Ascitis,A.U_AscitisMale,A.U_Estres_Calor,A.U_Estres_CalorMale, \
A.U_Muerte_Subita,A.U_Muerte_SubitaMale,A.U_Hidropericardio,A.U_HidropericardioMale,A.U_Uratosis,A.U_UratosisMale,A.U_Mat_caseosoPL, \
A.U_Mat_caseosoPLMale,A.U_OnfalitisPollo,A.U_OnfalitisPolloMale,A.U_Retencion_Yema,A.U_Retencion_YemaMale,A.U_Erosion_de_molleja, \
A.U_Erosion_de_mollejaMale,A.U_Sangre_en_Ciego,A.U_Sangre_en_CiegoMale,A.U_Pericarditis,A.U_PericarditisMale,A.U_PicajePollo, \
A.U_PicajePolloMale,A.U_ProlapsoPollo,A.U_ProlapsoPolloMale,A.U_PeritonitisPollo,A.U_PeritonitisPolloMale,A.U_MaltratoPollo, \
A.U_MaltratoPolloMale,A.U_Error_Sexo,A.U_Error_SexoMale,A.U_Cojo,A.U_CojoMale,A.U_Despatarrados,A.U_DespatarradosMale,A.GRN, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_FeedPerHenWK ELSE A.U_FeedPerHenWK END U_FeedPerHenWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_FeedPerMaleWK ELSE A.U_FeedPerMaleWK END U_FeedPerMaleWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_PercentProdTEWK ELSE A.U_PercentProdTEWK END U_PercentProdTEWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_PercentProdHEWK ELSE A.U_PercentProdHEWK END U_PercentProdHEWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_HenWeight ELSE A.U_HenWeight END U_HenWeight, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_MaleWeight ELSE A.U_MaleWeight END U_MaleWeight, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_TEPerHHACM ELSE A.U_TEPerHHACM END U_TEPerHHACM, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_HEPerHHACM ELSE A.U_HEPerHHACM END U_HEPerHHACM, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_HenMortWK ELSE A.U_HenMortWK END U_HenMortWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_MaleMortWK ELSE A.U_MaleMortWK END U_MaleMortWK, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_HenMortACM ELSE A.U_HenMortACM END U_HenMortACM, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_MaleMortACM ELSE A.U_MaleMortACM END U_MaleMortACM, \
CASE WHEN A.pk_especie in (41) and A.edad > 65.0 THEN  B.U_EggWeight ELSE A.U_EggWeight END U_EggWeight, \
A.FeedConsumedF,A.FeedConsumedM,A.ProdHT, A.ProdHI,A.PesoHvo, A.PesoH, A.PesoM,A.U_PorosoProd,A.U_SucioProd, \
A.U_SangreProd,A.U_DeformeProd,A.U_CascaraDebilProd,A.U_RotoProd,A.U_FarfaraProd,A.U_InvendibleProd, \
A.U_ChicoProd,A.U_DobleYemaProd,A.U_HI_de_piso,A.U_UnidadesSeleccionadas,A.DateCap,A.MalesCap,A.HensCap \
,A.AlimInicio ,A.AlimCrecimiento ,A.AlimPreReprod ,A.AlimReprodI ,A.AlimReprodII,A.AlimReprodMacho \
,A.PosturaCrecIRP ,A.PosturaPreInicioRP ,A.U_Ruptura_Aortica,A.U_Ruptura_AorticaMale,A.U_Bazo_Moteado,A.U_Bazo_MoteadoMale \
,A.PreInicialPavas ,A.InicialPavosRP ,A.CreIPavas ,A.CrecIIPavas ,A.MantPavas \
,A.ProteinProductsIRN_VFFF,ProteinProductsIRN_VFFM,Price,FechaMax,FechaCapitalizacionMax \
from {database_name}.Reprod_Reproductora_upd A \
LEFT JOIN \
{database_name}.stdrossedad65 B ON A.complexentityno = B.complexentityno")
#df_Reprod_Reproductora_upd2.createOrReplaceTempView("Reprod_Reproductora_upd2")

#where idespecie in (9)
#and A.edad > 65.0

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Reproductora_upd2"
}
df_Reprod_Reproductora_upd2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Reproductora_upd2")

print(' carga Reprod_Reproductora_upd2')
#Saca
df_SacaReproductoraTemp = spark.sql(f"SELECT EventDate, \
pk_tiempo, \
fecha, \
ComplexEntityNo, \
SUM(QuantityH) QuantityH, \
SUM(QuantityM) QuantityM, \
SUM(TransIngH) TransIngH, \
SUM(TransSalH) TransSalH, \
SUM(TransIngM) TransIngM, \
SUM(TransSalM) TransSalM \
FROM ( \
      SELECT \
      EventDate, \
      LT.pk_tiempo, \
      LT.fecha, \
      ComplexEntityNo, \
      Case when SourceCode = 'PmtsProcRecvTrans' and PE.PenNo = '01' then SUM(ABS(Quantity * 1.000000)) end QuantityH, \
      Case when SourceCode = 'PmtsProcRecvTrans' and PE.PenNo = '02' then SUM(ABS(Quantity)) end QuantityM, \
      Case when SourceCode = 'Pmts Transfers'    and PE.PenNo = '01' and BIN.TransCode = 1 then SUM(ABS(Quantity)) end TransIngH, \
      Case when SourceCode = 'Pmts Transfers'    and PE.PenNo = '01' and BIN.TransCode = 12 then SUM(ABS(Quantity)) end TransSalH, \
      Case when SourceCode = 'Pmts Transfers'    and PE.PenNo = '02' and BIN.TransCode = 1 then SUM(ABS(Quantity)) end TransIngM, \
      Case when SourceCode = 'Pmts Transfers'    and PE.PenNo = '02' and BIN.TransCode = 12 then SUM(ABS(Quantity)) end TransSalM \
      FROM {database_name}.si_bimentityinventory BIN \
      LEFT JOIN {database_name}.si_mvproteinentities PE ON PE.IRN = BIN.ProteinEntitiesIRN \
      LEFT JOIN {database_name}.lk_tiempo LT ON date_format(LT.fecha,'yyyy-MM-dd') = date_format(cast(EventDate as timestamp),'yyyy-MM-dd') \
      WHERE SourceCode in ('PmtsProcRecvTrans','Pmts Transfers') \
      GROUP BY EventDate,ComplexEntityNo,PenNo,SourceCode,TransCode,LT.pk_tiempo,LT.fecha \
      ) A \
GROUP BY EventDate,pk_tiempo,fecha,ComplexEntityNo")
#df_SacaReproductoraTemp.createOrReplaceTempView("SacaReproductora")
#date_format(cast(EventDate as timestamp),'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 12 MONTH), 'yyyyMM')

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SacaReproductora"
}
df_SacaReproductoraTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.SacaReproductora")

print('carga SacaReproductora')
#Inserta los datos de saca reproductora (ventas) , Inserta los datos de transferencias, Esta tabla es temporal hasta que se genere la tabla de hechos de saca reproductora
#Se  agrupa los datos de mortalidad, saca y transferencia por corral para generar los valores acumulados
df_ValoresParaAcumularTemp = spark.sql(f"SELECT \
RE.EventDate, \
RE.pk_tiempo, \
RE.fecha, \
RE.DateCap, \
date_format(cast(RE.DateCap as timestamp),'yyyyMMdd') fechaCap, \
RE.pk_etapa, \
RE.ComplexEntityNo, \
substring(RE.complexentityno,1,(length(RE.complexentityno)-3)) ComplexEntityNoGalpon, \
substring(RE.complexentityno,1,(length(RE.complexentityno)-6)) ComplexEntityNoLote, \
SUM(RE.MortalityF) MortF, \
SUM(RE.MortalityM) MortM, \
MAX(nvl(SR.QuantityH * 1.000000,0)) SacaF, \
MAX(nvl(SR.QuantityM,0)) SacaM, \
MAX(nvl(SR.TransIngH,0)) TransIngH, \
MAX(nvl(SR.TransSalH,0)) TransSalH, \
MAX(nvl(SR.TransIngM,0)) TransIngM, \
MAX(nvl(SR.TransSalM,0)) TransSalM \
FROM {database_name}.Reprod_Reproductora_upd2 RE \
left join {database_name}.SacaReproductora SR on RE.ComplexEntityNo = SR.ComplexEntityNo and RE.pk_tiempo = SR.pk_tiempo \
GROUP BY RE.EventDate,RE.pk_tiempo,RE.fecha,RE.DateCap,Re.pk_etapa,RE.ComplexEntityNo")
#df_ValoresParaAcumularTemp.createOrReplaceTempView("ValoresParaAcumular")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumular"
}
df_ValoresParaAcumularTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumular")

print('carga ValoresParaAcumular')
#df_SumatoriasTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa, pk_tiempo, fecha, nvl(SUM(MortF),0) MortAcumH , \
#nvl(SUM(MortM),0) MortAcumM, nvl(SUM(SacaF),0) SacaAcumH, nvl(SUM(SacaM),0) SacaAcumM, \
#nvl(SUM(TransIngH),0) TransIngAcumH, nvl(SUM(TransSalH),0) TransSalAcumH,  nvl(SUM(TransIngM),0) TransIngAcumM, nvl(SUM(TransSalM),0) TransSalAcumM \
#FROM {database_name}.ValoresParaAcumular \
#group by ComplexEntityNo, pk_etapa, pk_tiempo, fecha")
##df_SumatoriasTemp.createOrReplaceTempView("Sumatorias")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Sumatorias"
#}
#df_SumatoriasTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.Sumatorias")
#
#print('carga Sumatorias')
##Se genera los valores acumulados por Corral
#df_AcumuladosTemp = spark.sql(f"select MA.EventDate,MA.fecha, MA.pk_tiempo, MA.fechaCap, MA.pk_etapa,MA.ComplexEntityNo, \
#MortAcumH, \
#MortAcumM, \
#SacaAcumH, \
#SacaAcumM, \
#TransIngAcumH, \
#TransSalAcumH, \
#TransIngAcumM, \
#TransSalAcumM \
#from {database_name}.ValoresParaAcumular MA  LEFT JOIN \
#{database_name}.Sumatorias B ON B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha \
#order by MA.ComplexEntityNo,MA.pk_tiempo")
##df_AcumuladosTemp.createOrReplaceTempView("Acumulados")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Acumulados"
#}
#df_AcumuladosTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.Acumulados")
#
#
#print('carga Acumulados')
#Se genera los valores acumulados por Corral
df_AcumuladosTemp = spark.sql(f" WITH BSD AS ( \
    SELECT EventDate,fecha,pk_tiempo ,fechaCap,pk_etapa,ComplexEntityNo, \
         SUM(MortF) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS MortAcumH, \
         SUM(MortM) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS MortAcumM, \
         SUM(SacaF) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS SacaAcumH, \
         SUM(SacaM) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS SacaAcumM, \
         SUM(TransIngH) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS TransIngAcumH, \
         SUM(TransSalH) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS TransSalAcumH, \
         SUM(TransIngM) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS TransIngAcumM, \
         SUM(TransSalM) OVER ( \
         PARTITION BY ComplexEntityNo,pk_etapa \
         ORDER BY pk_tiempo \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
         ) AS TransSalAcumM \
    FROM {database_name}.ValoresParaAcumular \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNo, pk_tiempo")
#df_AcumuladosTemp.createOrReplaceTempView("Acumulados")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Acumulados"
}
df_AcumuladosTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Acumulados")


print('carga Acumulados')
# hasta aqui el campo SacaAcumH viene ok desde su origen -eq
#Se  agrupa los datos de mortalidad, saca y transferencia por galpon para generar los valores acumulados
df_ValoresParaAcumularGalponTemp= spark.sql(f"select EventDate, pk_tiempo,fecha, min(cast(DateCap as timestamp)) DateCap, \
min(cast(fechaCap as timestamp)) fechaCap, max(pk_etapa) pk_etapa, \
substring(complexentityno,1,(length(complexentityno)-3)) ComplexEntityNo, \
sum(MortF) MortF, sum(MortM) MortM, Sum(SacaF * 1.000000) SacaF, Sum(SacaM) SacaM, \
Sum(TransIngH) TransIngH, Sum(TransSalH) TransSalH, Sum(TransIngM) TransIngM, \
Sum(TransSalM) TransSalM \
from {database_name}.ValoresParaAcumular \
group by EventDate,pk_tiempo,fecha,substring(complexentityno,1,(length(complexentityno)-3)) \
order by cast(eventdate as timestamp)")
#df_ValoresParaAcumularGalponTemp.createOrReplaceTempView("ValoresParaAcumularGalpon")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumularGalpon"
}
df_ValoresParaAcumularGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumularGalpon")

print('carga ValoresParaAcumularGalpon')
#df_SumatoriasGalponTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa, pk_tiempo, fecha, nvl(SUM(MortF),0) MortAcumH , \
#nvl(SUM(MortM),0) MortAcumM, nvl(SUM(SacaF),0) SacaAcumH, nvl(SUM(SacaM),0) SacaAcumM, \
#nvl(SUM(TransIngH),0) TransIngAcumH, nvl(SUM(TransSalH),0) TransSalAcumH,  nvl(SUM(TransIngM),0) TransIngAcumM, nvl(SUM(TransSalM),0) TransSalAcumM \
#FROM {database_name}.ValoresParaAcumularGalpon \
#group by ComplexEntityNo, pk_etapa, pk_tiempo, fecha")
##df_SumatoriasTemp.createOrReplaceTempView("SumatoriasGalpon")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriasGalpon"
#}
#df_SumatoriasTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.SumatoriasGalpon")
#
#print('carga SumatoriasGalpon')
##Se genera los valores acumulados por Galpon
#df_AcumuladosGalponTemp = spark.sql(f"select EventDate,MA.pk_tiempo,MA.fecha,fechaCap,MA.pk_etapa, \
#MA.ComplexEntityNo ComplexEntityNoGalpon, \
#MortAcumH, \
#MortAcumM, \
#SacaAcumH, \
#SacaAcumM, \
#TransIngAcumH, \
#TransSalAcumH, \
#TransIngAcumM, \
#TransSalAcumM \
#from {database_name}.ValoresParaAcumularGalpon MA LEFT JOIN {database_name}.SumatoriasGalpon B ON B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha \
#order by MA.ComplexEntityNo,MA.pk_tiempo")
##df_AcumuladosGalponTemp.createOrReplaceTempView("AcumuladosGalpon")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosGalpon"
#}
#df_AcumuladosGalponTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.AcumuladosGalpon")
#
#print('carga AcumuladosGalpon')
#Se genera los valores acumulados por Galpon
df_AcumuladosGalponTemp = spark.sql(f"WITH BSD AS ( \
SELECT EventDate,pk_tiempo ,fecha,fechaCap,pk_etapa,ComplexEntityNo ComplexEntityNoGalpon, \
SUM(MortF) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS MortAcumH, \
SUM(MortM) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS MortAcumM, \
SUM(SacaF * 1.000000) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS SacaAcumH, \
SUM(SacaM) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS SacaAcumM, \
SUM(TransIngH) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransIngAcumH, \
SUM(TransSalH) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransSalAcumH, \
SUM(TransIngM) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransIngAcumM, \
SUM(TransSalM) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransSalAcumM \
FROM {database_name}.ValoresParaAcumularGalpon \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNoGalpon, pk_tiempo")
#df_AcumuladosGalponTemp.createOrReplaceTempView("AcumuladosGalpon")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosGalpon"
}
df_AcumuladosGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosGalpon")

print('carga AcumuladosGalpon')
#Se  agrupa los datos de mortalidad, saca y transferencia por lote para generar los valores acumulados
df_ValoresParaAcumularLoteTemp = spark.sql(f"select \
EventDate, \
pk_tiempo, \
fecha, \
min(DateCap) DateCap, \
min(fechaCap) fechaCap, \
max(pk_etapa) pk_etapa, \
substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNo, \
sum(MortF) MortF, \
sum(MortM) MortM, \
Sum(SacaF) SacaF, \
Sum(SacaM) SacaM, \
Sum(TransIngH) TransIngH, \
Sum(TransSalH) TransSalH, \
Sum(TransIngM) TransIngM, \
Sum(TransSalM) TransSalM \
from {database_name}.ValoresParaAcumular \
group by EventDate,pk_tiempo,fecha,substring(complexentityno,1,(length(complexentityno)-6)) \
order by pk_tiempo")
#df_ValoresParaAcumularLoteTemp.createOrReplaceTempView("ValoresParaAcumularLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumularLote"
}
df_ValoresParaAcumularLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumularLote")

print('carga ValoresParaAcumularLote')
#df_SumatoriasLoteTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa, pk_tiempo, fecha, nvl(SUM(MortF),0) MortAcumH ,nvl(SUM(MortM),0) MortAcumM, \
#nvl(SUM(SacaF),0) SacaAcumH, nvl(SUM(SacaM),0) SacaAcumM, \
#nvl(SUM(TransIngH),0) TransIngAcumH, nvl(SUM(TransSalH),0) TransSalAcumH,  nvl(SUM(TransIngM),0) TransIngAcumM, nvl(SUM(TransSalM),0) TransSalAcumM \
#FROM {database_name}.ValoresParaAcumularLote \
#group by ComplexEntityNo, pk_etapa, pk_tiempo, fecha")
##df_SumatoriasTemp.createOrReplaceTempView("SumatoriasLote")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriasLote"
#}
#df_SumatoriasTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.SumatoriasLote")
#
#print('carga SumatoriasLote')
##Se genera los valores acumulados por lote
#df_AcumuladosLoteTemp = spark.sql(f"select EventDate, MA.pk_tiempo,MA.fecha,fechaCap,MA.pk_etapa,MA.ComplexEntityNo ComplexEntityNoLote, \
#MortAcumH, \
#MortAcumM, \
#SacaAcumH, \
#SacaAcumM, \
#TransIngAcumH, \
#TransSalAcumH, \
#TransIngAcumM, \
#TransSalAcumM \
#from {database_name}.ValoresParaAcumularLote MA LEFT JOIN {database_name}.SumatoriasLote B ON B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha \
#order by MA.ComplexEntityNo,MA.pk_tiempo")
##df_AcumuladosLoteTemp.createOrReplaceTempView("AcumuladosLote")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosLote"
#}
#df_AcumuladosLoteTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.AcumuladosLote")
#
#print('carga AcumuladosLote')
#Se genera los valores acumulados por lote
df_AcumuladosLoteTemp = spark.sql(f"WITH BSD AS ( \
    SELECT EventDate,pk_tiempo,fecha ,fechaCap,pk_etapa,ComplexEntityNo ComplexEntityNoLote, \
SUM(MortF) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS MortAcumH, \
SUM(MortM) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS MortAcumM, \
SUM(SacaF) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS SacaAcumH, \
SUM(SacaM) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS SacaAcumM, \
SUM(TransIngH) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransIngAcumH, \
SUM(TransSalH) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransSalAcumH, \
SUM(TransIngM) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransIngAcumM, \
SUM(TransSalM) OVER ( \
     PARTITION BY ComplexEntityNo,pk_etapa \
     ORDER BY pk_tiempo \
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS TransSalAcumM \
    FROM {database_name}.ValoresParaAcumularLote \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNoLote, pk_tiempo")
#df_AcumuladosLoteTemp.createOrReplaceTempView("AcumuladosLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosLote"
}
df_AcumuladosLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosLote")

print('carga AcumuladosLote')
#Elimina la tabla de acumulados
#Se crea e inserta los valores acumulados por corral, galpon y lote
df_Reprod_Acumulados = spark.sql(f"select \
A.pk_tiempo, \
A.fecha, \
A.fechaCap, \
A.pk_etapa, \
A.ComplexEntityNo, \
substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNoGalpon, \
substring(A.complexentityno,1,(length(A.complexentityno)-6)) ComplexEntityNoLote, \
A.MortAcumH, \
A.MortAcumM, \
A.SacaAcumH, \
A.SacaAcumM, \
A.TransIngAcumH, \
A.TransSalAcumH, \
A.TransIngAcumM, \
A.TransSalAcumM, \
B.MortAcumH MortAcumHGalpon, \
B.MortAcumM MortAcumMGalpon, \
B.SacaAcumH * 1.000000 SacaAcumHGalpon, \
B.SacaAcumM SacaAcumMGalpon, \
B.TransIngAcumH TransIngAcumHGalpon, \
B.TransSalAcumH TransSalAcumHGalpon, \
B.TransIngAcumM TransIngAcumMGalpon, \
B.TransSalAcumM TransSalAcumMGalpon, \
C.MortAcumH MortAcumHLote, \
C.MortAcumM MortAcumMLote, \
C.SacaAcumH SacaAcumHLote, \
C.SacaAcumM SacaAcumMLote, \
C.TransIngAcumH TransIngAcumHLote, \
C.TransSalAcumH TransSalAcumHLote, \
C.TransIngAcumM TransIngAcumMLote, \
C.TransSalAcumM TransSalAcumMLote \
from {database_name}.Acumulados A \
left join {database_name}.AcumuladosGalpon B ON substring(A.complexentityno,1,(length(A.complexentityno)-3)) = B.ComplexEntityNoGalpon AND A.pk_tiempo = B.pk_tiempo \
left join {database_name}.AcumuladosLote   C ON substring(A.complexentityno,1,(length(A.complexentityno)-6)) = C.ComplexEntityNoLote   AND A.pk_tiempo = C.pk_tiempo")

#df_Reprod_Acumulados.createOrReplaceTempView("Reprod_Acumulados")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Acumulados"
}
df_Reprod_Acumulados.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Acumulados")

print('carga Reprod_Acumulados')
#Inserta los datos de mortalidad reproductora agrupados por día. (Un registro por día)
df_Reprod_Mort_DiarioTemp = spark.sql(f"SELECT FR.pk_tiempo,FR.fecha,1 as pk_empresa,FR.pk_division,FR.pk_zona,FR.pk_subzona,FR.pk_plantel,FR.pk_lote,FR.pk_galpon,FR.pk_sexo, \
FR.pk_etapa,FR.pk_standard,FR.pk_producto,FR.pk_tipoproducto,FR.pk_especie,FR.pk_estado, \
FR.pk_administrador,FR.pk_proveedor,FR.pk_generacion,FR.pk_semanavida,FR.pk_diasvida,FR.ComplexEntityNo, \
FR.FechaNacimiento,FR.Edad,FR.semanaReprod, \
SUM(FR.MortalityF) as MortH, \
SUM(FR.MortalityM) as MortM, \
MAX(TA.MortAcumH) as MortAcumH, \
MAX(TA.MortAcumM) as MortAcumM, \
MAX(TA.MortAcumHGalpon) as MortAcumHGalpon, \
MAX(TA.MortAcumMGalpon) as MortAcumMGalpon, \
MAX(TA.MortAcumHLote) as MortAcumHLote, \
MAX(TA.MortAcumMLote) as MortAcumMLote, \
SUM(FR.U_AccidentadosPollo) as U_PEAccidentadosH, \
SUM(FR.U_AccidentadosPolloMale) as U_PEAccidentadosM, \
SUM(FR.U_No_Viable) as U_PENoViableH, \
SUM(FR.U_No_ViableMale) as U_PENoViableM, \
SUM(FR.U_Higado_Hemorragico) as U_PEHigadoHemorragicoH, \
SUM(FR.U_Higado_HemorragicoMale) as U_PEHigadoHemorragicoM, \
SUM(FR.U_Pro_RespiratorioPL) as U_PEProblemaRespiratorioH, \
SUM(FR.U_Pro_RespiratorioPLMale) as U_PEProblemaRespiratorioM, \
SUM(FR.U_SCH) as U_PESCHH, \
SUM(FR.U_SCHMale) as U_PESCHM, \
SUM(FR.U_EnteritisPollo) as U_PEEnteritisH, \
SUM(FR.U_EnteritisPolloMale) as U_PEEnteritisM, \
SUM(FR.U_Ascitis) as U_PEAscitisH, \
SUM(FR.U_AscitisMale) as U_PEAscitisM, \
SUM(FR.U_Estres_Calor) as U_PEEstresPorCalorH, \
SUM(FR.U_Estres_CalorMale) as U_PEEstresPorCalorM, \
SUM(FR.U_Muerte_Subita) as U_PEMuerteSubitaH, \
SUM(FR.U_Muerte_SubitaMale) as U_PEMuerteSubitaM, \
SUM(FR.U_Hidropericardio) as U_PEHidropericardioH, \
SUM(FR.U_HidropericardioMale) as U_PEHidropericardioM, \
SUM(FR.U_Uratosis) as U_PEUratosisH, \
SUM(FR.U_UratosisMale) as U_PEUratosisM, \
SUM(FR.U_Mat_caseosoPL) as U_PEMaterialCaseosoH, \
SUM(FR.U_Mat_caseosoPLMale) as U_PEMaterialCaseosoM, \
SUM(FR.U_OnfalitisPollo) as U_PEOnfalitisH, \
SUM(FR.U_OnfalitisPolloMale) as U_PEOnfalitisM, \
SUM(FR.U_Retencion_Yema) as U_PERetencionDeYemaH, \
SUM(FR.U_Retencion_YemaMale) as U_PERetencionDeYemaM, \
SUM(FR.U_Erosion_de_molleja) as U_PEErosionDeMollejaH, \
SUM(FR.U_Erosion_de_mollejaMale) as U_PEErosionDeMollejaM, \
SUM(FR.U_Sangre_en_Ciego) as U_PESangreEnCiegoH, \
SUM(FR.U_Sangre_en_CiegoMale) as U_PESangreEnCiegoM, \
SUM(FR.U_Pericarditis) as U_PEPericarditisH, \
SUM(FR.U_PericarditisMale) as U_PEPericarditisM, \
SUM(FR.U_PicajePollo) as U_PEPicajeH, \
SUM(FR.U_PicajePolloMale) as U_PEPicajeM, \
SUM(FR.U_ProlapsoPollo) as U_PEProlapsoH, \
SUM(FR.U_ProlapsoPolloMale) as U_PEProlapsoM, \
SUM(FR.U_PeritonitisPollo) as U_PEPeritonitisH, \
SUM(FR.U_PeritonitisPolloMale) as U_PEPeritonitisM, \
SUM(FR.U_MaltratoPollo) as U_PEMaltratoH, \
SUM(FR.U_MaltratoPolloMale) as U_PEMaltratoM, \
SUM(FR.U_Error_Sexo) as U_PEErrorSexoH, \
SUM(FR.U_Error_SexoMale) as U_PEErrorSexoM, \
SUM(FR.U_Cojo) as U_PECojoH, \
SUM(FR.U_CojoMale) as U_PECojoM, \
SUM(FR.U_Despatarrados) as U_PEDespatarradosH, \
SUM(FR.U_DespatarradosMale) as U_PEDespatarradosM, \
MAX(FIR.Inventario) as Inventario, \
CASE WHEN FR.pk_sexo = 2 THEN MAX(FIR.Inventario * 1.000000) ELSE 0.0 END IngresoH, \
CASE WHEN FR.pk_sexo = 3 THEN MAX(FIR.Inventario) ELSE 0.0 END IngresoM, \
MAX(nvl(TSR.QuantityH,0)) as SacaH, \
MAX(nvl(TSR.QuantityM,0)) as SacaM, \
MAX(TA.SacaAcumH) as SacaAcumH, \
MAX(TA.SacaAcumM) as SacaAcumM, \
MAX(TA.SacaAcumHGalpon * 1.000000) as SacaAcumHGalpon, \
MAX(TA.SacaAcumMGalpon) as SacaAcumMGalpon, \
MAX(TA.SacaAcumHLote) as SacaAcumHLote, \
MAX(TA.SacaAcumMLote) as SacaAcumMLote, \
MAX(nvl(FR.HensCap,0)) CapH, \
MAX(nvl(FR.MalesCap,0)) CapM, \
FIR.ListaPadre, \
MAX(nvl(TSR.TransIngH,0)) TransIngH, \
MAX(nvl(TSR.TransSalH,0)) TransSalH, \
MAX(nvl(TSR.TransIngM,0)) TransIngM, \
MAX(nvl(TSR.TransSalM,0)) TransSalM, \
MAX(TA.TransIngAcumH) as TransIngAcumH, \
MAX(TA.TransSalAcumH) as TransSalAcumH, \
MAX(TA.TransIngAcumM) as TransIngAcumM, \
MAX(TA.TransSalAcumM) as TransSalAcumM, \
MAX(TA.TransIngAcumHGalpon) as TransIngAcumHGalpon, \
MAX(TA.TransSalAcumHGalpon) as TransSalAcumHGalpon, \
MAX(TA.TransIngAcumMGalpon) as TransIngAcumMGalpon, \
MAX(TA.TransSalAcumMGalpon) as TransSalAcumMGalpon, \
MAX(TA.TransIngAcumHLote) as TransIngAcumHLote, \
MAX(TA.TransSalAcumHLote) as TransSalAcumHLote, \
MAX(TA.TransIngAcumMLote) as TransIngAcumMLote, \
MAX(TA.TransSalAcumMLote) as TransSalAcumMLote, \
SUM(FR.U_Ruptura_Aortica) as U_RupturaAorticaH, \
SUM(FR.U_Ruptura_AorticaMale) as U_RupturaAorticaM, \
SUM(FR.U_Bazo_Moteado) as U_BazoMoteadoH, \
SUM(FR.U_Bazo_MoteadoMale) as U_BazoMoteadoM, \
MAX(FR.FechaMax ) FechaMax, \
MAX(FR.FechaCapitalizacionMax) FechaCapitalizacionMax \
FROM {database_name}.Reprod_Reproductora_upd2 FR \
LEFT JOIN {database_name}.ft_Reprod_Ingreso_Total FIR on FR.ComplexEntityNo = FIR.ComplexEntityNo \
LEFT JOIN {database_name}.SacaReproductora TSR on FR.ComplexEntityNo = TSR.ComplexEntityNo and FR.pk_tiempo = TSR.pk_tiempo \
LEFT JOIN {database_name}.Reprod_Acumulados TA on FR.ComplexEntityNo = TA.ComplexEntityNo and FR.pk_tiempo = TA.pk_tiempo \
WHERE FR.GRN = 'P' \
AND FR.pk_division in (4,5,2) \
GROUP BY FR.pk_tiempo,FR.fecha,FR.pk_division,FR.pk_zona,FR.pk_subzona,FR.pk_plantel,FR.pk_lote,FR.pk_galpon,FR.pk_sexo, \
FR.pk_etapa,FR.pk_standard,FR.pk_producto,FR.pk_tipoproducto,FR.pk_especie,FR.pk_estado,FR.pk_administrador, \
FR.pk_proveedor,FR.pk_generacion,FR.pk_semanavida,FR.pk_diasvida,FR.ComplexEntityNo,FR.FechaNacimiento,FR.Edad, \
FR.semanaReprod,FIR.ListaPadre")
#df_Reprod_Mort_DiarioTemp.createOrReplaceTempView("Reprod_Mort_Diario")
# 3 Pollo -- 5 Huevo -- 4 pavos
#date_format(FR.fecha,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 12 MONTH), 'yyyyMM')

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Mort_Diario"
}
df_Reprod_Mort_DiarioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Mort_Diario")

print('carga Reprod_Mort_Diario')
#Inserta los últimos 4 meses de la tabla
df_ft_Reprod_Mort_Diario =spark.sql(f"SELECT MRD.pk_tiempo,MRD.fecha,MRD.pk_empresa,MRD.pk_division,MRD.pk_zona,MRD.pk_subzona,MRD.pk_plantel,MRD.pk_lote,MRD.pk_galpon, \
MRD.pk_sexo,MRD.pk_etapa,MRD.pk_standard,MRD.pk_producto,MRD.pk_tipoproducto,MRD.pk_especie,MRD.pk_estado,MRD.pk_administrador, \
MRD.pk_proveedor,MRD.pk_generacion,MRD.pk_semanavida,MRD.pk_diasvida,MRD.ComplexEntityNo,MRD.FechaNacimiento,MRD.Edad,MRD.IngresoH, \
MRD.IngresoM,MRD.MortH,MRD.MortM,MRD.MortAcumH,MRD.MortAcumM,MRD.MortAcumHGalpon,MRD.MortAcumMGalpon,MRD.MortAcumHLote,MRD.MortAcumMLote, \
MRD.SacaH,MRD.SacaM,MRD.SacaAcumH,MRD.SacaAcumM,MRD.SacaAcumHGalpon,MRD.SacaAcumMGalpon,MRD.SacaAcumHLote,MRD.SacaAcumMLote,MRD.TransIngH, \
MRD.TransSalH,MRD.TransIngM,MRD.TransSalM,MRD.TransIngAcumH,MRD.TransSalAcumH,MRD.TransIngAcumM,MRD.TransSalAcumM,MRD.TransIngAcumHGalpon, \
MRD.TransSalAcumHGalpon,MRD.TransIngAcumMGalpon,MRD.TransSalAcumMGalpon,MRD.TransIngAcumHLote,MRD.TransSalAcumHLote,MRD.TransIngAcumMLote, \
MRD.TransSalAcumMLote, \
CASE WHEN MRD.pk_etapa = 1 THEN MRD.IngresoH - MRD.SacaAcumH - MRD.MortAcumH - MRD.TransSalAcumH + MRD.TransIngAcumH \
ELSE MRD.CapH - MRD.SacaAcumH - MRD.MortAcumH - MRD.TransSalAcumH + MRD.TransIngAcumH END SaldoH, \
CASE WHEN MRD.pk_etapa = 1 THEN MRD.IngresoM - MRD.SacaAcumM - MRD.MortAcumM - MRD.TransSalAcumM + MRD.TransIngAcumM \
ELSE MRD.CapM - MRD.SacaAcumM - MRD.MortAcumM - MRD.TransSalAcumM + MRD.TransIngAcumM END SaldoM, \
CASE WHEN MRD.pk_etapa = 1 THEN nvl((MRD.MortH/NULLIF(((MRD.IngresoH - MRD.SacaAcumH - MRD.MortAcumH - MRD.TransSalAcumH + MRD.TransIngAcumH) + MRD.MortH),0))*100,0) \
ELSE nvl((MRD.MortH/NULLIF(((MRD.CapH - MRD.SacaAcumH - MRD.MortAcumH - MRD.TransSalAcumH + MRD.TransIngAcumH) + MRD.MortH),0))*100,0) END PorcMortH, \
CASE WHEN MRD.pk_etapa = 1 THEN nvl((MRD.MortM/NULLIF(((MRD.IngresoM - MRD.SacaAcumM - MRD.MortAcumM - MRD.TransSalAcumM + MRD.TransIngAcumM) + MRD.MortM),0))*100,0) \
ELSE nvl((MRD.MortM/NULLIF(((MRD.CapM - MRD.SacaAcumM - MRD.MortAcumM - MRD.TransSalAcumM + MRD.TransIngAcumM) + MRD.MortM),0))*100,0) END PorcMortM, \
MRD.U_PEAccidentadosH,MRD.U_PEAccidentadosM,MRD.U_PENoViableH,MRD.U_PENoViableM,MRD.U_PEHigadoHemorragicoH,MRD.U_PEHigadoHemorragicoM,MRD.U_PEProblemaRespiratorioH, \
MRD.U_PEProblemaRespiratorioM,MRD.U_PESCHH,MRD.U_PESCHM,MRD.U_PEEnteritisH,MRD.U_PEEnteritisM,MRD.U_PEAscitisH,MRD.U_PEAscitisM,MRD.U_PEEstresPorCalorH, \
MRD.U_PEEstresPorCalorM,MRD.U_PEMuerteSubitaH,MRD.U_PEMuerteSubitaM,MRD.U_PEHidropericardioH,MRD.U_PEHidropericardioM,MRD.U_PEUratosisH,MRD.U_PEUratosisM, \
MRD.U_PEMaterialCaseosoH,MRD.U_PEMaterialCaseosoM,MRD.U_PEOnfalitisH,MRD.U_PEOnfalitisM,MRD.U_PERetencionDeYemaH,MRD.U_PERetencionDeYemaM,MRD.U_PEErosionDeMollejaH, \
MRD.U_PEErosionDeMollejaM,MRD.U_PESangreEnCiegoH,MRD.U_PESangreEnCiegoM,MRD.U_PEPericarditisH,MRD.U_PEPericarditisM,MRD.U_PEPicajeH,MRD.U_PEPicajeM,MRD.U_PEProlapsoH, \
MRD.U_PEProlapsoM,MRD.U_PEPeritonitisH,MRD.U_PEPeritonitisM,MRD.U_PEMaltratoH,MRD.U_PEMaltratoM,MRD.U_PEErrorSexoH,MRD.U_PEErrorSexoM,MRD.U_PECojoH,MRD.U_PECojoM, \
MRD.U_PEDespatarradosH,MRD.U_PEDespatarradosM,MRD.ListaPadre,MRD.U_RupturaAorticaH,MRD.U_RupturaAorticaM,MRD.U_BazoMoteadoH,MRD.U_BazoMoteadoM \
FROM {database_name}.Reprod_Mort_Diario MRD \
order by pk_tiempo")
#df_ft_Reprod_Mort_Diario.createOrReplaceTempView("ft_Reprod_Mort_DiarioTemp")
print('carga df_ft_Reprod_Mort_Diario')
# Verificar si la tabla gold ya existe - ft_Reprod_Mort_Diario
#gold_table = spark.read.format("parquet").load(path_target7)  
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes7 = spark.read.format("parquet").load(path_target7)
    datos_existentes7 = True
    logger.info(f"Datos existentes de ft_Reprod_Mort_Diario cargados: {df_existentes7.count()} registros")
except:
    datos_existentes7 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Mort_Diario")
    
    
if datos_existentes7:
    existing_data7 = spark.read.format("parquet").load(path_target7)
    data_after_delete7 = existing_data7.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str) ))
    filtered_new_data7 = df_ft_Reprod_Mort_Diario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data7 = filtered_new_data7.union(data_after_delete7)                             
   
    cant_ingresonuevo7 = filtered_new_data7.count()
    cant_total7 = final_data7.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Mort_DiarioTemporal"
    }
    final_data7.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Mort_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data7_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Mort_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Mort_Diario"
    }
    final_data7_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Mort_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Mort_Diario : {cant_ingresonuevo7}")
    print(f"Total de registros en la tabla ft_Reprod_Mort_Diario : {cant_total7}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Mort_DiarioTemporal')
    print(f"Tabla ft_Reprod_Mort_DiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Mort_Diario"
    }
    df_ft_Reprod_Mort_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Mort_Diario")
# Reproductora
df_LineaGeneticaXNivelTemp = spark.sql(f"SELECT PE.ComplexEntityNo,PE.ProteinBreedCodesIRN,PE.BreedName, pk_especie,cespecie \
FROM {database_name}.si_mvproteinentities PE \
LEFT JOIN {database_name}.lk_especie LES ON LES.IRN = CAST(PE.ProteinBreedCodesIRN AS VARCHAR(50)) \
ORDER BY PE.ComplexEntityNo")
#df_LineaGeneticaXNivelTemp.createOrReplaceTempView("LineaGeneticaXNivel")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/LineaGeneticaXNivel"
}
df_LineaGeneticaXNivelTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.LineaGeneticaXNivel")

print('carga LineaGeneticaXNivel')
#Se  agrupa los datos de ProdHT,ProdHI por corral para generar los valores acumulados
df_ValoresParaAcumularProdTemp = spark.sql(f"select \
RE.EventDate, \
RE.pk_tiempo, \
RE.fecha, \
RE.DateCap, \
date_format(RE.DateCap,'yyyyMMdd') fechaCap, \
RE.pk_etapa, \
RE.ComplexEntityNo, \
RE.edad as Edad, \
SUM(RE.ProdHT) ProdHT, \
SUM(RE.ProdHI) ProdHI \
from {database_name}.Reprod_Reproductora_upd2 RE \
group by RE.EventDate,RE.pk_tiempo,RE.fecha,RE.DateCap,Re.pk_etapa,RE.ComplexEntityNo,RE.Edad")
#df_ValoresParaAcumularProdTemp.createOrReplaceTempView("ValoresParaAcumularProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumularProd"
}
df_ValoresParaAcumularProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumularProd")

print('carga ValoresParaAcumularProd')
#df_SumatoriaProdTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa,pk_tiempo,fecha, nvl(SUM(ProdHT),0) ProdHTAcum, nvl(SUM(ProdHI),0) ProdHIAcum \
#FROM {database_name}.ValoresParaAcumularProd \
#group by ComplexEntityNo, pk_etapa,pk_tiempo,fecha")
##df_SumatoriaProdTemp.createOrReplaceTempView("SumatoriaProdTemp")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaProdTemp"
#}
#df_SumatoriaProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.SumatoriaProdTemp")
#
#print('carga SumatoriaProdTemp')
##Se genera los valores acumulados por corral
#df_AcumuladosProdTemp = spark.sql(f"select \
#EventDate, \
#MA.pk_tiempo, \
#MA.fecha, \
#fechaCap, \
#MA.pk_etapa, \
#MA.ComplexEntityNo, \
#Edad, \
#B.ProdHTAcum, \
#B.ProdHIAcum \
#from {database_name}.ValoresParaAcumularProd MA left join {database_name}.SumatoriaProdTemp B \
#ON  B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha")
##df_AcumuladosProdTemp.createOrReplaceTempView("AcumuladosProd")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosProd"
#}
#df_AcumuladosProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.AcumuladosProd")
#
#print('carga AcumuladosProd')
#Se genera los valores acumulados por corral
df_AcumuladosProdTemp = spark.sql(f"WITH BSD AS ( \
SELECT EventDate,pk_tiempo,fecha ,fechaCap,pk_etapa,ComplexEntityNo,Edad, \
SUM(ProdHT) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHTAcum, \
SUM(ProdHI) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHIAcum \
FROM {database_name}.ValoresParaAcumularProd \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNo, pk_tiempo")
#df_AcumuladosProdTemp.createOrReplaceTempView("AcumuladosProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosProd"
}
df_AcumuladosProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosProd")

print('carga AcumuladosProd')
#Se  agrupa los datos de ProdHT,ProdHI por galpon para generar los valores acumulados
df_ValoresParaAcumularGalponProdTemp = spark.sql(f"select \
EventDate, \
pk_tiempo, \
fecha, \
min(fechaCap) fechaCap, \
max(pk_etapa) pk_etapa, \
substring(complexentityno,1,(length(complexentityno)-3)) ComplexEntityNo, \
sum(ProdHT) ProdHT, \
sum(ProdHI) ProdHI \
from {database_name}.ValoresParaAcumularProd \
group by EventDate,fecha,pk_tiempo,substring(complexentityno,1,(length(complexentityno)-3)) \
order by eventdate")
#df_ValoresParaAcumularGalponProdTemp.createOrReplaceTempView("ValoresParaAcumularGalponProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumularGalponProd"
}
df_ValoresParaAcumularGalponProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumularGalponProd")

print('carga ValoresParaAcumularGalponProd')
#df_SumatoriaGalponProdTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa,pk_tiempo,fecha, nvl(SUM(ProdHT),0) ProdHTAcum, nvl(SUM(ProdHI),0) ProdHIAcum \
#FROM {database_name}.ValoresParaAcumularGalponProd \
#group by ComplexEntityNo, pk_etapa,pk_tiempo,fecha")
##df_SumatoriaGalponProdTemp.createOrReplaceTempView("SumatoriaGalponProdTemp")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaGalponProdTemp"
#}
#df_SumatoriaGalponProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.SumatoriaGalponProdTemp")
#
#print('carga SumatoriaGalponProdTemp')
##Se genera los valores acumulados por galpon
#df_AcumuladosGalponProdTemp = spark.sql(f"select \
#EventDate, \
#MA.pk_tiempo, \
#MA.fecha, \
#fechaCap, \
#MA.pk_etapa, \
#MA.ComplexEntityNo ComplexEntityNoGalpon, \
#B.ProdHTAcum, \
#B.ProdHIAcum \
#from {database_name}.ValoresParaAcumularGalponProd MA left join {database_name}.SumatoriaGalponProdTemp B \
#ON  B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha \
#order by MA.ComplexEntityNo,MA.pk_tiempo")
##df_AcumuladosGalponProdTemp.createOrReplaceTempView("AcumuladosGalponProd")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosGalponProd"
#}
#df_AcumuladosGalponProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.AcumuladosGalponProd")
#
#print('carga AcumuladosGalponProd')
#Se genera los valores acumulados por galpon
df_AcumuladosGalponProdTemp = spark.sql(f"WITH BSD AS ( \
SELECT EventDate,pk_tiempo,fecha ,fechaCap,pk_etapa,ComplexEntityNo ComplexEntityNoGalpon, \
SUM(ProdHT) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHTAcum, \
SUM(ProdHI) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHIAcum \
FROM {database_name}.ValoresParaAcumularGalponProd \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNoGalpon, pk_tiempo")
#df_AcumuladosGalponProdTemp.createOrReplaceTempView("AcumuladosGalponProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosGalponProd"
}
df_AcumuladosGalponProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosGalponProd")

print('carga AcumuladosGalponProd')
#Se  agrupa los datos de ProdHT,ProdHI por lote para generar los valores acumulados
df_ValoresParaAcumularLoteProdTemp = spark.sql(f"select \
EventDate, \
pk_tiempo, \
fecha, \
min(fechaCap) fechaCap, \
max(pk_etapa) pk_etapa, \
substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNo, \
sum(ProdHT) ProdHT, \
sum(ProdHI) ProdHI \
from {database_name}.ValoresParaAcumularProd \
group by EventDate,pk_tiempo,fecha,substring(complexentityno,1,(length(complexentityno)-6)) \
order by eventdate")
#df_ValoresParaAcumularLoteProdTemp.createOrReplaceTempView("ValoresParaAcumularLoteProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ValoresParaAcumularLoteProd"
}
df_ValoresParaAcumularLoteProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ValoresParaAcumularLoteProd")

print('carga ValoresParaAcumularLoteProd')
#df_SumatoriaLoteProdTemp = spark.sql(f"SELECT ComplexEntityNo, pk_etapa,pk_tiempo,fecha, nvl(SUM(ProdHT),0) ProdHTAcum, nvl(SUM(ProdHI),0) ProdHIAcum \
#FROM {database_name}.ValoresParaAcumularLoteProd \
#group by ComplexEntityNo, pk_etapa,pk_tiempo,fecha")
##df_SumatoriaLoteProdTemp.createOrReplaceTempView("SumatoriaLoteProdTemp")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/SumatoriaLoteProdTemp"
#}
#df_SumatoriaLoteProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.SumatoriaLoteProdTemp")
#
#print('carga SumatoriaLoteProdTemp')
##Se genera los valores acumulados por lote
#df_AcumuladosLoteProdTemp = spark.sql(f"select EventDate,MA.pk_tiempo,MA.fecha,fechaCap,MA.pk_etapa,MA.ComplexEntityNo ComplexEntityNoLote, \
#ProdHTAcum, \
#ProdHIAcum \
#from {database_name}.ValoresParaAcumularLoteProd MA left join {database_name}.SumatoriaGalponProdTemp B ON  \
#B.ComplexEntityNo= MA.ComplexEntityNo AND B.pk_etapa = MA.pk_etapa AND B.fecha <= MA.fecha \
#order by MA.ComplexEntityNo,MA.pk_tiempo")
##df_AcumuladosLoteProdTemp.createOrReplaceTempView("AcumuladosLoteProd")
#
## Escribir los resultados en ruta temporal
#additional_options = {
#"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosLoteProd"
#}
#df_AcumuladosLoteProdTemp.write \
#    .format("parquet") \
#    .options(**additional_options) \
#    .mode("overwrite") \
#    .saveAsTable(f"{database_name}.AcumuladosLoteProd")
#
#print('carga AcumuladosLoteProd')
#Se genera los valores acumulados por lote
df_AcumuladosLoteProdTemp = spark.sql(f"WITH BSD AS ( \
SELECT EventDate,pk_tiempo,fecha ,fechaCap,pk_etapa,ComplexEntityNo ComplexEntityNoLote, \
SUM(ProdHT) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHTAcum, \
SUM(ProdHI) OVER ( \
PARTITION BY ComplexEntityNo,pk_etapa \
ORDER BY pk_tiempo \
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
) AS ProdHIAcum \
FROM {database_name}.ValoresParaAcumularLoteProd \
) \
SELECT * FROM BSD \
ORDER BY ComplexEntityNoLote, pk_tiempo")
#df_AcumuladosLoteProdTemp.createOrReplaceTempView("AcumuladosLoteProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosLoteProd"
}
df_AcumuladosLoteProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosLoteProd")

print('carga AcumuladosLoteProd')
#Se crea e inserta los valores acumulados por corral, galpon y lote
df_AcumuladosReproductoraProdTemp = spark.sql(f"select \
A.pk_tiempo, \
A.fecha, \
A.fechaCap, \
A.pk_etapa, \
A.ComplexEntityNo, \
substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNoGalpon, \
substring(A.complexentityno,1,(length(A.complexentityno)-6)) ComplexEntityNoLote, \
A.ProdHTAcum, \
A.ProdHIAcum, \
B.ProdHTAcum ProdHTAcumGalpon, \
B.ProdHIAcum ProdHIAcumGalpon, \
C.ProdHTAcum ProdHTAcumLote, \
C.ProdHIAcum ProdHIAcumLote \
from {database_name}.AcumuladosProd A \
left join {database_name}.AcumuladosGalponProd B ON substring(A.complexentityno,1,(length(A.complexentityno)-3)) = B.ComplexEntityNoGalpon AND A.pk_tiempo = B.pk_tiempo \
left join {database_name}.AcumuladosLoteProd C ON substring(A.complexentityno,1,(length(A.complexentityno)-6)) = C.ComplexEntityNoLote AND A.pk_tiempo = C.pk_tiempo")
#df_AcumuladosReproductoraProdTemp.createOrReplaceTempView("AcumuladosReproductoraProd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/AcumuladosReproductoraProd"
}
df_AcumuladosReproductoraProdTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.AcumuladosReproductoraProd")

print('carga AcumuladosReproductoraProd')
#Convierta las fechas en valores númericos
df_TablaFNTemp = spark.sql(f"select \
pk_tiempo, \
fecha, \
substring(complexentityno,1,(length(complexentityno)-6)) ComplexEntityNo, \
unix_timestamp(cast(EventDate as timestamp)) NumIdfecha, \
FechaNacimiento, \
unix_timestamp(cast(fechanacimiento as timestamp)) NumFechaNacimiento \
from {database_name}.Reprod_Reproductora_upd2 \
order by pk_tiempo")
#df_TablaFNTemp.createOrReplaceTempView("TablaFN")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaFN"
}
df_TablaFNTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.TablaFN")

print('carga TablaFN')
#Saca un promedio de todas las fechas de nacimiento por lote 
df_FNPromLoteTemp = spark.sql(f"select \
ComplexEntityNo, unix_timestamp(cast(CAST(FROM_UNIXTIME(FechaNacimientoPromLote) as date) as timestamp))  FechaNacimientoPromLote \
from ( \
select ComplexEntityNo,avg(NumFechaNacimiento*1.0) FechaNacimientoPromLote \
from {database_name}.TablaFN \
group by ComplexEntityNo \
) A")
#df_FNPromLoteTemp.createOrReplaceTempView("FNPromLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/FNPromLote"
}
df_FNPromLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.FNPromLote")

print('carga FNPromLote')
#Calcula la edad en base a la fecha de nacimiento promedio y el idfecha por lote
df_Edad1Temp = spark.sql(f"select \
pk_tiempo, \
A.fecha, \
A.ComplexEntityNo, \
avg(FechaNacimientoPromLote) FechaNacimientoProm, \
avg(NumIdfecha) FechaIdFecha, \
avg(NumIdfecha) - (avg(FechaNacimientoPromLote)) RestaFecha, \
round(((avg(NumIdfecha) - (avg(FechaNacimientoPromLote)))*1.0)/7,0) + ((ROUND((avg(NumIdfecha) - (avg(FechaNacimientoPromLote))),0)*1.0)%7)/10 as Edad, \
(CASE WHEN (to_timestamp(avg(FechaNacimientoPromLote)))  IS NULL THEN 0 WHEN (avg(FechaNacimientoPromLote)) = -32 THEN 0 \
ELSE (DateDiff(to_timestamp(avg(NumIdfecha)), to_timestamp(avg(FechaNacimientoPromLote)))) + 6 END)/7 AS pk_diavidalote \
from {database_name}.TablaFN A \
left join {database_name}.FNPromLote B on A.complexentityno = B.complexentityno \
group by pk_tiempo,A.fecha, A.ComplexEntityNo \
order by pk_tiempo")
#df_Edad1Temp.createOrReplaceTempView("Edad1")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Edad1"
}
df_Edad1Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Edad1")

print('carga Edad1')
#Inserta los Standard para la edad iddiavidalote
df_STDLoteTemp = spark.sql(f"SELECT DISTINCT pk_semanavida,ComplexEntityNo,U_FeedPerHenWK ,U_FeedPerMaleWK ,U_PercentProdTEWK ,U_PercentProdHEWK , \
                            U_HenWeight,U_MaleWeight,U_TEPerHHACM,U_HEPerHHACM \
                            ,U_HenMortWK,U_MaleMortWK,U_HenMortACM,U_MaleMortACM,U_EggWeight \
                            FROM {database_name}.Reprod_Reproductora_upd2 PE ")
#df_STDLoteTemp.createOrReplaceTempView("STDLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDLote"
}
df_STDLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.STDLote")

print('carga STDLote')
#Se inserta un registro por día de reproductora
df_Reprod_DiarioTemp = spark.sql(f"select \
FR.pk_tiempo, \
FR.fecha, \
1 as pk_empresa, \
FR.pk_division, \
FR.pk_zona, \
FR.pk_subzona, \
FR.pk_plantel, \
FR.pk_lote, \
FR.pk_galpon, \
FR.pk_sexo, \
FR.pk_etapa, \
FR.pk_standard, \
FR.pk_producto, \
FR.pk_tipoproducto, \
max(FR.pk_especie) pk_especie, \
FR.pk_estado, \
FR.pk_administrador, \
FR.pk_proveedor, \
FR.pk_generacion, \
FR.pk_semanavida, \
FR.pk_diasvida, \
FR.ComplexEntityNo, \
FR.FechaNacimiento, \
cast(FROM_UNIXTIME(E.FechaNacimientoProm) as date) FechaNacimientoProm, \
FR.Edad, \
E.Edad EdadLoteCalculado, \
CASE WHEN E.Edad <= 0.0 THEN 0.0 ELSE E.Edad END EdadLoteModificado, \
E.pk_diavidalote, \
FR.semanaReprod, \
MAX(nvl(MR.IngresoH,0)) IngresoH, \
MAX(nvl(MR.IngresoM,0)) IngresoM, \
MAX(nvl(MR.SacaH,0)) SacaH, \
MAX(nvl(MR.SacaM,0)) SacaM, \
MAX(nvl(MR.SacaAcumH,0)) SacaAcumH, \
MAX(nvl(MR.SacaAcumM,0)) SacaAcumM, \
CASE WHEN MAX(nvl(MR.IngresoH * 1.000000,0.0)) <> 0 THEN MAX(nvl(MR.SacaAcumHGalpon * 1.000000,0.0)) ELSE 0.0 END SacaAcumHGalpon, \
CASE WHEN MAX(nvl(MR.IngresoM,0.0)) <> 0 THEN MAX(nvl(MR.SacaAcumMGalpon,0.0)) ELSE 0 END SacaAcumMGalpon, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(nvl(MR.SacaAcumHLote,0)) ELSE 0 END SacaAcumHLote, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(nvl(MR.SacaAcumMLote,0)) ELSE 0 END SacaAcumMLote, \
MAX(nvl(MR.MortH,0)) MortH, \
MAX(nvl(MR.MortM,0)) MortM, \
MAX(nvl(MR.MortAcumH,0)) MortAcumH, \
MAX(nvl(MR.MortAcumM,0)) MortAcumM, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(nvl(MR.MortAcumHGalpon,0)) ELSE 0 END  MortAcumHGalpon, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(nvl(MR.MortAcumMGalpon,0)) ELSE 0 END  MortAcumMGalpon, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(nvl(MR.MortAcumHLote,0)) ELSE 0 END  MortAcumHLote, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(nvl(MR.MortAcumMLote,0)) ELSE 0 END  MortAcumMLote, \
MAX(nvl(MR.TransIngH,0)) TransIngH, \
MAX(nvl(MR.TransSalH,0)) TransSalH, \
MAX(nvl(MR.TransIngM,0)) TransIngM, \
MAX(nvl(MR.TransSalM,0)) TransSalM, \
MAX(MR.TransIngAcumH) as TransIngAcumH, \
MAX(MR.TransSalAcumH) as TransSalAcumH, \
MAX(MR.TransIngAcumM) as TransIngAcumM, \
MAX(MR.TransSalAcumM) as TransSalAcumM, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(MR.TransIngAcumHGalpon) ELSE 0 END as TransIngAcumHGalpon, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(MR.TransSalAcumHGalpon) ELSE 0 END as TransSalAcumHGalpon, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(MR.TransIngAcumMGalpon) ELSE 0 END as TransIngAcumMGalpon, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(MR.TransSalAcumMGalpon) ELSE 0 END as TransSalAcumMGalpon, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(MR.TransIngAcumHLote) ELSE 0 END as TransIngAcumHLote, \
CASE WHEN MAX(nvl(MR.IngresoH,0)) <> 0 THEN MAX(MR.TransSalAcumHLote) ELSE 0 END as TransSalAcumHLote, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(MR.TransIngAcumMLote) ELSE 0 END as TransIngAcumMLote, \
CASE WHEN MAX(nvl(MR.IngresoM,0)) <> 0 THEN MAX(MR.TransSalAcumMLote) ELSE 0 END as TransSalAcumMLote, \
MAX(nvl(MR.SaldoH,0)) SaldoH, \
MAX(nvl(MR.SaldoM,0)) SaldoM, \
MAX(nvl(MR.PorcMortH,0)) PorcMortH, \
MAX(nvl(MR.PorcMortM,0)) PorcMortM, \
SUM(nvl(FR.FeedConsumedF,0)) ConsAlimH, \
SUM(nvl(FR.FeedConsumedM,0)) ConsAlimM, \
MAX(nvl(FR.U_FeedPerHenWK,0)) StdGADiaH, \
MAX(nvl(FR.U_FeedPerMaleWK,0)) StdGADiaM, \
MAX(nvl(FR.U_PercentProdTEWK,0)) StdPorcProdHT, \
MAX(nvl(FR.U_PercentProdHEWK,0)) StdPorcProdHI, \
MAX(nvl(FR.U_TEPerHHACM,0)) StdHTGEAcum, \
MAX(nvl(FR.U_HEPerHHACM,0)) StdHIGEAcum, \
MAX(nvl(FR.U_HenWeight,0)) StdPesoH, \
MAX(nvl(FR.U_MaleWeight,0)) StdPesoM, \
MAX(nvl(FR.U_EggWeight,0)) StdPesoHvo, \
MAX(nvl(FR.U_HenMortWK,0)) StdMortH, \
MAX(nvl(FR.U_MaleMortWK,0)) StdMortM, \
MAX(nvl(FR.U_HenMortACM,0)) StdMortAcmH, \
MAX(nvl(FR.U_MaleMortACM,0)) StdMortAcmM, \
MAX(nvl(ST.U_FeedPerHenWK,0)) StdGADiaHLt, \
MAX(nvl(ST.U_FeedPerMaleWK,0)) StdGADiaMLt, \
MAX(nvl(ST.U_PercentProdTEWK,0)) StdPorcProdHTLt, \
MAX(nvl(ST.U_PercentProdHEWK,0)) StdPorcProdHILt, \
MAX(nvl(ST.U_TEPerHHACM,0)) StdHTGEAcumLt, \
MAX(nvl(ST.U_HEPerHHACM,0)) StdHIGEAcumLt, \
MAX(nvl(ST.U_HenWeight,0)) StdPesoHLt, \
MAX(nvl(ST.U_MaleWeight,0)) StdPesoMLt, \
MAX(nvl(ST.U_EggWeight,0)) StdPesoHvoLt, \
MAX(nvl(ST.U_HenMortWK,0)) StdMortHLt, \
MAX(nvl(ST.U_MaleMortWK,0)) StdMortMLt, \
MAX(nvl(ST.U_HenMortACM,0)) StdMortAcmHLt, \
MAX(nvl(ST.U_MaleMortACM,0)) StdMortAcmMLt, \
SUM(nvl(FR.ProdHT,0)) ProdHT, \
SUM(nvl(FR.ProdHI,0)) ProdHI, \
SUM(nvl(FR.PesoHvo,0)) PesoHvo, \
SUM(nvl(FR.PesoH,0)) PesoH, \
SUM(nvl(FR.PesoM,0)) PesoM, \
SUM(nvl(FR.UniformityF,0)) UnifH, \
SUM(nvl(FR.UniformityM,0)) UnifM, \
SUM(nvl(FR.U_PorosoProd,0)) ProdPoroso, \
SUM(nvl(FR.U_SucioProd,0)) ProdSucio, \
SUM(nvl(FR.U_SangreProd,0)) ProdSangre, \
SUM(nvl(FR.U_DeformeProd,0)) ProdDeforme, \
SUM(nvl(FR.U_CascaraDebilProd,0)) ProdCascaraDebil, \
SUM(nvl(FR.U_RotoProd,0)) ProdRoto, \
SUM(nvl(FR.U_FarfaraProd,0)) ProdFarfara, \
SUM(nvl(FR.U_InvendibleProd,0)) ProdInvendible, \
SUM(nvl(FR.U_ChicoProd,0)) ProdChico, \
SUM(nvl(FR.U_DobleYemaProd,0)) ProdDobleYema, \
SUM(nvl(FR.U_HI_de_piso,0)) PorcHvoPiso, \
CASE WHEN FR.pk_sexo = 2 THEN SUM(nvl(FR.U_UnidadesSeleccionadas,0)) END SelecH, \
CASE WHEN FR.pk_sexo = 3 THEN SUM(nvl(FR.U_UnidadesSeleccionadas,0)) END Selecm, \
FR.DateCap FechaCap, \
MAX(nvl(FR.HensCap,0)) CapH, \
MAX(nvl(FR.MalesCap,0)) CapM, \
MAX(nvl(TA.ProdHTAcum,0)) ProdHTAcum, \
MAX(nvl(TA.ProdHIAcum,0)) ProdHIAcum, \
MAX(nvl(TA.ProdHTAcumGalpon,0)) ProdHTAcumGalpon, \
MAX(nvl(TA.ProdHIAcumGalpon,0)) ProdHIAcumGalpon, \
MAX(nvl(TA.ProdHTAcumLote,0)) ProdHTAcumLote, \
MAX(nvl(TA.ProdHIAcumLote,0)) ProdHIAcumLote, \
MAX(nvl(MR.U_PEErrorSexoH,0)) U_PEErrorSexoH, \
MAX(nvl(MR.U_PEErrorSexoM,0)) U_PEErrorSexoM, \
MR.ListaPadre, \
SUM(nvl(FR.AlimInicio,0)) AlimInicio, \
SUM(nvl(FR.AlimCrecimiento,0)) AlimCrecimiento, \
SUM(nvl(FR.AlimPreReprod,0)) AlimPreReprod, \
SUM(nvl(FR.AlimReprodI,0)) AlimReprodI, \
SUM(nvl(FR.AlimReprodII,0)) AlimReprodII, \
SUM(nvl(FR.AlimReprodMacho,0)) AlimReprodMacho, \
SUM(nvl(FR.PosturaCrecIRP,0)) PosturaCrecIRP, \
SUM(nvl(FR.PosturaPreInicioRP,0)) PosturaPreInicioRP, \
SUM(nvl(FR.PreInicialPavas,0)) PreInicialPavas, \
SUM(nvl(FR.InicialPavosRP,0)) InicialPavosRP, \
SUM(nvl(FR.CreIPavas,0)) CreIPavas, \
SUM(nvl(FR.CrecIIPavas,0)) CrecIIPavas, \
SUM(nvl(FR.MantPavas,0)) MantPavas, \
nvl((MAX(nvl(MR.SacaAcumH,0.000000)*1.000000)/NULLIF(MAX(nvl(FR.HensCap,0.000000)),0.000000))*100.0000000,0.000000) CondicionH, \
MAX(to_timestamp(FechaMax)) FechaMax, \
MAX(to_timestamp(FechaCapitalizacionMax)) FechaCapitalizacionMax \
from {database_name}.Reprod_Reproductora_upd2 FR \
LEFT JOIN {database_name}.ft_Reprod_Mort_Diario MR on FR.ComplexEntityNo = MR.ComplexEntityNo and FR.pk_diasvida = MR.pk_diasvida and FR.pk_tiempo = MR.pk_tiempo \
LEFT JOIN {database_name}.AcumuladosReproductoraProd TA on FR.ComplexEntityNo = TA.ComplexEntityNo and FR.pk_tiempo = TA.pk_tiempo \
LEFT JOIN {database_name}.Edad1 E on substring(FR.complexentityno,1,(length(FR.complexentityno)-6)) = E.complexentityno and Fr.pk_tiempo = E.pk_tiempo \
LEFT JOIN {database_name}.STDLote ST on FR.ComplexEntityNo = ST.ComplexEntityNo and cast(E.pk_diavidalote as int) = cast(ST.pk_semanavida as int) \
WHERE FR.GRN = 'P' \
AND FR.PK_division in (2,4,5) \
GROUP BY FR.pk_tiempo,FR.fecha,FR.pk_division,FR.pk_zona,FR.pk_subzona,FR.pk_plantel,FR.pk_lote,FR.pk_galpon,FR.pk_sexo,FR.pk_etapa,FR.pk_standard,FR.pk_producto, \
FR.pk_tipoproducto,FR.pk_estado,FR.pk_administrador,FR.pk_proveedor,FR.pk_generacion,FR.pk_semanavida,FR.pk_diasvida,FR.ComplexEntityNo, \
FR.FechaNacimiento,FR.Edad,E.Edad,E.pk_diavidalote,cast(FROM_UNIXTIME(E.FechaNacimientoProm) as date),FR.DateCap,FR.semanaReprod,MR.ListaPadre")
#-- 3 Pollo -- 5 Huevo -- 4 Pavo
#date_format(FR.fecha,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 12 MONTH), 'yyyyMM')
#df_Reprod_DiarioTemp.createorReplaceTempView("Reprod_Diario")
#1- hembre
#2- macho
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Diario"
}
df_Reprod_DiarioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Diario")

print('carga Reprod_Diario')
df_maxStdGADiaH = spark.sql(f"SELECT Complexentityno,pk_semanavida, max(StdGADiaH) StdGADiaH \
FROM {database_name}.Reprod_Diario group by Complexentityno,pk_semanavida")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxStdGADiaH"
}
df_maxStdGADiaH.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaxStdGADiaH")
print('carga MaxStdGADiaH')
#Inserta los últimos 4 meses de la tabla
df_ft_Reprod_Diario = spark.sql(f"select RE.pk_tiempo,RE.fecha,RE.pk_empresa,RE.pk_division,RE.pk_zona,RE.pk_subzona,RE.pk_plantel,RE.pk_lote, \
RE.pk_galpon,RE.pk_sexo,RE.pk_etapa,RE.pk_standard,RE.pk_producto,RE.pk_tipoproducto,RE.pk_especie,RE.pk_estado,RE.pk_administrador,RE.pk_proveedor, \
RE.pk_generacion,RE.pk_semanavida,RE.pk_diasvida,RE.ComplexEntityNo,RE.FechaNacimiento,RE.FechaCap,RE.Edad, \
RE.IngresoH AlojH \
,RE.IngresoM AlojM \
,RE.MortH \
,RE.MortM \
,RE.MortAcumH \
,RE.MortAcumM \
,ROUND((nvl((RE.MortH/NULLIF((RE.SaldoH)*1.000000,0.000000))*100.0000000,0.000000)),2) PorcMortH \
,ROUND((nvl((RE.MortM/NULLIF((RE.SaldoM)*1.000000,0.000000))*100.0000000,0.000000)),2) PorcMortM \
,CASE WHEN RE.pk_etapa = 1 THEN ROUND((nvl((RE.MortAcumH/NULLIF((RE.IngresoH)*1.0,0))*100.0000000,0.000000)),2) \
ELSE ROUND((nvl((RE.MortAcumH/NULLIF((RE.CapH)*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcMortAcumH \
,CASE WHEN RE.pk_etapa = 1 THEN ROUND((nvl((RE.MortAcumM/NULLIF((RE.IngresoM)*1.000000,0.000000))*100.0000000,0.000000)),2) \
ELSE ROUND((nvl((RE.MortAcumM/NULLIF((RE.CapM)*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcMortAcumM \
,RE.SacaH BenefH \
,RE.SacaM BenefM \
,RE.SacaAcumH BenefAcumH \
,RE.SacaAcumM BenefAcumM \
,RE.TransIngH \
,RE.TransSalH \
,RE.TransIngM \
,RE.TransSalM \
,RE.TransIngAcumH \
,RE.TransSalAcumH \
,RE.TransIngAcumM \
,RE.TransSalAcumM \
,RE.SaldoH \
,RE.SaldoM \
,ROUND((nvl(((RE.SaldoM)/(NULLIF((RE.SaldoH)*1.000000,0.000000)))*100,0.000000)),2) PorcMH \
,RE.StdMortH \
,RE.StdMortM \
,RE.StdMortAcmH \
,RE.StdMortAcmM \
,RE.SelecH \
,RE.SelecM \
,ROUND(nvl(((RE.ConsAlimH/NULLIF(RE.SaldoH,0.000000)))*1000.000000,0.000000),2) GADH \
,RE.StdGADiaH \
,nvl((ROUND((((RE.ConsAlimH/NULLIF(RE.SaldoH,0.000000)))*1000.000000),2) - ROUND((((RE2.ConsAlimH/NULLIF(RE2.SaldoH,0.000000)))*1000.000000),2)),0.000000) IncrGADH \
,nvl((RE.StdGADiaH - A.StdGADiaH ),0) IncrStdGADH \
,ROUND(nvl(((RE.ConsAlimM/NULLIF(RE.SaldoM,0.000000)))*1000.000000,0.000000),2) GADM \
,RE.StdGADiaM \
,RE.ProdHT \
,RE.ProdHTAcum \
,ROUND((nvl(((RE.ProdHT/NULLIF((RE.SaldoH)*1.000000,0.000000)))*100.0000000,0)),2) PorcProdHT \
,RE.StdPorcProdHT \
,RE.ProdHI \
,RE.ProdHIAcum \
,ROUND((nvl((RE.ProdHI/NULLIF((RE.ProdHT*1.000000),0))*100.0000000,0.000000)),2) PorcProdHI \
,RE.StdPorcProdHI \
,ROUND((nvl(RE.ProdHTAcum/NULLIF(RE.CapH*1.000000,0.000000),0.000000)),2) HTGE \
,ROUND((nvl(RE.ProdHIAcum/NULLIF(RE.CapH*1.000000,0.000000),0.000000)),2) HIGE \
,RE.StdHTGEAcum \
,RE.StdHIGEAcum \
,RE.PesoHvo \
,RE.PesoH \
,RE.StdPesoH \
,RE.PesoM \
,RE.StdPesoM \
,RE.UnifH \
,RE.UnifM \
,RE.PorcHvoPiso \
,CASE WHEN RE.pk_etapa = 1 THEN ROUND((nvl(((RE.SaldoH)/NULLIF(RE.IngresoH*1.000000,0.000000))*100.0000000,0.000000)),2) \
ELSE ROUND((nvl(((RE.SaldoH)/NULLIF(RE.CapH*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcViabProdH \
,CASE WHEN RE.pk_etapa = 1 THEN ROUND((nvl(((RE.SaldoM)/NULLIF(RE.IngresoM*1.000000,0.000000))*100.0000000,0.000000)),2) \
ELSE ROUND((nvl(((RE.SaldoM)/NULLIF(RE.CapM*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcViabProdM \
,(100.0000000 - RE.StdMortAcmH) StdViabH \
,0 StdViabM \
,RE.CapH \
,RE.CapM \
,RE.ConsAlimH \
,RE.ConsAlimM \
,RE.ProdPoroso \
,RE.ProdSucio \
,RE.ProdSangre \
,RE.ProdDeforme \
,RE.ProdCascaraDebil \
,RE.ProdRoto \
,RE.ProdFarfara \
,RE.ProdInvendible \
,RE.ProdChico \
,RE.ProdDobleYema \
,RE.U_PEErrorSexoH \
,RE.U_PEErrorSexoM \
,RE.ListaPadre \
,RE.AlimInicio \
,RE.AlimCrecimiento \
,RE.AlimPreReprod \
,RE.AlimReprodI \
,RE.AlimReprodII \
,RE.AlimReprodMacho \
,RE.PosturaCrecIRP \
,RE.PosturaPreInicioRP \
,RE.STDPesoHvo \
,RE.PreInicialPavas \
,RE.InicialPavosRP \
,RE.CreIPavas \
,RE.CrecIIPavas \
,RE.MantPavas \
from {database_name}.Reprod_Diario RE  left join \
{database_name}.Reprod_Diario RE2 on RE2.Complexentityno = RE.Complexentityno AND RE2.pk_diasvida = RE.pk_diasvida -1 \
left join {database_name}.MaxStdGADiaH A ON A.Complexentityno = RE.Complexentityno  AND A.pk_semanavida = RE.pk_semanavida -1 \
where RE.CondicionH < 5.00000")

print('carga df_ft_Reprod_Diario')
# Verificar si la tabla gold ya existe - ft_Reprod_Diario
#gold_table = spark.read.format("parquet").load(path_target8)  
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes8 = spark.read.format("parquet").load(path_target8)
    datos_existentes8 = True
    logger.info(f"Datos existentes de ft_Reprod_Diario cargados: {df_existentes8.count()} registros")
except:
    datos_existentes8 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Diario")


if datos_existentes8:
    existing_data8 = spark.read.format("parquet").load(path_target8)
    data_after_delete8 = existing_data8.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data8 = df_ft_Reprod_Diario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data8 = filtered_new_data8.union(data_after_delete8)                             
   
    cant_ingresonuevo8 = filtered_new_data8.count()
    cant_total8 = final_data8.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_DiarioTemporal"
    }
    final_data8.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data8_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Diario"
    }
    final_data8_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Diario : {cant_ingresonuevo8}")
    print(f"Total de registros en la tabla ft_Reprod_Diario : {cant_total8}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_DiarioTemporal')
    print(f"Tabla ft_Reprod_DiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Diario"
    }
    df_ft_Reprod_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Diario")
#Se inserta en tabla temporal los registros diarios agrupados por galpon en la etapa de cria
df_Reprod_Galpon_Diario_CriaTemp = spark.sql(f"select \
                                              RD.pk_tiempo, \
                                              RD.fecha, \
                                              RD.pk_empresa, \
                                              RD.pk_division, \
                                              RD.pk_zona, \
                                              RD.pk_subzona, \
                                              RD.pk_plantel, \
                                              RD.pk_lote, \
                                              RD.pk_galpon, \
                                              max(pk_etapa) pk_etapa, \
                                              min(pk_standard) pk_standard, \
                                              pk_tipoproducto, \
                                              max(RD.pk_especie) pk_especie, \
                                              max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especieg, \
                                              min(pk_estado) pk_estado, \
                                              RD.pk_administrador, \
                                              RD.pk_proveedor, \
                                              pk_generacion, \
                                              max(pk_semanavida) pk_semanavida, \
                                              max(pk_diasvida) pk_diasvida, \
                                              substring(RD.complexentityno,1,(length(RD.complexentityno)-3)) ComplexEntityNo, \
                                              semanaReprod, \
                                              min(fechanacimiento) fechanacimiento, \
                                              max(edad) edad, \
                                              max(nvl(IngresoH,0)) IngresoH, \
                                              max(nvl(IngresoM,0)) IngresoM, \
                                              sum(nvl(SacaH,0)) SacaH, \
                                              sum(nvl(SacaM,0)) SacaM, \
                                              max(nvl(SacaAcumHGalpon * 1.000000,0)) SacaAcumH, \
                                              max(nvl(SacaAcumMGalpon,0)) SacaAcumM, \
                                              sum(nvl(MortH,0)) MortH, \
                                              sum(nvl(MortM,0)) MortM, \
                                              max(nvl(MortAcumHGalpon,0)) MortAcumH, \
                                              max(nvl(MortAcumMGalpon,0)) MortAcumM, \
                                              sum(nvl(TransIngH,0)) TransIngH, \
                                              sum(nvl(TransSalH,0)) TransSalH, \
                                              sum(nvl(TransIngM,0)) TransIngM, \
                                              sum(nvl(TransSalM,0)) TransSalM, \
                                              max(nvl(TransIngAcumHGalpon,0)) TransIngAcumHGalpon, \
                                              max(nvl(TransSalAcumHGalpon,0)) TransSalAcumHGalpon, \
                                              max(nvl(TransIngAcumMGalpon,0)) TransIngAcumMGalpon, \
                                              max(nvl(TransSalAcumMGalpon,0)) TransSalAcumMGalpon, \
                                              CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoH,0)) - max(nvl(SacaAcumHGalpon * 1.000000,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) \
                                                      ELSE max(nvl(CapH,0)) - max(nvl(SacaAcumHGalpon * 1.000000,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) END SaldoH, \
                                              CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) \
                                                      ELSE max(nvl(CapM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) END SaldoM, \
                                              sum(nvl(ConsAlimH,0)) ConsAlimH, \
                                              sum(nvl(ConsAlimM,0)) ConsAlimM, \
                                              max(nvl(StdGADiaH,0)) StdGADiaH, \
                                              max(nvl(StdGADiaM,0)) StdGADiaM, \
                                              max(nvl(StdPorcProdHT,0)) StdPorcProdHT, \
                                              max(nvl(StdPorcProdHI,0)) StdPorcProdHI, \
                                              max(nvl(StdHTGEAcum,0)) StdHTGEAcum, \
                                              max(nvl(StdHIGEAcum,0)) StdHIGEAcum, \
                                              max(nvl(StdPesoH,0)) StdPesoH, \
                                              max(nvl(StdPesoM,0)) StdPesoM, \
                                              max(nvl(StdPesoHvo,0)) StdPesoHvo, \
                                              max(nvl(StdMortH,0)) StdMortH, \
                                              max(nvl(StdMortM,0)) StdMortM, \
                                              max(nvl(StdMortAcmH,0)) StdMortAcmH, \
                                              max(nvl(StdMortAcmM,0)) StdMortAcmM, \
                                              sum(nvl(ProdHT,0)) ProdHT, \
                                              sum(nvl(ProdHI,0)) ProdHI, \
                                              max(nvl(ProdHTAcumGalpon,0)) ProdHTAcum, \
                                              max(nvl(ProdHIAcumGalpon,0)) ProdHIAcum, \
                                              max(nvl(PesoHvo,0)) PesoHvo, \
                                              max(nvl(PesoH,0)) PesoH, \
                                              max(nvl(PesoM,0)) PesoM, \
                                              max(nvl(UnifH,0)) UnifH, \
                                              max(nvl(UnifM,0)) UnifM, \
                                              sum(nvl(ProdPoroso,0)) ProdPoroso, \
                                              sum(nvl(ProdSucio,0)) ProdSucio, \
                                              sum(nvl(ProdSangre,0)) ProdSangre, \
                                              sum(nvl(ProdDeforme,0)) ProdDeforme, \
                                              sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
                                              sum(nvl(ProdRoto,0)) ProdRoto, \
                                              sum(nvl(ProdFarfara,0)) ProdFarfara, \
                                              sum(nvl(ProdInvendible,0)) ProdInvendible, \
                                              sum(nvl(ProdChico,0)) ProdChico, \
                                              sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
                                              sum(nvl(SelecH,0)) SelecH, \
                                              sum(nvl(SelecM,0)) SelecM, \
                                              min(FechaCap) FechaCap, \
                                              max(nvl(CapH,0)) CapH, \
                                              max(nvl(CapM,0)) CapM, \
                                              nvl((ROUND(sum(PorcHvoPiso),2)),0) PorcHvoPiso, \
                                              sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
                                              sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
                                              SUM(nvl(AlimInicio,0)) AlimInicio, \
                                              SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
                                              SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
                                              SUM(nvl(AlimReprodI,0)) AlimReprodI, \
                                              SUM(nvl(AlimReprodII,0)) AlimReprodII, \
                                              SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
                                              SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
                                              SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
                                              SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
                                              SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
                                              SUM(nvl(CreIPavas,0)) CreIPavas, \
                                              SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
                                              SUM(nvl(MantPavas,0)) MantPavas, \
                                              nvl((MAX(nvl(SacaAcumHGalpon * 1.000000,0.000000)*1.0000000)/NULLIF(max(nvl(CapH,0.000000)),0.000000))*100.0000000,0.000000) CondicionH, \
                                              MAX(RD.FechaMax) FechaMax \
                                              from {database_name}.Reprod_Diario RD \
                                              LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on substring(RD.complexentityno,1,(length(RD.complexentityno)-3)) = FIR.ComplexEntityNo \
                                              where pk_etapa = 1 \
                                              group by RD.pk_tiempo,RD.fecha,RD.pk_empresa,RD.pk_division,RD.pk_zona,RD.pk_subzona,RD.pk_plantel,RD.pk_lote,RD.pk_galpon,pk_tipoproducto,RD.pk_administrador, \
                                              RD.pk_proveedor,pk_generacion,substring(RD.complexentityno,1,(length(RD.complexentityno)-3)),semanaReprod \
                                              order by pk_tiempo,pk_semanavida")

#df_Reprod_Galpon_Diario_CriaTemp.createOrReplaceTempView("Reprod_Galpon_Diario_Cria")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Galpon_Diario_Cria"
}
df_Reprod_Galpon_Diario_CriaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Galpon_Diario_Cria")
print('carga Reprod_Galpon_Diario_Cria')
#EQ_revisar la columan SacaAcumHGalpon
#Se inserta en tabla temporal los registros diarios agrupados por galpon en la etapa de levante
df_Reprod_Galpon_Diario_LevanteTemp = spark.sql(f"select \
                                                 RD.pk_tiempo, \
                                                 RD.fecha, \
                                                 RD.pk_empresa, \
                                                 RD.pk_division, \
                                                 RD.pk_zona, \
                                                 RD.pk_subzona, \
                                                 RD.pk_plantel, \
                                                 RD.pk_lote, \
                                                 RD.pk_galpon, \
                                                 max(pk_etapa) pk_etapa, \
                                                 min(pk_standard) pk_standard, \
                                                 pk_tipoproducto, \
                                                 max(RD.pk_especie) pk_especie, \
                                                 max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especieg, \
                                                 max(pk_estado) pk_estado, \
                                                 RD.pk_administrador, \
                                                 RD.pk_proveedor, \
                                                 pk_generacion, \
                                                 max(pk_semanavida) pk_semanavida, \
                                                 max(pk_diasvida) pk_diasvida, \
                                                 substring(RD.complexentityno,1,(length(RD.complexentityno)-3)) ComplexEntityNo, \
                                                 semanaReprod, \
                                                 min(fechanacimiento) fechanacimiento, \
                                                 max(edad) edad, \
                                                 max(nvl(IngresoH,0)) IngresoH, \
                                                 max(nvl(IngresoM,0)) IngresoM, \
                                                 sum(nvl(SacaH,0)) SacaH, \
                                                 sum(nvl(SacaM,0)) SacaM, \
                                                 max(nvl(SacaAcumHGalpon * 1.000000,0)) SacaAcumH, \
                                                 max(nvl(SacaAcumMGalpon,0)) SacaAcumM, \
                                                 sum(nvl(MortH,0)) MortH, \
                                                 sum(nvl(MortM,0)) MortM, \
                                                 max(nvl(MortAcumHGalpon,0)) MortAcumH, \
                                                 max(nvl(MortAcumMGalpon,0)) MortAcumM, \
                                                 sum(nvl(TransIngH,0)) TransIngH, \
                                                 sum(nvl(TransSalH,0)) TransSalH, \
                                                 sum(nvl(TransIngM,0)) TransIngM, \
                                                 sum(nvl(TransSalM,0)) TransSalM, \
                                                 max(nvl(TransIngAcumHGalpon,0)) TransIngAcumHGalpon, \
                                                 max(nvl(TransSalAcumHGalpon,0)) TransSalAcumHGalpon, \
                                                 max(nvl(TransIngAcumMGalpon,0)) TransIngAcumMGalpon, \
                                                 max(nvl(TransSalAcumMGalpon,0)) TransSalAcumMGalpon, \
                                                 CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoH,0)) - max(nvl(SacaAcumHGalpon * 1.000000,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) \
                                                      ELSE max(nvl(CapH,0)) - max(nvl(SacaAcumHGalpon * 1.000000,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) END SaldoH, \
                                                 CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) \
                                                      ELSE max(nvl(CapM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) END SaldoM, \
                                                 sum(nvl(ConsAlimH,0)) ConsAlimH, \
                                                 sum(nvl(ConsAlimM,0)) ConsAlimM, \
                                                 max(nvl(StdGADiaH,0)) StdGADiaH, \
                                                 max(nvl(StdGADiaM,0)) StdGADiaM, \
                                                 max(nvl(StdPorcProdHT,0)) StdPorcProdHT, \
                                                 max(nvl(StdPorcProdHI,0)) StdPorcProdHI, \
                                                 max(nvl(StdHTGEAcum,0)) StdHTGEAcum, \
                                                 max(nvl(StdHIGEAcum,0)) StdHIGEAcum, \
                                                 max(nvl(StdPesoH,0)) StdPesoH, \
                                                 max(nvl(StdPesoM,0)) StdPesoM, \
                                                 max(nvl(StdPesoHvo,0)) StdPesoHvo, \
                                                 max(nvl(StdMortH,0)) StdMortH, \
                                                 max(nvl(StdMortM,0)) StdMortM, \
                                                 max(nvl(StdMortAcmH,0)) StdMortAcmH, \
                                                 max(nvl(StdMortAcmM,0)) StdMortAcmM, \
                                                 sum(nvl(ProdHT,0)) ProdHT, \
                                                 sum(nvl(ProdHI,0)) ProdHI, \
                                                 max(nvl(ProdHTAcumGalpon,0)) ProdHTAcum, \
                                                 max(nvl(ProdHIAcumGalpon,0)) ProdHIAcum, \
                                                 max(nvl(PesoHvo,0)) PesoHvo, \
                                                 max(nvl(PesoH,0)) PesoH, \
                                                 max(nvl(PesoM,0)) PesoM, \
                                                 max(nvl(UnifH,0)) UnifH, \
                                                 max(nvl(UnifM,0)) UnifM, \
                                                 sum(nvl(ProdPoroso,0)) ProdPoroso, \
                                                 sum(nvl(ProdSucio,0)) ProdSucio, \
                                                 sum(nvl(ProdSangre,0)) ProdSangre, \
                                                 sum(nvl(ProdDeforme,0)) ProdDeforme, \
                                                 sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
                                                 sum(nvl(ProdRoto,0)) ProdRoto, \
                                                 sum(nvl(ProdFarfara,0)) ProdFarfara, \
                                                 sum(nvl(ProdInvendible,0)) ProdInvendible, \
                                                 sum(nvl(ProdChico,0)) ProdChico, \
                                                 sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
                                                 sum(nvl(SelecH,0)) SelecH, \
                                                 sum(nvl(SelecM,0)) SelecM, \
                                                 min(FechaCap) FechaCap, \
                                                 max(nvl(CapH,0)) CapH, \
                                                 max(nvl(CapM,0)) CapM, \
                                                 nvl((ROUND(sum(PorcHvoPiso),2)),0) PorcHvoPiso, \
                                                 sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
                                                 sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
                                                 SUM(nvl(AlimInicio,0)) AlimInicio, \
                                                 SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
                                                 SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
                                                 SUM(nvl(AlimReprodI,0)) AlimReprodI, \
                                                 SUM(nvl(AlimReprodII,0)) AlimReprodII, \
                                                 SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
                                                 SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
                                                 SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
                                                 SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
                                                 SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
                                                 SUM(nvl(CreIPavas,0)) CreIPavas, \
                                                 SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
                                                 SUM(nvl(MantPavas,0)) MantPavas, \
                                                 nvl((MAX(nvl(SacaAcumHGalpon * 1.000000,0.000000)*1.000000)/NULLIF(max(nvl(CapH,0.000000)),0.000000))*100.0000000,0.000000) CondicionH, \
                                                 MAX(RD.FechaMax) FechaMax \
                                                 from {database_name}.Reprod_Diario RD \
                                                 LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on substring(rtrim(ltrim(RD.complexentityno)),1,(length(RD.complexentityno)-3)) = rtrim(ltrim(FIR.complexentityno)) \
                                                 where pk_etapa = 3 \
                                                 group by RD.pk_tiempo,RD.fecha,RD.pk_empresa,RD.pk_division,RD.pk_zona,RD.pk_subzona,RD.pk_plantel,RD.pk_lote, \
                                                 RD.pk_galpon,pk_tipoproducto,RD.pk_administrador,RD.pk_proveedor,pk_generacion,substring(RD.complexentityno,1,(length(RD.complexentityno)-3)),semanaReprod \
                                                 order by pk_tiempo,pk_semanavida")
#df_Reprod_Galpon_Diario_LevanteTemp.createOrReplaceTempView("Reprod_Galpon_Diario_Levante")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Galpon_Diario_Levante"
}
df_Reprod_Galpon_Diario_LevanteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Galpon_Diario_Levante")
print('carga Reprod_Galpon_Diario_Levante')
#Inserta los últimos 4 meses de la tabla
#insert into DMPECUARIO.ft_Reprod_Galpon_Diario
df_ft_Reprod_Galpon_Diario1 = spark.sql(f"WITH Reprod_Galpon_Diario_Cria_Filtrado AS ( \
    SELECT \
    RPG.*, \
    ROUND(nvl((RPG.MortH / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcMortH, \
    ROUND(nvl((RPG.MortM / NULLIF((RPG.SaldoM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcMortM, \
    CASE \
    WHEN RPG.pk_etapa = 1 THEN ROUND(nvl((RPG.MortAcumH / NULLIF((RPG.IngresoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    ELSE ROUND(nvl((RPG.MortAcumH / NULLIF((RPG.CapH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    END AS PorcMortAcumH, \
    CASE \
    WHEN RPG.pk_etapa = 1 THEN ROUND(nvl((RPG.MortAcumM / NULLIF((RPG.IngresoM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    ELSE ROUND(nvl((RPG.MortAcumM / NULLIF((RPG.CapM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    END AS PorcMortAcumM, \
    ROUND(nvl((RPG.ConsAlimH / NULLIF(RPG.SaldoH, 0.000000)) * 1000.000000, 0.000000), 2) AS GADH, \
    ROUND(nvl((RPG.ConsAlimM / NULLIF(RPG.SaldoM, 0.000000)) * 1000.000000, 0.000000), 2) AS GADM, \
    ROUND(nvl((RPG.ProdHT / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcProdHT, \
    ROUND(nvl((RPG.ProdHI / NULLIF((RPG.ProdHT) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcProdHI, \
    ROUND(nvl(RPG.ProdHTAcum / NULLIF(RPG.CapH * 1.000000, 0.000000), 0.000000), 2) AS HTGE, \
    ROUND(nvl(RPG.ProdHIAcum / NULLIF(RPG.CapH * 1.000000, 0.000000), 0.000000), 2) AS HIGE, \
    CASE \
    WHEN RPG.pk_etapa = 1 THEN ROUND(nvl(((RPG.SaldoH) / NULLIF(RPG.IngresoH * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    ELSE ROUND(nvl(((RPG.SaldoH) / NULLIF(RPG.CapH * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    END AS PorcViabProdH, \
    CASE \
    WHEN RPG.pk_etapa = 1 THEN ROUND(nvl(((RPG.SaldoM) / NULLIF(RPG.IngresoM * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    ELSE ROUND(nvl(((RPG.SaldoM) / NULLIF(RPG.CapM * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
    END AS PorcViabProdM \
    FROM \
    {database_name}.Reprod_Galpon_Diario_Cria RPG \
    WHERE \
    (date_format(RPG.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')) \
), \
CalculosAdicionales AS ( \
SELECT \
*, \
nvl((GADH - LAG(GADH, 1) OVER (PARTITION BY Complexentityno ORDER BY pk_tiempo)), 0.000000) AS IncrGHAH, \
nvl((StdGADiaH - MAX(StdGADiaH) OVER (PARTITION BY Complexentityno, pk_semanavida ORDER BY pk_tiempo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), 0.000000) AS IncrStdGHAH \
FROM \
Reprod_Galpon_Diario_Cria_Filtrado \
) \
SELECT \
RPG.pk_tiempo, \
RPG.fecha, \
RPG.pk_empresa, \
RPG.pk_division, \
RPG.pk_zona, \
RPG.pk_subzona, \
RPG.pk_plantel, \
RPG.pk_lote, \
RPG.pk_galpon, \
RPG.pk_etapa, \
RPG.pk_standard, \
RPG.pk_tipoproducto, \
RPG.pk_especieg, \
RPG.pk_estado, \
RPG.pk_administrador, \
RPG.pk_proveedor, \
RPG.pk_generacion, \
RPG.pk_semanavida, \
RPG.pk_diasvida, \
RPG.ComplexEntityNo, \
RPG.FechaNacimiento, \
RPG.FechaCap, \
RPG.Edad, \
RPG.IngresoH AS AlojH, \
RPG.IngresoM AS AlojM, \
RPG.MortH, \
RPG.MortM, \
RPG.MortAcumH, \
RPG.MortAcumM, \
RPG.PorcMortH, \
RPG.PorcMortM, \
RPG.PorcMortAcumH, \
RPG.PorcMortAcumM, \
RPG.SacaH AS BenefH, \
RPG.SacaM AS BenefM, \
RPG.SacaAcumH AS BenefAcumH, \
RPG.SacaAcumM AS BenefAcumM, \
RPG.TransIngH, \
RPG.TransSalH, \
RPG.TransIngM, \
RPG.TransSalM, \
RPG.TransIngAcumHGalpon, \
RPG.TransSalAcumHGalpon, \
RPG.TransIngAcumMGalpon, \
RPG.TransSalAcumMGalpon, \
nvl(RPG.SaldoH, 0.000000) AS SaldoH, \
nvl(RPG.SaldoM, 0.000000) AS SaldoM, \
ROUND((nvl(((RPG.SaldoM) / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000)), 2) AS PorcMH, \
RPG.StdMortH, \
RPG.StdMortM, \
RPG.StdMortAcmH, \
RPG.StdMortAcmM, \
RPG.SelecH, \
RPG.SelecM, \
RPG.GADH, \
RPG.StdGADiaH, \
RPG.IncrGHAH, \
RPG.IncrStdGHAH, \
RPG.GADM, \
RPG.StdGADiaM, \
RPG.ProdHT, \
RPG.ProdHTAcum, \
RPG.PorcProdHT, \
RPG.StdPorcProdHT, \
RPG.ProdHI, \
RPG.ProdHIAcum, \
RPG.PorcProdHI, \
RPG.StdPorcProdHI, \
RPG.HTGE, \
RPG.HIGE, \
RPG.StdHTGEAcum, \
RPG.StdHIGEAcum, \
RPG.PesoHvo, \
RPG.PesoH, \
RPG.StdPesoH, \
RPG.PesoM, \
RPG.StdPesoM, \
RPG.UnifH, \
RPG.UnifM, \
RPG.PorcHvoPiso, \
RPG.PorcViabProdH, \
RPG.PorcViabProdM, \
(100 - RPG.StdMortAcmH) AS StdViabH, \
0 AS StdViabM, \
RPG.CapH, \
RPG.CapM, \
RPG.ConsAlimH, \
RPG.ConsAlimM, \
RPG.ProdPoroso, \
RPG.ProdSucio, \
RPG.ProdSangre, \
RPG.ProdDeforme, \
RPG.ProdCascaraDebil, \
RPG.ProdRoto, \
RPG.ProdFarfara, \
RPG.ProdInvendible, \
RPG.ProdChico, \
RPG.ProdDobleYema, \
RPG.U_PEErrorSexoH, \
RPG.U_PEErrorSexoM, \
RPG.AlimInicio, \
RPG.AlimCrecimiento, \
RPG.AlimPreReprod, \
RPG.AlimReprodI, \
RPG.AlimReprodII, \
RPG.AlimReprodMacho, \
RPG.PosturaCrecIRP, \
RPG.PosturaPreInicioRP, \
RPG.StdPesoHvo, \
RPG.PreInicialPavas, \
RPG.InicialPavosRP, \
RPG.CreIPavas, \
RPG.CrecIIPavas, \
RPG.MantPavas \
FROM \
CalculosAdicionales RPG")
print('carga df_ft_Reprod_Galpon_Diario1')
#aqui se encontro error de decimales y por ese capo se filtra
#from awsglue.context import GlueContext
#from pyspark.context import SparkContext
#from pyspark.sql.functions import col, year, month, count, lit
#from pyspark.sql.types import StringType
#
#
## Asumiendo que ya tienes el DataFrame cargado como ft_Reprod_Galpon_Diario_x1
## Si viene del catálogo de Glue:
## ft_Reprod_Galpon_Diario_x1 = glueContext.create_dynamic_frame.from_catalog(...).toDF()
#
## Convertir idfecha a fecha (paso a paso como en tu SQL)
#result_df = df_ft_Reprod_Galpon_Diario2.withColumn(
#    "fecha_formateada", 
#    col("fecha").cast(StringType()).substr(1, 8)  # equivalente a cast as varchar(8)
#).withColumn(
#    "fecha_date", 
#    col("fecha").cast("date")  # cast a date
#).groupBy(
#    year("fecha").alias("anio"),
#    month("fecha").alias("mes")
#).agg(
#    count(lit(1)).alias("conteo")
#).orderBy(
#    "anio", "mes"
#)
#
## Mostrar resultados
#result_df.show()
#
## Opcional: Guardar resultados
## glueContext.write_dynamic_frame.from_catalog(
##     frame=DynamicFrame.fromDF(result_df, glueContext, "resultado"),
##     database="tu_base_datos",
##     table_name="resultados_agrupados"
## )
df_ft_Reprod_Galpon_Diario2 = spark.sql(f"WITH Reprod_Galpon_Diario_Levante_Filtrado AS ( \
SELECT \
 RPG.*, \
ROUND(nvl((RPG.MortH / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcMortH, \
ROUND(nvl((RPG.MortM / NULLIF((RPG.SaldoM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcMortM, \
CASE \
WHEN RPG.pk_etapa = 1 THEN ROUND(nvl((RPG.MortAcumH / NULLIF((RPG.IngresoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
ELSE ROUND(nvl((RPG.MortAcumH / NULLIF((RPG.CapH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
END AS PorcMortAcumH, \
CASE \
WHEN RPG.pk_etapa = 1 THEN ROUND(nvl((RPG.MortAcumM / NULLIF((RPG.IngresoM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
ELSE ROUND(nvl((RPG.MortAcumM / NULLIF((RPG.CapM) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
END AS PorcMortAcumM, \
ROUND(nvl((RPG.ConsAlimH / NULLIF(RPG.SaldoH, 0.000000)) * 1000.000000, 0.000000), 2) AS GADH, \
ROUND(nvl((RPG.ConsAlimM / NULLIF(RPG.SaldoM, 0.000000)) * 1000.000000, 0.000000), 2) AS GADM, \
ROUND(nvl((RPG.ProdHT / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcProdHT, \
ROUND(nvl((RPG.ProdHI / NULLIF((RPG.ProdHT) * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) AS PorcProdHI, \
ROUND(nvl(RPG.ProdHTAcum / NULLIF(RPG.CapH * 1.000000, 0.000000), 0.000000), 2) AS HTGE, \
ROUND(nvl(RPG.ProdHIAcum / NULLIF(RPG.CapH * 1.000000, 0.000000), 0.000000), 2) AS HIGE, \
CASE \
WHEN RPG.pk_etapa = 1 THEN ROUND(nvl(((RPG.SaldoH) / NULLIF(RPG.IngresoH * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
ELSE ROUND(nvl(((RPG.SaldoH) / NULLIF(RPG.CapH * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
END AS PorcViabProdH, \
CASE \
WHEN RPG.pk_etapa = 1 THEN ROUND(nvl(((RPG.SaldoM) / NULLIF(RPG.IngresoM * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
ELSE ROUND(nvl(((RPG.SaldoM) / NULLIF(RPG.CapM * 1.000000, 0.000000)) * 100.0000000, 0.000000), 2) \
END AS PorcViabProdM \
FROM \
{database_name}.Reprod_Galpon_Diario_Levante RPG \
WHERE \
(date_format(RPG.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')) \
AND CondicionH < 5.00000 \
), \
CalculosAdicionales AS ( \
SELECT \
*, \
nvl((GADH - LAG(GADH, 1) OVER (PARTITION BY Complexentityno ORDER BY pk_tiempo)), 0.000000) AS IncrGHAH, \
nvl((StdGADiaH - MAX(StdGADiaH) OVER (PARTITION BY Complexentityno, pk_semanavida ORDER BY pk_tiempo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), 0.000000) AS IncrStdGHAH \
FROM \
Reprod_Galpon_Diario_Levante_Filtrado \
) \
SELECT \
RPG.pk_tiempo, \
RPG.fecha, \
RPG.pk_empresa, \
RPG.pk_division, \
RPG.pk_zona, \
RPG.pk_subzona, \
RPG.pk_plantel, \
RPG.pk_lote, \
RPG.pk_galpon, \
RPG.pk_etapa, \
RPG.pk_standard, \
RPG.pk_tipoproducto, \
RPG.pk_especieg, \
RPG.pk_estado, \
RPG.pk_administrador, \
RPG.pk_proveedor, \
RPG.pk_generacion, \
RPG.pk_semanavida, \
RPG.pk_diasvida, \
RPG.ComplexEntityNo, \
RPG.FechaNacimiento, \
RPG.FechaCap, \
RPG.Edad, \
RPG.IngresoH AS AlojH, \
RPG.IngresoM AS AlojM, \
RPG.MortH, \
RPG.MortM, \
RPG.MortAcumH, \
RPG.MortAcumM, \
RPG.PorcMortH, \
RPG.PorcMortM, \
RPG.PorcMortAcumH, \
RPG.PorcMortAcumM, \
RPG.SacaH AS BenefH, \
RPG.SacaM AS BenefM, \
RPG.SacaAcumH AS BenefAcumH, \
RPG.SacaAcumM AS BenefAcumM, \
RPG.TransIngH, \
RPG.TransSalH, \
RPG.TransIngM, \
RPG.TransSalM, \
RPG.TransIngAcumHGalpon, \
RPG.TransSalAcumHGalpon, \
RPG.TransIngAcumMGalpon, \
RPG.TransSalAcumMGalpon, \
nvl(RPG.SaldoH, 0.000000) AS SaldoH, \
nvl(RPG.SaldoM, 0.000000) AS SaldoM, \
ROUND((nvl(((RPG.SaldoM) / NULLIF((RPG.SaldoH) * 1.000000, 0.000000)) * 100.0000000, 0.000000)), 2) AS PorcMH, \
RPG.StdMortH, \
RPG.StdMortM, \
RPG.StdMortAcmH, \
RPG.StdMortAcmM, \
RPG.SelecH, \
RPG.SelecM, \
RPG.GADH, \
RPG.StdGADiaH, \
RPG.IncrGHAH, \
RPG.IncrStdGHAH, \
RPG.GADM, \
RPG.StdGADiaM, \
RPG.ProdHT, \
RPG.ProdHTAcum, \
RPG.PorcProdHT, \
RPG.StdPorcProdHT, \
RPG.ProdHI, \
RPG.ProdHIAcum, \
RPG.PorcProdHI, \
RPG.StdPorcProdHI, \
RPG.HTGE, \
RPG.HIGE, \
RPG.StdHTGEAcum, \
RPG.StdHIGEAcum, \
RPG.PesoHvo, \
RPG.PesoH, \
RPG.StdPesoH, \
RPG.PesoM, \
RPG.StdPesoM, \
RPG.UnifH, \
RPG.UnifM, \
RPG.PorcHvoPiso, \
RPG.PorcViabProdH, \
RPG.PorcViabProdM, \
(100 - RPG.StdMortAcmH) AS StdViabH, \
0 AS StdViabM, \
RPG.CapH, \
RPG.CapM, \
RPG.ConsAlimH, \
RPG.ConsAlimM, \
RPG.ProdPoroso, \
RPG.ProdSucio, \
RPG.ProdSangre, \
RPG.ProdDeforme, \
RPG.ProdCascaraDebil, \
RPG.ProdRoto, \
RPG.ProdFarfara, \
RPG.ProdInvendible, \
RPG.ProdChico, \
RPG.ProdDobleYema, \
RPG.U_PEErrorSexoH, \
RPG.U_PEErrorSexoM, \
RPG.AlimInicio, \
RPG.AlimCrecimiento, \
RPG.AlimPreReprod, \
RPG.AlimReprodI, \
RPG.AlimReprodII, \
RPG.AlimReprodMacho, \
RPG.PosturaCrecIRP, \
RPG.PosturaPreInicioRP, \
RPG.StdPesoHvo, \
RPG.PreInicialPavas, \
RPG.InicialPavosRP, \
RPG.CreIPavas, \
RPG.CrecIIPavas, \
RPG.MantPavas \
FROM \
CalculosAdicionales RPG")

df_ft_Reprod_Galpon_Diario =df_ft_Reprod_Galpon_Diario1.union(df_ft_Reprod_Galpon_Diario2)
#date_format(fecha,'yyyyMM') >= date_format(((current_date() - INTERVAL 5 HOURS) - INTERVAL 4 MONTH), 'yyyyMM')
print('carga df_ft_Reprod_Galpon_Diario2')
# Verificar si la tabla gold ya existe - ft_Reprod_Galpon_Diario
#gold_table = spark.read.format("parquet").load(path_target9)  
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes9 = spark.read.format("parquet").load(path_target9)
    datos_existentes9 = True
    logger.info(f"Datos existentes de ft_Reprod_Galpon_Diario cargados: {df_existentes9.count()} registros")
except:
    datos_existentes9 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Galpon_Diario")


if datos_existentes9:
    existing_data9 = spark.read.format("parquet").load(path_target9)
    data_after_delete9 = existing_data9.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str) ))
    filtered_new_data9 = df_ft_Reprod_Galpon_Diario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data9 = filtered_new_data9.union(data_after_delete9)                             
   
    cant_ingresonuevo9 = filtered_new_data9.count()
    cant_total9 = final_data9.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_DiarioTemporal"
    }
    final_data9.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data9_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Diario"
    }
    final_data9_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Galpon_Diario : {cant_ingresonuevo9}")
    print(f"Total de registros en la tabla ft_Reprod_Galpon_Diario : {cant_total9}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Galpon_DiarioTemporal')
    print(f"Tabla ft_Reprod_Galpon_DiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Diario"
    }
    df_ft_Reprod_Galpon_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Diario")
#Se inserta en tabla temporal los registros agrupados por galpon y edad en la etapa de cria
df_Reprod_Galpon_Edad_CriaTemp = spark.sql(f"select \
                                            max(A.pk_tiempo) pk_tiempo, \
                                            max(A.fecha) fecha, \
                                            A.pk_empresa, \
                                            A.pk_division, \
                                            A.pk_zona, \
                                            A.pk_subzona, \
                                            A.pk_plantel, \
                                            A.pk_lote, \
                                            A.pk_galpon, \
                                            max(pk_etapa) pk_etapa, \
                                            min(pk_standard) pk_standard, \
                                            pk_tipoproducto, \
                                            max(A.pk_especie) pk_especie, \
                                            max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especieg, \
                                            min(pk_estado) pk_estado, \
                                            A.pk_administrador, \
                                            A.pk_proveedor, \
                                            pk_generacion, \
                                            pk_semanavida, \
                                            substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNo, \
                                            max(semanaanioReprod) as semanaReprod, \
                                            min(A.fecha) FechaIni, \
                                            max(A.fecha) FechaFin, \
                                            min(A.fechanacimiento) fechanacimiento, \
                                            max(Edad) Edad, \
                                            max(nvl(IngresoH,0)) IngresoH, \
                                            max(nvl(IngresoM,0)) IngresoM, \
                                            sum(nvl(SacaH,0)) SacaH, \
                                            sum(nvl(SacaM,0)) SacaM, \
                                            max(nvl(SacaAcumHGalpon,0)) SacaAcumH, \
                                            max(nvl(SacaAcumMGalpon,0)) SacaAcumM, \
                                            sum(nvl(MortH,0)) MortH, \
                                            sum(nvl(MortM,0)) MortM, \
                                            max(nvl(MortAcumHGalpon,0)) MortAcumH, \
                                            max(nvl(MortAcumMGalpon,0)) MortAcumM, \
                                            sum(nvl(TransIngH,0)) TransIngH, \
                                            sum(nvl(TransSalH,0)) TransSalH, \
                                            sum(nvl(TransIngM,0)) TransIngM, \
                                            sum(nvl(TransSalM,0)) TransSalM, \
                                            max(nvl(TransIngAcumHGalpon,0)) TransIngAcumHGalpon, \
                                            max(nvl(TransSalAcumHGalpon,0)) TransSalAcumHGalpon, \
                                            max(nvl(TransIngAcumMGalpon,0)) TransIngAcumMGalpon, \
                                            max(nvl(TransSalAcumMGalpon,0)) TransSalAcumMGalpon, \
                                            CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoH,0)) - max(nvl(SacaAcumHGalpon,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) \
                                                 ELSE max(nvl(CapH,0)) - max(nvl(SacaAcumHGalpon,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) END SaldoH, \
                                            CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) \
                                                 ELSE max(nvl(CapM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) END SaldoM, \
                                            sum(nvl(ConsAlimH,0)) ConsAlimH, \
                                            sum(nvl(ConsAlimM,0)) ConsAlimM, \
                                            max(nvl(StdGADiaH,0)) StdGADiaH, \
                                            max(nvl(StdGADiaM,0)) StdGADiaM, \
                                            max(nvl(StdPorcProdHT,0)) StdPorcProdHT, \
                                            max(nvl(StdPorcProdHI,0)) StdPorcProdHI, \
                                            max(nvl(StdHTGEAcum,0)) StdHTGEAcum, \
                                            max(nvl(StdHIGEAcum,0)) StdHIGEAcum, \
                                            max(nvl(StdPesoH,0)) StdPesoH, \
                                            max(nvl(StdPesoM,0)) StdPesoM, \
                                            max(nvl(StdPesoHvo,0)) StdPesoHvo, \
                                            max(nvl(StdMortH,0)) StdMortH, \
                                            max(nvl(StdMortM,0)) StdMortM, \
                                            max(nvl(StdMortAcmH,0)) StdMortAcmH, \
                                            max(nvl(StdMortAcmM,0)) StdMortAcmM, \
                                            sum(nvl(ProdHT,0)) ProdHT, \
                                            sum(nvl(ProdHI,0)) ProdHI, \
                                            max(nvl(ProdHTAcumGalpon,0)) ProdHTAcum, \
                                            max(nvl(ProdHIAcumGalpon,0)) ProdHIAcum, \
                                            max(nvl(PesoHvo,0)) PesoHvo, \
                                            max(nvl(PesoH,0)) PesoH, \
                                            max(nvl(PesoM,0)) PesoM, \
                                            max(nvl(UnifH,0)) UnifH, \
                                            max(nvl(UnifM,0)) UnifM, \
                                            sum(nvl(ProdPoroso,0)) ProdPoroso, \
                                            sum(nvl(ProdSucio,0)) ProdSucio, \
                                            sum(nvl(ProdSangre,0)) ProdSangre, \
                                            sum(nvl(ProdDeforme,0)) ProdDeforme, \
                                            sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
                                            sum(nvl(ProdRoto,0)) ProdRoto, \
                                            sum(nvl(ProdFarfara,0)) ProdFarfara, \
                                            sum(nvl(ProdInvendible,0)) ProdInvendible, \
                                            sum(nvl(ProdChico,0)) ProdChico, \
                                            sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
                                            sum(nvl(SelecH,0)) SelecH, \
                                            sum(nvl(SelecM,0)) SelecM, \
                                            min(FechaCap) FechaCap, \
                                            max(nvl(CapH,0)) CapH, \
                                            max(nvl(CapM,0)) CapM, \
                                            ROUND((sum(nvl(PorcHvoPiso,0))/7),2) PorcHvoPiso, \
                                            sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
                                            sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
                                            SUM(nvl(AlimInicio,0)) AlimInicio, \
                                            SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
                                            SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
                                            SUM(nvl(AlimReprodI,0)) AlimReprodI, \
                                            SUM(nvl(AlimReprodII,0)) AlimReprodII, \
                                            SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
                                            SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
                                            SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
                                            SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
                                            SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
                                            SUM(nvl(CreIPavas,0)) CreIPavas, \
                                            SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
                                            SUM(nvl(MantPavas,0)) MantPavas, \
                                            (max(nvl(SacaAcumHGalpon*1.000000,0.000000))/NULLIF(max(nvl(CapH,0.000000)),0.000000))*100.0000000 CondicionH \
                                            from {database_name}.Reprod_Diario A \
                                            LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on substring(A.complexentityno,1,(length(A.complexentityno)-3)) = FIR.ComplexEntityNo \
                                            left join {database_name}.lk_tiempo t on t.pk_tiempo = A.pk_tiempo \
                                            where pk_etapa = 1 \
                                            group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon, \
                                            pk_tipoproducto,A.pk_administrador,A.pk_proveedor,pk_generacion,pk_semanavida, \
                                            substring(A.complexentityno,1,(length(A.complexentityno)-3)) \
                                            order by pk_tiempo,pk_semanavida")
#df_Reprod_Galpon_Edad_CriaTemp.createOrReplaceTempView("Reprod_Galpon_Edad_Cria")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Galpon_Edad_Cria"
}
df_Reprod_Galpon_Edad_CriaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Galpon_Edad_Cria")
print('carga Reprod_Galpon_Edad_Cria')
#Se inserta en tabla temporal los registros agrupados por galpon y edad en la etapa de Levante
df_Reprod_Galpon_Edad_LevanteTemp = spark.sql(f"select\
                                              max(A.pk_tiempo) pk_tiempo,\
                                              max(A.fecha) fecha, \
                                              A.pk_empresa,\
                                              A.pk_division,\
                                              A.pk_zona,\
                                              A.pk_subzona,\
                                              A.pk_plantel,\
                                              A.pk_lote,\
                                              A.pk_galpon,\
                                              max(pk_etapa) pk_etapa,\
                                              min(pk_standard) pk_standard,\
                                              pk_tipoproducto,\
                                              max(A.pk_especie) pk_especie,\
                                              max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especieg,\
                                              min(pk_estado) pk_estado,\
                                              A.pk_administrador,\
                                              A.pk_proveedor,\
                                              pk_generacion,\
                                              pk_semanavida,\
                                              substring(A.complexentityno,1,(length(A.complexentityno)-3)) ComplexEntityNo, \
                                              max(semanaanioReprod) as semanaReprod, \
                                              min(A.fecha) FechaIni, \
                                              max(A.fecha) FechaFin, \
                                              min(fechanacimiento) fechanacimiento,\
                                              max(Edad) Edad, \
                                              max(nvl(IngresoH,0)) IngresoH,\
                                              max(nvl(IngresoM,0)) IngresoM,\
                                              sum(nvl(SacaH,0)) SacaH,\
                                              sum(nvl(SacaM,0)) SacaM,\
                                              max(nvl(SacaAcumHGalpon,0)) SacaAcumH,\
                                              max(nvl(SacaAcumMGalpon,0)) SacaAcumM,\
                                              sum(nvl(MortH,0)) MortH,\
                                              sum(nvl(MortM,0)) MortM,\
                                              max(nvl(MortAcumHGalpon,0)) MortAcumH,\
                                              max(nvl(MortAcumMGalpon,0)) MortAcumM,\
                                              sum(nvl(ConsAlimH,0)) ConsAlimH,\
                                              sum(nvl(ConsAlimM,0)) ConsAlimM,\
                                              sum(nvl(TransIngH,0)) TransIngH,\
                                              sum(nvl(TransSalH,0)) TransSalH,\
                                              sum(nvl(TransIngM,0)) TransIngM,\
                                              sum(nvl(TransSalM,0)) TransSalM,\
                                              max(nvl(TransIngAcumHGalpon,0)) TransIngAcumHGalpon,\
                                              max(nvl(TransSalAcumHGalpon,0)) TransSalAcumHGalpon,\
                                              max(nvl(TransIngAcumMGalpon,0)) TransIngAcumMGalpon,\
                                              max(nvl(TransSalAcumMGalpon,0)) TransSalAcumMGalpon,\
                                              CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoH,0)) - max(nvl(SacaAcumHGalpon,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0))\
                                                      ELSE max(nvl(CapH,0)) - max(nvl(SacaAcumHGalpon,0)) - max(nvl(MortAcumHGalpon,0)) - max(nvl(TransSalAcumHGalpon,0)) + max(nvl(TransIngAcumHGalpon,0)) END SaldoH,\
                                              CASE WHEN max(pk_etapa) = 1 THEN max(nvl(IngresoM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0))\
                                                      ELSE max(nvl(CapM,0)) - max(nvl(SacaAcumMGalpon,0)) - max(nvl(MortAcumMGalpon,0)) - max(nvl(TransSalAcumMGalpon,0)) + max(nvl(TransIngAcumMGalpon,0)) END SaldoM,\
                                              max(nvl(StdGADiaH,0)) StdGADiaH,\
                                              max(nvl(StdGADiaM,0)) StdGADiaM,\
                                              max(nvl(StdPorcProdHT,0)) StdPorcProdHT,\
                                              max(nvl(StdPorcProdHI,0)) StdPorcProdHI,\
                                              max(nvl(StdHTGEAcum,0)) StdHTGEAcum,\
                                              max(nvl(StdHIGEAcum,0)) StdHIGEAcum,\
                                              max(nvl(StdPesoH,0)) StdPesoH,\
                                              max(nvl(StdPesoM,0)) StdPesoM,\
                                              max(nvl(StdPesoHvo,0)) StdPesoHvo,\
                                              max(nvl(StdMortH,0)) StdMortH,\
                                              max(nvl(StdMortM,0)) StdMortM,\
                                              max(nvl(StdMortAcmH,0)) StdMortAcmH,\
                                              max(nvl(StdMortAcmM,0)) StdMortAcmM,\
                                              sum(nvl(ProdHT,0)) ProdHT,\
                                              sum(nvl(ProdHI,0)) ProdHI,\
                                              max(nvl(ProdHTAcumGalpon,0)) ProdHTAcum,\
                                              max(nvl(ProdHIAcumGalpon,0)) ProdHIAcum,\
                                              max(nvl(PesoHvo,0)) PesoHvo,\
                                              max(nvl(PesoH,0)) PesoH,\
                                              max(nvl(PesoM,0)) PesoM,\
                                              max(nvl(UnifH,0)) UnifH,\
                                              max(nvl(UnifM,0)) UnifM,\
                                              sum(nvl(ProdPoroso,0)) ProdPoroso,\
                                              sum(nvl(ProdSucio,0)) ProdSucio,\
                                              sum(nvl(ProdSangre,0)) ProdSangre,\
                                              sum(nvl(ProdDeforme,0)) ProdDeforme,\
                                              sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil,\
                                              sum(nvl(ProdRoto,0)) ProdRoto,\
                                              sum(nvl(ProdFarfara,0)) ProdFarfara,\
                                              sum(nvl(ProdInvendible,0)) ProdInvendible,\
                                              sum(nvl(ProdChico,0)) ProdChico,\
                                              sum(nvl(ProdDobleYema,0)) ProdDobleYema,\
                                              sum(nvl(SelecH,0)) SelecH,\
                                              sum(nvl(SelecM,0)) SelecM,\
                                              min(FechaCap) FechaCap,\
                                              max(nvl(CapH,0)) CapH,\
                                              max(nvl(CapM,0)) CapM,\
                                              ROUND((sum(nvl(PorcHvoPiso,0))/7),2) PorcHvoPiso,\
                                              sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH,\
                                              sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM,\
                                              SUM(nvl(AlimInicio,0)) AlimInicio,\
                                              SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento,\
                                              SUM(nvl(AlimPreReprod,0)) AlimPreReprod,\
                                              SUM(nvl(AlimReprodI,0)) AlimReprodI,\
                                              SUM(nvl(AlimReprodII,0)) AlimReprodII,\
                                              SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho,\
                                              SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP,\
                                              SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP,\
                                              SUM(nvl(PreInicialPavas,0)) PreInicialPavas,\
                                              SUM(nvl(InicialPavosRP,0)) InicialPavosRP,\
                                              SUM(nvl(CreIPavas,0)) CreIPavas,\
                                              SUM(nvl(CrecIIPavas,0)) CrecIIPavas,\
                                              SUM(nvl(MantPavas,0)) MantPavas,\
                                              (max(nvl(SacaAcumHGalpon*1.000000,0.000000))/NULLIF(max(nvl(CapH,0.000000)),0.000000))*100.0000000 CondicionH\
                                              from {database_name}.Reprod_Diario A \
                                              LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on substring(A.complexentityno,1,(length(A.complexentityno)-3)) = FIR.ComplexEntityNo \
                                              left join {database_name}.lk_tiempo t on t.pk_tiempo = A.pk_tiempo \
                                              where pk_etapa = 3 \
                                              group by A.pk_empresa, A.pk_division, A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,pk_tipoproducto,\
                                                      A.pk_administrador,A.pk_proveedor,pk_generacion,pk_semanavida,substring(A.complexentityno,1,(length(A.complexentityno)-3))\
                                              order by pk_tiempo,pk_semanavida")

#df_Reprod_Galpon_Edad_LevanteTemp.createOrReplaceTempView("Reprod_Galpon_Edad_Levante")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Galpon_Edad_Levante"
}
df_Reprod_Galpon_Edad_LevanteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Galpon_Edad_Levante")
print('carga Reprod_Galpon_Edad_Levante')
#Inserta los últimos 4 meses de la tabla
#insert into DMPECUARIO.ft_Reprod_Galpon_Semana 
df_ft_Reprod_Galpon_Semana1 = spark.sql(f"select \
                                         RPG.pk_tiempo \
                                        ,RPG.fecha \
                                        ,RPG.pk_empresa \
                                        ,RPG.pk_division \
                                        ,RPG.pk_zona \
                                        ,RPG.pk_subzona \
                                        ,RPG.pk_plantel \
                                        ,RPG.pk_lote \
                                        ,RPG.pk_galpon \
                                        ,RPG.pk_etapa \
                                        ,RPG.pk_standard \
                                        ,RPG.pk_tipoproducto \
                                        ,RPG.pk_especieg \
                                        ,RPG.pk_estado \
                                        ,RPG.pk_administrador \
                                        ,RPG.pk_proveedor \
                                        ,RPG.pk_generacion \
                                        ,RPG.pk_semanavida \
                                        ,RPG.ComplexEntityNo \
                                        ,RPG.FechaNacimiento \
                                        ,RPG.Edad \
                                        ,RPG.FechaCap \
                                        ,RPG.FechaIni \
                                        ,RPG.FechaFin \
                                        ,RPG.IngresoH AlojH \
                                        ,RPG.IngresoM AlojM \
                                        ,RPG.MortH \
                                        ,RPG.MortM \
                                        ,RPG.MortAcumH \
                                        ,RPG.MortAcumM \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl((RPG.MortH/NULLIF((RPG.SaldoH)*1.0,0))*100,0)),2) END PorcMortH \
                                        ,CASE WHEN RPG.SacaAcumM > 500 THEN 0 ELSE ROUND((nvl((RPG.MortM/NULLIF((RPG.SaldoM)*1.0,0))*100,0)),2) END PorcMortM \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.IngresoH)*1.0,0))*100,0)),2) \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.CapH)*1.0,0))*100,0)),2) END PorcMortAcumH \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.IngresoM)*1.0,0))*100,0)),2) \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.CapM)*1.0,0))*100,0)),2) END PorcMortAcumM \
                                        ,RPG.SacaH BenefH \
                                        ,RPG.SacaM BenefM \
                                        ,RPG.SacaAcumH BenefAcumH \
                                        ,RPG.SacaAcumM BenefAcumM \
                                        ,RPG.TransIngH \
                                        ,RPG.TransSalH \
                                        ,RPG.TransIngM \
                                        ,RPG.TransSalM \
                                        ,RPG.TransIngAcumHGalpon \
                                        ,RPG.TransSalAcumHGalpon \
                                        ,RPG.TransIngAcumMGalpon \
                                        ,RPG.TransSalAcumMGalpon \
                                        ,nvl(RPG.SaldoH,0) SaldoH \
                                        ,nvl(RPG.SaldoM,0) SaldoM \
                                        ,ROUND((nvl(((RPG.SaldoM)/(NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcMH \
                                        ,RPG.StdMortH \
                                        ,RPG.StdMortM \
                                        ,RPG.StdMortAcmH \
                                        ,RPG.StdMortAcmM \
                                        ,RPG.SelecH \
                                        ,RPG.SelecM \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND(nvl(((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000,0),2) END GADH \
                                        ,RPG.StdGADiaH \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND(nvl((ROUND((((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000),3) - ROUND((((A.ConsAlimH/NULLIF(A.SaldoH,0))/7)*1000),3) ),0),2) END IncrGADH \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE nvl((RPG.StdGADiaH - A.StdGADiaH ),0) END IncrStdGADH \
                                        ,CASE WHEN RPG.SacaAcumM > 500 THEN 0 ELSE ROUND(nvl(((RPG.ConsAlimM/NULLIF(RPG.SaldoM,0))/7)*1000,0),2) END GADM \
                                        ,RPG.StdGADiaM \
                                        ,RPG.ProdHT \
                                        ,RPG.ProdHTAcum \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(((RPG.ProdHT/NULLIF((RPG.SaldoH)*1.0,0))/7)*100,0)),2) END PorcProdHT \
                                        ,RPG.StdPorcProdHT \
                                        ,RPG.ProdHI \
                                        ,RPG.ProdHIAcum \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl((RPG.ProdHI/NULLIF((RPG.ProdHT*1.0),0))*100,0)),2) END PorcProdHI \
                                        ,RPG.StdPorcProdHI \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(RPG.ProdHTAcum/NULLIF(RPG.CapH*1.0,0),0)),2) END HTGE \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(RPG.ProdHIAcum/NULLIF(RPG.CapH*1.0,0),0)),2) END HIGE \
                                        ,RPG.StdHTGEAcum \
                                        ,RPG.StdHIGEAcum \
                                        ,RPG.PesoHvo \
                                        ,RPG.PesoH \
                                        ,RPG.StdPesoH \
                                        ,RPG.PesoM \
                                        ,RPG.StdPesoM \
                                        ,RPG.UnifH \
                                        ,RPG.UnifM \
                                        ,RPG.PorcHvoPiso \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.IngresoH*1.0,0))*100,0)),2) \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.CapH*1.0,0))*100,0)),2) END PorcViabProdH \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.IngresoM*1.0,0))*100,0)),2) \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM > 500 THEN 0 \
                                              WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.CapM*1.0,0))*100,0)),2) END PorcViabProdM \
                                        ,(100 - RPG.StdMortAcmH) StdViabH \
                                        ,0 StdViabM \
                                        ,RPG.CapH \
                                        ,RPG.CapM \
                                        ,RPG.ConsAlimH \
                                        ,RPG.ConsAlimM \
                                        ,RPG.ProdPoroso \
                                        ,RPG.ProdSucio \
                                        ,RPG.ProdSangre \
                                        ,RPG.ProdDeforme \
                                        ,RPG.ProdCascaraDebil \
                                        ,RPG.ProdRoto \
                                        ,RPG.ProdFarfara \
                                        ,RPG.ProdInvendible \
                                        ,RPG.ProdChico \
                                        ,RPG.ProdDobleYema \
                                        ,RPG.U_PEErrorSexoH \
                                        ,RPG.U_PEErrorSexoM \
                                        ,RPG.AlimInicio \
                                        ,RPG.AlimCrecimiento \
                                        ,RPG.AlimPreReprod \
                                        ,RPG.AlimReprodI \
                                        ,RPG.AlimReprodII \
                                        ,RPG.AlimReprodMacho \
                                        ,RPG.PosturaCrecIRP \
                                        ,RPG.PosturaPreInicioRP \
                                        ,RPG.pk_semanavida pk_semanavida1 \
                                        ,RPG.pk_semanavida pk_semanavida2\
                                        ,RPG.STDPesoHvo \
                                        ,RPG. PreInicialPavas \
                                        ,RPG.InicialPavosRP \
                                        ,RPG.CreIPavas \
                                        ,RPG.CrecIIPavas \
                                        ,RPG.MantPavas \
                                        ,(RPG.SaldoH * RPG.StdGADiaH * 7)/1000 StdAlimH \
                                        ,(RPG.SaldoM * RPG.StdGADiaM * 7)/1000 StdAlimM \
                                        ,nvl((RPG.ConsAlimH + RPG.ConsAlimM) / nullif(RPG.ProdHI,0),0) AlimProdHI \
                                        ,ROW_NUMBER()  OVER(PARTITION BY RPG.ComplexEntityNo ORDER BY RPG.PK_semanavida ASC) RowNumber \
                                        from {database_name}.Reprod_Galpon_Edad_Cria RPG \
                                        LEFT JOIN {database_name}.Reprod_Galpon_Edad_Cria A on  A.Complexentityno = RPG.Complexentityno  AND A.pk_semanavida = RPG.pk_semanavida -1")

print('carga df_ft_Reprod_Galpon_Semana1')
df_ft_Reprod_Galpon_Semana2 = spark.sql(f"select \
                                         RPG.pk_tiempo \
                                        ,RPG.fecha \
                                        ,RPG.pk_empresa \
                                        ,RPG.pk_division \
                                        ,RPG.pk_zona \
                                        ,RPG.pk_subzona \
                                        ,RPG.pk_plantel \
                                        ,RPG.pk_lote \
                                        ,RPG.pk_galpon \
                                        ,RPG.pk_etapa \
                                        ,RPG.pk_standard \
                                        ,RPG.pk_tipoproducto \
                                        ,RPG.pk_especieg \
                                        ,RPG.pk_estado \
                                        ,RPG.pk_administrador \
                                        ,RPG.pk_proveedor \
                                        ,RPG.pk_generacion \
                                        ,RPG.pk_semanavida \
                                        ,RPG.ComplexEntityNo \
                                        ,RPG.FechaNacimiento \
                                        ,RPG.Edad \
                                        ,RPG.FechaCap \
                                        ,RPG.FechaIni \
                                        ,RPG.FechaFin \
                                        ,RPG.IngresoH AlojH \
                                        ,RPG.IngresoM AlojM \
                                        ,RPG.MortH \
                                        ,RPG.MortM \
                                        ,RPG.MortAcumH \
                                        ,RPG.MortAcumM \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl((RPG.MortH/NULLIF((RPG.SaldoH)*1.0,0))*100,0)),2) END PorcMortH \
                                        ,CASE WHEN RPG.SacaAcumM > 500 THEN 0 ELSE ROUND((nvl((RPG.MortM/NULLIF((RPG.SaldoM)*1.0,0))*100,0)),2) END PorcMortM \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.IngresoH)*1.0,0))*100,0)),2) \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.CapH)*1.0,0))*100,0)),2) END PorcMortAcumH \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.IngresoM)*1.0,0))*100,0)),2) \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.CapM)*1.0,0))*100,0)),2) END PorcMortAcumM \
                                        ,RPG.SacaH BenefH \
                                        ,RPG.SacaM BenefM \
                                        ,RPG.SacaAcumH BenefAcumH \
                                        ,RPG.SacaAcumM BenefAcumM \
                                        ,RPG.TransIngH \
                                        ,RPG.TransSalH \
                                        ,RPG.TransIngM \
                                        ,RPG.TransSalM \
                                        ,RPG.TransIngAcumHGalpon \
                                        ,RPG.TransSalAcumHGalpon \
                                        ,RPG.TransIngAcumMGalpon \
                                        ,RPG.TransSalAcumMGalpon \
                                        ,nvl(RPG.SaldoH,0) SaldoH \
                                        ,nvl(RPG.SaldoM,0) SaldoM \
                                        ,ROUND((nvl(((RPG.SaldoM)/(NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcMH \
                                        ,RPG.StdMortH \
                                        ,RPG.StdMortM \
                                        ,RPG.StdMortAcmH \
                                        ,RPG.StdMortAcmM \
                                        ,RPG.SelecH \
                                        ,RPG.SelecM \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND(nvl(((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000,0),2) END GADH \
                                        ,RPG.StdGADiaH \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND(nvl((ROUND((((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000),3) - ROUND((((A.ConsAlimH/NULLIF(A.SaldoH,0))/7)*1000),3) ),0),2) END IncrGADH \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE nvl((RPG.StdGADiaH - A.StdGADiaH ),0) END IncrStdGADH \
                                        ,CASE WHEN RPG.SacaAcumM > 500 THEN 0 ELSE ROUND(nvl(((RPG.ConsAlimM/NULLIF(RPG.SaldoM,0))/7)*1000,0),2) END GADM \
                                        ,RPG.StdGADiaM \
                                        ,RPG.ProdHT \
                                        ,RPG.ProdHTAcum \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(((RPG.ProdHT/NULLIF((RPG.SaldoH)*1.0,0))/7)*100,0)),2) END PorcProdHT \
                                        ,RPG.StdPorcProdHT \
                                        ,RPG.ProdHI \
                                        ,RPG.ProdHIAcum \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl((RPG.ProdHI/NULLIF((RPG.ProdHT*1.0),0))*100,0)),2) END PorcProdHI \
                                        ,RPG.StdPorcProdHI \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(RPG.ProdHTAcum/NULLIF(RPG.CapH*1.0,0),0)),2) END HTGE \
                                        ,CASE WHEN RPG.SacaAcumH > 500 THEN 0 ELSE ROUND((nvl(RPG.ProdHIAcum/NULLIF(RPG.CapH*1.0,0),0)),2) END HIGE \
                                        ,RPG.StdHTGEAcum \
                                        ,RPG.StdHIGEAcum \
                                        ,RPG.PesoHvo \
                                        ,RPG.PesoH \
                                        ,RPG.StdPesoH \
                                        ,RPG.PesoM \
                                        ,RPG.StdPesoM \
                                        ,RPG.UnifH \
                                        ,RPG.UnifM \
                                        ,RPG.PorcHvoPiso \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.IngresoH*1.0,0))*100,0)),2) \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumH <= 500 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.CapH*1.0,0))*100,0)),2) END PorcViabProdH \
                                        ,CASE WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 1 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.IngresoM*1.0,0))*100,0)),2) \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM > 500 THEN 0 \
                                        WHEN RPG.pk_etapa = 3 AND RPG.SacaAcumM <= 500 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.CapM*1.0,0))*100,0)),2) END PorcViabProdM \
                                        ,(100 - RPG.StdMortAcmH) StdViabH \
                                        ,0 StdViabM  \
                                        ,RPG.CapH \
                                        ,RPG.CapM \
                                        ,RPG.ConsAlimH \
                                        ,RPG.ConsAlimM \
                                        ,RPG.ProdPoroso \
                                        ,RPG.ProdSucio \
                                        ,RPG.ProdSangre \
                                        ,RPG.ProdDeforme \
                                        ,RPG.ProdCascaraDebil \
                                        ,RPG.ProdRoto \
                                        ,RPG.ProdFarfara \
                                        ,RPG.ProdInvendible \
                                        ,RPG.ProdChico \
                                        ,RPG.ProdDobleYema \
                                        ,RPG.U_PEErrorSexoH \
                                        ,RPG.U_PEErrorSexoM \
                                        ,RPG.AlimInicio \
                                        ,RPG.AlimCrecimiento \
                                        ,RPG.AlimPreReprod \
                                        ,RPG.AlimReprodI \
                                        ,RPG.AlimReprodII \
                                        ,RPG.AlimReprodMacho \
                                        ,RPG.PosturaCrecIRP \
                                        ,RPG.PosturaPreInicioRP \
                                        ,RPG.PK_semanavida pk_semanavida1 \
                                        ,RPG.PK_semanavida pk_semanavida2 \
                                        ,RPG.STDPesoHvo \
                                        ,RPG. PreInicialPavas \
                                        ,RPG.InicialPavosRP \
                                        ,RPG.CreIPavas \
                                        ,RPG.CrecIIPavas \
                                        ,RPG.MantPavas \
                                        ,(RPG.SaldoH * RPG.StdGADiaH * 7.000000)/1000.000000 StdAlimH \
                                        ,(RPG.SaldoM * RPG.StdGADiaM * 7.000000)/1000.000000 StdAlimM \
                                        ,nvl((RPG.ConsAlimH + RPG.ConsAlimM) / nullif(RPG.ProdHI,0.000000),0.000000) AlimProdHI \
                                        ,ROW_NUMBER()  OVER(PARTITION BY RPG.ComplexEntityNo ORDER BY RPG.pk_semanavida ASC) RowNumber \
                                        from {database_name}.Reprod_Galpon_Edad_Levante RPG \
                                        LEFT JOIN {database_name}.Reprod_Galpon_Edad_Levante A on  A.Complexentityno = RPG.Complexentityno  AND A.pk_semanavida = RPG.pk_semanavida -1 \
                                        where RPG.CondicionH < 5.000000")

df_ft_Reprod_Galpon_Semana =df_ft_Reprod_Galpon_Semana1.union(df_ft_Reprod_Galpon_Semana2)


  
print('carga df_ft_Reprod_Galpon_Semana')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target10)    
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes10 = spark.read.format("parquet").load(path_target10)
    datos_existentes10 = True
    logger.info(f"Datos existentes de ft_Reprod_Galpon_Semana cargados: {df_existentes10.count()} registros")
except:
    datos_existentes10 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Galpon_Semana")



if datos_existentes10:
    existing_data10 = spark.read.format("parquet").load(path_target10)
    data_after_delete10 = existing_data10.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data10 = df_ft_Reprod_Galpon_Semana.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data10 = filtered_new_data10.union(data_after_delete10)                             
   
    cant_ingresonuevo10 = filtered_new_data10.count()
    cant_total10 = final_data10.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_SemanaTemporal"
    }
    final_data10.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_SemanaTemporal")
    
    
    #schema = existing_data.schema
    final_data10_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_SemanaTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Semana"
    }
    final_data10_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Semana")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Galpon_Semana : {cant_ingresonuevo10}")
    print(f"Total de registros en la tabla ft_Reprod_Galpon_Semana : {cant_total10}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Galpon_SemanaTemporal')
    print(f"Tabla ft_Reprod_Galpon_SemanaTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Semana"
    }
    df_ft_Reprod_Galpon_Semana.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Semana")
df_PonderadosTemp = spark.sql(f"select \
                               pk_tiempo \
                               ,substring(complexentityno,1,(length(complexentityno)-6))complexentityno \
                               ,pk_lote \
                               ,sum(PesoHvoXSaldoH)PesoHvoXSaldoH \
                               ,sum(SaldoHDePesoHvo)SaldoHDePesoHvo \
                               ,nvl(sum(PesoHvoXSaldoH) / nullif(sum(SaldoHDePesoHvo),0),0) PesoHvoPond \
                               ,nvl(sum(PesoHXSaldoH) / nullif(sum(SaldoHDePesoH),0),0) PesoHPond \
                               ,nvl(sum(PesoMXSaldoM) / nullif(sum(SaldoMDePesoM),0),0) PesoMPond \
                               ,nvl(sum(PorcHvoPisoXSaldoH) / nullif(sum(SaldoHDePorcHvoPiso),0),0) PorcHvoPisoPond \
                                   from ( \
                                         select  pk_tiempo ,ComplexEntityNo ,pk_lote,SaldoH,SaldoM,PesoHvo \
                                                ,PesoHvo * SaldoH PesoHvoXSaldoH,case when PesoHvo>0 then SaldoH else 0 end SaldoHDePesoHvo \
                                                ,PesoH,(PesoH*SaldoH) PesoHXSaldoH,case when PesoH>0 then SaldoH else 0 end SaldoHDePesoH \
                                                ,PesoM,(PesoM*SaldoM) PesoMXSaldoM,case when PesoM>0 then SaldoM else 0 end SaldoMDePesoM \
                                                ,PorcHvoPiso,(PorcHvoPiso*SaldoH) PorcHvoPisoXSaldoH,case when PorcHvoPiso>0 then SaldoH else 0 end SaldoHDePorcHvoPiso \
                                         from {database_name}.Reprod_Diario \
                                         ) A \
                                    group by pk_tiempo,substring(complexentityno,1,(length(complexentityno)-6)),pk_lote")
#df_PonderadosTemp.createOrReplaceTempView("Ponderados")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Ponderados"
}
df_PonderadosTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Ponderados")
print('carga Ponderados')
df_CapitalizacionXLoteTemp = spark.sql(f"select substring(complexentityno,1,(length(complexentityno)-6)) Lote,SUM(CapH) CapH, SUM(CapM) CapM, date_format(FechaCapitalizacionMax,'yyyyMMdd') FechaCap \
                                              ,SUM(IngresoH) IngresoH, SUM(IngresoM) IngresoM \
                                        from \
                                            ( \
                                            select ComplexEntityNo,MAX(CapH) CapH,MAX(CapM) CapM,FechaCapitalizacionMax \
                                            ,MAX(IngresoH) IngresoH, MAX(IngresoM) IngresoM \
                                            from {database_name}.Reprod_Diario \
                                            group by ComplexEntityNo,FechaCapitalizacionMax \
                                            ) A \
                                        group by substring(complexentityno,1,(length(complexentityno)-6)),FechaCapitalizacionMax ")
#df_CapitalizacionXLoteTemp.createOrReplaceTempView("CapitalizacionXLote")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/CapitalizacionXLote"
}
df_CapitalizacionXLoteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.CapitalizacionXLote")
print('carga CapitalizacionXLote')
#Se inserta en tabla temporal los registros diarios agrupados por lote en la etapa de cria y levante
df_Reprod_Lote_Diario_CriaTemp = spark.sql(f"select \
                                            RP.pk_tiempo, \
                                            RP.fecha, \
                                            RP.pk_empresa, \
                                            RP.pk_division, \
                                            RP.pk_zona, \
                                            RP.pk_subzona, \
                                            RP.pk_plantel, \
                                            RP.pk_lote, \
                                            max(pk_etapa) pk_etapa, \
                                            min(pk_standard) pk_standard, \
                                            pk_tipoproducto, \
                                            max(RP.pk_especie) pk_especie, \
                                            max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especiel, \
                                            min(pk_estado) pk_estado, \
                                            RP.pk_administrador, \
                                            RP.pk_proveedor, \
                                            pk_generacion, \
                                            max(pk_semanavida) pk_semanavida, \
                                            max(pk_diasvida) pk_diasvida, \
                                            substring(RP.complexentityno,1,(length(RP.complexentityno)-6)) ComplexEntityNo, \
                                            min(fechanacimiento) fechanacimiento, \
                                            min(fechanacimientoprom) fechanacimientoprom, \
                                            max(RP.edad) edad, \
                                            max(EdadLoteModificado) EdadLoteModificado, \
                                            max(pk_diavidalote) pk_diavidalote, \
                                            SUM(nvl(RP.IngresoH,0)) IngresoH, \
                                            SUM(nvl(RP.IngresoM,0)) IngresoM, \
                                            sum(nvl(SacaH,0)) SacaH, \
                                            sum(nvl(SacaM,0)) SacaM, \
                                            MAX(nvl(SacaAcumHLote,0)) SacaAcumH, \
                                            MAX(nvl(SacaAcumMLote,0)) SacaAcumM, \
                                            sum(nvl(MortH,0)) MortH, \
                                            sum(nvl(MortM,0)) MortM, \
                                            MAX(nvl(MortAcumHLote,0)) MortAcumH, \
                                            MAX(nvl(MortAcumMLote,0)) MortAcumM, \
                                            sum(nvl(ConsAlimH,0)) ConsAlimH, \
                                            sum(nvl(ConsAlimM,0)) ConsAlimM, \
                                            sum(nvl(TransIngH,0)) TransIngH, \
                                            sum(nvl(TransSalH,0)) TransSalH, \
                                            sum(nvl(TransIngM,0)) TransIngM, \
                                            sum(nvl(TransSalM,0)) TransSalM, \
                                            max(nvl(TransIngAcumHLote,0)) TransIngAcumHLote, \
                                            max(nvl(TransSalAcumHLote,0)) TransSalAcumHLote, \
                                            max(nvl(TransIngAcumMLote,0)) TransIngAcumMLote, \
                                            max(nvl(TransSalAcumMLote,0)) TransSalAcumMLote, \
                                            Sum(nvl(SaldoH,0)) SaldoH, \
                                            Sum(nvl(SaldoM,0)) SaldoM, \
                                            max(nvl(StdGADiaHLt,0)) StdGADiaH, \
                                            max(nvl(StdGADiaMLt,0)) StdGADiaM, \
                                            max(nvl(StdPorcProdHTLt,0)) StdPorcProdHT, \
                                            max(nvl(StdPorcProdHILt,0)) StdPorcProdHI, \
                                            max(nvl(StdHTGEAcumLt,0)) StdHTGEAcum, \
                                            max(nvl(StdHIGEAcumLt,0)) StdHIGEAcum, \
                                            max(nvl(StdPesoHLt,0)) StdPesoH, \
                                            max(nvl(StdPesoMLt,0)) StdPesoM, \
                                            max(nvl(StdPesoHvoLt,0)) StdPesoHvo, \
                                            max(nvl(StdMortHLt,0)) StdMortH, \
                                            max(nvl(StdMortMLt,0)) StdMortM, \
                                            max(nvl(StdMortAcmHLt,0)) StdMortAcmH, \
                                            max(nvl(StdMortAcmMLt,0)) StdMortAcmM, \
                                            sum(nvl(ProdHT,0)) ProdHT, \
                                            sum(nvl(ProdHI,0)) ProdHI, \
                                            max(nvl(ProdHTAcumLote,0)) ProdHTAcum, \
                                            max(nvl(ProdHIAcumLote,0)) ProdHIAcum, \
                                            max(nvl(PesoHvo,0)) PesoHvo, \
                                            max(nvl(PesoHvoPond,0)) PesoHvoPond, \
                                            max(nvl(PesoH,0)) PesoH, \
                                            max(nvl(PesoHPond,0)) PesoHPond, \
                                            max(nvl(PesoM,0)) PesoM, \
                                            max(nvl(PesoMPond,0)) PesoMPond, \
                                            max(nvl(UnifH,0)) UnifH, \
                                            max(nvl(UnifM,0)) UnifM, \
                                            sum(nvl(ProdPoroso,0)) ProdPoroso, \
                                            sum(nvl(ProdSucio,0)) ProdSucio, \
                                            sum(nvl(ProdSangre,0)) ProdSangre, \
                                            sum(nvl(ProdDeforme,0)) ProdDeforme, \
                                            sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
                                            sum(nvl(ProdRoto,0)) ProdRoto, \
                                            sum(nvl(ProdFarfara,0)) ProdFarfara, \
                                            sum(nvl(ProdInvendible,0)) ProdInvendible, \
                                            sum(nvl(ProdChico,0)) ProdChico, \
                                            sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
                                            sum(nvl(SelecH,0)) SelecH, \
                                            sum(nvl(SelecM,0)) SelecM, \
                                            min(RP.FechaCap) FechaCap, \
                                            SUM(nvl(RP.CapH,0)) CapH, \
                                            SUM(nvl(RP.CapM,0)) CapM, \
                                            nvl((ROUND((sum(PorcHvoPiso)/7),2)),0) PorcHvoPiso, \
                                            max(nvl(PorcHvoPisoPond,0)) PorcHvoPisoPond, \
                                            sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
                                            sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
                                            SUM(nvl(AlimInicio,0)) AlimInicio, \
                                            SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
                                            SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
                                            SUM(nvl(AlimReprodI,0)) AlimReprodI, \
                                            SUM(nvl(AlimReprodII,0)) AlimReprodII, \
                                            SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
                                            SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
                                            SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
                                            SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
                                            SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
                                            SUM(nvl(CreIPavas,0)) CreIPavas, \
                                            SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
                                            SUM(nvl(MantPavas,0)) MantPavas, \
                                            nvl((MAX(nvl(SacaAcumHLote,0.000000)*1.000000)/NULLIF(SUM(nvl(RP.CapH,0.000000)),0.000000))*100.0000000,0.000000) CondicionH, \
                                            case when MAX(pk_etapa) = 1 then MIN(cast(date_add(cast(FechaCapitalizacionMax as timestamp), -1) as date)) \
                                                 when MAX(pk_etapa) = 3 then MAX(cast(date_add(cast(RP.FechaMax as timestamp), -1) as date)) end FechaCierre, \
                                            nvl(MAX(CL.CapH),0) CapHAcum, \
                                            nvl(MAX(CL.CapM),0) CapMAcum, \
                                            MAX(CL2.IngresoH) IngresoHAcum, \
                                            MAX(CL2.IngresoM) IngresoMAcum \
                                            from {database_name}.Reprod_Diario RP \
                                            left join {database_name}.Ponderados P on RP.pk_tiempo = P.pk_tiempo and cast(RP.pk_lote as integer) = cast(P.pk_lote as integer) \
                                            LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on substring(RP.complexentityno,1,(length(RP.complexentityno)-6)) = FIR.ComplexEntityNo \
                                            left join {database_name}.CapitalizacionXLote CL on substring(RP.complexentityno,1,(length(RP.complexentityno)-6)) = CL.Lote and RP.pk_tiempo = CL.FechaCap\
                                            left join {database_name}.CapitalizacionXLote CL2 on substring(RP.complexentityno,1,(length(RP.complexentityno)-6)) = CL2.Lote \
                                            group by RP.pk_tiempo,RP.fecha,RP.pk_empresa,RP.pk_division,RP.pk_zona,RP.pk_subzona,RP.pk_plantel,RP.pk_lote,pk_tipoproducto,RP.pk_administrador, \
                                                     RP.pk_proveedor,pk_generacion,substring(RP.complexentityno,1,(length(RP.complexentityno)-6)) \
                                            order by pk_tiempo,pk_semanavida")
#df_Reprod_Lote_Diario_CriaTemp.createOrReplaceTempView("Reprod_Lote_Diario_Cria")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Lote_Diario_Cria"
}
df_Reprod_Lote_Diario_CriaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Lote_Diario_Cria")
print('carga Reprod_Lote_Diario_Cria')
df_maxStdGADiaHReprodLoteDiario = spark.sql(f"SELECT Complexentityno,pk_diavidalote, max(StdGADiaH) StdGADiaH \
FROM {database_name}.Reprod_Lote_Diario_Cria group by Complexentityno,pk_diavidalote")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxStdGADiaHReprodLoteDiario"
}
df_maxStdGADiaHReprodLoteDiario.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaxStdGADiaHReprodLoteDiario")
print('carga MaxStdGADiaHReprodLoteDiario')
#Inserta los últimos 4 meses de la tabla
#insert into DMPECUARIO.ft_Reprod_Lote_Diario

df_ft_Reprod_Lote_Diario = spark.sql(f"select \
                                     RPG.pk_tiempo \
                                    ,RPG.fecha \
                                    ,RPG.pk_empresa \
                                    ,RPG.pk_division \
                                    ,RPG.pk_zona \
                                    ,RPG.pk_subzona \
                                    ,RPG.pk_plantel \
                                    ,RPG.pk_lote \
                                    ,RPG.pk_etapa \
                                    ,RPG.pk_standard \
                                    ,RPG.pk_tipoproducto \
                                    ,RPG.pk_especiel \
                                    ,RPG.pk_estado \
                                    ,RPG.pk_administrador \
                                    ,RPG.pk_proveedor \
                                    ,RPG.pk_generacion \
                                    ,RPG.pk_semanavida \
                                    ,RPG.pk_diavidalote \
                                    ,RPG.ComplexEntityNo \
                                    ,to_timestamp(RPG.FechaNacimientoProm) FechaNacimientoProm \
                                    ,cast(to_timestamp(RPG.FechaCap) as varchar(23)) FechaCap \
                                    ,RPG.EdadLoteModificado \
                                    ,RPG.IngresoH AlojH \
                                    ,RPG.IngresoM AlojM \
                                    ,RPG.MortH \
                                    ,RPG.MortM \
                                    ,RPG.MortAcumH \
                                    ,RPG.MortAcumM \
                                    ,ROUND((nvl((RPG.MortH/NULLIF((RPG.SaldoH)*1.0,0))*100,0)),2) PorcMortH \
                                    ,ROUND((nvl((RPG.MortM/NULLIF((RPG.SaldoM)*1.0,0))*100,0)),2) PorcMortM \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.IngresoH)*1.0,0))*100,0)),2) \
                                                  ELSE ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.CapH)*1.0,0))*100,0)),2) END PorcMortAcumH \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.IngresoM)*1.0,0))*100,0)),2) \
                                                  ELSE ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.CapM)*1.0,0))*100,0)),2) END PorcMortAcumM \
                                    ,RPG.SacaH BenefH \
                                    ,RPG.SacaM BenefM \
                                    ,RPG.SacaAcumH BenefAcumH \
                                    ,RPG.SacaAcumM BenefAcumM \
                                    ,RPG.TransIngH \
                                    ,RPG.TransSalH \
                                    ,RPG.TransIngM \
                                    ,RPG.TransSalM \
                                    ,RPG.TransIngAcumHLote \
                                    ,RPG.TransSalAcumHLote \
                                    ,RPG.TransIngAcumMLote \
                                    ,RPG.TransSalAcumMLote \
                                    ,RPG.SaldoH \
                                    ,RPG.SaldoM \
                                    ,ROUND((nvl(((RPG.SaldoM)/(NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcMH \
                                    ,RPG.StdMortH \
                                    ,RPG.StdMortM \
                                    ,RPG.StdMortAcmH \
                                    ,RPG.StdMortAcmM \
                                    ,RPG.SelecH \
                                    ,RPG.SelecM \
                                    ,ROUND(nvl(((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0)))*1000,0),2) GADH \
                                    ,RPG.StdGADiaH \
                                    ,round(nvl((ROUND((((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0)))*1000),3) - ROUND((((RPG2.ConsAlimH/NULLIF(RPG2.SaldoH,0)))*1000),3)),0),2) IncrGHAH \
                                    ,nvl((RPG.StdGADiaH - A.StdGADiaH),0) IncrStdGHAH \
                                    ,ROUND(nvl(((RPG.ConsAlimM/NULLIF(RPG.SaldoM,0)))*1000,0),2) GADM \
                                    ,RPG.StdGADiaM \
                                    ,RPG.ProdHT \
                                    ,RPG.ProdHTAcum \
                                    ,ROUND((nvl(((RPG.ProdHT/NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcProdHT \
                                    ,RPG.StdPorcProdHT \
                                    ,RPG.ProdHI \
                                    ,RPG.ProdHIAcum \
                                    ,ROUND((nvl((RPG.ProdHI/NULLIF((RPG.ProdHT*1.0),0))*100,0)),2) PorcProdHI \
                                    ,RPG.StdPorcProdHI \
                                    ,ROUND((nvl(RPG.ProdHTAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HTGE \
                                    ,ROUND((nvl(RPG.ProdHIAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HIGE \
                                    ,RPG.StdHTGEAcum \
                                    ,RPG.StdHIGEAcum \
                                    ,RPG.PesoHvoPond PesoHvo \
                                    ,RPG.PesoHPond PesoH \
                                    ,RPG.StdPesoH \
                                    ,RPG.PesoMPond PesoM \
                                    ,RPG.StdPesoM \
                                    ,RPG.UnifH \
                                    ,RPG.UnifM \
                                    ,RPG.PorcHvoPisoPond PorcHvoPiso \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.IngresoH*1.000000,0.000000))*100.0000000,0.000000)),2) \
                                        ELSE ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.CapH*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcViabProdH \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.IngresoM*1.000000,0.000000))*100.0000000,0.000000)),2) \
                                        ELSE ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.CapM*1.000000,0.000000))*100.0000000,0.000000)),2) END PorcViabProdM \
                                    ,(100 - RPG.StdMortAcmH) StdViabH \
                                    ,0 StdViabM \
                                    ,RPG.CapH \
                                    ,RPG.CapM \
                                    ,RPG.ConsAlimH \
                                    ,RPG.ConsAlimM \
                                    ,RPG.ProdPoroso \
                                    ,RPG.ProdSucio \
                                    ,RPG.ProdSangre \
                                    ,RPG.ProdDeforme \
                                    ,RPG.ProdCascaraDebil \
                                    ,RPG.ProdRoto \
                                    ,RPG.ProdFarfara \
                                    ,RPG.ProdInvendible \
                                    ,RPG.ProdChico \
                                    ,RPG.ProdDobleYema \
                                    ,RPG.U_PEErrorSexoH \
                                    ,RPG.U_PEErrorSexoM \
                                    ,RPG.AlimInicio \
                                    ,RPG.AlimCrecimiento \
                                    ,RPG.AlimPreReprod \
                                    ,RPG.AlimReprodI \
                                    ,RPG.AlimReprodII \
                                    ,RPG.AlimReprodMacho \
                                    ,RPG.PosturaCrecIRP \
                                    ,RPG.PosturaPreInicioRP \
                                    ,RPG.StdPesoHvo \
                                    ,RPG.PreInicialPavas \
                                    ,RPG.InicialPavosRP \
                                    ,RPG.CreIPavas \
                                    ,RPG.CrecIIPavas \
                                    ,RPG.MantPavas \
                                    ,RPG.FechaCierre FechaCierre\
                                    ,RPG.CapHAcum \
                                    ,RPG.CapMAcum \
                                    ,RPG.IngresoHAcum \
                                    ,RPG.IngresoMAcum \
                                    from {database_name}.Reprod_Lote_Diario_Cria RPG  \
                                    left join {database_name}.Reprod_Lote_Diario_Cria RPG2 on  RPG2.Complexentityno = RPG.Complexentityno AND  RPG2.pk_tiempo =  RPG.pk_tiempo - 1 and RPG2.pk_administrador = RPG.pk_administrador \
                                    left join {database_name}.MaxStdGADiaHReprodLoteDiario A ON A.Complexentityno = RPG.Complexentityno  AND A.pk_diavidalote = RPG.pk_diavidalote -1 \
                                    where RPG.CondicionH < 5.000000")

print('df_ft_Reprod_Lote_Diario')
# Verificar si la tabla gold ya existe - ft_Reprod_Lote_Diario

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes11 = spark.read.format("parquet").load(path_target11)
    datos_existentes11 = True
    logger.info(f"Datos existentes de ft_Reprod_Lote_Diario cargados: {df_existentes11.count()} registros")
except:
    datos_existentes11 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Lote_Diario")

    
    
if datos_existentes11:
   # gold_table = spark.read.format("parquet").load(path_target11)     
    
    #if gold_table is not None:
    existing_data11 = spark.read.format("parquet").load(path_target11)
    data_after_delete11 = existing_data11.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data11 = df_ft_Reprod_Lote_Diario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data11 = filtered_new_data11.union(data_after_delete11)                             
    
    cant_ingresonuevo11 = filtered_new_data11.count()
    cant_total11 = final_data11.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_DiarioTemporal"
    }
    final_data11.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data11_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Diario"
    }
    final_data11_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Diario")
        
        
    print(f"agrega registros nuevos a la tabla ft_Reprod_Lote_Diario : {cant_ingresonuevo11}")
    print(f"Total de registros en la tabla ft_Reprod_Lote_Diario : {cant_total11}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Lote_DiarioTemporal')
    print(f"Tabla ft_Reprod_Lote_DiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
            
    #else:
    #    additional_options = {
    #        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Diario"
    #    }
    #    df_ft_Reprod_Lote_Diario.write \
    #        .format("parquet") \
    #        .options(**additional_options) \
    #        .mode("overwrite") \
    #        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Diario")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Diario"
    }
    df_ft_Reprod_Lote_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Diario")
#Ponderados
df_Ponderados1Temp = spark.sql(f"select \
                                 max(pk_tiempo) pk_tiempo \
                                ,substring(complexentityno,1,(length(complexentityno)-6))complexentityno \
                                ,pk_lote \
                                ,pk_diavidalote \
                                ,sum(PesoHvoXSaldoH)PesoHvoXSaldoH \
                                ,sum(SaldoHDePesoHvo)SaldoHDePesoHvo \
                                ,nvl(sum(PesoHvoXSaldoH) / nullif(sum(SaldoHDePesoHvo),0),0) PesoHvoPond \
                                ,nvl(sum(PesoHXSaldoH) / nullif(sum(SaldoHDePesoH),0),0) PesoHPond \
                                ,nvl(sum(PesoMXSaldoM) / nullif(sum(SaldoMDePesoM),0),0) PesoMPond \
                                ,nvl(sum(PorcHvoPisoXSaldoH) / nullif(sum(SaldoHDePorcHvoPiso),0),0) PorcHvoPisoPond \
                                from( \
                                    select \
                                        pk_tiempo \
                                        ,ComplexEntityNo \
                                        ,pk_lote \
                                        ,pk_diavidalote \
                                        ,SaldoH \
                                        ,SaldoM \
                                        ,PesoHvo \
                                        ,(PesoHvo * SaldoH) PesoHvoXSaldoH \
                                        ,case when PesoHvo>0 then SaldoH else 0 end SaldoHDePesoHvo \
                                        ,PesoH \
                                        ,(PesoH*SaldoH) PesoHXSaldoH \
                                        ,case when PesoH>0 then SaldoH else 0 end SaldoHDePesoH \
                                        ,PesoM \
                                        ,(PesoM*SaldoM) PesoMXSaldoM \
                                        ,case when PesoM>0 then SaldoM else 0 end SaldoMDePesoM \
                                        ,PorcHvoPiso \
                                        ,(PorcHvoPiso*SaldoH) PorcHvoPisoXSaldoH \
                                        ,case when PorcHvoPiso>0 then SaldoH else 0 end SaldoHDePorcHvoPiso \
                                    from {database_name}.Reprod_Diario \
                                    ) A \
                                group by substring(complexentityno,1,(length(complexentityno)-6)),pk_lote,pk_diavidalote")
#df_Ponderados1Temp.createOrReplaceTempView("Ponderados1")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Ponderados1"
}
df_Ponderados1Temp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Ponderados1")
print('carga Ponderados1')
#Se inserta en tabla temporal los registros semanales agrupados por lote en la etapa de cria
df_Reprod_Lote_Sem_CriaTemp = spark.sql(f"select \
                                        max(A.pk_tiempo) pk_tiempo, \
                                        max(A.fecha) fecha, \
                                        A.pk_empresa, \
                                        A.pk_division, \
                                        A.pk_zona, \
                                        A.pk_subzona, \
                                        A.pk_plantel, \
                                        A.pk_lote, \
                                        max(pk_etapa) pk_etapa,  \
                                        min(pk_standard) pk_standard,  \
                                        pk_tipoproducto, \
                                        max(A.pk_especie) pk_especie, \
                                        max(nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0'))) pk_especiel, \
                                        min(pk_estado) pk_estado, \
                                        A.pk_administrador, \
                                        A.pk_proveedor, \
                                        pk_generacion, \
                                        A.pk_diavidalote pk_semanavida, \
                                        A.ComplexEntityNo, \
                                        min(fechanacimientoprom) fechanacimiento, \
                                        max(edad) edad, \
                                        max(EdadLoteModificado) EdadLoteModificado, \
                                        MAX(nvl(IngresoH,0)) IngresoH, \
                                        MAX(nvl(IngresoM,0)) IngresoM, \
                                        sum(nvl(SacaH,0)) SacaH, \
                                        sum(nvl(SacaM,0)) SacaM, \
                                        MAX(nvl(SacaAcumH,0)) SacaAcumH, \
                                        MAX(nvl(SacaAcumM,0)) SacaAcumM, \
                                        sum(nvl(MortH,0)) MortH, \
                                        sum(nvl(MortM,0)) MortM, \
                                        MAX(nvl(MortAcumH,0)) MortAcumH, \
                                        MAX(nvl(MortAcumM,0)) MortAcumM, \
                                        sum(nvl(ConsAlimH,0)) ConsAlimH, \
                                        sum(nvl(ConsAlimM,0)) ConsAlimM, \
                                        sum(nvl(TransIngH,0)) TransIngH, \
                                        sum(nvl(TransSalH,0)) TransSalH, \
                                        sum(nvl(TransIngM,0)) TransIngM, \
                                        sum(nvl(TransSalM,0)) TransSalM, \
                                        max(nvl(TransIngAcumHLote,0)) TransIngAcumHLote, \
                                        max(nvl(TransSalAcumHLote,0)) TransSalAcumHLote, \
                                        max(nvl(TransIngAcumMLote,0)) TransIngAcumMLote, \
                                        max(nvl(TransSalAcumMLote,0)) TransSalAcumMLote, \
                                        min(nvl(SaldoH,0)) SaldoH, \
                                        min(nvl(SaldoM,0)) SaldoM, \
                                        min(nvl(StdGADiaH,0)) StdGADiaH, \
                                        min(nvl(StdGADiaM,0)) StdGADiaM, \
                                        min(nvl(StdPorcProdHT,0)) StdPorcProdHT, \
                                        min(nvl(StdPorcProdHI,0)) StdPorcProdHI, \
                                        min(nvl(StdHTGEAcum,0)) StdHTGEAcum, \
                                        min(nvl(StdHIGEAcum,0)) StdHIGEAcum, \
                                        min(nvl(StdPesoH,0)) StdPesoH, \
                                        min(nvl(StdPesoM,0)) StdPesoM, \
                                        min(nvl(StdPesoHvo,0)) StdPesoHvo, \
                                        min(nvl(StdMortH,0)) StdMortH, \
                                        min(nvl(StdMortM,0)) StdMortM, \
                                        min(nvl(StdMortAcmH,0)) StdMortAcmH, \
                                        min(nvl(StdMortAcmM,0)) StdMortAcmM, \
                                        sum(nvl(ProdHT,0)) ProdHT, \
                                        sum(nvl(ProdHI,0)) ProdHI, \
                                        max(nvl(ProdHTAcum,0)) ProdHTAcum, \
                                        max(nvl(ProdHIAcum,0)) ProdHIAcum, \
                                        max(nvl(PesoHvo,0)) PesoHvo, \
                                        max(nvl(P.PesoHvoPond,0)) PesoHvoPond, \
                                        max(nvl(PesoH,0)) PesoH, \
                                        max(nvl(P.PesoHPond,0)) PesoHPond, \
                                        max(nvl(PesoM,0)) PesoM, \
                                        max(nvl(P.PesoMPond,0)) PesoMPond, \
                                        max(nvl(UnifH,0)) UnifH, \
                                        max(nvl(UnifM,0)) UnifM, \
                                        sum(nvl(ProdPoroso,0)) ProdPoroso, \
                                        sum(nvl(ProdSucio,0)) ProdSucio, \
                                        sum(nvl(ProdSangre,0)) ProdSangre, \
                                        sum(nvl(ProdDeforme,0)) ProdDeforme, \
                                        sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
                                        sum(nvl(ProdRoto,0)) ProdRoto, \
                                        sum(nvl(ProdFarfara,0)) ProdFarfara, \
                                        sum(nvl(ProdInvendible,0)) ProdInvendible, \
                                        sum(nvl(ProdChico,0)) ProdChico, \
                                        sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
                                        sum(nvl(SelecH,0)) SelecH, \
                                        sum(nvl(SelecM,0)) SelecM, \
                                        min(FechaCap) FechaCap, \
                                        MAX(nvl(CapH,0)) CapH, \
                                        MAX(nvl(CapM,0)) CapM, \
                                        nvl((ROUND((sum(PorcHvoPiso)),2)),0) PorcHvoPiso, \
                                        nvl((ROUND((sum(P.PorcHvoPisoPond)),2)),0) PorcHvoPisoPond, \
                                        sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
                                        sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
                                        SUM(nvl(AlimInicio,0)) AlimInicio, \
                                        SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
                                        SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
                                        SUM(nvl(AlimReprodI,0)) AlimReprodI, \
                                        SUM(nvl(AlimReprodII,0)) AlimReprodII, \
                                        SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
                                        SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
                                        SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
                                        SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
                                        SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
                                        SUM(nvl(CreIPavas,0)) CreIPavas, \
                                        SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
                                        SUM(nvl(MantPavas,0)) MantPavas, \
                                        (MAX(nvl(SacaAcumH,0.000000)*1.000000)/NULLIF(MAX(nvl(CapH,0.000000)),0.000000))*100.0000000 CondicionH \
                                        from {database_name}.Reprod_Lote_Diario_Cria A \
                                        left join {database_name}.Ponderados1 P on A.pk_tiempo = P.pk_tiempo and cast(A.pk_lote as integer) = cast(P.pk_lote as integer) \
                                        LEFT JOIN {database_name}.LineaGeneticaXNivel FIR on A.ComplexEntityNo = FIR.ComplexEntityNo \
                                        where pk_etapa = 1 \
                                        group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,pk_tipoproducto, \
                                                 A.pk_administrador,A.pk_proveedor,pk_generacion,A.pk_diavidalote,A.complexentityno \
                                        order by pk_tiempo,pk_semanavida")
#df_Reprod_Lote_Sem_CriaTemp.createOrReplaceTempView("Reprod_Lote_Sem_Cria")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Lote_Sem_Cria"
}
df_Reprod_Lote_Sem_CriaTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Lote_Sem_Cria")
print('carga Reprod_Lote_Sem_Cria')
#Se inserta en tabla temporal los registros semanales agrupados por lote en la etapa de Levante
df_Reprod_Lote_Sem_LevanteTemp =spark.sql(f"select max(A.pk_tiempo) pk_tiempo,max(A.fecha) fecha,A.pk_empresa,A.pk_division, \
A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,max(pk_etapa) pk_etapa,min(pk_standard) pk_standard,pk_tipoproducto, \
max(A.pk_especie) pk_especie,nvl(FIR.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especiel, \
min(pk_estado) pk_estado,A.pk_administrador,A.pk_proveedor,pk_generacion,A.pk_diavidalote pk_semanavida,A.ComplexEntityNo, \
min(cast(cast(fechanacimientoprom as timestamp) as date)) fechanacimiento, \
max(edad) edad, \
max(EdadLoteModificado) EdadLoteModificado, \
MAX(nvl(IngresoH,0)) IngresoH, \
MAX(nvl(IngresoM,0)) IngresoM, \
sum(nvl(SacaH,0)) SacaH, \
sum(nvl(SacaM,0)) SacaM, \
MAX(nvl(SacaAcumH,0)) SacaAcumH, \
MAX(nvl(SacaAcumM,0)) SacaAcumM, \
sum(nvl(MortH,0)) MortH, \
sum(nvl(MortM,0)) MortM, \
MAX(nvl(MortAcumH,0)) MortAcumH, \
MAX(nvl(MortAcumM,0)) MortAcumM, \
sum(nvl(ConsAlimH,0)) ConsAlimH, \
sum(nvl(ConsAlimM,0)) ConsAlimM, \
sum(nvl(TransIngH,0)) TransIngH, \
sum(nvl(TransSalH,0)) TransSalH, \
sum(nvl(TransIngM,0)) TransIngM, \
sum(nvl(TransSalM,0)) TransSalM, \
max(nvl(TransIngAcumHLote,0)) TransIngAcumHLote, \
max(nvl(TransSalAcumHLote,0)) TransSalAcumHLote, \
max(nvl(TransIngAcumMLote,0)) TransIngAcumMLote, \
max(nvl(TransSalAcumMLote,0)) TransSalAcumMLote, \
min(nvl(SaldoH,0)) SaldoH, \
min(nvl(SaldoM,0)) SaldoM, \
min(nvl(StdGADiaH,0)) StdGADiaH, \
min(nvl(StdGADiaM,0)) StdGADiaM, \
min(nvl(StdPorcProdHT,0)) StdPorcProdHT, \
min(nvl(StdPorcProdHI,0)) StdPorcProdHI, \
min(nvl(StdHTGEAcum,0)) StdHTGEAcum, \
min(nvl(StdHIGEAcum,0)) StdHIGEAcum, \
min(nvl(StdPesoH,0)) StdPesoH, \
min(nvl(StdPesoM,0)) StdPesoM, \
min(nvl(StdPesoHvo,0)) StdPesoHvo, \
min(nvl(StdMortH,0)) StdMortH, \
min(nvl(StdMortM,0)) StdMortM, \
min(nvl(StdMortAcmH,0)) StdMortAcmH, \
min(nvl(StdMortAcmM,0)) StdMortAcmM, \
sum(nvl(ProdHT,0)) ProdHT, \
sum(nvl(ProdHI,0)) ProdHI, \
max(nvl(ProdHTAcum,0)) ProdHTAcum, \
max(nvl(ProdHIAcum,0)) ProdHIAcum, \
max(nvl(PesoHvo,0)) PesoHvo, \
max(nvl(P.PesoHvoPond,0)) PesoHvoPond, \
max(nvl(PesoH,0)) PesoH, \
max(nvl(P.PesoHPond,0)) PesoHPond, \
max(nvl(PesoM,0)) PesoM, \
max(nvl(P.PesoMPond,0)) PesoMPond, \
max(nvl(UnifH,0)) UnifH, \
max(nvl(UnifM,0)) UnifM, \
sum(nvl(ProdPoroso,0)) ProdPoroso, \
sum(nvl(ProdSucio,0)) ProdSucio, \
sum(nvl(ProdSangre,0)) ProdSangre, \
sum(nvl(ProdDeforme,0)) ProdDeforme, \
sum(nvl(ProdCascaraDebil,0)) ProdCascaraDebil, \
sum(nvl(ProdRoto,0)) ProdRoto, \
sum(nvl(ProdFarfara,0)) ProdFarfara, \
sum(nvl(ProdInvendible,0)) ProdInvendible, \
sum(nvl(ProdChico,0)) ProdChico, \
sum(nvl(ProdDobleYema,0)) ProdDobleYema, \
sum(nvl(SelecH,0)) SelecH, \
sum(nvl(SelecM,0)) SelecM, \
min(FechaCap) FechaCap, \
MAX(nvl(CapH,0)) CapH, \
MAX(nvl(CapM,0)) CapM, \
nvl((ROUND((sum(PorcHvoPiso)),2)),0) PorcHvoPiso, \
nvl((ROUND((sum(P.PorcHvoPisoPond)),2)),0) PorcHvoPisoPond, \
sum(nvl(U_PEErrorSexoH,0)) U_PEErrorSexoH, \
sum(nvl(U_PEErrorSexoM,0)) U_PEErrorSexoM, \
SUM(nvl(AlimInicio,0)) AlimInicio, \
SUM(nvl(AlimCrecimiento,0)) AlimCrecimiento, \
SUM(nvl(AlimPreReprod,0)) AlimPreReprod, \
SUM(nvl(AlimReprodI,0)) AlimReprodI, \
SUM(nvl(AlimReprodII,0)) AlimReprodII, \
SUM(nvl(AlimReprodMacho,0)) AlimReprodMacho, \
SUM(nvl(PosturaCrecIRP,0)) PosturaCrecIRP, \
SUM(nvl(PosturaPreInicioRP,0)) PosturaPreInicioRP, \
SUM(nvl(PreInicialPavas,0)) PreInicialPavas, \
SUM(nvl(InicialPavosRP,0)) InicialPavosRP, \
SUM(nvl(CreIPavas,0)) CreIPavas, \
SUM(nvl(CrecIIPavas,0)) CrecIIPavas, \
SUM(nvl(MantPavas,0)) MantPavas, \
(MAX(nvl(SacaAcumH,0.000000)*1.000000)/NULLIF(MAX(nvl(CapH,0.000000)),0.000000))*100.0000000 CondicionH \
from Reprod_Lote_Diario_Cria A \
left join Ponderados1 P on A.pk_tiempo = P.pk_tiempo and cast(A.pk_lote as integer) = cast(P.pk_lote as integer) \
LEFT JOIN LineaGeneticaXNivel FIR on A.ComplexEntityNo = FIR.ComplexEntityNo \
where pk_etapa = 3 \
group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,pk_tipoproducto,A.pk_administrador,A.pk_proveedor, \
pk_generacion,A.pk_diavidalote,FIR.pk_especie,A.complexentityno \
order by pk_tiempo,pk_semanavida")
#df_Reprod_Lote_Sem_LevanteTemp.createOrReplaceTempView("Reprod_Lote_Sem_Levante")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Reprod_Lote_Sem_Levante"
}
df_Reprod_Lote_Sem_LevanteTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Reprod_Lote_Sem_Levante")
print('carga Reprod_Lote_Sem_Levante')
df_maxStdGADiaHReprodLoteSemana1 = spark.sql(f"SELECT Complexentityno,pk_semanavida, max(StdGADiaH) StdGADiaH FROM \
{database_name}.Reprod_Lote_Sem_Cria group by Complexentityno,pk_semanavida")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxStdGADiaHReprodLoteSemana1"
}
df_maxStdGADiaHReprodLoteSemana1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaxStdGADiaHReprodLoteSemana1")
print('carga MaxStdGADiaHReprodLoteSemana1')
#Inserta los últimos 4 meses de la tabla
#insert into DMPECUARIO.ft_Reprod_Lote_Semana 

df_ft_Reprod_Lote_Semana1 = spark.sql(f"select \
                                         RPG.pk_tiempo \
                                        ,RPG.fecha \
                                        ,RPG.pk_empresa \
                                        ,RPG.pk_division \
                                        ,RPG.pk_zona \
                                        ,RPG.pk_subzona \
                                        ,RPG.pk_plantel \
                                        ,RPG.pk_lote \
                                        ,RPG.pk_etapa \
                                        ,RPG.pk_standard \
                                        ,RPG.pk_tipoproducto \
                                        ,RPG.pk_especiel \
                                        ,RPG.pk_estado \
                                        ,RPG.pk_administrador \
                                        ,RPG.pk_proveedor \
                                        ,RPG.pk_generacion \
                                        ,RPG.pk_semanavida \
                                        ,RPG.ComplexEntityNo \
                                        ,RPG.FechaNacimiento \
                                        ,RPG.FechaCap \
                                        ,RPG.EdadLoteModificado \
                                        ,RPG.IngresoH AlojH \
                                        ,RPG.IngresoM AlojM \
                                        ,RPG.MortH \
                                        ,RPG.MortM \
                                        ,RPG.MortAcumH \
                                        ,RPG.MortAcumM \
                                        ,ROUND((nvl((RPG.MortH/NULLIF((RPG.SaldoH)*1.0,0))*100,0)),2) PorcMortH \
                                        ,ROUND((nvl((RPG.MortM/NULLIF((RPG.SaldoM)*1.0,0))*100,0)),2) PorcMortM \
                                        ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.IngresoH)*1.0,0))*100,0)),2) \
                                              ELSE ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.CapH)*1.0,0))*100,0)),2) END PorcMortAcumH \
                                        ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.IngresoM)*1.0,0))*100,0)),2) \
                                              ELSE ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.CapM)*1.0,0))*100,0)),2) END PorcMortAcumM \
                                        ,RPG.SacaH BenefH \
                                        ,RPG.SacaM BenefM \
                                        ,RPG.SacaAcumH BenefAcumH \
                                        ,RPG.SacaAcumM BenefAcumM \
                                        ,RPG.TransIngH \
                                        ,RPG.TransSalH \
                                        ,RPG.TransIngM \
                                        ,RPG.TransSalM \
                                        ,RPG.TransIngAcumHLote \
                                        ,RPG.TransSalAcumHLote \
                                        ,RPG.TransIngAcumMLote \
                                        ,RPG.TransSalAcumMLote \
                                        ,nvl(RPG.SaldoH,0) SaldoH \
                                        ,nvl(RPG.SaldoM,0) SaldoM \
                                        ,ROUND((nvl(((RPG.SaldoM)/(NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcMH \
                                        ,RPG.StdMortH \
                                        ,RPG.StdMortM \
                                        ,RPG.StdMortAcmH \
                                        ,RPG.StdMortAcmM \
                                        ,RPG.SelecH \
                                        ,RPG.SelecM \
                                        ,ROUND(nvl(((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0)/7))*1000,0),2) GADH \
                                        ,RPG.StdGADiaH \
                                        ,nvl((ROUND((((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000),1) - ROUND((((RPG2.ConsAlimH/NULLIF(RPG2.SaldoH,0))/7)*1000),1) ),0) IncrGHAH \
                                        ,nvl((RPG.StdGADiaH - A.StdGADiaH),0) IncrStdGHAH \
                                        ,ROUND(nvl(((RPG.ConsAlimM/NULLIF(RPG.SaldoM,0))/7)*1000,0),2) GADM \
                                        ,RPG.StdGADiaM \
                                        ,RPG.ProdHT \
                                        ,RPG.ProdHTAcum \
                                        ,ROUND((nvl(((RPG.ProdHT/NULLIF((RPG.SaldoH)*1.0,0))/7)*100,0)),2) PorcProdHT \
                                        ,RPG.StdPorcProdHT \
                                        ,RPG.ProdHI \
                                        ,RPG.ProdHIAcum \
                                        ,ROUND((nvl(((RPG.ProdHI/NULLIF((RPG.ProdHT*1.0),0)))*100,0)),2) PorcProdHI \
                                        ,RPG.StdPorcProdHI \
                                        ,ROUND((nvl(RPG.ProdHTAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HTGE \
                                        ,ROUND((nvl(RPG.ProdHIAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HIGE \
                                        ,RPG.StdHTGEAcum \
                                        ,RPG.StdHIGEAcum \
                                        ,RPG.PesoHvoPond \
                                        ,RPG.PesoHPond \
                                        ,RPG.StdPesoH \
                                        ,RPG.PesoMPond \
                                        ,RPG.StdPesoM \
                                        ,RPG.UnifH \
                                        ,RPG.UnifM \
                                        ,RPG.PorcHvoPisoPond \
                                        ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.IngresoH*1.0,0))*100,0)),2) \
                                              ELSE ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.CapH*1.0,0))*100,0)),2) END PorcViabProdH \
                                        ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.IngresoM*1.0,0))*100,0)),2) \
                                              ELSE ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.CapM*1.0,0))*100,0)),2) END PorcViabProdM \
                                        ,(100 - RPG.StdMortAcmH) StdViabH \
                                        ,(100 - RPG.StdMortAcmM) StdViabM \
                                        ,RPG.CapH \
                                        ,RPG.CapM \
                                        ,RPG.ConsAlimH \
                                        ,RPG.ConsAlimM \
                                        ,RPG.ProdPoroso \
                                        ,RPG.ProdSucio \
                                        ,RPG.ProdSangre \
                                        ,RPG.ProdDeforme \
                                        ,RPG.ProdCascaraDebil \
                                        ,RPG.ProdRoto \
                                        ,RPG.ProdFarfara \
                                        ,RPG.ProdInvendible \
                                        ,RPG.ProdChico \
                                        ,RPG.ProdDobleYema \
                                        ,RPG.U_PEErrorSexoH \
                                        ,RPG.U_PEErrorSexoM \
                                        ,RPG.AlimInicio \
                                        ,RPG.AlimCrecimiento \
                                        ,RPG.AlimPreReprod \
                                        ,RPG.AlimReprodI \
                                        ,RPG.AlimReprodII \
                                        ,RPG.AlimReprodMacho \
                                        ,RPG.PosturaCrecIRP \
                                        ,RPG.PosturaPreInicioRP \
                                        ,RPG.pk_semanavida pk_semanavida1 \
                                        ,RPG.StdPesoHvo \
                                        ,RPG. PreInicialPavas \
                                        ,RPG.InicialPavosRP \
                                        ,RPG.CreIPavas \
                                        ,RPG.CrecIIPavas \
                                        ,RPG.MantPavas \
                                        ,(RPG.SaldoH * RPG.StdGADiaH * 7)/1000 StdAlimH \
                                        ,(RPG.SaldoM * RPG.StdGADiaM * 7)/1000 StdAlimM \
                                        ,nvl((RPG.ConsAlimH + RPG.ConsAlimM) / nullif(RPG.ProdHI,0),0) AlimProdHI \
                                        from {database_name}.Reprod_Lote_Sem_Cria RPG \
                                        left join {database_name}.Reprod_Lote_Sem_Cria RPG2 on RPG2.Complexentityno = RPG.Complexentityno AND RPG2.pk_semanavida = RPG.pk_semanavida -1 and RPG2.pk_administrador = RPG.pk_administrador \
                                        left join {database_name}.MaxStdGADiaHReprodLoteSemana1 A on A.Complexentityno = RPG.Complexentityno  AND A.pk_semanavida = RPG.pk_semanavida -1")
print('df_ft_Reprod_Lote_Semana1')
df_maxStdGADiaHReprodLoteSemana2 = spark.sql(f"SELECT Complexentityno,pk_semanavida, max(StdGADiaH) StdGADiaH \
FROM {database_name}.Reprod_Lote_Sem_Cria group by Complexentityno,pk_semanavida")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaxStdGADiaHReprodLoteSemana2"
}
df_maxStdGADiaHReprodLoteSemana2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaxStdGADiaHReprodLoteSemana2")
print('carga MaxStdGADiaHReprodLoteSemana2')
df_ft_Reprod_Lote_Semana2 =spark.sql(f"select \
                                     RPG.pk_tiempo \
                                    ,RPG.fecha \
                                    ,RPG.pk_empresa \
                                    ,RPG.pk_division \
                                    ,RPG.pk_zona \
                                    ,RPG.pk_subzona \
                                    ,RPG.pk_plantel \
                                    ,RPG.pk_lote \
                                    ,RPG.pk_etapa \
                                    ,RPG.pk_standard \
                                    ,RPG.pk_tipoproducto \
                                    ,RPG.pk_especiel \
                                    ,RPG.pk_estado \
                                    ,RPG.pk_administrador \
                                    ,RPG.pk_proveedor \
                                    ,RPG.pk_generacion \
                                    ,RPG.pk_semanavida \
                                    ,RPG.ComplexEntityNo \
                                    ,RPG.FechaNacimiento \
                                    ,RPG.FechaCap \
                                    ,RPG.EdadLoteModificado \
                                    ,RPG.IngresoH AlojH \
                                    ,RPG.IngresoM AlojM \
                                    ,RPG.MortH \
                                    ,RPG.MortM \
                                    ,RPG.MortAcumH \
                                    ,RPG.MortAcumM \
                                    ,ROUND((nvl((RPG.MortH/NULLIF((RPG.SaldoH)*1.0,0))*100,0)),2) PorcMortH \
                                    ,ROUND((nvl((RPG.MortM/NULLIF((RPG.SaldoM)*1.0,0))*100,0)),2) PorcMortM \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.IngresoH)*1.0,0))*100,0)),2) \
                                          ELSE ROUND((nvl((RPG.MortAcumH/NULLIF((RPG.CapH)*1.0,0))*100,0)),2) END PorcMortAcumH \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.IngresoM)*1.0,0))*100,0)),2) \
                                          ELSE ROUND((nvl((RPG.MortAcumM/NULLIF((RPG.CapM)*1.0,0))*100,0)),2) END PorcMortAcumM \
                                    ,RPG.SacaH BenefH \
                                    ,RPG.SacaM BenefM \
                                    ,RPG.SacaAcumH BenefAcumH \
                                    ,RPG.SacaAcumM BenefAcumM \
                                    ,RPG.TransIngH \
                                    ,RPG.TransSalH \
                                    ,RPG.TransIngM \
                                    ,RPG.TransSalM \
                                    ,RPG.TransIngAcumHLote \
                                    ,RPG.TransSalAcumHLote \
                                    ,RPG.TransIngAcumMLote \
                                    ,RPG.TransSalAcumMLote \
                                    ,nvl(RPG.SaldoH,0) SaldoH \
                                    ,nvl(RPG.SaldoM,0) SaldoM \
                                    ,ROUND((nvl(((RPG.SaldoM)/(NULLIF((RPG.SaldoH)*1.0,0)))*100,0)),2) PorcMH \
                                    ,RPG.StdMortH \
                                    ,RPG.StdMortM \
                                    ,RPG.StdMortAcmH \
                                    ,RPG.StdMortAcmM \
                                    ,RPG.SelecH \
                                    ,RPG.SelecM \
                                    ,ROUND(nvl(((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000,0),2) GADH \
                                    ,RPG.StdGADiaH \
                                    ,nvl((ROUND((((RPG.ConsAlimH/NULLIF(RPG.SaldoH,0))/7)*1000),1) -  ROUND((((RPG2.ConsAlimH/NULLIF(RPG2.SaldoH,0))/7)*1000),1) ),0) IncrGHAH \
                                    ,nvl((RPG.StdGADiaH - A.StdGADiaH),0) IncrStdGHAH \
                                    ,ROUND(nvl(((RPG.ConsAlimM/NULLIF(RPG.SaldoM,0))/7)*1000,0),2) GADM \
                                    ,RPG.StdGADiaM \
                                    ,RPG.ProdHT \
                                    ,RPG.ProdHTAcum \
                                    ,ROUND((nvl(((RPG.ProdHT/NULLIF((RPG.SaldoH)*1.0,0))/7)*100,0)),2) PorcProdHT \
                                    ,RPG.StdPorcProdHT \
                                    ,RPG.ProdHI \
                                    ,RPG.ProdHIAcum \
                                    ,ROUND((nvl(((RPG.ProdHI/NULLIF((RPG.ProdHT*1.0),0)))*100,0)),2) PorcProdHI \
                                    ,RPG.StdPorcProdHI \
                                    ,ROUND((nvl(RPG.ProdHTAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HTGE \
                                    ,ROUND((nvl(RPG.ProdHIAcum/NULLIF(RPG.CapH*1.0,0),0)),2) HIGE \
                                    ,RPG.StdHTGEAcum \
                                    ,RPG.StdHIGEAcum \
                                    ,RPG.PesoHvoPond \
                                    ,RPG.PesoHPond \
                                    ,RPG.StdPesoH \
                                    ,RPG.PesoMPond \
                                    ,RPG.StdPesoM \
                                    ,RPG.UnifH \
                                    ,RPG.UnifM \
                                    ,RPG.PorcHvoPisoPond \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.IngresoH*1.0,0))*100,0)),2) \
                                          ELSE ROUND((nvl(((RPG.SaldoH)/NULLIF(RPG.CapH*1.0,0))*100,0)),2) END PorcViabProdH \
                                    ,CASE WHEN RPG.pk_etapa = 1 THEN ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.IngresoM*1.0,0))*100,0)),2) \
                                          ELSE ROUND((nvl(((RPG.SaldoM)/NULLIF(RPG.CapM*1.0,0))*100,0)),2) END PorcViabProdM \
                                    ,(100 - RPG.StdMortAcmH) StdViabH \
                                    ,(100 - RPG.StdMortAcmM) StdViabM \
                                    ,RPG.CapH \
                                    ,RPG.CapM \
                                    ,RPG.ConsAlimH \
                                    ,RPG.ConsAlimM \
                                    ,RPG.ProdPoroso \
                                    ,RPG.ProdSucio \
                                    ,RPG.ProdSangre \
                                    ,RPG.ProdDeforme \
                                    ,RPG.ProdCascaraDebil \
                                    ,RPG.ProdRoto \
                                    ,RPG.ProdFarfara \
                                    ,RPG.ProdInvendible \
                                    ,RPG.ProdChico \
                                    ,RPG.ProdDobleYema \
                                    ,RPG.U_PEErrorSexoH \
                                    ,RPG.U_PEErrorSexoM \
                                    ,RPG.AlimInicio \
                                    ,RPG.AlimCrecimiento \
                                    ,RPG.AlimPreReprod \
                                    ,RPG.AlimReprodI \
                                    ,RPG.AlimReprodII \
                                    ,RPG.AlimReprodMacho \
                                    ,RPG.PosturaCrecIRP \
                                    ,RPG.PosturaPreInicioRP \
                                    ,RPG.pk_semanavida pk_semanavida1 \
                                    ,RPG.StdPesoHvo \
                                    ,RPG. PreInicialPavas \
                                    ,RPG.InicialPavosRP \
                                    ,RPG.CreIPavas \
                                    ,RPG.CrecIIPavas \
                                    ,RPG.MantPavas \
                                    ,(RPG.SaldoH * RPG.StdGADiaH * 7)/1000.000000 StdAlimH \
                                    ,(RPG.SaldoM * RPG.StdGADiaM * 7)/1000.000000 StdAlimM \
                                    ,nvl((RPG.ConsAlimH + RPG.ConsAlimM) / nullif(RPG.ProdHI,0.000000),0.000000) AlimProdHI \
                                    from {database_name}.Reprod_Lote_Sem_Levante RPG \
                                    left join {database_name}.Reprod_Lote_Sem_Levante RPG2 on RPG2.Complexentityno = RPG.Complexentityno AND RPG2.pk_semanavida = RPG.pk_semanavida -1 and RPG2.pk_administrador = RPG.pk_administrador \
                                    left join {database_name}.MaxStdGADiaHReprodLoteSemana2 A on A.Complexentityno = RPG.Complexentityno  AND A.pk_semanavida = RPG.pk_semanavida -1 \
                                    where RPG.CondicionH <5.000000")

df_ft_Reprod_Lote_Semana = df_ft_Reprod_Lote_Semana1.union(df_ft_Reprod_Lote_Semana2)


print('carga df_ft_Reprod_Lote_Semana')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target12) 
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")


try:
    df_existentes12 = spark.read.format("parquet").load(path_target12)
    datos_existentes12 = True
    logger.info(f"Datos existentes de ft_Reprod_Lote_Semana cargados: {df_existentes12.count()} registros")
except:
    datos_existentes12 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Lote_Semana")



if datos_existentes12:
    existing_data12 = spark.read.format("parquet").load(path_target12)
    data_after_delete12 = existing_data12.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data12 = df_ft_Reprod_Lote_Semana.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data12 = filtered_new_data12.union(data_after_delete12)                             
   
    cant_ingresonuevo12 = filtered_new_data12.count()
    cant_total12 = final_data12.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_SemanaTemporal"
    }
    final_data12.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_SemanaTemporal")
    
    
    #schema = existing_data.schema
    final_data12_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_SemanaTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Semana"
    }
    final_data12_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Semana")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Lote_Semana : {cant_ingresonuevo12}")
    print(f"Total de registros en la tabla ft_Reprod_Lote_Semana : {cant_total12}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Lote_SemanaTemporal')
    print(f"Tabla ft_Reprod_Lote_SemanaTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Semana"
    }
    df_ft_Reprod_Lote_Semana.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Semana")
# Ponderados Galpon
df_PonderadoGalponMensualTemp = spark.sql(f"select \
                                           date_format(fecha,'yyyyMM') fecha \
                                           ,complexentityno \
                                           ,pk_lote \
                                           ,pk_galpon \
                                           ,sum(PesoHvoXSaldoH)PesoHvoXSaldoH \
                                           ,sum(SaldoHDePesoHvo)SaldoHDePesoHvo \
                                           ,nvl(sum(PesoHvoXSaldoH) / nullif(sum(SaldoHDePesoHvo),0),0) PesoHvoPond \
                                           ,nvl(sum(PesoHXSaldoH) / nullif(sum(SaldoHDePesoH),0),0) PesoHPond \
                                           ,nvl(sum(PesoMXSaldoM) / nullif(sum(SaldoMDePesoM),0),0) PesoMPond \
                                           ,nvl(sum(PorcHvoPisoXSaldoH) / nullif(sum(SaldoHDePorcHvoPiso),0),0) PorcHvoPisoPond \
                                           ,nvl(sum(UnifHXSaldoH) / nullif(sum(SaldoHDeUnifH),0),0) UnifHPond \
                                           ,nvl(sum(UnifMXSaldoM) / nullif(sum(SaldoMDeUnifM),0),0) UnifMPond \
                                           from ( \
                                                select \
                                                 a.pk_tiempo \
                                                ,b.fecha \
                                                ,ComplexEntityNo \
                                                ,pk_lote \
                                                ,pk_galpon \
                                                ,SaldoH \
                                                ,SaldoM \
                                                ,PesoHvo \
                                                ,(PesoHvo * SaldoH) PesoHvoXSaldoH \
                                                ,case when PesoHvo>0 then SaldoH else 0 end SaldoHDePesoHvo \
                                                ,PesoH \
                                                ,(PesoH*SaldoH) PesoHXSaldoH \
                                                ,case when PesoH>0 then SaldoH else 0 end SaldoHDePesoH \
                                                ,PesoM \
                                                ,(PesoM*SaldoM) PesoMXSaldoM \
                                                ,case when PesoM>0 then SaldoM else 0 end SaldoMDePesoM \
                                                ,PorcHvoPiso \
                                                ,(PorcHvoPiso*SaldoH) PorcHvoPisoXSaldoH \
                                                ,case when PorcHvoPiso>0 then SaldoH else 0 end SaldoHDePorcHvoPiso \
                                                ,UnifH \
                                                ,(UnifH*SaldoH) UnifHXSaldoH \
                                                ,case when UnifH>0 then SaldoH else 0 end SaldoHDeUnifH \
                                                ,UnifM \
                                                ,(UnifM*SaldoM) UnifMXSaldoM \
                                                ,case when UnifM>0 then SaldoM else 0 end SaldoMDeUnifM \
                                                from {database_name}.ft_Reprod_Galpon_Diario a\
                                                left join {database_name}.lk_tiempo b on a.pk_tiempo=b.pk_tiempo \
                                                ) A \
                                            group by date_format(fecha,'yyyyMM') ,ComplexEntityNo,pk_lote,pk_galpon") 
#df_PonderadoGalponMensualTemp.createOrReplaceTempView("PonderadoGalponMensual")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoGalponMensual"
}
df_PonderadoGalponMensualTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoGalponMensual")
print('carga PonderadoGalponMensual')
#Inserta los últimos 4 meses de la tabla
#insert into [DMPECUARIO].[ft_Reprod_Galpon_Mensual](
df_ft_Reprod_Galpon_Mensual = spark.sql(f"select \
                                         MAX(A.pk_tiempo) pk_tiempo \
                                        ,max(a.fecha) fecha \
                                        ,C.idmes \
                                        ,A.pk_empresa \
                                        ,A.pk_division \
                                        ,A.pk_zona \
                                        ,A.pk_subzona \
                                        ,A.pk_plantel \
                                        ,A.pk_lote \
                                        ,A.pk_galpon \
                                        ,A.pk_etapa \
                                        ,A.pk_standard \
                                        ,A.pk_tipoproducto \
                                        ,A.pk_especieg \
                                        ,A.pk_estado \
                                        ,A.pk_administrador \
                                        ,A.pk_proveedor \
                                        ,A.pk_generacion \
                                        ,A.ComplexEntityNo \
                                        ,A.FechaNacimiento \
                                        ,A.FechaCap \
                                        ,MAX(A.Edad) Edad \
                                        ,MAX(A.AlojH) AlojH \
                                        ,MAX(A.AlojM) AlojM \
                                        ,SUM(A.MortH) MortH \
                                        ,SUM(A.MortM) MortM \
                                        ,SUM(A.BenefH) BenefH \
                                        ,SUM(A.BenefM) BenefM \
                                        ,SUM(A.TransIngH) TransIngH \
                                        ,SUM(A.TransSalH) TransSalH \
                                        ,SUM(A.TransIngM) TransIngM \
                                        ,SUM(A.TransSalM) TransSalM \
                                        ,MIN(A.SaldoH) SaldoH \
                                        ,MIN(A.SaldoM) SaldoM \
                                        ,SUM(A.SelecH) SelecH \
                                        ,SUM(A.SelecM) SelecM \
                                        ,SUM(A.ProdHT) ProdHT \
                                        ,SUM(A.ProdHI) ProdHI \
                                        ,MAX(A.HIGE) HIGE \
                                        ,MAX(D.PesoHvoPond) PesoHvo \
                                        ,MAX(D.PesoHPond) PesoH \
                                        ,MAX(D.PesoMPond) PesoM \
                                        ,MAX(D.UnifHPond) UnifH \
                                        ,MAX(D.UnifMPond) UnifM \
                                        ,MAX(A.CapH) CapH \
                                        ,MAX(A.CapM) CapM \
                                        ,SUM(A.ConsAlimH) ConsAlimH \
                                        ,SUM(A.ConsAlimM) ConsAlimM \
                                        ,SUM(A.ProdPoroso) ProdPoroso \
                                        ,SUM(A.ProdSucio) ProdSucio \
                                        ,SUM(A.ProdSangre) ProdSangre \
                                        ,SUM(A.ProdDeforme) ProdDeforme \
                                        ,SUM(A.ProdCascaraDebil) ProdCascaraDebil \
                                        ,SUM(A.ProdRoto) ProdRoto \
                                        ,SUM(A.ProdFarfara) ProdFarfara \
                                        ,SUM(A.ProdInvendible) ProdInvendible \
                                        ,SUM(A.ProdChico) ProdChico \
                                        ,SUM(A.ProdDobleYema) ProdDobleYema \
                                        ,SUM(A.U_PEErrorSexoH) U_PEErrorSexoH \
                                        ,SUM(A.U_PEErrorSexoM) U_PEErrorSexoM \
                                        from {database_name}.ft_Reprod_Galpon_Diario A \
                                        left join {database_name}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo \
                                        left join {database_name}.lk_mes C on B.idmes = C.idmes \
                                        left join {database_name}.PonderadoGalponMensual D on C.idmes = D.fecha and A.pk_lote = D.pk_lote and A.pk_galpon = D.pk_galpon \
                                        group by C.idmes,A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_etapa \
                                                ,A.pk_standard,A.pk_tipoproducto,A.pk_especieg,A.pk_estado,A.pk_administrador,A.pk_proveedor,A.pk_generacion \
                                                ,A.ComplexEntityNo,A.FechaNacimiento,A.FechaCap \
                                                order by MAX(A.pk_tiempo)")

print('carga df_ft_Reprod_Galpon_Mensual')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target13)     

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")


try:
    df_existentes13 = spark.read.format("parquet").load(path_target13)
    datos_existentes13 = True
    logger.info(f"Datos existentes de ft_Reprod_Galpon_Mensual cargados: {df_existentes13.count()} registros")
except:
    datos_existentes13 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Galpon_Mensual")


    
if datos_existentes13:
    existing_data13 = spark.read.format("parquet").load(path_target13)
    data_after_delete13 = existing_data13.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data13 = df_ft_Reprod_Galpon_Mensual.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str) )
    final_data13 = filtered_new_data13.union(data_after_delete13)                             
   
    cant_ingresonuevo13 = filtered_new_data13.count()
    cant_total13 = final_data13.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_MensualTemporal"
    }
    final_data13.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_MensualTemporal")
    
    
    #schema = existing_data.schema
    final_data13_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Galpon_MensualTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Mensual"
    }
    final_data13_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Mensual")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Galpon_Mensual : {cant_ingresonuevo13}")
    print(f"Total de registros en la tabla ft_Reprod_Galpon_Mensual : {cant_total13}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Galpon_MensualTemporal')
    print(f"Tabla ft_Reprod_Galpon_MensualTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Galpon_Mensual"
    }
    df_ft_Reprod_Galpon_Mensual.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Galpon_Mensual")
#Ponderados Lote
df_PonderadoLoteMensualTemp = spark.sql(f"select \
                                        date_format(fecha,'yyyyMM') fecha \
                                        ,complexentityno \
                                        ,pk_lote \
                                        ,sum(PesoHvoXSaldoH)PesoHvoXSaldoH \
                                        ,sum(SaldoHDePesoHvo)SaldoHDePesoHvo \
                                        ,nvl(sum(PesoHvoXSaldoH) / nullif(sum(SaldoHDePesoHvo),0),0) PesoHvoPond \
                                        ,nvl(sum(PesoHXSaldoH) / nullif(sum(SaldoHDePesoH),0),0) PesoHPond \
                                        ,nvl(sum(PesoMXSaldoM) / nullif(sum(SaldoMDePesoM),0),0) PesoMPond\
                                        ,nvl(sum(PorcHvoPisoXSaldoH) / nullif(sum(SaldoHDePorcHvoPiso),0),0) PorcHvoPisoPond \
                                        ,nvl(sum(UnifHXSaldoH) / nullif(sum(SaldoHDeUnifH),0),0) UnifHPond\
                                        ,nvl(sum(UnifMXSaldoM) / nullif(sum(SaldoMDeUnifM),0),0) UnifMPond\
                                        from    ( \
                                                select \
                                                     a.pk_tiempo\
                                                     ,b.fecha \
                                                    ,ComplexEntityNo \
                                                    ,pk_lote \
                                                    ,SaldoH \
                                                    ,SaldoM \
                                                    ,PesoHvo \
                                                    ,(PesoHvo * SaldoH) PesoHvoXSaldoH \
                                                    ,case when PesoHvo>0 then SaldoH else 0 end SaldoHDePesoHvo \
                                                    ,PesoH \
                                                    ,(PesoH*SaldoH) PesoHXSaldoH \
                                                    ,case when PesoH>0 then SaldoH else 0 end SaldoHDePesoH \
                                                    ,PesoM \
                                                    ,(PesoM*SaldoM) PesoMXSaldoM \
                                                    ,case when PesoM>0 then SaldoM else 0 end SaldoMDePesoM \
                                                    ,PorcHvoPiso \
                                                    ,(PorcHvoPiso*SaldoH) PorcHvoPisoXSaldoH \
                                                    ,case when PorcHvoPiso>0 then SaldoH else 0 end SaldoHDePorcHvoPiso \
                                                    ,UnifH \
                                                    ,(UnifH*SaldoH) UnifHXSaldoH \
                                                    ,case when UnifH>0 then SaldoH else 0 end SaldoHDeUnifH \
                                                    ,UnifM \
                                                    ,(UnifM*SaldoM) UnifMXSaldoM \
                                                    ,case when UnifM>0 then SaldoM else 0 end SaldoMDeUnifM \
                                                    from {database_name}.ft_Reprod_Lote_Diario a\
                                                    left join {database_name}.lk_tiempo b on a.pk_tiempo=b.pk_tiempo \
                                                ) A \
                                        group by date_format(fecha,'yyyyMM')  ,ComplexEntityNo,pk_lote")
#df_PonderadoLoteMensualTemp.createOrReplaceTempView("PonderadoLoteMensual")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoLoteMensual"
}
df_PonderadoLoteMensualTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoLoteMensual")
print('carga PonderadoLoteMensual')
#Inserta los últimos 4 meses de la tabla
#insert into [DMPECUARIO].[ft_Reprod_Lote_Mensual](
df_ft_Reprod_Lote_Mensual= spark.sql(f"select \
                                     MAX(A.pk_tiempo) pk_tiempo \
                                    ,C.idmes \
                                    ,A.pk_empresa \
                                    ,A.pk_division \
                                    ,A.pk_zona \
                                    ,A.pk_subzona \
                                    ,A.pk_plantel \
                                    ,A.pk_lote \
                                    ,A.pk_etapa \
                                    ,A.pk_standard \
                                    ,A.pk_tipoproducto \
                                    ,A.pk_especiel pk_especie \
                                    ,A.pk_estado \
                                    ,A.pk_administrador \
                                    ,A.pk_proveedor \
                                    ,A.pk_generacion \
                                    ,A.ComplexEntityNo \
                                    ,A.FechaNacimientoprom FechaNacimiento\
                                    ,MAX(A.FechaCap) FechaCap \
                                    ,MAX(A.Edadlotemodificado) Edad \
                                    ,MAX(A.AlojH) AlojH \
                                    ,MAX(A.AlojM) AlojM \
                                    ,SUM(A.MortH) MortH \
                                    ,SUM(A.MortM) MortM \
                                    ,SUM(A.BenefH) BenefH \
                                    ,SUM(A.BenefM) BenefM \
                                    ,SUM(A.TransIngH) TransIngH \
                                    ,SUM(A.TransSalH) TransSalH \
                                    ,SUM(A.TransIngM) TransIngM \
                                    ,SUM(A.TransSalM) TransSalM \
                                    ,MIN(A.SaldoH) SaldoH \
                                    ,MIN(A.SaldoM) SaldoM \
                                    ,SUM(A.SelecH) SelecH \
                                    ,SUM(A.SelecM) SelecM \
                                    ,SUM(A.ProdHT) ProdHT \
                                    ,SUM(A.ProdHI) ProdHI \
                                    ,MAX(A.HIGE) HIGE \
                                    ,MAX(D.PesoHvoPond) PesoHvo \
                                    ,MAX(D.PesoHPond) PesoH \
                                    ,MAX(D.PesoMPond) PesoM \
                                    ,MAX(D.UnifHPond) UnifH \
                                    ,MAX(D.UnifMPond) UnifM \
                                    ,MAX(A.CapH) CapH \
                                    ,MAX(A.CapM) CapM \
                                    ,SUM(A.ConsAlimH) ConsAlimH \
                                    ,SUM(A.ConsAlimM) ConsAlimM \
                                    ,SUM(A.ProdPoroso) ProdPoroso \
                                    ,SUM(A.ProdSucio) ProdSucio \
                                    ,SUM(A.ProdSangre) ProdSangre \
                                    ,SUM(A.ProdDeforme) ProdDeforme \
                                    ,SUM(A.ProdCascaraDebil) ProdCascaraDebil \
                                    ,SUM(A.ProdRoto) ProdRoto \
                                    ,SUM(A.ProdFarfara) ProdFarfara \
                                    ,SUM(A.ProdInvendible) ProdInvendible \
                                    ,SUM(A.ProdChico) ProdChico \
                                    ,SUM(A.ProdDobleYema) ProdDobleYema \
                                    ,SUM(A.U_PEErrorSexoH) U_PEErrorSexoH \
                                    ,SUM(A.U_PEErrorSexoM) U_PEErrorSexoM \
                                    from {database_name}.ft_Reprod_Lote_Diario A \
                                    left join {database_name}.lk_tiempo B on A.pk_tiempo = B.pk_tiempo \
                                    left join {database_name}.lk_mes C on B.idmes = C.idmes \
                                    left join {database_name}.PonderadoLoteMensual D on C.idmes = D.fecha and A.pk_lote = D.pk_lote \
                                    group by C.idmes,A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_etapa,A.pk_standard \
                                    ,A.pk_tipoproducto,A.pk_especiel,A.pk_estado,A.pk_administrador,A.pk_proveedor,A.pk_generacion \
                                    ,A.ComplexEntityNo,A.FechaNacimientoprom \
                                    order by MAX(A.pk_tiempo)")

print('carga df_ft_Reprod_Lote_Mensual')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target14)     

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y%m")

try:
    df_existentes14 = spark.read.format("parquet").load(path_target14)
    datos_existentes14 = True
    logger.info(f"Datos existentes de ft_Reprod_Lote_Mensual cargados: {df_existentes14.count()} registros")
except:
    datos_existentes14 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Lote_Mensual")



if datos_existentes14:
    existing_data14 = spark.read.format("parquet").load(path_target14)
    data_after_delete14 = existing_data14.filter(~((col("idmes") >= fecha_str)))
    filtered_new_data14 = df_ft_Reprod_Lote_Mensual.filter((col("idmes") >= fecha_str))
    final_data14 = filtered_new_data14.union(data_after_delete14)                             
   
    cant_ingresonuevo14 = filtered_new_data14.count()
    cant_total14 = final_data14.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_MensualTemporal"
    }
    final_data14.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_MensualTemporal")
    
    
    #schema = existing_data.schema
    final_data14_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Lote_MensualTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Mensual"
    }
    final_data14_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Mensual")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Lote_Mensual : {cant_ingresonuevo14}")
    print(f"Total de registros en la tabla ft_Reprod_Lote_Mensual : {cant_total14}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Lote_MensualTemporal')
    print(f"Tabla ft_Reprod_Lote_MensualTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Lote_Mensual"
    }
    df_ft_Reprod_Lote_Mensual.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Lote_Mensual")
#Maxima fecha de cada corral
df_MaximoCorralTemp=spark.sql(f"select max(pk_tiempo) pk_tiempomax,min(pk_tiempo) pk_tiempomin,pk_empresa,pk_plantel,pk_lote,pk_galpon,pk_sexo,ComplexEntityNo,pk_etapa \
                               from {database_name}.ft_Reprod_Diario A \
                               group by pk_empresa,pk_plantel,pk_lote,pk_galpon,pk_sexo,ComplexEntityNo,pk_etapa")
#df_MaximoCorralTemp.createOrReplaceTempView("MaximoCorral")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaximoCorral"
}
df_MaximoCorralTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaximoCorral")
print('carga MaximoCorral')
#Ponderado liquidaciones Diario
df_PonderadoLiquidacionDiarioTemp=spark.sql(f"select ComplexEntityNo,pk_etapa \
                                                    ,nvl(sum(PorcProdHTXSaldoH) / nullif(sum(SaldoHDePorcProdHT),0),0) PorcProdHTPond \
                                                    ,nvl(sum(StdPorcProdHTXSaldoH) / nullif(sum(SaldoHDeStdPorcProdHT),0),0) StdPorcProdHTPond \
                                                    ,nvl(sum(PorcProdHIXSaldoH) / nullif(sum(SaldoHDePorcProdHI),0),0) PorcProdHIPond \
                                                    ,nvl(sum(StdPorcProdHIXSaldoH) / nullif(sum(SaldoHDeStdPorcProdHI),0),0) StdPorcProdHIPond \
                                             from ( \
                                                    select pk_tiempo,ComplexEntityNo,pk_etapa \
                                                    ,SaldoH \
                                                    ,PorcProdHT \
                                                    ,(PorcProdHT * SaldoH) PorcProdHTXSaldoH \
                                                    ,case when PorcProdHT>0 then SaldoH else 0 end SaldoHDePorcProdHT \
                                                    ,StdPorcProdHT \
                                                    ,(StdPorcProdHT * SaldoH) StdPorcProdHTXSaldoH \
                                                    ,case when StdPorcProdHT>0 then SaldoH else 0 end SaldoHDeStdPorcProdHT \
                                                    ,PorcProdHI \
                                                    ,(PorcProdHI * SaldoH) PorcProdHIXSaldoH \
                                                    ,case when PorcProdHI>0 then SaldoH else 0 end SaldoHDePorcProdHI \
                                                    ,StdPorcProdHI \
                                                    ,(StdPorcProdHI * SaldoH) StdPorcProdHIXSaldoH \
                                                    ,case when StdPorcProdHI>0 then SaldoH else 0 end SaldoHDeStdPorcProdHI \
                                                    from {database_name}.ft_Reprod_Diario \
                                                  ) A \
                                             group by ComplexEntityNo,pk_etapa")
#df_PonderadoLiquidacionDiarioTemp.createOrReplaceTempView("PonderadoLiquidacionDiario")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoLiquidacionDiario"
}
df_PonderadoLiquidacionDiarioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoLiquidacionDiario")
print('carga PonderadoLiquidacionDiario')
#-Maxima edad entera 
df_MaximaEdadDiarioTemp=spark.sql(f"select complexentityno,max(edad) EdadMax \
                                    from {database_name}.ft_Reprod_Diario \
                                    where cast(edad as varchar(20)) like '%.000000' \
                                    group by complexentityno")
#df_MaximaEdadDiarioTemp.createOrReplaceTempView("MaximaEdadDiario")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaximaEdadDiario"
}
df_MaximaEdadDiarioTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaximaEdadDiario")
print('carga MaximaEdadDiario')
df_ft_Reprod_Liquid_Diario = spark.sql(f"select max(A.pk_tiempo) pk_tiempo,max(a.fecha)fecha ,A.pk_empresa ,A.pk_division ,A.pk_zona ,A.pk_subzona ,A.pk_plantel \
                                              ,A.pk_lote ,A.pk_galpon ,A.pk_sexo ,A.pk_etapa ,A.pk_standard ,A.pk_producto ,A.pk_tipoproducto \
                                              ,A.pk_especie ,A.pk_estado ,A.pk_administrador ,A.pk_proveedor ,A.pk_generacion ,A.ComplexEntityNo \
                                              ,A.FechaNacimiento ,A.FechaCap ,max(A.Edad) Edad ,max(A.AlojH) AlojH ,max(A.AlojM) AlojM \
                                              ,sum(A.MortH) MortH ,sum(A.MortM) MortM \
                                              ,case when A.pk_etapa = 1 then sum(A.MortH)/nullif((max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) \
                                              else sum(A.MortH)/nullif((max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) end PorcMortH \
                                              ,case when A.pk_etapa = 1 then sum(A.MortM)/nullif((max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) \
                                              else sum(A.MortM)/nullif((max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) end PorcMortM \
                                              ,Sum(A.BenefH) BenefH \
                                              ,sum(A.BenefM) BenefM \
                                              ,sum(A.TransIngH)TransIngH \
                                              ,sum(A.TransSalH)TransSalH \
                                              ,sum(A.TransIngM)TransIngM \
                                              ,sum(A.TransSalM)TransSalM \
                                              ,case when A.pk_etapa = 1 then (max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) \
                                              else (max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) end SaldoH \
                                              ,case when A.pk_etapa = 1 then (max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) \
                                              else (max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) end SaldoM \
                                              ,(case when A.pk_etapa = 1 then (max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) \
                                              else (max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) end) / \
                                              NULLIF((case when A.pk_etapa = 1 then (max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) \
                                              else (max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) end)*1.0,0) PorcMH \
                                              ,sum(A.SelecH) SelecH \
                                              ,sum(A.SelecM) SelecM \
                                              ,case when A.pk_etapa = 1 then sum(A.ConsAlimH)/nullif((max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) \
                                              else sum(A.ConsAlimH)/nullif((max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) end GADH \
                                              ,case when A.pk_etapa = 1 then sum(A.ConsAlimM)/nullif((max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) \
                                              else sum(A.ConsAlimM)/nullif((max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) end GADM \
                                              ,sum(A.ProdHT) ProdHT \
                                              ,max(PorcProdHTPond) PorcProdHT \
                                              ,max(StdPorcProdHTPond) StdPorcProdHT \
                                              ,sum(A.ProdHI) ProdHI \
                                              ,max(PorcProdHIPond) PorcProdHI \
                                              ,max(StdPorcProdHIPond) StdPorcProdHI \
                                              ,max(A1.HTGE) HTGE \
                                              ,max(A1.HIGE) HIGE \
                                              ,max(A1.StdHTGEAcum) StdHTGEAcum \
                                              ,max(A1.StdHIGEAcum) StdHIGEAcum \
                                              ,max(A.PesoH) PesoH \
                                              ,max(A.PesoM) PesoM \
                                              ,max(A1.UnifH) UnifH \
                                              ,max(A1.UnifM) UnifM \
                                              ,max(A1.PorcViabProdH) PorcViabProdH \
                                              ,max(A1.PorcViabProdM) PorcViabProdM \
                                              ,max(A.CapH) CapH \
                                              ,max(A.CapM) CapM \
                                              ,sum(A.U_PEErrorSexoH) U_PEErrorSexoH \
                                              ,sum(A.U_PEErrorSexoM) U_PEErrorSexoM \
                                              ,A.ListaPadre \
                                              from {database_name}.ft_Reprod_Diario A \
                                              left join {database_name}.MaximoCorral B on A.complexentityno = B.complexentityno and A.pk_etapa = B.pk_etapa \
                                              left join {database_name}.PonderadoLiquidacionDiario C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_etapa = C.pk_etapa \
                                              left join {database_name}.MaximaEdadDiario D on A.ComplexEntityNo = D.ComplexEntityNo \
                                              left join {database_name}.ft_Reprod_Diario A1 on A1.complexentityno = b.complexentityno and A1.pk_tiempo = B.pk_tiempomax and A1.pk_etapa = B.pk_etapa \
                                              where A.edad <= D.EdadMax \
                                              group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_sexo \
                                              ,A.pk_standard,A.pk_producto,A.pk_tipoproducto,A.pk_especie,A.pk_estado,A.pk_administrador \
                                              ,A.pk_proveedor,A.pk_generacion,A.ComplexEntityNo,A.FechaNacimiento,A.FechaCap,A.ListaPadre \
                                              ,b.complexentityno,b.pk_tiempomax,b.pk_etapa,A.pk_etapa")
print('carga df_ft_Reprod_Liquid_Diario')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target15)   

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes15 = spark.read.format("parquet").load(path_target15)
    datos_existentes15 = True
    logger.info(f"Datos existentes de ft_Reprod_Liquid_Diario cargados: {df_existentes15.count()} registros")
except:
    datos_existentes15 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Liquid_Diario")



if datos_existentes15:
    existing_data15 = spark.read.format("parquet").load(path_target15)
    data_after_delete15 = existing_data15.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data15 = df_ft_Reprod_Liquid_Diario.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data15 = filtered_new_data15.union(data_after_delete15)                             
   
    cant_ingresonuevo15 = filtered_new_data15.count()
    cant_total15 = final_data15.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Liquid_DiarioTemporal"
    }
    final_data15.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_DiarioTemporal")
    
    
    #schema = existing_data.schema
    final_data15_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Liquid_DiarioTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Liquid_Diario"
    }
    final_data15_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_Diario")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Liquid_Diario : {cant_ingresonuevo15}")
    print(f"Total de registros en la tabla ft_Reprod_Liquid_Diario : {cant_total15}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Liquid_DiarioTemporal')
    print(f"Tabla ft_Reprod_Liquid_DiarioTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Liquid_Diario"
    }
    df_ft_Reprod_Liquid_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_Diario")
#Maxima edad entera 
df_MaximaEdadGalponTemp = spark.sql(f"select complexentityno,max(edad) EdadMax \
                                     from {database_name}.ft_Reprod_Galpon_Diario \
                                     where cast(edad as varchar(20)) like '%.000000' \
                                     group by complexentityno")
#df_MaximaEdadGalponTemp.createOrReplaceTempView("MaximaEdadGalpon")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaximaEdadGalpon"
}
df_MaximaEdadGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaximaEdadGalpon")
print('carga MaximaEdadGalpon')
#Maxima fecha de cada Galpon
df_MaximoGalponTemp = spark.sql(f"select max(pk_tiempo) pk_tiempomax,min(pk_tiempo) pk_tiempomin,pk_empresa,pk_plantel,pk_lote,pk_galpon,ComplexEntityNo,pk_etapa \
                                 from {database_name}.ft_Reprod_Galpon_Diario A \
                                 group by pk_empresa,pk_plantel,pk_lote,pk_galpon,ComplexEntityNo,pk_etapa")
#df_MaximoGalponTemp.createOrReplaceTempView("MaximoGalpon")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/MaximoGalpon"
}
df_MaximoGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.MaximoGalpon")
print('carga MaximoGalpon')
#Ponderado liquidaciones Galpon
df_PonderadoLiquidacionGalponTemp = spark.sql(f"select ComplexEntityNo,pk_etapa \
                                                ,nvl(sum(PorcProdHTXSaldoH) / nullif(sum(SaldoHDePorcProdHT),0),0) PorcProdHTPond \
                                                ,nvl(sum(StdPorcProdHTXSaldoH) / nullif(sum(SaldoHDeStdPorcProdHT),0),0) StdPorcProdHTPond \
                                                ,nvl(sum(PorcProdHIXSaldoH) / nullif(sum(SaldoHDePorcProdHI),0),0) PorcProdHIPond \
                                                ,nvl(sum(StdPorcProdHIXSaldoH) / nullif(sum(SaldoHDeStdPorcProdHI),0),0) StdPorcProdHIPond \
                                              from ( \
                                                    select pk_tiempo,ComplexEntityNo,pk_etapa,SaldoH,PorcProdHT \
                                                    ,(PorcProdHT * SaldoH) PorcProdHTXSaldoH \
                                                    ,case when PorcProdHT>0 then SaldoH else 0 end SaldoHDePorcProdHT \
                                                    ,StdPorcProdHT \
                                                    ,(StdPorcProdHT * SaldoH) StdPorcProdHTXSaldoH \
                                                    ,case when StdPorcProdHT>0 then SaldoH else 0 end SaldoHDeStdPorcProdHT \
                                                    ,PorcProdHI \
                                                    ,(PorcProdHI * SaldoH) PorcProdHIXSaldoH \
                                                    ,case when PorcProdHI>0 then SaldoH else 0 end SaldoHDePorcProdHI \
                                                    ,StdPorcProdHI \
                                                    ,(StdPorcProdHI * SaldoH) StdPorcProdHIXSaldoH \
                                                    ,case when StdPorcProdHI>0 then SaldoH else 0 end SaldoHDeStdPorcProdHI \
                                                    from {database_name}.ft_Reprod_Galpon_Diario \
                                                   ) A \
                                                group by ComplexEntityNo ,pk_etapa")
#df_PonderadoLiquidacionGalponTemp.createOrReplaceTempView("PonderadoLiquidacionGalpon")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PonderadoLiquidacionGalpon"
}
df_PonderadoLiquidacionGalponTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.PonderadoLiquidacionGalpon")
print('carga PonderadoLiquidacionGalpon')
#Se crea una tabla temporal para hallar los encasetados
df_Liquidacion2Temp1 = spark.sql(f"select \
                                 max(X.pk_tiempo) pk_tiempo \
                                ,X.pk_etapa \
                                ,max(X.Edad) Edad \
                                ,X.ComplexEntityNo \
                                ,min(X.FechaIni) FechaIni \
                                ,max(X.SaldoH) SaldoHMax \
                                ,min(X.SaldoH) SaldoHMin \
                                ,max(X.SaldoM) SaldoMMax \
                                ,min(X.SaldoM) SaldoMMin \
                                ,MAX(A.MortH) MortH1Sem \
                                ,MAX(B.MortH) MortH2Sem \
                                ,MAX(A.MortM) MortM1Sem \
                                ,(max(X.SaldoH) + MAX(A.MortH)) EncasetadaH \
                                ,(max(X.SaldoM) + MAX(A.MortM)) EncasetadaM \
                                ,MAX(C.UnifH) Unif12Sem \
                                ,MAX(D.UnifH) Unif18Sem \
                                ,MAX(E.UnifH) Unif23Sem \
                                from {database_name}.ft_Reprod_Galpon_Semana X \
                                left join {database_name}.ft_Reprod_Galpon_Semana A on A.complexentityno = X.complexentityno and A.pk_etapa = X.pk_etapa and A.edad = 1.0 \
                                left join {database_name}.ft_Reprod_Galpon_Semana B on B.complexentityno = X.complexentityno and B.pk_etapa = X.pk_etapa and B.edad = 2.0 \
                                left join {database_name}.ft_Reprod_Galpon_Semana C on C.complexentityno = X.complexentityno and C.pk_etapa = X.pk_etapa and C.edad = 12.0 \
                                left join {database_name}.ft_Reprod_Galpon_Semana D on D.complexentityno = X.complexentityno and D.pk_etapa = X.pk_etapa and D.edad = 18.0 \
                                left join {database_name}.ft_Reprod_Galpon_Semana E on E.complexentityno = X.complexentityno and E.pk_etapa = X.pk_etapa and E.edad = 23.0 \
                                where X.pk_etapa = 1 \
                                group by X.pk_etapa,X.ComplexEntityNo")
print('df_Liquidacion2Temp1')
df_Liquidacion2Temp2 = spark.sql(f"select \
                                    max(X.pk_tiempo) pk_tiempo \
                                    ,X.pk_etapa \
                                    ,max(X.Edad) Edad \
                                    ,X.ComplexEntityNo \
                                    ,min(X.FechaIni) FechaIni \
                                    ,max(X.SaldoH) SaldoHMax \
                                    ,min(X.SaldoH) SaldoHMin \
                                    ,max(X.SaldoM) SaldoMMax \
                                    ,min(X.SaldoM) SaldoMMin \
                                    ,MAX(A.MortH) MortH1Sem \
                                    ,0 MortH2Sem \
                                    ,MAX(A.MortM) MortM1Sem \
                                    ,max(X.SaldoH) +  MAX(A.MortH) EncasetadaH \
                                    ,max(X.SaldoM) +  MAX(A.MortM) EncasetadaM \
                                    ,0 Unif12Sem \
                                    ,0 Unif18Sem \
                                    ,0 Unif23Sem \
                                    from {database_name}.ft_Reprod_Galpon_Semana X \
                                    left join {database_name}.ft_Reprod_Galpon_Semana A on A.complexentityno = X.complexentityno and A.pk_etapa = X.pk_etapa and A.RowNumber = 1.0 \
                                    where X.pk_etapa = 3 \
                                    group by X.pk_etapa,X.ComplexEntityNo")

df_Liquidacion2 = df_Liquidacion2Temp1.union(df_Liquidacion2Temp2)
#df_Liquidacion2.createOrReplaceTempView("Liquidacion2")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Liquidacion2"
}
df_Liquidacion2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.Liquidacion2")
print('carga Liquidacion2')

#Inserta los últimos 4 meses de la tabla
#insert into [DMPECUARIO].[ft_Reprod_Liquid_Galpon]
df_ft_Reprod_Liquid_Galpon =spark.sql(f"select max(A.pk_tiempo) pk_tiempo,max(A.fecha) fecha,A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_etapa \
,A.pk_standard,A.pk_tipoproducto,A.pk_especieg pk_especie,A.pk_estado,A.pk_administrador,A.pk_proveedor,A.pk_generacion,A.ComplexEntityNo,A.FechaNacimiento \
,A.FechaCap,max(A.Edad) Edad,max(A.AlojH) AlojH,max(A.AlojM) AlojM, \
sum(A.MortH) MortH \
,sum(A.MortM) MortM \
,case when A.pk_etapa = 1 then sum(A.MortH)*1.0/nullif((max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) \
else sum(A.MortH)*1.0/nullif((max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) end PorcMortH \
,case when A.pk_etapa = 1 then sum(A.MortM)*1.0/nullif((max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) \
else sum(A.MortM)*1.0/nullif((max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) end PorcMortM \
,Sum(A.BenefH) BenefH \
,sum(A.BenefM) BenefM \
,sum(A.TransIngH)TransIngH \
,sum(A.TransSalH)TransSalH \
,sum(A.TransIngM)TransIngM \
,sum(A.TransSalM)TransSalM \
,case when A.pk_etapa = 1 then (max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) \
else (max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) end SaldoH \
,case when A.pk_etapa = 1 then (max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) \
else (max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) end SaldoM \
,(case when A.pk_etapa = 1 then (max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) \
else (max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)) end) /  \
NULLIF((case when A.pk_etapa = 1 then (max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) \
else (max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)) end)*1.0,0) PorcMH \
,sum(A.SelecH) SelecH \
,sum(A.SelecM) SelecM \
,case when A.pk_etapa = 1 then sum(A.ConsAlimH)/nullif((max(A.AlojH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) \
else sum(A.ConsAlimH)/nullif((max(A.CapH) - sum(A.MortH) - Sum(A.BenefH) - sum(A.TransSalH) +  sum(A.TransIngH)),0) end GADH \
,case when A.pk_etapa = 1 then sum(A.ConsAlimM)/nullif((max(A.AlojM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) \
else sum(A.ConsAlimM)/nullif((max(A.CapM) - sum(A.MortM) - Sum(A.BenefM) - sum(A.TransSalM) +  sum(A.TransIngM)),0) end GADM \
,sum(A.ProdHT) ProdHT \
,max(PorcProdHTPond) PorcProdHT \
,max(StdPorcProdHTPond) StdPorcProdHT \
,sum(A.ProdHI) ProdHI \
,max(PorcProdHIPond) PorcProdHI \
,max(StdPorcProdHIPond) StdPorcProdHI \
,max(F.HTGE) HTGE \
,max(F.HIGE) HIGE \
,max(F.StdHTGEAcum) StdHTGEAcum \
,max(F.StdHIGEAcum) StdHIGEAcum \
,max(A.PesoH) PesoH \
,max(A.PesoM) PesoM \
,max(F.UnifH) UnifH \
,max(F.UnifM) UnifM \
,max(F.PorcViabProdH) PorcViabProdH \
,max(F.PorcViabProdM) PorcViabProdM \
,max(A.CapH) CapH \
,max(A.CapM) CapM \
,sum(A.U_PEErrorSexoH) U_PEErrorSexoH \
,sum(A.U_PEErrorSexoM) U_PEErrorSexoM \
,max(A.StdPesoH) StdPesoH \
,max(A.StdPesoM) StdPesoM \
,sum(A.ConsAlimH) ConsAlimH \
,sum(A.ConsAlimM) ConsAlimM \
,D.FechaIni \
,max(D.EncasetadaH) EncasetadaH \
,max(D.EncasetadaM) EncasetadaM \
,((max(D.MortH1Sem) + max(D.MortH2Sem))*1.0 / nullif(max(D.EncasetadaH),0))*100 PorcMortH2Sem \
,((sum(A.U_PEErrorSexoH)*1.0)/nullif(max(D.EncasetadaH),0)) *100 PorcU_PEErrorSexoH \
,(((sum(A.MortH)*1.0)/nullif(max(D.EncasetadaH),0))*100)-(((sum(A.U_PEErrorSexoH)*1.0)/nullif(max(D.EncasetadaH),0)) *100) PorcMortHFinal \
,(sum(A.SelecH)*1.0/nullif(max(D.EncasetadaH),0))*100 PorcEncasetadaSeleccH \
,(max(D.SaldoHMin)*1.0/nullif(max(D.EncasetadaH),0))*100 PorcEncasetadaSaldoH \
,((sum(A.ConsAlimH) + sum(A.ConsAlimM)) * 1.0) / nullif(max(D.SaldoHMin),0) PorcConsAlimHMSaldoH \
,sum(A.ConsAlimH)/nullif(max(D.SaldoHMin),0) PorcConsAlimH \
,sum(A.ConsAlimM)/nullif(max(D.SaldoMMin),0) PorcConsAlimM \
,max(D.Unif12Sem) Unif12Sem \
,max(D.Unif18Sem) Unif18Sem \
,max(D.Unif23Sem) Unif23Sem \
,((sum(A.ConsAlimH) + sum(A.ConsAlimM)) * 1.0) / nullif((max(D.SaldoHMin) + max(D.SaldoMMin)),0) ConsAlimAve \
from {database_name}.ft_Reprod_Galpon_Diario A \
left join {database_name}.MaximoGalpon B on A.complexentityno = B.complexentityno and A.pk_etapa = B.pk_etapa \
left join {database_name}.PonderadoLiquidacionGalpon C on A.ComplexEntityNo = C.ComplexEntityNo and A.pk_etapa = C.pk_etapa \
left join {database_name}.Liquidacion2 D on A.ComplexEntityNo = D.ComplexEntityNo and A.pk_etapa = D.pk_etapa \
left  join {database_name}.MaximaEdadGalpon E on A.ComplexEntityNo = E.ComplexEntityNo \
left join {database_name}.ft_Reprod_Galpon_Diario F on F.complexentityno = b.complexentityno and F.pk_tiempo = B.pk_tiempomax and F.pk_etapa = B.pk_etapa \
where A.Edad <= E.EdadMax \
group by A.pk_empresa,A.pk_division,A.pk_zona,A.pk_subzona,A.pk_plantel,A.pk_lote,A.pk_galpon,A.pk_standard,A.pk_tipoproducto \
,A.pk_especieg,A.pk_estado,A.pk_administrador,A.pk_proveedor,A.pk_generacion,A.ComplexEntityNo,A.FechaNacimiento,A.FechaCap \
,b.complexentityno,b.pk_tiempomax,b.pk_etapa,A.pk_etapa,D.FechaIni")
print('df_ft_Reprod_Liquid_Galpon')
# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target16)  

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes16 = spark.read.format("parquet").load(path_target16)
    datos_existentes16 = True
    logger.info(f"Datos existentes de ft_Reprod_Liquid_Galpon cargados: {df_existentes16.count()} registros")
except:
    datos_existentes16 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_Liquid_Galpon")



if datos_existentes16:
    existing_data16 = spark.read.format("parquet").load(path_target16)
    data_after_delete16 = existing_data16.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data16 = df_ft_Reprod_Liquid_Galpon.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data16 = filtered_new_data16.union(data_after_delete16)                             
   
    cant_ingresonuevo16 = filtered_new_data16.count()
    cant_total16 = final_data16.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Liquid_GalponTemporal"
    }
    final_data16.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_GalponTemporal")
    
    
    #schema = existing_data.schema
    final_data16_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_Liquid_GalponTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Liquid_Galpon"
    }
    final_data16_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_Galpon")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_Liquid_Galpon : {cant_ingresonuevo16}")
    print(f"Total de registros en la tabla ft_Reprod_Liquid_Galpon : {cant_total16}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_Liquid_GalponTemporal')
    print(f"Tabla ft_Reprod_Liquid_GalponTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_Liquid_Galpon"
    }
    df_ft_Reprod_Liquid_Galpon.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_Liquid_Galpon")
df_ConsumoAlimentoReprodTemp1 = spark.sql(f"SELECT \
 nvl(MO.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha = cast('1899-11-30' as date)))pk_tiempo \
 ,MO.fecha\
,1 pk_empresa \
,nvl(MO.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
,nvl(MO.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
,nvl(MO.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona \
,nvl(MO.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
,nvl(MO.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
,nvl(MO.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
,nvl(MO.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo \
,nvl(MO.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard \
,nvl(MO.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(LPC.pk_productoconsumo,(select pk_productoconsumo from {database_name}.lk_productoconsumo where cproductoconsumo='0')) pk_productoconsumo \
,nvl(MO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name}.lk_grupoconsumo where cgrupoconsumo=0)) pk_grupoconsumo \
,nvl(LSG.pk_subgrupoconsumo,(select pk_subgrupoconsumo from {database_name}.lk_subgrupoconsumo where csubgrupoconsumo=0)) pk_subgrupoconsumo \
,nvl(MO.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(MO.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(MO.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(MO.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador \
,nvl(MO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,MO.pk_diasvida \
,MO.pk_semanavida \
,MO.ComplexEntityNo \
,MO.FechaNacimiento Nacimiento \
,MO.Edad AS Edad \
,MO.FeedConsumedF FeedConsumed \
,nvl(MO.pk_alimentof,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) pk_alimento \
,nvl(LFR.pk_formula,(select pk_formula from {database_name}.lk_formula where cformula='0')) pk_formula \
,MO.Price Valor \
FROM {database_name}.Reprod_Reproductora_upd2 MO  \
LEFT JOIN {database_name}.lk_formula LFR ON LFR.ProteinProductsIRN = cast(MO.ProteinProductsIRN_VFFF as varchar(50)) \
LEFT JOIN {database_name}.lk_productoconsumo LPC ON LFR.ProteinProductsIRN = LPC.IRN \
LEFT JOIN {database_name}.lk_subgrupoconsumo LSG ON LPC.pk_subgrupoconsumo = LSG.pk_subgrupoconsumo \
WHERE MO.FeedConsumedF <> 0")
print('carga df_ConsumoAlimentoReprodTemp1')
df_ConsumoAlimentoReprodTemp2 = spark.sql(f"SELECT \
nvl(MO.pk_tiempo,(select pk_tiempo from {database_name}.lk_tiempo where fecha = '1899-11-30'))pk_tiempo \
,MO.fecha\
,1 pk_empresa \
,nvl(MO.pk_division,(select pk_division from {database_name}.lk_division where cdivision=0)) pk_division \
,nvl(MO.pk_zona,(select pk_zona from {database_name}.lk_zona where czona='0')) pk_zona \
,nvl(MO.pk_subzona,(select pk_subzona from {database_name}.lk_subzona where csubzona='0')) pk_subzona \
,nvl(MO.pk_plantel,(select pk_plantel from {database_name}.lk_plantel where cplantel='0')) pk_plantel \
,nvl(MO.pk_lote,(select pk_lote from {database_name}.lk_lote where clote='0')) pk_lote \
,nvl(MO.pk_galpon,(select pk_galpon from {database_name}.lk_galpon where cgalpon='0')) pk_galpon \
,nvl(MO.pk_sexo,(select pk_sexo from {database_name}.lk_sexo where csexo=0)) pk_sexo \
,nvl(MO.pk_standard,(select pk_standard from {database_name}.lk_standard where cstandard='0')) pk_standard \
,nvl(MO.pk_producto,(select pk_producto from {database_name}.lk_producto where cproducto='0')) pk_producto \
,nvl(LPC.pk_productoconsumo,(select pk_productoconsumo from {database_name}.lk_productoconsumo where cproductoconsumo='0')) pk_productoconsumo \
,nvl(MO.pk_grupoconsumo,(select pk_grupoconsumo from {database_name}.lk_grupoconsumo where cgrupoconsumo=0)) pk_grupoconsumo \
,nvl(LSG.pk_subgrupoconsumo,(select pk_subgrupoconsumo from {database_name}.lk_subgrupoconsumo where csubgrupoconsumo=0)) pk_subgrupoconsumo \
,nvl(MO.pk_tipoproducto,(select pk_tipoproducto from {database_name}.lk_tipoproducto where ntipoproducto='Sin Tipo Producto')) pk_tipoproducto \
,nvl(MO.pk_especie,(select pk_especie from {database_name}.lk_especie where cespecie='0')) pk_especie \
,nvl(MO.pk_estado,(select pk_estado from {database_name}.lk_estado where cestado=0)) pk_estado \
,nvl(MO.pk_administrador,(select pk_administrador from {database_name}.lk_administrador where cadministrador='0'))pk_administrador \
,nvl(MO.pk_proveedor,(select pk_proveedor from {database_name}.lk_proveedor where cproveedor=0)) pk_proveedor \
,MO.pk_diasvida \
,MO.pk_semanavida \
,MO.ComplexEntityNo \
,MO.FechaNacimiento Nacimiento \
,MO.Edad AS Edad \
,MO.FeedConsumedF FeedConsumed \
,nvl(MO.pk_alimentof,(select pk_alimento from {database_name}.lk_alimento where calimento='0')) pk_alimento \
,nvl(LFR.pk_formula,(select pk_formula from {database_name}.lk_formula where cformula='0')) pk_formula \
,MO.Price Valor \
FROM {database_name}.Reprod_Reproductora_upd2 MO \
LEFT JOIN {database_name}.lk_formula LFR ON LFR.ProteinProductsIRN = cast(MO.ProteinProductsIRN_VFFM as varchar(50)) \
LEFT JOIN {database_name}.lk_productoconsumo LPC ON LFR.ProteinProductsIRN = LPC.IRN \
LEFT JOIN {database_name}.lk_subgrupoconsumo LSG ON LPC.pk_subgrupoconsumo = LSG.pk_subgrupoconsumo \
WHERE MO.FeedConsumedM <> 0")

df_ConsumoAlimentoReprodTemp = df_ConsumoAlimentoReprodTemp1.union(df_ConsumoAlimentoReprodTemp2)
#df_ConsumoAlimentoReprodTemp.createOrReplaceTempView("ConsumoAlimentoReprodTemp")
print('carga df_ConsumoAlimentoReprodTemp2')
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsumoAlimentoReprodTemp"
}
df_ConsumoAlimentoReprodTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsumoAlimentoReprodTemp")
print('carga ConsumoAlimentoReprodTemp')

df_ConsumoAlimentoReprodTemp_upd = spark.sql(f"SELECT \
 pk_tiempo \
,fecha\
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
,pk_productoconsumo \
,pk_grupoconsumo \
,pk_subgrupoconsumo \
,pk_tipoproducto \
,pk_especie \
,pk_estado \
,pk_administrador \
,pk_proveedor \
,CASE WHEN Edad in (0,-1) THEN 2 \
      WHEN Edad <= 0 AND SUBSTRING(ComplexEntityNo,1,1) = 'V' THEN 2 \
      ELSE pk_diasvida END pk_diasvida \
,CASE WHEN Edad in (0,-1) THEN 2 \
      WHEN Edad <= 0 AND SUBSTRING(ComplexEntityNo,1,1) = 'V' THEN 2 \
      ELSE pk_semanavida END pk_semanavida \
,ComplexEntityNo \
,Nacimiento \
,Edad \
,FeedConsumed \
,pk_alimento \
,pk_formula \
,Valor \
from ConsumoAlimentoReprodTemp")

#df_ConsumoAlimentoReprodTemp_upd.createOrReplaceTempView("ConsumoAlimentoReprodTemp_upd")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsumoAlimentoReprodTemp_upd"
}
df_ConsumoAlimentoReprodTemp_upd.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsumoAlimentoReprodTemp_upd")
print('carga ConsumoAlimentoReprodTemp_upd')
df_ConsumoAlimentoReprod = spark.sql(f"SELECT \
 MAX(pk_tiempo) pk_tiempo \
,MAX(fecha) fecha \
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
,pk_productoconsumo \
,pk_grupoconsumo \
,pk_subgrupoconsumo \
,pk_tipoproducto \
,pk_especie \
,pk_estado \
,pk_administrador \
,pk_proveedor \
,pk_diasvida \
,pk_semanavida \
,complexentityno \
,nacimiento \
,MAX(edad) edad \
,SUM(FeedConsumed) ConsDia \
,pk_alimento \
,pk_formula \
,SUM(Valor) Valor \
FROM {database_name}.ConsumoAlimentoReprodTemp_upd \
GROUP BY pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo \
,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo,pk_tipoproducto \
,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,pk_semanavida,complexentityno \
,nacimiento,pk_alimento,pk_formula")

#df_ConsumoAlimentoReprod.createorReplaceTempView("ConsumoAlimentoReprod")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ConsumoAlimentoReprod"
}
df_ConsumoAlimentoReprod.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ConsumoAlimentoReprod")
print('carga ConsumoAlimentoReprod')

df_ft_Reprod_ConsumoAlimento =spark.sql(f"SELECT * FROM {database_name}.ConsumoAlimentoReprod WHERE ConsDia <> 0 \
                                            order by pk_tiempo desc")
print('carga df_ft_Reprod_ConsumoAlimento')

# Verificar si la tabla gold ya existe
#gold_table = spark.read.format("parquet").load(path_target17)   
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

fechaactual = datetime.now().replace(day=1)
fecha_menos_doce_meses = fechaactual - relativedelta(months=36)
fecha_str = fecha_menos_doce_meses.strftime("%Y-%m-%d")

try:
    df_existentes17 = spark.read.format("parquet").load(path_target17)
    datos_existentes17 = True
    logger.info(f"Datos existentes de ft_Reprod_ConsumoAlimento cargados: {df_existentes17.count()} registros")
except:
    datos_existentes17 = False
    logger.info("No se encontraron datos existentes en ft_Reprod_ConsumoAlimento")



if datos_existentes17:
    existing_data17 = spark.read.format("parquet").load(path_target17)
    data_after_delete17 = existing_data17.filter(~((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data17 = df_ft_Reprod_ConsumoAlimento.filter((date_format(col("fecha"),"yyyy-MM-dd") >= fecha_str))
    final_data17 = filtered_new_data17.union(data_after_delete17)                             
   
    cant_ingresonuevo17 = filtered_new_data17.count()
    cant_total17 = final_data17.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_ConsumoAlimentoTemporal"
    }
    final_data17.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_ConsumoAlimentoTemporal")
    
    
    #schema = existing_data.schema
    final_data17_2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_Reprod_ConsumoAlimentoTemporal")
            
            
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_ConsumoAlimento"
    }
    final_data17_2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_ConsumoAlimento")
            
    print(f"agrega registros nuevos a la tabla ft_Reprod_ConsumoAlimento : {cant_ingresonuevo17}")
    print(f"Total de registros en la tabla ft_Reprod_ConsumoAlimento : {cant_total17}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    #glueContext.purge_table("{database_name}", "ft_mortalidadtemporal", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name, Name='ft_Reprod_ConsumoAlimentoTemporal')
    print(f"Tabla ft_Reprod_ConsumoAlimentoTemporal eliminada correctamente de la base de datos '{database_name}'.")
        
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_Reprod_ConsumoAlimento"
    }
    df_ft_Reprod_ConsumoAlimento.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name}.ft_Reprod_ConsumoAlimento")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()