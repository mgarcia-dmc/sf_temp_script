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
JOB_NAME = "nt_prd_actualiza_consumos_gold"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
# Parámetros de entrada global
bucket_name_target = "ue1stgprodas3dtl005-gold"
bucket_name_source = "ue1stgprodas3dtl005-silver"
bucket_name_prdmtech = "UE1STGPRODAS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES/"
database_name_tmp = "bi_sf_tmp"
database_name_br ="mtech_prd_sf_br"
database_name_si ="mtech_prd_sf_si"
database_name_gl ="mtech_prd_sf_gl"
print('cargando rutas')
## FechaAlojamientoPond
df_TablaFNXPobIni= spark.sql(f"""
select pk_lote,ComplexEntityNo,PobInicial,FechaAlojamiento,
datediff(to_date(FechaAlojamiento,'yyyyMMdd'), DATE '1900-01-01') AS NumFechaAlojamiento,
datediff(to_date(FechaAlojamiento,'yyyyMMdd'), DATE '1900-01-01') * cast(PobInicial as BIGINT) AS NumFNXPobIni
from {database_name_gl}.ft_consolidado_Corral 
where pk_empresa = 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaFNXPobIni"
}
df_TablaFNXPobIni.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaFNXPobIni")
print('carga TablaFNXPobIni',df_TablaFNXPobIni.count())
df_TablaPondFechaAloj= spark.sql(f"""
select pk_lote
,substring(complexentityno,1,(LENGTH(complexentityno)-3)) ComplexEntityNo
,SUM(COALESCE(PobInicial,0)) PobInicial
,SUM(COALESCE(NumFNXPobIni,0)) NumFNXPobIni
,FLOOR(CASE WHEN SUM(COALESCE(PobInicial,0)) = 0 THEN 0 ELSE SUM(COALESCE(NumFNXPobIni,0))/SUM(COALESCE(PobInicial,0)) END) NumFNPond
from {database_name_tmp}.TablaFNXPobIni group by pk_lote,substring(complexentityno,1,(LENGTH(complexentityno)-3)) 
union 
select pk_lote
,substring(complexentityno,1,(LENGTH(complexentityno)-6)) ComplexEntityNo
,SUM(COALESCE(PobInicial,0)) PobInicial
,SUM(COALESCE(NumFNXPobIni,0)) NumFNXPobIni
,FLOOR(CASE WHEN SUM(COALESCE(PobInicial,0)) = 0 THEN 0 ELSE SUM(COALESCE(NumFNXPobIni,0))/SUM(COALESCE(PobInicial,0)) END) NumFNPond
from {database_name_tmp}.TablaFNXPobIni group by pk_lote,substring(complexentityno,1,(LENGTH(complexentityno)-6)) 
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaPondFechaAloj"
}
df_TablaPondFechaAloj.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaPondFechaAloj")
print('carga TablaPondFechaAloj',df_TablaPondFechaAloj.count())
df_FechaAlojamientoPond= spark.sql(f"""
select pk_lote,ComplexEntityNo,date_add(DATE '1900-01-01', CAST(NumFNPond AS INT)) FechaAlojamientoPromPond from {database_name_tmp}.TablaPondFechaAloj
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/FechaAlojamientoPond"
}
df_FechaAlojamientoPond.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.FechaAlojamientoPond")
print('carga FechaAlojamientoPond',df_FechaAlojamientoPond.count())
df_FechaNacimiento= spark.sql(f"""
select ProteinEntitiesIRN,ComplexEntityNo,FirstHatchDate,GRN from {database_name_si}.si_mvBrimEntities
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/FechaNacimiento"
}
df_FechaNacimiento.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.FechaNacimiento")
print('carga FechaNacimiento',df_FechaNacimiento.count())
df_STDXPob= spark.sql(f"""
select descripfecha,pk_diasvida,pk_lote,pk_plantel,ComplexEntityNo,MortAcum,STDConsDia,PobInicial,STDConsDia * PobInicial STDXPob
from {database_name_gl}.ft_consolidado_Diario where pk_empresa = 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/STDXPob"
}
df_STDXPob.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.STDXPob")
print('carga STDXPob',df_STDXPob.count())
df_PondSTD =spark.sql(f"""
select pk_diasvida
,1014 pk_productoconsumo
,B.clote ComplexEntityNo
,SUM(MortAcum) MortAcum
,SUM(STDXPob) STDXPob
,SUM(PobInicial) PobInicial
,CASE WHEN SUM(PobInicial) = 0 THEN 0 ELSE SUM(STDXPob)/SUM(PobInicial) END PondSTDConsDia
from {database_name_tmp}.STDXPob A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
group by pk_diasvida,B.clote
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/PondSTD"
}
df_PondSTD.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.PondSTD")
print('carga PondSTD',df_PondSTD.count())
df_TablaAloj = spark.sql(f"""
select descripfecha,pk_lote,substring(complexentityno,1,(LENGTH(complexentityno)-6)) ComplexEntityNo,SUM(CantAloj) CantAloj
from {database_name_gl}.ft_alojamiento where pk_empresa = 1
group by pk_lote,descripfecha,substring(complexentityno,1,(LENGTH(complexentityno)-6)) order by 1
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaAloj"
}
df_TablaAloj.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaAloj")
print('carga TablaAloj',df_TablaAloj.count())
#Realiza TablaAlojAcum
df_TablaAlojAcum = spark.sql(f"""
    SELECT
        M.descripfecha,
        M.pk_lote,
        M.ComplexEntityNo,
        SUM(CASE WHEN MT.descripfecha <= M.descripfecha THEN MT.CantAloj ELSE 0 END) AS CantAloj
    FROM
        {database_name_tmp}.TablaAloj M
    LEFT JOIN
        {database_name_tmp}.TablaAloj MT ON M.ComplexEntityNo = MT.ComplexEntityNo
    GROUP BY
        M.descripfecha,
        M.pk_lote,
        M.ComplexEntityNo
    ORDER BY
        M.ComplexEntityNo,
        M.descripfecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/TablaAlojAcum"
}
df_TablaAlojAcum.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.TablaAlojAcum")
print('carga TablaAlojAcum',df_TablaAlojAcum.count())
#Realiza Consumos1
df_Consumos1 = spark.sql(f"""
SELECT 
 COALESCE(LT.fecha,'18991130') fecha
,1 as pk_empresa
    ,COALESCE(LD.pk_division,1) pk_division
,COALESCE(LZ.pk_zona,1) pk_zona
,COALESCE(LSZ.pk_subzona,1)pk_subzona
,COALESCE(LPL.pk_plantel,1)pk_plantel
,COALESCE(LLT.pk_lote,1)pk_lote
,COALESCE(LG.pk_galpon,1)pk_galpon
,COALESCE(LPD.pk_producto,1)pk_producto
,COALESCE(LGC.pk_grupoconsumo,1) pk_grupoconsumo
,COALESCE(LSGC.pk_subgrupoconsumo,1) pk_subgrupoconsumo
,LPC.pk_productoconsumo
,COALESCE(LAD.pk_administrador,1)pk_administrador
,COALESCE(PRO.pk_proveedor,1) pk_proveedor
,DateDiff(FAP.FechaAlojamientoPromPond, PPW.xdate) a1
,CASE WHEN LPC.pk_productoconsumo in (966,1014,2440) THEN 
		(CASE WHEN FAP.FechaAlojamientoPromPond IS NULL THEN 141
				WHEN FAP.FechaAlojamientoPromPond = '1899-11-30' THEN 141
				WHEN DateDiff(PPW.xdate,FAP.FechaAlojamientoPromPond) <= 0 THEN 141
				ELSE DateDiff(PPW.xdate,FAP.FechaAlojamientoPromPond) END) 
	ELSE 
		(CASE WHEN FN.FirstHatchDate IS NULL THEN 141
				WHEN FN.FirstHatchDate = '1899-11-30' THEN 141
				WHEN DateDiff(PPW.xdate,FN.FirstHatchDate) <= 0 THEN 141
				ELSE DateDiff(PPW.xdate,FN.FirstHatchDate) END) 
	END + 1 AS pk_diasvida
,MPPW.TransactionEntityID ComplexEntityNo
,FN.FirstHatchDate AS FechaNacimiento
,FAP.FechaAlojamientoPromPond AS FechaAlojamientoPond
,PPW.xdate As FechaResgistro
,FN.GRN
,CASE WHEN LPC.pk_productoconsumo in (966,1014,2440) THEN 
		(CASE WHEN FAP.FechaAlojamientoPromPond IS NULL THEN 0 
				WHEN FAP.FechaAlojamientoPromPond = '1899-11-30' THEN 0 
				ELSE DateDiff(PPW.xdate,FAP.FechaAlojamientoPromPond) END) 
	ELSE 
		(CASE WHEN FN.FirstHatchDate IS NULL THEN 0 
				WHEN FN.FirstHatchDate = '1899-11-30' THEN 0 
				ELSE DateDiff(PPW.xdate,FN.FirstHatchDate) END) 
	END AS Edad
,PPW.Quantity
,CASE WHEN LPC.pk_productoconsumo = 2440 THEN 21.6
		WHEN LPC.pk_productoconsumo = 301 THEN 8.45
		WHEN LPC.pk_productoconsumo = 1671 THEN 8.45
		WHEN LPC.pk_productoconsumo = 825 THEN 8.45
		WHEN LPC.pk_productoconsumo = 638 THEN 8.45
		ELSE 1 END AS Factor
,CASE WHEN LPC.pk_productoconsumo = 2440 THEN PPW.Quantity * 21.6
		WHEN LPC.pk_productoconsumo = 301 THEN PPW.Quantity * 8.45
		WHEN LPC.pk_productoconsumo = 1671 THEN PPW.Quantity * 8.45
		WHEN LPC.pk_productoconsumo = 825 THEN PPW.Quantity * 8.45
		WHEN LPC.pk_productoconsumo = 638 THEN PPW.Quantity * 8.45
		ELSE PPW.Quantity END AS Cantidad
,PPW.xValue Valor
FROM (select * from {database_name_si}.si_ProteinProductWHUsage 
      where(date_format(cast(EventDate as timestamp),'yyyy-MM-dd') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyy-MM-dd'))
      ) PPW
LEFT JOIN (select * from {database_name_si}.si_mvProteinProductWHUsage 
           where(date_format(cast(xDate as timestamp),'yyyy-MM-dd') >= date_format(add_months(trunc(current_date, 'month'),-9),'yyyy-MM-dd'))
          ) MPPW ON PPW.proteinentitiesirn = MPPW.proteinentitiesirn and PPW.irn = MPPW.irn
LEFT JOIN {database_name_gl}.lk_tiempo LT ON LT.fecha = PPW.EventDate
LEFT JOIN {database_name_si}.si_ProteinCostCenters PCC  ON CAST(PCC.IRN AS VARCHAR(50)) =CAST(PPW.ProteinCostCentersIRN AS VARCHAR(50))
LEFT JOIN {database_name_gl}.lk_division LD ON LD.IRN = CAST(PCC.ProteinDivisionsIRN as VARCHAR(50))
LEFT JOIN {database_name_gl}.lk_productoconsumo LPC ON LPC.IRN = CAST(PPW.ProteinProductsIRN AS VARCHAR(50))
LEFT JOIN {database_name_gl}.lk_grupoconsumo LGC ON LGC.pk_grupoconsumo = LPC.pk_grupoconsumo
LEFT JOIN {database_name_gl}.lk_subgrupoconsumo LSGC ON LSGC.pk_subgrupoconsumo = LPC.pk_subgrupoconsumo
LEFT JOIN {database_name_gl}.lk_plantel LPL ON LPL.IRN = CAST(PPW.ProteinFarmsIRN AS VARCHAR(50))
LEFT JOIN (select *,EntityNo EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) not in ('c1','c2','c3') or notes is null
            union all
           select *, substring(EntityNo,1,2) + substring(Notes,2,1) + substring(EntityNo,3,3) EntityNoCat 
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('c1','c2','c3') and length(EntityNo) <> 7
            union all
           select *, EntityNo EntityNoCat
           from {database_name_si}.si_proteinentities
           where substring(notes,1,5) in ('c1','c2','c3') and length(EntityNo) = 7
           ) PE ON PE.IRN  = PPW.ProteinEntitiesIRN
LEFT JOIN {database_name_si}.si_ProteinStandardVersions PSV ON PE.ProteinStandardVersionsIRN =PSV.IRN
LEFT JOIN {database_name_gl}.lk_galpon LG ON LG.IRN = CAST(PE.ProteinHousesIRN AS VARCHAR(50)) and cast(lg.activeflag as int) in (0,1)
LEFT JOIN {database_name_gl}.lk_producto LPD ON LPD.IRNProteinProductsAnimals = CAST(PE.ProteinProductsAnimalsIRN AS varchar(50))
LEFT JOIN {database_name_gl}.lk_standard LST ON LST.IRN = cast(PSV.ProteinStandardsIRN as varchar(50))
LEFT JOIN {database_name_gl}.lk_lote LLT ON LLT.pk_plantel= LPL.pk_plantel AND LLT.nlote = CAST(PE.EntityNo AS VARCHAR (50)) and llt.activeflag in (0,1)
LEFT JOIN {database_name_gl}.lk_administrador LAD ON LAD.IRN = CAST(PE.ProteinTechSupervisorsIRN AS VARCHAR(50))
LEFT JOIN {database_name_gl}.lk_subzona LSZ ON LSZ.pk_subzona=LPL.pk_subzona
LEFT JOIN {database_name_gl}.lk_zona LZ ON LZ.pk_zona = LPL.pk_zona
LEFT JOIN {database_name_gl}.lk_Tiempo LTP ON LTP.fecha =PPW.EventDate
LEFT JOIN {database_name_si}.si_mvBrimFarms MB ON CAST(MB.ProteinFarmsIRN AS VARCHAR(50)) = LPL.IRN 
LEFT JOIN {database_name_gl}.lk_proveedor PRO ON PRO.cproveedor = MB.VendorNo
LEFT JOIN {database_name_tmp}.FechaNacimiento FN ON PPW.proteinentitiesirn = FN.proteinentitiesirn
LEFT JOIN {database_name_tmp}.FechaAlojamientoPond FAP ON MPPW.TransactionEntityID = FAP.ComplexEntityNo
WHERE LD.pk_division in (2,4)
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Consumos1"
}
df_Consumos1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Consumos1")
print('carga Consumos1',df_Consumos1.count())
#Realiza Acumulado
df_Acumulado = spark.sql(f"""
WITH BaseConsumos AS (
    SELECT
        fecha,
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_productoconsumo,
        pk_subgrupoconsumo,
        pk_diasvida,
        ComplexEntityNo,GRN,
        Cantidad,
        Valor
    FROM
        {database_name_tmp}.Consumos1
),
AcumulacionesBase AS (
    SELECT
        fecha,
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_productoconsumo,
        pk_subgrupoconsumo,
        pk_diasvida,
        ComplexEntityNo,GRN,
        SUM(Cantidad) AS Cantidad,
        SUM(Valor) AS Valor
    FROM
        BaseConsumos
    GROUP BY
        fecha,
        pk_plantel,
        pk_lote,
        pk_galpon,
        pk_productoconsumo,
        pk_subgrupoconsumo,
        pk_diasvida,
        ComplexEntityNo,GRN
)
SELECT
    fecha,
    pk_plantel,
    pk_lote,
    pk_galpon,
    pk_productoconsumo,
    pk_subgrupoconsumo,
    pk_diasvida,
    ComplexEntityNo,
    SUM(Cantidad) OVER (PARTITION BY ComplexEntityNo, pk_productoconsumo ORDER BY fecha ASC) AS CantAcum,
    SUM(Cantidad) OVER (PARTITION BY ComplexEntityNo, pk_subgrupoconsumo ORDER BY fecha ASC) AS CantAcumSg,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL THEN Cantidad ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo, fecha) AS CantAcumGasLote,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL AND GRN = 'H' THEN Cantidad ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo, fecha) AS CantAcumGasGalpon,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL THEN Cantidad ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo ORDER BY fecha ASC) AS CantAcumHGasLote,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL AND GRN = 'H' THEN Cantidad ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo ORDER BY fecha ASC) AS CantAcumHGasGalpon,
    SUM(Valor) OVER (PARTITION BY ComplexEntityNo, pk_productoconsumo ORDER BY fecha ASC) AS ValorAcum,
    SUM(Valor) OVER (PARTITION BY ComplexEntityNo, pk_subgrupoconsumo ORDER BY fecha ASC) AS ValorAcumSg,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL THEN Valor ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo, fecha) AS ValorAcumGasLote,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL AND GRN = 'H' THEN Valor ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo, fecha) AS ValorAcumGasGalpon,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL THEN Valor ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo ORDER BY fecha ASC) AS ValorAcumHGasLote,
    SUM(CASE WHEN pk_lote IS NOT NULL AND pk_subgrupoconsumo IS NOT NULL AND fecha IS NOT NULL AND GRN = 'H' THEN Valor ELSE 0 END) OVER (PARTITION BY pk_lote, pk_subgrupoconsumo ORDER BY fecha ASC) AS ValorAcumHGasGalpon
FROM
    AcumulacionesBase
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Acumulado"
}
df_Acumulado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Acumulado")
print('carga Acumulado',df_Acumulado.count())
#Realiza Consumos2
df_Consumos2 = spark.sql(f"""
with ConsumosPob as (
select A.fecha,A.ComplexEntityNo,MAX(CantAloj) PobInicialAcumALaFecha
from {database_name_tmp}.Consumos1 A
left join {database_name_tmp}.TablaAlojAcum TAA on a.pk_lote = TAA.pk_lote and TAA.descripfecha<= a.fecha
group by A.fecha,A.ComplexEntityNo)
,temp1 as (select ComplexEntityNo,max(descripfecha) fecha from {database_name_gl}.ft_consolidado_Corral group by ComplexEntityNo)
,temp2 as (SELECT descripfecha fecha,pk_estado,ComplexEntityNo,FechaAlojamiento,MAX(pk_diasvida)pk_diasvida,MAX(PobInicial)PobInicial, MAX(KilosRendidos) KilosRendidos, MAX(AvesRendidas) AvesRendidas, MAX(MortDia) MortDia 
FROM {database_name_gl}.ft_consolidado_lote WHERE flagartatipico = 2 and pk_empresa = 1 GROUP BY ComplexEntityNo,FechaAlojamiento,descripfecha,pk_estado)

SELECT 
CON.fecha,
CON.pk_empresa,
CON.pk_division,
CON.pk_zona,
CON.pk_subzona,
CON.pk_plantel,
CON.pk_lote,
CON.pk_galpon,
CASE WHEN CON.GRN = 'F' THEN 1 WHEN CON.GRN = 'H' THEN 1 WHEN CON.GRN = 'P' THEN CC.pk_sexo	END AS pk_sexo,
CASE WHEN CON.GRN = 'F' THEN 5 WHEN CON.GRN = 'H' THEN 5 WHEN CON.GRN = 'P' THEN CC.pk_standard	END AS pk_standard,
CON.pk_producto,
CON.pk_productoconsumo,
CON.pk_grupoconsumo,
CON.pk_subgrupoconsumo,
TP.pk_tipoproducto,
CASE WHEN CON.GRN = 'F' THEN 1 WHEN CON.GRN = 'H' THEN 1 WHEN CON.GRN = 'P' THEN CC.pk_especie END AS pk_especie,
CASE WHEN CON.GRN = 'F' THEN CL.pk_estado WHEN CON.GRN = 'H' THEN CG.pk_estado WHEN CON.GRN = 'P' THEN CC.pk_estado	END AS pk_estado,
CON.pk_administrador,
CON.pk_proveedor,
CON.pk_diasvida,
SV.pk_semanavida,
CON.ComplexEntityNo,
CON.GRN,
Edad,
CASE WHEN CON.GRN = 'F' THEN CL.FechaAlojamiento WHEN CON.GRN = 'H' THEN CG.FechaAlojamiento WHEN CON.GRN = 'P' THEN CC.FechaAlojamiento END AS FechaAlojamiento,
CASE WHEN CON.GRN = 'F' THEN CL.pk_diasvida WHEN CON.GRN = 'H' THEN CG.pk_diasvida WHEN CON.GRN = 'P' THEN CC.pk_diasvida END -1 AS EdadConsolidado,
CASE WHEN CON.GRN = 'F' THEN CL.fecha WHEN CON.GRN = 'H' THEN CG.descripfecha WHEN CON.GRN = 'P' THEN CC.descripfecha	END AS FechaConsolidado,
CON.Cantidad,
ACUM.CantAcum,
ACUM.CantAcumSg,
ACUM.CantAcumGasLote,
ACUM.CantAcumGasGalpon,
ACUM.CantAcumHGasLote,
ACUM.CantAcumHGasGalpon,
CASE WHEN CON.pk_subgrupoconsumo = 4 AND (CASE WHEN CON.GRN = 'F' THEN CL.pk_estado WHEN CON.GRN = 'H' THEN CG.pk_estado WHEN CON.GRN = 'P' THEN CC.pk_estado END) IN (3,4)
	 THEN COALESCE(((CON.Cantidad * 1000)/
		  COALESCE((CASE WHEN CON.GRN = 'F' THEN CL.KilosRendidos
					   WHEN CON.GRN = 'H' THEN CG.KilosRendidos
					   WHEN CON.GRN = 'P' THEN CC.KilosRendidos
					   END),0)),0)
	 WHEN CON.pk_diasvida = 142
	 THEN COALESCE(((CON.Cantidad * 1000)/
		  COALESCE((CASE WHEN CON.GRN = 'F' THEN CL.PobInicial
					   WHEN CON.GRN = 'H' THEN CG.PobInicial
					   WHEN CON.GRN = 'P' THEN CC.PobInicial
					   END),0)),0)
	 ELSE COALESCE(((CON.Cantidad * 1000)/
		  COALESCE((CP.PobInicialAcumALaFecha),0)),0)
END AS `Cantidad/1000`,
CON.Valor,
ACUM.ValorAcum,
ACUM.ValorAcumSg,
ACUM.ValorAcumGasLote,
ACUM.ValorAcumGasGalpon,
ACUM.ValorAcumHGasLote,
ACUM.ValorAcumHGasGalpon,
CASE WHEN CON.GRN = 'F' THEN COALESCE(CON.Valor/COALESCE(CL.KilosRendidos,0),0)	ELSE 0 END CostoKilosRendidos,
CASE WHEN CON.GRN = 'F' AND CL.pk_estado IN (3,4) THEN COALESCE(CON.Valor/COALESCE(CL.AvesRendidas,0),0) WHEN CON.GRN = 'F' AND CL.pk_estado = 2 THEN COALESCE(CON.Valor/COALESCE((CL.PobInicial - CL.MortDia),0),0) ELSE 0 END CostoAve,
CASE WHEN CON.GRN = 'F' THEN CL.PobInicial WHEN CON.GRN = 'H' THEN CG.PobInicial WHEN CON.GRN = 'P' THEN CC.PobInicial END AS PobInicial,
CL.PobInicial PobInicialLote,
COALESCE(CP.PobInicialAcumALaFecha,0) PobInicialAcumALaFecha,
CASE WHEN CON.GRN = 'F' THEN CL.KilosRendidos WHEN CON.GRN = 'H' THEN CG.KilosRendidos WHEN CON.GRN = 'P' THEN CC.KilosRendidos	END AS KilosRendidos,
CASE WHEN CON.GRN = 'F' THEN CL.AvesRendidas WHEN CON.GRN = 'H' THEN CG.AvesRendidas WHEN CON.GRN = 'P' THEN CC.AvesRendidas END AS AvesRendidas,
CASE WHEN CON.GRN = 'F' THEN CL.MortDia	WHEN CON.GRN = 'H' THEN CG.MortDia WHEN CON.GRN = 'P' THEN CC.MortDia END AS Mortalidad,
PSTD.MortAcum MortAcumAgua,
CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN CL.PobInicial - PSTD.MortAcum ELSE 0 END AS SaldoPobInicial,
CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN ((CON.Cantidad*1000)*1000/(CL.PobInicial - PSTD.MortAcum)) ELSE 0 END RealAgua,
PSTD.PondSTDConsDia PondSTDConsDiaAgua,
PSTD.PondSTDConsDia*2 STDAgua
FROM {database_name_tmp}.Consumos1 CON
LEFT JOIN {database_name_gl}.lk_diasvida DV ON CON.pk_diasvida = DV.pk_diasvida
LEFT JOIN {database_name_gl}.lk_semanavida SV ON DV.pk_semanavida = SV.pk_semanavida
LEFT JOIN temp1 ON CON.ComplexEntityNo = temp1.ComplexEntityNo
LEFT JOIN {database_name_gl}.ft_consolidado_Corral CC ON CON.ComplexEntityNo = CC.ComplexEntityNo  AND CC.descripfecha = temp1.fecha
LEFT JOIN {database_name_gl}.ft_consolidado_Galpon CG ON CON.ComplexEntityNo = CG.ComplexEntityNo
LEFT JOIN temp2 CL ON CON.ComplexEntityNo = CL.ComplexEntityNo
LEFT JOIN {database_name_gl}.lk_producto LP ON CON.pk_producto = LP.pk_producto
LEFT JOIN {database_name_gl}.lk_tipoproducto TP ON UPPER(LP.grupoproducto) = UPPER(TP.ntipoproducto)
LEFT JOIN {database_name_tmp}.PondSTD PSTD ON CON.ComplexEntityNo = PSTD.ComplexEntityNo AND CON.pk_productoconsumo = PSTD.pk_productoconsumo AND CON.pk_diasvida = PSTD.pk_diasvida
LEFT JOIN ConsumosPob CP ON CON.ComplexEntityNo = CP.ComplexEntityNo AND CON.fecha = CP.fecha
LEFT JOIN {database_name_tmp}.Acumulado ACUM ON CON.ComplexEntityNo = ACUM.ComplexEntityNo AND CON.fecha = ACUM.fecha AND CON.pk_diasvida = ACUM.pk_diasvida AND CON.pk_productoconsumo = ACUM.pk_productoconsumo 
WHERE (CON.ComplexEntityNo like 'P%' or CON.ComplexEntityNo like 'V%') and CON.ComplexEntityNo not like 'pusm%'
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Consumos2"
}
df_Consumos2.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Consumos2")
print('carga Consumos2',df_Consumos2.count())
#Realiza Consumos3
df_Consumos3 = spark.sql(f"""
SELECT 
 CON.fecha
,CON.pk_empresa
,CON.pk_division
,CON.pk_zona
,CON.pk_subzona
,CON.pk_plantel
,CON.pk_lote
,CON.pk_galpon
,CON.pk_sexo
,CON.pk_standard
,CON.pk_producto
,CON.pk_productoconsumo
,CON.pk_grupoconsumo
,CON.pk_subgrupoconsumo
,CON.pk_tipoproducto
,CON.pk_especie
,CON.pk_estado
,CON.pk_administrador
,CON.pk_proveedor
,CON.pk_diasvida
,CON.pk_semanavida
,CON.ComplexEntityNo
,CON.GRN
,CON.Edad
,CON.FechaAlojamiento
,CON.EdadConsolidado
,CON.FechaConsolidado
,CON.Cantidad
,CON.CantAcum
,CON.CantAcumSg
,CON.CantAcumGasLote
,CON.CantAcumGasGalpon
,CON.CantAcumHGasLote
,CON.CantAcumHGasGalpon
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.Cantidad * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.Cantidad * 1000 / COALESCE(CON.PobInicialLote,0)),0)
	  ELSE COALESCE((CON.Cantidad * 1000 / COALESCE(CON.PobInicialAcumALaFecha,0)),0)
	END AS `Cantidad/1000`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcum * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcum * 1000 / COALESCE(CON.PobInicialLote,0)),0)
	  ELSE COALESCE((CON.CantAcum * 1000 / COALESCE(CON.PobInicialAcumALaFecha,0)),0)
	END AS `Cantidad/1000Acum`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcumSg * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcumSg * 1000 / COALESCE(CON.PobInicialLote,0)),0)
	  ELSE COALESCE((CON.CantAcumSg * 1000 / COALESCE(CON.PobInicialAcumALaFecha,0)),0)
	END AS `Cantidad/1000AcumSg`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcumGasLote * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcumGasLote * 1000 / COALESCE(CON.PobInicial,0)),0)
	  ELSE COALESCE((CON.CantAcumGasLote * 1000 / COALESCE(CON.PobInicial,0)),0)
	END AS `Cantidad/1000AcumGasLote`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcumGasGalpon * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcumGasGalpon * 1000 / COALESCE(CON.PobInicial,0)),0)
	  ELSE COALESCE((CON.CantAcumGasGalpon * 1000 / COALESCE(CON.PobInicial,0)),0)
	END AS `Cantidad/1000AcumGasGalpon`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcumHGasLote * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcumHGasLote * 1000 / COALESCE(CON.PobInicialLote,0)),0)
	  ELSE COALESCE((CON.CantAcumHGasLote * 1000 / COALESCE(CON.PobInicialAcumALaFecha,0)),0)
	END AS `Cantidad/1000AcumHGasLote`
,CASE WHEN CON.pk_subgrupoconsumo = 4 AND CON.pk_estado IN (3,4) THEN COALESCE((CON.CantAcumHGasGalpon * 1000 / COALESCE(CON.KilosRendidos,0)),0)
	  WHEN CON.Edad < 0 THEN COALESCE((CON.CantAcumHGasGalpon * 1000 / COALESCE(CON.PobInicialLote,0)),0)
	  ELSE COALESCE((CON.CantAcumHGasGalpon * 1000 / COALESCE(CON.PobInicialAcumALaFecha,0)),0)
	END AS `Cantidad/1000AcumHGasGalpon`
,CON.Valor
,CON.ValorAcum
,CON.ValorAcumSg
,CON.ValorAcumGasLote
,CON.ValorAcumGasGalpon
,CON.ValorAcumHGasLote
,CON.ValorAcumHGasGalpon
,CASE WHEN CON.GRN = 'F' THEN COALESCE(CON.Valor/NULLIF(CON.KilosRendidos,0),0) ELSE 0 END CostoKilosRendidos
,CASE WHEN CON.GRN = 'F' AND CON.pk_estado IN (3,4) THEN COALESCE(CON.Valor/COALESCE(CON.AvesRendidas,0),0)
		WHEN CON.GRN = 'F' AND CON.pk_estado = 2 THEN COALESCE(CON.Valor/COALESCE((CON.PobInicial - PSTD.MortAcum),0),0)
	ELSE 0 END CostoAve
,CON.PobInicial
,CON.PobInicialLote
,CON.PobInicialAcumALaFecha
,CON.KilosRendidos
,CON.AvesRendidas
,CON.Mortalidad
,CASE WHEN CON.GRN = 'F' THEN PSTD.MortAcum
	ELSE STD.MortAcum
	END MortAcum
,CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN CON.PobInicial - PSTD.MortAcum ELSE 0 END AS SaldoPobInicial
,CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN ((CON.Cantidad*1000)*1000/(CON.PobInicial - PSTD.MortAcum)) ELSE 0 END RealAgua
,CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN PSTD.PondSTDConsDia ELSE 0 END PondSTDConsDiaAgua
,CASE WHEN CON.GRN = 'F'AND CON.pk_productoconsumo = 1014 THEN PSTD.PondSTDConsDia*2 ELSE 0 END STDAgua
FROM {database_name_tmp}.Consumos2 CON
LEFT JOIN {database_name_tmp}.PondSTD PSTD ON CON.ComplexEntityNo = PSTD.ComplexEntityNo AND CON.pk_diasvida = PSTD.pk_diasvida
LEFT JOIN {database_name_tmp}.STDXPob STD ON cast(CON.ComplexEntityNo as varchar(50)) = cast(STD.ComplexEntityNo as varchar(50)) AND CON.pk_diasvida = STD.pk_diasvida
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Consumos3"
}
df_Consumos3.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Consumos3")
print('carga Consumos3',df_Consumos3.count())
#Realiza Agrupado1
df_Agrupado1 = spark.sql(f"""
with agru as (select max(fecha) fecha,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,1 pk_galpon,1 pk_sexo,5 pk_standard,max(pk_producto) pk_producto
			,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo,pk_tipoproducto,1 pk_especie,pk_estado,pk_administrador,pk_proveedor,max(pk_diasvida) pk_diasvida
			,max(pk_semanavida) pk_semanavida,ComplexEntityNo,GRN,max(Edad) Edad,FechaAlojamiento,EdadConsolidado,FechaConsolidado,sum(Cantidad) Cantidad
			,max(CantAcum) CantAcum,max(CantAcumSg) CantAcumSg,max(CantAcumGasLote) CantAcumGasLote,max(CantAcumGasGalpon) CantAcumGasGalpon
			,max(CantAcumHGasLote) CantAcumHGasLote
			,max(CantAcumHGasGalpon) CantAcumHGasGalpon
			,sum(Cantidad/1000) `Cantidad/1000`
			,max(`Cantidad/1000Acum`)`Cantidad/1000Acum`
			,max(`Cantidad/1000AcumSg`) `Cantidad/1000AcumSg`
			,max(`Cantidad/1000AcumGasLote`) `Cantidad/1000AcumGasLote`
			,max(`Cantidad/1000AcumGasGalpon`) `Cantidad/1000AcumGasGalpon`
			,max(`Cantidad/1000AcumHGasLote`) `Cantidad/1000AcumHGasLote`
			,max(`Cantidad/1000AcumHGasGalpon`)`Cantidad/1000AcumHGasGalpon`
			,sum(Valor) Valor
			,max(ValorAcum) ValorAcum
			,max(ValorAcumSg) ValorAcumSg
			,max(ValorAcumGasLote) ValorAcumGasLote
			,max(ValorAcumGasGalpon) ValorAcumGasGalpon
			,max(ValorAcumHGasLote) ValorAcumHGasLote
			,max(ValorAcumHGasGalpon) ValorAcumHGasGalpon
			,0 CostoKilosRendidos
			,0 CostoAve
			,max(PobInicial) PobInicial
			,max(KilosRendidos) KilosRendidos
			,max(AvesRendidas) AvesRendidas
			,max(Mortalidad) Mortalidad
			,max(MortAcum) MortAcum
			,0 SaldoPobInicial,0 RealAgua,0 PondSTDConsDiaAgua,0 STDAgua
	from {database_name_tmp}.consumos3
	where GRN = 'P'
	group by pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo,pk_tipoproducto,pk_estado,pk_administrador
			,pk_proveedor,ComplexEntityNo,GRN,FechaAlojamiento,EdadConsolidado,FechaConsolidado)
			
select
 max(A.fecha) fecha
,A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,1 pk_galpon
,1 pk_sexo
,5 pk_standard
,max(pk_producto) pk_producto
,A.pk_productoconsumo
,A.pk_grupoconsumo
,A.pk_subgrupoconsumo
,A.pk_tipoproducto
,1 pk_especie
,B.pk_estado
,A.pk_administrador
,A.pk_proveedor
,max(A.pk_diasvida) pk_diasvida
,max(pk_semanavida) pk_semanavida
,B.ComplexEntityNo
,'F' GRN
,max(Edad) Edad
,B.FechaAlojamiento
,B.pk_diasvida-1 EdadConsolidado
,B.descripfecha FechaConsolidado
,sum(Cantidad) Cantidad
,max(CantAcum) CantAcum
,max(CantAcumSg) CantAcumSg
,max(CantAcumGasLote) CantAcumGasLote
,max(CantAcumGasGalpon) CantAcumGasGalpon
,max(CantAcumHGasLote) CantAcumHGasLote
,max(CantAcumHGasGalpon) CantAcumHGasGalpon
,sum(`Cantidad/1000`) `Cantidad/1000`
,max(`Cantidad/1000Acum`) `Cantidad/1000Acum`
,max(`Cantidad/1000AcumSg`) `Cantidad/1000AcumSg`
,max(`Cantidad/1000AcumGasLote`) `Cantidad/1000AcumGasLote`
,max(`Cantidad/1000AcumGasGalpon`) `Cantidad/1000AcumGasGalpon`
,max(`Cantidad/1000AcumHGasLote`) `Cantidad/1000AcumHGasLote`
,max(`Cantidad/1000AcumHGasGalpon`) `Cantidad/1000AcumHGasGalpon`
,sum(Valor) Valor
,max(ValorAcum) ValorAcum
,max(ValorAcumSg) ValorAcumSg
,max(ValorAcumGasLote) ValorAcumGasLote
,max(ValorAcumGasGalpon) ValorAcumGasGalpon
,max(ValorAcumHGasLote) ValorAcumHGasLote
,max(ValorAcumHGasGalpon) ValorAcumHGasGalpon
,COALESCE(sum(Valor)/COALESCE(sum(A.KilosRendidos),0),0) CostoKilosRendidos
,case when B.pk_estado in (3,4) then COALESCE(sum(Valor)/COALESCE(sum(A.KilosRendidos),0),0) else COALESCE(sum(Valor)/COALESCE((sum(A.PobInicial)-sum(A.MortAcum)),0),0) end CostoAve
,max(B.PobInicial) PobInicial
,sum(A.PobInicial) PobInicialCons
,max(B.KilosRendidos) KilosRendidos
,sum(A.KilosRendidos) KilosRendidosCons
,max(B.AvesRendidas) AvesRendidas
,sum(A.AvesRendidas) AvesRendidasCons
,max(B.MortDia) Mortalidad
,sum(A.MortAcum) MortAcumCons
,0 SaldoPobInicial
,0 RealAgua
,0 PondSTDConsDiaAgua
,0 STDAgua
from agru A
left join {database_name_gl}.ft_consolidado_lote B on substring(A.complexentityno,1,(LENGTH(A.complexentityno)-6)) = B.ComplexEntityNo
where grn ='P' and B.ComplexEntityNo is not null AND b.flagartatipico = 2
group by A.pk_empresa
,A.pk_division
,A.pk_zona
,A.pk_subzona
,A.pk_plantel
,A.pk_lote
,A.pk_productoconsumo
,A.pk_grupoconsumo
,A.pk_subgrupoconsumo
,A.pk_tipoproducto
,B.pk_estado
,A.pk_administrador
,A.pk_proveedor
,B.ComplexEntityNo
,B.FechaAlojamiento
,B.pk_diasvida
,B.descripfecha
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Agrupado1"
}
df_Agrupado1.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Agrupado1")
print('carga Agrupado1',df_Agrupado1.count())
#Realiza ft_consumo_Diario
df_ft_consumo_Diario = spark.sql(f"""
SELECT d.pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,pk_semanavida,ComplexEntityNo,GRN,FechaAlojamiento,Edad,EdadConsolidado,date_format(FechaConsolidado,"yyyyMMdd") FechaConsolidado
,SUM(Cantidad) Cantidad
,MAX(CantAcum) CantAcum
,MAX(CantAcumSg) CantAcumSg
,MAX(CantAcumGasLote) CantAcumGasote
,MAX(CantAcumGasGalpon) CantAcumGasGalpon
,MAX(CantAcumHGasLote) CantAcumHGasote
,MAX(CantAcumHGasGalpon) CantAcumHGasGalpon
,SUM(`Cantidad/1000`) `Cantidad/1000`
,MAX(`Cantidad/1000Acum`) `Cantidad/1000Acum`
,MAX(`Cantidad/1000AcumSg`) `Cantidad/1000AcumSg`
,MAX(`Cantidad/1000AcumGasLote`) `Cantidad/1000AcumGasLote`
,MAX(`Cantidad/1000AcumGasGalpon`) `Cantidad/1000AcumGasGalpon`
,MAX(`Cantidad/1000AcumHGasLote`) `Cantidad/1000AcumHGasLote`
,MAX(`Cantidad/1000AcumHGasGalpon`) `Cantidad/1000AcumHGasGalpon`
,SUM(Valor) Valor
,max(ValorAcum) ValorAcum
,max(ValorAcumSg) ValorAcumSg
,max(ValorAcumGasLote) ValorAcumGasLote
,max(ValorAcumGasGalpon) ValorAcumGasGalpon
,max(ValorAcumHGasLote) ValorAcumHGasLote
,max(ValorAcumHGasGalpon) ValorAcumHGasGalpon
,MAX(PobInicial) PobInicial
,MAX(PobInicial) PobInicialCons
,MAX(KilosRendidos) KilosRendidos
,MAX(KilosRendidos) KilosRendidosCons
,MAX(AvesRendidas) AvesRendidas
,MAX(AvesRendidas) AvesRendidasCons
,MAX(Mortalidad) Mortalidad
,MAX(MortAcum) MortAcum
,MAX(CostoAve) CostoAve
,MAX(CostoKilosRendidos) CostoKilosRendidos
,MAX(SaldoPobInicial) SaldoPobInicialAgua
,MAX(RealAgua) RealAgua
,MAX(PondSTDConsDiaAgua)PondSTDConsDiaAgua
,MAX(STDAgua) STDAgua
,'' categoria
,cast(c.fecha as date) DescripFecha
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
,'' DescripSubGrupoConsumo
,'' DescripTipoProducto
,'' DescripEspecie
,'' DescripEstado
,'' DescripAdministrador
,'' DescripProveedor
,'' DescripDiaVida
,'' DescripSemanaVida
FROM {database_name_tmp}.Consumos3 C left join {database_name_gl}.lk_tiempo d on c.fecha=d.fecha
WHERE date_format(c.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
GROUP BY pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,pk_semanavida,ComplexEntityNo,GRN,Edad,FechaAlojamiento,EdadConsolidado,FechaConsolidado,cast(c.fecha as date)

Union

SELECT pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,pk_semanavida,ComplexEntityNo,GRN,FechaAlojamiento,Edad,EdadConsolidado,date_format(FechaConsolidado,"yyyyMMdd") FechaConsolidado
,SUM(Cantidad) Cantidad
,MAX(CantAcum) CantAcum
,MAX(CantAcumSg) CantAcumSg
,MAX(CantAcumGasLote) CantAcumGasLote
,MAX(CantAcumGasGalpon) CantAcumGasGalpon
,MAX(CantAcumHGasLote) CantAcumHGasLote
,MAX(CantAcumHGasGalpon) CantAcumHGasGalpon
,SUM(`Cantidad/1000`) `Cantidad/1000`
,MAX(`Cantidad/1000Acum`) `Cantidad/1000Acum`
,MAX(`Cantidad/1000AcumSg`) `Cantidad/1000AcumSg`
,MAX(`Cantidad/1000AcumGasLote`) `Cantidad/1000AcumGasLote`
,MAX(`Cantidad/1000AcumGasGalpon`) `Cantidad/1000AcumGasGalpon`
,MAX(`Cantidad/1000AcumHGasLote`) `Cantidad/1000AcumHGasLote`
,MAX(`Cantidad/1000AcumHGasGalpon`) `Cantidad/1000AcumHGasGalpon`
,SUM(Valor) Valor
,max(ValorAcum) ValorAcum
,max(ValorAcumSg) ValorAcumSg
,max(ValorAcumGasLote) ValorAcumGasLote
,max(ValorAcumGasGalpon) ValorAcumGasGalpon
,max(ValorAcumHGasLote) ValorAcumHGasLote
,max(ValorAcumHGasGalpon) ValorAcumHGasGalpon
,MAX(PobInicial) PobInicial
,MAX(PobInicialCons) PobInicialCons
,MAX(KilosRendidos) KilosRendidos
,MAX(KilosRendidosCons) KilosRendidosCons
,MAX(AvesRendidas) AvesRendidas
,MAX(AvesRendidasCons) AvesRendidasCons
,MAX(Mortalidad) Mortalidad
,MAX(MortAcumCons) MortAcumCons
,MAX(CostoAve) CostoAve
,MAX(CostoKilosRendidos) CostoKilosRendidos
,MAX(SaldoPobInicial) SaldoPobInicial
,MAX(RealAgua) RealAgua
,MAX(PondSTDConsDiaAgua)PondSTDConsDiaAgua
,MAX(STDAgua) STDAgua
,'' categoria
,cast(c.fecha as date) DescripFecha
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
,'' DescripSubGrupoConsumo
,'' DescripTipoProducto
,'' DescripEspecie
,'' DescripEstado
,'' DescripAdministrador
,'' DescripProveedor
,'' DescripDiaVida
,'' DescripSemanaVida
FROM {database_name_tmp}.Agrupado1 C left join {database_name_gl}.lk_tiempo d on c.fecha=d.fecha
WHERE date_format(c.fecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM')
GROUP BY pk_tiempo,pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_diasvida,pk_semanavida,ComplexEntityNo,GRN,Edad,FechaAlojamiento,EdadConsolidado,FechaConsolidado,cast(c.fecha as date)
""")
print('carga df_ft_consumo_Diario',df_ft_consumo_Diario.count())
#df_Consumos41 = spark.sql(f"""
#select date_format(FechaConsolidado,"yyyyMMdd")
#from {database_name_tmp}.Consumos3 A
#where a.ComplexEntityNo = 'P051-2406' and a.pk_productoconsumo=1136
#""")
#df_Consumos41.show()
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target31 = f"{bucket_name_prdmtech}ft_consumo_diario/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consumo_diario cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consumo_diario")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str)))
    filtered_new_data = df_ft_consumo_Diario.filter((date_format(col("descripfecha"),"yyyy-MM-dd") >= fecha_str))
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consumo_diarioTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consumo_diarioTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consumo_diarioTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consumo_diario"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consumo_diario")
            
    print(f"agrega registros nuevos a la tabla ft_consumo_diario : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consumo_diario : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consumo_diarioTemporal')
    print(f"Tabla ft_consumo_diarioTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consumo_diario"
    }
    df_ft_consumo_Diario.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consumo_diario")
#Realiza Consumos4
df_Consumos4 = spark.sql(f"""
select 
 pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo,pk_tipoproducto
,pk_especie,pk_estado,pk_administrador,pk_proveedor,max(pk_diasvida) pk_diasvida,pk_semanavida,ComplexEntityNo,grn,FechaAlojamiento,EdadConsolidado,FechaConsolidado
,SUM(COALESCE(Cantidad,0)) Cantidad
,SUM(COALESCE(`Cantidad/1000`,0)) `Cantidad/1000`
,SUM(COALESCE(Valor,0)) Valor
,MAX(COALESCE(PobInicial,0)) PobInicial
from {database_name_gl}.ft_consumo_diario A
group by pk_empresa,pk_division,pk_zona,pk_subzona,pk_plantel,pk_lote,pk_galpon,pk_sexo,pk_standard,pk_producto,pk_productoconsumo,pk_grupoconsumo,pk_subgrupoconsumo
,pk_tipoproducto,pk_especie,pk_estado,pk_administrador,pk_proveedor,pk_semanavida,ComplexEntityNo,grn,FechaAlojamiento,EdadConsolidado,FechaConsolidado
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/Consumos4"
}
df_Consumos4.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.Consumos4")
print('carga Consumos4', df_Consumos4.count())
#Realiza ft_consumo_Semanal
df_ft_consumo_SemanalT = spark.sql(f"""
WITH Consumos3Agregado AS (
    SELECT
        ComplexEntityNo,
        pk_productoconsumo,
        pk_semanavida,
        SUM(Cantidad) AS TotalCantidadSemana,
        SUM(`Cantidad/1000`) AS TotalCantidad_1000Semana,
        SUM(Valor) AS TotalValorSemana
    FROM
        {database_name_tmp}.Consumos3
    GROUP BY
        ComplexEntityNo,
        pk_productoconsumo,
        pk_semanavida
),
CantidadesSemana1 AS (
    SELECT
        ComplexEntityNo,
        pk_productoconsumo,
        SUM(CASE WHEN pk_semanavida = 2 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem1,
        SUM(CASE WHEN pk_semanavida = 2 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem1,
        SUM(CASE WHEN pk_semanavida = 2 THEN TotalValorSemana ELSE 0 END)           AS ValorSem1,
        SUM(CASE WHEN pk_semanavida = 3 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem2,
        SUM(CASE WHEN pk_semanavida = 3 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem2,
        SUM(CASE WHEN pk_semanavida = 3 THEN TotalValorSemana ELSE 0 END)           AS ValorSem2,
        SUM(CASE WHEN pk_semanavida = 4 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem3,
        SUM(CASE WHEN pk_semanavida = 4 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem3,
        SUM(CASE WHEN pk_semanavida = 4 THEN TotalValorSemana ELSE 0 END)           AS ValorSem3,
        SUM(CASE WHEN pk_semanavida = 5 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem4,
        SUM(CASE WHEN pk_semanavida = 5 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem4,
        SUM(CASE WHEN pk_semanavida = 5 THEN TotalValorSemana ELSE 0 END)           AS ValorSem4,
        SUM(CASE WHEN pk_semanavida = 6 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem5,
        SUM(CASE WHEN pk_semanavida = 6 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem5,
        SUM(CASE WHEN pk_semanavida = 6 THEN TotalValorSemana ELSE 0 END)           AS ValorSem5,
        SUM(CASE WHEN pk_semanavida = 7 THEN TotalCantidadSemana ELSE 0 END)        AS CantSem6,
        SUM(CASE WHEN pk_semanavida = 7 THEN TotalCantidad_1000Semana ELSE 0 END)   AS Cant_1000Sem6,
        SUM(CASE WHEN pk_semanavida = 7 THEN TotalValorSemana ELSE 0 END)           AS ValorSem6
    FROM
        Consumos3Agregado
    GROUP BY
        ComplexEntityNo,
        pk_productoconsumo
),
CantidadesSemanaMas AS (
    SELECT
        ComplexEntityNo,
        pk_productoconsumo,
        SUM(CASE WHEN pk_semanavida >= 8 AND pk_semanavida <= 21 THEN TotalCantidadSemana ELSE 0 END) AS CantSemMas,
        SUM(CASE WHEN pk_semanavida >= 8 AND pk_semanavida <= 21 THEN TotalCantidad_1000Semana ELSE 0 END) AS Cant_1000SemMas,
        SUM(CASE WHEN pk_semanavida >= 8 AND pk_semanavida <= 21 THEN TotalValorSemana ELSE 0 END) AS ValorSemMas
    FROM
        Consumos3Agregado
    GROUP BY
        ComplexEntityNo,
        pk_productoconsumo
),
CantidadesLimpieza AS (
    SELECT
        ComplexEntityNo,
        pk_productoconsumo,
        SUM(CASE WHEN pk_semanavida = 22 THEN TotalCantidadSemana ELSE 0 END) AS CantLimpieza,
        SUM(CASE WHEN pk_semanavida = 22 THEN TotalCantidad_1000Semana ELSE 0 END) AS Cant_1000Limpieza,
        SUM(CASE WHEN pk_semanavida = 22 THEN TotalValorSemana ELSE 0 END) AS ValorLimpieza
    FROM
        Consumos3Agregado
    GROUP BY
        ComplexEntityNo,
        pk_productoconsumo
),
CantidadesTotal AS (
    SELECT
        ComplexEntityNo,
        pk_productoconsumo,
        SUM(TotalCantidadSemana) AS CantTotal,
        SUM(TotalCantidad_1000Semana) AS Cant_1000Total,
        SUM(TotalValorSemana) AS ValorTotal
    FROM
        Consumos3Agregado
    GROUP BY
        ComplexEntityNo,
        pk_productoconsumo
)
select T.pk_tiempo,
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
    A.pk_productoconsumo,
    A.pk_grupoconsumo,
    A.pk_subgrupoconsumo,
    A.pk_tipoproducto,
    A.pk_especie,
    A.pk_estado,
    A.pk_administrador,
    A.pk_proveedor,
    max(pk_diasvida) pk_diasvida,
    A.ComplexEntityNo,
    A.GRN,
    A.FechaAlojamiento,
    A.EdadConsolidado,
    COALESCE(Sem1.CantSem1, 0) AS CantidadSem1,
    COALESCE(Sem1.CantSem2, 0) AS CantidadSem2,
    COALESCE(Sem1.CantSem3, 0) AS CantidadSem3,
    COALESCE(Sem1.CantSem4, 0) AS CantidadSem4,
    COALESCE(Sem1.CantSem5, 0) AS CantidadSem5,
    COALESCE(Sem1.CantSem6, 0) AS CantidadSem6,
    COALESCE(SemMas.CantSemMas, 0) AS CantidadSemMas,
    COALESCE(Limpieza.CantLimpieza, 0) AS CantidadLimpieza,
    COALESCE(Total.CantTotal, 0) AS CantidadTotal,
    COALESCE(Sem1.Cant_1000Sem1, 0) AS `Cantidad/1000Sem1`,
    COALESCE(Sem1.Cant_1000Sem2, 0) AS `Cantidad/1000Sem2`,
    COALESCE(Sem1.Cant_1000Sem3, 0) AS `Cantidad/1000Sem3`,
    COALESCE(Sem1.Cant_1000Sem4, 0) AS `Cantidad/1000Sem4`,
    COALESCE(Sem1.Cant_1000Sem5, 0) AS `Cantidad/1000Sem5`,
    COALESCE(Sem1.Cant_1000Sem6, 0) AS `Cantidad/1000Sem6`,
    COALESCE(SemMas.Cant_1000SemMas, 0) AS `Cantidad/1000SemMas`,
    COALESCE(Limpieza.Cant_1000Limpieza, 0) AS `Cantidad/1000Limpieza`,
    COALESCE(Total.Cant_1000Total, 0) AS `Cantidad/1000Total`,
    COALESCE(Sem1.ValorSem1, 0) AS ValorSem1,
    COALESCE(Sem1.ValorSem2, 0) AS ValorSem2,
    COALESCE(Sem1.ValorSem3, 0) AS ValorSem3,
    COALESCE(Sem1.ValorSem4, 0) AS ValorSem4,
    COALESCE(Sem1.ValorSem5, 0) AS ValorSem5,
    COALESCE(Sem1.ValorSem6, 0) AS ValorSem6,
    COALESCE(SemMas.ValorSemMas, 0) AS ValorSemMas,
    COALESCE(Limpieza.ValorLimpieza, 0) AS ValorLimpieza,
    COALESCE(Total.ValorTotal, 0) AS ValorTotal,    
    COALESCE(MAX(PobInicial), 0) AS PobInicial
    ,'' categoria           
    ,to_date(FechaConsolidado, 'yyyyMMdd') descripfecha        
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
    ,'' descripgrupoconsumo 
    ,'' descripsubgrupoconsumo
    ,'' descriptipoproducto 
    ,'' descripespecie      
    ,'' descripestado       
    ,'' descripadministrador
    ,'' descripproveedor    
    ,'' descripdiavida       
FROM {database_name_tmp}.Consumos4 A
LEFT JOIN CantidadesTotal Total ON A.ComplexEntityNo = Total.ComplexEntityNo AND A.pk_productoconsumo = Total.pk_productoconsumo
LEFT JOIN CantidadesSemana1 Sem1 ON A.ComplexEntityNo = Sem1.ComplexEntityNo AND A.pk_productoconsumo = Sem1.pk_productoconsumo
LEFT JOIN CantidadesSemanaMas SemMas ON A.ComplexEntityNo = SemMas.ComplexEntityNo AND A.pk_productoconsumo = SemMas.pk_productoconsumo
LEFT JOIN CantidadesLimpieza Limpieza ON A.ComplexEntityNo = Limpieza.ComplexEntityNo AND A.pk_productoconsumo = Limpieza.pk_productoconsumo
LEFT JOIN {database_name_gl}.lk_tiempo T ON to_date(A.FechaConsolidado, 'yyyyMMdd') = T.fecha
GROUP BY T.pk_tiempo,
    A.FechaConsolidado,
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
    A.pk_productoconsumo,
    A.pk_grupoconsumo,
    A.pk_subgrupoconsumo,
    A.pk_tipoproducto,
    A.pk_especie,
    A.pk_estado,
    A.pk_administrador,
    A.pk_proveedor,
    A.ComplexEntityNo,
    A.GRN,
    A.FechaAlojamiento,
    A.EdadConsolidado,
    Sem1.CantSem1,Sem1.CantSem2,Sem1.CantSem3,Sem1.CantSem4,Sem1.CantSem5,Sem1.CantSem6,
    SemMas.CantSemMas,Limpieza.CantLimpieza,Total.CantTotal,
    Sem1.Cant_1000Sem1,Sem1.Cant_1000Sem2,Sem1.Cant_1000Sem3,Sem1.Cant_1000Sem4,Sem1.Cant_1000Sem5,Sem1.Cant_1000Sem6,
    SemMas.Cant_1000SemMas,Limpieza.Cant_1000Limpieza,Total.Cant_1000Total,
    Sem1.ValorSem1,Sem1.ValorSem2,Sem1.ValorSem3,Sem1.ValorSem4,Sem1.ValorSem5,Sem1.ValorSem6,
    SemMas.ValorSemMas,Limpieza.ValorLimpieza,Total.ValorTotal    
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ft_consumo_semanalT"
}
df_ft_consumo_SemanalT.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_tmp}.ft_consumo_semanalT")
print('carga ft_consumo_semanalT',df_ft_consumo_SemanalT.count())
df_ft_consumo_semanal = spark.sql(f"""SELECT * from {database_name_tmp}.ft_consumo_semanalT
WHERE (date_format(DescripFecha,'yyyyMM') >= date_format(add_months(trunc(current_date, 'month'),-4),'yyyyMM') or pk_estado = 2) AND pk_empresa = 1
""")
print('carga df_ft_consumo_semanal', df_ft_consumo_semanal.count())
fechaactual = datetime.now().replace(day=1)
fecha_menos = fechaactual - relativedelta(months=4)
fecha_str = fecha_menos.strftime("%Y-%m-%d")
file_name_target31 = f"{bucket_name_prdmtech}ft_consumo_semanal/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de ft_consumo_semanal cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info("No se encontraron datos existentes en ft_consumo_semanal")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(((date_format(col("DescripFecha"), 'yyyyMM') >= fecha_str) | (col("pk_estado") == 2)) & (col("pk_empresa") == 1)))
    filtered_new_data = df_ft_consumo_semanal.filter((date_format(col("DescripFecha"), 'yyyyMM') >= fecha_str) | (col("pk_estado") == 2)).filter(col("pk_empresa") == 1)
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consumo_semanalTemporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_tmp}.ft_consumo_semanalTemporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/ft_consumo_semanalTemporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consumo_semanal"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consumo_semanal")
            
    print(f"agrega registros nuevos a la tabla ft_consumo_semanal : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla ft_consumo_semanal : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_tmp, Name='ft_consumo_semanalTemporal')
    print(f"Tabla ft_consumo_semanalTemporal eliminada correctamente de la base de datos '{database_name_tmp}'.")
else:
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_consumo_semanal"
    }
    df_ft_consumo_semanal.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_gl}.ft_consumo_semanal")
# Después de que todo haya finalizado, llama a commit() para confirmar el trabajo
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()