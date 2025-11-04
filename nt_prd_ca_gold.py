from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Definir el nombre del trabajo
JOB_NAME = "nt_prd_ca_gold"

# Inicializar GlueContext y Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Inicializar el trabajo de Glue
job = Job(glueContext)
job.init(JOB_NAME, {})

# Confirmar inicialización
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
database_name="default"
df_mortalidad = spark.sql(f"select distinct mortalityname nmortalidad,replace(B.ColumnName, 'U_', 'U_PE') irn ,1 idempresa \
                                    from {database_name}.si_BrimMortalities A left join {database_name}.si_MtSysUserFields B on A.MtSysUserFieldsIRN = B.IRN \
                                    where speciestype = 1 and B.ColumnName not like 'U_PE%'")
print('carga df_mortalidad')
additional_options = {
    "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_mortalidad"
}
df_mortalidad.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ca_mortalidad")
print('crea ca_mortalidad')
df_mortalidad2 = spark.sql(f"select MortalityName, B.ColumnName irn,1 idempresa \
                             from default.si_BrimMortalities A left join  default.si_MtSysUserFields B on A.MtSysUserFieldsIRN = B.IRN \
                             where speciestype = 1 and B.ColumnName like 'U_PE%' and B.ColumnName not in (select distinct irn from default.ca_mortalidad)")

print('carga df_mortalidad2')
df_mortalidad3 = df_mortalidad.union(df_mortalidad2)
print('unifica los df df_mortalidad y df_mortalidad2')
additional_options = {
    "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_mortalidad2"
}
df_mortalidad3.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ca_mortalidad2")
print('crea ca_mortalidad2')
df_mortalidad4 = spark.sql(f"select SUBSTRING(MortalityName, INSTR(MortalityName,'PV M ') + 5, LENGTH(MortalityName)) MortalityName, replace(B.ColumnName,'U_PVE', 'U_PE') irn ,1 idempresa \
from {database_name}.si_brimmortalities A \
left join {database_name}.si_mtsysuserfields B on A.MtSysUserFieldsIRN = B.IRN \
where speciestype = 2 and replace(B.ColumnName,'U_PVE', 'U_PE') not in (select distinct IRN from {database_name}.ca_mortalidad2)")
print('carga df_mortalidad4')
df_mortalidad5 = df_mortalidad3.union(df_mortalidad4)
print('unifica los df df_mortalidad3 y df_mortalidad4')
additional_options = {
    "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_mortalidadtemp"
}
df_mortalidad5.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ca_mortalidadtemp")
print('crea la temporal de la lk_causamortalidad')
df_LotePavoTemp1 = spark.sql(f"SELECT DISTINCT substring(b.ComplexEntityNo,1,length(b.ComplexEntityNo)-6) as clote,b.EntityNo as nlote,b.FarmNo as noplantel, \
            case when substring(d.Notes,1,2) IS NULL then '' else substring(d.Notes,1,2) end as Notes, \
            CONCAT(substring(b.EntityNo,1,2), '0' ,case when substring(d.Notes,2,1) IS NULL then '' else substring(d.Notes,2,1) end,substring(b.EntityNo,3,3)) as EntityNoNotes, \
            CONCAT(substring(b.EntityNo,1,2), '0' ,case when substring(d.Notes,2,1) IS NULL then '' else substring(d.Notes,2,1) end) as AhnoCampana \
            FROM {database_name}.si_brimfieldtrans a INNER JOIN \
            {database_name}.si_mvbrimentities b on a.ProteinEntitiesIRN=b.ProteinEntitiesIRN INNER JOIN \
            {database_name}.si_proteinfarms c on b.ProteinFarmsIRN=c.IRN INNER JOIN \
            {database_name}.si_proteinentities d on d.IRN = a.ProteinEntitiesIRN \
            WHERE c.FarmType=7 and b.SpeciesType=2 and EntityHistoryFlag = 0 and d.Notes is not null and  length(b.EntityNo)<>7")
            
df_LotePavoTemp2 = spark.sql(f"SELECT DISTINCT substring(b.ComplexEntityNo,1,length(b.ComplexEntityNo)-6) as clote,b.EntityNo as nlote,b.FarmNo as noplantel, \
            case when substring(d.Notes,1,2) IS NULL then '' else substring(d.Notes,1,2) end as Notes, b.EntityNo as EntityNoNotes, \
            substring(b.EntityNo,1,length(b.EntityNo)-3) as AhnoCampana \
            FROM {database_name}.si_brimfieldtrans a INNER JOIN \
            {database_name}.si_mvbrimentities b on a.ProteinEntitiesIRN=b.ProteinEntitiesIRN INNER JOIN \
            {database_name}.si_proteinfarms c on b.ProteinFarmsIRN=c.IRN INNER JOIN \
            {database_name}.si_proteinentities d on d.IRN = a.ProteinEntitiesIRN \
            WHERE c.FarmType=7 and b.SpeciesType=2 and EntityHistoryFlag = 0 and d.Notes is not null  and  length(b.EntityNo)=7")
            
df_LotePavoTemp = df_LotePavoTemp1.union(df_LotePavoTemp2)
            
additional_options = {
    "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_lotepavo"
}
df_LotePavoTemp.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name}.ca_lotepavo")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
import boto3

# Boto3 setup
s3 = boto3.resource('s3')

# Configuración inicial
path_lista = "s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/listatablas.txt"
lista_df = spark.read.option("delimiter", "|").format("csv").option("header", "true").load(path_lista)
database_name = "default"
database_name2 = "sf_gold"
bucket_name = "ue1stgtestas3dtl005-gold"

# Recorrer los registros obtenidos con collect()
for row in lista_df.collect():
 
    table_name = f"{row[0]}".lower()
    pk_name = f"{row[2]}"
    pk_join = f"{row[1]}"
    path_target = f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/{table_name}"
    temp_target_path = f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/{table_name}_temp"
    prefix_target = f"UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/{table_name}/"
    prefix_target_temp = f"UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/{table_name}_temp/"

    print(f"➡️ Ejecutando {table_name}")
    # Intentar cargar los datos fuente
    try:
        if table_name == "lk_empresa":
            source_df = spark.sql("SELECT * FROM (SELECT DISTINCT  CASE WHEN CompanyNo='PONEDORAS HL SAC' THEN  2 WHEN CompanyNo='SAN FERNANDO' THEN  1 END  cempresa, \
                                    CASE WHEN CompanyName = 'PONEDORAS REPRO SAC' then 'RINCONADA' ELSE CompanyName END as nempresa \
                                    FROM default.si_proteincompanies \
                                    UNION \
                                    SELECT 4,'SIN EMPRESA') \
                                    UNION \
                                    SELECT 3, 'CHIMU' \
                                    ORDER BY  1 ")
        elif table_name == "lk_mes":
            source_df = spark.sql("SELECT distinct idmes,idanio,mes,nmes,abrev FROM default.si_lk_mes")
        elif table_name == "lk_tiempo":
            source_df = spark.sql("SELECT distinct case when substring(fecha,2,1)='/' or substring(fecha,3,1)  = '/' then cast(concat(substring(cast (((((anio * 100) + mes) * 100)+dia) as varchar(8)),1,4), \
            '-',substring(cast (((((anio * 100) + mes) * 100)+dia) as varchar(8)),5,2),'-',substring(cast (((((anio * 100) + mes) * 100)+dia) as varchar(8)),7,2)) as date) \
            else cast(fecha as date) end fecha ,anio,trimestre,mes,semana,dia,diasemana,CONCAT('T',trimestre) ntrimestre,nmes,nmes3L,nsemana,ndiaSemana,ndia, \
            CONCAT(anio,case when mes>9 then mes else CONCAT('0',mes) end) as idmes,semanaReprod,semanaanioReprod,semanaIncub \
            FROM default.si_lk_tiempo")
        elif table_name == "lk_tipoproducto":
            source_df = spark.sql("SELECT 0 ctipoproducto , 'Sin Tipo Producto' ntipoproducto,'SAN FERNANDO' AS CompanyNo, 1 idempresa \
                                   UNION SELECT 1 ctipoproducto , 'Vivo' ntipoproducto,'SAN FERNANDO' AS CompanyNo, 1 idempresa \
                                   UNION SELECT 2 ctipoproducto , 'Beneficio' ntipoproducto,'SAN FERNANDO' AS CompanyNo, 1 idempresa \
                                   UNION SELECT 3 ctipoproducto , 'Empacado' ntipoproducto,'SAN FERNANDO' AS CompanyNo, 1 idempresa \
                                   UNION SELECT 4 ctipoproducto , 'Trozado' ntipoproducto,'SAN FERNANDO' AS CompanyNo, 1 idempresa ")
        elif table_name == "lk_subzona":
            source_df = spark.sql("SELECT DISTINCT GrowoutNo as csubzona, GrowoutName as nsubzona, IRN, '1' as Activeflag, 1 as idempresa FROM default.si_proteingrowoutcodes \
                                  UNION SELECT '0','Sin Subzona','00000000-0000-0000-0000-000000000000','1',1")
        elif table_name == "lk_zona":
            source_df = spark.sql("SELECT DISTINCT CountyNo as czona, CountyName as nzona,IRN,'1' as Activeflag, 1 as idempresa FROM default.si_proteincounties \
                                   UNION SELECT '0','Sin Zona','00000000-0000-0000-0000-000000000000','1',1")
        elif table_name == "lk_plantel":
            source_df = spark.sql("SELECT distinct PF.FarmNo cplantel,PF.FarmName nplantel, PF.FarmNo noplantel,PF.IRN,PF.Address1 direccion1,PF.Address2 direccion2,CASE WHEN ISNULL(LZ.PK_Zona)=true THEN 0 ELSE LZ.PK_Zona END PK_Zona, PF.ActiveFlag, PF.ProteinCostCentersIRN,LS.PK_Subzona,1 idempresa,UFC.Name Cluster,PV.VendorShortName duenho \
                             FROM default.si_proteinfarms PF \
                             LEFT JOIN default.lk_zona LZ ON CAST(LZ.IRN AS varchar(50)) = CAST(PF.ProteinCountiesIRN AS varchar(50)) \
                             LEFT JOIN default.lk_subzona LS ON LS.IRN = CAST(pf.ProteinGrowoutCodesIRN AS varchar(50)) \
                             LEFT JOIN default.si_brimfarms BF ON PF.IRN = BF.proteinfarmsirn \
                             LEFT JOIN default.si_mtsysuserfieldcodes UFC ON UFC.IRN = BF.U_Cluster \
                             LEFT JOIN default.si_mvproteinvendors PV ON PV.IRN = PF.ProteinVendorsIRN \
                             UNION \
                             SELECT 0,'Sin Plantel','0','00000000-0000-0000-0000-000000000000','Sin direccion 1','Sin direccion 2',(select PK_Zona from default.lk_zona where czona=0),true ,'00000000-0000-0000-0000-000000000000',(select PK_Subzona from default.lk_subzona where csubzona=0),1,'',''")
        elif table_name == "lk_galpon":
            source_df = spark.sql("SELECT distinct CONCAT(rtrim(b.cplantel),'-' ,rtrim(a.HouseNo)) as cgalpon,CONCAT(rtrim(b.cplantel),'-' ,rtrim(a.HouseNo)) as ngalpon,rtrim(a.HouseNo) as nogalpon ,rtrim(b.cplantel) as noplantel, a.HeadCapacity as capacidad, a.IRN, a.ActiveFlag, \
                                b.PK_Plantel, a.Length, a.Width, a.Area, 1 as idempresa FROM default.si_proteinhouses a left join default.lk_plantel b on cast(a.ProteinFarmsIRN as varchar(50))=b.IRN \
                                union all \
                                select '0' cgalpon,'0' ngalpon,'0' nogalpon,'0' noplantel, 0 capacidad,'00000000-0000-0000-0000-000000000000' IRN, true ActiveFlag, \
                                (select pk_plantel from default.lk_plantel where cplantel='0') PK_Plantel, 0 Length, 0 Width, 0 Area, 1 idempresa")
        
        elif table_name == "lk_division":
            source_df = spark.sql("SELECT DISTINCT DivisionNo cdivision, DivisionName ndivision,IRN,'1' activeflag FROM default.si_proteindivisions \
                                  UNION SELECT 0,'Sin Division','00000000-0000-0000-0000-000000000000','1'")
        elif table_name == "lk_lote":
            df_LoteTemp = spark.sql(f"SELECT DISTINCT CONCAT(rtrim(a.FarmNo),'-' ,rtrim(a.EntityNo)) AS clote,rtrim(a.EntityNo) as nlote,rtrim(a.FarmNo) as noplantel,'' as IRN, \
            '1' as ActiveFlag, case when isnull(b.pk_plantel) = true then 1 else b.pk_plantel end pk_plantel,1 as idempresa,LD.ndivision \
            FROM {database_name}.si_mvbrimentities a left join default.lk_Plantel b ON b.IRN like a.ProteinFarmsIRN \
            LEFT JOIN {database_name}.si_proteincostcenters PCC ON a.ProteinCostCentersIRN = PCC.IRN \
            LEFT JOIN {database_name}.lk_division LD ON LD.IRN = cast(PCC.ProteinDivisionsIRN as varchar(50)) \
            order by 1")
            
            additional_options = {
                "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_lote"
            }
            df_LoteTemp.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{database_name}.ca_lote")
            
            df_LoteFinalTemp = spark.sql(f"select distinct \
            a.clote \
            ,a.nlote \
            ,a.noplantel \
            ,IRN \
            ,activeflag \
            ,pk_plantel \
            ,idempresa \
            ,b.notes \
            ,case when b.clote is null then rtrim(a.clote) else concat(rtrim(a.noplantel),'-',rtrim(b.EntityNoNotes)) end clote2 \
            ,case when b.clote is null then rtrim(a.nlote) else rtrim(b.EntityNoNotes) end nlote2 \
            ,case when b.clote is null then rtrim(a.clote) else concat(rtrim(a.noplantel),'-',rtrim(b.AhnoCampana)) end PlantelCampana \
            from {database_name}.ca_lote a \
            left join {database_name}.ca_lotepavo b on a.clote = b.clote")
            
            additional_options = {
                "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_lotefinal"
            }
            df_LoteFinalTemp.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{database_name}.ca_lotefinal")
            
            df_LoteReproductoraTemp = spark.sql(f"SELECT DISTINCT  a.ComplexEntityNo as clote ,rtrim(a.EntityNo) as nlote, rtrim(a.FarmNo) noplantel,'' as IRN,'1' as ActiveFlag, \
            case when isnull(b.pk_plantel) = true then (select pk_plantel from {database_name}.lk_plantel where cplantel='Sin Plantel') \
            else b.pk_plantel end pk_plantel, 1 as idempresa,'' as Notes,a.ComplexEntityNo as clote2,rtrim(a.EntityNo) as nlote2, a.ComplexEntityNo as PlantelCampana \
            FROM {database_name}.si_mvbimentities a left join default.lk_Plantel b ON b.IRN = a.ProteinFarmsIRN \
            WHERE a.GRN='F' \
            union \
            SELECT '0' as clote ,'0' as nlote, '0' noplantel, '00000000-0000-0000-0000-000000000000' as IRN,'1' as ActiveFlag, \
            (select pk_plantel from {database_name}.lk_plantel where cplantel='Sin Plantel') pk_plantel,1 as idempresa,null as Notes,'0' clote2,'0' nlote2,'' PlantelCampana ")
            
            additional_options = {
                "path": f"s3://ue1stgtestas3dtl005-gold/UE1STGTESTAS3PRD001/MTECH/SAN_FERNANDO/MAESTROS/Temp/ca_lotereproductora"
            }
            df_LoteReproductoraTemp.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{database_name}.ca_lotereproductora")
            df_Lote = spark.sql(f"SELECT distinct * FROM {database_name}.ca_lotefinal \
            union \
            Select distinct * from {database_name}.ca_lotereproductora")
    
            source_df = df_Lote
        elif table_name == "lk_semanavida":
            source_df = spark.sql("SELECT DISTINCT csemanavida, nsemanavida FROM default.si_lk_semanavida order by 1")
        elif table_name == "lk_diasvida":
            source_df = spark.sql("SELECT DISTINCT CASE WHEN (CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                                   WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                                   ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                                              END) > 9 \
                                                        THEN CONCAT('D',(CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                                              WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                                              ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                                                         END)) \
                                                        ELSE CONCAT('D0',(CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                                               WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                                               ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                                                          END)) \
                                                    END cdiavida, \
                                CONCAT('Dia ',(CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                    WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                    ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                               END)) nodiavida, \
                                CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                     WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                     ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                END FirstHatchDateAge, \
                                CASE WHEN  (CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                 WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                 ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                                 END) = 0 THEN 1 \
                                     ELSE cast(CEIL((CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                                     WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                                     ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                    END) / 7.0) + 1 as int) end PK_SemanaVida  \
                                FROM default.si_brimfieldtrans a inner join default.si_mvbrimentities b on a.ProteinEntitiesIRN = b.ProteinEntitiesIRN \
                                WHERE CASE WHEN isnull(FirstHatchDate) = true THEN 0 \
                                           WHEN date_format(cast(FirstHatchDate as timestamp),'yyyy-MM-dd') = '1899-11-30' THEN 0 \
                                           ELSE DateDiff(date_format(cast(a.xdate as timestamp),'yyyy-MM-dd'),date_format(cast(b.FirstHatchDate as timestamp),'yyyy-MM-dd')) \
                                      END >= 0 \
                                ORDER BY 3")
        elif table_name == "lk_sexo":
            source_df = spark.sql("SELECT  0 csexo,'Sin Sexo' nsexo,'00-Sin Sexo' IRN \
                                   UNION SELECT  1 csexo,'Hembra'   nsexo,'01-Hembra' IRN \
                                   UNION SELECT  2 csexo,'Macho'    nsexo,'02-Macho'  IRN \
                                   UNION SELECT  3 csexo,'Mixto'    nsexo,'03-Mixto' IRN")
        elif table_name == "lk_administrador":
            source_df = spark.sql("SELECT DISTINCT TechSupervisorNo as cadministrador, TechSupervisorname as nadministrador, IRN , U_Administrador as abrevadministrador,FarmType as tipo, 1 as idempresa \
                                  FROM default.si_proteintechsupervisors UNION SELECT 0,'Sin Administrador','00000000-0000-0000-0000-000000000000','Sin. Admin',1,1 ORDER BY 1")
        elif table_name == "lk_standard":
            source_df = spark.sql("SELECT DISTINCT StandardNo cstandard, StandardName nstandard, IRN, SpeciesType, '1' activeflag, 1 AS idempresa,CASE WHEN StandardNo like '%AD%' THEN 'Adulto' WHEN StandardNo like '%JO%' THEN 'Joven' ELSE '-' END AS lstandard \
                              FROM default.si_proteinstandards UNION SELECT '0','Sin Standard','00000000-0000-0000-0000-000000000000',0,'1',1,'-' order by 1")
        elif table_name == "lk_grupoconsumo":
#            source_df = spark.sql("select distinct cgrupoconsumo, ngrupoconsumo, idempresa, IRN, cast(activeflag as boolean) as activeflag \
#                                    FROM default.si_lk_grupoconsumo \
#                                   union select  0 as cgrupoconsumo,'Sin Grupo Consumo' as ngrupoconsumo,1 AS idempresa,'' as irn,True as activeflag")
            source_df = spark.sql(f"select distinct cgrupoconsumo, ngrupoconsumo, idempresa, IRN, cast(activeflag as boolean) as activeflag \
                                    FROM default.si_lk_grupoconsumo")
        elif table_name == "lk_producto":
            source_df = spark.sql(f"SELECT cproducto, nproducto, \
            CASE WHEN cproducto IN ('47665','47677','47690','47704','58089','47719','47724','PVHE (CD)') THEN 'EMPACADO' \
                 WHEN cproducto IN ('47738','47753','57571','57587','58052','58070','121774','121775','121776','121777','121778','121779','121780','121781','58105','121773','PVMA (GH1H2)','PVMA (DEF)') \
                     THEN 'TROZADO' ELSE grupoproducto END AS grupoproducto, IRNProteinProductsAnimals, irn, activeflag, ProductType, \
            CASE WHEN cproducto IN ('47665','47677','47690','47704','58089','47719','47724','PVHE (CD)') THEN 'EMPACADO' \
                 WHEN cproducto IN ('47738','47753','57571','57587','58052','58070','121774','121775','121776','121777','121778','121779','121780','121781','58105','121773','PVMA (GH1H2)','PVMA (DEF)') \
                     THEN 'TROZADO' ELSE grupoproductosalida END AS grupoproductosalida, idempresa \
                                    FROM ( \
                                                    SELECT DISTINCT PP.ProductNo cproducto,Description nproducto, \
                                                           (CASE PP.ProductNo WHEN 'PO V H' THEN 'VIVO' WHEN 'PO V M'  THEN 'VIVO'ELSE 'BENEFICIO' end) grupoproducto, \
                                                           nvl(CAST(PPA.IRN AS varchar(50)),'NONE') IRNProteinProductsAnimals, \
                                                           PP.IRN,1 activeflag,nvl(GC.pk_grupoconsumo,12) AS ProductType, \
                                                           (CASE Description WHEN '%BENEF%' THEN 'BENEFICIO' ELSE 'VIVO' END) grupoproductosalida, \
                                                           1 AS idempresa \
                                                     FROM \
                                                          (select *,CASE WHEN ProductType = 6 AND Description like '%GAS%' THEN 'Gas' \
                                                                         WHEN ProductType = 6 AND Description like '%CASC%' THEN 'Cama' \
                                                                         WHEN ProductType = 6 AND Description like '%AGUA%' THEN 'Agua' \
                                                                         WHEN ProductType = 6 AND Description like '%KRAFT%' THEN 'Kraft' \
                                                                         WHEN ProductType = 12 THEN 'Medicina' \
                                                                         ELSE 'Sin Sub Grupo Consumo' END SubGrupoConsumo \
                                                                         from default.si_proteinproducts \
                                                          ) PP LEFT JOIN default.si_proteinproductsanimals PPA ON PPA.ProteinProductsIRN = PP.IRN \
                                                    LEFT JOIN default.lk_grupoconsumo GC ON PP.ProductType = GC.cgrupoconsumo \
                                            ) X \
                                    UNION \
                                    select '0' cproducto,'0' nproducto,'0' grupoproducto,'0' IRNProteinProductsAnimals,'0' IRN,1 activeflag, 1 ProductType, \
                                           '0' grupoproductosalida,1 idempresa \
                                    order by 1")
#            source_df = spark.sql("SELECT DISTINCT PP.ProductNo cproducto,Description nproducto, \
#                             (CASE PP.ProductNo WHEN 'PO V H' THEN 'VIVO' WHEN 'PO V M'  THEN 'VIVO'ELSE 'BENEFICIO' end) grupoproducto, \
#                             CASE WHEN ISNULL(CAST(PPA.IRN AS varchar(50))) = true THEN 'NONE' ELSE CAST(PPA.IRN AS varchar(50)) END  IRNProteinProductsAnimals, \
#                             PP.IRN,1 activeflag,gc.pk_grupoconsumo PK_Grupoconsumo, \
#                             (CASE Description WHEN '%BENEF%' THEN 'BENEFICIO' ELSE 'VIVO' END) grupoproductosalida,1 AS idempresa \
#                              FROM default.si_proteinproducts PP LEFT JOIN default.si_proteinproductsanimals PPA ON PPA.ProteinProductsIRN = PP.IRN \
#                              LEFT JOIN default.lk_grupoconsumo GC ON PP.ProductType = GC.cgrupoconsumo \
#                              UNION \
#                              select '0' cproducto,'0' nproducto,'0' grupoproducto,'0' IRNProteinProductsAnimals,'0' IRN,1 activeflag, 1 PK_Grupoconsumo,'0' grupoproductosalida,'1' idempresa \
#                              order by 1")
        elif table_name == "lk_estado":
            source_df = spark.sql("SELECT DISTINCT status cestado, CASE WHEN status = 1 then 'Activo' WHEN status = 2 then 'Inactivo' WHEN status = 3 then 'Cerrado' else 'Sin Estado' end nestado, \
                                  CASE WHEN status = 1 then 'Activo' WHEN status = 2 then 'Inactivo' WHEN status = 3 then 'Cerrado' else 'Sin Estado' end IRN, 1 AS Activeflag \
                                  FROM default.si_proteinentities union select distinct 0,'Sin Estado','Sin Estado','1' order by 1")
        elif table_name == "lk_especie":
            source_df = spark.sql("SELECT DISTINCT BreedNo cespecie,BreedName nespecie,SpeciesType tipo,IRN,ProteinBreedCodeTypesIRN,ActiveFlag,1 AS idempresa FROM default.si_proteinbreedcodes \
                                   UNION SELECT '0' cespecie,'0' nespecie,0 tipo,'00000000-0000-0000-0000-000000000000' IRN,'00000000-0000-0000-0000-000000000000' ProteinBreedCodeTypesIRN,true ActiveFlag,1  idempresa order by 1")
        elif table_name == "lk_alimento":
            source_df = spark.sql("SELECT DISTINCT FeedTypeNo calimento,FeedTypeName nalimento,cast(IRN as varchar(50)) IRN,CreationDate,1 AS idempresa FROM default.si_fmimfeedtypes \
                                   UNION SELECT '0','SIN ALIMENTO','00000000-0000-0000-0000-000000000000','2010-06-04 11:39:11.743',1 order by 1")
        elif table_name == "lk_proveedor":
            source_df = spark.sql("SELECT DISTINCT CASE WHEN isnull(VendorNo) = true THEN 0 ELSE VendorNo END  cproveedor, CASE WHEN isnull(VendorName)=true THEN 'Sin Proveedor' ELSE VendorName END  nproveedor, 1 AS idempresa \
                                   FROM default.si_mvbrimfarms UNION SELECT 0, 'Sin Proveedor',1 order by 1")
        elif table_name == "lk_formula":
            source_df = spark.sql("SELECT distinct MFF.FormulaNo cformula,MFF.FormulaName nformula,MFF.IRN,LA.pk_alimento,MFF.ProteinProductsIRN,ActiveFlag, 1 AS idempresa FROM default.si_mvfmimfeedformulas MFF \
                                   LEFT JOIN default.lk_alimento LA ON LA.IRN = CAST(MFF.FmimFeedTypesIRN AS VARCHAR(50)) UNION \
                                SELECT 0, 'Sin Formula','00000000-0000-0000-0000-000000000000',(select distinct  pk_alimento from default.lk_alimento where calimento='0'),'00000000-0000-0000-0000-000000000000',true,1 order by 1")
        elif table_name == "lk_subgrupoconsumo":
            source_df = spark.sql("SELECT 0 csubgrupoconsumo,'Sin Sub Grupo Consumo' nsubgrupoconsumo,'Sin Sub Grupo Consumo' irn,1 activeflag, 1 idempresa \
                                   UNION \
                                   SELECT 1 csubgrupoconsumo,'Gas' nsubgrupoconsumo,'Gas' irn,1 activeflag, 1 idempresa \
                                   UNION \
                                   SELECT 2 csubgrupoconsumo,'Cama' nsubgrupoconsumo,'Cascara' irn,1 activeflag, 1 idempresa \
                                   UNION \
                                   SELECT 3 csubgrupoconsumo,'Agua' nsubgrupoconsumo,'Agua' irn,1 activeflag, 1 idempresa \
                                   UNION \
                                   SELECT 4 csubgrupoconsumo,'Kraft' nsubgrupoconsumo,'Kraft' irn,1 activeflag, 1 idempresa \
            UNION SELECT 5 csubgrupoconsumo,'Medicina' nsubgrupoconsumo,'Medicina' irn,1 activeflag, 1 idempresa")
        elif table_name == "lk_productoconsumo":
            source_df = spark.sql("SELECT distinct PP.productNo cproductoconsumo,SUBSTRING(PP.Description,1,50) nproductoconsumo,PP.IRN,PP.ActiveFlag, \
            GC.pk_grupoconsumo pk_grupoconsumo, 1 idempresa,pk_subgrupoconsumo \
            FROM (select *,CASE WHEN ProductType = 6 AND Description like '%GAS%' THEN 'Gas' \
            WHEN ProductType = 6 AND Description like '%CASC%' THEN 'Cama' \
            WHEN ProductType = 6 AND Description like '%AGUA%' THEN 'Agua' \
            WHEN ProductType = 6 AND Description like '%KRAFT%' THEN 'Kraft' \
            WHEN ProductType = 12 THEN 'Medicina' \
            ELSE 'Sin Sub Grupo Consumo' END SubGrupoConsumo from default.si_proteinproducts) PP \
            LEFT OUTER JOIN  default.si_proteinproductsanimals PPA ON PPA.ProteinProductsIRN =PP.IRN \
            LEFT JOIN default.lk_grupoconsumo GC ON PP.ProductType = GC.cgrupoconsumo AND GC.idempresa = 1 \
            LEFT JOIN default.lk_subgrupoconsumo SGC ON PP.SubGrupoConsumo = SGC.nsubgrupoconsumo \
            WHERE ProteinProductsIRN IS NULL \
            UNION \
            select 0,'Sin Productoconsumo','00000000-0000-0000-0000-000000000000',true,(select distinct pk_grupoconsumo from default.lk_grupoconsumo where cgrupoconsumo=0),1,(select distinct pk_subgrupoconsumo from default.lk_subgrupoconsumo where csubgrupoconsumo='0') \
            order by 1")
        elif table_name == "lk_generacion":
            source_df = spark.sql("select 0 cgeneracion,'Sin Generación' ngeneracion \
                                    union select 4 cgeneracion,'Abuelos' ngeneracion \
                                    union select 5 cgeneracion,'Padres' ngeneracion")
        elif table_name == "lk_etapa":
            source_df = spark.sql("select 'C' cetapa,'Cría' netapa union \
            select 'E' cetapa,'Engorde' netapa union \
            select 'P' cetapa,'Postura' netapa union \
            select 'R' cetapa,'Recría' netapa union \
            select 'SE' cetapa,'Sin Etapa' netapa  ")
        elif table_name == "lk_incubadora":
            source_df = spark.sql("select distinct HatcheryNo cincubadora,HatcheryName nincubadora,HatcheryNo noincubadora,IRN,Activeflag,ProteinCostCentersIRN,1 AS idempresa from default.si_proteinfacilityhatcheries \
                                  UNION SELECT '0', 'Sin Incubadora','0','00000000-0000-0000-0000-000000000000',true,'00000000-0000-0000-0000-000000000000',1 order by 1")
        elif table_name == "lk_conductor":
            source_df = spark.sql("SELECT distinct DriverNo cconductor, DriverName nconductor, IRN, activeflag,1 idempresa FROM default.si_proteindrivers \
                                   UNION SELECT '0', 'Sin Conductor','00000000-0000-0000-0000-000000000000',true,1")
        elif table_name == "lk_vehiculo":
            source_df = spark.sql("SELECT distinct rtrim(VehicleNo) cvehiculo, rtrim(VehicleName) nvehiculo,rtrim(VehicleName) marca ,IRN, activeflag,1 AS idempresa FROM default.si_proteinvehicles \
                                   UNION SELECT '0', 'Sin Vehiculo','Sin Vehiculo','00000000-0000-0000-0000-000000000000',true,1")
        elif table_name == "lk_maquinaincubadora":
            source_df = spark.sql("SELECT DISTINCT CONCAT(rtrim(hatcheryno),'-',rtrim(setterno)) cmaquinaincubadora ,setterno, settername,himsettersirn,rtrim(hatcheryno) hatcheryno,pk_incubadora, 1 as idempresa \
                                    FROM default.si_mvhimhatchtrans A \
                                    LEFT JOIN default.si_himhatchtrans B on A.IRN = B.IRN \
                                    LEFT JOIN default.lk_incubadora C on rtrim(C.cincubadora) = rtrim(A.hatcheryno) \
                                    WHERE himsettersirn is not null \
                                    union select '0','0','0','0','0',1,1 \
                                    ORDER BY hatcheryno")
        elif table_name == "lk_maquinanacedora":
            source_df = spark.sql(f"SELECT DISTINCT CONCAT(rtrim(hatcheryno),'-',rtrim(hatcherno)) cmaquinanacedora ,hatcherno as nmaquinanacedora, hatchername as nomaquinanacedora, himhatchersirn,rtrim(hatcheryno) as cincubadora,pk_incubadora, 1 as idempresa \
                                    FROM default.si_mvhimhatchtrans A \
                                    LEFT JOIN default.si_himhatchtrans B on A.IRN = B.IRN \
                                    LEFT JOIN default.lk_incubadora C on rtrim(C.cincubadora) = rtrim(A.hatcheryno) \
                                    WHERE himhatchersirn IS NOT NULL \
                                    union select '0','0','0','0','0',1,1 \
                                    ORDER BY cincubadora")
        elif table_name == "lk_ft_excel_incub_std_raza":
            source_df = spark.sql("select distinct concat(raza,edad) crazaedad,* from default.si_ft_excel_incub_std_raza")
        elif table_name == "lk_ft_excel_categoria":
            source_df = spark.sql("select distinct b.pk_lote, a.lote,a.categoria from default.si_ft_excel_categoria a left join default.lk_lote b on a.lote = b.clote ")
        elif table_name == "lk_causamortalidad":
            source_df = spark.sql("select distinct '' as cmortalidad,nmortalidad as nmortalidad,replace(irn,'í','i') as irn ,idempresa as idempresa from default.ca_mortalidadtemp")
        elif table_name == "lk_planta":
            source_df = spark.sql("SELECT distinct PlantNo cplanta, PlantName nplanta,ProteinCostCentersIRN, activeflag,1 AS pk_empresa,IRN FROM default.si_proteinfacilityplants \
                                   UNION SELECT '0', 'Sin Planta','00000000-0000-0000-0000-000000000000',true,1,'00000000-0000-0000-0000-000000000000'")
        elif table_name == "lk_duenho":
            source_df = spark.sql("select distinct rtrim(VendorNo) cduenho, rtrim(VendorName) nduenho, rtrim(VendorShortName) nduenhocorto, 1 as idempresa \
                                    from default.si_mvproteinvendors UNION \
                                    select 0 cduenho, 'Sin duenho' nduenho, 'Sin duenho' nduenhocorto, 1 as idempresa")
        
    except Exception as e:
        print(f"⚠️ Error al ejecutar la consulta para {table_name}: {e}")
        print(f"Saltando al siguiente registro...\n")
        continue  # Salta al siguiente registro sin detener el script
    
    try:
        df_existentes = spark.read.format("parquet").load(path_target)
        datos_existentes = True
    except:
        datos_existentes = False
    
    if datos_existentes:
        # Agregar columna de clave primaria
        if pk_name.strip():  # Verifica si pk_name no está vacío ni contiene solo espacios
            source_df = source_df.withColumn(f"{pk_name}", expr(f"ROW_NUMBER() OVER (ORDER BY {pk_join})"))

        if table_name == "lk_causamortalidad":
            # Intentar cargar el archivo destino si existe
            target_df = spark.read.parquet(path_target)
            print(f"Archivo destino cargado desde: {path_target}")
            max_pk = target_df.agg(F.max(f"{pk_name}".lower())).collect()[0][0] or 0
            target_df2 = target_df.select(F.col("cmortalidad"),F.col("nmortalidad"), F.col("irn"), F.col("idempresa")).orderBy("pk_causamortalidad")
            # Realizar uniones basadas en la clave primaria
            updated_df = source_df
            unchanged_df = updated_df.join(target_df2, pk_join, "left_anti")
            unchanged_df = unchanged_df.withColumn(f"{pk_name}", expr(f"ROW_NUMBER() OVER (ORDER BY {pk_join})") + max_pk )
            final_df = target_df.union(unchanged_df.select(F.col("cmortalidad"),F.col("nmortalidad"), F.col("irn"), F.col("idempresa"), F.col("pk_causamortalidad")))  
        else:
            # Intentar cargar el archivo destino si existe
            target_df = spark.read.parquet(path_target)
            print(f"Archivo destino cargado desde: {path_target}")
            # Realizar uniones basadas en la clave primaria
            updated_df = source_df
            unchanged_df = target_df.join(source_df, pk_join, "left_anti")
            final_df = updated_df.union(unchanged_df)
    else:
        # Si no existe, inicializar como un DataFrame vacío con el esquema del origen
        print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
        if pk_name.strip():  # Verifica si pk_name no está vacío ni contiene solo espacios
            source_df = source_df.withColumn(f"{pk_name}", expr(f"ROW_NUMBER() OVER (ORDER BY {pk_join})"))
        final_df = source_df

    # Guardar en carpeta temporal
    final_df.write.mode("overwrite").parquet(temp_target_path)

    # Eliminar carpeta destino en S3
    g_bucket = s3.Bucket(bucket_name)
    g_bucket.objects.filter(Prefix=prefix_target).delete()

    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(temp_target_path)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{database_name}.{table_name}")

    # Contar registros y finalizar
    record_count = df_temp.count()

    # Eliminar carpeta temporal
    g_bucket.objects.filter(Prefix=prefix_target_temp).delete()    

    print(f"Registros procesados: {record_count}")
    print(f"Tabla registrada en el catálogo: {database_name}.{table_name}")  
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()