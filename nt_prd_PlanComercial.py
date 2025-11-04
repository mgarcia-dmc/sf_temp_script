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
JOB_NAME = "nt_prd_PlanComercial"

# Inicializa el trabajo de Glue
job = Job(glueContext)  # Crea una instancia del trabajo de Glue
job.init(JOB_NAME, {})  # Inicializa el trabajo con el nombre

# Mensaje para confirmar que todo está listo
print(f"Glue Job '{JOB_NAME}' inicializado correctamente.")
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp,date_format,to_date,col
from datetime import datetime,timedelta
print("inicia spark")
# Parámetros de entrada global
import boto3
ssm = boto3.client("ssm")
 
## Parametros Globales 
amb             = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
db_sf_costo_tmp = ssm.get_parameter(Name='p_db_prd_sf_costo_tmp', WithDecryption=True)['Parameter']['Value']
db_sf_costo_gl  = ssm.get_parameter(Name='p_db_prd_sf_costo_gl', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_si  = ssm.get_parameter(Name='p_db_prd_sf_costo_si', WithDecryption=True)['Parameter']['Value']
#db_sf_costo_br  = ssm.get_parameter(Name='p_db_prd_sf_costo_br', WithDecryption=True)['Parameter']['Value']

#db_sf_pec_tmp = ssm.get_parameter(Name='p_db_prd_sf_pec_tmp', WithDecryption=True)['Parameter']['Value']
db_sf_pec_gl  = ssm.get_parameter(Name='p_db_prd_sf_pec_gl', WithDecryption=True)['Parameter']['Value']
db_sf_pec_si  = ssm.get_parameter(Name='p_db_prd_sf_pec_si', WithDecryption=True)['Parameter']['Value']

db_sf_fin_gold   = ssm.get_parameter(Name='db_fin_gold', WithDecryption=True)['Parameter']['Value']
#db_sf_fin_silver = ssm.get_parameter(Name='db_fin_silver', WithDecryption=True)['Parameter']['Value']

ambiente = f"{amb}"
bucket_name_target = f"ue1stg{ambiente}as3dtl005-gold"
bucket_name_prdmtech = f"UE1STG{ambiente.upper()}AS3PRD001/MTECH/SAN_FERNANDO/TRANSACCIONALES_COSTOS/"
#bucket_name_prdmtech2 = f"UE1STG{ambiente.upper}AS3PRD001/MTECH/SAN_FERNANDO/MAESTROS_COSTOS/"

database_name_costos_tmp = db_sf_costo_tmp
database_name_costos_gl  = db_sf_costo_gl
#database_name_costos_si  = db_sf_costo_si
#database_name_costos_br   = db_sf_costo_br

#database_name_tmp = db_sf_pec_tmp
database_name_gl  = db_sf_pec_gl
database_name_si  = db_sf_pec_si

db_sap_fin_gl = db_sf_fin_gold
#db_sap_fin_si = db_sf_fin_silver
print('cargando ruta ',bucket_name_prdmtech)
df_UPD_KE24 = spark.sql(f"""
select 
 mon                 
,periodo             
,per                 
,cebe                
,articulo            
,ce                  
,ndocref            
,docanul             
,fefactura           
,fecontab            
,creadoel            
,pedclte             
,nposr               
,cloper              
,case when ingresovta is null then 0 when ingresovta = '' then 0 else ingresovta end ingresovta
,case when ingresos is null then 0 when ingresos = '' then 0 else ingresos end ingresos
,case when otrosingr is null then 0 when otrosingr = '' then 0 else otrosingr end otrosingr
,case when cvtasreal is null then 0 when cvtasreal = '' then 0 else cvtasreal end cvtasreal
,case when cvtasrefer is null then 0 when cvtasrefer = '' then 0 else cvtasrefer end cvtasrefer 
,case when otrosdesc is null then 0 when otrosdesc = '' then 0 else otrosdesc end otrosdesc
,descuentos          
,ajcvotro            
,otrosegre           
,ajustemtech         
,cstvtas             
,case when cantfactub is null then 0 when cantfactub = '' then 0 else cantfactub end cantfactub            
,umb                 
,case when pesofactum is null then 0 when pesofactum = '' then 0 else pesofactum end pesofactum
,umb2                
,monex               
,cantfactum          
,umb3                
,cantentcwm          
,mermacant           
,ndocum              
,creadopor           
,soc                 
,objetopa            
,operref             
,clfac               
,ofvta               
,ramo                
,zv                  
,orgvt               
,cdis                
,se                  
,vendedor            
,cliente             
,gven                
,llaveov_cd          
,departamento        
,deccebe             
,lprecio             
,grclientepedido     
,jerarquiaproducto   
,grupoarticulo       
,tipodoccomercial    
,anulada             
,denominacion        
,preciobase          
,escontabilizado     
,year                
,0 precio_interno_periodico 
,cvtasrefer_real     
,mescostor           
,aniocostor    
FROM {db_sap_fin_gl}.KE24
where month(cast(Fecontab as date)) = month(current_date-1) and year(cast(Fecontab as date)) = year(current_date-1)
""")
print("carga KE24 --> Registros procesados:", df_UPD_KE24.count())
table_name = 'KE24'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

fecha_menos = datetime.today()+timedelta(days=-1)
fecha_str = fecha_menos.strftime("%Y%m")

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(date_format(to_date(col("Fecontab"), "yyyy-MM-dd"), "yyyyMM") == fecha_str))
    filtered_new_data = df_UPD_KE24.filter(date_format(to_date(col("Fecontab"), "yyyy-MM-dd"), "yyyyMM") == fecha_str)
    final_data = filtered_new_data.union(data_after_delete)                             
    
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_ponderado = spark.sql(f"""
WITH consolida AS (select 
 s.Articulo 
,s.Ejercicio 
,s.Per
,sum(s.Cant_CW) as Cant 
from {database_name_costos_gl}.KE24_CostosVenta s
group by s.Articulo,s.Ejercicio,s.Per)
,consolida1 AS (
SELECT 
 t.Articulo 
,t.Ejercicio 
,t.Per 
,((t.Precio_interno_periodico*t.Cant_CW)/COALESCE(b.Cant,0)) as cant 
FROM {database_name_costos_gl}.KE24_CostosVenta as t 
inner join consolida b on t.Articulo = b.Articulo and t.Ejercicio = b.Ejercicio and t.Per = b.Per)

select 
 Articulo 
,Ejercicio 
,Per 
,sum(Cant) as preciop 
from consolida1 
group by Articulo,Ejercicio ,Per
""")

# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/ponderado"
}
df_ponderado.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.ponderado")
print('carga df_ponderado',df_ponderado.count())
df_UPD2_KE24 = spark.sql(f"""
select 
 a.mon                 
,a.periodo             
,a.per                 
,a.cebe                
,a.articulo            
,a.ce                  
,a.ndocref            
,a.docanul             
,a.fefactura           
,a.fecontab            
,a.creadoel            
,a.pedclte             
,a.nposr               
,a.cloper              
,a.ingresovta
,a.ingresos
,a.otrosingr
,a.cvtasreal
,a.cvtasrefer 
,a.otrosdesc
,a.descuentos          
,a.ajcvotro            
,a.otrosegre           
,a.ajustemtech         
,a.cstvtas             
,a.cantfactub
,a.umb                 
,a.pesofactum
,a.umb2                
,a.monex               
,a.cantfactum          
,a.umb3                
,a.cantentcwm          
,a.mermacant           
,a.ndocum              
,a.creadopor           
,a.soc                 
,a.objetopa            
,a.operref             
,a.clfac               
,a.ofvta               
,a.ramo                
,a.zv                  
,a.orgvt               
,a.cdis                
,a.se                  
,a.vendedor            
,a.cliente             
,a.gven                
,a.llaveov_cd          
,a.departamento        
,a.deccebe             
,a.lprecio             
,a.grclientepedido     
,a.jerarquiaproducto   
,a.grupoarticulo       
,a.tipodoccomercial    
,a.anulada             
,a.denominacion        
,a.preciobase          
,a.escontabilizado     
,a.year                
,CASE WHEN  (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = month(current_date-1)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = year(current_date-1)) AND
            (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) - 1 = CAST(B.Per AS INT)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = CAST(B.Ejercicio AS INT)) AND
            B.Articulo IS NOT NULL THEN B.preciop ELSE A.Precio_interno_periodico
 END AS Precio_interno_periodico
,a.cvtasrefer_real     
,CASE
        WHEN
            (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = month(current_date-1)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = year(current_date-1)) AND
            (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) - 1 = CAST(B.Per AS INT)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = CAST(B.Ejercicio AS INT)) AND
            B.Articulo IS NOT NULL
        THEN B.Per
        ELSE A.MesCostoR
    END AS MesCostoR
,CASE
        WHEN
            (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = month(current_date-1)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = year(current_date-1)) AND
            (MONTH(TO_DATE(A.fecontab, 'yyyy-MM-dd')) - 1 = CAST(B.Per AS INT)) AND
            (YEAR(TO_DATE(A.fecontab, 'yyyy-MM-dd')) = CAST(B.Ejercicio AS INT)) AND
            B.Articulo IS NOT NULL
        THEN B.Ejercicio
        ELSE A.AnioCostoR
    END AS AnioCostoR
FROM {database_name_costos_gl}.KE24 a
LEFT JOIN {database_name_costos_tmp}.ponderado b on cast(a.Articulo as int) = cast(b.Articulo as int)
""")
print("carga UPD2 KE24 --> Registros procesados:", df_UPD2_KE24.count())
table_name = 'KE24'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD2_KE24 #filtered_new_data.union(data_after_delete)                             
   
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{bucket_name_target}'.")
df_UPD3_KE24 = spark.sql(f"""   
select 
 mon                 
,periodo             
,per                 
,cebe                
,articulo            
,ce                  
,ndocref            
,docanul             
,fefactura           
,fecontab            
,creadoel            
,pedclte             
,nposr               
,cloper              
,ingresovta          
,ingresos            
,otrosingr           
,cvtasreal           
,cvtasrefer          
,otrosdesc           
,descuentos          
,ajcvotro            
,otrosegre           
,ajustemtech         
,cstvtas             
,cantfactub          
,umb                 
,pesofactum          
,umb2                
,monex               
,cantfactum          
,umb3                
,cantentcwm          
,mermacant           
,ndocum              
,creadopor           
,soc                 
,objetopa            
,operref             
,clfac               
,ofvta               
,ramo                
,zv                  
,orgvt               
,cdis                
,se                  
,vendedor            
,cliente             
,gven                
,llaveov_cd          
,departamento        
,deccebe             
,lprecio             
,grclientepedido     
,jerarquiaproducto   
,grupoarticulo       
,tipodoccomercial    
,anulada             
,denominacion        
,preciobase          
,escontabilizado     
,year                
,precio_interno_periodico
,COALESCE(case when (Precio_interno_periodico is not null or Precio_interno_periodico<>0) then 
    (case
        WHEN  PesoFactUM=0 and CVtasRefer<> 0 then CantFactUB * Precio_interno_periodico
        when  PesoFactUM<> 0  then PesoFactUM * Precio_interno_periodico
    end) 
end , 0) CVtasRefer_real     
,mescostor           
,aniocostor
FROM {database_name_costos_gl}.KE24
where Fecontab >='2022-02-01'
union all
select
 mon                 
,periodo             
,per                 
,cebe                
,articulo            
,ce                  
,ndocref            
,docanul             
,fefactura           
,fecontab            
,creadoel            
,pedclte             
,nposr               
,cloper              
,ingresovta          
,ingresos            
,otrosingr           
,cvtasreal           
,cvtasrefer          
,otrosdesc           
,descuentos          
,ajcvotro            
,otrosegre           
,ajustemtech         
,cstvtas             
,cantfactub          
,umb                 
,pesofactum          
,umb2                
,monex               
,cantfactum          
,umb3                
,cantentcwm          
,mermacant           
,ndocum              
,creadopor           
,soc                 
,objetopa            
,operref             
,clfac               
,ofvta               
,ramo                
,zv                  
,orgvt               
,cdis                
,se                  
,vendedor            
,cliente             
,gven                
,llaveov_cd          
,departamento        
,deccebe             
,lprecio             
,grclientepedido     
,jerarquiaproducto   
,grupoarticulo       
,tipodoccomercial    
,anulada             
,denominacion        
,preciobase          
,escontabilizado     
,year                
,precio_interno_periodico
,COALESCE(CVtasRefer_real,0) CVtasRefer_real
,mescostor           
,aniocostor
FROM {database_name_costos_gl}.KE24
where Fecontab <'2022-02-01'   
""")
print("carga UPD KE24 --> Registros procesados:", df_UPD3_KE24.count())
table_name = 'KE24'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD3_KE24 #filtered_new_data.union(data_after_delete)                             
   
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{db_sap_fin_gl}'.")
df_dat = spark.sql(f""" 
SELECT   
    'ComercialCosto'  as descrip
    ,V2.DESCPJERARQUIA
    ,'DEL ' as del
    ,V1.DESCRIPDPTO
    ,PesoFactUM AS TON
    ,(IngresoVta+Ingresos+Otrosingr+OtrosDesc) AS PRECIO
    ,case when CVtasRefer_real <> 0 then CVtasRefer_real else 0 end AS COSTO
    ,Fecontab
    ,Mon
    ,V1.PROVINCIA
    ,'KG' as kg
    ,SUBSTRING(Ndocref,1,1) as doc
    ,MesCostoR
    ,AnioCostoR
FROM {database_name_costos_gl}.KE24 A
INNER join {database_name_costos_gl}.KE24_Vinculo V1 on A.ZV = V1.ZONAVTA
INNER JOIN {database_name_costos_gl}.KE24_Vinculo01 V2 on A.CEBE = V2.CEBE
WHERE ZV IS NOT NULL AND MONTH(TO_DATE(fecontab, 'yyyy-MM-dd')) = month(current_date-1) and year(TO_DATE(fecontab, 'yyyy-MM-dd')) = year(current_date-1)
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/dat"
}
df_dat.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.dat")
print("carga dat --> Registros procesados:", df_dat.count())
df_ft_plangerencial = spark.sql(f""" 
SELECT 
'ComercialCosto' Clase
,DESCPJERARQUIA Tipo
,'DEL' Periodo
,DESCRIPDPTO Plaza
,SUM(TON) Ton
,'' pplaza
,SUM(PRECIO) Precio
,SUM(COSTO) Costo
,'' Margen
,Fecontab Fecha
,Mon MONEDA
,PROVINCIA
,'' gasto               
,'' gunit               
,'' kilose              
,'' gastoa              
,'' gastoc              
,'' gastol
,'KG' Unidad
,'' articulo            
,'' kproyt              
,'' kproyd              
,'' saldo               
,'' pesoprom            
,'' edad                
,'' granja              
,'' galpon              
,'' corral              
,'' diascrianza         
,'' estado              
,'' region              
,'' pobinicial          
,'' farmno              
,'' obs                 
,'' pk_mes               
,'' fechacierre         
,'' ganancia            
,'' conversion          
,'' avesrendidasproy    
,'' pesopromproy        
,'' conversionh         
,'' cedescripcion       
,'' kilosrendidos       
,'' saldot              
,'' avesrendidas        
,'' saldomediano        
,'' saldou              
,'' pesom               
,'' pesot               
,'' saldototal          
,'' pesototal
,doc TDoc
,max(MesCostoR) MesCosto
,max(AnioCostoR) AnioCosto
,'' kproys              
,1 pk_empresa
FROM {database_name_costos_tmp}.dat
GROUP BY DESCPJERARQUIA
,DESCRIPDPTO
,Fecontab
,Mon
,PROVINCIA
,doc
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}ft_plangerencial"
}
df_ft_plangerencial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.ft_plangerencial")
print("carga ft_plangerencial --> Registros procesados:", df_ft_plangerencial.count())
#Gastos Informe comercial
df_UPD_ft_plangerencial = spark.sql(f""" 
select 
 Clase
,f.Tipo
,Periodo
,Plaza
,Ton
,pplaza
,Precio
,Costo
,Margen
,f.Fecha
,Moneda
,f.Provincia
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.Gasto,0) ELSE COALESCE(f.Gasto,0) END AS Gasto
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.Gunit,0) ELSE COALESCE(f.Gunit,0) END AS Gunit
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.Kilos,0) ELSE COALESCE(f.KilosE,0) END AS KilosE
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.GastoA,0) ELSE COALESCE(f.GastoA,0) END AS GastoA
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.GastoC,0) ELSE COALESCE(f.GastoC,0) END AS GastoC
,CASE WHEN G.Tipo IS NOT NULL AND G.Provincia IS NOT NULL AND G.EsGerencia = 1 THEN COALESCE(G.GastoL,0) ELSE COALESCE(f.GastoL,0) END AS GastoL
,Unidad
,articulo            
,kproyt              
,kproyd              
,saldo               
,pesoprom            
,edad                
,granja              
,galpon              
,corral              
,diascrianza         
,estado              
,region              
,pobinicial          
,farmno              
,obs                 
,pk_mes               
,fechacierre         
,ganancia            
,conversion          
,avesrendidasproy    
,pesopromproy        
,conversionh         
,cedescripcion       
,kilosrendidos       
,saldot              
,avesrendidas        
,saldomediano        
,saldou              
,pesom               
,pesot               
,saldototal          
,pesototal           
,tdoc                
,mescosto            
,aniocosto           
,kproys              
,pk_empresa       
FROM {database_name_costos_gl}.ft_plangerencial f
left join {database_name_costos_gl}.KE24_Gasto01 G ON f.Tipo = G.Tipo and f.Provincia = G.provincia
""")
print("carga UPD ft_plangerencial --> Registros procesados:", df_UPD_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD_ft_plangerencial #filtered_new_data.union(data_after_delete)                             
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_UPD2_ft_plangerencial = spark.sql(f""" 
WITH
 KproyT_PolloBeneficiado AS (SELECT SUM(Kilos) + SUM(Can) AS KproyT_Val FROM {db_sap_fin_gl}.PlanCarga WHERE OrgVt = '1002' and month(Fecha) = month(current_date-1) and year(Fecha) = year(current_date-1))
,KproyT_PolloVivo AS (SELECT SUM(Kilos) AS KproyT_Val FROM {db_sap_fin_gl}.PlanCarga WHERE OrgVt = '1001' and month(Fecha) = month(current_date-1) and year(Fecha) = year(current_date-1))
,KproyD_PolloVivo AS (SELECT SUM(Kilos) AS KproyD_Val FROM {db_sap_fin_gl}.PlanCargaD WHERE OrgVt = '1001' AND Fecha=  current_date-1)
,KproyD_PolloBeneficiado AS (SELECT SUM(Kilos) + SUM(Can) AS KproyD_Val FROM {db_sap_fin_gl}.PlanCargaD WHERE OrgVt = '1002' AND Fecha= current_date-1)
,KproyS_PolloVivo AS (SELECT SUM(Kilos) AS KproyS_Val FROM {db_sap_fin_gl}.PlanCargaD WHERE OrgVt = '1001' and Fecha >= current_date-9 and  Fecha <= current_date-2)
,KproyS_PolloBeneficiado AS (SELECT SUM(Kilos) + SUM(Can) AS KproyS_Val FROM {db_sap_fin_gl}.PlanCargaD WHERE OrgVt = '1002' and Fecha >= current_date-9 and  Fecha <= current_date-2)
SELECT
 Clase
,Tipo
,Periodo
,Plaza
,Ton
,pplaza
,Precio
,Costo
,Margen
,Fecha
,Moneda
,Provincia
,Gasto
,Gunit
,KilosE
,GastoA
,GastoC
,GastoL
,Unidad
,articulo
,CASE
    WHEN f.Tipo = 'POLLO BENEFICIADO' THEN COALESCE(kpt_ben.KproyT_Val, f.KproyT) -- Use original if join is NULL
    WHEN f.Tipo = 'POLLO VIVO' THEN COALESCE(kpt_vivo.KproyT_Val, f.KproyT)
    ELSE f.KproyT
 END AS KproyT
,CASE
    WHEN f.Tipo = 'POLLO VIVO' THEN COALESCE(kpd_vivo.KproyD_Val, f.KproyD)
    WHEN f.Tipo = 'POLLO BENEFICIADO' THEN COALESCE(kpd_ben.KproyD_Val, f.KproyD)
    ELSE f.KproyD
 END AS KproyD
,saldo               
,pesoprom            
,edad                
,granja              
,galpon              
,corral              
,diascrianza         
,estado              
,region              
,pobinicial          
,farmno              
,obs                 
,pk_mes               
,fechacierre         
,ganancia            
,conversion          
,avesrendidasproy    
,pesopromproy        
,conversionh         
,cedescripcion       
,kilosrendidos       
,saldot              
,avesrendidas        
,saldomediano        
,saldou              
,pesom               
,pesot               
,saldototal          
,pesototal
,TDoc
,MesCosto
,AnioCosto
,CASE
    WHEN f.Tipo = 'POLLO VIVO' THEN COALESCE(kps_vivo.KproyS_Val, f.KproyS)
    WHEN f.Tipo = 'POLLO BENEFICIADO' THEN COALESCE(kps_ben.KproyS_Val, f.KproyS)
    ELSE f.KproyS
 END AS KproyS              
,pk_empresa
FROM
    {database_name_costos_gl}.ft_plangerencial AS f
LEFT JOIN
    KproyT_PolloBeneficiado AS kpt_ben ON 1=1 -- Join on 1=1 for cross-join behavior, or use a dummy key if available
LEFT JOIN
    KproyT_PolloVivo AS kpt_vivo ON 1=1
LEFT JOIN
    KproyD_PolloVivo AS kpd_vivo ON 1=1
LEFT JOIN
    KproyD_PolloBeneficiado AS kpd_ben ON 1=1
LEFT JOIN
    KproyS_PolloVivo AS kps_vivo ON 1=1
LEFT JOIN
    KproyS_PolloBeneficiado AS kps_ben ON 1=1
""")
print("carga UPD 2 ft_plangerencial --> Registros procesados:", df_UPD2_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD2_ft_plangerencial
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")

    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
#Pecuario
df_pesoedaddetalle = spark.sql(f""" 
select 
substring(A.ComplexEntityNo,1,9) Granja, 
A.Peso,
0 PesoM,
0 PesoTipo,
A.SaldoAves SaldoAve,
0 SaldoAveT,
0 SaldoAveM,
descripfecha EventDate, 
A.pk_diasvida Edad,
B.nogalpon HouseNo,
c.csexo PenNo,
'-' Descripcion,
D.noplantel FarmNo,
E.czona Region
from {database_name_gl}.ft_consolidado_Diario A
left join {database_name_gl}.lk_galpon B on A.pk_galpon = B.pk_galpon
left join {database_name_gl}.lk_sexo C on A.pk_sexo = C.pk_sexo
left join {database_name_gl}.lk_lote D on A.pk_lote = D.pk_lote
left join {database_name_gl}.lk_zona E on A.pk_zona = E.pk_zona
where A.pk_empresa = 1 and A.pk_division = 4 and A.pk_estado = 2
and A.pk_diasvida >= 29
and A.descripfecha  = current_date-1
and A.peso <>0
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/pesoedaddetalle"
}
df_pesoedaddetalle.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.pesoedaddetalle")
print("carga pesoedaddetalle --> Registros procesados:", df_pesoedaddetalle.count())
df_PesoLote = spark.sql(f""" 
SELECT  
 Granja ComplexEntityNo
,Peso
,EventDate Fecha
,Edad
,HouseNo Galpon
,PenNo Corral
,SaldoAve + SaldoAveM as SaldoD
,SaldoAveT SaldoTD
,Descripcion
,FarmNo
,Region
,PesoTipo
,PesoM
,SaldoAveM SaldoMediano
,SaldoAve Saldo
FROM {database_name_costos_tmp}.pesoedaddetalle
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}PesoLote"
}
df_PesoLote.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_gl}.PesoLote")
print("carga PesoLote --> Registros procesados:", df_PesoLote.count())
df_pesoedaddetalle01 = spark.sql(f""" 
select 
substring(A.ComplexEntityNo,1,9) Granja, 
A.Peso,
0 PesoM,
0 PesoTipo,
A.SaldoAves SaldoAve,
0 SaldoAveT,
0 SaldoAveM,
descripfecha EventDate, 
A.pk_diasvida Edad,
B.nogalpon HouseNo,
c.csexo PenNo,
'-' Descripcion,
D.noplantel FarmNo,
E.csubzona Region
from {database_name_gl}.ft_consolidado_Diario A
left join {database_name_gl}.lk_galpon B on A.pk_galpon = B.pk_galpon
left join {database_name_gl}.lk_sexo C on A.pk_sexo = C.pk_sexo
left join {database_name_gl}.lk_lote D on A.pk_lote = D.pk_lote
left join {database_name_gl}.lk_subzona E on A.pk_subzona = E.pk_subzona
where A.pk_empresa = 1 and A.pk_division = 4 and A.pk_estado = 2
and A.pk_diasvida >= 29
and A.descripfecha  = current_date-1
and A.peso <>0
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/pesoedaddetalle01"
}
df_pesoedaddetalle01.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.pesoedaddetalle01")
print("carga pesoedaddetalle01 --> Registros procesados:", df_pesoedaddetalle01.count())
df_UPD_PesoLote = spark.sql(f""" 
SELECT
 PL.ComplexEntityNo
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.Peso <> PL1.Peso) AND PL1.Peso <> 0 THEN PL1.Peso ELSE PL.Peso END AS Peso
,PL.Fecha
,PL.Edad
,PL.Galpon
,PL.Corral
,PL.SaldoD
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.SaldoTD <> PL1.SaldoAveT) AND (PL1.SaldoAve > 0 OR PL1.SaldoAveM > 0 OR PL1.SaldoAveT > 0) THEN PL1.SaldoAveT ELSE PL.SaldoTD END AS SaldoTD
,PL.Descripcion
,PL.Region
,PL.FarmNo
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.PesoTipo <> PL1.PesoTipo) AND PL1.PesoTipo IS NOT NULL THEN PL1.PesoTipo ELSE PL.PesoTipo END AS PesoTipo
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.PesoM <> PL1.PesoM) AND PL1.PesoM <> 0 THEN PL1.PesoM ELSE PL.PesoM END AS PesoM
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.SaldoMediano <> PL1.SaldoAveM) AND (PL1.SaldoAve > 0 OR PL1.SaldoAveM > 0 OR PL1.SaldoAveT > 0) THEN PL1.SaldoAveM ELSE PL.SaldoMediano END AS SaldoMediano
,CASE WHEN PL1.Granja IS NOT NULL AND (PL.Saldo <> PL1.SaldoAve) AND (PL1.SaldoAve > 0 OR PL1.SaldoAveM > 0 OR PL1.SaldoAveT > 0) THEN PL1.SaldoAve ELSE PL.Saldo END AS Saldo
FROM
    {database_name_costos_gl}.PesoLote AS PL
LEFT JOIN
    {database_name_costos_tmp}.pesoedaddetalle01 AS PL1
ON
    PL.ComplexEntityNo = PL1.Granja AND PL.Galpon = PL1.HouseNo AND PL.Corral = PL1.PenNo
""")
print("carga UPD PesoLote --> Registros procesados:", df_UPD_PesoLote.count())
table_name = 'PesoLote'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD_PesoLote #filtered_new_data.union(data_after_delete)                             
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_INS_ft_plangerencial = spark.sql(f""" 
SELECT 
 'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,current_date Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,SaldoD Saldo
,COALESCE(Peso,0) pesoProm
,Edad
,Descripcion Granja
,Galpon
,Corral
,'' DiasCrianza
,'' estado
,Region
,'' PobInicial
,FarmNo
,'Excedente' obs
,'' pk_mes
,'' FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,SaldoTD SaldoT
,'' AvesRendidas
,SaldoMediano    
,Saldo SaldoU
,COALESCE(PesoM,0) PesoM
,PesoTipo PesoT
,'' SaldoTotal
,'' PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,1 pk_empresa
FROM {database_name_costos_gl}.PesoLote
where edad >= 29 and edad <= 81
""")
print("carga INS ft_plangerencial --> Registros procesados:", df_INS_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(col("Tipo")=="Engorde"))
    filtered_new_data = df_INS_ft_plangerencial.filter(col("Tipo")=="Engorde")
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_alojacorral = spark.sql(f""" 
select
 B.noplantel Granja
,B.nlote Lote
,C.nogalpon Galpon
,D.csexo Corral
,A.ComplexEntityNo
,A.PobInicial
from {database_name_gl}.ft_consolidado_Corral A
left join {database_name_gl}.lk_lote B on A.pk_lote = B.pk_lote
left join {database_name_gl}.lk_galpon C on A.pk_galpon = C.pk_galpon
left join {database_name_gl}.lk_sexo D on A.pk_sexo = D.pk_sexo
where A.pk_estado = 2 and A.pk_empresa = 1 and A.pk_division = 4
order by 5
""")
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojacorral"
}
df_alojacorral.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.alojacorral")
print("carga alojacorral --> Registros procesados:", df_alojacorral.count())
df_alojacorralconsolida = spark.sql(f""" 
select  
 Granja
,Lote
,Galpon
,Corral
,ComplexEntityNo
,sum(PobInicial) as Poblacion
from {database_name_costos_tmp}.alojacorral 
where PobInicial is not null
group by Granja
,Lote
,Galpon
,Corral
,ComplexEntityNo
""")        
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/alojacorralconsolida"
}
df_alojacorralconsolida.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.alojacorralconsolida")
print("carga alojacorralconsolida --> Registros procesados:", df_alojacorralconsolida.count())
df_UPD3_ft_plangerencial = spark.sql(f""" 
select 
 Clase
,Tipo
,Periodo
,Plaza
,Ton
,PPlaza
,Precio
,Costo
,Margen
,Fecha
,Moneda
,Provincia
,Gasto
,Gunit
,KilosE
,GastoA
,GastoC
,GastoL
,Unidad
,Articulo
,KproyT
,KproyD
,Saldo
,PesoProm
,Edad
,PG.Granja
,PG.Galpon
,PG.Corral
,DiasCrianza
,estado
,Region
,case when AC.Granja is not null then AC.Poblacion else PG.PobInicial end PobInicial
,FarmNo
,Obs
,pk_mes
,FechaCierre
,Ganancia
,Conversion
,AvesRendidasProy
,PesoPromProy
,ConversionH
,ceDescripcion
,KilosRendidos
,SaldoT
,AvesRendidas
,SaldoMediano
,SaldoU
,PesoM
,PesoT
,SaldoTotal
,PesoTotal
,TDoc
,MesCosto
,AnioCosto
,KproyS
,pk_empresa
from {database_name_costos_gl}.ft_plangerencial PG 
left join {database_name_costos_tmp}.alojacorralconsolida AC 
on PG.FarmNo=AC.Granja and PG.Galpon=AC.Galpon and PG.Corral=AC.Corral       
""") 
print("carga UPD3 ft_plangerencial --> Registros procesados:", df_UPD3_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD3_ft_plangerencial
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")

    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_INS2_ft_plangerencial = spark.sql(f""" 
WITH fechacierre_cte AS (
SELECT MAX(TO_DATE(fechacierre, 'yyyyMMdd')) AS MaxFechaCierre FROM {database_name_gl}.ft_consolidado_Lote CL_CTE WHERE CL_CTE.pk_estado = 3 AND CL_CTE.pk_empresa = 1 AND CL_CTE.pk_division = 4),
main_data AS (
SELECT CL.ComplexEntityNo,CL.DiasCrianza,CL.pk_estado,CL.PobInicial,CL.pk_mes,CL.fechacierre
FROM {database_name_gl}.ft_consolidado_Lote CL
WHERE CL.pk_estado = 3 AND CL.FechaCrianza <> '18991130' AND CL.FechaFinSaca <> '18991130' AND CL.pk_empresa = 1 AND CL.pk_division = 4 AND CL.FlagArtAtipico = 1)
SELECT
'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,'' Saldo
,'' PesoProm
,'' Edad
,'' Granja
,'' Galpon
,'' Corral
,DiasCrianza
,pk_estado Estado
,'' Region
,PobInicial
,'' FarmNo
,'DiasCrianza' obs
,pk_mes
,FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,'' SaldoT
,'' AvesRendidas
,'' SaldoMediano
,'' SaldoU
,'' PesoM
,'' PesoT
,'' SaldoTotal
,'' PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,1 pk_empresa
FROM main_data MD
CROSS JOIN fechacierre_cte f
WHERE YEAR(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = YEAR(f.MaxFechaCierre) AND MONTH(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = MONTH(f.MaxFechaCierre)
""") 
print('carga df_INS2_ft_plangerencial',df_INS2_ft_plangerencial.count())
table_name = 'ft_plangerencial'
    
additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
}
df_INS2_ft_plangerencial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name}")
print(f'se inserta la data a {table_name}', df_INS2_ft_plangerencial.count())
#Ganancia
df_INS3_ft_plangerencial = spark.sql(f""" 
WITH fechacierre_cte AS (
SELECT MAX(TO_DATE(fechacierre, 'yyyyMMdd')) AS MaxFechaCierre FROM {database_name_gl}.ft_consolidado_Lote CL_CTE WHERE CL_CTE.pk_estado = 3 AND CL_CTE.pk_empresa = 1 AND CL_CTE.pk_division = 4),
main_data AS (
SELECT CL.ComplexEntityNo,CL.pk_estado,CL.KilosRendidos,CL.pk_mes,CL.fechacierre,CL.GananciaDiaVenta,CL.AvesRendidas
FROM {database_name_gl}.ft_consolidado_Lote CL
WHERE CL.pk_estado = 3 AND CL.pk_empresa = 1 AND CL.pk_division = 4 AND CL.FlagArtAtipico = 1)
SELECT
'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,'' Saldo
,'' PesoProm
,'' Edad
,'' Granja
,'' Galpon
,'' Corral
,'' DiasCrianza
,pk_estado Estado
,'' Region
,KilosRendidos PobInicial
,'' FarmNo
,'Ganancia' obs
,pk_mes
,FechaCierre
,GananciaDiaVenta Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,'' SaldoT
,AvesRendidas
,'' SaldoMediano
,'' SaldoU
,'' PesoM
,'' PesoT
,'' SaldoTotal
,'' PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,1 pk_empresa
FROM main_data MD
CROSS JOIN fechacierre_cte f
WHERE YEAR(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = YEAR(f.MaxFechaCierre) AND MONTH(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = MONTH(f.MaxFechaCierre)
""") 
print('carga df_INS3_ft_plangerencial',df_INS3_ft_plangerencial.count())
table_name = 'ft_plangerencial'

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
}
df_INS3_ft_plangerencial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name}")
print(f"se inserta la data a {table_name}", df_INS3_ft_plangerencial.count())
df_UPD4_ft_plangerencial = spark.sql(f""" 
select 
 Clase
,Tipo
,Periodo
,Plaza
,Ton
,PPlaza
,Precio
,Costo
,Margen
,Fecha
,Moneda
,Provincia
,Gasto
,Gunit
,KilosE
,GastoA
,GastoC
,GastoL
,Unidad
,Articulo
,KproyT
,KproyD
,Saldo
,PesoProm
,Edad
,PG.Granja
,PG.Galpon
,PG.Corral
,case when Tipo='Engorde' and obs='Ganancia' then 0 else DiasCrianza end DiasCrianza
,estado
,Region
,case when Tipo='Engorde' and obs='Ganancia' then 0 else PobInicial end PobInicial
,FarmNo
,Obs
,pk_mes
,FechaCierre
,case when Tipo='Engorde' and obs='DiasCrianza' then 0 else Ganancia end Ganancia
,Conversion
,AvesRendidasProy
,PesoPromProy
,ConversionH
,ceDescripcion
,KilosRendidos
,SaldoT
,AvesRendidas
,SaldoMediano
,SaldoU
,PesoM
,PesoT
,SaldoTotal
,PesoTotal
,TDoc
,MesCosto
,AnioCosto
,KproyS
,pk_empresa
from {database_name_costos_gl}.ft_plangerencial PG 
""") 
print('carga df_UPD4_ft_plangerencial',df_UPD4_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD4_ft_plangerencial
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")

    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_INS4_ft_plangerencial = spark.sql(f""" 
WITH fechacierre_cte AS (
SELECT MAX(TO_DATE(fechacierre, 'yyyyMMdd')) AS MaxFechaCierre FROM {db_sap_fin_gl}.ft_consolidado_Lote CL_CTE WHERE CL_CTE.pk_estado = 3 AND CL_CTE.pk_empresa = 1 AND CL_CTE.pk_division = 4),
main_data AS (
SELECT CL.ComplexEntityNo,CL.PesoProm,B.noplantel Granja,CL.pk_estado,CL.pk_mes,CL.FechaCierre,CL.ICA,CL.KilosRendidos
FROM {database_name_gl}.ft_consolidado_Lote CL
left join {database_name_gl}.lk_lote B on CL.pk_lote = B.pk_lote
WHERE CL.pk_estado = 3 AND CL.pk_empresa = 1 AND CL.pk_division = 4 AND CL.FlagArtAtipico = 1)
SELECT
'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,'' Saldo
,PesoProm
,'' Edad
,Granja
,'' Galpon
,'' Corral
,'' DiasCrianza
,pk_estado Estado
,'' Region
,'' PobInicial
,'' FarmNo
,'Ica' obs
,pk_mes
,FechaCierre
,'' Ganancia
,ICA Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,KilosRendidos
,'' SaldoT
,'' AvesRendidas
,'' SaldoMediano
,'' SaldoU
,'' PesoM
,'' PesoT
,'' SaldoTotal
,'' PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,1 pk_empresa
FROM main_data MD
CROSS JOIN fechacierre_cte f
WHERE YEAR(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = YEAR(f.MaxFechaCierre) AND MONTH(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = MONTH(f.MaxFechaCierre)
""") 
print('carga df_INS4_ft_plangerencial',df_INS4_ft_plangerencial.count())
table_name = 'ft_plangerencial'

additional_options = {
    "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
}
df_INS4_ft_plangerencial.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("append") \
    .saveAsTable(f"{database_name_costos_gl}.{table_name}")
print(f'se inserta la data a {table_name}', df_INS4_ft_plangerencial.count())
df_UPD5_ft_plangerencial = spark.sql(f""" 
with ConversionH as (
SELECT PG.Provincia,SUBSTRING(PG.Provincia,1,4) as FarmNo,mIN(CL.ICA) as ConversionH
FROM {database_name_costos_gl}.ft_plangerencial PG
inner join {database_name_gl}.ft_consolidado_Lote CL on SUBSTRING(PG.Provincia,1,4) = SUBSTRING(CL.ComplexEntityNo,1,4)
where obs='Ica' GROUP BY PG.Provincia)
SELECT
 Clase
,Tipo
,Periodo
,Plaza
,Ton
,PPlaza
,Precio
,Costo
,Margen
,Fecha
,Moneda
,PG.Provincia
,Gasto
,Gunit
,KilosE
,GastoA
,GastoC
,GastoL
,Unidad
,Articulo
,KproyT
,KproyD
,Saldo
,PesoProm
,Edad
,Granja
,Galpon
,Corral
,DiasCrianza
,Estado
,Region
,PobInicial
,PG.FarmNo
,obs
,pk_mes
,FechaCierre
,Ganancia
,Conversion
,AvesRendidasProy
,PesoPromProy
,CASE WHEN obs='Ica' THEN C.ConversionH ELSE PG.ConversionH end ConversionH
,ceDescripcion
,KilosRendidos
,SaldoT
,AvesRendidas
,SaldoMediano
,SaldoU
,PesoM
,PesoT
,SaldoTotal
,PesoTotal
,TDoc
,MesCosto
,AnioCosto
,KproyS
,pk_empresa
from {database_name_costos_gl}.ft_plangerencial PG 
left join ConversionH C on c.Provincia  =  PG.Provincia 
""") 
print("carga UPD5 ft_plangerencial --> Registros procesados:", df_UPD5_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    final_data = df_UPD5_ft_plangerencial
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")

    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_disparidadpesos = spark.sql(f""" 
WITH fechacierre_cte AS (
SELECT MAX(TO_DATE(fechacierre, 'yyyyMMdd')) AS MaxFechaCierre FROM {database_name_gl}.ft_consolidado_Lote CL_CTE WHERE CL_CTE.pk_estado = 3 AND CL_CTE.pk_empresa = 1 AND CL_CTE.pk_division = 4),
main_data AS (
SELECT CL.ComplexEntityNo,CL.PesoProm,CL.STDPesoProm,CL.PobInicial,CL.FechaCierre,CL.FechaAlojamiento,DATEDIFF(CL.FechaAlojamiento,CL.FechaCierre) as Edad
FROM {database_name_gl}.ft_consolidado_Lote CL
WHERE CL.pk_estado = 3 AND CL.pk_empresa = 1 AND CL.pk_division = 4 AND CL.FlagArtAtipico = 1)
select 
CASE when LENGTH(MD.ComplexEntityNo)=11 then SUBSTRING(MD.ComplexEntityNo,1,6)
when LENGTH(MD.ComplexEntityNo)=10 then SUBSTRING(MD.ComplexEntityNo,1,6)
when LENGTH(MD.ComplexEntityNo)=9  then SUBSTRING(MD.ComplexEntityNo,1,4)
when LENGTH(MD.ComplexEntityNo)=8  then SUBSTRING(MD.ComplexEntityNo,1,4) end as Granja
,CASE when LENGTH(MD.ComplexEntityNo)=11 then SUBSTRING(MD.ComplexEntityNo,8,4)
when LENGTH(MD.ComplexEntityNo)=10 then SUBSTRING(MD.ComplexEntityNo,8,3)
when LENGTH(MD.ComplexEntityNo)=9  then SUBSTRING(MD.ComplexEntityNo,6,4)
when LENGTH(MD.ComplexEntityNo)=8  then SUBSTRING(MD.ComplexEntityNo,6,3) end as Lote 
,ComplexEntityNo
,PesoProm
,STDPesoProm
,PobInicial
,FechaCierre
,FechaAlojamiento
,Edad
FROM main_data MD
CROSS JOIN fechacierre_cte f
WHERE YEAR(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = YEAR(f.MaxFechaCierre) AND MONTH(TO_DATE(MD.fechacierre, 'yyyyMMdd')) = MONTH(f.MaxFechaCierre)
""")  
# Escribir los resultados en ruta temporal
additional_options = {
"path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temp/disparidadpesos"
}
df_disparidadpesos.write \
    .format("parquet") \
    .options(**additional_options) \
    .mode("overwrite") \
    .saveAsTable(f"{database_name_costos_tmp}.disparidadpesos")
print("carga disparidadpesos --> Registros procesados:", df_disparidadpesos.count())
df_UPD6_ft_plangerencial = spark.sql(f""" 
SELECT
 'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,a.ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,'' Saldo
,a.PesoProm PesoProm
,'' Edad
,FarmName Granja
,'' Galpon
,'' Corral
,'' DiasCrianza
,'' Estado
,'' Region
,a.PobInicial PobInicial
,'' FarmNo
,'DisparidadPesos' obs
,'' pk_mes
,'' FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,STDPesoProm PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,'' SaldoT
,'' AvesRendidas
,'' SaldoMediano
,'' SaldoU
,'' PesoM
,'' PesoT
,'' SaldoTotal
,'' PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,'' pk_empresa
from {database_name_costos_tmp}.disparidadpesos a 
inner join {database_name_si}.si_mvBrimEntities C on c.ComplexEntityNo  =  a.ComplexEntityNo 
where a.PesoProm<> 0
""") 
print("carga UPD6 ft_plangerencial --> Registros procesados:", df_UPD6_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(col("obs")=="DisparidadPesos"))
    filtered_new_data = df_UPD6_ft_plangerencial.filter(col("obs")=="DisparidadPesos")
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
df_UPD7_ft_plangerencial = spark.sql(f""" 
SELECT
 'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,0 Saldo
,0 PesoProm
,Edad
,Descripcion Granja
,Galpon
,Corral
,'' DiasCrianza
,'' Estado
,Region
,'' PobInicial
,FarmNo
,'Promedio' obs
,'' pk_mes
,'' FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,0 SaldoT
,'' AvesRendidas
,0 SaldoMediano
,0 SaldoU
,0 PesoM
,0 PesoT
,Saldo SaldoTotal
,Peso PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,'' pk_empresa
from {database_name_costos_gl}.PesoLote
where edad >= 29 and edad <= 81 and Saldo > 0  
union all
SELECT
 'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,0 Saldo
,0 PesoProm
,Edad
,Descripcion Granja
,Galpon
,Corral
,'' DiasCrianza
,'' Estado
,Region
,'' PobInicial
,FarmNo
,'Promedio' obs
,'' pk_mes
,'' FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,0 SaldoT
,'' AvesRendidas
,0 SaldoMediano
,0 SaldoU
,0 PesoM
,0 PesoT
,SaldoMediano SaldoTotal
,PesoM PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,'' pk_empresa
from {database_name_costos_gl}.PesoLote
where edad >= 29 and edad <= 81 and SaldoMediano > 0  
union all
SELECT
 'ProduccionEngorde' Clase
,'Engorde' Tipo
,'DEL' Periodo
,'Activo' Plaza
,'' Ton
,'' PPlaza
,'' Precio
,'' Costo
,'' Margen
,DATE_SUB(CURRENT_DATE(), 1) Fecha
,'' Moneda
,ComplexEntityNo Provincia
,'' Gasto
,'' Gunit
,'' KilosE
,'' GastoA
,'' GastoC
,'' GastoL
,'UN' Unidad
,'' Articulo
,'' KproyT
,'' KproyD
,0 Saldo
,0 PesoProm
,Edad
,Descripcion Granja
,Galpon
,Corral
,'' DiasCrianza
,'' Estado
,Region
,'' PobInicial
,FarmNo
,'Promedio' obs
,'' pk_mes
,'' FechaCierre
,'' Ganancia
,'' Conversion
,'' AvesRendidasProy
,'' PesoPromProy
,'' ConversionH
,'' ceDescripcion
,'' KilosRendidos
,0 SaldoT
,'' AvesRendidas
,0 SaldoMediano
,0 SaldoU
,0 PesoM
,0 PesoT
,SaldoTD SaldoTotal
,PesoTipo PesoTotal
,'' TDoc
,'' MesCosto
,'' AnioCosto
,'' KproyS
,'' pk_empresa
from {database_name_costos_gl}.PesoLote
where edad >= 29 and edad <= 81 and SaldoTD > 0 
""") 
print("carga UPD7 ft_plangerencial --> Registros procesados:", df_UPD7_ft_plangerencial.count())
table_name = 'ft_plangerencial'
file_name_target31 = f"{bucket_name_prdmtech}{table_name}/"
path_target31 = f"s3://{bucket_name_target}/{file_name_target31}"

try:
    df_existentes = spark.read.format("parquet").load(path_target31)
    datos_existentes = True
    logger.info(f"Datos existentes de {table_name} cargados: {df_existentes.count()} registros")
except:
    datos_existentes = False
    logger.info(f"No se encontraron datos existentes en {table_name}")

if datos_existentes:
    existing_data = spark.read.format("parquet").load(path_target31)
    data_after_delete = existing_data.filter(~(col("obs")=="Promedio"))
    filtered_new_data = df_UPD7_ft_plangerencial.filter(col("obs")=="Promedio")
    final_data = filtered_new_data.union(data_after_delete)                             
   
    cant_ingresonuevo = filtered_new_data.count()
    cant_total = final_data.count()
    
    # Escribir los resultados en ruta temporal
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal"
    }
    final_data.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_tmp}.{table_name}Temporal")

    final_data2 = spark.read.format("parquet").load(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/{table_name}Temporal")         
    additional_options = {
        "path": f"s3://{bucket_name_target}/{bucket_name_prdmtech}{table_name}"
    }
    final_data2.write \
        .format("parquet") \
        .options(**additional_options) \
        .mode("overwrite") \
        .saveAsTable(f"{database_name_costos_gl}.{table_name}")
            
    print(f"agrega registros nuevos a la tabla {table_name} : {cant_ingresonuevo}")
    print(f"Total de registros en la tabla {table_name} : {cant_total}")
     #Limpia la ubicación temporal
    glueContext.purge_s3_path(f"s3://{bucket_name_target}/{bucket_name_prdmtech}Temporal/", {"retentionPeriod": 0})
    glue_client.delete_table(DatabaseName=database_name_costos_tmp, Name=f'{table_name}Temporal')
    print(f"Tabla {table_name}Temporal eliminada correctamente de la base de datos '{database_name_costos_tmp}'.")
spark.stop() 
job.commit()  # Llama a commit al final del trabajo
print("termina notebook")
job.commit()