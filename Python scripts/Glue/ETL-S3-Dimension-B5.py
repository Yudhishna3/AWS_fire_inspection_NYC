import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1715070267933 = glueContext.create_dynamic_frame.from_catalog(database="s3-data-catalogue", table_name="final_merge_csv", transformation_ctx="AmazonS3_node1715070267933")

# Script generated for node location_dim
SqlQuery400 = '''
SELECT distinct
    NEIGHBORHOOD Neighborhood,
    BOROUGH as Borough,
    LEFT(NTA_x, 10) AS NTA
FROM
    myDataSource;
'''
location_dim_node1715071691920 = sparkSqlQuery(glueContext, query = SqlQuery400, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "location_dim_node1715071691920")

# Script generated for node Dim_CertificatesOfFitness
SqlQuery401 = '''
SELECT DISTINCT COF_ID AS cof_id,
    COF_NUM,
    LEFT(COF_TYPE, 20) AS COF_TYPE
FROM myDataSource;
'''
Dim_CertificatesOfFitness_node1715072402228 = sparkSqlQuery(glueContext, query = SqlQuery401, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "Dim_CertificatesOfFitness_node1715072402228")

# Script generated for node Building_classification_dim
SqlQuery402 = '''
SELECT distinct
    `BUILDING CLASS CATEGORY` as BuildingClassCategory,
     CAST(`TAX CLASS AS OF FINAL ROLL` AS STRING) as TaxClassFinalRoll,
    `BUILDING CLASS AS OF FINAL ROLL` as BuildingClassFinalRoll,
    `TAX CLASS AT TIME OF SALE` as TaxClassAtTimeOfSale,
    `BUILDING CLASS AT TIME OF SALE` as BuildingClassAtTimeOfSale
FROM
    myDataSource;
'''
Building_classification_dim_node1715070782974 = sparkSqlQuery(glueContext, query = SqlQuery402, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "Building_classification_dim_node1715070782974")

# Script generated for node Dim_FireSafetyInspection
SqlQuery403 = '''
SELECT distinct
    LAST_VISIT_DT as InspectionDate,
    LEFT(LAST_INSP_STAT, 20) as InspectionStatus,
    LAST_FULL_INSP_DT as LastFullInspectionDate,
    ALPHA as FireSafetyGrade,
    COF_NUM as COF_ID,
    LEFT(COF_TYPE, 20) AS COF_TYPE
FROM
    myDataSource;
'''
Dim_FireSafetyInspection_node1715072075722 = sparkSqlQuery(glueContext, query = SqlQuery403, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "Dim_FireSafetyInspection_node1715072075722")

# Script generated for node Property_dim
SqlQuery404 = '''
SELECT distinct
   ADDRESS as Address,
   `ZIP CODE` as ZipCode,
   `LAND SQUARE FEET` as LANDSQUAREFEET,
   `GROSS SQUARE FEET` as GROSSSQUAREFEET,
   `YEAR BUILT` as YearBuilt,
   BIN ,
   CAST(BBL AS INT) AS BBL,
   `SALE PRICE` as SalePrice
FROM
    myDataSource;
'''
Property_dim_node1715070280823 = sparkSqlQuery(glueContext, query = SqlQuery404, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "Property_dim_node1715070280823")

# Script generated for node Owner_dim
SqlQuery405 = '''
SELECT distinct
    OWNER_NAME as OwnerName,
    LAST_VISIT_DT as LastVisitDate
FROM
    myDataSource;
'''
Owner_dim_node1715071466450 = sparkSqlQuery(glueContext, query = SqlQuery405, mapping = {"myDataSource":AmazonS3_node1715070267933}, transformation_ctx = "Owner_dim_node1715071466450")

# Script generated for node MySQL
MySQL_node1715073041965 = glueContext.write_dynamic_frame.from_catalog(frame=location_dim_node1715071691920, database="rds-crawled-test2", table_name="test2_dim_location", transformation_ctx="MySQL_node1715073041965")

# Script generated for node MySQL
MySQL_node1715072535112 = glueContext.write_dynamic_frame.from_catalog(frame=Dim_CertificatesOfFitness_node1715072402228, database="rds-crawled-test2", table_name="test2_dim_certificatesoffitness", transformation_ctx="MySQL_node1715072535112")

# Script generated for node MySQL
MySQL_node1715072958899 = glueContext.write_dynamic_frame.from_catalog(frame=Building_classification_dim_node1715070782974, database="rds-crawled-test2", table_name="test2_dim_buildingclassification", transformation_ctx="MySQL_node1715072958899")

# Script generated for node MySQL
MySQL_node1715073019180 = glueContext.write_dynamic_frame.from_catalog(frame=Dim_FireSafetyInspection_node1715072075722, database="rds-crawled-test2", table_name="test2_dim_firesafetyinspection", transformation_ctx="MySQL_node1715073019180")

# Script generated for node MySQL
MySQL_node1715072535725 = glueContext.write_dynamic_frame.from_catalog(frame=Property_dim_node1715070280823, database="rds-crawled-test2", table_name="test2_dim_property", transformation_ctx="MySQL_node1715072535725")

# Script generated for node MySQL
MySQL_node1715072983813 = glueContext.write_dynamic_frame.from_catalog(frame=Owner_dim_node1715071466450, database="rds-crawled-test2", table_name="test2_dim_property_owner", transformation_ctx="MySQL_node1715072983813")

job.commit()