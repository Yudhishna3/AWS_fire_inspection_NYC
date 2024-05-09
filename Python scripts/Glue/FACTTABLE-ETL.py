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

# Script generated for node property_Dim
property_Dim_node1715083882598 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_property", transformation_ctx="property_Dim_node1715083882598")

# Script generated for node Amazon S3
AmazonS3_node1715083879473 = glueContext.create_dynamic_frame.from_catalog(database="s3-data-catalogue", table_name="final_merge_csv", transformation_ctx="AmazonS3_node1715083879473")

# Script generated for node cof_dim
cof_dim_node1715083880828 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_certificatesoffitness", transformation_ctx="cof_dim_node1715083880828")

# Script generated for node inspection_dim
inspection_dim_node1715083881383 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_firesafetyinspection", transformation_ctx="inspection_dim_node1715083881383")

# Script generated for node location_dim
location_dim_node1715083881959 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_location", transformation_ctx="location_dim_node1715083881959")

# Script generated for node owner_dim
owner_dim_node1715083883196 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_property_owner", transformation_ctx="owner_dim_node1715083883196")

# Script generated for node building_classification_dim
building_classification_dim_node1715083880235 = glueContext.create_dynamic_frame.from_catalog(database="rds-crawled-test2", table_name="test2_dim_buildingclassification", transformation_ctx="building_classification_dim_node1715083880235")

# Script generated for node SQL Query
SqlQuery434 = '''
SELECT
    p.PropertyID,
    o.OwnerID,
    i.InspectionID,
    co.COF_ID,
    bc.BuildingClassID,
    lc.LocationID
FROM
    merged mg
LEFT JOIN
    dim_property p ON mg.ADDRESS = p.ADDRESS
    AND mg.`ZIP CODE` = p.ZIPCODE
    AND mg.`LAND SQUARE FEET` = p.`LANDSQUAREFEET`
    AND mg.`GROSS SQUARE FEET` = p.`GROSSSQUAREFEET`
    AND mg.`YEAR BUILT` = p.`YEARBUILT`
    AND mg.BIN = p.BIN
    AND mg.BBL = p.BBL
LEFT JOIN
    Dim_property_Owner o ON mg.owner_name = o.ownername
    AND mg.LAST_VISIT_DT = o.LastVisitDate
LEFT JOIN
    Dim_FireSafetyInspection i ON mg.LAST_VISIT_DT = i.InspectionDate
    AND mg.LAST_INSP_STAT = i.InspectionStatus
    AND mg.LAST_FULL_INSP_DT = i.LastFullInspectionDate
    AND mg.ALPHA = i.FireSafetyGrade
    AND mg.COF_ID = i.COF_ID
    AND mg.COF_NUM = i.COF_NUM
    AND mg.COF_TYPE = i.COF_TYPE
LEFT JOIN
    Dim_CertificatesOfFitness co ON mg.COF_ID = co.COF_ID
    AND mg.COF_NUM = co.COF_NUM
    AND mg.COF_TYPE = co.COF_TYPE
LEFT JOIN
    Dim_BuildingClassification bc ON mg.`BUILDING CLASS CATEGORY` = bc.BuildingClassCategory
    AND mg.`BUILDING CLASS AS OF FINAL ROLL` = bc.BuildingClassFinalRoll
    AND mg.`TAX CLASS AT TIME OF SALE` = bc.TaxClassAtTimeOfSale
    AND mg.`BUILDING CLASS AT TIME OF SALE` = bc.BuildingClassAtTimeOfSale
LEFT JOIN
    Dim_Location lc ON mg.BOROUGH = lc.Borough
    AND mg.NEIGHBORHOOD = lc.Neighborhood
    AND mg.NTA_X = lc.NTA;
'''
SQLQuery_node1715084026387 = sparkSqlQuery(glueContext, query = SqlQuery434, mapping = {"merged":AmazonS3_node1715083879473, "Dim_BuildingClassification":building_classification_dim_node1715083880235, "Dim_CertificatesOfFitness":cof_dim_node1715083880828, "Dim_Location":location_dim_node1715083881959, "dim_property":property_Dim_node1715083882598, "Dim_property_Owner":owner_dim_node1715083883196, "Dim_FireSafetyInspection":inspection_dim_node1715083881383}, transformation_ctx = "SQLQuery_node1715084026387")

# Script generated for node MySQL
MySQL_node1715084499577 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1715084026387, database="rds-crawled-test2", table_name="test2_fact_realestate", transformation_ctx="MySQL_node1715084499577")

job.commit()