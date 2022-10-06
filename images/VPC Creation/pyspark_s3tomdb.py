import sys
import json
import logging
import boto3
import pyspark

###################GLUE import##############
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import *

#### ###creating spark and gluecontext ###############

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Read from the Catalog DB ###

ds = glueContext.create_dynamic_frame.from_catalog(database="partner-demo-mdb", table_name="airports_json",transformation_ctx= "datasource0")

#### MongoDB Atlas Connection ### ##UPDATE the MongoDB Atlas connection##

mongo_uri = "mongodb://xxxxxx.xxxascf.mongodb.net:27017,xxxxxxhard-00-01.xxxx.mongodb.net:27017,axxxxxpub-s-02.frzascf.mongodb.net:27017/?ssl=true&replicaSet=atlas-br364o-shard-0&authSource=admin&retryWrites=true&w=majority"

logger = glueContext.get_logger()
logger.info("Connecting...")


write_mongo_options = {
    "uri": mongo_uri,
    "database": "xxxxx",  ##UPDATE MongoDB Atlas Database name ##
    "collection": "xxxx",  ##UPDATE Collection name ##
    "username": "xxx",  ##UPDATE username ##
    "password": "xxxxxx" ##UPDATE the password ##
}

# Write DynamicFrame to MongoDB and DocumentDB
glueContext.write_dynamic_frame.from_options( ds, connection_type="mongodb", connection_options= write_mongo_options)


print("written to MDB!")

job.commit()
