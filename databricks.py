# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

dbutils.fs.ls("/FileStore/tables")

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe

aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")


# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

AWS_S3_BUCKET = "user-12f7a43505b1-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest_s3_mount"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/pinterest_s3_mount/topics/12f7a43505b1.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)


file_location = "/mnt/pinterest_s3_mount/topics/12f7a43505b1.geo/partition=0/*.json" 
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)


file_location = "/mnt/pinterest_s3_mount/topics/12f7a43505b1.user/partition=0/*.json" 
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
