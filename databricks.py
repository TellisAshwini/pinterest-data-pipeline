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

from pyspark.sql.functions import *
from pyspark.sql.types import *

#df_pin
"""
Replace empty entries and entries with no relevant data in each column with Nones

"""
df_pin = df_pin.dropDuplicates()
#print((df_pin.count(), len(df_pin.columns)))
df_pin = df_pin.withColumnRenamed('index', 'ind')
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", 
"tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")
#df_pin.filter(df_pin['follower_count'].rlike('[A-Za-z]')).show()
df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', '[%k]', '000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', '[%M]', '000000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace('follower_count', '[%User Info Error%]', ''))
df_pin = df_pin.withColumn('follower_count', df_pin['follower_count'].cast(IntegerType()))
df_pin = df_pin.withColumn('save_location', regexp_replace('save_location', 'Local save in *', ''))


#df_geo
df_geo = df_geo.dropDuplicates()
df_geo = df_geo.withColumn("coordinates", array(col("latitude"), col("longitude")))
df_geo = df_geo.drop('latitude', 'longitude')
df_geo = df_geo.withColumn("timestamp", df_geo["timestamp"].cast(TimestampType()))
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")


#df_user
df_user = df_user.dropDuplicates()
df_user = df_user.withColumn("user_name", concat(col("first_name"), lit(" "), col("last_name")))
df_user = df_user.drop("first_name", "last_name")
df_user = df_user.withColumn('date_joined', df_user['date_joined'].cast(TimestampType()))
df_user = df_user.select("ind", "user_name", "age", "date_joined")

# creating temporary tables
df_pin.createOrReplaceTempView('df_pin')
df_geo.createOrReplaceTempView('df_geo')
df_user.createOrReplaceTempView('df_user')

#TASK 4: Most popular category in each country
spark.sql("select country, category, category_count from (select *, count(category) over(partition by country) as category_count \
          from (select *, \
          dense_rank() over(partition by country order by rn desc) dr \
          from \
          (select country, category, max(rn) as rn from (select g.country, p.category, \
          count(p.category) over (partition by g.country order by g.country) as category_count, \
          row_number() over(partition by g.country, p.category order by g.country) rn\
          from df_geo g \
          inner join df_pin p \
          on g.ind = p.ind) X group by country, category)X) Y) Z where dr = 1" ).show()

#TASK 5: Most popular category in each year between  2018 and 2022
#category_count is the number of time the highest category is repeated
spark.sql("select post_year, category, m_rn as category_count from \
          (SELECT *, dense_rank() over (partition by post_year order by m_rn desc) DN from \
          (select post_year, category, max(rn) as m_rn from \
          (select *, row_number() over(partition by post_year, category order by post_year, category) rn from \
          (select EXTRACT(YEAR FROM g.timestamp) as post_year, p.category \
           from df_geo g inner join df_pin p on g.ind = p.ind \
          where EXTRACT(YEAR FROM g.timestamp) between 2018 and 2022 order by 1)X)Y group by post_year, category)Z)W \
          where DN = 1").show()

#TASK 6
#STEP 1
spark.sql("SELECT country, poster_name, follower_count from \
(select g.*,p.*, row_number() over (partition by country order by p.follower_count desc) rn \
from df_geo g inner join df_pin p on g.ind = p.ind) X \
where rn = 1 order by follower_count desc").show()

spark.sql("SELECT country, follower_count from \
(select g.*,p.*, row_number() over (partition by country order by p.follower_count desc) rn \
from df_geo g inner join df_pin p on g.ind = p.ind) X \
where rn = 1").show()

#TASK_7
spark.sql("select age_group, category, rn as category_count from \
          (select *, dense_rank() over(partition by age_group order by rn desc) dn from \
          (select *, row_number() over(partition by age_group, category order by category) rn from \
          (select u.age, p.category, case \
          when u.age between 18 and 24 then '18-24' \
          when u.age between 25 and 35 then '25-35' \
          when u.age between 36 and 50 then '36-50' \
          when u.age > 50 then '+50' \
          end as age_group \
          from df_user u \
          inner join df_pin p \
          on u.ind = p.ind)X) Y)Z where dn = 1").show()

#TASK_8
spark.sql("SELECT age_group, follower_count, rn, row_count from \
            (select *, row_number() over (partition by age_group order by follower_count) rn, \
            count(*) over(partition by age_group) row_count from \
                (select u.*, p.*, case \
                when u.age between 18 and 24 then '18-24' \
                when u.age between 25 and 35 then '25-35' \
                when u.age between 36 and 50 then '36-50' \
                when u.age > 50 then '+50' \
                end as age_group \
                from df_user u \
                inner join df_pin p \
                on u.ind = p.ind \
                ) X \
            )Y \
        where rn in ( FLOOR((row_count + 1) / 2), FLOOR( (row_count + 2) / 2) )").show()

#TASK_9
spark.sql("select post_year, count(*) as number_users_joined from \
            (select *, EXTRACT(YEAR FROM date_joined) as post_year from df_user \
            )X \
          group by post_year").show()

#TASK_10
spark.sql("select post_year, rn median_follower_count from (select post_year, row_number() over(partition by post_year order by post_year) rn, \
          count(*) over(Partition by post_year) u_count from \
            (select *, EXTRACT(YEAR FROM date_joined) as post_year from df_user)X) Y \
            where rn in ( FLOOR((u_count + 1) / 2), FLOOR( (u_count + 2) / 2) )").show()

#TASK_11
spark.sql("select age_group, post_year, follower_count as median_follower_count from (select *, \
          row_number() over(partition by post_year, age_group order by post_year) rn, \
          count(*) over(partition by post_year, age_group) row_count from \
          (select p.follower_count, u.age, \
          case \
          when u.age between 18 and 24 then '18-24' \
          when u.age between 25 and 35 then '25-35' \
          when u.age between 36 and 50 then '36-50' \
          when u.age > 50 then '+50' \
          end as age_group, \
          EXTRACT(YEAR FROM g.timestamp) as post_year \
           from df_user u \
          inner join df_pin p on u.ind = p.ind \
          inner join df_geo g on p.ind = g.ind) X \
          where post_year between 2015 and 2020 \
          order by post_year)Y \
          where rn in ( FLOOR((row_count + 1) / 2), FLOOR( (row_count + 2) / 2) ) ").show()