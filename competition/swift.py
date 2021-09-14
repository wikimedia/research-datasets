
#%%
# import os
# os.chdir("/home/fab/code/research-datasets")
# from images.swift import swift_spark, append_image_bytes
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql import Row, SparkSession, Window
# import pyspark.sql.functions as F
# import pyspark.sql.types as T

# %%

# %%

from images.swift import open_image
from urllib.parse import unquote
file_from_url = F.udf(lambda url: unquote(url.split('/')[-1]), 'string')
project_from_url = F.udf(lambda url: url.split('/')[4], 'string')

#%%
if False:
    competition_images = (swift_spark.read.csv(
        'images/competition_image_urls.tsv',
        sep='\t', 
        schema='i INT, image_url STRING')
    .withColumn('project', project_from_url('image_url'))
    .withColumn('image_file_name', file_from_url('image_url'))
    .cache()) 

    append_image_bytes(
        input_df=competition_images,
        output_dir='images/competition/all/pixels',
        swift_download_errors_dir='images/competition/all/swift_errors/'
    )

#%% testing images
if False:
    test_images = (swift_spark.read.csv(
            'images/competition_test_urls.tsv',
            sep='\t', 
            schema='article_project STRING, article STRING, image_url STRING')
        .withColumn('image_file_name', file_from_url('image_url'))
        .withColumn('project', project_from_url('image_url'))
        .cache()) 
    
    append_image_bytes(
        input_df=test_images,
        output_dir='images/competition/test/pixels',
        swift_download_errors_dir='images/competition/test/swift_errors/'
    )
#%% validation images
if False:
    validation_images = (swift_spark.read.csv(
            'images/competition_val_urls.tsv',
            sep='\t', 
            schema='article_project STRING, article STRING, image_url STRING')
        .withColumn('project', project_from_url('image_url'))
        .withColumn('image_file_name', file_from_url('image_url'))
        .cache()) 

    append_image_bytes(
        input_df=validation_images,
        output_dir='images/competition/validation/pixels',
        swift_download_errors_dir='images/competition/validation/swift_errors/'
    )
#%% The code below are snippets for interactive validation 

# to work with the images or the errors in notebook
i = spark.read.format('avro').load('images/competition/all/pixels/*').cache()
e = spark.read.csv('images/competition/all/swift_errors/*',sep='\t')

# %%
i.printSchema()
# root
#  |-- i: integer (nullable = true)
#  |-- image_url: string (nullable = true)
#  |-- project: string (nullable = true)
#  |-- image_file_name: string (nullable = true)
#  |-- thumbnail_size: string (nullable = true)
#  |-- image: struct (nullable = true)
#  |    |-- image_bytes_b64: string (nullable = true)
#  |    |-- format: string (nullable = true)
#  |    |-- width: integer (nullable = true)
#  |    |-- height: integer (nullable = true)
#  |    |-- image_bytes_sha1: string (nullable = true)
#  |    |-- error: string (nullable = true)

i.count()
# 6711755

i.agg(F.mean('image.height')).show()
# +-----------------+
# |avg(image.height)|
# +-----------------+
# |275.5353631265522|
# +-----------------+

e.count()
# 32200

e.count()/i.count()
# 0.004797552950010839

group_error = F.udf(lambda ex: ex[:3],'string')
e.groupBy(group_error('_c5')).count().orderBy('count',ascending=False).show()
# +-------------+-----+
# |<lambda>(_c5)|count|
# +-------------+-----+
# |          404|32135|
# |          429|   64|
# |          HTT|    1|
# +-------------+-----+

# %%


b = i.select("image.image_bytes_b64").limit(10).collect()
# %%
open_image(decode_b64(b[0]['image_bytes_b64']))
# %%
i.agg(F.mean('image.width')).show()
# %%
a = swift_spark.read.format('avro').load('images/competition/all/pixels/*').cache()
a.agg(F.mean('image.width')).show()
# %%
