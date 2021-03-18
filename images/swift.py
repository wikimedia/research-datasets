#%%

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt

import base64
import hashlib
import imageio
from io import BytesIO
import mwparserfromhell
import numpy as np
import os
from PIL import Image
import re
import requests
from requests.adapters import HTTPAdapter
import wmfdata

# %%

# measurments from a test:
mb_per_image = 61.8e3/1e6
images_per_second_per_thread = 1e6/(4.6*60*60)/(20*4)
# images on commons to download
num_commons = 53e6

#config
num_images=num_commons
# num_images=1e6
file_size=250

num_cores=80
query_per_seconds=100

total_mb=num_images*mb_per_image
print(f'for {num_images} images, expected GB {num_images*mb_per_image/1e3} of image data')
print(f'use {num_images*mb_per_image/file_size} partitions for files of size {file_size}mb')
print(f'expected total duration with {num_cores} cores: {num_images/images_per_second_per_thread/3600/24/num_cores} days')
# print(f'for a {query_per_seconds} qps to swift, use {query_per_seconds/images_per_second_per_thread} cores')
# print(f'expected total duration: {num_images/query_per_seconds/3600}h ')

# seconds_per_image = (4.6*60*60)/1e6
# print(f'expected total duration for {num_images} images: {num_images*seconds_per_image/3600/24} days')


#%%

# measurments from a test:
mb_per_image = 20*300/1e5
images_per_second_executor = 1e5/(1.4*60*60)/10
# images on commons to download
num_commons = 53e6

#config
# num_images=num_commons
num_images=1e6
file_size=250

num_cores=20
query_per_seconds=100

total_mb=num_images*mb_per_image
print(f'for {num_images} images, expected GB {num_images*mb_per_image/1e3} of image data')
print(f'use {num_images*mb_per_image/file_size} partitions for files of size {file_size}mb')
print(f'expected total duration with {num_cores} cores: {num_images/images_per_second_executor/3600/24/num_cores} days')
# print(f'for a {query_per_seconds} qps to swift, use {query_per_seconds/images_per_second_executor} executors')
# print(f'expected total duration: {num_images/query_per_seconds/3600}h ')

seconds_per_image = 34*60/1e5
print(f'expected total duration for {num_images} images: {num_images*seconds_per_image/3600/24} days')

#%%
# the number of workers allocated to this spark application querying swift. 
swift_load = 20

spark_config = {
    'spark.driver.extraJavaOptions' : ' '.join('-D{}={}'.format(k, v) for k, v in {
        'http.proxyHost': 'webproxy.eqiad.wmnet',
        'http.proxyPort': '8080',
        'https.proxyHost': 'webproxy.eqiad.wmnet',
        'https.proxyPort': '8080',
    }.items()),
    'spark.sql.shuffle.partitions': 512,
    'spark.dynamicAllocation.maxExecutors': swift_load,
    'spark.executor.memory': '8g',
    'spark.executor.cores': 4,
    'spark.driver.memory': '4g',
}   

swift_host = "https://ms-fe.svc.eqiad.wmnet"
max_retries = 5
timeout =  15
wiki_db = "commons"
thumb_size = "400px"

#%%


swift_spark = wmfdata.spark.get_custom_session(
    master="yarn",
    app_name="swifting",
    spark_config=spark_config)

from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from operator import add


#%%
get_image_bytes('common','A_new_map_of_Upper_and_Lower_Canada_from_the_latest_authorities.jpg','400px')


#%%

@F.udf(returnType=T.BooleanType())
def filter_images(page_title):
    """Return true if we are generally interested in this row"""
    prefixes = ['File:']
    good_start = any(page_title.startswith(prefix) for prefix in prefixes)
    suffixes = ['.png', '.jpg', '.jpeg', '.gif']
    good_end = any(page_title.endswith(suffix) for suffix in suffixes)
    return good_start and good_end


image_file_name = F.udf(lambda title: title[5:].replace(" ", "_"), 'string')


def get_image_bytes(wiki_db, file_name, thumb_size):
    md5 = hashlib.md5(file_name.encode('utf-8')).hexdigest()
    shard = f"{md5[0]}/{md5[:2]}"
    uri = f"{swift_host}/wikipedia/{wiki_db}/thumb/{shard}/{file_name}/{thumb_size}-{file_name}"
    return requests.get(uri, verify=False, timeout=timeout).content


@F.udf(returnType=T.ArrayType(T.StringType()))
def download_image(file_name):
    #built in retries causes some dependency issue on the worker, doing retries manually
    error=None
    for retry_count in range(max_retries):
        try:
            image_bytes = get_image_bytes(wiki_db, file_name, thumb_size)
            return (base64.b64encode(image_bytes).decode('utf-8'), None)
        except Exception as e:
            print(f"try {retry_count}, swift download error for file {file_name}. Exception: {e}")
            error=str(e)
    return (None, error)

# %%
def download_images(input_df, partitions):
    """
    `input_df` is expected to have a `image_file_name` column
    returns two dataframes
        `input` dataframe with a `image_bytes_b64` column containing the image bytes
        `input` dataframe with a `download_error` column containing the errors for each file that wasn't downloaded
    """
    attempted_images_bytes = (input_df
        .repartition(partitions)
        .withColumn("attempted_thumbnail_b64", download_image('image_file_name'))
        .cache())

    download_errors = (attempted_images_bytes
        .withColumn('download_error', F.col('attempted_thumbnail_b64').getItem(1))
        .where(F.col('download_error').isNotNull())
        .drop('attempted_thumbnail_b64'))

    with_images_bytes = (attempted_images_bytes
        .withColumn('image_bytes_b64', F.col('attempted_thumbnail_b64').getItem(0))
        .where(F.col('image_bytes_b64').isNotNull())
        .drop('attempted_thumbnail_b64'))

    return (with_images_bytes, download_errors)

# %%

commons_cols = [
    "page_id",
    "page_namespace",
    "page_title",
    "user_id",
    "user_text",
    "revision_id",
    "revision_text_sha1",
    "revision_timestamp",
    "snapshot",
    "wiki_db"]


# def commons_images_to_hdfs(
#     wiki_db = "commons",
#     snapshot = '2021-01',                   
#     commons_hdfs_output_dir):
wiki_db = "commons"
snapshot = '2021-01'                   
commons_hdfs_output_dir = 'commons_images/'
commons_download_errors_dir = 'commons_images_errors/'

# %%
wiki = (swift_spark
    .sql(f"""
    select * from wmf.mediawiki_wikitext_current where snapshot='{snapshot}' and wiki_db='{wiki_db}wiki'
    """))


wiki = wiki.filter(filter_images("page_title")).select(commons_cols)

# %%


# commons_images_schema = (T.StructType()
#     .add("page_id", T.StringType(), True)
#     .add("page_namespace", T.StringType(), True)
#     .add("page_title", T.StringType(), True)
#     .add("user_id", T.StringType(), True)
#     .add("user_text", T.StringType(), True)
#     .add("revision_id", T.StringType(), True)
#     .add("revision_text_sha1", T.StringType(), True)
#     .add("revision_timestamp", T.StringType(), True)
#     .add("snapshot", T.StringType(), True)
#     .add("wiki_db", T.StringType(), True))

# image_bytes_schema = (T.StructType()
#     .add("image_file_name", T.StringType(), True)
#     .add("image_bytes_b64_sha1", T.StringType(), True)
#     .add("image_bytes_b64", T.StringType(), True))


# %%

test = (wiki
    .withColumn('image_file_name', image_file_name('page_title'))
    # .limit(10))
    # .limit(100000))
    .limit(1000000))
# T.StructType(commons_images_schema.fields+image_bytes_schema.fields)

# %%
# test.show()
images,errors = download_images(test,240)

#%%
images.count()
# errors.count()

#%%
(images
    .withColumn('image_bytes_b64_sha1', F.sha1(F.col('image_bytes_b64')))
    .write.mode("overwrite").save(commons_hdfs_output_dir)
)


(errors
    .write.mode("overwrite").save(commons_download_errors_dir)
)

# %%
# images.count()

images = spark.read.parquet(commons_hdfs_output_dir)
#%%
errors = spark.read.parquet(commons_download_errors_dir)

#%%
e = errors.select('image_file_name', 'download_error').limit(200).collect()
#%%

f = 'Lingo_Street,_Northside,_Cincinnati,_OH_(39814660943).jpg' 

images.filter(F.col('image_file_name')==f).show()


#%%

file_type = F.udf(lambda fn: fn.split('.')[-1], 'string')

t = images.withColumn('file_type', file_type('image_file_name')).groupBy('file_type').count().orderBy('count', ascending=False).cache()
t.show()
#%%

# x = images.limit(10).select('image_bytes_b64').collect()
bb = base64.b64decode(x[0].image_bytes_b64)
image = Image.open(BytesIO(bb))
# Image.open(bb)
# %%

h=image.quantize(250)
h
# .histogram()
# len(h)
# %%
len(image.histogram())
# %%
import plotly.express as px
fig = px.line(image.quantize(50).histogram())
fig.show()


#%%

small = images.limit(100).cache()
# %%
c=5
@F.udf(returnType='string')
def hist_buckets(image_bytes_b64):
    try:
        bb = base64.b64decode(image_bytes_b64)
        image = Image.open(BytesIO(bb))
    except:
        return 'load image error'
    try:
        hist = [int(v/500) for v in image.quantize(c).histogram()[:c]]
        return ','.join([str(h) for h in hist])
    except:
        return 'image histogram error'
    


small1 = small.withColumn('hist', hist_buckets('image_bytes_b64')).cache()

# image.size
# %%
(images
    .withColumn('hist', hist_buckets('image_bytes_b64'))
    .groupBy('hist')
    .count()
    .orderBy('count',ascending=False)
    .write.mode("overwrite").csv('image_hists/', compression='none', sep='\t')
)

# %%
top = spark.read.csv('image_hists/*',sep='\t').orderBy('_c1',ascending=False).limit(1000).collect()
# %%
t = [r._c0 for r in top[:100]]
t
# %%
p = (images
    .withColumn('hist', hist_buckets('image_bytes_b64'))
    .filter(F.col('hist').isin(t))
    .cache())
p.show()
# %%
from IPython.core.display import display, HTML

@F.udf(returnType=T.StringType())
def html_image(img_b64):
    return '<img src="data:image/png;base64,{0:s}">'.format(img_b64)


html_all = (p
    .filter(F.col('hist')=='27,39,53,64,55')
    .withColumn("thumbnail", html_image(F.col("image_bytes_b64")))
    .select('page_title','thumbnail')
    .toPandas()
    .to_html(escape=False))

display(HTML(html_all))

# t[:10]
# %%
