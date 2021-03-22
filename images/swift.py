#%%

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt

import base64
import hashlib
import imageio
from io import BytesIO
import math
import mwparserfromhell
import numpy as np
import os
from PIL import Image
import re
import requests
from requests.adapters import HTTPAdapter
import wmfdata

#%%
# the number of workers allocated to this spark application querying swift. 
swift_load = 20

spark_config = {
    'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4',
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

# measurments from a test:
mb_per_image = 61.8e3/1e6
images_per_second_per_thread = 1e6/(4.6*60*60)/(20*4)
# images on commons to download
num_images_commons = 53e6

def estimate_partitions(num_images, file_size_mb=200):
    num_cores=4*swift_load
    num_partitions=int(num_images*mb_per_image/file_size_mb)
    total_mb=num_images*mb_per_image
    print('estimate required number of partitions')
    print(f'for {num_images} images, expected GB {num_images*mb_per_image/1e3} of image data')
    print(f'use {num_partitions} partitions for files of size {file_size_mb}mb')
    print(f'expected total duration with {num_cores} cores: {num_images/images_per_second_per_thread/3600/24/num_cores} days')
    return num_partitions


swift_host = "https://ms-fe.svc.eqiad.wmnet"
max_retries = 5
timeout =  15

#%%

swift_spark = wmfdata.spark.get_session(
    app_name='repartition wiki images', 
    extra_settings={'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
    ship_python_env=False)

from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from operator import add
from pyspark import StorageLevel
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
def download_image(file_name, wiki_db, thumb_size):
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
def download_images(input_df, thumb_size, partitions):
    """
    `input_df` is expected to have a `image_file_name` column
    returns two dataframes
        `input` dataframe with a `image_bytes_b64` column containing the image bytes
        `input` dataframe with a `download_error` column containing the errors for each file that wasn't downloaded
    """
    input_df = input_df.withColumn('thumbnail_size', F.lit(thumb_size))
    input_df = input_df.repartition(partitions) if partitions is not None else input_df
    attempted_images_bytes = (input_df
        .withColumn("attempted_thumbnail_b64", download_image('image_file_name', 'wiki_db', 'thumbnail_size'))
        .persist(StorageLevel.DISK_ONLY))

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

#commons_hdfs_output_dir = 'commons_images/'
#commons_download_errors_dir = 'commons_images_errors/'

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

def commons_images_to_hdfs(
    images_hdfs_output,
    snapshot,
    wiki_db = "commons",
    thumb_size='400px',
    temp_wiki_dir='temp_wiki/',
    desired_file_size_mb=200,
    mb_per_swift_job=50000,
    swift_download_errors=None):
    """
    downloads images from swift and extends the input
    schema with the image bytes as base64 encoded strings
    in avro encoded files
    """
    global swift_spark
    print(f'calculating the number of partitions required for a target file size {desired_file_size_mb}')
    wiki = swift_spark.sql(f"select * from wmf.mediawiki_wikitext_current where snapshot='{snapshot}'")
    wiki = wiki.where(F.col("wiki_db")==f'{wiki_db}wiki') if wiki_db is not None else wiki
    wiki = wiki.filter(filter_images("page_title")).select(commons_cols)

    wiki  = (wiki
        .withColumn('image_file_name', image_file_name('page_title'))
        #.limit(10))
        .limit(100000))
        #.limit0000))

    # num_images = wiki.count()
    num_images = 100000
    num_partitions = estimate_partitions(num_images, desired_file_size_mb)

    print(f"repartitioning the input wiki into {num_partitions} files for batched downloads")
    (wiki
        .repartition(num_partitions)
        # .write.format("avro").mode("overwrite").save(temp_wiki_dir))
        .write.format("parquet").mode("overwrite").save(temp_wiki_dir))

    swift_spark.stop()
    swift_spark = wmfdata.spark.get_custom_session(
        master="yarn",
        app_name="swifting",
        spark_config=spark_config)



    print(f"downloading images from swift using {swift_load} executors")
    files_per_batch=int(mb_per_swift_job/desired_file_size_mb)   
    print(f"using {math.ceil(num_partitions/files_per_batch)} spark jobs, processing around {files_per_batch} files ({mb_per_swift_job}MB) each")
    for batch, file_range in enumerate([range(i,min(num_partitions, i + files_per_batch)) for i in range(0, num_partitions, files_per_batch)]):
        paths = [f'{temp_wiki_dir}part-{f:05d}-*' for f in file_range ]
        # wiki_batch = swift_spark.read.format('avro').load(paths)        
        wiki_batch = swift_spark.read.parquet(paths)        
        images, errors = download_images(input_df=wiki_batch, thumb_size=thumb_size, partitions=None)
        print(f'downloading batch {batch} - files {file_range}')
        (images
           .withColumn('image_bytes_b64_sha1', F.sha1(F.col('image_bytes_b64')))
           .write.format("avro").mode("overwrite").save(f'{images_hdfs_output}batch_{batch}')
        )
        if swift_download_errors is not None:
            print(f"writing errors from swift")
            errors.write.format("avro").mode("overwrite").save(f'{swift_download_errors}batch_{batch}')
        print(f'finished batch {batch}')
    print("all done")
# %%
# sn='2021-01'
# swift_spark.sql(f"select * from wmf.mediawiki_wikitext_current where snapshot='{sn}'").show()
commons_images_to_hdfs(images_hdfs_output='test_images/',snapshot='2021-02',thumb_size='400px',desired_file_size_mb=200,mb_per_swift_job=25,swift_download_errors='test_errors/')
# %%


images = swift_spark.read.format('avro').load('test_images/*').show()
#%%
errors = swift_spark.read.format('avro').load('test_errors/*')
errors.select('download_error').limit(10).collect()
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
