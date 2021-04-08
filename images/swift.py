#%%
import base64
from random import randrange
import hashlib
import imageio
from io import BytesIO
import math
import os
from PIL import Image
import requests
from urllib.parse import unquote
import wmfdata
#%%
# the max number of threads that can query swift at the same time. should be a multiple of 4 
threads_querying_swift = 80
cores_per_executor = 4
executors = int(threads_querying_swift/cores_per_executor)
spark_config = {
    'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4',
    'spark.driver.extraJavaOptions' : ' '.join('-D{}={}'.format(k, v) for k, v in {
        'http.proxyHost': 'webproxy.eqiad.wmnet',
        'http.proxyPort': '8080',
        'https.proxyHost': 'webproxy.eqiad.wmnet',
        'https.proxyPort': '8080',
    }.items()),
    'spark.sql.shuffle.partitions': 512,
    'spark.dynamicAllocation.maxExecutors': executors,
    'spark.executor.memory': '8g',
    'spark.executor.cores': cores_per_executor,
    'spark.driver.memory': '4g',
}


# %%

# 400px  
mb_per_image_400px = 61.8e3/1e6
# 300px 
mb_per_image_300px = 39.4e3/1e6
# measured with 400px
images_per_second_per_thread = 1e6/(4.6*60*60)/(20*4)
# images on commons to download
num_images_commons = 53e6

# %%
def estimate_partitions(num_images, thumbnail_size, file_size_mb=200):
    # mb_per_image = mb_per_image_400px * (thumbnail_size**2/400**2)
    mb_per_image = mb_per_image_300px * (thumbnail_size**2/ 300**2)
    num_partitions = int(num_images*mb_per_image/file_size_mb)
    total_mb = num_images*mb_per_image
    print(f"""estimate required number of partitions
    for {num_images} images, expected GB {num_images*mb_per_image/1e3} of image data for {thumbnail_size}px thumbnails
    use {num_partitions} partitions for files of size {file_size_mb}mb
    expected total duration with {threads_querying_swift} cores: {num_images/images_per_second_per_thread/3600/24/threads_querying_swift} days
    """)
    return num_partitions
#%%


#%%
swift_spark = wmfdata.spark.get_custom_session(
    master="yarn",
    app_name="swifting",
    spark_config=spark_config,
    ship_python_env=True)


from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark

# %%
# TODO these methods should be in their own module eventually  
def decode_b64(image_b64):
    """decode the base64 encoded image bytes into a byte array"""
    return base64.b64decode(image_b64)

def open_image(image_bytes):
    """returns a PIL.Image from bytes"""
    return Image.open(BytesIO(image_bytes))

image_schema="""
struct<
    image_bytes_b64:string,
    format:string,
    width:int,
    height:int,
    image_bytes_sha1:string,
    error:string
>
""" 


@F.udf(returnType=image_schema)
def parse_image(image_bytes_b64):
    try:    
        image_bytes = decode_b64(image_bytes_b64)
        image = open_image(image_bytes)
        image_bytes_sha1 = hashlib.sha1(image_bytes).hexdigest()        
        return Row(
            image_bytes_b64=image_bytes_b64,
            format=image.format,
            width=image.width,
            height=image.height,
            image_bytes_sha1=image_bytes_sha1,
            error=None
        )
    except Exception as e:
        print(f"Error parsing image bytes. Exception: {e}")
        return Row(
            image_bytes_b64=image_bytes_b64,
            format=None,
            width=None,
            height=None,
            image_bytes_sha1=None,
            error=str(e))

#%%
@F.udf(returnType=T.BooleanType())
def filter_images(page_title):
    """Return true if we are generally interested in this row"""
    prefixes = ['File:']
    good_start = any(page_title.startswith(prefix) for prefix in prefixes)
    suffixes = ['.png', '.jpg', '.jpeg', '.gif']
    good_end = any(page_title.endswith(suffix) for suffix in suffixes)
    return good_start and good_end


swift_host = "https://ms-fe.svc.eqiad.wmnet"
max_retries = 5
timeout =  15

def get_image_bytes(project, file_name, thumb_size):
    md5 = hashlib.md5(file_name.encode('utf-8')).hexdigest()
    shard = f"{md5[0]}/{md5[:2]}"
    uri = f"{swift_host}/wikipedia/{project}/thumb/{shard}/{file_name}/{thumb_size}-{file_name}"
    res = requests.get(uri, verify=True, timeout=timeout)
    res.raise_for_status()
    return res.content

@F.udf(returnType=T.ArrayType(T.StringType()))
def download_image(file_name, project, thumb_size):
    #built in retries causes some dependency issue on the worker, doing retries manually
    error=None
    for retry_count in range(max_retries):
        try:
            image_bytes = get_image_bytes(project, file_name, thumb_size)
            return (base64.b64encode(image_bytes).decode('utf-8'), None)
        except requests.exceptions.HTTPError as e:
            print(f"try {retry_count}, swift download error for file {file_name}. Exception: {e}")
            error=str(e)
            if e.response.status_code==404:
                # no need to retry
                break
        except Exception as e:
            print(f"try {retry_count}, swift download error for file {file_name}. Exception: {e}")
            error=str(e)
    return (None, error)

def download_images(input_df, thumb_size, partitions):
    """
    `input_df` is expected to have a `image_file_name` and a `project` column
    returns two dataframes
        `input` dataframe with a `image_bytes_b64` column containing the image bytes
        `input` dataframe with a `download_error` column containing the errors for each file that wasn't downloaded
    """
    input_df = input_df.withColumn('thumbnail_size', F.lit(thumb_size))
    input_df = input_df.repartition(partitions) if partitions is not None else input_df
    attempted_images_bytes = (input_df
        .withColumn('attempted_thumbnail_b64', download_image('image_file_name', 'project', 'thumbnail_size'))
        .persist(pyspark.StorageLevel.DISK_ONLY))

    download_errors = (attempted_images_bytes
        .withColumn('download_error', F.col('attempted_thumbnail_b64').getItem(1))
        .where(F.col('download_error').isNotNull())
        .drop('attempted_thumbnail_b64'))

    with_images_bytes = (attempted_images_bytes
        .where(F.col('attempted_thumbnail_b64').getItem(0).isNotNull())
        .withColumn('image', parse_image(F.col('attempted_thumbnail_b64').getItem(0)))
        # .withColumn('image', F.col('attempted_thumbnail_b64').getItem(0))
        .drop('attempted_thumbnail_b64'))

    return (with_images_bytes, download_errors)

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


def repartition_images_files(
    df, 
    temp_images_dir='images/temp_images_files/',
    desired_file_size_mb=200):
    """
    repartitions and writes a dataframe in a such a way that once the images
    for all files in a partition are downloaded in a subsequent job, the 
    resulting file written the partition has a desired size (200mb is a hdfs friendly number)
    """
    print(f'calculating the number of partitions required for a target file size {desired_file_size_mb}')

    num_images = df.count()
    num_partitions = estimate_partitions(num_images, desired_file_size_mb)

    print(f"repartitioning the input df into {num_partitions} files for batched download jobs")
    (df
        .repartition(num_partitions)
        .write.format("avro").mode("overwrite").save(temp_images_dir))
    print(f'repartitioned temp files written to : {temp_images_dir}')
    return num_partitions


def swift_images_to_hdfs(
    images_hdfs_output,
    num_partitions,
    thumb_size='400px',
    temp_images_dir='images/temp_images_files/',
    desired_file_size_mb=200,
    mb_per_swift_job=100000,
    swift_download_errors=None):
    """
    downloads images from swift and extends the input
    schema with the image bytes as base64 encoded strings
    in avro encoded files
    """
    print(f"downloading images from swift using {swift_load} executors")
    files_per_batch=int(mb_per_swift_job/desired_file_size_mb)   
    print(f"using {math.ceil(num_partitions/files_per_batch)} spark jobs, processing around {files_per_batch} files ({mb_per_swift_job}MB) each")
    for batch, file_range in enumerate([range(i,min(num_partitions, i + files_per_batch)) for i in range(0, num_partitions, files_per_batch)]):
        paths = [f'{temp_images_dir}part-{f:05d}-*' for f in file_range ]
        print(f'files in batch:{paths}')
        wiki_batch = swift_spark.read.format('avro').load(paths)
        # wiki_batch = swift_spark.read.parquet(paths)        
        images, errors = download_images(input_df=wiki_batch, thumb_size=thumb_size, partitions=None)
        print(f'downloading batch {batch} - files {file_range}')
        print(f'number of partitions: {wiki_batch.rdd.getNumPartitions()}')
        print(f'count: {wiki_batch.count()}')
        (images
           .write.format("avro").mode("overwrite").save(f'{images_hdfs_output}batch_{batch}')
        )
        if swift_download_errors is not None:
            print(f"writing errors from swift")
            errors.write.format("avro").mode("overwrite").save(f'{swift_download_errors}batch_{batch}')
        print(f'finished batch {batch}')
    print("all done")


def append_image_bytes(
    input_df, 
    output_dir,
    thumb_size='300px',
    desired_file_size_mb=200,
    mb_per_swift_job=50000,
    swift_download_errors_dir=None):
    """
    `input_df` DataFrame is required to have columns `image_file_name` and `project`.
    `output_dir` the directory where the avro files are written to
      
    The images in the input `input_df` are retrieved from swift, then the input columns and the new image columns are written to avro encoded files. 
    """

    print(f'calculating the number of partitions required for a target file size {desired_file_size_mb}')
    num_images = input_df.count()
    num_partitions = estimate_partitions(num_images, desired_file_size_mb)

    images, errors = download_images(
        input_df=input_df, 
        thumb_size=thumb_size, 
        partitions=num_partitions)

    print(f"downloading images into {images.rdd.getNumPartitions()} files and writing them to {output_dir}")
    (images
       .write.format("avro").mode("overwrite").save(output_dir)
    )
    if swift_download_errors_dir is not None:
        print(f"writing errors from swift to {swift_download_errors_dir}")
        errors.write.csv(
            path=swift_download_errors_dir,
            sep='\t',
            compression='none',
            mode="overwrite")

    return images, errors

