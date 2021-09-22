#%%
# seems the current working directory is not working with vscode ipython
# import os
# os.chdir("code/research-datasets")
# os.getcwd()

#%%
from images.swift import create_spark_context_for_swift
import os
import wmfdata
#%%


# configuration
# TODO, when this becomes a scheduled pipeline, the configuration become either arguments to the DAG function, or command line args
wiki_db = "commons"
snapshot = "2021-06"

# create a spark context for pre-processing the images data
# note, don't use this context for querying swift!
spark = wmfdata.spark.get_session(
        app_name='prepare-images',
        extra_settings={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4',
            'spark.sql.shuffle.partitions': 512},
        ship_python_env=False)



#pyspark imports can only be made after the spark context is initialized...
from pyspark.sql import Window


#%%

# history = spark.read.load(f"hdfs:///wmf/data/wmf/mediawiki/wikitext/history/snapshot={snapshot}/wiki_db={wiki_db}wiki/")
history = (spark
    .sql(f"""
    select * from wmf.mediawiki_wikitext_history where snapshot='{snapshot}' and wiki_db='{wiki_db}wiki'
    """)
    # .cache()
)

prefixes = ['File:']
# prefixes = ['Commons:Deletion_requests/File:']
suffixes = ['.png', '.jpg', '.jpeg', '.gif']

@F.udf(returnType=T.BooleanType())
def filter_images(page_title):
    """Return true if we are generally interested in this row"""    
    good_start = any(page_title.startswith(prefix) for prefix in prefixes)    
    good_end = any(page_title.endswith(suffix) for suffix in suffixes)
    return good_start and good_end

#%%
#  TODO, use a udf for partioning where the deletion event goes into the same partition as the normal /File:xyz,
# the deleted event Commons:Deletion_requests/File: needs to be added the allowd prefixes in filter_images

# then filter for deleted pages. For now, deleted images result in a 404 entry if error logging is enabled.
# the history includes all edits, the most recent edit is used for each image

images = history.filter(filter_images("page_title"))
w = Window.partitionBy("page_title")

images = (images
    .withColumn('max_ts', F.max('revision_timestamp').over(w))
    .where(F.col('revision_timestamp') == F.col('max_ts'))
    .drop('max_ts')
    # .cache()
    .write.format("parquet").mode("overwrite").save("commons_images/input_images")
)

#%%
# 


# a spark context suitable for querying swift
swift_spark = create_spark_context_for_swift()

images = (swift_spark
    .read
    .parquet("commons_images/input_images/*")
    # .limit(1000)  
    # .cache()
)
#%%    

@F.udf(returnType='string')
def extract_file_name(f):
    return f[5:].replace(" ", "_")
    # return quote(f)

append_df = (innn
    .withColumn("image_file_name", extract_file_name(F.col('page_title')))
    .withColumn("project", F.lit('commons'))    
)

#%%

# i,e = download_images(append_df, 300, 1)
# i.count()
append_df
#%%
append_image_bytes(
    input_df=append_df,
    output_dir='commons_images/pixels',
    swift_download_errors_dir='commons_images/swift_errors'
)
# image_feature = history.select(history["page_title"].alias("file_name"), "common_features.*").cache()

# `image_file_name` and `project`
# %%
# history.printSchema()
x = images.limit(10).collect()
# %%

(swift_spark
    .read.format("avro")
    .load("commons_images/pixels/*")
    .count()
)

# !log started a job in the analytics yarn cluster that downloads commons images from swift

#%%


ee = (swift_spark
    .read.format("avro")
    .load("commons_images/swift_errors/*")
    .select('download_error')
    .collect()
)
# %%
[e.download_error[:4] for e in ee]

# %%
