#%% 

# %load_ext autoreload
# %autoreload 2

from IPython.core.display import display, HTML
import numpy as np
import pandas as pd
import wmfdata

spark = None

create_spark_session_at_import = False

def make_default_spark_session():
    global spark
    spark = wmfdata.spark.get_session(
        app_name='interactive',
        extra_settings={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4',
            'spark.sql.shuffle.partitions': 512},
        ship_python_env=False)


if create_spark_session_at_import:
    make_default_spark_session()
else:
    # Even if we don't want a running context at first, we still want
    # to import the spark modules, so we run findspark manually
    import findspark
    findspark.init('/usr/lib/spark2')


from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T


# transform is available in spark 3, remove when switching versions
from pyspark.sql.dataframe import DataFrame
def transform(self, func):
    return func(self)
DataFrame.transform = transform

# %%