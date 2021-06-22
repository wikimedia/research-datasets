# %%

# !pip install pillow
# !pip install imageio

import imageio
from io import BytesIO
from PIL import Image

import numpy as np
import pandas as pd
import wmfdata

# TODO, when yarn labels are available, add the label configuration to 
# get a context for the GPU nodes only, ship with tensorflow dependency
spark = wmfdata.spark.get_session(app_name = 'imageing', ship_python_env=False)

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
# %%
