# %%

! pip install pillow
! pip install python-swiftclient
! pip install imageio
! pip install xlsxWriter
# !pip install --ignore-installed --upgrade git+https://github.com/wikimedia/wmfdata-python.git@release
! pip install --ignore-installed --upgrade git+https://github.com/ottomata/wmfdata.git@conda

#%%

import wmfdata

# %%

%matplotlib inline

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt


import imageio
from io import BytesIO
from PIL import Image


from collections import Counter
import datetime
import itertools
import math
import mwparserfromhell
import numpy as np
import os
import pandas as pd
import re

plt.rcParams["figure.figsize"] = (20,15)

# %%
spark = wmfdata.spark.get_session(app_name = 'coviding', ship_python_env=False)

from pyspark.sql import Row, SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from operator import add

# transform is available in spark 3, remove when switching versions
from pyspark.sql.dataframe import DataFrame
def transform(self, func):
    return func(self)
DataFrame.transform = transform
