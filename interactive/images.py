
# !pip install pillow
# !pip install imageio

import imageio
from io import BytesIO
from PIL import Image

import numpy as np
import pandas as pd


# TODO, when yarn labels are available, add the label configuration to 
# get a context for the GPU nodes only
spark = wmfdata.spark.get_session(app_name = 'imageing', ship_python_env=True)
