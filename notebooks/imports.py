# %load ../imports.py
%matplotlib inline
%load_ext autoreload
%autoreload 2

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import seaborn as sns
width=20
height=3
plt.rcParams["figure.figsize"] = (width,height)
sns.set(rc={'figure.figsize':(width,height)})

#import seaborn as sns
import os
from collections import OrderedDict

from IPython.display import display

pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.set_option("display.max_columns", None)

import folium
import plotly.express as px
import plotly.graph_objects as go

import sys
import os

from sklearn.metrics import r2_score

import scipy.integrate
import seaborn as sns

import pyarrow as pa
import pyarrow.parquet as pq

import dask.dataframe
import statsmodels.api as sm

#sys.path.append('../')
from src.visualization import visualize
import scipy.integrate

try:
    import trip_statistics
except:
    import src.models.pipelines.longterm.scripts.prepdata.trip_statistics
    sys.path.insert(0, src.models.pipelines.longterm.scripts.prepdata.trip_statistics.path)
    import trip_statistics
    
try:
    import trip_id,prepare_dataset,trips
except:
    import src.models.pipelines.longterm.scripts.prepdata.trip
    sys.path.insert(0, src.models.pipelines.longterm.scripts.prepdata.trip.path)
    import trip_id,prepare_dataset,trips

try:
    import clean_statistics
except:
    import src.models.pipelines.longterm.scripts.prepdata.clean_statistics
    sys.path.insert(0, src.models.pipelines.longterm.scripts.prepdata.clean_statistics.path)
    import clean_statistics