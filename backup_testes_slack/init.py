import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import plotly.express as px
import numpy as np
import sqlalchemy # handle database
import missingno as msno
import tensorflow as tf
from tensorflow.keras import layers
from sklearn.metrics import mean_absolute_error as mae
from sklearn.preprocessing import MinMaxScaler
import warnings

sns.set()
warnings.filterwarnings("ignore")

# Load data from Postgres
eng = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")

connection = eng.connect()

query = "SELECT * from expected_data;"

# data frame
df = pd.read_sql(query, con=connection)

df.head()

# check for missing values
df.info()

# statistical overview
print(df.describe())

# correlation
correlation_matrix = df.corr()

plt.figure(figsize=(10,8))
sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap="copper")
plt.title("Correlation plot", fontsize=14)
plt.show()
