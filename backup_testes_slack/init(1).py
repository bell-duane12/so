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
from sklearn.preprocessing import LabelEncoder
import warnings

sns.set()
warnings.filterwarnings("ignore")

# Load data from Postgres
eng = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")

connection = eng.connect()

query = "SELECT * from expected_data;"

# data frame
df = pd.read_sql(query, con=connection)

print(df.head())

# check for missing values
print(df.info())

# statistical overview
print(df.describe())

# correlation
correlation_matrix = df.corr()

plt.figure(figsize=(10,8))
#sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap="copper")
#plt.title("Correlation plot", fontsize=14)
#plt.show()

#plt.plot(df['exp_timestamp'], df['exp_energy'])
plt.plot(df['exp_energy'])
plt.title("Expected Energy [MWh]", fontsize=14)
plt.show()

plt.plot(df['exp_pr'])
plt.title("Expected Performance Ratio [%]", fontsize=14)
plt.show()

plt.plot(df['exp_irradiance'])
plt.title("Expected Irradiance [KWh/m²]", fontsize=14)
plt.show()

