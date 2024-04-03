

"""
Projeto: Previsão de Geração em Usinas Fotovoltaicas.
"""


# Importa bibliotecas e funções
import sqlalchemy
import pandas
from functools import reduce
import sklearn.neighbors._base
import sys
sys.modules['sklearn.neighbors.base'] = sklearn.neighbors._base
from missingpy import MissForest
import warnings
warnings.filterwarnings('ignore')




# Define função de filtragem de outliers.
def filter_outliers(data_frame, whisker_factor=1.5):
    """
    Remoção de outliers pelo método IQR.
    """ 
    q1 = data_frame.quantile(0.25)
    q3 = data_frame.quantile(0.75)
    iqr = q3 - q1
    outliers = (data_frame < (q1 - whisker_factor*iqr)) | (data_frame > (q3 + whisker_factor*iqr))
    
    # Método #1: Impute outliers (com a média, mediana, etc. --> vamos remover e imputar com machine learning depois)
    #data_frame[outliers] = data_frame.mean()  # Substitui os outliers pela média
    
    # Método #2: Capping outliers (replace with nearest non-outlier value --> maintains overall distribution, while reducing influence of extreme values)
    #data_frame[data_frame < (q1 - whisker_factor*iqr)] = q1 - whisker_factor*iqr  # cap low outliers
    #data_frame[data_frame > (q3 + whisker_factor*iqr)] = q3 + whisker_factor*iqr  # cap high outliers
    
    # Método #3: Remove outliers
    return data_frame[~outliers.any(axis=1)]




# Define função de tratamento de valores inexistentes.
def impute_missing_values(data_frame):
    """
    Imputa valores não existentes via machine learning, utilizando variante do método Random Forest.
    """
    complete_data = MissForest().fit_transform(data_frame)
    return pandas.DataFrame(complete_data, columns=data_frame.columns)





#def main():
"""
Função principal do programa, de onde são chamadas todas as outras funções.
"""

# Conecta com a base de dados PostgreSQL
db = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")
connection = db.connect()

# Solar Station Data ----------------------------------------------------------------------------------------
# Irradiância [W/m²] no plano dos painéis
query_poa_irradiance = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=79002;"
poa_irradiance = pandas.read_sql(query_poa_irradiance, con=connection)
poa_irradiance = poa_irradiance.rename(columns={'tm_value': 'irradiance'})
poa_irradiance = poa_irradiance.sort_values(by='event_date')
poa_irradiance = filter_outliers(poa_irradiance)

# Temperatura [°C] do Módulo
query_module_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78275;"
module_temperature = pandas.read_sql(query_module_temperature, con=connection)
module_temperature = module_temperature.rename(columns={'tm_value': 'module_temperature'})
module_temperature = module_temperature.sort_values(by='event_date')
module_temperature = filter_outliers(module_temperature)

# Temperatura [°C] do Ar
query_air_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78272;"
air_temperature = pandas.read_sql(query_air_temperature, con=connection)
air_temperature = air_temperature.rename(columns={'tm_value': 'air_temperature'})
air_temperature = air_temperature.sort_values(by='event_date')
air_temperature = filter_outliers(air_temperature)
# -----------------------------------------------------------------------------------------------------------

# Inverter Data ---------------------------------------------------------------------------------------------

# UG1 ---------------------------------------------------------------------

# Inversor Sungrow 1.1 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78345;"
daily_yield_inv_11 = pandas.read_sql(query_daily_yield_inv_11, con=connection)
daily_yield_inv_11 = daily_yield_inv_11.rename(columns={'tm_value': 'daily_yield_inv_11'})
daily_yield_inv_11 = daily_yield_inv_11.sort_values(by='event_date')
daily_yield_inv_11 = filter_outliers(daily_yield_inv_11)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78352;"
dc_power_inv_11 = pandas.read_sql(query_dc_power_inv_11, con=connection)
dc_power_inv_11 = dc_power_inv_11.rename(columns={'tm_value': 'dc_power_inv_11'})
dc_power_inv_11 = dc_power_inv_11.sort_values(by='event_date')
dc_power_inv_11 = filter_outliers(dc_power_inv_11)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78359;"
ac_active_power_inv_11 = pandas.read_sql(query_ac_active_power_inv_11, con=connection)
ac_active_power_inv_11 = ac_active_power_inv_11.rename(columns={'tm_value': 'ac_active_power_inv_11'})
ac_active_power_inv_11 = ac_active_power_inv_11.sort_values(by='event_date')
ac_active_power_inv_11 = filter_outliers(ac_active_power_inv_11)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78346;"
total_yield_inv_11 = pandas.read_sql(query_total_yield_inv_11, con=connection)
total_yield_inv_11 = total_yield_inv_11.rename(columns={'tm_value': 'total_yield_inv_11'})
total_yield_inv_11 = total_yield_inv_11.sort_values(by='event_date')
total_yield_inv_11 = filter_outliers(total_yield_inv_11)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.2 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78368;"
daily_yield_inv_12 = pandas.read_sql(query_daily_yield_inv_12, con=connection)
daily_yield_inv_12 = daily_yield_inv_12.rename(columns={'tm_value': 'daily_yield_inv_12'})
daily_yield_inv_12 = daily_yield_inv_12.sort_values(by='event_date')
daily_yield_inv_12 = filter_outliers(daily_yield_inv_12)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78375;"
dc_power_inv_12 = pandas.read_sql(query_dc_power_inv_12, con=connection)
dc_power_inv_12 = dc_power_inv_12.rename(columns={'tm_value': 'dc_power_inv_12'})
dc_power_inv_12 = dc_power_inv_12.sort_values(by='event_date')
dc_power_inv_12 = filter_outliers(dc_power_inv_12)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78382;"
ac_active_power_inv_12 = pandas.read_sql(query_ac_active_power_inv_12, con=connection)
ac_active_power_inv_12 = ac_active_power_inv_12.rename(columns={'tm_value': 'ac_active_power_inv_12'})
ac_active_power_inv_12 = ac_active_power_inv_12.sort_values(by='event_date')
ac_active_power_inv_12 = filter_outliers(ac_active_power_inv_12)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78369;"
total_yield_inv_12 = pandas.read_sql(query_total_yield_inv_12, con=connection)
total_yield_inv_12 = total_yield_inv_12.rename(columns={'tm_value': 'total_yield_inv_12'})
total_yield_inv_12 = total_yield_inv_12.sort_values(by='event_date')
total_yield_inv_12 = filter_outliers(total_yield_inv_12)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.3 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78391;"
daily_yield_inv_13 = pandas.read_sql(query_daily_yield_inv_13, con=connection)
daily_yield_inv_13 = daily_yield_inv_13.rename(columns={'tm_value': 'daily_yield_inv_13'})
daily_yield_inv_13 = daily_yield_inv_13.sort_values(by='event_date')
daily_yield_inv_13 = filter_outliers(daily_yield_inv_13)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78398;"
dc_power_inv_13 = pandas.read_sql(query_dc_power_inv_13, con=connection)
dc_power_inv_13 = dc_power_inv_13.rename(columns={'tm_value': 'dc_power_inv_13'})
dc_power_inv_13 = dc_power_inv_13.sort_values(by='event_date')
dc_power_inv_13 = filter_outliers(dc_power_inv_13)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78405;"
ac_active_power_inv_13 = pandas.read_sql(query_ac_active_power_inv_13, con=connection)
ac_active_power_inv_13 = ac_active_power_inv_13.rename(columns={'tm_value': 'ac_active_power_inv_13'})
ac_active_power_inv_13 = ac_active_power_inv_13.sort_values(by='event_date')
ac_active_power_inv_13 = filter_outliers(ac_active_power_inv_13)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78392;"
total_yield_inv_13 = pandas.read_sql(query_total_yield_inv_13, con=connection)
total_yield_inv_13 = total_yield_inv_13.rename(columns={'tm_value': 'total_yield_inv_13'})
total_yield_inv_13 = total_yield_inv_13.sort_values(by='event_date')
total_yield_inv_13 = filter_outliers(total_yield_inv_13)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.4 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78414;"
daily_yield_inv_14 = pandas.read_sql(query_daily_yield_inv_14, con=connection)
daily_yield_inv_14 = daily_yield_inv_14.rename(columns={'tm_value': 'daily_yield_inv_14'})
daily_yield_inv_14 = daily_yield_inv_14.sort_values(by='event_date')
daily_yield_inv_14 = filter_outliers(daily_yield_inv_14)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78421;"
dc_power_inv_14 = pandas.read_sql(query_dc_power_inv_14, con=connection)
dc_power_inv_14 = dc_power_inv_14.rename(columns={'tm_value': 'dc_power_inv_14'})
dc_power_inv_14 = dc_power_inv_14.sort_values(by='event_date')
dc_power_inv_14 = filter_outliers(dc_power_inv_14)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78428;"
ac_active_power_inv_14 = pandas.read_sql(query_ac_active_power_inv_14, con=connection)
ac_active_power_inv_14 = ac_active_power_inv_14.rename(columns={'tm_value': 'ac_active_power_inv_14'})
ac_active_power_inv_14 = ac_active_power_inv_14.sort_values(by='event_date')
ac_active_power_inv_14 = filter_outliers(ac_active_power_inv_14)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78415;"
total_yield_inv_14 = pandas.read_sql(query_total_yield_inv_14, con=connection)
total_yield_inv_14 = total_yield_inv_14.rename(columns={'tm_value': 'total_yield_inv_14'})
total_yield_inv_14 = total_yield_inv_14.sort_values(by='event_date')
total_yield_inv_14 = filter_outliers(total_yield_inv_14)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.5 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
daily_yield_inv_15 = pandas.read_sql(query_daily_yield_inv_15, con=connection)
daily_yield_inv_15 = daily_yield_inv_15.rename(columns={'tm_value': 'daily_yield_inv_15'})
daily_yield_inv_15 = daily_yield_inv_15.sort_values(by='event_date')
daily_yield_inv_15 = filter_outliers(daily_yield_inv_15)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78444;"
dc_power_inv_15 = pandas.read_sql(query_dc_power_inv_15, con=connection)
dc_power_inv_15 = dc_power_inv_15.rename(columns={'tm_value': 'dc_power_inv_15'})
dc_power_inv_15 = dc_power_inv_15.sort_values(by='event_date')
dc_power_inv_15 = filter_outliers(dc_power_inv_15)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78451;"
ac_active_power_inv_15 = pandas.read_sql(query_ac_active_power_inv_15, con=connection)
ac_active_power_inv_15 = ac_active_power_inv_15.rename(columns={'tm_value': 'ac_active_power_inv_15'})
ac_active_power_inv_15 = ac_active_power_inv_15.sort_values(by='event_date')
ac_active_power_inv_15 = filter_outliers(ac_active_power_inv_15)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78438;"
total_yield_inv_15 = pandas.read_sql(query_total_yield_inv_15, con=connection)
total_yield_inv_15 = total_yield_inv_15.rename(columns={'tm_value': 'total_yield_inv_15'})
total_yield_inv_15 = total_yield_inv_15.sort_values(by='event_date')
total_yield_inv_15 = filter_outliers(total_yield_inv_15)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.6 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78460;"
daily_yield_inv_16 = pandas.read_sql(query_daily_yield_inv_16, con=connection)
daily_yield_inv_16 = daily_yield_inv_16.rename(columns={'tm_value': 'daily_yield_inv_16'})
daily_yield_inv_16 = daily_yield_inv_16.sort_values(by='event_date')
daily_yield_inv_16 = filter_outliers(daily_yield_inv_16)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78467;"
dc_power_inv_16 = pandas.read_sql(query_dc_power_inv_16, con=connection)
dc_power_inv_16 = dc_power_inv_16.rename(columns={'tm_value': 'dc_power_inv_16'})
dc_power_inv_16 = dc_power_inv_16.sort_values(by='event_date')
dc_power_inv_16 = filter_outliers(dc_power_inv_16)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78474;"
ac_active_power_inv_16 = pandas.read_sql(query_ac_active_power_inv_16, con=connection)
ac_active_power_inv_16 = ac_active_power_inv_16.rename(columns={'tm_value': 'ac_active_power_inv_16'})
ac_active_power_inv_16 = ac_active_power_inv_16.sort_values(by='event_date')
ac_active_power_inv_16 = filter_outliers(ac_active_power_inv_16)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78461;"
total_yield_inv_16 = pandas.read_sql(query_total_yield_inv_16, con=connection)
total_yield_inv_16 = total_yield_inv_16.rename(columns={'tm_value': 'total_yield_inv_16'})
total_yield_inv_16 = total_yield_inv_16.sort_values(by='event_date')
total_yield_inv_16 = filter_outliers(total_yield_inv_16)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.7 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78483;"
daily_yield_inv_17 = pandas.read_sql(query_daily_yield_inv_17, con=connection)
daily_yield_inv_17 = daily_yield_inv_17.rename(columns={'tm_value': 'daily_yield_inv_17'})
daily_yield_inv_17 = daily_yield_inv_17.sort_values(by='event_date')
daily_yield_inv_17 = filter_outliers(daily_yield_inv_17)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78490;"
dc_power_inv_17 = pandas.read_sql(query_dc_power_inv_17, con=connection)
dc_power_inv_17 = dc_power_inv_17.rename(columns={'tm_value': 'dc_power_inv_17'})
dc_power_inv_17 = dc_power_inv_17.sort_values(by='event_date')
dc_power_inv_17 = filter_outliers(dc_power_inv_17)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78497;"
ac_active_power_inv_17 = pandas.read_sql(query_ac_active_power_inv_17, con=connection)
ac_active_power_inv_17 = ac_active_power_inv_17.rename(columns={'tm_value': 'ac_active_power_inv_17'})
ac_active_power_inv_17 = ac_active_power_inv_17.sort_values(by='event_date')
ac_active_power_inv_17 = filter_outliers(ac_active_power_inv_17)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78484;"
total_yield_inv_17 = pandas.read_sql(query_total_yield_inv_17, con=connection)
total_yield_inv_17 = total_yield_inv_17.rename(columns={'tm_value': 'total_yield_inv_17'})
total_yield_inv_17 = total_yield_inv_17.sort_values(by='event_date')
total_yield_inv_17 = filter_outliers(total_yield_inv_17)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 1.8 -------------------------------
# Daily Power Yield [kWh] ------------------
query_daily_yield_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78506;"
daily_yield_inv_18 = pandas.read_sql(query_daily_yield_inv_18, con=connection)
daily_yield_inv_18 = daily_yield_inv_18.rename(columns={'tm_value': 'daily_yield_inv_18'})
daily_yield_inv_18 = daily_yield_inv_18.sort_values(by='event_date')
daily_yield_inv_18 = filter_outliers(daily_yield_inv_18)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78513;"
dc_power_inv_18 = pandas.read_sql(query_dc_power_inv_18, con=connection)
dc_power_inv_18 = dc_power_inv_18.rename(columns={'tm_value': 'dc_power_inv_18'})
dc_power_inv_18 = dc_power_inv_18.sort_values(by='event_date')
dc_power_inv_18 = filter_outliers(dc_power_inv_18)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78520;"
ac_active_power_inv_18 = pandas.read_sql(query_ac_active_power_inv_18, con=connection)
ac_active_power_inv_18 = ac_active_power_inv_18.rename(columns={'tm_value': 'ac_active_power_inv_18'})
ac_active_power_inv_18 = ac_active_power_inv_18.sort_values(by='event_date')
ac_active_power_inv_18 = filter_outliers(ac_active_power_inv_18)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78507;"
total_yield_inv_18 = pandas.read_sql(query_total_yield_inv_18, con=connection)
total_yield_inv_18 = total_yield_inv_18.rename(columns={'tm_value': 'total_yield_inv_18'})
total_yield_inv_18 = total_yield_inv_18.sort_values(by='event_date')
total_yield_inv_18 = filter_outliers(total_yield_inv_18)
# ------------------------------------------
# ----------------------------------------------------
# -------------------------------------------------------------------------

# UG2 ---------------------------------------------------------------------
# Inversor Sungrow 2.1 -------------------------------
query_daily_yield_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78529;"
daily_yield_inv_21 = pandas.read_sql(query_daily_yield_inv_21, con=connection)
daily_yield_inv_21 = daily_yield_inv_21.rename(columns={'tm_value': 'daily_yield_inv_21'})
daily_yield_inv_21 = daily_yield_inv_21.sort_values(by='event_date')
daily_yield_inv_21 = filter_outliers(daily_yield_inv_21)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78536;"
dc_power_inv_21 = pandas.read_sql(query_dc_power_inv_21, con=connection)
dc_power_inv_21 = dc_power_inv_21.rename(columns={'tm_value': 'dc_power_inv_21'})
dc_power_inv_21 = dc_power_inv_21.sort_values(by='event_date')
dc_power_inv_21 = filter_outliers(dc_power_inv_21)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78543;"
ac_active_power_inv_21 = pandas.read_sql(query_ac_active_power_inv_21, con=connection)
ac_active_power_inv_21 = ac_active_power_inv_21.rename(columns={'tm_value': 'ac_active_power_inv_21'})
ac_active_power_inv_21 = ac_active_power_inv_21.sort_values(by='event_date')
ac_active_power_inv_21 = filter_outliers(ac_active_power_inv_21)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78530;"
total_yield_inv_21 = pandas.read_sql(query_total_yield_inv_21, con=connection)
total_yield_inv_21 = total_yield_inv_21.rename(columns={'tm_value': 'total_yield_inv_21'})
total_yield_inv_21 = total_yield_inv_21.sort_values(by='event_date')
total_yield_inv_21 = filter_outliers(total_yield_inv_21)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.2 -------------------------------
query_daily_yield_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78552;"
daily_yield_inv_22 = pandas.read_sql(query_daily_yield_inv_22, con=connection)
daily_yield_inv_22 = daily_yield_inv_22.rename(columns={'tm_value': 'daily_yield_inv_22'})
daily_yield_inv_22 = daily_yield_inv_22.sort_values(by='event_date')
daily_yield_inv_22 = filter_outliers(daily_yield_inv_22)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78559;"
dc_power_inv_22 = pandas.read_sql(query_dc_power_inv_22, con=connection)
dc_power_inv_22 = dc_power_inv_22.rename(columns={'tm_value': 'dc_power_inv_22'})
dc_power_inv_22 = dc_power_inv_22.sort_values(by='event_date')
dc_power_inv_22 = filter_outliers(dc_power_inv_22)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78566;"
ac_active_power_inv_22 = pandas.read_sql(query_ac_active_power_inv_22, con=connection)
ac_active_power_inv_22 = ac_active_power_inv_22.rename(columns={'tm_value': 'ac_active_power_inv_22'})
ac_active_power_inv_22 = ac_active_power_inv_22.sort_values(by='event_date')
ac_active_power_inv_22 = filter_outliers(ac_active_power_inv_22)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78553;"
total_yield_inv_22 = pandas.read_sql(query_total_yield_inv_22, con=connection)
total_yield_inv_22 = total_yield_inv_22.rename(columns={'tm_value': 'total_yield_inv_22'})
total_yield_inv_22 = total_yield_inv_22.sort_values(by='event_date')
total_yield_inv_22 = filter_outliers(total_yield_inv_22)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.3 -------------------------------
query_daily_yield_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78575;"
daily_yield_inv_23 = pandas.read_sql(query_daily_yield_inv_23, con=connection)
daily_yield_inv_23 = daily_yield_inv_23.rename(columns={'tm_value': 'daily_yield_inv_23'})
daily_yield_inv_23 = daily_yield_inv_23.sort_values(by='event_date')
daily_yield_inv_23 = filter_outliers(daily_yield_inv_23)
# Total DC power [W] -----------------------
query_dc_power_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78582;"
dc_power_inv_23 = pandas.read_sql(query_dc_power_inv_23, con=connection)
dc_power_inv_23 = dc_power_inv_23.rename(columns={'tm_value': 'dc_power_inv_23'})
dc_power_inv_23 = dc_power_inv_23.sort_values(by='event_date')
dc_power_inv_23 = filter_outliers(dc_power_inv_23)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78589;"
ac_active_power_inv_23 = pandas.read_sql(query_ac_active_power_inv_23, con=connection)
ac_active_power_inv_23 = ac_active_power_inv_23.rename(columns={'tm_value': 'ac_active_power_inv_23'})
ac_active_power_inv_23 = ac_active_power_inv_23.sort_values(by='event_date')
ac_active_power_inv_23 = filter_outliers(ac_active_power_inv_23)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78576;"
total_yield_inv_23 = pandas.read_sql(query_total_yield_inv_23, con=connection)
total_yield_inv_23 = total_yield_inv_23.rename(columns={'tm_value': 'total_yield_inv_23'})
total_yield_inv_23 = total_yield_inv_23.sort_values(by='event_date')
total_yield_inv_23 = filter_outliers(total_yield_inv_23)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.4 -------------------------------
query_daily_yield_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78598;"
daily_yield_inv_24 = pandas.read_sql(query_daily_yield_inv_24, con=connection)
daily_yield_inv_24 = daily_yield_inv_24.rename(columns={'tm_value': 'daily_yield_inv_24'})
daily_yield_inv_24 = daily_yield_inv_24.sort_values(by='event_date')
daily_yield_inv_24 = filter_outliers(daily_yield_inv_24)
# Total DC power [W] -----------------------
query_dc_power_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78605;"
dc_power_inv_24 = pandas.read_sql(query_dc_power_inv_24, con=connection)
dc_power_inv_24 = dc_power_inv_24.rename(columns={'tm_value': 'dc_power_inv_24'})
dc_power_inv_24 = dc_power_inv_24.sort_values(by='event_date')
dc_power_inv_24 = filter_outliers(dc_power_inv_24)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78612;"
ac_active_power_inv_24 = pandas.read_sql(query_ac_active_power_inv_24, con=connection)
ac_active_power_inv_24 = ac_active_power_inv_24.rename(columns={'tm_value': 'ac_active_power_inv_24'})
ac_active_power_inv_24 = ac_active_power_inv_24.sort_values(by='event_date')
ac_active_power_inv_24 = filter_outliers(ac_active_power_inv_24)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78559;"
total_yield_inv_24 = pandas.read_sql(query_total_yield_inv_24, con=connection)
total_yield_inv_24 = total_yield_inv_24.rename(columns={'tm_value': 'total_yield_inv_24'})
total_yield_inv_24 = total_yield_inv_24.sort_values(by='event_date')
total_yield_inv_24 = filter_outliers(total_yield_inv_24)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.5 -------------------------------
query_daily_yield_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
daily_yield_inv_25 = pandas.read_sql(query_daily_yield_inv_25, con=connection)
daily_yield_inv_25 = daily_yield_inv_25.rename(columns={'tm_value': 'daily_yield_inv_25'})
daily_yield_inv_25 = daily_yield_inv_25.sort_values(by='event_date')
daily_yield_inv_25 = filter_outliers(daily_yield_inv_25)
# Total DC power [W] -----------------------
query_dc_power_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78628;"
dc_power_inv_25 = pandas.read_sql(query_dc_power_inv_25, con=connection)
dc_power_inv_25 = dc_power_inv_25.rename(columns={'tm_value': 'dc_power_inv_25'})
dc_power_inv_25 = dc_power_inv_25.sort_values(by='event_date')
dc_power_inv_25 = filter_outliers(dc_power_inv_25)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78635;"
ac_active_power_inv_25 = pandas.read_sql(query_ac_active_power_inv_25, con=connection)
ac_active_power_inv_25 = ac_active_power_inv_25.rename(columns={'tm_value': 'ac_active_power_inv_25'})
ac_active_power_inv_25 = ac_active_power_inv_25.sort_values(by='event_date')
ac_active_power_inv_25 = filter_outliers(ac_active_power_inv_25)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78622;"
total_yield_inv_25 = pandas.read_sql(query_total_yield_inv_25, con=connection)
total_yield_inv_25 = total_yield_inv_25.rename(columns={'tm_value': 'total_yield_inv_25'})
total_yield_inv_25 = total_yield_inv_25.sort_values(by='event_date')
total_yield_inv_25 = filter_outliers(total_yield_inv_25)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.6 -------------------------------
query_daily_yield_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78644;"
daily_yield_inv_26 = pandas.read_sql(query_daily_yield_inv_26, con=connection)
daily_yield_inv_26 = daily_yield_inv_26.rename(columns={'tm_value': 'daily_yield_inv_26'})
daily_yield_inv_26 = daily_yield_inv_26.sort_values(by='event_date')
daily_yield_inv_26 = filter_outliers(daily_yield_inv_26)
# Total DC power [W] -----------------------
query_dc_power_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78651;"
dc_power_inv_26 = pandas.read_sql(query_dc_power_inv_26, con=connection)
dc_power_inv_26 = dc_power_inv_26.rename(columns={'tm_value': 'dc_power_inv_26'})
dc_power_inv_26 = dc_power_inv_26.sort_values(by='event_date')
dc_power_inv_26 = filter_outliers(dc_power_inv_26)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78658;"
ac_active_power_inv_26 = pandas.read_sql(query_ac_active_power_inv_26, con=connection)
ac_active_power_inv_26 = ac_active_power_inv_26.rename(columns={'tm_value': 'ac_active_power_inv_26'})
ac_active_power_inv_26 = ac_active_power_inv_26.sort_values(by='event_date')
ac_active_power_inv_26 = filter_outliers(ac_active_power_inv_26)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78645;"
total_yield_inv_26 = pandas.read_sql(query_total_yield_inv_26, con=connection)
total_yield_inv_26 = total_yield_inv_26.rename(columns={'tm_value': 'total_yield_inv_26'})
total_yield_inv_26 = total_yield_inv_26.sort_values(by='event_date')
total_yield_inv_26 = filter_outliers(total_yield_inv_26)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.7 -------------------------------
query_daily_yield_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78667;"
daily_yield_inv_27 = pandas.read_sql(query_daily_yield_inv_27, con=connection)
daily_yield_inv_27 = daily_yield_inv_27.rename(columns={'tm_value': 'daily_yield_inv_27'})
daily_yield_inv_27 = daily_yield_inv_27.sort_values(by='event_date')
daily_yield_inv_27 = filter_outliers(daily_yield_inv_27)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78674;"
dc_power_inv_27 = pandas.read_sql(query_dc_power_inv_27, con=connection)
dc_power_inv_27 = dc_power_inv_27.rename(columns={'tm_value': 'dc_power_inv_27'})
dc_power_inv_27 = dc_power_inv_27.sort_values(by='event_date')
dc_power_inv_27 = filter_outliers(dc_power_inv_27)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78628;"
ac_active_power_inv_27 = pandas.read_sql(query_ac_active_power_inv_27, con=connection)
ac_active_power_inv_27 = ac_active_power_inv_27.rename(columns={'tm_value': 'ac_active_power_inv_27'})
ac_active_power_inv_27 = ac_active_power_inv_27.sort_values(by='event_date')
ac_active_power_inv_27 = filter_outliers(ac_active_power_inv_27)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78668;"
total_yield_inv_27 = pandas.read_sql(query_total_yield_inv_27, con=connection)
total_yield_inv_27 = total_yield_inv_27.rename(columns={'tm_value': 'total_yield_inv_27'})
total_yield_inv_27 = total_yield_inv_27.sort_values(by='event_date')
total_yield_inv_27 = filter_outliers(total_yield_inv_27)
# ------------------------------------------
# ----------------------------------------------------

# Inversor Sungrow 2.8 -------------------------------
query_daily_yield_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78690;"
daily_yield_inv_28 = pandas.read_sql(query_daily_yield_inv_28, con=connection)
daily_yield_inv_28 = daily_yield_inv_28.rename(columns={'tm_value': 'daily_yield_inv_28'})
daily_yield_inv_28 = daily_yield_inv_28.sort_values(by='event_date')
daily_yield_inv_28 = filter_outliers(daily_yield_inv_28)
# ------------------------------------------
# Total DC power [W] -----------------------
query_dc_power_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78697;"
dc_power_inv_28 = pandas.read_sql(query_dc_power_inv_28, con=connection)
dc_power_inv_28 = dc_power_inv_28.rename(columns={'tm_value': 'dc_power_inv_28'})
dc_power_inv_28 = dc_power_inv_28.sort_values(by='event_date')
dc_power_inv_28 = filter_outliers(dc_power_inv_28)
# ------------------------------------------
# Total active power [kW] ------------------
query_ac_active_power_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78704;"
ac_active_power_inv_28 = pandas.read_sql(query_ac_active_power_inv_28, con=connection)
ac_active_power_inv_28 = ac_active_power_inv_28.rename(columns={'tm_value': 'ac_active_power_inv_28'})
ac_active_power_inv_28 = ac_active_power_inv_28.sort_values(by='event_date')
ac_active_power_inv_28 = filter_outliers(ac_active_power_inv_28)
# ------------------------------------------
# Total power yield [kWh] ------------------
query_total_yield_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78691;"
total_yield_inv_28 = pandas.read_sql(query_total_yield_inv_28, con=connection)
total_yield_inv_28 = total_yield_inv_28.rename(columns={'tm_value': 'total_yield_inv_28'})
total_yield_inv_28 = total_yield_inv_28.sort_values(by='event_date')
total_yield_inv_28 = filter_outliers(total_yield_inv_28)
# ------------------------------------------
# ----------------------------------------------------
# -------------------------------------------------------------------------


# Série temporal com todas as estampas de tempo
full_timeseries = pandas.Series(pandas.date_range(start=poa_irradiance['event_date'].min().date(),
                                                    end=poa_irradiance['event_date'].max().date() + pandas.Timedelta(days=1),
                                                   freq=pandas.Timedelta('5min')), name="event_date")


 
# Cria uma lista de referências das variáveis, acompanhas pelas estampas de tempo
data_frame_refs = [ 
                    full_timeseries, daily_yield_inv_11,
                    full_timeseries, daily_yield_inv_12,
                    full_timeseries, daily_yield_inv_13,
                    full_timeseries, daily_yield_inv_14,
                    full_timeseries, daily_yield_inv_15,
                    full_timeseries, daily_yield_inv_16,
                    full_timeseries, daily_yield_inv_17,
                    full_timeseries, daily_yield_inv_18,
                    full_timeseries, daily_yield_inv_21,
                    full_timeseries, daily_yield_inv_22,
                    full_timeseries, daily_yield_inv_23,
                    full_timeseries, daily_yield_inv_24,
                    full_timeseries, daily_yield_inv_25,
                    full_timeseries, daily_yield_inv_26,
                    full_timeseries, daily_yield_inv_27,
                    full_timeseries, daily_yield_inv_28,
                    
                    full_timeseries, poa_irradiance,
                    full_timeseries, module_temperature,
                    full_timeseries, air_temperature,
                    
                    full_timeseries, total_yield_inv_11,
                    full_timeseries, total_yield_inv_12,
                    full_timeseries, total_yield_inv_13,
                    full_timeseries, total_yield_inv_14,
                    full_timeseries, total_yield_inv_15,
                    full_timeseries, total_yield_inv_16,
                    full_timeseries, total_yield_inv_17,
                    full_timeseries, total_yield_inv_18,
                    full_timeseries, total_yield_inv_21,
                    full_timeseries, total_yield_inv_22,
                    full_timeseries, total_yield_inv_23,
                    full_timeseries, total_yield_inv_24,
                    full_timeseries, total_yield_inv_25,
                    full_timeseries, total_yield_inv_26,
                    full_timeseries, total_yield_inv_27,
                    full_timeseries, total_yield_inv_28,

                    full_timeseries, dc_power_inv_11,
                    full_timeseries, dc_power_inv_12,
                    full_timeseries, dc_power_inv_13,
                    full_timeseries, dc_power_inv_14,
                    full_timeseries, dc_power_inv_15,
                    full_timeseries, dc_power_inv_16,
                    full_timeseries, dc_power_inv_17,
                    full_timeseries, dc_power_inv_18,
                    full_timeseries, dc_power_inv_21,
                    full_timeseries, dc_power_inv_22,
                    full_timeseries, dc_power_inv_23,
                    full_timeseries, dc_power_inv_24,
                    full_timeseries, dc_power_inv_25,
                    full_timeseries, dc_power_inv_26,
                    full_timeseries, dc_power_inv_27,
                    full_timeseries, dc_power_inv_28,

                    full_timeseries, ac_active_power_inv_11,
                    full_timeseries, ac_active_power_inv_12,
                    full_timeseries, ac_active_power_inv_13,
                    full_timeseries, ac_active_power_inv_14,
                    full_timeseries, ac_active_power_inv_15,
                    full_timeseries, ac_active_power_inv_16,
                    full_timeseries, ac_active_power_inv_17,
                    full_timeseries, ac_active_power_inv_18,
                    full_timeseries, ac_active_power_inv_21,
                    full_timeseries, ac_active_power_inv_22,
                    full_timeseries, ac_active_power_inv_23,
                    full_timeseries, ac_active_power_inv_24,
                    full_timeseries, ac_active_power_inv_25,
                    full_timeseries, ac_active_power_inv_26,
                    full_timeseries, ac_active_power_inv_27,
                    full_timeseries, ac_active_power_inv_28,
                    
                  ]


# Merge (left-join) pelo timestamp mais próximo (tolerância máxima de 5 minutos)
data_frame = reduce(lambda df_left, df_right: pandas.merge_asof(df_left, df_right,
                                                                on='event_date',
                                                                tolerance=pandas.Timedelta(5, 'minutes')), data_frame_refs)


# Indexa a base de dados pelo timestamp
data_frame = data_frame.set_index('event_date')


# Verifica se há registros negativos para alguma das colunas
for column in data_frame:
    if (data_frame[column].any() < 0):
        print('Coluna ' + column + ' possui valores negativos!')
        

# Imputa valores não existentes via machine learning
data_frame = impute_missing_values(data_frame)


# Especifica colunas a serem agregadas pela soma
cols = [
        'daily_yield_inv_11',
        'daily_yield_inv_12',
        'daily_yield_inv_13',
        'daily_yield_inv_14',
        'daily_yield_inv_15',
        'daily_yield_inv_16',
        'daily_yield_inv_17',
        'daily_yield_inv_18',
        'daily_yield_inv_21',
        'daily_yield_inv_22',
        'daily_yield_inv_23',
        'daily_yield_inv_24',
        'daily_yield_inv_25',
        'daily_yield_inv_26',
        'daily_yield_inv_27',
        'daily_yield_inv_28',
       ]

# Em coluna única, agrega pela soma a geração diária de todos os inversores
data_frame['daily_yield_4_all_invs'] = data_frame[cols].sum(axis=1)

cols = [
        'total_yield_inv_11',
        'total_yield_inv_12',
        'total_yield_inv_13',
        'total_yield_inv_14',
        'total_yield_inv_15',
        'total_yield_inv_16',
        'total_yield_inv_17',
        'total_yield_inv_18',
        'total_yield_inv_21',
        'total_yield_inv_22',
        'total_yield_inv_23',
        'total_yield_inv_24',
        'total_yield_inv_25',
        'total_yield_inv_26',
        'total_yield_inv_27',
        'total_yield_inv_28',
       ]

# Em coluna única, agrega pela soma a geração total de todos os inversores
data_frame['total_yield_4_all_invs'] = data_frame[cols].sum(axis=1)

cols = [
        'dc_power_inv_11',
        'dc_power_inv_12',
        'dc_power_inv_13',
        'dc_power_inv_14',
        'dc_power_inv_15',
        'dc_power_inv_16',
        'dc_power_inv_17',
        'dc_power_inv_18',
        'dc_power_inv_21',
        'dc_power_inv_22',
        'dc_power_inv_23',
        'dc_power_inv_24',
        'dc_power_inv_25',
        'dc_power_inv_26',
        'dc_power_inv_27',
        'dc_power_inv_28',
       ]

# Em coluna única, agrega pela soma a potência dc de todos os inversores
data_frame['total_dc_power_4_all_invs'] = data_frame[cols].sum(axis=1)

cols = [
        'ac_active_power_inv_11',
        'ac_active_power_inv_12',
        'ac_active_power_inv_13',
        'ac_active_power_inv_14',
        'ac_active_power_inv_15',
        'ac_active_power_inv_16',
        'ac_active_power_inv_17',
        'ac_active_power_inv_18',
        'ac_active_power_inv_21',
        'ac_active_power_inv_22',
        'ac_active_power_inv_23',
        'ac_active_power_inv_24',
        'ac_active_power_inv_25',
        'ac_active_power_inv_26',
        'ac_active_power_inv_27',
        'ac_active_power_inv_28',
       ]

# Em coluna única, agrega pela soma a potência ac de todos os inversores
data_frame['total_ac_power_4_all_invs'] = data_frame[cols].sum(axis=1)



#if __name__ == "__main__":
#    main()

