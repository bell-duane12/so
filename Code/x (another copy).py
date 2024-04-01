import sqlalchemy
import pandas
from functools import reduce

import sklearn.neighbors._base
import sys
sys.modules['sklearn.neighbors.base'] = sklearn.neighbors._base
from missingpy import MissForest



db = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")
connection = db.connect()

# Solar Station Data ----------------------------------------------------------------------------------------
# Irradiância [W/m²] no plano dos painéis
query_poa_irradiance = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=79002;"
poa_irradiance = pandas.read_sql(query_poa_irradiance, con=connection)
poa_irradiance = poa_irradiance.rename(columns={'tm_value': 'irradiance'})
poa_irradiance = poa_irradiance.sort_values(by='event_date')

# Temperatura [°C] do Módulo
query_module_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78275;"
module_temperature = pandas.read_sql(query_module_temperature, con=connection)
module_temperature = module_temperature.rename(columns={'tm_value': 'module_temperature'})
module_temperature = module_temperature.sort_values(by='event_date')

# Velocidade [m/s] do Vento
query_wind_speed = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78274;"
wind_speed = pandas.read_sql(query_wind_speed, con=connection)
wind_speed = wind_speed.rename(columns={'tm_value': 'wind_speed'})
wind_speed = wind_speed.sort_values(by='event_date')

# Umidade [%] do Ar
query_air_umidity = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78273;"
air_umidity = pandas.read_sql(query_air_umidity, con=connection)
air_umidity = air_umidity.rename(columns={'tm_value': 'air_umidity'})
air_umidity = air_umidity.sort_values(by='event_date')

# Temperatura [°C] do Ar
query_air_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78272;"
air_temperature = pandas.read_sql(query_air_temperature, con=connection)
air_temperature = air_temperature.rename(columns={'tm_value': 'air_temperature'})
air_temperature = air_temperature.sort_values(by='event_date')
# -----------------------------------------------------------------------------------------------------------

# Inverter Data -------------------------------------------------------------------------------------------------

# UG1 ---------------------------------------------------------------------

# Daily Power Yield [kWh] ----------------------------------------
# Inversor Sungrow 1.1
query_daily_yield_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78345;"
daily_yield_inv_11 = pandas.read_sql(query_daily_yield_inv_11, con=connection)
daily_yield_inv_11 = daily_yield_inv_11.rename(columns={'tm_value': 'daily_yield_inv_11'})
daily_yield_inv_11 = daily_yield_inv_11.sort_values(by='event_date')

# Inversor Sungrow 1.2
query_daily_yield_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78368;"
daily_yield_inv_12 = pandas.read_sql(query_daily_yield_inv_12, con=connection)
daily_yield_inv_12 = daily_yield_inv_12.rename(columns={'tm_value': 'daily_yield_inv_12'})
daily_yield_inv_12 = daily_yield_inv_12.sort_values(by='event_date')

# Inversor Sungrow 1.3
query_daily_yield_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78391;"
daily_yield_inv_13 = pandas.read_sql(query_daily_yield_inv_13, con=connection)
daily_yield_inv_13 = daily_yield_inv_13.rename(columns={'tm_value': 'daily_yield_inv_13'})
daily_yield_inv_13 = daily_yield_inv_13.sort_values(by='event_date')

# Inversor Sungrow 1.4
query_daily_yield_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78414;"
daily_yield_inv_14 = pandas.read_sql(query_daily_yield_inv_14, con=connection)
daily_yield_inv_14 = daily_yield_inv_14.rename(columns={'tm_value': 'daily_yield_inv_14'})
daily_yield_inv_14 = daily_yield_inv_14.sort_values(by='event_date')

# Inversor Sungrow 1.5
query_daily_yield_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
daily_yield_inv_15 = pandas.read_sql(query_daily_yield_inv_15, con=connection)
daily_yield_inv_15 = daily_yield_inv_15.rename(columns={'tm_value': 'daily_yield_inv_15'})
daily_yield_inv_15 = daily_yield_inv_15.sort_values(by='event_date')

# Inversor Sungrow 1.6
query_daily_yield_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78460;"
daily_yield_inv_16 = pandas.read_sql(query_daily_yield_inv_16, con=connection)
daily_yield_inv_16 = daily_yield_inv_16.rename(columns={'tm_value': 'daily_yield_inv_16'})
daily_yield_inv_16 = daily_yield_inv_16.sort_values(by='event_date')

# Inversor Sungrow 1.7
query_daily_yield_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78483;"
daily_yield_inv_17 = pandas.read_sql(query_daily_yield_inv_17, con=connection)
daily_yield_inv_17 = daily_yield_inv_17.rename(columns={'tm_value': 'daily_yield_inv_17'})
daily_yield_inv_17 = daily_yield_inv_17.sort_values(by='event_date')

# Inversor Sungrow 1.8
query_daily_yield_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78506;"
daily_yield_inv_18 = pandas.read_sql(query_daily_yield_inv_18, con=connection)
daily_yield_inv_18 = daily_yield_inv_18.rename(columns={'tm_value': 'daily_yield_inv_18'})
daily_yield_inv_18 = daily_yield_inv_18.sort_values(by='event_date')
# ----------------------------------------------------------------
# -------------------------------------------------------------------------

# UG2 ---------------------------------------------------------------------
# Inversor Sungrow 2.1
query_daily_yield_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78529;"
daily_yield_inv_21 = pandas.read_sql(query_daily_yield_inv_21, con=connection)
daily_yield_inv_21 = daily_yield_inv_21.rename(columns={'tm_value': 'daily_yield_inv_21'})
daily_yield_inv_21 = daily_yield_inv_21.sort_values(by='event_date')

# Inversor Sungrow 2.2
query_daily_yield_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78552;"
daily_yield_inv_22 = pandas.read_sql(query_daily_yield_inv_22, con=connection)
daily_yield_inv_22 = daily_yield_inv_22.rename(columns={'tm_value': 'daily_yield_inv_22'})
daily_yield_inv_22 = daily_yield_inv_22.sort_values(by='event_date')

# Inversor Sungrow 2.3
query_daily_yield_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78575;"
daily_yield_inv_23 = pandas.read_sql(query_daily_yield_inv_23, con=connection)
daily_yield_inv_23 = daily_yield_inv_23.rename(columns={'tm_value': 'daily_yield_inv_23'})
daily_yield_inv_23 = daily_yield_inv_23.sort_values(by='event_date')

# Inversor Sungrow 2.4
query_daily_yield_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78598;"
daily_yield_inv_24 = pandas.read_sql(query_daily_yield_inv_24, con=connection)
daily_yield_inv_24 = daily_yield_inv_24.rename(columns={'tm_value': 'daily_yield_inv_24'})
daily_yield_inv_24 = daily_yield_inv_24.sort_values(by='event_date')

# Inversor Sungrow 2.5
query_daily_yield_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
daily_yield_inv_25 = pandas.read_sql(query_daily_yield_inv_25, con=connection)
daily_yield_inv_25 = daily_yield_inv_25.rename(columns={'tm_value': 'daily_yield_inv_25'})
daily_yield_inv_25 = daily_yield_inv_25.sort_values(by='event_date')

# Inversor Sungrow 2.6
query_daily_yield_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78644;"
daily_yield_inv_26 = pandas.read_sql(query_daily_yield_inv_26, con=connection)
daily_yield_inv_26 = daily_yield_inv_26.rename(columns={'tm_value': 'daily_yield_inv_26'})
daily_yield_inv_26 = daily_yield_inv_26.sort_values(by='event_date')

# Inversor Sungrow 2.7
query_daily_yield_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78667;"
daily_yield_inv_27 = pandas.read_sql(query_daily_yield_inv_27, con=connection)
daily_yield_inv_27 = daily_yield_inv_27.rename(columns={'tm_value': 'daily_yield_inv_27'})
daily_yield_inv_27 = daily_yield_inv_27.sort_values(by='event_date')

# Inversor Sungrow 2.8
query_daily_yield_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78690;"
daily_yield_inv_28 = pandas.read_sql(query_daily_yield_inv_28, con=connection)
daily_yield_inv_28 = daily_yield_inv_28.rename(columns={'tm_value': 'daily_yield_inv_28'})
daily_yield_inv_28 = daily_yield_inv_28.sort_values(by='event_date')
# -------------------------------------------------------------------------

# Outras variáveis interessantes a nível dos inversores (a maioria diretamente relacionada à geração diária) --
# Total DC power [W]
# Total active power [kW]
# Total power yield [kWh]
# MPPT 1 current [A]
# MPPT 1 voltage [V]
# Internal temperature [°C]
# -------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------

# Compila todos os data frames em uma lista
data_frames = [ #poa_irradiance,
                #module_temperature,
                #wind_speed,
                #air_umidity,
                #air_temperature,
                daily_yield_inv_11,
                #daily_yield_inv_12,
                #daily_yield_inv_13,
                #daily_yield_inv_14,
                #daily_yield_inv_15,
                daily_yield_inv_16,
                #daily_yield_inv_17,
                #daily_yield_inv_18,
                #daily_yield_inv_21,
                #daily_yield_inv_22,
                #daily_yield_inv_23,
                #daily_yield_inv_24,
                #daily_yield_inv_25,
                #daily_yield_inv_26,
                #daily_yield_inv_27,
                #daily_yield_inv_28,
                poa_irradiance,
                module_temperature,
                #wind_speed,
                #air_umidity,
                air_temperature
              ]
#data_frames.sort(key=len)#, reverse=True)


# Merge data frames
#data_frame = reduce(lambda df_left, df_right: pandas.merge(df_left, df_right, on=['event_date'],
#                                                           how='left'), data_frames)

datetimes_full = pandas.Series(pandas.date_range(start='1/1/2024', end='2/1/2024',
                                                 freq=pandas.Timedelta('5min')), name="event_date")

# Teste
data_frame = reduce(lambda df_left, df_right: pandas.merge_asof(df_left, df_right,
                                                                on='event_date',
                                                                tolerance=pandas.Timedelta("5m")), data_frames)
data_frame = data_frame.set_index('event_date')

new = pandas.merge_asof(datetimes_full, daily_yield_inv_11, on='event_date', tolerance=pandas.Timedelta("5m"))
new.isna().sum()
new['daily_yield_inv_11'].isna().sum() + len(daily_yield_inv_11)

#data_frame_n.between_time(start_time='00:00', end_time='02:00')
#data_frame.at_time('23:57:40')

print(data_frame.isna().sum())

df = module_temperature
column_name = 'module_temperature'
whisker_factor = 1.5
q1 = df[column_name].quantile(0.25)
q3 = df[column_name].quantile(0.75)
iqr = q3 - q1
filter = (df[column_name] >= q1 - whisker_factor*iqr) & (df[column_name] <= q3 + whisker_factor*iqr)
