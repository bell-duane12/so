import pandas
import sqlalchemy
from functools import reduce

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt

from pmdarima.arima import auto_arima
from statsmodels.tsa.stattools import adfuller

from prophet import Prophet

import data_preprocessing
import modeling


#def main():

STA_CODE = '120'             # Código da Estação/Usina (nesse caso, a usina de Coromandel)
QTD_UGS = 2                  # Quantidade de Unidades Geradoras (UGs) existentes na Usina
QTD_INVERSORES_POR_UG = 8    # Quantidade de Inversores em cada UG
START_DATE = "'2024-01-01'"  # Data de início dos dados a serem selecionados
END_DATE =   "'2024-01-24'"  # Data final dos dados a serem selecionados

# Banco de Dados
db_user =      'ssit'
db_password =  '4t1ufvSGD'
db_ip =        'localhost'
db_port =      '5432'
db_name =      'ssit_ufv'

# Map weather variables to high_codes
weather_variables_map = {'irradiance':         79002, # all ugs
                         #'module_temperature': 78275, # all ugs
                         'air_temperature':    78272, # all ugs
                        }

# Map generation variables to high_codes (in order)
generation_variables_map = {
                     'daily_yield_inv': [78345, # 1.1 -- ug_id.inverter_id
                                         78368, # 1.2
                                         78391, # 1.3
                                         78414, # 1.4
                                         78437, # 1.5
                                         78460, # 1.6
                                         78483, # 1.7
                                         78506, # 1.8
                                         78529, # 2.1
                                         78552, # 2.2
                                         78575, # 2.3
                                         78598, # 2.4
                                         78437, # 2.5
                                         78644, # 2.6
                                         78667, # 2.7
                                         78690, # 2.8
                                        ],
##                     'total_yield_inv': [78346, # 1.1
##                                         78369, # 1.2
##                                         78392, # 1.3
##                                         78415, # 1.4
##                                         78438, # 1.5
##                                         78461, # 1.6
##                                         78484, # 1.7
##                                         78507, # 1.8
##                                         78530, # 2.1
##                                         78553, # 2.2
##                                         78576, # 2.3
##                                         78559, # 2.4
##                                         78622, # 2.5
##                                         78645, # 2.6
##                                         78668, # 2.7
##                                         78691, # 2.8
##                                        ],
##                     'dc_power_inv'   : [78352, # 1.1
##                                         78375, # 1.2
##                                         78398, # 1.3
##                                         78421, # 1.4
##                                         78444, # 1.5
##                                         78467, # 1.6
##                                         78490, # 1.7
##                                         78513, # 1.8
##                                         78536, # 2.1
##                                         78559, # 2.2
##                                         78582, # 2.3
##                                         78605, # 2.4
##                                         78628, # 2.5
##                                         78651, # 2.6
##                                         78674, # 2.7
##                                         78697, # 2.8
##                                        ],
##                     'ac_power_inv'   : [78359, # 1.1
##                                         78382, # 1.2
##                                         78405, # 1.3
##                                         78428, # 1.4
##                                         78451, # 1.5
##                                         78474, # 1.6
##                                         78497, # 1.7
##                                         78520, # 1.8
##                                         78543, # 2.1
##                                         78566, # 2.2
##                                         78589, # 2.3
##                                         78612, # 2.4
##                                         78635, # 2.5
##                                         78658, # 2.6
##                                         78628, # 2.7
##                                         78704, # 2.8
##                                        ],
                    }

# Cria a estrutura que conterá os dados ----------------------------
data = {}
for variable_name, _ in weather_variables_map.items():
    data[variable_name] = pandas.DataFrame()

for variable_name, _ in generation_variables_map.items():
    data[variable_name] = {}
    for ug in range(1, QTD_UGS+1):
        for inverter in range(1, QTD_INVERSORES_POR_UG+1):
            data[variable_name][(ug,inverter)] = pandas.DataFrame() 
# ------------------------------------------------------------------

# Cria lista auxiliar de índices, onde cada índice é representado pela tupla (ug_id, inverter_id)
indexes_list = []
for index, _ in data[next(iter(generation_variables_map))].items():
    indexes_list.append(index) 

# Conecta-se à base de dados PostgreSQL
db = sqlalchemy.create_engine("postgresql://" + db_user + ":" + db_password + "@" + db_ip + ":" + db_port + "/" + db_name)
connection = db.connect()

# Seleciona os dados solarimétricos
for variable, high_code in weather_variables_map.items():
    query = "SELECT event_date, tm_value FROM tm_history WHERE sta_code=" + STA_CODE + "\
             AND event_date BETWEEN " + START_DATE + " AND " + END_DATE + "\
             AND high_code=" + str(high_code) + ';'
    data[variable] = pandas.read_sql(query, con=connection)
    data[variable] = data[variable].rename(columns={'tm_value': variable})
    data[variable] = data[variable].sort_values(by='event_date')
    
# Seleciona os dados de geração
for variable, high_codes in generation_variables_map.items():
    for index, high_code in enumerate(high_codes, start=0):
        query = "SELECT event_date, tm_value FROM tm_history WHERE sta_code=" + STA_CODE + "\
                 AND event_date BETWEEN " + START_DATE + " AND " + END_DATE + "\
                 AND high_code=" + str(high_code) + ';'
        data[variable][indexes_list[index]] = pandas.read_sql(query, con=connection)
        data[variable][indexes_list[index]] = data[variable][indexes_list[index]].rename(columns={'tm_value': variable + str(indexes_list[index])})
        data[variable][indexes_list[index]] = data[variable][indexes_list[index]].sort_values(by='event_date')

# Série temporal com todas as estampas de tempo
full_timeseries = pandas.Series(pandas.date_range(start=data['irradiance']['event_date'].min().date(),
                                                    end=data['irradiance']['event_date'].max().date() + pandas.Timedelta(days=1),
                                                   freq=pandas.Timedelta('5min')), name="event_date")

# Lista de referências a todas as variáveis, acompanhadas pelos timestamps
data_frame_refs = []
for key, _ in data.items():
    if key in generation_variables_map.keys():
        for index in indexes_list:
            data_frame_refs.append(full_timeseries)
            data_frame_refs.append(data[key][index])
            
for key, _ in data.items():
    if key in weather_variables_map.keys():
        data_frame_refs.append(full_timeseries)
        data_frame_refs.append(data[key])
        
# Merge (left-join) pelo timestamp mais próximo (tolerância máxima de 5 minutos)
data_frame = reduce(lambda df_left, df_right: pandas.merge_asof(df_left, df_right,
                                                                on='event_date',
                                                                tolerance=pandas.Timedelta(5, 'minutes')), data_frame_refs)

# Indexa a base de dados 
datetime_index = data_frame.set_index('event_date', drop=False).index
data_frame = data_frame.set_index('event_date', drop=True)
data_frame = data_frame.reset_index(drop=True)


# Verifica se há registros negativos para alguma das colunas
for column in data_frame:
    if (data_frame[column].any() < 0):
        print('Coluna ' + column + ' possui valores negativos!')


data_frame['daily_yield_inv(1, 1)'] = data_frame['daily_yield_inv(1, 1)'].bfill()
data_frame['daily_yield_inv(1, 2)'] = data_frame['daily_yield_inv(1, 2)'].bfill()
data_frame['daily_yield_inv(1, 3)'] = data_frame['daily_yield_inv(1, 3)'].bfill()
data_frame['daily_yield_inv(1, 4)'] = data_frame['daily_yield_inv(1, 4)'].bfill()
data_frame['daily_yield_inv(1, 5)'] = data_frame['daily_yield_inv(1, 5)'].bfill()
data_frame['daily_yield_inv(1, 6)'] = data_frame['daily_yield_inv(1, 6)'].bfill()
data_frame['daily_yield_inv(1, 7)'] = data_frame['daily_yield_inv(1, 7)'].bfill()
data_frame['daily_yield_inv(1, 8)'] = data_frame['daily_yield_inv(1, 8)'].bfill()
data_frame['daily_yield_inv(2, 1)'] = data_frame['daily_yield_inv(2, 1)'].bfill()
data_frame['daily_yield_inv(2, 2)'] = data_frame['daily_yield_inv(2, 2)'].bfill()
data_frame['daily_yield_inv(2, 3)'] = data_frame['daily_yield_inv(2, 3)'].bfill()
data_frame['daily_yield_inv(2, 4)'] = data_frame['daily_yield_inv(2, 4)'].bfill()
data_frame['daily_yield_inv(2, 5)'] = data_frame['daily_yield_inv(2, 5)'].bfill()
data_frame['daily_yield_inv(2, 6)'] = data_frame['daily_yield_inv(2, 6)'].bfill()
data_frame['daily_yield_inv(2, 7)'] = data_frame['daily_yield_inv(2, 7)'].bfill()
data_frame['daily_yield_inv(2, 8)'] = data_frame['daily_yield_inv(2, 8)'].bfill()

# Imputa valores não existentes via machine learning
data_frame = data_preprocessing.impute_missing_values(data_frame)
data_frame.index = datetime_index


# Em coluna única, agrega pela soma a respectiva variável de geração de todos os inversores
for key in generation_variables_map.keys():
  cols = []
  for column in data_frame:
      if key in column:
          cols.append(column)
  data_frame['global_' + key] = data_frame[cols].sum(axis=1)


# Salva na database os dados processados, na tabela processed_data
data_frame.to_sql(name='processed_data', con=db, if_exists='replace', index_label='timestamp')


# Modelagem e avaliação
#modeling.modeling(data_frame)    
# Cria colunas separadas para o mês, dia, ano, hora, minuto e segundo, como requerido pela modelagem
data_frame['month']  = data_frame.index.month
data_frame['day']    = data_frame.index.day
data_frame['year']   = data_frame.index.year
data_frame['hour']   = data_frame.index.hour
data_frame['minute'] = data_frame.index.minute
data_frame['second'] = data_frame.index.second

# Variáveis preditoras
features = [#'global_dc_power_inv', 'global_ac_power_inv', 'global_total_yield_inv',
            'irradiance',
            #'module_temperature',
            'air_temperature',
            'year', 'month', 'day', 'hour', 'minute', 'second'
           ]

# Variável alvo
target = 'global_daily_yield_inv'

# Seleciona e nomeia os dados de interesse 
df = data_frame.reindex(columns=features)
X = df.values
y = data_frame[target].values.reshape(-1, 1)

# Particiona o conjunto de dados em um conjunto para treinamento e outro para teste
X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(X, y, data_frame.index, test_size=0.2, shuffle=False)

# Augmented Dickey-Fuller (ADF) Test -- Stationarity Test
adf_test_result = adfuller(data_frame['global_daily_yield_inv'])
print('Resultado do Teste de Estacionaridade: ')
descriptions = ['ADF Test Statistic: ', 'p-value: ', '# Lags: ', '# Observations: ']
for result, description in zip(adf_test_result, descriptions):
    print(description + str(result))   
if adf_test_result[1] <= 0.05:
    print('Há forte evidência de que a série é estacionária!')
else:
    print('Há forte evidência de que a série não é estacionária!')

number_of_obs_4_each_day = len(data_frame['2024-01-01':'2024-01-01'])

# Modelagem via sarima
##sarima_model = auto_arima(y_train,
##                          #start_p=0,
##                          #d=1,
##                          #start_q=0,
##                          #max_p=4,
##                          #max_d=4,
##                          #max_q=4,
##                          #start_P=0,
##                          #D=1,
##                          #start_Q=0,
##                          #max_P=1,
##                          #max_D=1,
##                          #max_Q=1,
##                          #m=len(idx_test),
##                          seasonal=True, 
##                          trace=True,
##                          supress_warning=True,
##                          stepwise=True,
##                          #n_fits=1
##                        )
##
### Imprime resumo das estatísticas da modelagem
##print(sarima_model.summary())
##
### Predição da série de teste
##prediction = pandas.DataFrame(sarima_model.predict(n_periods=len(idx_test)), index = idx_test)
##
##plt.plot(idx_test, prediction, color='blue')
##plt.plot(idx_test, y_test, color='red')
##plt.show()

# Formata o data frame no formato exigido pelo prophet
##dff = pandas.DataFrame(data_frame['global_daily_yield_inv'])
##dff['ds'] = data_frame.index
##dff.rename(columns={'global_daily_yield_inv':'y'}, inplace=True)
dff = pandas.DataFrame(y_train, columns=['y'])
dff['ds'] = idx_train

# Modelagem via prophet
m = Prophet()
m.fit(dff)

# Forecast via prophet model
dff2 = pandas.DataFrame(y_test, columns=['y'])
dff2['ds'] = idx_test
forecast = m.predict(dff2)

# Plot
plt.plot(idx_test, forecast['yhat'], color='blue')
plt.plot(idx_test, y_test, color='red')
plt.legend(['Prophet Forecast', 'Real (Measured)'])
plt.show()

# Normaliza os dados 
##scaler = MinMaxScaler() 
##X_train = scaler.fit_transform(X_train)
##X_test = scaler.transform(X_test)
##
### Cria o modelo de previsões pelo método de Regressão,
### utilizando os dados selecionados para treinamento do modelo
##regressor = RandomForestRegressor()
##regressor.fit(X_train, y_train)
##
### Prevê os valores da variável alvo para os respectivos dados de teste, e
### avalia o desempenho do modelo via métrica de pontuação r2 (r2 score/coefficient)
##predicted_test_values = regressor.predict(X_test)
##r2_scorE = round(r2_score(predicted_test_values, y_test) * 100, 2)
##print("r2 == ", r2_scorE)
##
### Print
##plt.plot(predicted_test_values, color='blue')
##plt.plot(y_test, color='red')
##plt.show()


#if __name__ == "__main__":
#    main()
