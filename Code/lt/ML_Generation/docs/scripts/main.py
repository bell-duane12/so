import pandas
import sys, getopt
import sqlalchemy
from functools import reduce

from sklearn.model_selection import train_test_split

import matplotlib.pyplot as plt

import data_preprocessing
from modeling import autoregressive



def main():

    STA_CODE = '120'             # Código da Estação/Usina (nesse caso, a usina de Coromandel)
    QTD_UGS = 2                  # Quantidade de Unidades Geradoras (UGs) existentes na Usina
    QTD_INVERSORES_POR_UG = 8    # Quantidade de Inversores em cada UG
    START_DATE = "'2023-01-01'"  # Data de início dos dados a serem selecionados
    END_DATE =   "'2024-03-31'"  # Data final dos dados a serem selecionados

    # Banco de Dados
    db_user =      'ssit'
    db_password =  '4t1ufvSGD'
    db_ip =        '192.168.2.21'
    db_port =      '54324'
    db_name =      'ssit_comerc'

    # Map weather variables to high_codes
    weather_variables_map = {'irradiance':         75953, # all ugs
                             'air_temperature':    75954, # all ugs
                            }

    # Map generation variables to high_codes (in order)
    generation_variables_map = {
                         'daily_yield_inv': [76050, # 1.1 -- ug_id.inverter_id
                                             76027, # 1.2
                                             76073, # 1.3
                                             76096, # 1.4
                                             76119, # 1.5
                                             76142, # 1.6
                                             76165, # 1.7
                                             76188, # 1.8
                                             76211, # 2.1
                                             76234, # 2.2
                                             76257, # 2.3
                                             76280, # 2.4
                                             76303, # 2.5
                                             76326, # 2.6
                                             76349, # 2.7
                                             76372, # 2.8
                                            ],
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

    # Cria série temporal com todas as estampas de tempo
    full_timeseries = pandas.Series(pandas.date_range(start=data['irradiance']['event_date'].min().date(),
                                                        end=data['irradiance']['event_date'].max().date() + pandas.Timedelta(days=1),
                                                       freq=pandas.Timedelta('5 min')), name="event_date")

    # Lista de referências a todas as variáveis, acompanhadas pelas estampas de tempo
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


    # Imputa valores não existentes 
    #for key in generation_variables_map.keys():
    #    for column in data_frame:
    #        if key in column:
    #            data_frame[column] = data_frame[column].bfill()

    data_frame = data_preprocessing.impute_missing_values(data_frame)
    data_frame.index = datetime_index


    # Em coluna única, agrega pela soma a geração de todos os inversores
    for key in generation_variables_map.keys():
      cols = []
      for column in data_frame:
          if key in column:
              cols.append(column)
      data_frame['global_' + key] = data_frame[cols].sum(axis=1)


    # Salva na database os dados processados, na tabela processed_data
    #data_frame.to_sql(name='processed_data', con=db, if_exists='replace', index_label='timestamp')


    # Modelagem e avaliação ---------------------------------------------------------------------------------

    # Cria colunas separadas para o mês, dia, ano, hora, minuto e segundo
    data_frame['month']  = data_frame.index.month
    data_frame['day']    = data_frame.index.day
    data_frame['year']   = data_frame.index.year
    data_frame['hour']   = data_frame.index.hour
    data_frame['minute'] = data_frame.index.minute
    data_frame['second'] = data_frame.index.second

    # Variáveis preditoras
    features = ['irradiance',        
                'air_temperature',   
                'year', 'month', 'day', 'hour', 'minute', 'second'
               ]

    # Variável alvo
    target = 'global_daily_yield_inv'

    # Seleciona e nomeia os dados de interesse 
    df = data_frame.reindex(columns=features)
    X = df.values
    y = data_frame[target].values.reshape(-1, 1)

    # Particiona o conjunto de dados históricos em um conjunto para treinamento do modelo e outro para teste do modelo
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(X, y, data_frame.index, test_size=0.1, shuffle=False)

    # Modelagem autoregressiva e avaliação do modelo
    forecast = autoregressive().modeling(X_train, X_test, y_train, y_test, idx_train, idx_test)

    # Plota no mesmo gráfico os valores de geração medidos e os valores de geração previstos pelo modelo
    plt.plot(forecast['ds'], forecast['yhat'], color='blue')
    plt.plot(forecast['ds'], y_test, color='red')
    plt.legend(['Prev. Autorreg.', 'Real (Medido)'])
    plt.ylabel('Geração Diária Global [kWh]')
    plt.xlabel('Data [aaaa-mm-dd]')
    plt.show()

    # -------------------------------------------------------------------------------------------------------




if __name__ == "__main__":
   main()

