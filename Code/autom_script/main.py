import pandas
import sqlalchemy
from functools import reduce
import data_preprocessing

def main():

        STA_CODE = '120'             # Código da Estação/Usina (nesse caso, a usina de Coromandel)
        QTD_UGS = 2                  # Quantidade de Unidades Geradoras (UGs) existentes na Usina
        QTD_INVERSORES_POR_UG = 8    # Quantidade de Inversores em cada UG
        START_DATE = "'2023-08-01'"  # Data de início dos dados a serem selecionados
        END_DATE =   "'2024-02-01'"  # Data final dos dados a serem selecionados

        # Banco de Dados
        db_user =      'ssit'
        db_password =  '4t1ufvSGD'
        db_ip =        'localhost'
        db_port =      '5432'
        db_name =      'ssit_ufv'

        # Map weather variables to high_codes
        weather_variables_map = {'irradiance':         79002, # all ugs
                                 'module_temperature': 78275, # all ugs
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
                             'total_yield_inv': [78346, # 1.1
                                                 78369, # 1.2
                                                 78392, # 1.3
                                                 78415, # 1.4
                                                 78438, # 1.5
                                                 78461, # 1.6
                                                 78484, # 1.7
                                                 78507, # 1.8
                                                 78530, # 2.1
                                                 78553, # 2.2
                                                 78576, # 2.3
                                                 78559, # 2.4
                                                 78622, # 2.5
                                                 78645, # 2.6
                                                 78668, # 2.7
                                                 78691, # 2.8
                                                ],
                             'dc_power_inv'   : [78352, # 1.1
                                                 78375, # 1.2
                                                 78398, # 1.3
                                                 78421, # 1.4
                                                 78444, # 1.5
                                                 78467, # 1.6
                                                 78490, # 1.7
                                                 78513, # 1.8
                                                 78536, # 2.1
                                                 78559, # 2.2
                                                 78582, # 2.3
                                                 78605, # 2.4
                                                 78628, # 2.5
                                                 78651, # 2.6
                                                 78674, # 2.7
                                                 78697, # 2.8
                                                ],
                             'ac_power_inv'   : [78359, # 1.1
                                                 78382, # 1.2
                                                 78405, # 1.3
                                                 78428, # 1.4
                                                 78451, # 1.5
                                                 78474, # 1.6
                                                 78497, # 1.7
                                                 78520, # 1.8
                                                 78543, # 2.1
                                                 78566, # 2.2
                                                 78589, # 2.3
                                                 78612, # 2.4
                                                 78635, # 2.5
                                                 78658, # 2.6
                                                 78628, # 2.7
                                                 78704, # 2.8
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

        # Série temporal com todas as estampas de tempo
        full_timeseries = pandas.Series(pandas.date_range(start=data['irradiance']['event_date'].min().date(),
                                                            end=data['irradiance']['event_date'].max().date() + pandas.Timedelta(days=1),
                                                           freq=pandas.Timedelta('5min')), name="event_date")

        # Lista de referências a todas as variáveis, acompanhadas pela timeseries 
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

        # Indexa a base de dados pelo timestamp
        data_frame = data_frame.set_index('event_date')

        # Verifica se há registros negativos para alguma das colunas
        for column in data_frame:
            if (data_frame[column].any() < 0):
                print('Coluna ' + column + ' possui valores negativos!')


        # Imputa valores não existentes via machine learning
        data_frame = data_preprocessing.impute_missing_values(data_frame)


        # Em coluna única, agrega pela soma a respectiva variável de geração de todos os inversores
        for key in generation_variables_map.keys():
          cols = []
          for column in data_frame:
              if key in column:
                  cols.append(column)
          data_frame[key + '_4_all'] = data_frame[cols].sum(axis=1)


        # Salva os dados processados na base de dados -------------------------------------------------------
        # Cria a estrutra da tabela processed_data, onde serão inseridos os dados
        processed_data_table = sqlalchemy.Table(
                                 'processed_data', sqlalchemy.MetaData(),
                                 sqlalchemy.Column('DateTime', sqlalchemy.DateTime()),
                                 *(sqlalchemy.Column(column, sqlalchemy.Double()) for column in data_frame))
        # Formata os dados para inserir na base 
        processed_data = [{'DateTime' : [timestamp for timestamp in full_timeseries]}] + [dict([(key, data_frame[key]) for key in data_frame])]
        # Database Statement to insert the data in the selected table
        statement = sqlalchemy.insert(processed_data_table)
        # Insert the data and return the result
        result = connection.execute(statement, processed_data)  
        # ---------------------------------------------------------------------------------------------------



if __name__ == "__main__":
        main()

