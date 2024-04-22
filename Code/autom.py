import pandas
import sqlalchemy

QTD_UGS = 2
QTD_INVERSORES_POR_UG = 8

constant_variables_across_ugs = ['irradiance', 'module_temperature', 'air_temperature']

variable_keys = ['daily_yield_inv',
                 'total_yield_inv',
                 'dc_power_inv',
                 'ac_power_inv',
                ]

data = {}


for variable_name in constant_variables_across_ugs:
    data[variable_name] = pandas.DataFrame()

for variable_name in variable_keys:
    data[variable_name] = {}
    for ug in range(1, QTD_UGS+1):
        for inverter in range(1, QTD_INVERSORES_POR_UG+1):
            data[variable_name][(ug,inverter)] = pandas.DataFrame() #(ug,inverter)

############################################### 

map_constant_variables_across_ugs = {'irradiance': 79002, 'module_temperature': 78275, 'air_temperature': 78272}

map_variable_keys = {'daily_yield_inv': {(1,1): 78345, (1,2): 78368},
                     'total_yield_inv': {(1,1): 78346, (1,2): 78369},
                     'dc_power_inv'   : {(1,1): 78352, (1,2): 78375},
                     'ac_power_inv'   : {(1,1): 78359, (1,2): 78382},
                    }

db = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")
connection = db.connect()

for variable, high_code in map_constant_variables_across_ugs.items():
    query = "SELECT event_date, tm_value FROM tm_history WHERE sta_code=120 \
             AND event_date BETWEEN '2023-08-01' and '2024-02-01' \
             AND high_code=" + str(high_code) + ';'
    data[variable] = pandas.read_sql(query, con=connection)
    data[variable] = data[variable].rename(columns={'tm_value': variable})

for variable, high_codes in map_variable_keys.items():
    for index, high_code in high_codes.items():
        query = "SELECT event_date, tm_value FROM tm_history WHERE sta_code=120 \
                 AND event_date BETWEEN '2023-08-01' and '2024-02-01' \
                 AND high_code=" + str(high_code) + ';'
        data[variable][index] = pandas.read_sql(query, con=connection)
        data[variable][index] = data[variable][index].rename(columns={'tm_value': variable})


