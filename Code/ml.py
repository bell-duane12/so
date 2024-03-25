"""
Projeto: Previsao de Geracao em Usinas Fotovoltaicas.
"""

def data_reading_from_database(arguments):
    """
    Leitura dos dados do banco em disco para a memoria ram.
    """
    import sqlalchemy
    import pandas
    
    print("1) Reading data from database into ram ..")

    db = sqlalchemy.create_engine("postgresql://ssit:4t1ufvSGD@localhost:5432/ssit_ufv")
    connection = db.connect()

    # Solar Station Data ----------------------------------------------------------------------------------------
    # Irradiância [W/m²] no plano dos painéis
    query_poa_irradiance = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=79002;"
    poa_irradiance = pandas.read_sql(query_poa_irradiance, con=connection)

    # Temperatura [°C] do Módulo
    query_module_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78275;"
    module_temperature = pandas.read_sql(query_module_temperature, con=connection)
    
    # Velocidade [m/s] do Vento
    query_wind_speed = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78274;"
    wind_speed = pandas.read_sql(query_wind_speed, con=connection)
    
    # Umidade [%] do Ar
    query_air_umidity = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78273;"
    air_umidity = pandas.read_sql(query_air_umidity, con=connection)
    
    # Temperatura [°C] do Ar
    query_air_temperature = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78272;"
    air_temperature = pandas.read_sql(query_air_temperature, con=connection)
    # -----------------------------------------------------------------------------------------------------------
    
    # Inverter Data -------------------------------------------------------------------------------------------------
    
    # UG1 ---------------------------------------------------------------------
    
    # Daily Power Yield [kWh] ----------------------------------------
    # Inversor Sungrow 1.1
    query_daily_yield_inv_11 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78345;"
    daily_yield_inv_11 = pandas.read_sql(query_daily_yield_inv_11, con=connection)
    
    # Inversor Sungrow 1.2
    query_daily_yield_inv_12 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78368;"
    daily_yield_inv_12 = pandas.read_sql(query_daily_yield_inv_12, con=connection)
    
    # Inversor Sungrow 1.3
    query_daily_yield_inv_13 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78391;"
    daily_yield_inv_13 = pandas.read_sql(query_daily_yield_inv_13, con=connection)
    
    # Inversor Sungrow 1.4
    query_daily_yield_inv_14 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78414;"
    daily_yield_inv_14 = pandas.read_sql(query_daily_yield_inv_14, con=connection)
    
    # Inversor Sungrow 1.5
    query_daily_yield_inv_15 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
    daily_yield_inv_15 = pandas.read_sql(query_daily_yield_inv_15, con=connection)
    
    # Inversor Sungrow 1.6
    query_daily_yield_inv_16 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78460;"
    daily_yield_inv_16 = pandas.read_sql(query_daily_yield_inv_16, con=connection)
    
    # Inversor Sungrow 1.7
    query_daily_yield_inv_17 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78483;"
    daily_yield_inv_17 = pandas.read_sql(query_daily_yield_inv_17, con=connection)
    
    # Inversor Sungrow 1.8
    query_daily_yield_inv_18 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78506;"
    daily_yield_inv_18 = pandas.read_sql(query_daily_yield_inv_18, con=connection)
    # ----------------------------------------------------------------
    # -------------------------------------------------------------------------
    
    # UG2 ---------------------------------------------------------------------
    query_daily_yield_inv_21 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78529;"
    daily_yield_inv_21 = pandas.read_sql(query_daily_yield_inv_21, con=connection)
    
    # Inversor Sungrow 2.2
    query_daily_yield_inv_22 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78552;"
    daily_yield_inv_22 = pandas.read_sql(query_daily_yield_inv_22, con=connection)
    
    # Inversor Sungrow 2.3
    query_daily_yield_inv_23 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78575;"
    daily_yield_inv_23 = pandas.read_sql(query_daily_yield_inv_23, con=connection)
    
    # Inversor Sungrow 2.4
    query_daily_yield_inv_24 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78598;"
    daily_yield_inv_24 = pandas.read_sql(query_daily_yield_inv_24, con=connection)
    
    # Inversor Sungrow 2.5
    query_daily_yield_inv_25 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78437;"
    daily_yield_inv_25 = pandas.read_sql(query_daily_yield_inv_25, con=connection)
    
    # Inversor Sungrow 2.6
    query_daily_yield_inv_26 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78644;"
    daily_yield_inv_26 = pandas.read_sql(query_daily_yield_inv_26, con=connection)
    
    # Inversor Sungrow 2.7
    query_daily_yield_inv_27 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78667;"
    daily_yield_inv_27 = pandas.read_sql(query_daily_yield_inv_27, con=connection)
    
    # Inversor Sungrow 2.8
    query_daily_yield_inv_28 = "SELECT event_date, tm_value from tm_history_120_2024_01 where high_code=78690;"
    daily_yield_inv_28 = pandas.read_sql(query_daily_yield_inv_28, con=connection)
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
    data_frame = {}
    data_frame = pandas.DataFrame( {
    	       #'event_date':         poa_irradiance['event_date'],
    	       'poa_irradiance':     poa_irradiance['tm_value'],
    	       'module_temperature': module_temperature['tm_value'],
    	       'wind_speed':         wind_speed['tm_value'],
    	       'air_umidity':        air_umidity['tm_value'],
    	       'air_temperature':    air_temperature['tm_value'],
    	       'daily_yield_inv_11': daily_yield_inv_11['tm_value'],
    	       'daily_yield_inv_12': daily_yield_inv_12['tm_value'],
    	       'daily_yield_inv_13': daily_yield_inv_13['tm_value'],
    	       'daily_yield_inv_14': daily_yield_inv_14['tm_value'],
    	       'daily_yield_inv_15': daily_yield_inv_15['tm_value'],
    	       'daily_yield_inv_16': daily_yield_inv_16['tm_value'],
    	       'daily_yield_inv_17': daily_yield_inv_17['tm_value'],
    	       'daily_yield_inv_18': daily_yield_inv_18['tm_value'],
    	       'daily_yield_inv_21': daily_yield_inv_21['tm_value'],
    	       'daily_yield_inv_22': daily_yield_inv_22['tm_value'],
    	       'daily_yield_inv_23': daily_yield_inv_23['tm_value'],
    	       'daily_yield_inv_24': daily_yield_inv_24['tm_value'],
    	       'daily_yield_inv_25': daily_yield_inv_25['tm_value'],
    	       'daily_yield_inv_26': daily_yield_inv_26['tm_value'],
    	       'daily_yield_inv_27': daily_yield_inv_27['tm_value'],
    	       'daily_yield_inv_28': daily_yield_inv_28['tm_value'],
    	      }
                )

    return data_frame

def data_cleansing(data_frame, column_name, whisker_factor=1.5):
    """
    Remocao de outliers e dados inconsistentes.
    """
    import pandas
    
    print("2) Data cleansing ..")

    q1 = data_frame[column_name].quantile(0.25)
    q3 = data_frame[column_name].quantile(0.75)
    iqr = q3 - q1
    filter = (data_frame[column_name] >= q1 - whisker_factor*iqr) & (data_frame[column_name] <= q3 + whisker_factor*iqr)
    return data_frame.loc[filter]

def data_missing_value_treatment(data_frame, column_name, range):
    """
    """
    from missingpy import MissForest
    import pandas
    
    print("3) Missing value imputation ..")

    data_frame_filled = MissForest().fit_transform(data_frame)
    return pandas.DataFrame(data_frame_filled, column_name)

def data_wrangling(data_frame, column_name, function):
    """
    """
    print("4) Data wrangling ..")
    pass

def feature_normalization(data_frame):
    """
    """
    pass

def exploratory_data_analysis(data_frame):
    """
    """
    pass

def modeling(data_frame):
    """
    """
    pass

def modeling_evaluation(data_frame):
    """
    """
    pass

def main():
    """
    Funcao principal.
    """
    data_reading_from_database(None)
    data_cleansing(None)
    data_missing_value_treatment(None)
    data_wrangling(None)


if __name__ == "__main__":
    main()
    
