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

    query = "SELECT * from xxxxx;"
    data_frame = pandas.read_sql(query, con=connection)
    return data_frame;

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
    
