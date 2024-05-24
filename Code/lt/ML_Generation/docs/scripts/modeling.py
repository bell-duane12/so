import pandas

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import r2_score

from abc import ABC, abstractmethod

from sklearn.ensemble import RandomForestRegressor
from prophet import Prophet



# Classe base abstrata para previsões
class forecasting(ABC):
    
    @abstractmethod
    def modeling(self, X_train, X_test, y_train, y_test, idx_train, idx_test):
        pass




# Modelagem autoregressiva.
# Para a modelagem autoregressiva, os valores futuros de geração são previstos 
# com base nos valores históricos de geração.
class autoregressive(forecasting):
    
    # Implementa o método abstrato da classe base
    def modeling(self, X_train, X_test, y_train, y_test, idx_train, idx_test):
        
        # Cria as tabelas no formato exigido pelo Prophet
        df_train = pandas.DataFrame(y_train, columns=['y'])
        df_train['ds'] = idx_train
        df_test = pandas.DataFrame(y_test, columns=['y'])
        df_test['ds'] = idx_test
        
        # Treina o modelo de previsões via Prophet
        model = Prophet()
        model.fit(df_train)

        # Prevê os valores da variável alvo para o intervalo de teste, a partir do modelo treinado
        forecast = model.predict(df_test)
        
        return forecast




# Modelagem multivariável (via regressão múltipla).
# Para a modelagem multivariável, tem-se regressores adicionais 
# (irradiância e/ou temperatura do ar, etc) para prever a geração.
# Sendo assim, para prever valores futuros de geração, é necessário 
# obter os valores futuros dos preditores via API de previsão do tempo.
# A GHI consumida via API pode ser transposta para POA utilizando a 
# biblioteca pvlib.
# Os preditores também são necessários para um cálculo mais assertivo do PR.
class multiregressive(forecasting):

    # Implementa o método abstrato da classe base
    def modeling(X_train, X_test, y_train, y_test, idx_train, idx_test):

        # Normaliza os dados 
        scaler = MinMaxScaler() 
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

        # Cria o modelo de previsões pelo método de Regressão,
        # utilizando os dados históricos selecionados para treinamento do modelo
        regressor = RandomForestRegressor()
        regressor.fit(X_train, y_train)

        # Prevê os valores da variável alvo para os respectivos dados de teste, e
        # avalia o desempenho do modelo via métrica de pontuação r2 (r2 score/coefficient)
        predicted_test_values = regressor.predict(X_test)
        r2_scor = round(r2_score(predicted_test_values, y_test) * 100, 2)
        print("r2 == ", r2_scor)
