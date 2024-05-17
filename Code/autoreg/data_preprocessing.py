import sklearn.neighbors._base
import sys
sys.modules['sklearn.neighbors.base'] = sklearn.neighbors._base
from missingpy import MissForest
import warnings
warnings.filterwarnings('ignore')
import pandas


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






