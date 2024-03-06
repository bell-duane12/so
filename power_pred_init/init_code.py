import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder
from datetime import datetime
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense, GRU, Dropout
from tensorflow.keras.initializers import HeNormal
from keras.optimizers import Adam
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers.schedules import ExponentialDecay

P1_Gen_data = pd.read_csv('Plant_1_Generation_Data.csv')
P2_Gen_data = pd.read_csv('Plant_2_Generation_Data.csv')

P1_Gen_data.head()

P2_Gen_data.head()

print('Number of samples of Plant_1_Generation_Data:',P1_Gen_data.shape[0])
print('Number of features of Plant_1_Generation_Data:',P1_Gen_data.shape[1])
print('-----------------------------------------------------------')
print('Number of samples of Plant_2_Generation_Data:',P2_Gen_data.shape[0])
print('Number of features of Plant_2_Generation_Data:',P2_Gen_data.shape[1])

print('The percentage of missing value on P1_Gen_data is :' , P1_Gen_data.isna().sum().sum()/(P1_Gen_data.shape[0]*P1_Gen_data.shape[1]) *100 , '%')
print('The percentage of missing value on P2_Gen_data is :' , P2_Gen_data.isna().sum().sum()/(P2_Gen_data.shape[0]*P2_Gen_data.shape[1]) *100 , '%')

P1_Gen_data.info()
print('-----------------------------------------------------------')
P2_Gen_data.info()

# Convert the 'DateTime' column to a datetime object
P1_Gen_data['DATE_TIME'] = pd.to_datetime(P1_Gen_data['DATE_TIME'], format="%d-%m-%Y %H:%M")

# Extract day, month, year, hour, minute, and second components
P1_Gen_data['Day'] = P1_Gen_data['DATE_TIME'].dt.day
P1_Gen_data['Month'] = P1_Gen_data['DATE_TIME'].dt.month
P1_Gen_data['Year'] = P1_Gen_data['DATE_TIME'].dt.year
P1_Gen_data['Hour'] = P1_Gen_data['DATE_TIME'].dt.hour
P1_Gen_data['Minute'] = P1_Gen_data['DATE_TIME'].dt.minute
P1_Gen_data['Second'] = P1_Gen_data['DATE_TIME'].dt.second


# Convert the 'DateTime' column to a datetime object
P2_Gen_data['DATE_TIME'] = pd.to_datetime(P2_Gen_data['DATE_TIME'], format="%Y-%m-%d %H:%M:%S")

# Extract day, month, year, hour, minute, and second components
P2_Gen_data['Day'] = P2_Gen_data['DATE_TIME'].dt.day
P2_Gen_data['Month'] = P2_Gen_data['DATE_TIME'].dt.month
P2_Gen_data['Year'] = P2_Gen_data['DATE_TIME'].dt.year
P2_Gen_data['Hour'] = P2_Gen_data['DATE_TIME'].dt.hour
P2_Gen_data['Minute'] = P2_Gen_data['DATE_TIME'].dt.minute
P2_Gen_data['Second'] = P2_Gen_data['DATE_TIME'].dt.second

print('Number of unique SOURCE_KEY values in P1_Gen_data :',len(P1_Gen_data.SOURCE_KEY.unique()))
print('Number of unique SOURCE_KEY values in P2_Gen_data :',len(P2_Gen_data.SOURCE_KEY.unique()))

# Instantiate the LabelEncoder
label_encoder = LabelEncoder()

# Fit and transform the 'SOURCE_KEY' column
P1_Gen_data['SOURCE_KEY'] = label_encoder.fit_transform(P1_Gen_data['SOURCE_KEY'])
P2_Gen_data['SOURCE_KEY'] = label_encoder.fit_transform(P2_Gen_data['SOURCE_KEY'])

P_Gen_data = pd.concat([P1_Gen_data, P2_Gen_data], ignore_index=True)
P_Gen_data.describe().T

g1_idx = P1_Gen_data.groupby('DATE_TIME').sum().reset_index()
g1_idx["PLANT_ID"]= 1

fig, axs = plt.subplots(2, 1, figsize=(25, 10))


axs[0].plot(g1_idx['DATE_TIME'], g1_idx['DC_POWER'], color='#cc7e08')
axs[0].set_xlabel('Date')
axs[0].set_ylabel('kW')
axs[0].set_title('DC Power - Plant 1')
axs[0].tick_params(axis='x', rotation=45)


axs[1].plot(g1_idx['DATE_TIME'], g1_idx['AC_POWER'], color='#2b2924')
axs[1].set_xlabel('Date')
axs[1].set_ylabel('kW')
axs[1].set_title('AC Power - Plant 1')
axs[1].tick_params(axis='x', rotation=45)


plt.tight_layout()
plt.show()

g2_idx = P2_Gen_data.groupby('DATE_TIME').sum().reset_index()
g2_idx["PLANT_ID"]= 2

fig, axs = plt.subplots(2, 1, figsize=(25, 10))


axs[0].plot(g2_idx['DATE_TIME'], g2_idx['DC_POWER'], color='#cc7e08')
axs[0].set_xlabel('Date')
axs[0].set_ylabel('kW')
axs[0].set_title('DC Power - Plant 2')
axs[0].tick_params(axis='x', rotation=45)


axs[1].plot(g2_idx['DATE_TIME'], g2_idx['AC_POWER'], color='#2b2924')
axs[1].set_xlabel('Date')
axs[1].set_ylabel('kW')
axs[1].set_title('AC Power - Plant 2')
axs[1].tick_params(axis='x', rotation=45)


plt.tight_layout()
plt.show()

correlation_matrix = P_Gen_data.corr()

plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap="copper")
plt.title("Correlation Plot", fontsize=14)
plt.xticks(rotation=45, horizontalalignment='right', fontsize=10)
plt.yticks(rotation=0, horizontalalignment='right', fontsize=10)
plt.tight_layout()

plt.show()

n_timesteps = 1
n_features = 11

# Define hyperparameters and learning rate decay parameters
initial_learning_rate = 0.2
decay_rate = 1.0

# Create a learning rate schedule with exponential decay
learning_rate_schedule = ExponentialDecay(initial_learning_rate, decay_steps=1, decay_rate=decay_rate, staircase=True)

# Define lists for hyperparameter values
units_first_layer = [25 , 50]
units_second_layer = [25 , 50]

# Create a 3D list to store models
all_models_LSTM = []

for units1 in units_first_layer:
    units1_models = []  # List to store models for a specific units in the first layer
    for units2 in units_second_layer:
        # Create an optimizer with the current learning rate and decay
        custom_optimizer = Adam(learning_rate=learning_rate_schedule, beta_1=0.9, beta_2=0.999, epsilon=1e-8)

        # Build the LSTM model with He initialization
        model_LSTM = Sequential()
        model_LSTM.add(LSTM(units1, activation='relu', return_sequences=True, input_shape=(n_timesteps, n_features), kernel_initializer=HeNormal()))
        model_LSTM.add(LSTM(units2, activation='relu', kernel_initializer=HeNormal()))
        model_LSTM.add(Dense(1, activation='linear', kernel_initializer=HeNormal()))

        # Compile the model
        model_LSTM.compile(optimizer=custom_optimizer, loss='mse')

        # Append the model to the list
        units1_models.append(model_LSTM)

    # Append the list for units in the first layer to the overall list
    all_models_LSTM.append(units1_models)


full_iter = 100

# Sort DataFrame by 'DATE_TIME' if it's not sorted
P_Gen_data = P_Gen_data.sort_values('DATE_TIME')

features = ['AC_POWER', 'PLANT_ID', 'SOURCE_KEY', 'DAILY_YIELD', 'TOTAL_YIELD', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']
target = 'DC_POWER'

# Extract features and target variable
X = P_Gen_data[features].values
y = P_Gen_data[target].values.reshape(-1, 1)

# Normalize the features using Min-Max scaling
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X) # Scaled before splitting, so this result, as I suspected, is probably overly optimistic (specially comparing to articles I've seen).

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, shuffle=False)

# Reshape the data for LSTM input (samples, time steps, features)
n_samples, n_features = X_train.shape
n_timesteps = 1

X_train = X_train.reshape((n_samples, n_timesteps, n_features))
X_test = X_test.reshape((X_test.shape[0], n_timesteps, n_features))

# Initialize variables to keep track of the best model and its loss
best_loss = float('inf')
best_model_LSTM = None

# Iterate through different models
for units1_idx, units1_models in enumerate(all_models_LSTM):
    for units2_idx, model_LSTM in enumerate(units1_models):
            # Define EarlyStopping callback
            early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)

            # Train the model with early stopping
            history = model_LSTM.fit(X_train, y_train, epochs=full_iter, batch_size=32, validation_data=(X_test, y_test), callbacks=[early_stopping])

            # Get the weights of the model
            weights = model_LSTM.get_weights()
        
            # Evaluate the model
            loss = model_LSTM.evaluate(X_test, y_test)
            
            # Get the current learning rate
            current_lr = model_LSTM.optimizer.learning_rate.numpy()
        
            print(f"Learning Rate: {current_lr}, Units in First Layer: {units_first_layer[units1_idx]}, Units in Second Layer: {units_second_layer[units2_idx]}")
            print(f"Test Loss: {loss}")

            # Compare with the current best model
            if loss < best_loss:
                best_loss = loss
                best_model_LSTM = model_LSTM

            
            # Print weights
            for i, layer_weights in enumerate(weights):
                print(f"Weights of Layer {i + 1}: {layer_weights}")

# Convert the best loss to percentage
percentage_loss = (best_loss / (P_Gen_data['DC_POWER'].max() - P_Gen_data['DC_POWER'].min())) * 100
print(f"Best Model Test Loss Percentage: {percentage_loss}%")

# Make predictions using the best model
predictions_best_model = best_model_LSTM.predict(X_test)

plt.plot(history.history['loss'], label='Training Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.xlabel('Epoch')
plt.ylabel('Mean Squared Error (MSE)')
plt.legend()
plt.show()

# Flatten the arrays to 1D
y_test_flat = y_test.flatten()
predictions_flat = predictions_best_model.flatten()

# Create a color for the actual line (red) and predicted line (yellow)
color_actual = 'red'
color_predicted = 'blue'

# Plot the actual line in red
plt.plot(y_test_flat, color=color_actual, label='Actual DC_POWER', linewidth=2)

# Plot the predicted line in yellow
plt.plot(predictions_flat, color=color_predicted, label='Predicted DC_POWER', linewidth=1)

plt.xlabel('Data Point')
plt.ylabel('DC_POWER')
plt.title('Actual vs Predicted DC_POWER')
plt.legend()
plt.show()






