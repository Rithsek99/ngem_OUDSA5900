# Import necessary libraries
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dropout, Dense
from keras.regularizers import l2
from keras.callbacks import EarlyStopping
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('peru_data.db')
# Read the data from the database
data = pd.read_sql('SELECT * FROM country_death_positive_cases', conn)
# Extracting second column only [i.e., number of cases data] and converting into numpy array
training_set = data.iloc[:, 2:3].values
dataset_total = training_set

# Dividing test and train data 
len_of_test_data = 20
training_set = training_set[:-len_of_test_data]
test_data = training_set[-len_of_test_data:]

# Feature scaling
sc = MinMaxScaler(feature_range= (0, 1))
training_set_scaled = sc.fit_transform(training_set)

# Creating a datastructure with 60 timestamps and 1 output.
X_train = []
y_train = []
timestamp = 10
for i in range(timestamp,len(training_set)):
    X_train.append(training_set_scaled[i-timestamp:i, 0])
    y_train.append(training_set_scaled[i])
X_train, y_train = np.array(X_train), np.array(y_train)

# Reshaping first dim - size of train data, sec dim - number of time steps, third dim - number of indicators 
X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
X_train.shape

# Part 2 - Building the RNN 
# Importing the keras Libraries and packages

# Initializing the RNN
model = Sequential()
model.add(LSTM(units=50,return_sequences=True,input_shape=(X_train.shape[1], 1)))
model.add(Dropout(0.2))
model.add(LSTM(units=50,return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(units=50,return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(units=50))
model.add(Dropout(0.2))
model.add(Dense(units=1))

# Compiling the RNN
model.compile(optimizer='adam',loss='mean_squared_error')

# Fitting the RNN to the training set
model.fit(X_train,y_train,epochs=40,batch_size=64)

# Part 3 - Making the predictions and visualizing the results
# Getting the real number of cases
real_num_cases = test_data

# Getting the predicted number of cases 
inputs = dataset_total[len(dataset_total)-len(test_data)-10: ]
inputs = inputs.reshape(-1, 1)
inputs = sc.transform(inputs)
X_test = []
timestamp = 10
finaliter = len_of_test_data + timestamp
for i in range(timestamp,finaliter):
    X_test.append(inputs[i-timestamp:i, 0])
X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))
predicted_cases = model.predict(X_test)
predicted_cases = sc.inverse_transform(predicted_cases)

print(predicted_cases)