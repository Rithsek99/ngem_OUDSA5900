import csv
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.layers import LSTM
from tensorflow.keras.utils import plot_model
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error


# Load the data
dataframe = pd.read_csv('LIMA_DPT_PVN_DISTRICT.csv', usecols=[5], engine='python')
dataset = dataframe.values
dataset = dataset.astype('float32')
# fix random seed for reproducibility
tf.random.set_seed(7)

# Normalize the dataset
scaler = MinMaxScaler(feature_range=(0, 1))
dataset = scaler.fit_transform(dataset)

# Split into train and test sets
train_size = int(len(dataset) * 0.8)
test_size = len(dataset) - train_size
train, test = dataset[0:train_size,:], dataset[train_size:len(dataset),:]

# convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)

# Reshape into X=t and Y=t+1
look_back = 3
trainX, trainY = create_dataset(train, look_back)
testX, testY = create_dataset(test, look_back)

# Reshape input to be [samples, time steps, features] using Window Method
trainX = np.reshape(trainX, (trainX.shape[0], trainX.shape[1], 1))
testX = np.reshape(testX, (testX.shape[0], testX.shape[1], 1))

# Build the LSTM model
model = Sequential()
model.add(LSTM(units=25,return_sequences=True,input_shape=(look_back, 1)))
model.add(Dropout(0.2))
model.add(LSTM(units=25,return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(units=10,return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(units=10))
model.add(Dropout(0.4))
model.add(Dense(units=1))
model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(trainX, trainY, epochs=100, batch_size=32, verbose=2)


plot_model(model, to_file='model_window.png', show_shapes=True, show_layer_names=True, rankdir='LR')
# Make predictions
trainPredict = model.predict(trainX)
testPredict = model.predict(testX)
# invert predictions
trainPredict = scaler.inverse_transform(trainPredict)
trainY = scaler.inverse_transform([trainY])
testPredict = scaler.inverse_transform(testPredict)
testY = scaler.inverse_transform([testY])
# calculate root mean squared error
trainScore = np.sqrt(mean_squared_error(trainY[0], trainPredict[:,0]))
print('Train Score: %.2f RMSE' % (trainScore))
testScore = np.sqrt(mean_squared_error(testY[0], testPredict[:,0]))
print('Test Score: %.2f RMSE' % (testScore))

# Define the number of time steps to predict
n_steps = 14

# Initialize an empty list to store the predicted values
predicted_values = []

# Initialize the input data with the last n time steps from the original data
input_data = dataset[-look_back:]

# Use the LSTM model to predict the next time step and append it to the list of predicted values
for i in range(n_steps):
    # Reshape the input data to fit the LSTM model
    input_data = np.reshape(input_data, (1, look_back, 1))
    
    # Use the model to predict the next time step
    next_step = model.predict(input_data)[0][0]
    
    # Append the predicted value to the list of predicted values
    predicted_values.append(next_step)
    
    # Shift the input data by one time step and replace the last value with the predicted value
    input_data = np.roll(input_data, -1)
    input_data[-1] = next_step

# Convert the predicted values back to their original scale using the inverse_transform() function of the scaler
predicted_values = scaler.inverse_transform(np.array(predicted_values).reshape(-1, 1))

# Shift train predictions for plotting
trainPredictPlot = np.empty_like(dataset)
trainPredictPlot[:, :] = np.nan
trainPredictPlot[look_back:len(trainPredict)+look_back, :] = trainPredict
# shift test predictions for plotting
testPredictPlot = np.empty_like(dataset)
testPredictPlot[:, :] = np.nan
testPredictPlot[len(trainPredict)+(look_back*2)+1:len(dataset)-1, :] = testPredict

# Shift next 14 days predictions for plotting
next14PredictPlot = np.empty_like(dataset)
next14PredictPlot[:, :] = np.nan
next14PredictPlot = np.append(next14PredictPlot, predicted_values)

# plot baseline and predictions
plt.figure(figsize=(20, 15))
plt.plot(scaler.inverse_transform(dataset), label='Actual')
plt.plot(trainPredictPlot, label='Train Predictions')
plt.plot(testPredictPlot, label='Test Predictions')
plt.plot(next14PredictPlot, label='Prediction for next 14 days')
plt.legend()
plt.title('Lima District Positive Prediction')
plt.savefig('Lima_Positive.png')
plt.show()