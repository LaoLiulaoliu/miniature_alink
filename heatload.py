import time
import joblib
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


fname = '/mnt/d/heatload.csv'
mname = '/mnt/d/b.m'

def train(fname):
    df = pd.read_csv(fname, header=None)
    df.columns = ['time', 'outdoor_temperature', 'instantaneous_heat']
    x = df['outdoor_temperature'].values.reshape(-1, 1)
    y = df['instantaneous_heat'].values
    lr = LinearRegression(fit_intercept=True, normalize=True)
    return lr.fit(x, y)


model = train(fname)
joblib.dump(model, mname)

start = time.time() * 1000
model = joblib.load(mname)
end = time.time() * 1000
print(f'load cost: {end - start}ms')

value = np.asarray([[6]])
start = time.time() * 1000
result = model.predict(value)
end = time.time() * 1000
print(f'predict cost: {end - start}ms')
print(result)
