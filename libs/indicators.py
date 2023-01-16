import numpy as np

def moving_averages(Data):
    ma = Data
    for i in range(len(Data.columns)):
        try:
            ma[str(ma.columns[i]) + '_MA10'] = ma[ma.columns[i]].rolling(10).mean()
            ma[str(ma.columns[i]) + '_MA50'] = ma[ma.columns[i]].rolling(50).mean()
            ma[str(ma.columns[i]) + '_MA200'] = ma[ma.columns[i]].rolling(200).mean()
        except ValueError:
            pass
    #this function returns a new dataset - it does not append to the source, 
    # so you must assign the function to a new dataset variable    
    return ma

# Exponential moving average (EMA) tells us the weighted mean of the previous K data points. 
# EMA places a greater weight and significance on the most recent data points. 
# In python we use the Exponentially Weighted set of functions (the mean, in our case)
def exp_moving_averages(Data):
    ema = Data
    for i in range(len(ema.columns)):
        try:
            ema[str(ema.columns[i]) + '_EMA10'] = ema[ema.columns[i]].ewm(span=10, adjust=False).mean()
            ema[str(ema.columns[i]) + '_EMA50'] = ema[ema.columns[i]].ewm(span=50, adjust=False).mean()
            ema[str(ema.columns[i]) + '_EMA200'] = ema[ema.columns[i]].ewm(span=200, adjust=False).mean()
        except ValueError:
            pass
    #this function returns a new dataset - it does not append to the source, 
    # so you must assign the function to a new dataset variable    
    return ema


def pct_change(Data):
  for i in range(len(Data.columns)):
    try:
      Data[str(Data.columns[i]) + '_pct_change'] = Data[Data.columns[i]].pct_change()
    except ValueError:
      pass
  return Data

def log_returns(Data):
  for i in range(len(Data.columns)):
    try:
      Data[str(Data.columns[i]) + '_log_returns'] = np.log(Data[Data.columns[i]]/Data[Data.columns[i]].shift())
    except ValueError:
      pass
  return Data