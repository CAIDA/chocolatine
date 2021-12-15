import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller  # Check stationarity
from statsmodels.tsa.arima.model import ARIMA
import statsmodels.tools.sm_exceptions

# ARIMA error
from numpy.linalg import LinAlgError

# Error estimation
from sklearn.metrics import mean_squared_error
from math import sqrt

import sys
from math import ceil, floor

class ArimaPredictor(object):
    def __init__(self, armaparams, datafreq):
        self.datafreq = datafreq
        self.arma = armaparams
        self.ppw = (7 * 24 * 60 * 60) / self.datafreq

    def sanitize_training(self, df, startts, endts):

        a = pd.concat(
            [df.shift(x).rename("-{}w".format(x//self.ppw)) for x in [self.ppw * i for i in range(0, 10, 2)]], axis=1)

        a = a[startts:endts].median(axis=1).interpolate()
        return a

    def prepare_histories(self, hist, startts, predtime):

        padstartts = startts - pd.DateOffset(weeks=1)
        endts = predtime

        print(hist, padstartts, endts)
        hist[padstartts:endts] = self.sanitize_training(
                hist[:endts], padstartts, endts)

        print(hist)
        return None, None
