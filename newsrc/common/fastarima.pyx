#!/usr/bin/python3
# cython: language_level=3

from statsmodels.tsa.arima.model import ARIMA
import statsmodels.tools.sm_exceptions
from numpy.linalg import LinAlgError

import sys
import logging
import pandas as pd
from sklearn.metrics import mean_squared_error
from math import sqrt
from timeit import default_timer as timer
import numpy as np

def get_current_value(value, history, ppw):
    res = value
    res -= history.iloc[-ppw]

    return res

def invert_diff(prediction, history, ppw):
    res = prediction
    res += history.iloc[-ppw]

    return res

def get_median_absolute_deviations(df, maxn, predictions):
    mads = []
    for n in range(maxn):
        a = predictions[n::maxn] - df[n::maxn]
        mads.append(a.abs().median() * 1.4826 * 3)
    return mads


def fast_walk_forward_validation(df, diff_df, order_p, order_q, test_start,
        mad, ppw):

    start = timer()
    name = df.name
    n = 60

    history = df[:test_start].copy().drop(test_start, errors='ignore')
    diff_history = diff_df[:test_start].copy().drop(test_start,
            errors='ignore')

    predictions = []
    diff_predictions = []
    diff_std = []

    try:
        index = 0
        size = len(df[test_start:])

        while index <= size + n:
            model = ARIMA(diff_history.values, order=(order_p, 0, order_q))
            iterstart = timer()
            try:
                fitted_model = model.fit()
            except statsmodels.tools.sm_exceptions.MissingDataError as err:
                sys.exit(-1)
            except(ValueError):
                logging.debug('Order {},{} for {} does not have stationary '
                              'parameters'.format(order_p, order_q, name))
                return None
            iterend = timer()

            forecast = fitted_model.get_forecast(steps=n, alpha=0.01)

            if index + n < len(df[test_start:]):
                repeats = n
            else:
                repeats = len(df[test_start:]) - index

            for i in range(repeats):
                prediction = forecast.predicted_mean[i]
                inverted_prediction = invert_diff(prediction, history, ppw)
                current_value = df[test_start:].iloc[index + i]
                current_time = df[test_start:].index[index + i]

                predictions.append(inverted_prediction)
                diff_predictions.append(prediction)
                diff_std.append(forecast.se_mean[i])

                if np.isnan(current_value):
                    history.at[current_time] = inverted_prediction
                    diff_history.at[current_time] = prediction
                else:
                    # validation mode
                    history.at[current_time] = current_value
                    diff_history.at[current_time] = get_current_value(
                            current_value, history, ppw)

                if len(history) > ppw:
                    history = history[-ppw:]
                    diff_history = diff_history[-ppw:]
            index += n
            print("index=%d, took %.3f seconds" % (index - n, iterend - iterstart))

        d = diff_df[test_start:].copy().to_frame()
        d['diff-predictions'] = diff_predictions
        d.dropna(inplace=True)

        error = sqrt(mean_squared_error(d[d.columns[0]].values,
                d[d.columns[1]].values))

        mad = get_median_absolute_deviations(df[test_start:], n, predictions)
        print(order_p, order_q, error, mad)
    except(LinAlgError):
        logging.error('{} error: model did not converge.'.format(df.name))
        return None

    end = timer()
    logging.debug('Computing ARMA{},{} for {} took {:.2f} seconds.'.format(
            order_p, order_q, name, end-start))

    return 1
