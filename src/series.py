# This file is part of chocolatine
#
# Copyright (C) 2021 The Regents of the University of California
# All Rights Reserved
#
# Permission to copy, modify, and distribute this software and its
# documentation for academic research and education purposes, without fee, and
# without a written agreement is hereby granted, provided that
# the above copyright notice, this paragraph and the following paragraphs
# appear in all copies.
#
# Permission to make use of this software for other than academic research and
# education purposes may be obtained by contacting:
#
# Office of Innovation and Commercialization
# 9500 Gilman Drive, Mail Code 0910
# University of California
# La Jolla, CA 92093-0910
# (858) 534-5815
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of the
# University of California. The software program and documentation are supplied
# “as is”, without any accompanying services from The Regents. The Regents does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN “AS IS” BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
# OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.

import multiprocessing, queue, time, signal, datetime
from fetcher import ChocFetcherJob
import pandas as pd
from dateutil.tz import tzutc

#import pyximport
#pyximport.install()

# base prediction model class -- will always predict 0.0 as the next value
class PredictModel(object):
    def __init__(self, key):
        self.expectednexttimestamp = 0
        self.key = key

    def reconstructModel(self, loaded):
        return

    def saveModel(self, dbsess):
        return

    def buildModel(self, history):
        print("Building ZERO model for", self.key)
        return

    def predictNext(self, timestamp):
        return 0.0

    def updateModel(self, latest, timestamp, metric):
        return

class SARIMAModel(PredictModel):
    def __init__(self, key):
        # TODO init parameters here
        self.expectednexttimestamp = 0
        self.key = key

    def reconstructModel(self, loaded):
        return

    def saveModel(self, dbsess):
        return

    def buildModel(self, history):

        print("Building model for", self.key)
        #trainer = arima.Arima(
        #        [history.first_valid_index(), history.last_valid_index()],
        #        12,
        #        [60 * 24 * 7],
        #        3,
        #        False,
        #        False,
        #        0,
        #        50)

        #print(trainer.prepare_analysis(history, None, False))

        return

    def predictNext(self, timestamp):
        return 1.0

    def updateModel(self, latest, timestamp, metric):
        return

class ChocTimeSeriesFamily(object):
    def __init__(self, key, trainingdays = 70, calibdays = 7):
        self.serieskey = key
        self.values = pd.DataFrame()
        self.trainingdays = trainingdays
        self.calibdays = calibdays

        self.savedlive = {}

        self.models = {}

    def addLiveData(self, dataval, timestamp, metric):
        if metric not in self.models or self.models[metric] is None:

            if timestamp not in self.savedlive:
                self.savedlive[timestamp] = {'timestamp': timestamp}
            self.savedlive[timestamp][metric] = dataval

            # if we somehow end up with enough live data to generate a model,
            # then just go ahead and use that instead
            # TODO

        else:

            # update our existing model
            self.model.updateModel(dataval, timestamp, metric)

    def mergeLiveData(self):
        if self.savedlive != {}:
            liveseries = pd.DataFrame.from_records(
                    list(self.savedlive.values()), index='timestamp')
            liveseries = liveseries.sort_index(0)
            self.values.combine_first(liveseries)

            self.savedlive = {}


    def addFetchedData(self, fetched):
        if self.serieskey != fetched.serieskey:
            # TODO log error
            return

        if fetched.serieskey in self.models:
            # we already have a model, update if we can

            # does the fetched data already cover a time period that is
            # part of our model -- it shouldn't but you never know...

            # should be simply incrementally update our existing model
            # (i.e. fetched data is relatively small and the start is close
            # to where our existing model left off)? Or is there enough fetched
            # data that we would be better off to rebuild from scratch?

            return

        if len(fetched.result) == 0:
            print("Empty result for %s?" % (fetched.serieskey))
            return

        print(self.serieskey, len(fetched.result))

        pdseries = pd.DataFrame.from_records(fetched.result, index="timestamp")
        if self.values.empty:
            self.values = pdseries
        else:
            self.values.combine_first(pdseries)

        if self.values.empty:
            return

        self.mergeLiveData()

        lastts = self.values.last_valid_index()
        firstts = lastts - pd.Timedelta(self.trainingdays + self.calibdays,
                unit='d')

        t1 = self.values.index.searchsorted(firstts)
        self.values = self.values.iloc[t1:]

        # do we have enough data to try generating a SARIMA model?
        self.generateSARIMAModels()

    def generateSARIMAModels(self):
        if self.values.empty:
            return

        print("Generating SARIMA for", self.serieskey)

        for c in list(self.values):
            zeroes = (self.values[c] == 0).sum()

            if self.values.shape[0] < 0.75 * (self.trainingdays + self.calibdays) * 24 * 60:
                # too many missing values
                print(c, self.values.shape[0], 0.75 * (self.trainingdays + self.calibdays) * 24 * 60, self.values.last_valid_index(),  self.values.first_valid_index())
                print("too many missing values")
                continue

            lastts = self.values.last_valid_index()
            firstts = self.values.first_valid_index()

            if lastts - firstts < pd.Timedelta(
                        self.trainingdays + self.calibdays,
                        unit='d') * 0.95:
                # need close to the full training + calibration period
                print("history does not cover full time period")
                continue

            if zeroes / self.values.shape[0] >= 0.5:
                self.models[c] = PredictModel(self.serieskey + "." + c)
                # TODO how should we handle cases where a zero-based model
                # changes to be not zero-based... ?

            else:
                self.models[c] = SARIMAModel(self.serieskey + "." + c)


            self.models[c].buildModel(self.values[c])

        print("Done generating SARIMA for", self.serieskey)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
