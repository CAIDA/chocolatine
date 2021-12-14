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
# "as is", without any accompanying services from The Regents. The Regents does
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
# HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
# OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.

import zmq, time, sys
import pandas as pd
from common.fetcher import ChocFetcher, ChocFetcherJobType
from common.arimabuilder import ChocArimaPool

class ChocModeller(object):

    def __init__(self, iodaapiurl, arimaworkers, arimahistory):

        self.fetcher = ChocFetcher(iodaapiurl)
        self.outstanding_arima = set()
        self.arimapool = ChocArimaPool(arimaworkers)
        self.arimahistory = arimahistory

        self.context = zmq.Context()
        self.jobsock = None
        self.resultsock = None


    def startInputReader(self, prodhost, prodport):
        # TODO exception handling
        self.jobsock = self.context.socket(zmq.PULL)
        self.jobsock.connect("tcp://%s:%d" % (prodhost, prodport))

        return True

    def startResultPublisher(self, pubhost, pubsock):
        # TODO exception handling
        self.resultsock = self.context.socket(zmq.PUB)
        self.resultsock.connect("tcp://%s:%s" % (pubhost, pubsock))

        return True

    def deriveBestModel(self, fetched, key):
        expectedpoints = (self.arimahistory / 60.0)
        if fetched.empty:
            return None

        zeroes = (fetched["signalValue"] == 0).sum()

        if fetched.shape[0] < 0.75 * expectedpoints:
            print("Series '%s' has too many missing values to derive a model" % (key))
            return None

        lastts = fetched.last_valid_index()
        firstts = fetched.first_valid_index()

        if lastts - firstts < pd.Timedelta(self.arimahistory, units="seconds") * 0.95:
            print("Fetched history for series '%s' does not cover full time period" % (key))
            return None

        if zeroes / fetched.shape[0] >= 0.5:
            print("Series '%s' is mostly zeroes -- suggesting zero-based model" % (key))

            # TODO insert zero model into db
            return 0

        # Try derive a good ARMA model for this
        self.arimapool.addJob(key, fetched['signalValue'])
        return 1


    def _fetchData(self, injob):
        fetched = None
        seriestype = injob['serieskey'].split('.')[0]

        if seriestype == "darknet":
            fetched = self.fetcher.fetchTelescopeData(injob['serieskey'],
                injob['timestamp'], self.arimahistory)

        if fetched is None:
            return None

        pdseries = pd.DataFrame.from_records(fetched, index="timestamp")
        return pdseries

    def run(self):
        if self.jobsock is None:
            print("Error: need to call startInputReader() before calling run()")
            return
        if self.resultsock is None:
            print("Error: need to call startResultPublisher() before calling run()")
            return

        self.arimapool.startWorkers()


        while True:
            try:
                injob = self.jobsock.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError as zmqe:
                if zmqe.errno == zmq.EAGAIN:
                    injob = None
                else:
                    print("ZMQError when reading job: %s" % (str(zmqe)))
                    return

            if injob is not None:
                print(injob)
                fetched = self._fetchData(injob)
                if fetched is not None:
                    self.deriveBestModel(fetched, injob['serieskey'])

            completedArima = self.arimapool.getCompleted()
            for c in completedArima:
                print("ARIMA model generated for", c['serieskey'])
                print("model is %s (mads=%s) -- time taken %.2f" % (c['arma'], c['mads'], c['modeltime']))
                try:
                    self.resultsock.send_json(c, zmq.NOBLOCK)
                except zmq.ZMQError as zmqe:
                    if zmqe.errno != zmq.EAGAIN:
                        print("ZMQError when writing completed ARIMA model: %s" % (str(zmqe)))
                        return

            time.sleep(1)

model = ChocModeller("https://api.ioda.caida.org/dev/signals/raw", 4,
        12 * 7 * 24 * 60 * 60)

model.startInputReader("localhost", 44332)
model.startResultPublisher("localhost", 44333)
try:
    model.run()
except KeyboardInterrupt:
    print("Exiting chocolatine modeller")
    pass

model.arimapool.haltWorkers()
