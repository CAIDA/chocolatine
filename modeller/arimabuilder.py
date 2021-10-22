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


import multiprocessing, queue, time, signal
import pandas as pd
from threadpoolctl import threadpool_limits

import pyximport
pyximport.install()
import arima

class ChocArimaTrainer(object):
    def __init__(self, trainerid):
        self.trainerid = trainerid
        self.inq = multiprocessing.Queue()
        self.outq = multiprocessing.Queue()
        self.oob = multiprocessing.Queue()
        self.process = None

    def setProcess(self, p):
        self.process = p

    def getProcess(self):
        return self.process

    def addJob(self, key, data):
        self.inq.put((key, data))

    def addResult(self, model):
        self.outq.put(model)

    def halt(self):
        self.oob.put(None)

    def getResult(self):
        try:
            res = self.outq.get(False)
        except queue.Empty:
            return None
        return res

    def getNextJob(self):
        try:
            job = self.oob.get(False)
            return (None, None)
        except queue.Empty:
            pass

        try:
            job = self.inq.get(False)
        except queue.Empty:
            return None
        return job

class ChocArimaPool(object):
    def __init__(self, workers):
        self.numworkers = workers
        self.workers = []
        self.nextassign = 0

    def startWorkers(self):
        if self.numworkers <= 0:
            return -1

        for i in range(0, self.numworkers):
            self.workers.append(ChocArimaTrainer(i))
            p = multiprocessing.Process(target=runArimaTrainer, daemon=True,
                    args=(self.workers[i],),
                    name="ChocolatineArimaTrainer-%d" % (i))
            self.workers[i].setProcess(p)
            p.start()

        return self.numworkers

    def haltWorkers(self):
        for i in range(0, self.numworkers):
            self.workers[i].halt()
            self.workers[i].getProcess().join()


    def getCompleted(self):
        done = []

        for i in range(0, self.numworkers):
            while True:
                res = self.workers[i].getResult()
                if res is None:
                    break
                done.append(res)

        return done

    def addJob(self, key, data):
        self.workers[self.nextassign].addJob(key, data)
        self.nextassign += 1

        if self.nextassign >= self.numworkers:
            self.nextassign = self.nextassign % self.numworkers

def runArimaTrainer(trainer):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while True:
        job = trainer.getNextJob()
        if job is None:
            time.sleep(1)
            continue
        if job[0] is None and job[1] is None:
            break

        history = job[1]
        print("Generating ARIMA model for '%s'" % (job[0]))
        model = arima.Arima(
                [history.last_valid_index() - pd.DateOffset(weeks=1),
                        history.last_valid_index()],
                12,
                [60 * 24 * 7],
                3,
                False,
                False,
                0,
                50)
        start = time.time()
        with threadpool_limits(limits=8, user_api='blas'):
            restuple = model.prepare_analysis(history, 24 * 7 * 60, None, False)
        end = time.time()
        res = {'serieskey': job[0], 'arma': restuple[2], 'mads': restuple[3],
               'modeltime': end - start}
        print(res)
        trainer.addResult(res)

