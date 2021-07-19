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

import multiprocessing, queue, time, random, signal
from enum import Enum
from collections import namedtuple
import influxdb_client

class ChocFetcherJobType(Enum):
    CHOC_FETCH_DATA = 1
    CHOC_FETCH_HALT_WORKER = 2



class ChocFetcherJob(object):
    def __init__(self, jobtype, serieskey, timestamp, duration):
        self.jobtype = jobtype
        self.duration = duration
        self.serieskey = serieskey
        self.result = []
        self.zerovalues = 0
        self.assignedfetcher = -1
        self.timestamp = timestamp

        # TODO make configurable
        self.metrics = ['uniq_src_ip', 'uniq_src_asn', 'pkt_cnt']

def generateNetacqQuery(keyterms):

    if len(keyterms) == 1:
        return f'and (r._measurement == \"geo_continent\") and (r.continent_code == \"{keyterms[0]}\")'

    if len(keyterms) == 2:
        return f'and (r._measurement == \"geo_country\") and (r.continent_code == \"{keyterms[0]}\") and (r.country_code == \"{keyterms[1]}\")'

    if len(keyterms) == 3:
        return f'and (r._measurement == \"geo_region\") and (r.continent_code == \"{keyterms[0]}\") and (r.country_code == \"{keyterms[1]}\") and (r.region_code == \"{keyterms[2]}\")'

    if len(keyterms) == 4:
        return f'and (r._measurement == \"geo_county\") and (r.continent_code == \"{keyterms[0]}\") and (r.country_code == \"{keyterms[1]}\") and (r.region_code == \"{keyterms[2]}\") and (r.county_code == \"{keyterms[3]}\")'

    return ''
        


class ChocFetcher(object):
    def __init__(self, fetchid, influxurl, influxtoken, influxbucket):
        self.fetchid = fetchid
        self.inq = multiprocessing.Queue()
        self.outq = multiprocessing.Queue()
        self.oob = multiprocessing.Queue()
        self.process = None
        self.influxurl = influxurl
        self.influxtoken = influxtoken
        self.influxclient = None
        self.influxbucket = influxbucket

    def __del__(self):
        if self.influxclient is not None:
            self.influxclient.close()

    def setProcess(self, p):
        self.process = p

    def getProcess(self):
        return self.process

    def addJob(self, serieskey, timestamp, duration):
        job = ChocFetcherJob(ChocFetcherJobType.CHOC_FETCH_DATA, serieskey,
                timestamp, duration)
        self.inq.put(job)

    def addResult(self, resjob):
        self.outq.put(resjob)

    def halt(self):
        job = ChocFetcherJob(ChocFetcherJobType.CHOC_FETCH_HALT_WORKER, "", 0,
                0)
        self.oob.put(job)

    def getNextJob(self):
        try:
            job = self.oob.get(False)
            return job
        except queue.Empty:
            pass

        try:
            job = self.inq.get(False)
        except queue.Empty:
            return None
        return job

    def runQueryJob(self, qjob):
        if self.influxclient is None:
            self.influxclient = influxdb_client.InfluxDBClient( \
                    url=self.influxurl, token=self.influxtoken, org='-')

        query_api = self.influxclient.query_api()
        keysplit = qjob.serieskey.split('.')

        query = f'from(bucket: \"{self.influxbucket}\") |> range(start:-{qjob.duration}) |> filter(fn:(r) => (r.telescope == \"{keysplit[1]}\") and (r.filter == \"{keysplit[2]}\") '

        if keysplit[3] == 'geo' and keysplit[4] == 'netacuity':
            # XXX temporary
            if len(keysplit[5:]) > 2:
                return

            geoquery = generateNetacqQuery(keysplit[5:])
            if geoquery == '':
                print("cannot form influxdb query for netacuity series %s" % (qjob.serieskey))
                return

            query += geoquery
        elif keysplit[3] == 'overall':
            query += 'and (r._measurement == \"summary\")'
        elif keysplit[3] == 'traffic' and keysplit[4] == 'protocol':
            query += f'and (r._measurement == \"ip_protocol\") and (r.protocol == \"{keysplit[5]}\")'
        elif keysplit[3] == 'traffic' and keysplit[4] == 'port':
            # TODO
            return
        elif keysplit[3] == 'geo' and keysplit[4] == 'maxmind':
            # TODO
            return
        elif keysplit[3] == 'traffic' and keysplit[4] == 'icmp':
            # TODO
            return
        elif keysplit[3] == 'routing' and keysplit[4] == 'asn':
            # TODO
            return
        else:
            print("cannot form influxdb query for %s" % (qjob.serieskey))
            print(keysplit)
            assert(0)
            return

        query += ")"
        query += " |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")"
        tables = query_api.query(query)
        if len(tables) == 0:
            return

        for rec in tables[0].records:
            d = {"timestamp": rec.get_time()}

            for metric in qjob.metrics:
                if metric in rec.values:
                    d[metric] = rec.values[metric]

            qjob.result.append(d)
        self.addResult(qjob)



    def getResult(self):
        try:
            job = self.outq.get(False)
        except queue.Empty:
            return None

        return job

class ChocFetcherPool(object):
    def __init__(self, numworkers, influxurl, influxtoken, influxbucket):
        self.numworkers = numworkers
        self.workers = []
        self.nextassign = 0
        self.influxurl = influxurl
        self.influxtoken = influxtoken
        self.influxbucket = influxbucket


    def startWorkers(self):
        if self.numworkers <= 0:
            # TODO log failure
            return -1

        for i in range(0, self.numworkers):
            self.workers.append(ChocFetcher(i, self.influxurl,
                    self.influxtoken, self.influxbucket))
            p = multiprocessing.Process(target=runChocFetcher, daemon=True,
                    args=(self.workers[i],),
                    name="ChocolatineFetcher-%d" % (i))
            self.workers[i].setProcess(p)
            p.start()

        return self.numworkers

    def haltWorkers(self):
        for i in range(0, self.numworkers):
            self.workers[i].halt()
            self.workers[i].getProcess().join()

    def assignJob(self, serieskey, timestamp, duration='1850h'):
        self.workers[self.nextassign].addJob(serieskey, timestamp, duration)
        self.nextassign += 1
        if self.nextassign >= self.numworkers:
            self.nextassign = self.nextassign % self.numworkers


    def getCompletedJobs(self):
        done = []

        for i in range(0, self.numworkers):
            while True:
                res = self.workers[i].getResult()
                if res is None:
                    break
                done.append(res)

        return done

def runChocFetcher(fetcher):

    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while (1):
        todo = fetcher.getNextJob()
        if todo is None:
            time.sleep(0.1)
            continue

        if todo.jobtype == ChocFetcherJobType.CHOC_FETCH_HALT_WORKER:
            break

        #print("fetcher", fetcher.fetchid, "running job", todo.serieskey,
        #        "from", todo.timestamp)

        todo.assignedfetcher = fetcher.fetchid
        #time.sleep(random.randrange(5,10))
        fetcher.runQueryJob(todo)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
