from kafka import KafkaConsumer
import pandas as pd
from common.fetcher import ChocFetcher
import multiprocessing, queue, signal, time
import zmq

def parseTskBatch(content):
    if content[0:9] != b'TSKBATCH\x00':
        return None

    content = content[9:]
    timestamp = int.from_bytes(content[0:4], "big")

    content = content[4:]
    chanlen = int.from_bytes(content[0:2], "big")

    channel = content[2:2+chanlen].decode("utf-8")

    content = content[chanlen + 2:]
    keylen = int.from_bytes(content[0:2], "big")
    keystr = content[2:2+keylen].decode("utf-8")

    content = content[keylen+2:]
    value = int.from_bytes(content[0:8], "big")

    return timestamp, keystr, value, channel

def getDefaultModel(key):
    if key[0] == "darknet" and key[3] == "geo" and len(key) == 7:
        # geo-country
        return (1, 1)

    # TODO default arma models for other series types

    # No idea what this is, just go with a (1,1) for now
    return (1, 1)

class RealTimeSeries(object):

    def __init__(self, keystr, workerid):
        self.workerid = workerid
        self.keystr = keystr
        self.history = {}
        self.hist_timestamps = []

        self.arma = None
        self.arma_source=""
        self.last_arma_check = 0
        self.arma_requested = False

        self.fetchedhistory = False

        if keystr.split('.')[0] == "darknet":
            self.datafreq = 60
        else:
            self.datafreq = 300

        self.predictstart = 0
        self.predictions = []

        self.zmq_out = None

    def addQueue(self, zmqout):
        self.zmq_out = zmqout

    def getWorkerId(self):
        return self.workerid

    def checkArmaModel(self, timestamp):
        # TODO if we have a model already, consider whether we should
        # request a re-build (e.g. our cumulative prediction error is too high)?

        if self.arma_source == "database":
            return

        # query model DB for a known best model for this series
        now = time.time()
        if now - self.last_arma_check >= 15 * 60 * 60:
            self.last_arma_check = int(now)

            # TODO query the database for our model
            print("%u Checking database for ARMA model (%s)" % \
                    (int(now), self.keystr))

        if self.arma_source == "database":
            return

        # if no model found, request one be built
        if self.arma_requested == False:
            self.requestModel(self.keystr, timestamp)
            self.arma_requested = True

        # then pick the default model for this series type
        self.arma = getDefaultModel(self.keystr.split('.'))
        self.arma_source = "default"


    def fetchHistory(self, fetcher, firstlive):
        fetched = None
        seriestype = self.keystr.split('.')[0]

        if seriestype == "darknet":
            fetched = fetcher.fetchTelescopeData(self.keystr,
                firstlive, 4 * 7 * 24 * 60 * 60)

        if fetched is None:
            return None

        for f in fetched:
            dts = f['timestamp'].timestamp()
            self.history[dts] = f['signalValue']
            self.hist_timestamps.append(dts)

        self.hist_timestamps.sort()
        self.fetchedhistory = True
        return True

    def requestModel(self, serieskey, timestamp):
        self.zmq_out.send_json({"serieskey": serieskey, "ts": timestamp})


class ChocRTWorker(object):
    def __init__(self, workerid, iodaapi, zmqctxt, zmqinternal):
        self.fetcher = ChocFetcher(iodaapi)
        self.knownSeries = {}
        self.workerid = workerid
        self.zmq = zmqctxt
        self.zmqinternal = zmqinternal

        self.inq = multiprocessing.Queue()
        self.oob = multiprocessing.Queue()

        self.zmq_out = zmqctxt.socket(zmq.PUSH)
        self.zmq_out.connect(zmqinternal)

    def addNewSeries(self, s, timestamp):
        self.knownSeries[s.keystr] = s

        s.addQueue(self.zmq_out)
        s.checkArmaModel(timestamp)
        s.fetchHistory(self.fetcher, timestamp)
        print(s.workerid, s.keystr, s.hist_timestamps[0], s.hist_timestamps[-1])

        # TODO make predictions up until the end of the hour

    def addLiveData(self, serieskey, timestamp, value):

        if serieskey not in self.knownSeries:
            s = RealTimeSeries(serieskey, self.workerid)
            self.addNewSeries(s, timestamp)
        else:
            s = self.knownSeries[serieskey]
            s.checkArmaModel(timestamp)

        print(s.workerid, s.keystr, timestamp, value)

        # TODO compare value against predicted value

        # TODO add value to history (if not outlier), otherwise use prediction

        # TODO if we've filled the last hour (or we're 20+ minutes into an
        # unpredicted hour), generate some predictions

            # TODO repeat compare+add for any stashed values for the new hour

    def queueLiveData(self, serieskey, timestamp, value):
        self.inq.put((0, serieskey, timestamp, value))

    def queueNewSeries(self, s, timestamp):
        self.inq.put((1, s, timestamp))

    def halt(self):
        self.oob.put(None)

    def runNextJob(self):
        try:
            job = self.oob.get(False)
            return -1
        except queue.Empty:
            pass

        try:
            job = self.inq.get(False)
        except queue.Empty:
            return 1

        if job[0] == 0:
            self.addLiveData(job[1], job[2], job[3])
        elif job[0] == 1:
            self.addNewSeries(job[1], job[2])

        return 0

class ChocRTPool(object):
    def __init__(self, workers, iodaapi, zmqpushaddr, zmqinternal):
        self.numworkers = workers
        self.workers = []
        self.processes = []
        self.nextassign = 0
        self.knownSeries = {}
        self.iodaapi = iodaapi

        self.zmq = zmq.Context()
        self.zmqsock = self.zmq.socket(zmq.PUSH)
        self.zmqsock.bind(zmqpushaddr)
        self.zmq_internal = zmqinternal

        self.zmq_in = self.zmq.socket(zmq.PULL)
        self.zmq_in.bind(zmqinternal)

    def startWorkers(self):
        if self.numworkers <= 0:
            return -1

        for i in range(0, self.numworkers):
            self.workers.append(ChocRTWorker(i, self.iodaapi, self.zmq,
                    self.zmq_internal))
            p = multiprocessing.Process(target=runRTWorker, daemon=True,
                    args=(self.workers[i],),
                    name="ChocolatineRTWorker-%s" % (i))
            self.processes.append(p)

            p.start()

        return self.numworkers

    def haltWorkers(self):
        for i in range(0, self.numworkers):
            self.workers[i].halt()
            self.processes[i].join()

    def updateSeriesLive(self, serieskey, value, timestamp):
        if serieskey not in self.knownSeries:
            self.addSeriesToWorker(serieskey, timestamp)

        wkid = self.knownSeries[serieskey].getWorkerId()
        self.workers[wkid].queueLiveData(serieskey, timestamp, value)

    def addSeriesToWorker(self, serieskey, timestamp):
        if serieskey in self.knownSeries:
            return

        s = RealTimeSeries(serieskey, self.nextassign)
        self.workers[self.nextassign].queueNewSeries(s, timestamp)
        self.knownSeries[serieskey] = s

        self.nextassign += 1
        if self.nextassign >= self.numworkers:
            self.nextassign = self.nextassign % self.numworkers

    def pollModelRequests(self):

        while True:
            try:
                req = self.zmq_in.recv_json(flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                break

            print("Requesting model build for %s" % (req))
            self.zmqsock.send_json(req)


def runRTWorker(worker):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while True:
        x = worker.runNextJob()
        if x < 0:
            break
        elif x > 0:
            time.sleep(x)


if __name__ == "__main__":

    MAXWORKERS = 8
    IODAAPI="https://api.ioda.caida.org/dev/signals/raw"
    ZMQADDR="tcp://127.0.0.1:44332"
    ZMQINTERNAL="tcp://127.0.0.1:19999"

    workers = ChocRTPool(MAXWORKERS, IODAAPI, ZMQADDR, ZMQINTERNAL)

    kc = KafkaConsumer("choc_live_testing.foobar",
            bootstrap_servers="sicily.cc.gatech.edu:9092",
            group_id = "chocolatine",
            client_id= "ababab")

    workers.startWorkers()

    for msg in kc:
        try:
            ts, keystr, value, channel = parseTskBatch(msg.value)

            if keystr not in workers.knownSeries:
                workers.addSeriesToWorker(keystr, ts)

            print(ts, keystr, value)
            workers.updateSeriesLive(keystr, value, ts)
            kc.commit()
        except KeyboardInterrupt:
            break

        workers.pollModelRequests()

    workers.haltWorkers()
