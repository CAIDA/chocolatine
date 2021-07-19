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

from kafka import KafkaConsumer
import sys, string, json, struct
from pytimeseries.tsk.proxy import TskReader
import confluent_kafka
from fetcher import ChocFetcherPool, ChocFetcher
from series import ChocTimeSeriesFamily

TSKBATCH_VERSION = 0


class ChocConsumer(TskReader):
    def __init__(self, topic_prefix, channel, consumer_group, brokers,
            heartbeat_ms=5000, partition=None, commit_offsets=True):

        if sys.version_info[0] == 2:
            self.channel = channel
        else:
            self.channel = bytes(channel, 'ascii')

        self.topic_name = ".".join([topic_prefix, channel])
        self.consumer_group = ".".join([consumer_group, self.topic_name])
        self.partition = partition
        self.lasttimestamp = 0
        self.timeseries = {}

        conf = {
            'bootstrap.servers': brokers,
            'group.id': self.consumer_group,
            'default.topic.config': {'auto.offset.reset': 'latest'},
            'heartbeat.interval.ms': heartbeat_ms,
            'api.version.request': True,
            'enable.auto.commit': commit_offsets
        }

        self.kc = confluent_kafka.Consumer(conf)
        if self.partition:
            topic_list = [confluent_kafka.TopicPartition(self.topic_name,
                    self.partition)]
            self.kc.assign(topic_list)
        else:
            self.kc.subscribe([self.topic_name])

    def handle_msg(self, msg, fetchers):
        msg_partition = msg.partition()
        msgbuf = msg.value()

        try:
            msg_time, version, channel, offset = self._parse_header(msgbuf)
        except struct.error:
            raise RuntimeError("malformed Kafka message")

        msgbuflen = len(msgbuf)
        if version != TSKBATCH_VERSION:
            raise RuntimeError("Kafka message with version %d "
                    "(expected %d)" % (version, TSKBATCH_VERSION))
        if channel != self.channel:
            raise RuntimeError("Kafka message with channel %s "
                    "(expected %s)" % (channel, self.channel))

        self.process_msg_header(msg_time, version, channel, msg_partition,
                msgbuf, msgbuflen)

        while offset < msgbuflen:
            try:
                key, val, offset = self._parse_kv(msgbuf, offset)
            except struct.error:
                raise RuntimeError("Could not parse Kafka key/value")

            self.process_keyvalue(key, val, msg_partition, fetchers)

    def process_msg_header(self, msgtime, version, channel, partition, buf,
            buflen):

        if msgtime != self.lasttimestamp:
            self.lasttimestamp = msgtime

    def process_keyvalue(self, key, val, partition, fetchers):
        keystr = key.decode('utf-8')
        keyfam = ".".join(keystr.split('.')[:-1])

        if keyfam not in self.timeseries:
            fetchers.assignJob(keyfam, self.lasttimestamp, "1850h")
            self.timeseries[keyfam] = ChocTimeSeriesFamily(keyfam)

        self.timeseries[keyfam].addLiveData(val, self.lasttimestamp,
                keystr[keystr.rindex('.')+1:])

    def process_fetched_data(self, jobdone):

        # possible scenarios
        # 1. we have enough good data -- pass off to model generator
        #   a) we generate a viable model, yay
        #   b) data is too erratic for a good model?
        # 2. we have some good data but not enough
        # 3. most of our data is zeroes

        if jobdone.serieskey not in self.timeseries:
            # should never happen?
            return

        series = self.timeseries[jobdone.serieskey]
        series.addFetchedData(jobdone)

# TODO add config options for all of these parameters...
if __name__ == '__main__':
    reader = ChocConsumer('reporttest', 'channelname', 'XXXX',
            'KAFKAHOST:9092')

    try:
        fetchers = ChocFetcherPool(5, "http://INFLUXDBSERVER:8086", "USERNAME:PASSWORD", "stardust_ucsdnt/autogen")

        if fetchers.startWorkers() < 0:
            sys.exit(-1)

        shutdown = False

        while True:
            if shutdown:
                break

            completed = fetchers.getCompletedJobs()

            for comp in completed:
                reader.process_fetched_data(comp)

            msg = reader.poll(1)
            eof_since_data = 0
            msg_cnt = 0
            while msg is not None:
                if not msg.error():
                    try:
                        reader.handle_msg(msg, fetchers)
                    except RuntimeError as e:
                        print(e)
                    eof_since_data = 0
                elif msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    eof_since_data += 1
                    if eof_since_data >= 10:
                        break
                else:
                    # unhandled error
                    shutdown = True
                    print(msg.error)

                if shutdown or msg_cnt >= 50:
                    break
                msg_cnt += 1
                msg = reader.poll(1)
    except KeyboardInterrupt:
        print("halting Chocolatine...")
        shutdown = True
    except Exception as e:
        raise(e)
    finally:
        fetchers.haltWorkers()



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
