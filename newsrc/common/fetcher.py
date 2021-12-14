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

from enum import Enum
import pandas as pd
import requests
from requests.exceptions import HTTPError

# 12 weeks -- 10 weeks to train, 1 week to calibrate, 1 week to test
DEFAULT_DURATION=(12 * 7) * 24 * 60 * 60

DEFAULT_API_URL="https://api.ioda.caida.org/dev/signals/raw"

class ChocFetcherJobType(Enum):
    CHOC_FETCH_TELESCOPE_DATA = 1
    CHOC_FETCH_BGP_DATA = 2
    CHOC_FETCH_TRINOCULAR_DATA = 3

class ChocFetcher(object):
    def __init__(self, apiurl = DEFAULT_API_URL):
        self.iodaapiurl = apiurl

    def _formTelescopeQuery(self, serieskey, endtime, duration):

        keysplit = serieskey.split('.')

        if len(keysplit) < 4:
            return None

        dataSource = keysplit[1]
        if keysplit[3] == "geo":
            if len(keysplit) == 6:
                entityType = "continent"
            elif len(keysplit) == 7:
                entityType = "country"
            elif len(keysplit) == 8:
                entityType = "region"
            elif len(keysplit) == 7:
                entityType = "country"
            entityCode = keysplit[-1]
        elif keysplit[3] == "routing":
            entityType = "asn"
            entityCode = keysplit[-1]
        else:
            return None


        queryArgs = "/%s/%s?from=%u&until=%u&datasource=%s&maxPoints=%u" % ( \
                entityType, entityCode, endtime - duration, endtime,
                dataSource, (duration / 60) + 1)
        return queryArgs

    def _fetchIodaData(self, serieskey, queryArgs):
        try:
            resp = requests.get(self.iodaapiurl + queryArgs)
            resp.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            print(f'Non-HTTP error occurred {err}')
            return None

        jsonresult = resp.json()['data'][0][0]
        t = jsonresult['from']
        step = jsonresult['step']
        native = jsonresult['nativeStep']
        if step != native:
            print(f'Step value ({step}) for series {serieskey} does not match nativeStep ({native})')
            return None

        res = []
        for v in jsonresult['values']:
            res.append({"timestamp": pd.Timestamp(t, unit='s'),
                        "signalValue": v})
            t += step

        return res

    def fetchTelescopeData(self, serieskey, endtime, duration):
        print(serieskey)
        queryArgs = self._formTelescopeQuery(serieskey, endtime, duration)
        if queryArgs is None:
            return None
        return self._fetchIodaData(serieskey, queryArgs)


