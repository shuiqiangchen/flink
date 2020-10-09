################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import time

from pyflink.datastream.functions import Collector, TimerService


class InternalCollector(Collector):

    def __init__(self):
        self.buf = []

    def collect_proc_timer(self, a, key):
        self.buf.append((0, a, key, None))

    def collect_event_timer(self, a, key):
        self.buf.append((1, a, key, None))

    def collect_data(self, a):
        self.buf.append((2, a))

    def collect(self, a):
        self.collect_data(a)

    def clear(self):
        self.buf.clear()


class InternalTimerService(TimerService):
    def __init__(self, collector, ctx):
        self._collector: InternalCollector = collector
        self._ctx = ctx

    def current_processing_time(self):
        return int(time.time() * 1000)

    def register_processing_time_timer(self, t):
        current_key = self._ctx.keyed_state_backend.get_current_key()
        self._collector.collect_proc_timer(t, current_key)

    def register_event_time_timer(self, t):
        current_key = self._ctx.keyed_state_backend.get_current_key()
        self._collector.collect_event_timer(t, current_key)
