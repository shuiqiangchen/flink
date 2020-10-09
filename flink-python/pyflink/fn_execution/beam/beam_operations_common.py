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

from apache_beam.coders import PickleCoder

from pyflink.common.state import ListState, ValueState, StateDescriptor, T
from pyflink.datastream.functions import RuntimeContext
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend


class InternalRuntimeContext(RuntimeContext):
    def __init__(self, keyed_state_backend):
        self.keyed_state_backend: RemoteKeyedStateBackend = keyed_state_backend

    def get_list_state(self, descriptor: StateDescriptor) -> ListState:
        return InternalListState(descriptor, self.keyed_state_backend)

    def get_state(self, descriptor: StateDescriptor) -> ValueState:
        return InternalValueState(descriptor, self.keyed_state_backend)


class InternalListState(ListState):

    def __init__(self, descriptor: StateDescriptor, keyed_state_backend: RemoteKeyedStateBackend):
        self.state_descriptor = descriptor
        self.keyed_state_backend = keyed_state_backend
        self.value_coder = PickleCoder()
        self.list_state = self.keyed_state_backend.get_list_state(self.state_descriptor.name,
                                                                  self.value_coder)

    def add(self, v):
        if not self.list_state.is_valid:
            self._reload_state()
        self.list_state.add(v)

    def get(self):
        if not self.list_state.is_valid:
            self._reload_state()

        return self.list_state.get()

    def clear(self):
        self._reload_state()
        self.list_state.clear()

    def _reload_state(self):
        self.list_state = self.keyed_state_backend.get_list_state(self.state_descriptor.name,
                                                                  self.value_coder)


class InternalValueState(ValueState):

    def __init__(self, descriptor: StateDescriptor, keyed_state_backend: RemoteKeyedStateBackend):
        self.state_descriptor = descriptor
        self.keyed_state_backend = keyed_state_backend
        self.value_coder = PickleCoder()
        self.value_state = self.keyed_state_backend.get_value_state(self.state_descriptor.name,
                                                                    self.value_coder)

    def value(self) -> T:
        self._reload_state()
        return self.value_state.value()

    def update(self, value: T) -> None:
        self._reload_state()
        self.value_state.update(value)

    def clear(self) -> None:
        self._reload_state()
        self.value_state.clear()

    def _reload_state(self):
        self.value_state = self.keyed_state_backend.get_value_state(self.state_descriptor.name,
                                                                    self.value_coder)
