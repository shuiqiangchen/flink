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
# cython: language_level=3

from apache_beam.coders.coder_impl cimport StreamCoderImpl
from apache_beam.runners.worker.operations cimport Operation

from pyflink.fn_execution.coder_impl_fast cimport BaseCoderImpl

cdef class BeamStatelessFunctionOperation(Operation):
    cdef Operation consumer
    cdef StreamCoderImpl _value_coder_impl
    cdef BaseCoderImpl _output_coder
    cdef list user_defined_funcs
    cdef object func
    cdef bint _is_python_coder
    cdef bint _metric_enabled
    cdef readonly object base_metric_group

    cdef void _update_gauge(self, base_metric_group)

cdef class BeamScalarFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class BeamTableFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class DataStreamStatelessFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class BeamStatefulFunctionOperation(BeamStatelessFunctionOperation):
    cdef object keyed_state_backend

cdef class BeamStreamGroupAggregateOperation(BeamStatefulFunctionOperation):
    cdef object generate_update_before
    cdef object grouping
    cdef object group_agg_function
    cdef object index_of_count_star
    cdef object state_cache_size
    cdef object state_cleaning_enabled

cdef class BeamDataStreamStatefulFunctionOperation(BeamStatefulFunctionOperation):
    cdef object _collector
    cdef object runtime_context
    cdef object function_context

cdef class PandasAggregateFunctionOperation(BeamStatelessFunctionOperation):
    pass

cdef class PandasBatchOverWindowAggregateFunctionOperation(BeamStatelessFunctionOperation):
    cdef list windows
    cdef list bounded_range_window_index
    cdef list is_bounded_range_window
    cdef list window_indexes
    cdef list mapper
