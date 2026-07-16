#!/usr/bin/env python3
import runpy, sys, types

# Create a lightweight fake pyflink package so ValueState.py can be imported without installing pyflink
pyflink = types.ModuleType("pyflink")

# datastream subpackage
datastream = types.ModuleType("pyflink.datastream")
datastream.functions = types.ModuleType("pyflink.datastream.functions")
datastream.state = types.ModuleType("pyflink.datastream.state")

# common.typeinfo
common = types.ModuleType("pyflink.common")
common.typeinfo = types.ModuleType("pyflink.common.typeinfo")

# Minimal base class used by the user's file
class KeyedProcessFunction:
    def open(self, runtime_context):
        pass
    def process_element(self, value, ctx):
        pass

# Minimal ValueStateDescriptor used to match the constructor signature
class ValueStateDescriptor:
    def __init__(self, name, typeinfo):
        self.name = name
        self.typeinfo = typeinfo

# Minimal Types helper
class Types:
    @staticmethod
    def LONG():
        return int

# Attach into fake modules
datastream.functions.KeyedProcessFunction = KeyedProcessFunction
datastream.state.ValueStateDescriptor = ValueStateDescriptor
common.typeinfo.Types = Types

# Register fake modules in sys.modules so import in ValueState.py succeeds
sys.modules['pyflink'] = pyflink
sys.modules['pyflink.datastream'] = datastream
sys.modules['pyflink.datastream.functions'] = datastream.functions
sys.modules['pyflink.datastream.state'] = datastream.state
sys.modules['pyflink.common'] = common
sys.modules['pyflink.common.typeinfo'] = common.typeinfo

# Execute the user's file and grab RunningTotal
ns = runpy.run_path('ValueState.py')
RunningTotal = ns['RunningTotal']

# Dummy state implementation to mimic Flink ValueState
class DummyState:
    def __init__(self):
        self._v = None
    def value(self):
        return self._v
    def update(self, v):
        self._v = v

# Dummy runtime context to return the DummyState
class DummyRuntimeContext:
    def __init__(self):
        self._state = DummyState()
    def get_state(self, descriptor):
        return self._state

# Instantiate and simulate processing
rt = RunningTotal()
rt.open(DummyRuntimeContext())

inputs = [1, 2, 5, 10]
for v in inputs:
    out = list(rt.process_element(v, None))
    print('input', v, '->', out)
