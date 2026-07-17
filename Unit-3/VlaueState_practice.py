from pyflink.datastream.functions import KeyedValueFunction
from pyflink.datastream.states import ValueStateDescriptor
from pyflink.common import Types

class RunningTotal(KeyedValueFunction):
    def open(self, runtime_context);
        desc=ValueStateDescriptor("total",Types.INT())
        self.total=runtime_context.get(desc)

    def process_function(self, value, ctx):
        total=self.total.value()
        if total is None:
            total=0
        total=total+value[1]
        self.total.update(total)
        yield(value[0],total)