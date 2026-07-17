from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class RunningTotal(KeyedProcessFunction):
	def open(self, runtime_context):
		desc = ValueStateDescriptor("total",Types.INT())
		self.total_state = runtime_context.get_state(desc)

	def process_element(self, value, ctx):
		current = self.total_state.value()
		if current is None:
			current = 0

		current = current + value[1]
		self.total_state.update(current)
		return [(value[0], current)]




from pyflink.datastream.functions import KeyedvalueFunction
from pyflink.datastream.state import ValueStateDisciptor
from pyflink.common import Types

class RunningTotal(KeyedProcessFunction):
	def open(self, runtime_context):
		desc=ValueStateDescriptor("total",Types.INT())
		self.total=runtime_context.get_state(desc)

	def process_function(self, value, ctx):
		total=self.total.value()
		if total is None:
			total=0
		total=total+value[1]
		self.total.update(total)
		yield(value[0],total)







from pyflink.datastream.functions import KeyedValueFunction
from pyflink.datasteam.states import ValueStateDescriptor
from pyflink.common import Types

class RunningTotal(KeyedValueFunction):
	def open(self, runtime_context):
		desc=ValueStateDescriptor("total",Types.INT())
		self.total=runtime_context.get(desc)

	def process_element(self, value, ctx):
		total=self.total.value()
		if total is None:
			total=0

		total=total+value[1]
		self.total.update(total)
		yield(value[0],total)








from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class RunningTotal(KeyedProcessFunction):
	def open(self, running_context):
		desc=ValueStateDescriptor("total",Types.INT())
		self.total=running_context.get(desc)

	def process_element(self, ctx, value):
		total=self.total.value()
		if total is None:
			total=0
		total=total+value[1]
		self.total.update(total)
		yield(value[0],total)




