from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class RunningSpend(KeyedProcessFunction):
	def open(self, runtime_context):
		desc = ValueStateDescriptor("total_spend",Types.INT())
		self.total_state = runtime_context.get_state(desc)

	def process_element(self, value, ctx):
		current = self.total_state.value()
		if current is None:
			current = 0

		current = current + value[1]
		self.total_state.update(current)
		return [(value[0], current)]
