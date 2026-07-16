from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common import Types

class PurchaseHistory(KeyedProcessFunction):
	"""A simple KeyedProcessFunction that keeps a list of purchases per key.

	Note: This class is intended to be used inside a PyFlink job; the methods
	mirror the PyFlink `KeyedProcessFunction` lifecycle (`open`,
	`process_element`).
	"""

	def open(self, runtime_context):
		desc = ListStateDescriptor("purchase_history", Types.INT())
		self.history_state = runtime_context.get_list_state(desc)

	def process_element(self, value, ctx):
		# value is expected to be a tuple like (key, purchase_id)
		self.history_state.add(value[1])
		history = list(self.history_state.get())
		# Return a tuple for downstream use (non-standard for Flink APIs,
		# but useful for unit-testing this function outside of a job)
		return [(value[0], history)]