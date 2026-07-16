from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class RunningTotal(KeyedProcessFunction):

    def open(self, runtime_context):
        # Create a ValueStateDescriptor to hold the running total
        state_descriptor = ValueStateDescriptor(
            "running_total",  # State name
            Types.LONG()      # State type
        )
        self.running_total_state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        # Get the current running total from the state
        current_total = self.running_total_state.value()
        if current_total is None:
            current_total = 0

        # Update the running total with the new value
        new_total = current_total + value
        self.running_total_state.update(new_total)

        # Emit the updated running total
        yield new_total


