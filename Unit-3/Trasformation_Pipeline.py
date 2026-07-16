from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

ds = env.from_collection(
	collection=[5, 10, 15, 20, 25],
	type_info=Types.INT()
)

filtered = ds.filter(lambda x: x > 10)

result = filtered.map(
	lambda x: x * 2,
	output_type=Types.INT()
)

result.print()
env.execute("Basic Transformation Pipeline")