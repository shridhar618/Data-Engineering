from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

lines = env.from_collection(
	collection=[
		"flink stream processing",
		"pyflink data stream",
	],
	type_info=Types.STRING(),
)

words = lines.flat_map(
	lambda x: x.split(" "),
	output_type=Types.STRING(),
)

words.print()
env.execute("Word Splitting using flat_map")