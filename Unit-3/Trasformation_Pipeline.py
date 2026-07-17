from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

ds = env.from_collection([5, 10, 15, 20, 25])

filtered = ds.filter(lambda x: x > 10)

result = filtered.map(lambda x: x * 2)

result.print()
env.execute("Basic Transformation Pipeline")