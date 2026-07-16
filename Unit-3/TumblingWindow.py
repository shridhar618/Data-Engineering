from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.common import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows


class TxnTimestampAssigner(TimestampAssigner):
	def extract_timestamp(self, value, record_timestamp):
		return int(value[2])


def create_stream_environment():
	env = StreamExecutionEnvironment.get_execution_environment()
	env.set_parallelism(1)
	return env


def create_source(env):
	data = [
		("A", 100, 1000),
		("A", 50, 2000),
		("B", 30, 3000),
		("A", 70, 7000),
	]

	return env.from_collection(
		collection=data,
		type_info=Types.TUPLE([
			Types.STRING(),
			Types.INT(),
			Types.LONG(),
		]),
	)


def create_watermark_strategy():
	return (
		WatermarkStrategy.for_monotonous_timestamps()
		.with_timestamp_assigner(TxnTimestampAssigner())
	)


def build_result_stream(ds):
	return (
		ds.assign_timestamps_and_watermarks(create_watermark_strategy())
		.key_by(lambda x: x[0], key_type=Types.STRING())
		.window(TumblingEventTimeWindows.of(Time.seconds(5)))
		.reduce(
			lambda a, b: (a[0], a[1] + b[1], b[2]),
			output_type=Types.TUPLE([
				Types.STRING(),
				Types.INT(),
				Types.LONG(),
			]),
		)
	)


def main():
	env = create_stream_environment()
	ds = create_source(env)
	result = build_result_stream(ds)
	result.print()
	env.execute("Event-Time Tumbling Window")


if __name__ == "__main__":
	main()