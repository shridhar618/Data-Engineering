from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

env = StreamExecutionEnvironment.get_execution_environment()

consumer = FlinkKafkaConsumer(
	"flink-input",
	SimpleStringSchema(),
	{
		"bootstrap.servers": "localhost:9092"
	},
)

consumer.set_start_from_earliest()

ds = env.add_source(consumer)
ds.print()

env.execute("Kafka as Flink Source")