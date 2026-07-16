from pyflink.datastream import StramExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer,FlinkKafkaProducer

env=StreamExecutionEnvironment.get_execution_environment()
consumer=FlinkKafkaConsumer("flink-input")

stream=env.add_source(consumer)

alerts=stream.filter(lambda x: "ERROR" in x)
producer=FlinkKafkaProducer("flink-alerts")

alerts.add_sink(producer)

env.execute("Kafka as Flink Source and Sink")
