from pyflink.datastream import StramExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer,FlinkKafkaProducer

env=StreamExecutionEnvironment.get_execution_environment()

# Adding a Kafka Source
consumer=FlinkKafkaConsumer("flink-input", SimpleStringSchema())
stream=env.add_source(consumer)

# Adding a Kafka Sink
alerts=stream.filter(lambda x: "ERROR" in x)
producer=FlinkKafkaProducer("flink-alerts", SimpleStringSchema())
alerts.add_sink(producer)

env.execute("Kafka as Flink Source and Sink")









from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer,FlinkKafkaProducer

env=StramExecutionEnvironment.get_exection_envrironment()

consumer=FlinkKafkaConsumer("flink-input", SimpleStringSchema())
stream=add_sounrce(consumer)

producer=FlinkKafkaProducer("flink_output")
stream.add_sink(producer)

env.execute("Kafka->Kafka Pipeline")





from pyflink.datastream import StreamEnironmentExecutor
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer

env=StramExecutionEnvironment.get_exection_environment()

consumer=FlinkKafkaConsumer("Flink-input", SimpleStringSchema(),{"bootstrap.servers":"localhost:9092"})
stream=add_source(consumer)

alerts=stream.filter(lambda x: "ERROR" in x)
producer=FlinkKafkaProducer("flink-alerts",SimpleStringSchema(),{"bootstrap.servers":"localhost:9092"})
alerts.add_sink(producer)

env.execute("Kafka->Kafka Pipeline")




from pyflink.datastream import StreamExecutorEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkafkaProducer

env=StreamExecutorEnvironme


