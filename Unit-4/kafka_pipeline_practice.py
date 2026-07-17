from sympy import prod

from pyflink.datastream import StreamExectionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer

env=StreamExectionEnvironment.get_execytion_environment()

consumer=FlinkKafkaConsumer("flink-input",SimpleStringSchema(),{"bootstrap.servers":"localhost:9092"})
stream=add_source(consumer)

alerts=stream.filter(lambda x:"ERROR" in x)
producer=FlinkKafkaProducer("flink-alert",SimpleStringSchema(),{"bootstrap.servers":"localhost:9092"})
alerts.add_sink(producer)

env.execute("Kafka Pipelining")






from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer

env=StreamExecutionEnivironment.get_execution_environment()

consumer=FlinkKafkaConsumer("flink-input",SimpleStringSchema(),{"bootstrap.servers":"localhost:9092"})
stream=add_sourse(consumer)

alerts=stream.filter(lambda x: "ERROR" in x)
producer=FlinkKafkaProducer("flink-alert",SimpleStringSchema(),{"bootstrap.servers":"localhost9092"})
alerts.add_sink(producer)

env.execute("kafka pipelining")

