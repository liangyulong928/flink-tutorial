package ac.sict.reid.leo.Sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers("localhost:9092").
                setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder().
                                setTopic("ws").
                                setValueSerializationSchema(new SimpleStringSchema()).build()
                ).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).
                setTransactionalIdPrefix("sict-reid-").
                setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "").
                build();
        stream.sinkTo(kafkaSink);
        environment.execute();
    }
}
