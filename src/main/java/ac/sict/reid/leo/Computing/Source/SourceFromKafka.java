package ac.sict.reid.leo.Computing.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().
                setBootstrapServers("localhost:9092").setTopics("topic_1").
                setStartingOffsets(OffsetsInitializer.latest()).
                setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStreamSource<String> stream = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        stream.print("Kafka");
        environment.execute();
    }
}
