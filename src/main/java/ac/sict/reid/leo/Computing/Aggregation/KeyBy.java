package ac.sict.reid.leo.Computing.Aggregation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

//        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(e -> e.id);
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.id;
            }
        });

        //聚合 : sum , min , max , minBy , maxBy
        // POJO 只能通过字段名称啦指定
        keyedStream.max("vc");


        environment.execute();
    }
}
