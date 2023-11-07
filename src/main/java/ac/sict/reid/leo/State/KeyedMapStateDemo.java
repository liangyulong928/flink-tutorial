package ac.sict.reid.leo.State;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class KeyedMapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L));

        sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    MapState<Integer,Integer> vcCountMapState;

                    @Override
                    public void open(Configuration param) throws Exception {
                        super.open(param);
                        vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMapState", Types.INT,Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        Integer vc = waterSensor.getVc();
                        if (vcCountMapState.contains(vc)){
                            int count = vcCountMapState.get(vc);
                            vcCountMapState.put(vc,++count);
                        }
                        else {
                            vcCountMapState.put(vc,1);
                        }
                        StringBuilder builder = new StringBuilder();
                        builder.append("=========================\n");
                        builder.append("传感器id为:" + waterSensor.getId() + "\n");
                        for (Map.Entry<Integer, Integer> entry : vcCountMapState.entries()) {
                            builder.append(entry.toString() + "\n");
                        }
                        builder.append("=========================\n");
                        collector.collect(builder.toString());
                    }
                }
        ).print();
        environment.execute();
    }
}
