package ac.sict.reid.leo.State;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());

        // 广播配置
        DataStreamSource<String> config = environment.socketTextStream("localhost", 8888);

        MapStateDescriptor<String, Integer> mapState = new MapStateDescriptor<>("broad-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = config.broadcast(mapState);
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = ds.connect(configBS);
        sensorBCS.process(
                new BroadcastProcessFunction<WaterSensor, String, String>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        ReadOnlyBroadcastState<String, Integer> broadcastState = readOnlyContext.getBroadcastState(mapState);
                        Integer threshold = broadcastState.get("threshold");
                        threshold = (threshold==null?0:threshold);
                        if (waterSensor.getVc() > threshold){
                            collector.collect(waterSensor + ",水位线超过指定阈值："+threshold + "!!!");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String s, BroadcastProcessFunction<WaterSensor, String, String>.Context context, Collector<String> collector) throws Exception {
                        BroadcastState<String, Integer> broadcastState = context.getBroadcastState(mapState);
                        broadcastState.put("threshold",Integer.valueOf(s));
                    }
                }
        ).print();

        environment.execute();


    }
}
