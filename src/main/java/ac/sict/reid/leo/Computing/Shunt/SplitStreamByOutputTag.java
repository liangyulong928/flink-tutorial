package ac.sict.reid.leo.Computing.Shunt;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamByOutputTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        OutputTag<WaterSensor> s1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class)) {};
        OutputTag<WaterSensor> s2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class)) {};
        SingleOutputStreamOperator<WaterSensor> ds_1 = ds.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                if ("s1".equals(waterSensor.getId())) {
                    context.output(s1, waterSensor);
                } else if ("s2".equals(waterSensor.getId())) {
                    context.output(s2, waterSensor);
                } else {
                    collector.collect(waterSensor);
                }
            }
        });
        ds_1.print("主流，非s1,s2的传感器");
        SideOutputDataStream<WaterSensor> sideOutput_1 = ds_1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> sideOutput_2 = ds_1.getSideOutput(s2);
        sideOutput_1.printToErr("s1");
        sideOutput_2.printToErr("s2");
        environment.execute();
    }
}
