package ac.sict.reid.leo.Process;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L)
                );

        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = ds.keyBy(waterSensor -> waterSensor.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                        if (waterSensor.getVc() > 10) {
                            context.output(warnTag, "当前水位=" + waterSensor.getVc() + ",大于阈值10!!!");
                        }
                        collector.collect(waterSensor);
                    }
                }
        );
        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");

        environment.execute();
    }
}
