package ac.sict.reid.leo.State;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KeyedListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L));

        sensorDS.keyBy(r->r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Integer>("vcListState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        vcListState.add(waterSensor.getVc());
                        Iterable<Integer> vcListIt = vcListState.get();
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            vcList.add(vc);
                            vcList.sort(((o1, o2) -> o2 - o1));
                            if (vcList.size() > 3){
                                vcList.remove(3);
                            }
                            collector.collect("传感器id为"+waterSensor.getId()+",最大的3个水位值="+vcList.toString());
                            vcListState.update(vcList);
                        }

                    }
                }
        ).print();

        environment.execute();
    }
}
