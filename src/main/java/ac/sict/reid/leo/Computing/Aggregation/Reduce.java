package ac.sict.reid.leo.Computing.Aggregation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.socketTextStream("localhost",7777).
                map(new WaterSensorMapFunction()).keyBy(WaterSensor::getId).
                reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor ws_1, WaterSensor ws_2) throws Exception {
                        System.out.println("Demo7_Reduce.reduce");
                        int max = Math.max(ws_1.getVc(), ws_2.getVc());
                        if (ws_1.getVc() > ws_2.getVc()){
                            ws_1.setVc(max);
                            return ws_1;
                        }
                        else {
                            ws_2.setVc(max);
                            return ws_2;
                        }
                    }
                }).print();
        environment.execute();
    }

}
