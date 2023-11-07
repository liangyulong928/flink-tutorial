package ac.sict.reid.leo.Process;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        (element, tx) -> element.getTs() * 1000L
                )
        );

        KeyedStream<WaterSensor, String> keyedStream = ds.keyBy(r -> r.getId());

        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                // 注册定时器
                TimerService timerService = context.timerService();

                // A.事件时间的案例
                Long timestamp = context.timestamp();
                timerService.registerEventTimeTimer(5000L);
                System.out.println("当前key = " + currentKey + ",当前时间 = " + timestamp + ",注册了一个5s的定时器");

                // 获取当前时间进展：处理时间 - 当前系统时间，事件时间 - 当前watermark
                long wm = timerService.currentWatermark();

            }

            /**
             * TODO 2.时间进展到定时器注册的时间，调用该方法
             *
             * @param timestamp     当前时间进展，定时器被触发的时间
             * @param ctx           上下文
             * @param out           采集器
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp,ctx,out);
                String currentKey = ctx.getCurrentKey();
                System.out.println("当前key = " + currentKey + ",当前时间 = " + timestamp + ",定时器触发");
            }
        });


        process.print();
        environment.execute();
    }
}
