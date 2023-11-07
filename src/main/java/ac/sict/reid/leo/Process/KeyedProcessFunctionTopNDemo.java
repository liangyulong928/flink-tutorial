package ac.sict.reid.leo.Process;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyedProcessFunctionTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L)
                );

        /*
        * TODO
        *  根据vc做keyby，开窗，分别count
        *       -> 增量聚合，计算count  -> 全窗口，对计算结果count封装，带上窗口结束时间标签
        *  对同一窗口count值进行处理，排序，取前n个 -> 使用windowEnd做keyBy -> 使用process调用
        *       -> 使用定时器，对存起来的结果进行排序，取前n个
        * */

        SingleOutputStreamOperator<Tuple3<Integer,Integer,Long>> winAgg = ds.keyBy(r -> r.getVc()).
                window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))).
                aggregate(new VcCountAgg(),new WindowResult());

        winAgg.keyBy(r -> r.f2).process(new TopN(2)).print();

        environment.execute();

    }

    private static class VcCountAgg implements AggregateFunction<WaterSensor,Integer,Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }

    private static class WindowResult extends ProcessWindowFunction<Integer,Tuple3<Integer,Integer,Long>,Integer, TimeWindow> {
        @Override
        public void process(Integer integer, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            Integer count = iterable.iterator().next();
            long end = context.window().getEnd();
            collector.collect(Tuple3.of(integer,count,end));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long,Tuple3<Integer,Integer,Long>,String> {

        private int threshold;
        private Map<Long, List<Tuple3<Integer,Integer,Long>>> dataListMap;

        public TopN(int i) {
            this.threshold = i;
            dataListMap = new HashMap<>();
        }


        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context context, Collector<String> collector) throws Exception {
            Long windowEnd = value.f2;
            if (dataListMap.containsKey(windowEnd)){
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            }
            else {
                List<Tuple3<Integer,Integer,Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataListMap.put(windowEnd,dataList);
            }
            context.timerService().registerEventTimeTimer(windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp,ctx,out);
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort((o1,o2)->o2.f1 - o1.f1);
            StringBuilder outStr = new StringBuilder();
            outStr.append("====================================\n");
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top" +(i+1) + "\n");
                outStr.append("vc = " + vcCount.f0 + "\n");
                outStr.append("count="+vcCount.f2+"\n");
                outStr.append("窗口结束时间 = "+vcCount.f2 + "\n");
                outStr.append("====================================\n");
            }

            dataList.clear();

            out.collect(outStr.toString());
        }
    }
}
