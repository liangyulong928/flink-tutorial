# Flink 处理函数

在本部分中，针对自定义处理逻辑，通过“处理函数”接口，实现对转换算子的概括性表达。

#### 处理函数的使用

基于DataStream调用process方法，方法中传入ProcessFunction参数。

```java
stream.process(new MyProcessFunction())
```

所有的处理函数都是RichFunction，富函数可以调用的处理函数都可以调用。

##### ProcessFunction解析

```java
public abstract class ProcessFunction<I,O> extends AbstractRichFunction{
  																// I: input,输入的数据类型; O:Output,输出的数据类型
  ...
  public abstract void processElement(I value,Context ctx,Collector<O> out) throws Exception;
  /*
  *	处理的核心逻辑，每个元素都会调用一次
  * 参数：
  *  1. value: 当前流中的输入元素，即正在处理的数据，类型与流中数据类型一致。
  *  2. ctx: 类型是ProcessFunction中定义的内部抽象类Context，表示当前运行的上下文，可以获得当前的时间戳，并提供用于查询时间和注册定时器的“定时服务，以及可以将数据发送到“侧输出流”的方法.output()
  *  3. out: 收集器（Collector）,用于返回输出数据。
  * */
  
  public void onTimer(long timestamp, onTimerContext ctx,Collector<O> out) throws Exception{}
  /*
  * 只有注册好的定时器触发时候才调用
  * 参数 timestamp 指设定好的触发时间，事件时间语义下就是水位线。
  **/

  ...
}
```

#### 处理函数的分类

- ProcessFunction：基于DataStream直接调用.process()时作为参数传入。
- KeyedProcessFunction：基于KeyedStream调用.process()时候作为参数传入。
- ProcessWindowFunction：基于WindowedStream调用.process()时候作为参数传入。
- ProcessAllWindowFunction：基于AllWindowedStream调用.process()时候作为参数传入。
- CoProcessFunction：合并（connect）两条流之后的处理函数
- ProcessJoinFunction：间接连接（interval join）两条流之后的处理函数
- BroadcastProcessFunction：广播连接流处理函数
- KeyedBroadcastProcessFunction：按键分区的广播连接流处理函数

##### 按键分区处理函数

ProcessFunction的Context提供了timerService方法可以直接返回TimeService对象。

TimeService相关方法

```java
// 获取当前处理时间
long currentProcessingTime();

// 获取当前的水位线
long currentWatermark();

// 注册处理时间定时器，当处理时间超过time时触发
void registerProcessingTimeTimer(long time);

// 注册事件时间定时器，当水位线超过time时触发
void registerEventTimeTimer(long time);

// 删除触发时间为time的处理时间定时器
void deleteProcessingTimeTimer(long time);

// 删除触发时间为time的事件时间定时器
void deleteEventTimeTimer(long time)；
```

TimeService会以key和时间戳为标准，对定时器进行去重，对每个key和时间戳，最多只有一个定时器，如果注册多次，onTimer方法也仅被调用一次。

## KeyedProcessFunction案例

```java
keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                // 注册定时器
                TimerService timerService = context.timerService();

                // A.事件时间的案例
                Long timestamp = context.timestamp();
                timerService.registerEventTimeTimer(5000L);
                System.out.println("当前key = " + currentKey + ",当前时间 = " + timestamp + ",注册了一个5s的定时器");

                // B.处理时间的案例
                long currentTs = timerService.currentProcessingTime();
                timerService.registerProcessingTimeTimer(currentTs + 5000L);
                System.out.println("当前key = " + currentKey + ",当前时间 = " + currentTs + ",注册了一个5s的定时器");

                // C.获取process的当前watermark
                long currentWatermark = timerService.currentWatermark();
                System.out.println("当前数据 = " + waterSensor + ",当前watermark = " +currentWatermark);
            }
        })
```

获取当前时间进展

```java
keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                // 注册定时器
                TimerService timerService = context.timerService();

                // 获取当前时间进展：处理时间 - 当前系统时间，事件时间 - 当前watermark
                long currentTs = timerService.currentProcessingTime();
                long wm = timerService.currentWatermark();
                
            }
        });
```

定时器触发

```java
SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
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
```

## 窗口处理函数

调用方式

```java
 SingleOutputStreamOperator<String> process = ds.keyBy(t -> t.getId()).
   window(TumblingEventTimeWindows.of(Time.seconds(10))).process(
   new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, 
                                Context context,
                                Iterable<WaterSensor> iterable, 
                                Collector<String> collector) throws Exception {

            }
        });
```

## 综合应用案例——Top N

需求：实时统计一段时间内的出现次数最多的水位。例如，统计最近10秒内出现次数最多的两个水位，并且每5秒更新一次。

```java
public class ProcessAllWindowTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.
          getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = environment.
          socketTextStream("localhost", 7777).
          map(new WaterSensorMapFunction()).
          assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                          Duration.ofSeconds(3)).
                                withTimestampAssigner(
                                  (element, ts) -> element.getTs() * 1000L));

        ds.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),
                                                Time.seconds(5))).
          process(new MyTopNPAWF()).print();
        environment.execute();
    }
}

public class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor,String, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        for (WaterSensor sensor : iterable){
            Integer vc = sensor.getVc();
            if (hashMap.containsKey(vc)){
                hashMap.put(vc, hashMap.get(vc)+1);
            }
            else {
                hashMap.put(vc,1);
            }
        }

        List<Tuple2<Integer,Integer>> datas = new ArrayList<>();
        for (Integer vc : hashMap.keySet()) {
            datas.add(Tuple2.of(vc,hashMap.get(vc)));
        }

        datas.sort((o1, o2) -> o2.f1 - o1.f1);
        /*
        * Lambda表达式定义了一个匿名函数，实现了Comparator接口的compare方法。
        * 在这个Lambda表达式中，o1和o2分别代表要比较的两个对象。
        * o2.f1 - o1.f1表示对这两个对象的f1属性进行比较，并以降序的方式进行排序。
        * 如果o2.f1的值大于o1.f1的值，那么结果为正数，表示o2应该排在o1之前；如果相等，结果为0；如果o2.f1的值小于o1.f1的值，结果为负数，表示o2应该排在o1之后。
        */

        StringBuilder outStr = new StringBuilder();
        outStr.append("====================================\n");
        for (int i = 0; i < Math.min(2, datas.size()); i++) {
            Tuple2<Integer, Integer> vcCount = datas.get(i);
            outStr.append("Top" + (i+1) + "\n");
            outStr.append("vc="+vcCount.f0+"\n");
            outStr.append("count="+vcCount.f1+"\n");
            outStr.append("窗口的结束时间 = "+ 
                          DateFormatUtils.format(context.window().getEnd(),
                                                 "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
            outStr.append("================================\n");
        }

        collector.collect(outStr.toString());
    }
}
```

另一种方法：使用KeyedProcessFunction

优化思路

1. 对数据按键分区，分别统计vc出现次数
2. 增量聚合，得到结果最后进行输出

```java
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

  // 实现对象的属性进行聚合计算
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

  // 定义在时间窗口内对数据流进行计算的函数
    private static class WindowResult extends ProcessWindowFunction<Integer,Tuple3<Integer,Integer,Long>,Integer, TimeWindow> {
        @Override
        public void process(Integer integer, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
          
          // 因为数据已经完成聚合，所以迭代器里只有一条数据，next一次即可
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
            dataList.sort((o1,o2)->o2.f1-o1.f1);
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
```

## 侧输出流

需求：对每个传感器，水位超过10的输出告警信息

```java
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
```

输出结果

```java
主流> WaterSensor{id='s1', ts=4, vc=55}
warn> 当前水位=55,大于阈值10!!!
主流> WaterSensor{id='s2', ts=3, vc=43}
warn> 当前水位=43,大于阈值10!!!
warn> 当前水位=21,大于阈值10!!!
主流> WaterSensor{id='s1', ts=4, vc=21}
```