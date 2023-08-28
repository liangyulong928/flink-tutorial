# Flink 水位线

### 时间语义

- 事件时间：数据产生的时间
- 处理时间：数据真正被处理的时刻

一般情况下，业务日志数据中都会记录数据生成的时间戳(timestamp)，它就可以作为事件时间的判断基础。Flink 将事件时间作为默认的时间语义。

### 事件时间和窗口

逻辑时钟：事件进展靠着数据记录的时间戳来推动，使计算过程完全不依赖处理时间（系统时间）

### 水位线

用来衡量事件时间进展的标记。

##### 有序流中的水位线

理想状态下希望数据按生成顺序进入流中，每条数据产生一个水位线。在实际中，由于数据量非常大，为提高效率，每隔一段时间生成一个水位线。

##### 乱序流中的水位线

数据在节点中传输会导致顺序发生改变的乱序数据。

- 当数据量小时候，对数据提取时间戳插入水位线。（对乱序情况判定，判断时间戳是否比之前的大，如果小于之前的时间戳则不在生成新的水位线 d）。
- 数据量大的时候，同样以周期生成水位线，只需要保存一下之前所有数据中的最大时间戳，需要插入水位线时，就以最大时间戳作为新的水位线。
- 对于迟到数据，为了让窗口正确收集迟到数据，可以等上一段时间（比如 2 秒）。即用当前已有数据最大时间戳减去 2 秒，就是要插入水位线的时间戳。

##### 水位线特征

- 水位线是插入数据流中的一个标记，可以认为是一个特殊数据。
- 水位线主要是一个时间戳，表示当前事件时间的进展。
- 水位线基于数据的时间戳生成。
- 水位线时间戳必须单调递增，确保时钟一直向前推进。
- 水位线可以设置延迟来保证正确处理乱序数据。
- 水位线表示时间戳之前所有数据都到齐了，之后流中不会出现小于该时间戳数据。

### 生成水位线

基本原则：水位线设置后表示这个时间之前的数据全部到齐，之后不再出现。

##### 生成策略

有序流中内置水位线设置

```java
package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WaterlineCreateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());

        //TODO 定义 Watermark 策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.
                <WaterSensor>forMonotonousTimestamps().         // 指定watermark生成：升序、没有等待时间
                withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    // 指定时间戳分配器，从数据中提取
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestrap) {
                        System.out.println("数据=" + element + ",recordTs = " + recordTimestrap);
                        return element.getTs() * 1000L;
                    }
                }
        );

        // TODO 指定 watermark 策略
        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensor.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorWithWatermark.keyBy(value -> value.getId()).
          window(TumblingProcessingTimeWindows.of(Time.seconds(10))).process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.spliterator().estimateSize();
                        collector.collect("key = "+s+" 的窗口[" + start+" , "+ end +")包含 " + count + " 条数据 ===>" +iterable.toString());
                    }
                }
        ).print();

        environment.execute();


    }
}
```

乱序流中内置水位线设置

```java
package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());

        //TODO 定义 Watermark 策略
        // 指定时间戳分配器，从数据中提取
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).         
                withTimestampAssigner((element, recordTimestrap) -> {
                    System.out.println("数据=" + element + ",recordTs = " + recordTimestrap);
                    return element.getTs() * 1000L;
                }
        );

        // TODO 指定 watermark 策略
        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensor.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorWithWatermark.keyBy(value -> value.getId()).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.spliterator().estimateSize();
                        collector.collect("key = "+s+" 的窗口[" + start+" , "+ end +")包含 " + count + " 条数据 ===>" +iterable.toString());
                    }
                }
        ).print();

        environment.execute();

    }
}
```

### 水位线的传递

上游任务处理完水位线、时钟改变以后，要把当前水位线再次发出，广播给所有下游子任务。

此时当一个任务接收到了多个上游并行任务传递过来的水位线时，应该以最小的那个作为当前任务的事件时钟。（每个任务都以“处理完之前所有数据”为标准来确定自己的时钟。）

#### 模拟传递

```java
package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.PhysicalPartition.Computing.MyPartitioner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkIdlenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        SingleOutputStreamOperator<Integer> sockerDS = environment.socketTextStream("localhost", 7777).
                partitionCustom(new MyPartitioner(), r -> r).
                map(r -> Integer.parseInt(r)).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps().
                                withTimestampAssigner((r, ts) -> r * 1000L).withIdleness(Duration.ofSeconds(5))
                );

        sockerDS.keyBy(r -> r % 2).window(TumblingEventTimeWindows.of(Time.seconds(10))).
                process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.spliterator().estimateSize();
                        collector.collect("key = "+integer+" 的窗口[" + start+" , "+ end +")包含 " + count + " 条数据 ===>" +iterable.toString());
                    }
                }).print();

        environment.execute();

    }
}
```

### 迟到数据的处理

```java
package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).
                map(new WaterSensorMapFunction());

      
      // 水平线设置为乱序等待3秒
        WatermarkStrategy<WaterSensor> watermarkStrategy
          = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
          										withTimestampAssigner(
          														(element, recordTimestrap) -> 
          										(element.getTs() * 1000L));
        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark 
          = sensor.assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process 
          = sensorWithWatermark.keyBy(sensorDocker -> sensorDocker.
                                      getId()).
          														// 推迟 2 秒关窗
                                      window(TumblingEventTimeWindows.of(Time.seconds(2))).
                                      // 关窗后的迟到数据放入侧输出流
          														sideOutputLateData(lateTag).process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                                Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                long count = iterable.spliterator().estimateSize();
                                collector.collect("key = " + s + " 的窗口[" + start + " , " + end + ")包含 " + count + " 条数据 ===>" + iterable.toString());
                            }
                        }
                );

        process.print();
      	// 从主流获取侧输出流，打印
        process.getSideOutput(lateTag).printToErr("关窗后的迟到数据");
        environment.execute();

    }
}
```

## 基于时间的合流

### 窗口联结

```java
package ac.sict.reid.leo.Join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.fromElements(
                Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 3), Tuple2.of("c", 4)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = environment.fromElements(
                Tuple3.of("a", 1, 1), Tuple3.of("a", 11, 1), Tuple3.of("b", 2, 1),
                Tuple3.of("b", 12, 1), Tuple3.of("c", 14, 1), Tuple3.of("d", 15, 1)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        /*
        * TODO window join
        *   1. 落在同一个时间窗口范围内才能匹配
        *   2. 根据keyBy的key，来进行匹配关联
        *   3. 只能拿到匹配上的数据，类似有固定时间范围的inner join
        * */

        DataStream<String> join = ds1.join(ds2).where(r1 -> r1.f0).equalTo(r2 -> r2.f0).
                window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {

                    /**
                     *
                     * 关联上的数据，调用Join方法
                     *
                     * @param stringIntegerTuple2               ds1的数据
                     * @param stringIntegerIntegerTuple3        ds2的数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public String join(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) throws Exception {
                        return stringIntegerTuple2 + "<---------------->" + stringIntegerIntegerTuple3;
                    }
                });

        join.print();
        environment.execute();
    }
}
```

## 间接联结

处理的时间间隔不固定情况。Interval Join方法针对一条流的每个数据，开辟出时间戳前后一段时间间隔，这期间是否有来自另一条流的数据匹配。

##### 原理

给定两个时间点，作为间隔的“上界”和“下界”，对一条流A伤的任意数据a的，开辟一段时间间隔[a.timestamp + lowerBound , a.timestamp + upperBound]，即 a 的时间戳为中心，下至下界点、上至上界点的一个闭区间：这段时间作为可以匹配另一条数据的窗口范围。

```java
package ac.sict.reid.leo.Join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.fromElements(
                Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 3), Tuple2.of("c", 4)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = environment.fromElements(
                Tuple3.of("a", 1, 1), Tuple3.of("a", 11, 1), Tuple3.of("b", 2, 1),
                Tuple3.of("b", 12, 1), Tuple3.of("c", 14, 1), Tuple3.of("d", 15, 1)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        /*
        * TODO interval join
        *   1. 分别做keyBy, key ----> 解决关联条件
        *   2. 调用interval join
        * */
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        ks1.intervalJoin(ks2).between(Time.seconds(-2),Time.seconds(2)).process(
                new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     *
                     * 两条流数据匹配上调用该方法
                     *
                     * @param stringIntegerTuple2                   ks1 data
                     * @param stringIntegerIntegerTuple3            ks2 data
                     * @param context                               上下文
                     * @param collector                             采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception{
                        collector.collect(stringIntegerTuple2 + "<------------>" + stringIntegerIntegerTuple3);
                    }
                }
        ).print();

        environment.execute();

    }
}
```

对于处理迟到数据

```java
package ac.sict.reid.leo.Join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/*
* 针对有迟到数据情况的间接联结
*
* */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.socketTextStream("localhost", 7777).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[0], Integer.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                        withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = environment.socketTextStream("localhost", 8888).map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple3.of(split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                        withTimestampAssigner(
                                (value, ts) -> value.f1 * 1000L
                        )
        );

        /*
        * TODO Interval join
        *  1. 只支持事件时间
        *  2. 指定上界、下界的偏移，负号表示时间往前，正号表示时间往后
        *  3. process中只能处理 join 上的数据
        *  4. 两条流关联后的watermark，以两条流中最小的为准
        *  5. 如果当前数据的事件时间小于当前watermark，就是迟到数据，主流的process不处理
        *                ----> 将迟到数据放入侧输出流
        * */

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r -> r.f0);

        OutputTag<Tuple2<String,Integer>> ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String,Integer,Integer>> ks2LateTag = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2).between(Time.seconds(-2), Time.seconds(2)).
                sideOutputLeftLateData(ks1LateTag).sideOutputRightLateData(ks2LateTag).process(
                        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                                collector.collect(stringIntegerTuple2 + "<---------->" + stringIntegerIntegerTuple3);
                            }
                        }
                );

        process.print("主流");
        process.getSideOutput(ks1LateTag).printToErr("ks1 迟到数据");
        process.getSideOutput(ks2LateTag).printToErr("ks2 迟到数据");

        environment.execute();
    }
}
```

