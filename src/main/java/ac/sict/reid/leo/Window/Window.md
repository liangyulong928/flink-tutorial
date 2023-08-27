# Flink 窗口
Flink作为流计算引擎，主要用来处理无界数据流。数据源源不断、无穷无尽。通过将无限数据切割成有限的“数据块”进行处理，就有“窗口”的概念。
在Flink中，窗口可以把流切割成有限大小的多个“存储桶”，每个数据都会分发的对应的桶中。当到达窗口结束时间时，就会对每个桶中收集数据进行计算处理。
窗口不是静态准备好的，是动态创建的——有数据到达时才会创建对应窗口。窗口结束时间时，窗口会触发计算并关闭。

## 窗口的分类
按照驱动类型分：

- 时间窗口：以时间点来定义窗口的开始和结束，到达结束时间时，窗口不再收集数据，触发计算输出结果。
- 计数窗口：基于元素个数截取数据，到达固定个数触发计算关闭窗口。

窗口分配规则分类：

- 滚动窗口：窗口之间没有重叠，也没有间隔。每个数据被分配到一个窗口，而且只属于一个窗口。
- 滑动窗口：除窗口大小外，还有滑动步长概念（代表计算频率）。数据可能被分配到多个窗口中。
- 会话窗口：长 度不固定，起始时间和结束时间也不固定，各分区之间没有关联，窗口之间一定不重叠。
- 全局窗口：相同key的所有数据分配到同一窗口，窗口没有结束，默认不会触发计算。

## 窗口API

- 按键分区窗口（Keyed Windows）

```java
stream.keyBy(...).window(...)
```

- 非按键分区

```java
stream.windowAll(...)
```

没有进行 keyBy 则原始的 DataStream 不会分多条逻辑流。窗口只有一个task执行，并行度为1。

#### 代码中窗口API调用

```java
stream.keyBy(<key selector>).window(<window assigner>).aggregate(<window function>)
```

#### 窗口分配器

定义窗口分配器是构建窗口算子的第一步，作用是定义数据应该被分配到哪个窗口。（指定窗口的类型）

定义方式

```java
stream.keyBy(...).window(< WindowAssigner >)  //返回WindowsStream
  
stream.windowAll(< WindowAssigner >)  //返回AllWindowedStream
```

时间窗口

```java
// 滚动处理时间窗口
stream.keyBy(...).window(TumblingProcessingTimeWindows.of(Time.second(5))).aggregate(...)

// 滑动处理时间窗口
stream.keyBy(...).window(SlidingProcessingTimeWindows.of(Time.seconds(10))).aggregate(...)
  
// 处理时间会话窗口
stream.keyBy(...).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).aggregate(...)
  
// 滚动事件时间窗口
stream.keyBy(...).window(TumblingEventTimeWindows.of(Time.second(5))).aggregate(...)
  
// 滑动事件时间窗口
stream.keyBy(...).window(SlidingEventTimeWindows.of(Time.second(10),Time.second(5))).aggregate(...)
  
// 事件时间会话窗口
stream.keyBy(...).window(EventTimeSessionWindows.withGap(Time.second(10))).aggregate(...)
```

计数窗口

```java
// 滚动计数窗口
stream.keyBy(...).countWindow(10)									// 当窗口元素达到10的时候，触发计算执行
  
// 滑动计数窗口
stream.keyBy(...).countWindow(10, 3)					// 每个窗口统计10个数据，每隔3个数据就统计输出一次结果
```

全局窗口

```java
stream.keyBy(...).window(GlobalWindows.create());
```

## 窗口函数

作用：定义窗口如何进行计算的操作

#### 增量聚合函数

将数据收集出来，进行聚合。

- ReduceFunction

```java
package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ReduceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.
                getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("localhost", 7777).
                map(new WaterSensorMapFunction()).keyBy(r -> r.getId()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(10))).
                reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor t0, WaterSensor t1) throws Exception {
                        System.out.println("调用reduce方法，之前结果：" + t0 + "，现在来的数据：" + t1);
                        return new WaterSensor(t0.getId(), System.currentTimeMillis(),
                                t0.getVc() + t1.getVc());
                    }
                }).print();

        environment.execute();

    }

}
```

- AggregateFunction
```java
package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> stream = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(sensor -> sensor.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sensorKS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> aggregate = sensorKS.aggregate(new org.apache.flink.api.common.functions.AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器\n");
                return 0;
            }

            @Override
            public Integer add(WaterSensor waterSensor, Integer integer) {
                System.out.println("调用add方法，value = " + waterSensor + "\n");
                return integer + waterSensor.getVc();
            }

            @Override
            public String getResult(Integer integer) {
                System.out.println("调用getResult方法\n");
                return integer.toString();
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                System.out.println("调用merge方法\n");
                return null;
            }
        });

        aggregate.print();

        environment.execute();


    }

}
```
输出结果
```shell
创建累加器

调用add方法，value = WaterSensor{id='s1', ts=2, vc=3}

调用add方法，value = WaterSensor{id='s1', ts=3, vc=5}

调用add方法，value = WaterSensor{id='s1', ts=2, vc=5}

调用add方法，value = WaterSensor{id='s1', ts=3, vc=6}

调用getResult方法

19
```

####  全窗口函数

在增量聚合函数中，每次窗口进行计算后窗口的数据不会被保存。然而，在计算中如果需要基于全部数据处理，需要窗口上下文的信息时，可以通过全窗口函数：通过收集窗口数据，在内部缓存起来，等窗口要输出结果时候在取出运算。

与增量聚合函数的区别：数据不是即来即处理，而是在窗口结束后统一处理。

方法

- ProcessWindowFunction

处理窗口函数。除了可以拿到窗口中所有数据以外，还可以拿到（上下文对象 => 包含窗口信息、当前时间和状态信息）

```java
package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = sensor.keyBy(value -> value.id);

        // 窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowed = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = windowed.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            // 分析参数：s => 窗口的key , context => 上下文信息 , iterable => 窗口包含的数据 , collector => 输出收集器
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                long count = iterable.spliterator().estimateSize();
                long start = context.window().getStart();
                long end = context.window().getEnd();
                collector.collect("key = " + s + " 的窗口[" + start + "," + end + "]包含 " + count + " 条数据 ====> " + iterable.toString());
            }
        });

        process.print();

        environment.execute();

    }
}
```

输出结果

```shell
key = s1 的窗口[1693101120000,1693101130000]包含 5 条数据 ====> [WaterSensor{id='s1', ts=4, vc=5}, WaterSensor{id='s1', ts=4, vc=5}, WaterSensor{id='s1', ts=4, vc=5}, WaterSensor{id='s1', ts=4, vc=5}, WaterSensor{id='s1', ts=4, vc=5}]
key = s2 的窗口[1693101120000,1693101130000]包含 4 条数据 ====> [WaterSensor{id='s2', ts=3, vc=4}, WaterSensor{id='s2', ts=3, vc=4}, WaterSensor{id='s2', ts=3, vc=4}, WaterSensor{id='s2', ts=3, vc=4}]
```

# 综合操作

增量聚合和全窗口函数的结合使用

```java
package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateAndProcessDemo {

    /**
     * @param args
     *
     * 思路：基于增量聚合函数处理窗口数据，每来一个数据就做一次聚合；等窗口需要触发计算，调用全窗口函数处理逻辑输出结果
     *
     * 此时，全窗口函数不再缓存所有数据，直接将增量聚合函数结果作为Iterable类型输入
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = sensor.keyBy(value -> value.id);

        WindowedStream<WaterSensor, String, TimeWindow> windowed = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /*
        * 窗口函数
        *
        * 增量聚合 Aggregate + 全窗口 process
        *    1、增量聚合函数处理数据：来一条计算一条
        *    2、窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
        *    3、经过全窗口函数的处理包装后，输出
        *
        * 结合两者优点：
        *   1、增量聚合：即时计算，存储中间结果，占用空间少
        *   2、全窗口函数：通过上下文实现灵活功能
        * */

        SingleOutputStreamOperator<String> aggregate = windowed.aggregate(new MyAgg(), new MyProcess());

        aggregate.print();

        environment.execute();


    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String> {

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器..");
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            System.out.println("调用add方法，value=" + waterSensor);
            return integer + waterSensor.getVc();
        }

        @Override
        public String getResult(Integer integer) {
            System.out.println("调用getResult方法：");
            return integer.toString();
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            System.out.println("调用merge方法..");
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long count = iterable.spliterator().estimateSize();

            collector.collect("key = "+ s + "的窗口[" + start + "," + end + ") 包含 " + count + "条数据 ====>" + iterable.toString());
        }
    }
}
```

输出结果

```shell
创建累加器..
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
创建累加器..
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用add方法，value=WaterSensor{id='s2', ts=3, vc=4}
调用add方法，value=WaterSensor{id='s1', ts=4, vc=5}
调用getResult方法：
key = s2的窗口[1693101010000,1693101020000) 包含 1条数据 ====>[24]
调用getResult方法：
key = s1的窗口[1693101010000,1693101020000) 包含 1条数据 ====>[30]
```

