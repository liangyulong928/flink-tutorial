# Flink 状态管理
算子任务分为有状态和无状态两种。

- 无状态算子任务只需要观察每个独立事件，根据当前输入的数据直接转换输出结果。
- 有状态的算子任务除了当前数据意外，还需要一些其他数据（状态）得到计算结果。

有状态算子一般处理流程：

1. 算子任务收到上游发来的数据
2. 获取当前状态
3. 根据业务逻辑进行计算，更新状态
4. 得到计算结果，输出发送到下游任务

## 状态分类

#### 托管状态和原始状态

托管状态由Flink统一管理，状态存储访问、故障恢复和重组等一系列问题都由Flink实现。（通常方法）

原始状态是自定义的，需要我们自己管理、实现状态序列化和故障恢复。

#### 算子状态和按键分区状态

托管状态中的两种实现形式。主要解决算子任务按照并行度进行子任务划分，不同子任务会占据不同Slot，不同Slot计算资源物理隔离，状态只对当前实例有效的情况。

- 算子状态中，作用范围仅限当前算子任务实例，对一个并行子任务占据一个分区，所处理的数据都会访问相同的状态，状态对同一任务是共享的。
- 按键分区状态中，状态是根据输入流中定义的key来维护和访问的，所以只能定义在按键分区流中，也就是keyBy之后才能使用。

无论是Keyed State 还是 Operator State，都是在本地实例上维护的，每个并行子任务维护对应状态，算子子任务之间状态不共享。

## 按键分区状态

使用Keyed State必须基于KeyedStream。

#### ValueState

状态中只保存一个value。

案例：检测各传感器水位值，如果连续两个水位值超过10，就输出报警。

```java
public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.
          getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = environment.
          socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                          Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L));

        sensorDS.keyBy(r -> r.getId()).
          process(new KeyedProcessFunction<String, WaterSensor, String>() {
            ValueState<Integer> lastVcState;

            @Override
            public void open(Configuration param) throws Exception {
                super.open(param);
                lastVcState = getRuntimeContext().
                  getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                Integer vc = waterSensor.getVc();
                if (Math.abs(vc - lastVc) > 10){
                    collector.collect("传感器 = "+waterSensor.getId()+" ==> 当前水位值 = "+ vc + ",与上一条水位值 = "+ lastVc + ",相差超过10!!!!");
                }
                lastVcState.update(vc);

            }
        }).print();

        environment.execute();
    }
}
```

#### ListState

针对每种传感器输出最高的3个水位值

```java
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
```

#### MapState

```java
sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    MapState<Integer,Integer> vcCountMapState;

                    @Override
                    public void open(Configuration param) throws Exception {
                        super.open(param);
                        vcCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMapState", Types.INT,Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        Integer vc = waterSensor.getVc();
                        if (vcCountMapState.contains(vc)){
                            int count = vcCountMapState.get(vc);
                            vcCountMapState.put(vc,++count);
                        }
                        else {
                            vcCountMapState.put(vc,1);
                        }
                        StringBuilder builder = new StringBuilder();
                        builder.append("=========================\n");
                        builder.append("传感器id为:" + waterSensor.getId() + "\n");
                        for (Map.Entry<Integer, Integer> entry : vcCountMapState.entries()) {
                            builder.append(entry.toString() + "\n");
                        }
                        builder.append("=========================\n");
                        collector.collect(builder.toString());
                    }
                }
        ).print();
```

#### ReducingState

```java
sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration param){
                        sumVcState = this.getRuntimeContext().
                                getReducingState(new ReducingStateDescriptor<Integer>("sumVcState",Integer::sum,Integer.class));
                    }


                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, Integer>.Context context, Collector<Integer> collector) throws Exception {
                        sumVcState.add(waterSensor.getVc());
                        collector.collect(sumVcState.get());
                    }
                }
        ).print();
```

#### AggregatingState

```java
public class KeyedAggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                                withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L));

        sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    AggregatingState<Integer,Double> vcArgAggState;

                    @Override
                    public void open(Configuration param) throws Exception {
                        super.open(param);
                        vcArgAggState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                        "vcAvgAggState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                                        return Tuple2.of(integerIntegerTuple2.f0 + integer,integerIntegerTuple2.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                                        return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
//                                        return Tuple2.of(integerIntegerTuple2.f0 + acc1.f0,integerIntegerTuple2.f1 + acc1.f1);
                                        return null;
                                    }
                                }, Types.TUPLE(Types.INT,Types.INT)
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        vcArgAggState.add(waterSensor.getVc());
                        Double vcAvg = vcArgAggState.get();
                        collector.collect("传感器id为："+waterSensor.getId()+",平均水位值="+vcAvg);
                    }
                }
        ).print();
        environment.execute();
    }
}
```

#### TTL：状态生存时间

当状态在内存中存在的时间超出这个值时，就将它清除。

实现方法：设置失效时间 = 当前时间 + TTL  => 设置清除条件被触发则判断失效。

```java
sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration param) throws Exception{
                        super.open(param);
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5)).
                                setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).
                                setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).
                                setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                        stateDescriptor.enableTimeToLive(stateTtlConfig);
                        this.lastVcState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        Integer lastVc = lastVcState.value();
                        collector.collect("key="+waterSensor.getId()+",状态值="+lastVc);
                        if (waterSensor.getVc() > 10){
                            lastVcState.update(waterSensor.getVc());
                        }
                    }
                }
        ).print();
```

## 算子状态

#### 列表状态

算子并行度进行缩放调整时，算子的列表状态中所有元素被统一收集起来，然后在均匀分配（轮询）给所有并行任务。

```java
package ac.sict.reid.leo.State;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        environment.socketTextStream("localhost",7777).map(new MyCountMapFunction()).print();
        environment.execute();
    }


    private static class MyCountMapFunction implements MapFunction<String,Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String s) throws Exception {
            return ++count;
        }

        /**
         *
         * TODO 将本地变量拷贝到算子状态中，开启checkpoint时才会调用
         *
         * @param functionSnapshotContext
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("snapshotState...");
            state.clear();
            state.add(count);
        }

        /**
         * TODO 程序启动和恢复时，从状态中把数据添加到本地变量，每个子任务调用一次
         *
         * @param functionInitializationContext
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            System.out.println("initializeState ... ");
            ListState<Long> state = functionInitializationContext.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("state", Types.LONG)
            );
            // 把数据拷贝到本地变量
            if (functionInitializationContext.isRestored()){
                for (Long c : state.get()) {
                    count += c;
                }
            }
        }
    }
}
```

#### 联合列表状态

并行度调整时，相比于常规列表的轮询分配，联合列表状态直接广播状态的完成列表。子任务获取到了联合后完成的大列表，自行选择使用的状态项和要丢弃的状态项。

#### 广播状态

目标是希望算子并行子任务都保持同一份“全局”状态，用做统一的配置和规则设定。

##### 综合案例

需求：水位超过指定的阈值发送告警，阈值可以动态更改

```java
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
```

