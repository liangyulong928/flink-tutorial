package ac.sict.reid.leo.Computing.PhysicalPartition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区算子：Flink的分区策略
 */
public class ShuffleExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);
        /*
        * TODO shuffle：
        * 把流中数据完全随机打乱，均匀传递到下游任务分区。
        * */
        stream.shuffle().print();
        /*
        * TODO rebalance：
        * 把流中数据按照先后顺序依次分发，实现轮询重分区。
        * 使用Round-Robin负载均衡算法
        * */
        stream.rebalance().print();
        /*
        * TODO broadcast：
        * 数据在不同的分区都保留一份，将输入数据复制并发送到下游算子并行任务中。
        * */
        stream.broadcast().print();
        /*
        * TODO global:
        * 全局分区，将所有输入流数据都发送到下游算子的第一个并行子任务中（让下游任务并行度变为 1 ）
        * 慎重使用
        * */
        stream.global().print();
        environment.execute();
    }
}
