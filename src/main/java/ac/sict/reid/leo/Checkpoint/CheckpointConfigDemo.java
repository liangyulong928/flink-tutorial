package ac.sict.reid.leo.Checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CheckpointConfigDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 使用hdfs保存检查点，导入依赖
        System.setProperty("HADOOP_USER_NAME","root");

        /*
        * TODO 配置检查点
        *   1.启动检查点
        *   2.指定检查点的存储位置
        *   3.设置 checkpoint 超时时间：默认 10 分钟
        *   4.设置同时运行checkpoint最大数量
        *   5.设置最小等待间隔：
        *          上一轮 checkpoint 结束到下一轮 checkpoint 开始之间的间隔设置了 > 0 ，并发就会变成 1
        *   6.设置取消作业时外部系统持久化会保留
        *   7.设置checkpoint连续失败次数
        * */

        environment.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://localhost:8020/path");
        checkpointConfig.setCheckpointTimeout(60000);               // 10 分钟
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        /*
        * TODO 非对齐检查点
        * */
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));



    }
}
