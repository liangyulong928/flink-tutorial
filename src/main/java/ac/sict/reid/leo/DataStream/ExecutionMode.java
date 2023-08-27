package ac.sict.reid.leo.DataStream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream API 执行模式包括：Streaming、Batch、AutoMatic
 */
public class ExecutionMode {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        //设置批执行模式
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        /*
        * 通过命令行模式设置：
        *      flink run -Dexecution.runtime-mode=BATCH jar
        * */


        // 触发程序执行
        environment.execute();
    }
}
