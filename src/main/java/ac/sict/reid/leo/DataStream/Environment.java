package ac.sict.reid.leo.DataStream;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Environment {

    /**
     *
     * 介绍多种创建执行华宁的方式
     *
     * @param args
     */
    public static void main(String[] args) {


        // 根据当前运行上下文得出正确结果
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建本地执行环境，默认并发度是本地CPU核心数
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        //返回集群执行环境
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081, "path/to/jar");
    }
}
