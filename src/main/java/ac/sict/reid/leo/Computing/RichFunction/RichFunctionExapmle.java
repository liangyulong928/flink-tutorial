package ac.sict.reid.leo.Computing.RichFunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionExapmle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.fromElements(1,2,3,4).map(new RichMapFunction<Integer, Integer>() {

            /**
             *
             * Rich Function 的生命周期概念
             * open方法：Rich Function的初始化方法，开启一个算子的生命周期，当一个算子实际工作方法被调用之前，open会首先被调用。
             *
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引是："+getRuntimeContext().getIndexOfThisSubtask()+"的任务的生命周期开始");
            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }


            /**
             *
             * close方法：生命周期中最后一个调用的方法，类似于结束方法，用来做一些清理工作。
             *
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引是："+getRuntimeContext().getIndexOfThisSubtask()+"的任务的生命周期结束");
            }
        }).print();
        environment.execute();
    }
}
