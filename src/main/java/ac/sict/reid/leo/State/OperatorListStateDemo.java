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
