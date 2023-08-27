package ac.sict.reid.leo.Computing.merge;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> ds_1 = environment.fromElements(1, 2, 3);
        DataStreamSource<Integer> ds_2 = environment.fromElements(2, 2, 3);
        DataStreamSource<String> ds_3 = environment.fromElements("2", "2", "3");
        ds_1.union(ds_2,ds_3.map(Integer::valueOf)).print();
        environment.execute();
    }
}
