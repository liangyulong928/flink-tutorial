package ac.sict.reid.leo.Computing.Shunt;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = environment.socketTextStream("localhost",7777);
        SingleOutputStreamOperator<Integer> ds = stream.map(Integer::valueOf);
        SingleOutputStreamOperator<Integer> ds1 = ds.filter(x -> x % 2 == 0);
        SingleOutputStreamOperator<Integer> ds2 = ds.filter(x -> x % 2 == 1);
        ds1.print("偶数");
        ds2.print("奇数");
        environment.execute();
    }
}