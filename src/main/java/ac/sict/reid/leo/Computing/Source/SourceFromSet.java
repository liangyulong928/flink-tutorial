package ac.sict.reid.leo.Computing.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceFromSet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = Arrays.asList(1, 22, 3);
        DataStreamSource<Integer> ds = environment.fromCollection(data);
        ds.print();
        environment.execute();
    }
}
