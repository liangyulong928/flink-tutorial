package ac.sict.reid.leo.Computing.PhysicalPartition;

import ac.sict.reid.leo.Computing.PhysicalPartition.Computing.MyPartitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Custom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 7777);
        DataStream<String> stringDataStream = stream.partitionCustom(new MyPartitioner(), value -> value);
        stringDataStream.print();
        environment.execute();
    }

}
