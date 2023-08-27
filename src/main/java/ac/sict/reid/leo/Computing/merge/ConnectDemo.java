package ac.sict.reid.leo.Computing.merge;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> source_1 = environment.socketTextStream("localhost", 7777).map(
                i -> Integer.parseInt(i)
        );
        DataStreamSource<String> source_2 = environment.socketTextStream("localhost", 8888);
        /*
        * TODO 使用connect回流
        * 1、 一次只能连接 2 条流
        * 2、 流的数据类型可以不一样
        * 3、 连接后可以调用map、flatmap等处理，但是各处理各的
        * */
        ConnectedStreams<Integer, String> connect = source_1.connect(source_2);
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return "来源于数字流:" + integer.toString();
            }

            @Override
            public String map2(String s) throws Exception {
                return "来源于字母流:" + s;
            }
        });
        result.print();
        environment.execute();
    }
}
