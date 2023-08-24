package ac.sict.reid.leo.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineStream = env.readTextFile("data/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).keyBy(data -> data.f0).sum(1);
        sum.print();
        env.execute();
    }
}
