package ac.sict.reid.leo.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("data/words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> word2One = lineDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        });
        UnsortedGrouping<Tuple2<String, Long>> word2OneUG = word2One.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = word2OneUG.sum(1);
        sum.print();
    }
}