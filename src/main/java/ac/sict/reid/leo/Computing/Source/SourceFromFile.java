package ac.sict.reid.leo.Computing.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                                new Path("data/words.txt")).build();
                                            //也可以是目录，读取该目录下所有文件；也可以读取HDFS，使用hdfs://...
        environment.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"file").print();
        environment.execute();
    }
}
