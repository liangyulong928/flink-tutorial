package ac.sict.reid.leo.Sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        // 开启Checkpoint
        environment.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);



        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<String>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "Number:" + aLong;
                    }
                }, Long.MAX_VALUE,RateLimiterStrategy.perSecond(1000),Types.STRING);

        DataStreamSource<String> dataGen = environment.fromSource(dataGeneratorSource,
                WatermarkStrategy.noWatermarks(), "data-generator");
        /*
        * WatermarkStrategy.noWatermarks(): 这是用于指定水位线生成策略的部分。
        *   水位线在流处理中用于处理事件时间（event time）的概念，它可以帮助系统进行事件的有序处理和时间延迟处理。
        *   noWatermarks()，这表示没有特定的水位线生成策略，也就是没有对事件时间进行特殊处理。
        * */



        FileSink<String> fileSink = FileSink.<String>forRowFormat(
                                                    //设置文件输出行存储格式，此外Flink支持批量编码形式：forBulkFormat
                new Path("./data"), new SimpleStringEncoder<>("UTF-8")
        ).withOutputFileConfig(
                OutputFileConfig.builder().withPartPrefix("sict-reid-").withPartSuffix(".log").build()
        ).withBucketAssigner(
                //按照目录分桶：每小时一个目录
                new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault())
        ).withRollingPolicy(
                //文件滚动策略：1 分钟
                DefaultRollingPolicy.builder().withRolloverInterval(Duration.ofMinutes(1)).withMaxPartSize(
                        new MemorySize(1024*1024)
                ).build()
        ).build();


        dataGen.sinkTo(fileSink);
        environment.execute();
    }
}
