package ac.sict.reid.leo.Process;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor,String, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        for (WaterSensor sensor : iterable){
            Integer vc = sensor.getVc();
            if (hashMap.containsKey(vc)){
                hashMap.put(vc, hashMap.get(vc)+1);
            }
            else {
                hashMap.put(vc,1);
            }
        }

        List<Tuple2<Integer,Integer>> datas = new ArrayList<>();
        for (Integer vc : hashMap.keySet()) {
            datas.add(Tuple2.of(vc,hashMap.get(vc)));
        }

        datas.sort((o1, o2) -> o2.f1 - o1.f1);

        StringBuilder outStr = new StringBuilder();
        outStr.append("====================================\n");
        for (int i = 0; i < Math.min(2, datas.size()); i++) {
            Tuple2<Integer, Integer> vcCount = datas.get(i);
            outStr.append("Top" + (i+1) + "\n");
            outStr.append("vc="+vcCount.f0+"\n");
            outStr.append("count="+vcCount.f1+"\n");
            outStr.append("窗口的结束时间 = "+ DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss.SSS") + "\n");
            outStr.append("================================\n");
        }

        collector.collect(outStr.toString());
    }
}
