package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Weather, Weather, Text, DoubleWritable> {
    @Override
    protected void reduce(Weather key, Iterable<Weather> values, Reducer<Weather, Weather, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("开始处理" + key.toString() + "的温度数据");
        int day = -1;
        for (Weather w : values) {
            if (day == -1) {
                context.write(new Text(w.getDateTimeStr()), new DoubleWritable(w.getTemporary()));
                day = w.getDay();
            } else if (day != w.getDay()) {
                context.write(new Text(w.getDateTimeStr()), new DoubleWritable(w.getTemporary()));
                day = w.getDay();
                break; // 只取两天，所以执行到这里就代表已经获取到了两天的数据
            }
        }
    }
}
