package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 天气数据mapper处理
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Weather, Weather> {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String TEMP_END_SUFFIX = "°C";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString().trim();
        String[] splits = line.split("\\s+");
        String timeFormatStr = splits[0] + " " + splits[1];

        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Weather outValue = new Weather();
        Text outKey = new Text();
        try {
            Date datetime = sdf.parse(timeFormatStr);
            outValue.setTime(datetime);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(datetime);

            outValue.setYear(calendar.get(Calendar.YEAR));
            outValue.setMonth(calendar.get(Calendar.MONTH) + 1);
            outValue.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            outValue.setDateTimeStr(timeFormatStr);


            String temperature = splits[2];
            outValue.setTemporary(Double.parseDouble(temperature.substring(0, temperature.length() - TEMP_END_SUFFIX.length())));

            outKey.set(splits[0].substring(0, splits[0].lastIndexOf("-")));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        context.write(outValue, outValue);
    }
}
