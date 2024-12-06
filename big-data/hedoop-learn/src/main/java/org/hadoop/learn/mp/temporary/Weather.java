package org.hadoop.learn.mp.temporary;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 天气数据
 */
@Data
public class Weather implements WritableComparable<Weather> {

    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private Integer year;
    private Integer month;
    private Integer day;

    private String dateTimeStr;

    private Date time;

    private Double temporary;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);

        out.writeUTF(this.dateTimeStr);
        out.writeDouble(this.temporary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();

        this.dateTimeStr = in.readUTF();
        this.temporary = in.readDouble();

        try {
            if (this.dateTimeStr != null && !this.dateTimeStr.equals("")) {
                SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
                this.time = sdf.parse(this.dateTimeStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int compareTo(Weather o) {
        if (o == null) {
            return -1;
        }

        // 这里按照年月倒序排，然后按照温度倒序排
        String yearMonth = this.year + "-" + this.month;
        String targetYearMonth = o.getYear() + "-" + o.getMonth();

        int cr = yearMonth.compareTo(targetYearMonth);

        if (cr == 0) {
            cr = this.temporary.compareTo(o.getTemporary());
        }

        return -cr;
    }

    /**
     * 自定义比较器
     */
    public static class WeatherComparator extends WritableComparator {

        public WeatherComparator() {
            super(Weather.class);
        }

        static {
            WritableComparator.define(Weather.class, new WeatherComparator());
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return super.compare(a, b);
        }
    }
}
