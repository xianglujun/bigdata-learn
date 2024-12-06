package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherGroupingComparator extends WritableComparator {

    Logger logger = LoggerFactory.getLogger(WeatherGroupingComparator.class);

    protected WeatherGroupingComparator() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        logger.error("WeatherGroupingComparator........................");

        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;

        String yearMonth = w1.getYear() + "-" + w1.getMonth();
        String targetYearMonth = w2.getYear() + "-" + w2.getMonth();
        return yearMonth.compareTo(targetYearMonth);
    }
}
