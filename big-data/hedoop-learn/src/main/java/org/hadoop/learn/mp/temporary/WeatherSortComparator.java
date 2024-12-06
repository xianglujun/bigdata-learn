package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherSortComparator extends WritableComparator {

    Logger logger = LoggerFactory.getLogger(WeatherSortComparator.class);

    public WeatherSortComparator() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        logger.error("执行WeatherSortComparator...............");

        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        return w1.compareTo(w2);
    }
}
