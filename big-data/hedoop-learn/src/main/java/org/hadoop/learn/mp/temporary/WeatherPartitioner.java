package org.hadoop.learn.mp.temporary;

import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 分区器
 */
public class WeatherPartitioner extends Partitioner<Weather, Weather> {
    Logger logger = LoggerFactory.getLogger(WeatherPartitioner.class);
    @Override
    public int getPartition(Weather text, Weather weather, int numPartitions) {
        logger.error("执行partitioner................");
        return (weather.getYear() + weather.getMonth()) % numPartitions;
    }
}
