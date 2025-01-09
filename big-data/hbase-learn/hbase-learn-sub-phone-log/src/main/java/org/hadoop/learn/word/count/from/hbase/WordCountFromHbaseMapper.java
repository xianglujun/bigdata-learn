package org.hadoop.learn.word.count.from.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.List;

/**
 * 用于对输入的拆分，将句子拆分成单词等
 */
public class WordCountFromHbaseMapper implements TableMap<Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable(1);

    @Override
    public void map(ImmutableBytesWritable key, Result value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        byte[] bases = Bytes.toBytes("base");
        List<Cell> columnCells = value.getColumnCells(bases, Bytes.toBytes("line"));
        for (Cell cell : columnCells) {
            String line = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            String[] words = line.split(" ");
            for (String word : words) {
                String s = word.toLowerCase().replaceAll("!([a-z]+)", "");
                outKey.set(s);
                output.collect(outKey, outValue);
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }
}
