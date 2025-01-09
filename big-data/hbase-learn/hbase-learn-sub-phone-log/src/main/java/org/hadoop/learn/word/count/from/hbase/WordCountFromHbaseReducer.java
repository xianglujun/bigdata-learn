package org.hadoop.learn.word.count.from.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCountFromHbaseReducer implements TableReduce<Text, LongWritable> {
    private static final String NAMESPACE = "wordcount";
    private static final String TABLE = "wordcount2";

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }

        Put put = new Put(key.getBytes());
        put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("count"), Bytes.toBytes(sum));
        output.collect(new ImmutableBytesWritable(key.getBytes()), put);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {
        try {
            Connection connection = ConnectionFactory.createConnection(job);
            Admin admin = connection.getAdmin();
            initNamespace(admin);
            initTable(admin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initTable(Admin admin) {
        TableName tableName = TableName.valueOf(NAMESPACE, TABLE);
        try {
            boolean isExits = admin.tableExists(tableName);
            if (!isExits) {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base")).build());
                admin.createTable(tableDescriptorBuilder.build());
                System.out.println("表创建成功: " + TABLE);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initNamespace(Admin admin) {
        try {
            admin.getNamespaceDescriptor(NAMESPACE);
        } catch (NamespaceNotFoundException e) {
            try {
                admin.createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
                System.out.println("namespace创建成功: " + NAMESPACE);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
