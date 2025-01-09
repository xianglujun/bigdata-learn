package org.hadoop.learn.word.count.from.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * 这里是做数据初始化
 */
public class SentencesInitializer {
    private String namespace;
    private String tableName;
    private TableName tbName;
    private Connection connection;

    public SentencesInitializer(String namespace, String tableName, Configuration cfg) throws IOException {
        this.namespace = namespace;
        this.tableName = tableName;
        this.tbName = TableName.valueOf(namespace, tableName);
        this.connection = ConnectionFactory.createConnection(cfg);
    }

    public void init() throws IOException {
        try {
            initNamespace();
            initTable();

            // 读取数据，并写入到数据表
            int key = 1;
            InputStream is = this.getClass().getResourceAsStream("sentences.txt");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line = null;
                BufferedMutator bufferedMutator = connection.getBufferedMutator(this.tbName);
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().equals("")) {
                        Put put = new Put(Bytes.toBytes(key));
                        put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("line"), Bytes.toBytes(line));
                        bufferedMutator.mutate(put);
                        key++;
                    }
                }
                bufferedMutator.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void initTable() throws IOException {
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(this.tbName);
        if (b) {
            admin.disableTable(this.tbName);
            admin.truncateTable(this.tbName, true);

            return;
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(this.tbName);
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base")).build());
        admin.createTable(tableDescriptorBuilder.build());
    }

    private void initNamespace() throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(this.namespace);
        } catch (NamespaceNotFoundException e) {
            e.printStackTrace();
            admin.createNamespace(NamespaceDescriptor.create(this.namespace).build());
        }
    }
}
