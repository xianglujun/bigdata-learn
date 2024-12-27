package org.hadoop.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * 该类用来测试对HBase的Table的操作
 */
public class HBaseTableTest {

    /**
     * 这个是一个管理类，主要用于HBase的管理，包括创建表，删除表，列族管理等管理员能做的他都能够做
     */
    private Admin admin;
    private Configuration cfg;
    private Connection connection;

    @Before
    public void setUp() throws IOException {
        this.cfg = new Configuration();
        // 让代码能够链接zookeeper,
        cfg.set("hbase.zookeeper.quorum", "node1,node2,node3");
        this.connection = ConnectionFactory.createConnection(cfg);

        this.admin = connection.getAdmin();
    }

    private Table getTable(String tableName) throws IOException {
        Assert.assertNotNull("链接未初始化", this.connection);
        return this.connection.getTable(TableName.valueOf(tableName));
    }

    @Test
    public void createTableIfNotExists() throws IOException {
        String tableName = "api_table";
        TableName tbName = TableName.valueOf(tableName);
        boolean isExists = this.admin.tableExists(tbName);
        if (!isExists) {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            builder.setColumnFamilies(HBaseTableUtil.createColumnFamilyDescs("cf1", "cf2"));
            this.admin.createTable(builder.build());
            System.out.println(String.format("创建%s表成功", tableName));
        }
    }

    @Test
    public void getTableInfo() throws IOException {
        String tableName = "api_table";
        TableName tbName = isTableExists(tableName);

        List<TableDescriptor> descriptorList = this.admin.listTableDescriptors(Arrays.asList(tbName));
        Assert.assertTrue("表不存在", descriptorList.size() > 0);
        for (TableDescriptor tableDescriptor : descriptorList) {
            System.out.println("tableName: " + tableDescriptor.getTableName());
            System.out.println("columnFamily: " + Arrays.toString(tableDescriptor.getColumnFamilyNames().toArray()));
            System.out.println("flushPolicyClassName: " + tableDescriptor.getFlushPolicyClassName());
            System.out.println("maxFileSizes: " + tableDescriptor.getMaxFileSize());
            System.out.println("--------------------------------------------------------");
        }
    }

    /**
     * 向表中插入数据
     */
    @Test
    public void putData() throws IOException {
        String tableName = "api_table";
        isTableExists(tableName);

        Table table = this.getTable(tableName);
        Put put = new Put(Bytes.toBytes("1"));
        // 加入一行数据
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(18));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("address"), Bytes.toBytes("beijing"));
        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("phone"), Bytes.toBytes("123456789"));
        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("email"), Bytes.toBytes("zhangsan@163.com"));

        table.put(put);
    }

    /**
     * 获取一行数据
     */
    @Test
    public void getRow() throws IOException {
        String tableName = "api_table";
        isTableExists(tableName);
        Table table = this.getTable(tableName);

        Result result = table.get(new Get(Bytes.toBytes("1")));
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    private TableName isTableExists(String tableName) throws IOException {
        TableName tbName = TableName.valueOf(tableName);

        boolean isExists = this.admin.tableExists(tbName);
        Assert.assertTrue("表不存在", isExists);
        return tbName;
    }

    @Test
    public void deleteTable() throws IOException {
        String tableName = "api_table";
        TableName tbName = isTableExists(tableName);
        this.admin.disableTable(tbName);
        this.admin.deleteTable(tbName);
    }

    @After
    public void destroy() throws IOException {
        if (this.admin != null) {
            this.admin.close();
        }
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.close();
        }
    }

    public static class HBaseTableUtil {
        public static ColumnFamilyDescriptor createColumnFamilyDesc(String cfName) {
            return ColumnFamilyDescriptorBuilder.newBuilder(cfName.getBytes()).build();
        }

        public static Collection<ColumnFamilyDescriptor> createColumnFamilyDescs(String... cfNames) {
            Collection<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            for (String cfName : cfNames) {
                columnFamilyDescriptors.add(createColumnFamilyDesc(cfName));
            }
            return columnFamilyDescriptors;
        }
    }

}
