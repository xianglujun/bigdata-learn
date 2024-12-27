package org.hadoop.hbase.learn.phone.log;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.hadoop.learn.phone.log.HBasConnectionHelper;
import org.hadoop.learn.phone.log.PhoneLogInitializor;
import org.junit.Test;

import java.io.IOException;

public class PhoneLogTest {

    private static final String FLAG = "phone_log_test";
    private TableName tableName = TableName.valueOf("phone_log:phone_log");

    @Test
    public void init() throws IOException {
        PhoneLogInitializor initializor = new PhoneLogInitializor("phone_log", "phone_log");
        initializor.randomData();
    }

    /**
     * 查询通话在3月份的所有通话记录
     */
    @Test
    public void query() throws IOException {
        Scan scan = new Scan();
        Filter startFilter = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("datetime"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("2024-01-01 00:00:00"));
        Filter endFilter = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("datetime"), CompareOperator.LESS, Bytes.toBytes("2024-06-15 13:07:58"));

        FilterList filterList = new FilterList(startFilter, endFilter);
        scan.setFilter(filterList);

        scan.withStartRow(Bytes.toBytes("13310019762_1711679478875"), true);
        scan.withStopRow(Bytes.toBytes("13310953490_1716287948631"), true);

//        byte[] bases = Bytes.toBytes("base");
//        scan.addColumn(bases, Bytes.toBytes("datetime"));
//        scan.addColumn(bases, Bytes.toBytes("rnum"));
//        scan.addColumn(bases, Bytes.toBytes("snum"));
//        scan.addColumn(bases, Bytes.toBytes("seconds"));

        Connection connection = HBasConnectionHelper.getConnection(FLAG);
        Table table = connection.getTable(this.tableName);
        ResultScanner scanner = table.getScanner(scan);
        this.printResult(scanner);
    }

    private void printResult(ResultScanner scanner) {
        for (Result result : scanner) {
            this.printResult(result);
        }
    }

    private void printResult(Result result) {
        byte[] bases = Bytes.toBytes("base");
        System.out.println(Bytes.toString(result.getRow()) + "\t" +
                Bytes.toString(getBytes(result, bases, "rnum"))
                + "\t" + Bytes.toString(getBytes(result, bases, "snum"))
                + "\t" + Bytes.toString(getBytes(result, bases, "datetime"))
                + "\t" + Bytes.toInt(getBytes(result, bases, "seconds")));
    }

    private static byte[] getBytes(Result result, byte[] bases, String column) {
        Cell cell = result.getColumnLatestCell(bases, Bytes.toBytes(column));
        return CellUtil.cloneValue(cell);
    }
}
