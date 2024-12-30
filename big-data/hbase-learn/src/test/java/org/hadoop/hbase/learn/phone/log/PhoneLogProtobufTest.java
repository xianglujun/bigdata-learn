package org.hadoop.hbase.learn.phone.log;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.hadoop.learn.phone.log.HBasConnectionHelper;
import org.hadoop.learn.phone.log.proto.PhoneInfo;
import org.hadoop.learn.phone.log.proto.PhoneLogProtoBufSingleInitializor;
import org.junit.Test;

import java.io.IOException;

public class PhoneLogProtobufTest {

    private static final String FLAG = "phone_log_test";
    private TableName tableName = TableName.valueOf("phone_log_info:phone_log");

    @Test
    public void init() throws IOException {
        PhoneLogProtoBufSingleInitializor initializor = new PhoneLogProtoBufSingleInitializor("phone_log_info", "phone_log");
        initializor.randomData();
    }

    /**
     * 查询通话在3月份的所有通话记录
     */
    @Test
    public void query() throws IOException {
        Scan scan = new Scan();
        Connection connection = HBasConnectionHelper.getConnection(FLAG);
        Table table = connection.getTable(this.tableName);
        ResultScanner scanner = table.getScanner(scan);
        this.printResult(scanner);
    }

    private void printResult(ResultScanner scanner) throws InvalidProtocolBufferException {
        for (Result result : scanner) {
            this.printResult(result);
        }
    }

    private void printResult(Result result) throws InvalidProtocolBufferException {
        byte[] bases = Bytes.toBytes("base");
        byte[] infos = getBytes(result, bases, "info");
        PhoneInfo info = PhoneInfo.parseFrom(infos);
        System.out.println(info.getSnum() + "\t" + info.getRnum() + "\t" + info.getSeconds() + "\t" + info.getDatetime());
    }

    private static byte[] getBytes(Result result, byte[] bases, String column) {
        Cell cell = result.getColumnLatestCell(bases, Bytes.toBytes(column));
        return CellUtil.cloneValue(cell);
    }
}
