package org.hadoop.learn.phone.log.proto;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.hadoop.learn.phone.log.HBasConnectionHelper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

/**
 * 用于数据初始化，后续的逻辑实现
 */
public class PhoneLogProtoBufSingleInitializor {

    private static final String SPLITTER = ":";
    private static final String FLAG = "init";

    private TableName tableName;
    private String tbName;
    private String namespace;
    private volatile boolean isInited = false;

    private Connection connection;

    private static final Object MUTEX = new Object();

    public PhoneLogProtoBufSingleInitializor(String tableName, String namespace) throws IOException {
        this.tableName = TableName.valueOf(namespace + SPLITTER + tableName);
        this.tbName = tableName;
        this.namespace = namespace;
        this.connection = HBasConnectionHelper.getConnection(FLAG);
    }

    /**
     * 随机产生数据，数据格式为:
     * snum: 拨打手机号码
     * rnum: 接听电话号码
     * seonds: 拨打电话时间
     * datetime：拨打时间
     */
    public void randomData() throws IOException {
        synchronized (MUTEX) {
            if (this.isInited) {
                System.err.println("已经初始化，无需再次初始化..");
                return;
            }

            init();

            // 产生数据
            this.generateData();
            this.isInited = true;
        }
    }

    private void generateData() throws IOException {
        Table table = this.connection.getTable(this.tableName);
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            String snum = getPhone();
            String rnum = getPhone();
            int seconds = random.nextInt(1000);
            Calendar calendar = getDateTime();
            String datetime = this.sdf.format(calendar.getTime());

            Put put = new Put(Bytes.toBytes(snum + "_" + calendar.getTime().getTime()));
            byte[] base = Bytes.toBytes("base");

            PhoneInfo.Builder builder1 = PhoneInfo.newBuilder();
            builder1.setRnum(rnum);
            builder1.setSnum(snum);
            builder1.setSeconds(seconds);
            builder1.setDatetime(datetime);

            put.addColumn(base, Bytes.toBytes("info"), builder1.build().toByteArray());
            table.put(put);
        }
    }

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Calendar getDateTime() {
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, random.nextInt(12));
        calendar.set(Calendar.DAY_OF_MONTH, random.nextInt(30));
        calendar.set(Calendar.HOUR_OF_DAY, random.nextInt(24));
        calendar.set(Calendar.MINUTE, random.nextInt(60));
        calendar.set(Calendar.SECOND, random.nextInt(60));


        return calendar;
    }

    private static final String[] PHONE_PREFIX = new String[]{"133", "135", "136", "139", "199", "185"};

    private String getPhone() {
        Random random = new Random();
        int idx = random.nextInt(PHONE_PREFIX.length);
        String prefix = PHONE_PREFIX[idx];

        int i = random.nextInt(100_000_000);
        String phone = prefix + i;
        if (phone.length() < 11) {
            int dur = 11 - phone.length();
            for (int j = 0; j < dur; j++) {
                phone += "0";
            }
        }
        return phone;
    }

    /**
     * 初始化数据
     */
    private void init() throws IOException {
        initNamespace();
        initTable();
    }

    private void initTable() throws IOException {
        Admin admin = connection.getAdmin();
        boolean isExists = admin.tableExists(this.tableName);
        if (!isExists) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(this.tableName);
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base")).build());
            admin.createTable(tableDescriptorBuilder.build());
        }
    }

    private void initNamespace() throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(this.namespace);
        } catch (NamespaceNotFoundException e) {
            System.err.println(String.format("%s不存在，创建namespace", this.namespace));
            admin.createNamespace(NamespaceDescriptor.create(this.namespace).build());
        }
    }
}
