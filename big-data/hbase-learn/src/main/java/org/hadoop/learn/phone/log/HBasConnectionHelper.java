package org.hadoop.learn.phone.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HBasConnectionHelper {
    private static Configuration cfg = new Configuration();

    private static Map<String, Connection> cache = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                destroy();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        cfg.set("hbase.zookeeper.quorum", "node1,node2,node3");
    }

    public static Connection getConnection(String flag) throws IOException {
        if (cache.containsKey(flag)) {
            Connection connection = cache.get(flag);
            if (connection != null && !connection.isClosed()) {
                return connection;
            }
        }

        Connection connection = ConnectionFactory.createConnection(cfg);
        cache.put(flag, connection);
        return connection;
    }

    public static void destroy() throws IOException {
        if (!cache.isEmpty()) {
            for (Connection connection : cache.values()) {
                connection.close();
            }
            System.out.println("所有链接已关闭");
        }
    }
}
