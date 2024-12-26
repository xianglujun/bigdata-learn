package org.hadoop.hive.learn.client;

import java.sql.*;

public class HiveClientDemo {

    private static final String CLASS = "org.apache.hive.jdbc.HiveDriver";

    static {
        try {
            Class.forName(CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.56.105:10000/mydb", "user1", "user1");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from person");
        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            names[i] = metaData.getColumnName(i + 1);
        }

        while (resultSet.next()) {
            for (int i = 0; i < names.length; i++) {
                System.out.println(names[i] + ":" + resultSet.getString(i + 1));
            }
            System.out.println("------------------------------------------------------");
        }
    }
}
