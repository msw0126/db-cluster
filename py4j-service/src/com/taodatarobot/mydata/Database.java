package com.taodatarobot.mydata;

import com.taodatarobot.mydata.jdbcmappingImp.JdbcMapping;
import com.taodatarobot.mydata.jdbcmappingImp.MySqlMapping;
import com.taodatarobot.mydata.jdbcmappingImp.OracleMapping;

import java.security.InvalidParameterException;
import java.sql.*;
import java.util.*;

public class Database {

    private static final String[] LIST_TABLE_TYPES = {"TABLE"};

    public static Connection connect(String dbType, String ip, int port, String db, String user, String password) throws ClassNotFoundException, SQLException {
        JdbcMapping driver = JdbcMapping.get(dbType);
        Class.forName(driver.getDriverClass());
        String url = driver.getConnectionUrl(ip,port, db);
        return DriverManager.getConnection(url, user, password);
    }


    public static List<String> listDatabaseTable(String dbType, String ip, int port, String db, String user, String password) throws SQLException, ClassNotFoundException {
        JdbcMapping driver = JdbcMapping.get(dbType);
        try (Connection conn = connect(dbType, ip, port, db, user, password)){
            List<String> tables = new ArrayList<>();
            DatabaseMetaData meta = conn.getMetaData();
            String schemaPattern = driver.schemaPattern()? user:null;
            ResultSet res = meta.getTables(null, schemaPattern,null,LIST_TABLE_TYPES);
            while(res.next()){
                tables.add(res.getString("TABLE_NAME"));
            }
            return tables;
        }
    }

    public static List<String> queryDatabaseTable(String dbType, String ip, int port, String db, String user, String password, String table) throws SQLException, ClassNotFoundException {
//        JdbcMapping driver = JdbcMapping.get(dbType);
        try (Connection conn = connect(dbType, ip, port, db, user, password)){
            List<String> tables = new ArrayList<>();
            Statement stmt = conn.createStatement();
            if(dbType == "mysql"){
                String sql = String.format("show tables like '%%%s%%'", table);
                stmt.executeQuery(sql);
            }else if(dbType == "oracle"){
                String sql = String.format("select table_name,tablespace_name,temporary from user_tables where table_name like '%%%s%%'", table);
                stmt.executeQuery(sql);
            }
            ResultSet resultSet = stmt.getResultSet();
            while(resultSet.next()){
                String tableName = resultSet.getString(1);
                tables.add(tableName);
            }
            return tables;
        }
    }

    public static LinkedHashMap<String, List<String>>  viewDatabaseTable(String dbType, String ip, int port, String db, String user, String password, String table, int n) throws SQLException, ClassNotFoundException {
//        Connection conn = connect(dbType, ip, port, db, user, password);
        LinkedHashMap<String, List<String>> result = new LinkedHashMap<>();
        Statement statement = null;
        try (Connection conn = connect(dbType, ip, port, db, user, password)){
            statement = conn.createStatement();
            if(dbType == "mysql"){
                statement.execute(String.format("select * from %s limit %d", table, n));
            }else if(dbType == "oracle"){
                statement.execute(String.format("select * from %s where rownum <= %d", table, n));
            }
//            statement.execute(String.format("select * from %s limit %d", table, n));
//            statement.execute(String.format("select * from %s where rownum <= %d", table, n));
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnNum = metaData.getColumnCount();

            for (int i = 0; i < columnNum; i++) {
                String columnName = metaData.getColumnName(i + 1);
                result.put(columnName, new ArrayList<>());
            }

            while (resultSet.next()) {
                for (String column : result.keySet()) {
                    String v = "" + resultSet.getObject(column);
                    result.get(column).add(v);
                }
            }
            LinkedHashMap<String, List<String>> resultFinal = new LinkedHashMap<>();
            for (String column : result.keySet()) {
                String[] col = column.split("\\.");
                resultFinal.put(col[col.length - 1], result.get(column));
            }
            return resultFinal;

        }
    }

    public static ColumnDescription[] preview(String dbType, String ip, int port, String db, String user, String password, String table, int n) throws SQLException, ClassNotFoundException {
        try (Connection conn = connect(dbType, ip, port, db, user, password)){
            JdbcMapping driver = JdbcMapping.get(dbType);
            String topNSelectSql = driver.topN(table, n);
            Statement statement = conn.createStatement();
            ResultSet res = statement.executeQuery(topNSelectSql);
            ResultSetMetaData meta = res.getMetaData();

            int columns = meta.getColumnCount();
            ColumnDescription[] descriptions = new ColumnDescription[columns];
            for(int i=1;i<=columns;i++){
                ColumnDescription description = new ColumnDescription(meta.getColumnName(i),
                        meta.getColumnTypeName(i),
                        meta.getPrecision(i),
                        meta.getScale(i),
                        n);
                description.setSparkDataType(driver);
                descriptions[i-1] = description;
            }

            int row = 0;
            while(res.next()){
                for(int i=1;i<=columns;i++){
                    descriptions[i-1].getSampleData()[row] = res.getString(i);
                }
                row++;
            }

            if(row<n){
                for(ColumnDescription description : descriptions){
                    description.trimSampleData(row);
                }
            }
            return descriptions;
        }
    }

    public static void main(String[] args) throws Exception{
//        List<String> list = listDatabaseTable("mysql", "192.168.1.150", 3306, "databrain", "root", "taoshu12345");
//        System.out.println(list);
//
//        List<String> list1 = queryDatabaseTable("mysql", "192.168.1.150", 3306, "databrain", "root", "taoshu12345", "user");
//        System.out.println(list1);
//
//        LinkedHashMap<String, List<String>> list2 = viewDatabaseTable("mysql", "192.168.1.150", 3306, "databrain", "root", "taoshu12345", "sys_role_user", 10 );
//        System.out.println(list2);
//
//        ColumnDescription[] descriptions = preview("mysql", "192.168.1.150",3306, "databrain","root","taoshu12345", "sys_role_user", 10);
//        for (ColumnDescription description : descriptions){
//            System.out.println(String.format("('%s','%s')", description.getName(), description.getSparkDataType()));
//        }




        List<String> list1 = listDatabaseTable("oracle", "127.0.0.1", 1521, "orcl", "SYSTEM", "taoshu12345");
        System.out.println(list1);

        List<String> list = queryDatabaseTable("oracle", "127.0.0.1", 1521, "orcl", "SYSTEM", "taoshu12345", "QUEUE");
        System.out.println(list);

        LinkedHashMap<String, List<String>> list2 = viewDatabaseTable("oracle", "127.0.0.1", 1521, "orcl", "SYSTEM", "taoshu12345", "AQ$_QUEUES", 10 );
        System.out.println(list2);

        ColumnDescription[] descriptions = preview("oracle", "127.0.0.1",1521, "orcl","SYSTEM","taoshu12345", "AQ$_QUEUES", 10);
        for (ColumnDescription description : descriptions){
            System.out.println(String.format("('%s','%s')", description.getName(), description.getSparkDataType()));
        }

    }
}
