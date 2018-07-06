package com.taodatarobot.mydata.component.bean;

import java.util.HashMap;
import java.util.List;

public class DatabaseImportConfig {
    private String dbType;
    private String url;
    private String user;
    private String password;
    private String table;
    private List<DataType> schema;
    private String destPath;

    private static final HashMap<String,String> FIELD_DECORATOR = new HashMap<>();
    static {
        FIELD_DECORATOR.put("oracle", "\"");
        FIELD_DECORATOR.put("mysql", "`");
    }

    private static final HashMap<String,String> TABLE_DECORATOR = new HashMap<>();
    static {
        TABLE_DECORATOR.put("oracle", "\"");
        TABLE_DECORATOR.put("mysql", "");
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<DataType> getSchema() {
        return schema;
    }

    public void setSchema(List<DataType> schema) {
        this.schema = schema;
    }

    public String getDestPath() {
        return destPath;
    }

    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }

    public String getFieldDecorator(){
        return FIELD_DECORATOR.get(dbType);
    }

    public String getTableDecorator(){
        return TABLE_DECORATOR.get(dbType);
    }
}
