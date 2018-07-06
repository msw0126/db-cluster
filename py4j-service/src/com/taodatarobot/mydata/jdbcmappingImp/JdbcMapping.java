package com.taodatarobot.mydata.jdbcmappingImp;

import com.taodatarobot.mydata.SparkDataType;

import java.security.InvalidParameterException;
import java.util.Objects;

public abstract class JdbcMapping {

    public abstract String getDriverClass();
    public abstract String getUrl();
    public abstract String getTopSelectSql();
    public abstract String mappingToSpark(String type, int precision, int scale);
    public boolean schemaPattern(){return false;};

    public String getConnectionUrl(String ip, int port, String db){
        return String.format(getUrl(), ip, port, db);
    }

    public String topN(String table, int n){
        return String.format(getTopSelectSql(), table, n);
    }

    public static JdbcMapping get(String dbType){
        JdbcMapping driver = null;
        switch(dbType){
            case "mysql":
                driver = MySqlMapping.create();
                break;
            case "oracle":
                driver = OracleMapping.create();
                break;
            default:
                throw new InvalidParameterException(dbType + " is not supported!");
        }
        return driver;
    }

    public static void main(String[] args) {
        JdbcMapping map = get("mysql");
        assert Objects.equals(map.mappingToSpark("decimal", 1, 0), SparkDataType.BIGINT);
        assert Objects.equals(map.mappingToSpark("decimal", 1, 1), SparkDataType.DOUBLE);
        assert Objects.equals(map.mappingToSpark("varchar", 1, 0), SparkDataType.STRING);
        assert Objects.equals(map.mappingToSpark("blob", 1, 0), SparkDataType.UNSUPPORT);
        assert Objects.equals(map.mappingToSpark("date", 1, 0), SparkDataType.DATE);
        assert Objects.equals(map.mappingToSpark("year", 1, 0), SparkDataType.DATE);
        assert Objects.equals(map.mappingToSpark("tinytext", 1, 0), SparkDataType.STRING);
        assert Objects.equals(map.mappingToSpark("float", 1, 0), SparkDataType.DOUBLE);
        assert Objects.equals(map.mappingToSpark("tinyint", 1, 0), SparkDataType.INT);
    }
}
