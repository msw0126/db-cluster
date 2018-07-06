package com.taodatarobot.mydata.jdbcmappingImp;

import com.taodatarobot.mydata.SparkDataType;

public class MySqlMapping extends JdbcMapping{

    private static final String driverClass = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://%s:%d/%s";
    private static final String topSelectSql = "select * from %s limit %d";
    private static final MySqlMapping mapping = new MySqlMapping();

    private MySqlMapping(){}

    static JdbcMapping create(){
        return  mapping;
    }

    @Override
    public String getDriverClass() {
        return driverClass;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String getTopSelectSql() {
        return topSelectSql;
    }

    public String mappingToSpark(String type, int precision, int scale) {
        String typeUpper = type.toUpperCase();
        switch(typeUpper){
            case "BIGINT":
                return SparkDataType.BIGINT;
            case "DECIMAL":
                if(scale==0)
                    return SparkDataType.BIGINT;
            case "DOUBLE":case "FLOAT":
                return SparkDataType.DOUBLE;
            case "INT":case "MEDIUMINT":case "SMALLINT":case "TINYINT":
                return SparkDataType.INT;
            case "CHAR":case "LONGTEXT":case "MEDIUMTEXT": case "TEXT": case "TINYTEXT": case "VARCHAR":
                return SparkDataType.STRING;
            case "DATE": case "YEAR":
                return SparkDataType.DATE;
            case "DATETIME":
                return SparkDataType.TIME_STAMP;
            default:
                return SparkDataType.UNSUPPORT;
        }
    }
}
