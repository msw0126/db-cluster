package com.taodatarobot.mydata.jdbcmappingImp;

import com.taodatarobot.mydata.SparkDataType;

public class OracleMapping extends JdbcMapping{

    private static final String driverClass = "oracle.jdbc.driver.OracleDriver";
    private static final String url = "jdbc:oracle:thin:@%s:%d:%s";
    private static final String topSelectSql = "select * from \"%s\" where rownum <= %d";
    private static final OracleMapping mapping = new OracleMapping();

    private OracleMapping(){}

    static JdbcMapping create(){
        return mapping;
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

    @Override
    public String mappingToSpark(String type, int precision, int scale) {
        String typeUpper = type.toUpperCase();
        switch(typeUpper){
            case "BIGINT":case "INT":case "SMALLINT":
                return SparkDataType.BIGINT;
            case "NUMBER":
                if(scale==0)
                    return SparkDataType.BIGINT;
            case "DOUBLE":case "FLOAT": case "REAL":
                return SparkDataType.DOUBLE;
            case "CHAR":case "VARCHAR2": case "VARCHAR":case "NCHAR":case "NVARCHAR2": case "NVARCHAR":
                return SparkDataType.STRING;
            case "DATE":
                return SparkDataType.DATE;
            default:
                return SparkDataType.UNSUPPORT;
        }
    }

    @Override
    public boolean schemaPattern() {
        return true;
    }
}
