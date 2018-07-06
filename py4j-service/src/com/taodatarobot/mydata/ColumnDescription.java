package com.taodatarobot.mydata;

import com.taodatarobot.mydata.jdbcmappingImp.JdbcMapping;

import java.util.Arrays;

public class ColumnDescription {

    private String name;
    private String databaseType;
    private String[] sampleData;
    private int precision;
    private int scale;
    private String sparkDataType;

    public ColumnDescription(String name, String databaseType, int precision, int scale, int n){
        this.name = name;
        this.databaseType = databaseType;
        this.precision = precision;
        this.scale = scale;
        this.sampleData = new String[n];
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String[] getSampleData() {
        return sampleData;
    }

    public void setSampleData(String[] sampleData) {
        this.sampleData = sampleData;
    }

    public void trimSampleData(int row){
        this.sampleData = Arrays.copyOf(sampleData, row);
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getSparkDataType() {
        return sparkDataType;
    }

    public void setSparkDataType(JdbcMapping mapping) {
        this.sparkDataType = mapping.mappingToSpark(databaseType, precision, scale);
    }
}
