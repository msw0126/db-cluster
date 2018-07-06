package com.taodatarobot.mydata.component.bean;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DatabaseImportConfigTest {

    private void makeTestFile(){
        DatabaseImportConfig config = new DatabaseImportConfig();
        config.setDbType("mysql");
        config.setUser("root");
        config.setPassword("111111");
        config.setUrl("jdbc:mysql://192.168.1.192:3306/hello");
        config.setTable("train");
        config.setDestPath("/test");

        List<DataType> schema = new ArrayList<>();
        String schemaStr = "id,string|Target,string|AccountStatus,string|DurationInMonth,bigint|CreditHistory,string|Purpose,string|CreditAmount,double|SavingsAccount,string|Employment,string|InstallmentRate,double|PersonalStatus,string|OtherDebtors,string|PresentResidenceSince,double|Property,string|Age,int|OtherInstallmentPlans,string|Housing,string|NumberOfCreditsInBank,double|Job,string|NumberOfPeopleMaintenance,double|Telephone,string|ForeignWorker,string|year1,date";
        for(String data : schemaStr.split("\\|")){
            String[] nameValue = data.split(",");
            DataType tp = new DataType();
            tp.setField(nameValue[0]);
            tp.setSparkType(nameValue[1]);
            schema.add(tp);
        }
        config.setSchema(schema);
        System.out.println(JSON.toJSONString(config,true));
    }

    private void loadConfig() throws Exception{
        String path = "/database_sample.json";
        String truePath = DatabaseImportConfigTest.class.getResource(path).getPath();
        String json = FileUtils.readFileToString(new File(truePath));
        DatabaseImportConfig config = JSON.parseObject(json, DatabaseImportConfig.class);
        assert config.getSchema().size() == 23;
    }

    public static void main(String[] args) throws Exception {
        DatabaseImportConfigTest test = new DatabaseImportConfigTest();
        test.makeTestFile();
        test.loadConfig();
    }

}
