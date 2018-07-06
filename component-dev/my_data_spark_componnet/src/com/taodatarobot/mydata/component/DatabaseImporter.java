package com.taodatarobot.mydata.component;

import com.alibaba.fastjson.JSON;
import com.taodatarobot.mydata.component.bean.DataType;
import com.taodatarobot.mydata.component.bean.DatabaseImportConfig;
import com.taodatarobot.mydata.component.util.Util;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DatabaseImporter {

    private String configPath;

    public DatabaseImporter(String configPath){
        this.configPath = configPath;
    }

    /**
     *  加载配置文件为配置类实例
     * @return 解析后的配置类实例
     * @throws IOException 配置文件不存在
     */
    private DatabaseImportConfig loadConfig() throws IOException {
        System.out.println("importing config");
        String json = FileUtils.readFileToString(new File(configPath));
        DatabaseImportConfig config = JSON.parseObject(json, DatabaseImportConfig.class);
        System.out.println("config imported:");
        System.out.println(json);
        return config;
    }

    /**
     * 从数据库中读取数据，并转换为指定的数据类型
     * @param sc SPARK SQLContext
     * @param config 读取配置，包含数据库链接，表，用户名，密码，字段对应的SPARK数据类型
     * @return 数据
     */
    private Dataset getDataFrameFromDb(SQLContext sc, DatabaseImportConfig config){
        // oracle 12c 中使用 "field", mysql5.7中使用 `field`
        String fieldDecorator = config.getFieldDecorator();
        // oracle 12c 中使用 "table", mysql5.7中使用 table
        String tableDecorator = config.getTableDecorator();
        List<String> selectList = new ArrayList<>();
        Map<String, String> targetSchema = new HashMap<>(); // 目标数据类型字典
        for(DataType dataType : config.getSchema()){
            String field = dataType.getField();
            String sparkType = dataType.getSparkType();
            selectList.add(String.format("%s%s%s", fieldDecorator, field, fieldDecorator));
            targetSchema.put(field, sparkType);
        }
        String table = config.getTable();
        String url = config.getUrl();
        String selectFields = StringUtils.join(selectList,',');
        String selectSql = String.format("(select %s from %s%s%s) %s", selectFields, tableDecorator, table, tableDecorator, table);

        // 指定用户名、密码
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        properties.setProperty("password", config.getPassword());

        // 读取数据
        System.out.println("using sql:");
        System.out.println(selectSql);
        Dataset data = sc.read().jdbc(url, selectSql, properties);
        System.out.println("data imported:");
        data.show();
        System.out.println("the initial schema of data is:");
        data.printSchema();
        // 数据类型转换
        data = transform(data, targetSchema);
        System.out.println("data transformed:");
        System.out.println("the transformed schema of data is:");
        data.printSchema();
        return data;
    }

    /**
     * 转换data set的数据类型为指定数据类型
     * @param data 数据
     * @param targetSchema 目标数据类型
     * @return 转换后的data set
     */
    private Dataset transform(Dataset data, Map<String, String> targetSchema){
        int columnNum = targetSchema.size();
        String[] selectFields = new String[columnNum];
        StructField[] fields = data.schema().fields();
        int convertNum = 0; // 标记，需要进行转换的列数量
        for(int i=0;i<columnNum;i++){
            StructField field = fields[i];
            String fieldName = field.name();
            String targetType = targetSchema.get(fieldName);
            if(Objects.equals(field.dataType().typeName(), targetType)){
                // 类型相同，不需要转换
                selectFields[i] = String.format("`%s`", fieldName);
            }else{
                // 类型不同，进行转换
                selectFields[i] = String.format("cast(`%s` as %s) as `%s`",
                        fieldName, targetType, fieldName);
                convertNum ++;
            }
        }
        // 不需要转换，返回原数据
        if(convertNum==0) return data;
        return data.selectExpr(selectFields);
    }

    /**
     * 导出data set到parquet
     * 如果目标路径已存在，会覆盖
     * @param data 数据
     * @param config 配置，包含目标路径
     */
    private void export(Dataset data, DatabaseImportConfig config){
        String destPath = config.getDestPath();
        System.out.println(String.format("exporting data as parquet to %s", destPath));
        data.write().mode(SaveMode.Overwrite).parquet(destPath);
    }

    /**
     * 执行数据导入
     * @throws IOException 配置文件不存在
     */
    public void execute() throws IOException {
        DatabaseImportConfig config = loadConfig();
        SQLContext spark = Util.getSQLContext();
        Dataset dataset = getDataFrameFromDb(spark, config);
        export(dataset, config);
    }
}
