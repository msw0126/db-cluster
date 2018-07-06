package com.taodatarobot.mydata.component;

import org.apache.commons.cli.*;

import java.io.IOException;

public class Main {

    static class Config{
        String config;
        String type;

        Config(String config, String type){
            this.config = config;
            this.type = type;
        }

    }

    private static Config parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        // 配置文件名称
        Option configOption = new Option("c", "text", true, "config file name(String)");
        configOption.setRequired(true);
        options.addOption(configOption);
        // 导入数据的类型
        Option typeOption = new Option("t", "text",true, "import type(String)");
        typeOption.setRequired(true);
        options.addOption(typeOption);

        // 新建解析器
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        return new Config(cmd.getOptionValue("c"), cmd.getOptionValue("t"));
    }

    public static void main(String[] args) throws ParseException, IOException {
        Config config = parseArgs(args);
        if("database".equals(config.type)){
            DatabaseImporter importer = new DatabaseImporter(config.config);
            importer.execute();
        }
    }
}
