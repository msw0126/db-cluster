package com.taodatarobot.mydata.component;

import org.apache.commons.cli.ParseException;

import java.io.IOException;

public class MainTest {
    public static void main(String[] args) throws IOException, ParseException {
        String path = "/database_sample.json";
        String truePath = MainTest.class.getResource(path).getPath();
        Main.main(new String[]{"-c", truePath, "-t", "database"});
    }
}
