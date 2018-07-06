package com.taodatarobot.mydata.component;

import java.io.IOException;

public class DatabaseImporterTest {
    public static void main(String[] args) throws IOException {
        String path = "/database_sample.json";
        String truePath = DatabaseImporterTest.class.getResource(path).getPath();
        DatabaseImporter dbImporter = new DatabaseImporter(truePath);
        dbImporter.execute();
    }
}
