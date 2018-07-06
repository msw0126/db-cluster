package com.taodatarobot.mydata;

import py4j.GatewayServer;

import org.apache.log4j.Logger;
import py4j.GatewayServer;

public class Entry {
//    static Logger logger = Logger.getLogger(Database.class.getName());
    private Database Database = new Database();
    public Database getDatabase() {
        return Database;
    }
    public static void main(String[] args) {
        Entry entry = new Entry();
        GatewayServer server = new GatewayServer(entry);
        server.start();
    }
}
