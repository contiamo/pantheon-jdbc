package com.contiamo.pantheon.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

public class DriverTest {

    public static void main(String[] args) throws Exception {
        DriverManager.registerDriver(new Driver());
        String url = "jdbc:pantheon://localhost:4300/pantheon/jdbc/?catalogId=da89ec50-4460-435f-a78b-c56179dfee4c;schemaName=Foodmart;secure=false;token=s73551747a63e5f36eac4be81433e2e7f9a21eeaba5a0939cc1e194742f48bdd";
        Connection connection = DriverManager.getConnection(url);
        connection.close();
    }
}
