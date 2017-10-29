package com.learn.big.data.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class ReadResources {

    public static String getSchema() {

        String schema = "";
        ClassLoader classLoader = ReadResources.class.getClassLoader();
        try {
            schema = IOUtils.toString(classLoader.getResourceAsStream("schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }
}
