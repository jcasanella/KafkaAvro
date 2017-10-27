package com.learn.big.data.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class SomeUtils {

    public String getSchema() {

        String schema = "";
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            schema = IOUtils.toString(classLoader.getResourceAsStream("schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }
}
