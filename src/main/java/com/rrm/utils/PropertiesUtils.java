package com.rrm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtils {
    final static Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);
    public static String readProperty(String property)  {
        try {
            ClassPathResource resource = new ClassPathResource("application.properties");
            //ClassPathResource resource = new ClassPathResource("config/jdbc.properties");
            Properties properties= PropertiesLoaderUtils.loadProperties(resource);
            return properties.getProperty(property);
        } catch (IOException e) {
            logger.error("读取配置文件错误");
        }
        return "";
    }
}
