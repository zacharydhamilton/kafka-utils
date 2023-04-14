package com.github.zacharydhamilton.kafka.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class Utils {
    /**
     * Check for the necessary configurations to initialize. If any are missing, fails. 
     * 
     * @param configuredProps - A Properties object containing the 
     * @param requiredProps - An ArrayList of Strings representing the required properties to check for. 
     * @throws ConfigException
     */
    public static void preInitChecks(Properties configuredProps, ArrayList<String> requiredProps) throws ConfigException {
        ArrayList<String> missingProps = new ArrayList<String>();
        for (String prop : requiredProps) {
            if (configuredProps.get(prop).equals(null)) {
                missingProps.add(prop);
            }
        }
        if (missingProps.size() > 0) {
            throw new ConfigException("Missing required properties: " + missingProps.toString());
        }
    }
    
    /**
     * Load properties from an application properties file.
     * 
     * @param props - An existing Properties object to add the properties to.
     * @param file - An existing file containing properties to add to the Properties object. 
     * @throws IOException
     */
    public static void addPropsFromFile(Properties props, String file) throws IOException {
        if (!Files.exists(Paths.get(file))) {
            throw new IOException("Config file (" + file + ") does not exist or was not found.");
        }
        try (InputStream inputStream = new FileInputStream(file)) {
            props.load(inputStream);
        }
    }

    /**
     * 
     * @param props - An existing Properties object to add the properties to.
     * @param assertedProps - An ArrayList of Strings representing the required properties to add.
     * @throws ConfigException
     */
    public static void addPropsFromEnv(Properties props, ArrayList<String> requiredProps) throws ConfigException {
        String kafkaKey = "";
        String kafkaSecret = "";
        String srKey = "";
        String srSecret = "";
        for (String prop : requiredProps) {
            switch (prop) {
                case "BOOTSTRAP_SERVERS":
                    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, System.getenv(prop));
                    break;
                case "KAFKA_KEY": 
                    kafkaKey = System.getenv(prop);
                    break;
                case "KAFKA_SECRET": 
                    kafkaSecret = System.getenv(prop);
                case "SCHEMA_REGISTRY_URL":
                    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv(prop));
                    break;
                case "SCHEMA_REGISTRY_KEY":
                    srKey = System.getenv(prop);
                    break;
                case "SCHEMA_REGISTRY_SECRET":
                    srSecret = System.getenv(prop);
                default: 
                    throw new ConfigException("Unsupported or unknown config: " + prop);
            }
        }
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule   required username='"+kafkaKey+"'   password='"+kafkaSecret+"';");
        props.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srKey+":"+srSecret);
    }
}
