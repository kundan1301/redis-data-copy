package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader{

    private int batchSize = 1;
    private String sourceRedisHost = "127.0.0.1";
    private Integer sourceRedisPort = 6379;
    private String destRedisHost = "127.0.0.1";
    private Integer destRedisPort = 6380;
    private Integer maxScan = 1;
    private Integer maxConnection = 50;
    private Integer minIdle = 20;
    private boolean useDumpAndRestore = false;

    private static final String filePathKey = "config.file.path";
    private static final String sourceRedisHostKey = "redis.source.host";
    private static final String sourceRedisPortKey = "redis.source.port";
    private static final String destRedisHostKey = "redis.dest.host";
    private static final String destRedisPortKey = "redis.dest.port";
    private static final String maxScanKey = "redis.scan.size";
    private static final String batchSizeKey = "bath.size";



    private static ConfigReader configReader = null;
    private ConfigReader(){};


    public static synchronized ConfigReader getInstance(){
        if(configReader == null){
            String filePath = System.getProperty(filePathKey);
            Properties prop = new Properties();
            if(filePath != null){
                try {
                    prop.load(new FileInputStream(filePath));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            configReader=new ConfigReader();

            configReader.sourceRedisHost = prop.getProperty(sourceRedisHostKey,configReader.sourceRedisHost);
            configReader.sourceRedisPort = Integer.parseInt(prop.getProperty(sourceRedisPortKey,configReader.sourceRedisPort+""));
            configReader.destRedisHost = prop.getProperty(destRedisHostKey,configReader.destRedisHost);
            configReader.destRedisPort = Integer.parseInt(prop.getProperty(destRedisPortKey,configReader.destRedisPort+""));
            configReader.batchSize = Integer.parseInt(prop.getProperty(batchSizeKey,configReader.batchSize+""));
            configReader.maxScan = Integer.parseInt(prop.getProperty(maxScanKey,configReader.maxScan+""));


            configReader.sourceRedisHost = System.getProperty(sourceRedisHostKey,configReader.sourceRedisHost);
            configReader.sourceRedisPort = Integer.parseInt(System.getProperty(sourceRedisPortKey,configReader.sourceRedisPort+""));
            configReader.destRedisHost = System.getProperty(destRedisHostKey,configReader.destRedisHost);
            configReader.destRedisPort = Integer.parseInt(System.getProperty(destRedisPortKey,configReader.destRedisPort+""));
            configReader.batchSize = Integer.parseInt(System.getProperty(batchSizeKey,configReader.batchSize+""));
            configReader.maxScan = Integer.parseInt(prop.getProperty(maxScanKey,configReader.maxScan+""));

        }
        return configReader;

    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getSourceRedisHost() {
        return sourceRedisHost;
    }

    public Integer getSourceRedisPort() {
        return sourceRedisPort;
    }

    public String getDestRedisHost() {
        return destRedisHost;
    }

    public Integer getDestRedisPort() {
        return destRedisPort;
    }

    public Integer getMaxScan() {
        return maxScan;
    }

    public Integer getMaxConnection() {
        return maxConnection;
    }

    public Integer getMinIdle() {
        return minIdle;
    }


    public boolean getUseDumpAndRestore() {
        return useDumpAndRestore;
    }

    @Override
    public String toString() {
        return "ConfigReader{" +
                "batchSize=" + batchSize +
                ", sourceRedisHost='" + sourceRedisHost + '\'' +
                ", sourceRedisPort=" + sourceRedisPort +
                ", destRedisHost='" + destRedisHost + '\'' +
                ", destRedisPort=" + destRedisPort +
                ", maxScan=" + maxScan +
                ", maxConnection=" + maxConnection +
                ", minIdle=" + minIdle +
                '}';
    }
}
