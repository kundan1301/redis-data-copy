package util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class SourceRedisPool {
    private JedisPool pool;
    private static SourceRedisPool sourceRedisPool = null;

    private SourceRedisPool(){};

    public static SourceRedisPool getInstance(){
        if(sourceRedisPool==null){
            synchronized (SourceRedisPool.class) {
                if (sourceRedisPool == null) {
                    ConfigReader config = ConfigReader.getInstance();
                    GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
                    poolConfig.setMaxTotal(config.getMaxConnection());
                    poolConfig.setMaxIdle(config.getMaxScan());
                    poolConfig.setMinIdle(config.getMinIdle());
                    sourceRedisPool = new SourceRedisPool();
                    sourceRedisPool.pool = new JedisPool(poolConfig,config.getSourceRedisHost(),config.getSourceRedisPort());
                }
            }
        }
        return sourceRedisPool;
    }

    public JedisPool getPool() {
        return pool;
    }

    public static Jedis getClient(){
        return getInstance().getPool().getResource();
    }

    public static void returnClient(Jedis jedis){
        getInstance().getPool().returnResource(jedis);
    }


}
