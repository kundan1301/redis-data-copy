package util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class DestRedisPool{
    private JedisPool pool;
    private static DestRedisPool destRedisPool = null;
    private DestRedisPool(){};

    public static DestRedisPool getInstance(){
        if(destRedisPool==null){
            synchronized (DestRedisPool.class){
                if(destRedisPool==null){
                    ConfigReader config = ConfigReader.getInstance();
                    GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
                    poolConfig.setMaxTotal(config.getMaxConnection());
                    poolConfig.setMaxIdle(config.getMaxConnection());
                    poolConfig.setMinIdle(config.getMinIdle());
                    destRedisPool = new DestRedisPool();
                    destRedisPool.pool = new JedisPool(poolConfig,config.getDestRedisHost(),config.getDestRedisPort());
                }
            }
        }
        return destRedisPool;
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
