import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.RestoreParams;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;
import util.ConfigReader;
import util.DestRedisPool;
import util.SourceRedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyKey implements Runnable {
    private String key;

    public CopyKey(String key) {
        this.key = key;
    }

    @Override
    public void run() {
        deleteKey();
        boolean useDumpAndRestore = ConfigReader.getInstance().getUseDumpAndRestore();
        boolean dumpAndRestore = false;
        if(useDumpAndRestore) dumpAndRestore = dumpAndRestore();
        if(dumpAndRestore){
            copyTTL();
            return;
        }

        Jedis jedis = SourceRedisPool.getInstance().getPool().getResource();
        String type = jedis.type(this.key);
        SourceRedisPool.getInstance().getPool().returnResource(jedis);
        long startMilli = System.currentTimeMillis();
        //System.out.printf("Starting migration of key: %s, type: %s \n", this.key,type);
        //System.out.flush();
        switch (type) {
            case "string":
                migrateString();
                break;
            case "set":
                migrateSet();
                break;
            case "zset":
                migrateSortedSet();
                break;
            case "list":
                migrateList();
                break;
            case "hash":
                migrateHash();
                break;
            default:
                dumpAndRestore();
        }
        //System.out.printf("migration of key: %s, type: %s completed. Time taken in millis: %d \n", this.key,type, (System.currentTimeMillis()-startMilli));
        copyTTL();
    }

    public boolean migrateString() {
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        String value = sourceJedis.get(key);
        String res = destJedis.set(key, value);
        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }

    public boolean migrateList() {
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        int batchSize = ConfigReader.getInstance().getBatchSize();
        List<String> res = new ArrayList<>();
        int start = 0;
        do {
            res = sourceJedis.lrange(key, start, start + batchSize - 1);
            if(res.size()>0)  destJedis.rpush(key, res.toArray(new String[0]));
            start = start + batchSize;
        } while (res.size() > 0);

        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }

    public boolean migrateSet() {
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        int batchSize = ConfigReader.getInstance().getBatchSize();

        ScanParams scanParams = new ScanParams();
        scanParams.count(batchSize);
        String cursor = "0";
        ScanResult<String> scanResult;
        do {
            scanResult = sourceJedis.sscan(key, cursor, scanParams);
            cursor = scanResult.getCursor();
            List<String> result = scanResult.getResult();
            if(result.size()> 0) destJedis.sadd(key, result.toArray(new String[0]));
        } while (!scanResult.isCompleteIteration());

        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }

    public boolean migrateSortedSet() {
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        int batchSize = ConfigReader.getInstance().getBatchSize();

        ScanParams scanParams = new ScanParams();
        scanParams.count(batchSize);
        String cursor = "0";
        ScanResult<Tuple> scanResult;
        do {
            scanResult = sourceJedis.zscan(key, cursor, scanParams);
            cursor = scanResult.getCursor();
            List<Tuple> result = scanResult.getResult();
            Map<String, Double> map = new HashMap<>();
            for (Tuple t : result) {
                map.put(t.getElement(), t.getScore());
            }
            if(map.size() > 0) destJedis.zadd(key, map);
        } while (!scanResult.isCompleteIteration());

        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }

    public boolean migrateHash() {
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        int batchSize = ConfigReader.getInstance().getBatchSize();

        ScanParams scanParams = new ScanParams();
        scanParams.count(batchSize);
        String cursor = "0";
        ScanResult<Map.Entry<String, String>> scanResult;
        do {
            scanResult = sourceJedis.hscan(key, cursor, scanParams);
            cursor = scanResult.getCursor();
            List<Map.Entry<String, String>> result = scanResult.getResult();
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, String> res : result) {
                map.put(res.getKey(), res.getValue());
            }
            if(map.size()>0) destJedis.hmset(key, map);
        } while (!scanResult.isCompleteIteration());

        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }

    public boolean dumpAndRestore() {
        long start = System.currentTimeMillis();
        //System.out.println("Dumping key: "+this.key);
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        RestoreParams restoreParams = new RestoreParams();
        restoreParams=restoreParams.replace();
        byte[] res = sourceJedis.dump(key);
        //System.out.println("Restoring key: "+this.key);
        String reply = destJedis.restore(key, 0, res,restoreParams);
        //System.out.printf("Dump and restore complete key: %s, time taken %d \n", this.key, (System.currentTimeMillis()-start));
        //System.out.flush();
        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return "OK".equals(reply);
    }

    public void copyTTL(){
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        long ttl = sourceJedis.ttl(this.key);
        if(ttl > 0) {
            destJedis.expire(this.key, ttl);
            //System.out.println("ttl set for key: " + this.key);
        }
        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
    }

    public void deleteKey(){
        Jedis destJedis = DestRedisPool.getClient();
        destJedis.del(this.key);
        DestRedisPool.returnClient(destJedis);
    }

}
