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
        boolean useDumpAndRestore = ConfigReader.getInstance().getUseDumpAndRestore();
        if(useDumpAndRestore){
            dumpAndRestore();
            return;
        }
        Jedis jedis = SourceRedisPool.getInstance().getPool().getResource();
        String type = jedis.type(this.key);
        SourceRedisPool.getInstance().getPool().returnResource(jedis);
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
        Jedis sourceJedis = SourceRedisPool.getClient();
        Jedis destJedis = DestRedisPool.getClient();
        RestoreParams restoreParams = new RestoreParams();
        restoreParams=restoreParams.replace();
        byte[] res = sourceJedis.dump(key);
        destJedis.restore(key, 0, res,restoreParams);
        SourceRedisPool.returnClient(sourceJedis);
        DestRedisPool.returnClient(destJedis);
        return true;
    }
}
