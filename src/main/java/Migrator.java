import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import util.ConfigReader;
import util.SourceRedisPool;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Migrator {

    public boolean migrate(){
        ScanParams scanParams = new ScanParams();
        scanParams.count(ConfigReader.getInstance().getScanCount());
        String cursor = "0";
        ScanResult<String> scanResult = null;
        Jedis jedis = SourceRedisPool.getClient();
        do {
            System.out.printf("Scanning keys, cursor is:%s\n",cursor);
            System.out.flush();
            scanResult = jedis.scan(cursor, scanParams);
            cursor=scanResult.getCursor();
            List<String> result = scanResult.getResult();
            System.out.printf("Scanning complete, migrating keys, total scanned keys count are:%d\n",result.size());
            System.out.flush();
            result.stream().map(item->(new CopyKey(item))).map(item-> CompletableFuture.runAsync(item)).collect(Collectors.toList()).stream().map(CompletableFuture::join).collect(Collectors.toList());
            System.out.println("Migration of keys complete\n");
        }while (!scanResult.isCompleteIteration()) ;
        SourceRedisPool.returnClient(jedis);
        return true;
    }






}
