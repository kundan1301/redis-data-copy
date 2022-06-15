import redis.clients.jedis.Jedis;
import util.ConfigReader;
import util.DestRedisPool;
import util.SourceRedisPool;

public class Main {
    public static void main(String[] args){
        //System.setProperty("config.file.path","/Users/kundankr/Downloads/redis.properties");
        //System.out.println(ConfigReader.getInstance());
        intialize();
        Migrator migrator = new Migrator();
        migrator.migrate();
    }

    public static void intialize(){
        ConfigReader.getInstance();
        Jedis jedis = SourceRedisPool.getClient();
        SourceRedisPool.returnClient(jedis);
        jedis= DestRedisPool.getClient();
        DestRedisPool.returnClient(jedis);
    }
}
