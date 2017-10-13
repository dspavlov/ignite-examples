import java.io.IOException;
import java.util.TreeMap;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ClientNode {
    public static void main(String[] args) throws IOException, InterruptedException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        ServerNode.setupCustomIp(cfg);

        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {

            final IgniteCache<Integer, Developer> developers = ignite.getOrCreateCache(ServerNode.DEVELOPER);
            developers.putAll(new TreeMap<Integer, Developer>() {{
                put(3, new Developer("Ivan"));
                put(4, new Developer("Egor", "Middle"));
            }});

            for(Cache.Entry<Integer, Developer> dev: developers) {
                System.out.println("Developer: " + dev);
            }

             /*
            int requiredSize = 1000000;
            final IgniteCache<Object, Object> orders = ignite.cache(ServerNode.ORDERS);
            System.out.println("Cache size is " + orders.size());

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                final int size = orders.size();
                System.out.println("Cache size is " + size);
                if (size >= requiredSize)
                    break;
            }*/

            System.out.println("Press any key to close client");
            System.in.read();
        }
    }
}
