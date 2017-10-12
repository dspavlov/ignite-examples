import java.io.IOException;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ClientNode {
    public static void main(String[] args) throws IOException, InterruptedException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        ServerNode.setupCustomIp(cfg);

        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {


            final ClusterGroup grp = ignite.cluster().forServers().forRandom();
            ignite.message(grp).send("OrderStreamer", "Start");

            final IgniteCache<Integer, Object> orders = ignite.cache(ServerNode.ORDERS);
            System.out.println("Cache size is " + orders.size());

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                final int size = orders.size();
                System.out.println("Cache size is " + size);
                if (size >= 1000000)
                    break;
            }

            for (Cache.Entry<Integer, Object> next : orders) {
                if (next.getKey() % 1000 == 0)
                    System.out.println(next.getKey());
            }

            System.out.println("Press any key to close client");
            System.in.read();
        }
    }
}
