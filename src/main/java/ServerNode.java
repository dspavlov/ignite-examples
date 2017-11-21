import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;

import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ServerNode {

    public static final String ORDERS = "Orders";
    public static final String ORDER_STREAMER_TOPIC = "OrderStreamer";

    public static void main(String[] args) throws IOException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        setupCustomIp(cfg);

        setupLoadBalancing(cfg);

        final CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(ORDERS);
        cfg.setCacheConfiguration(ccfg);

        try (final Ignite ignite = Ignition.start(cfg)) {
            ignite.events().localListen(evt -> {
                final CacheEvent evt1 = (CacheEvent)evt;
                final Object key = evt1.key();
                System.out.println("expired key " + key);
                //do processing
                return true;
            }, EventType.EVT_CACHE_OBJECT_EXPIRED);

            ignite.message(ignite.cluster().forRemotes()).localListen(
               ORDER_STREAMER_TOPIC, (uuid, e)->{
                    initialLoad(ignite);
                    return false; // stop listening
                });
            System.out.println("Press any key to shutdown server");
            System.in.read();
        }
    }

    public static void setupLoadBalancing(IgniteConfiguration cfg) {
        final RoundRobinLoadBalancingSpi loadBalancingSpi = new RoundRobinLoadBalancingSpi();
        loadBalancingSpi.setPerTask(true);
        cfg.setLoadBalancingSpi(loadBalancingSpi);

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
    }

    private static void initialLoad(Ignite ignite) {
        final ThreadLocalRandom tlr = ThreadLocalRandom.current();
        if (ignite.cache(ORDERS).size() == 0) {
            final IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(ORDERS);
            IntStream.range(0, 1000000)
                .forEach(
                    i -> {
                        streamer.addData(i, "Order-" + i + "-" + tlr.nextInt());
                    }
                );
            streamer.close();
        }
    }

    public static void setupCustomIp(IgniteConfiguration cfg) {
        final TcpDiscoverySpi spi = new TcpDiscoverySpi();
        final TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.setAddresses(Collections.singletonList("127.0.0.1:33333..33339"));
        spi.setIpFinder(finder);
        spi.setLocalPort(33333);
        spi.setLocalPortRange(6);
        cfg.setDiscoverySpi(spi);
    }
}
