import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ServerNode {

    public static final String ORDERS = "Orders";
    public static final String ORDER_STREAMER_TOPIC = "OrderStreamer";

    public static void main(String[] args) throws IOException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        setupCustomIp(cfg);

        final CacheConfiguration c2 = new CacheConfiguration();
        c2.setName(ORDERS);
        cfg.setCacheConfiguration(c2);
        try (final Ignite ignite = Ignition.start(cfg)) {

            initialLoad(ignite);

            System.out.println("Press any key to shutdown server");
            System.in.read();
        }
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
        finder.setAddresses(Collections.singletonList("127.0.0.1:33333"));
        spi.setIpFinder(finder);
        spi.setLocalPort(33333);
        cfg.setDiscoverySpi(spi);
    }
}
