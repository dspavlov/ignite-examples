import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.persistentstore.SnapshotFuture;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 *
 */
public class ServerNode {

    public static final String ORDERS = "Orders";
    public static final String ORDER_STREAMER_TOPIC = "OrderStreamer";

    public static void main(String[] args) throws IOException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setWorkDirectory(new File("work").getAbsolutePath());
        setupCustomIp(cfg);

        cfg.setPeerClassLoadingEnabled(true);

        setupLoadBalancing(cfg);

        cfg.setCacheConfiguration(getAcntConfiguration());

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        try (final Ignite ignite = Ignition.start(cfg)) {
            if (!ignite.active()
                && ignite.cluster().forServers().nodes().size() >= 2) {
                System.err.println("Activating cluster");
                ignite.active(true);
            }

            ignite.message(ignite.cluster().forRemotes()).localListen(
                ORDER_STREAMER_TOPIC, (uuid, e) -> {
                    initialLoad(ignite);
                    return false; // stop listening
                });
            System.out.println("Press any key to shutdown server, " + ignite.cluster().localNode().consistentId());
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

    @NotNull static CacheConfiguration<Object, Account> getAcntConfiguration() {
        final CacheConfiguration<Object, Account> acntCcfg = new CacheConfiguration<>(ClientNode.ACCOUNT);
        acntCcfg.setCacheMode(CacheMode.PARTITIONED);
        acntCcfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        acntCcfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        acntCcfg.setBackups(1);
        acntCcfg.setIndexedTypes(Integer.class, Account.class);
        return acntCcfg;
    }

    @NotNull public static IgniteConfiguration prepareIgniteConfiguration() {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        setupCustomIp(cfg);

        cfg.setPeerClassLoadingEnabled(true);

        setupLoadBalancing(cfg);
        return cfg;
    }
}
