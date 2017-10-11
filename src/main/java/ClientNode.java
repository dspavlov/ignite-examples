import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

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

            final IgniteCache<Object, Object> orders = ignite.cache(ServerNode.ORDERS);
            System.out.println("Cache size is " + orders.size());

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                final int size = orders.size();
                System.out.println("Cache size is " + size);
                if (size >= 1000000)
                    break;
            }

            final int partitions = ignite.affinity(ServerNode.ORDERS).partitions();
            final List<Integer> parts = IntStream.range(0, partitions).boxed().collect(Collectors.toList());
            final Integer result = ignite.compute().apply((p) -> {
                System.err.println("Running job for partition " + p);
                return p;
            }, parts, new IgniteReducer<Integer, Integer>() {
                public int cur;

                @Override public boolean collect(@Nullable Integer integer) {
                    cur = Math.max(integer, cur);
                    return true;
                }

                @Override public Integer reduce() {
                    return cur;
                }
            });
            System.out.println("Max partition " + result);

            System.out.println("Press any key to close client");
            System.in.read();
        }
    }
}
