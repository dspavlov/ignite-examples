import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ClientNode {

    private static void runMapReduce(Ignite ignite) {
        final ClusterGroup grp = ignite.cluster().forServers();
        long base = 1_000_000_000;
        final List<BigInteger> parms = LongStream.range(base, base + 10_000)
            .mapToObj(Long::toString).map(BigInteger::new).collect(Collectors.toList());

        final BigInteger res = ignite.compute(grp).apply((i) -> {
            final boolean prime = i.isProbablePrime(64);

            if (prime)
                System.err.println("Found prime number: " + i);
            else
                System.err.println("Not prime " + i);

            return prime ? i : null;
        }, parms, new IgniteReducer<BigInteger, BigInteger>() {
            private BigInteger sum = BigInteger.ZERO;

            @Override public boolean collect(@Nullable BigInteger integer) {
                if (integer == null)
                    return true;
                System.err.println("Reduce for found prime number: " + integer);
                sum = sum.add(integer);
                return true;
            }

            @Override public BigInteger reduce() {
                return sum;
            }
        });
        System.out.println("Sum of primaries " + res);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        ServerNode.setupCustomIp(cfg);

        ServerNode.setupLoadBalancing(cfg);

        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {

            final Stream<Future<Void>> stream = IntStream.range(0, 10_000).mapToObj(
                i -> {
                    final Callable<Void> callable = () -> {
                        runBinaryBuild(ignite);
                        return null;
                    };
                    return ForkJoinPool.commonPool().submit(callable);
                }
            );

            stream.forEach(task -> {
                try {
                    task.get();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            runMapReduce(ignite);

            final ClusterGroup grp = ignite.cluster().forServers().forRandom();
            ignite.message(grp).send("OrderStreamer", "Start");

            ignite.events().localListen(e -> {
                System.out.println("Node joined: " + e);
                return true;
            }, EVT_NODE_JOINED);

            final IgniteCache<Object, Object> orders = ignite.cache(ServerNode.ORDERS);
            System.out.println("Cache size is " + orders.size());

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                final int size = orders.size();
                System.out.println("Cache size is " + size);
                if (size >= 1000000)
                    break;
            }

            System.out.println("Press any key to close client");
            System.in.read();
        }
    }

    static boolean debug = true;

    private static BinaryObject runBinaryBuild(Ignite ignite) {
        BinaryObjectBuilder bob = ignite.binary().builder("CustomType");
        final Map<String, String> input = new HashMap<>();
        input.put("field0", "value1");
        input.put("field1", "value2");
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            bob.setField(entry.getKey(), entry.getValue());

            if (debug) {
                //System.out.println("Thread: " + entry.getKey() + ":" + entry.getValue());
            }
        }

        BinaryObject bo = bob.build();
        if (debug)
            System.out.println("Thread: " + Thread.currentThread().getName() + ", object: " + bo);
        return bo;
    }

}
