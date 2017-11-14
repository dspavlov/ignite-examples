import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.persistentstore.GridSnapshot;
import org.gridgain.grid.persistentstore.SnapshotFuture;
import org.gridgain.grid.persistentstore.SnapshotInfo;

/**
 *
 */
public class ClientNode {

    private static final int COUNT = 10000;
    public static final String ACCOUNT = "Account";



    public static void main(String[] args) throws Exception {
        final IgniteConfiguration cfg = ServerNode.prepareIgniteConfiguration();

        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {

            final GridGain gg = ignite.plugin(GridGain.PLUGIN_NAME);

            final GridSnapshot snapshot = gg.snapshot();

            final CacheConfiguration<Object, Account> acntCcfg = ServerNode.getAcntConfiguration();

            final IgniteCache<Object, Account> cache = ignite.getOrCreateCache(acntCcfg);

            final int size = cache.size();
            if (size >= COUNT)
                System.err.println("Accounts cache size is " + size);
            else {
                System.err.println("Accounts to be filled, currently have: " + size);
                initialLoad(ignite);
            }


            snapshotsDemo(snapshot);

            System.out.println("Press any key to close client");
            System.in.read();
        }
    }

    private static void snapshotsDemo(GridSnapshot snapshot) {
        long someSnId = -1;
        final List<SnapshotInfo> infos = snapshot.listSnapshots(null);
        for (SnapshotInfo next : infos) {
            someSnId = next.snapshotId();
            System.err.println("Snapshot found; " + someSnId);
        }

        if (someSnId <= 0) {
            System.out.println("Starting snapshot for account cache...");
            SnapshotFuture backupFut = snapshot.createFullSnapshot(
                Collections.singleton(ACCOUNT), "Message from shapshots");

            final Object o = backupFut.get();

            System.err.println("Snapshot created: " + o);
        }

        boolean restore = false;
        if (someSnId > 0 && restore) {
            snapshot.restoreSnapshot(someSnId, null, null)
                .get();
            System.err.println("Restore snapshot finished");
        }
    }

    private static void initialLoad(Ignite ignite) {
        long totalBalance = 1_000_000;
        try (IgniteDataStreamer<Integer, Account> streamer = ignite.dataStreamer(ACCOUNT)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < COUNT; i++) {
                final Account acnt = new Account(i, "Organization-" + i);
                final long cur = i == COUNT - 1 ? totalBalance : (long)(Math.random() * totalBalance);
                acnt.setBalance(cur);
                totalBalance -= cur;
                streamer.addData(i, acnt);

                if (i > 0 && i % 1_000 == 0)
                    System.out.println("Done: " + i);
            }
        }
    }

    /*
     private static void runMapReduce(Ignite ignite) {
        final ClusterGroup grp = ignite.cluster().forServers();
        long base = 1_000_000_000;
        final List<BigInteger> parms = LongStream.range(base, base+10_000)
            .mapToObj(Long::toString).map(BigInteger::new).collect(Collectors.toList());

        final BigInteger res = ignite.compute(grp).apply((i) -> {
            final boolean prime = i.isProbablePrime(64);

            if(prime)
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

     *
     runMapReduce(ignite);

     final ClusterGroup grp = ignite.cluster().forServers().forRandom();
     ignite.message(grp).send("OrderStreamer", "Start");

     ignite.events().localListen(e->{
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

     */

}
