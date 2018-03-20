package tx;

import java.util.concurrent.ForkJoinPool;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

public class TxDeadlockExample {
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start()) {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

            ccfg.setName("dlDetectExample");
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

            ForkJoinPool.commonPool().submit(() -> {
                try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.READ_COMMITTED, 3000, 0)) {

                    cache.put(1, "1");

                    Thread.sleep(1000);

                    cache.put(2, "2");

                    transaction.commit();

                    return null;
                }
            });

            try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.READ_COMMITTED, 3000, 0)) {

                cache.put(2, "2");

                Thread.sleep(1000);

                cache.put(1, "1");

                transaction.commit();
            }
            catch (CacheException e) {

                if (e.getCause() instanceof TransactionTimeoutException &&
                    e.getCause().getCause() instanceof TransactionDeadlockException)

                    System.out.println(e.getCause().getCause().getMessage());
            }

        }

    }
}
