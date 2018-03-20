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
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionTimeoutException;

public class TxOptimisticExample {
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start()) {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

            ccfg.setName("dlDetectExample");
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

            ForkJoinPool.commonPool().submit(() -> {
                try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                    TransactionIsolation.SERIALIZABLE, 3000, 0)) {

                    cache.put(1, "1");

                    Thread.sleep(1000);

                    cache.put(2, "2");

                    transaction.commit();
                }
                catch (TransactionOptimisticException e) {
                    e.printStackTrace();
                }

                return null;
            });

            try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.SERIALIZABLE, 3000, 0)) {

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
            catch (TransactionOptimisticException e) {
                e.printStackTrace();
            }
        }

        Ignite ignite = Ignition.start();
        // Start transaction in optimistic mode with serializable isolation level.
        while (true) {
            try (Transaction tx =
                     ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                         TransactionIsolation.SERIALIZABLE)) {
                // Modify cache entries as part of this transaction.
                //....

                // commit transaction.
                tx.commit();

                // Transaction succeeded. Leave the while loop.
                break;
            }
            catch (TransactionOptimisticException e) {
                // Transaction has failed. Retry.
            }
        }
    }
}
