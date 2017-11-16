import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Created by dpavlov on 14.11.2017.
 */
public class AccountTransferClientNode {

    public static void main(String[] args) throws Exception {
        final IgniteConfiguration cfg = ServerNode.prepareIgniteConfiguration();

        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {
            final AtomicBoolean cancel = new AtomicBoolean();
            startAcntTransfer(ignite, cancel);

            System.out.println("Press any key to stop acnt transfer");
            System.in.read();
            cancel.set(true);

        }
    }

    private static void startAcntTransfer(Ignite ignite, AtomicBoolean cancel) {
        final IgniteCache<Object, Account> accounts = ignite.getOrCreateCache(ClientNode.ACCOUNT);

        final Random rnd = new Random();

        cancel.set(false);
        Thread t = new Thread(new Runnable() {
            private void transfer(Account a1, Account a2) {
                long amount = (long)(rnd.nextInt(10000));
                if (a1.getBalance() >= amount) {
                    long tm = System.currentTimeMillis();
                    a1.setBalance(a1.getBalance()-amount);
                    a2.setBalance(a2.getBalance()+amount);

                    a1.ts = tm;
                    a2.ts = tm;
                }
            }

            @Override public void run() {
                final int acntCnt = accounts.size() * 2;
                if (acntCnt == 0) {
                    System.err.println("No Account found in the system, can't start acnt transfer");
                    return;
                }
                while (!cancel.get()) {
                    boolean pause = false;
                    if (pause) {
                        try {
                            Thread.sleep(50);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            e.printStackTrace();
                            return;
                        }
                    }

                    // Generate random keys and sort them.
                    int k1 = rnd.nextInt(acntCnt);
                    int k2 = rnd.nextInt(acntCnt);

                    while (k1 == k2)
                        k2 = rnd.nextInt(acntCnt);


                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 60000, 2)) {
                        Account acc1 = accounts.get(k1);
                        Account acc2 = accounts.get(k2);

                        if (acc1 == null)
                            acc1 = new Account(k1, "Dynamic-"+k1);

                        if (acc2 == null)
                            acc2 = new Account(k2, "Dynamic-" + k1);

                        transfer(acc1, acc2);

                        // Store updated account in cache.
                        if (k2 > k1) {
                            accounts.put(k1, acc1);
                            accounts.put(k2, acc2);
                        } else {
                            accounts.put(k2, acc2);
                            accounts.put(k1, acc1);
                        }

                        if (rnd.nextInt(100) > 10) {
                            tx.commit();
                            log("Tx commit: " + k1 + "->" + k2);
                        }
                        else
                            tx.rollback();
                    }
                    catch (Throwable e) {
                        cancel.set(true);

                        log("Load failed: " + e.getMessage());
                    }
                }

            }
        });

        t.start();
    }

    private static void log(String s) {

        System.out.println("[" + new SimpleDateFormat("HH:mm:ss.SSS").format(System.currentTimeMillis()) + "] " + s);
    }
}