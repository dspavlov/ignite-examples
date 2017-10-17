import java.io.File;
import java.net.MalformedURLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Created by dpavlov on 17.10.2017
 */
public class DataStreamerLoadTest {
    public static void main(String[] args) throws Exception {

        System.setProperty(IgniteSystemProperties.IGNITE_HOME, new File(".").getAbsolutePath());
        final int size;
        try (final Ignite ignite = Ignition.start(new File("server.xml").toURL())) {
            ignite.active(true);

            CacheConfiguration<UserRawKey, UserRawValue> rawUserCacheConfig = new
                CacheConfiguration<UserRawKey, UserRawValue>();
            rawUserCacheConfig.setName("RawUserData");
            rawUserCacheConfig.setIndexedTypes(
                UserRawKey.class,
                UserRawValue.class);
            rawUserCacheConfig.setSqlSchema("PUBLIC");
            rawUserCacheConfig.setQueryParallelism(8);
            final IgniteCache<UserRawKey, UserRawValue> cache = ignite.getOrCreateCache(rawUserCacheConfig);
            final IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cache.getName());
            final AtomicLong cnt = new AtomicLong();
            IntStream.range(0, 550_000_000)
                .parallel()
                .forEach(i -> {
                    final ThreadLocalRandom random = ThreadLocalRandom.current();
                    final int randInt = random.nextInt();
                    final UserRawKey key = new UserRawKey("day" + i,
                        "siteId " + i,
                        "meetingNo" + Integer.toString(randInt),
                        "confId" + Integer.toString(random.nextInt()),
                        "uId" + Integer.toString(random.nextInt()),
                        "gId" + Integer.toString(random.nextInt())
                    );
                    final UserRawValue val = new UserRawValue();
                    val.p12 = val.p11 = val.p10 = val.p9 = val.p8 = val.p7 =
                        val.p6 = val.p5 = val.p4 = val.p3 = val.p2 = val.p1 = "p1";
                    streamer.addData(key, val);
                    final long processed = cnt.incrementAndGet();
                    if (processed % 10000 == 0)
                        System.out.println("Processed: " + processed + " cache size is " +
                            cache.size());
                });
            streamer.close();

            size = cache.size();

            ignite.active(false);
        }
        System.out.println("Total cache size is " + size);
    }
}
