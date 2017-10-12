import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Created by dpavlov on 11.10.2017.
 */
public class ClientNode {
    public static void main(String[] args) {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {

            final IgniteCache<Integer, String> test = ignite.getOrCreateCache("EntryTest");
            test.putAll(new TreeMap<Integer, String>() {{
                put(1, "hi");
                put(2, "there");
                put(3, "!");
            }});

            CacheEntryProcessor<Integer, String, Integer> ep = (entry, parms) -> {
                String v = entry.getValue();
                entry.setValue("_" + v + "_");
                System.err.println("Running entry processor");
                return v.length();
            };
            final Integer invoke = test.invoke(1, ep);



            Set<Integer> keys = new HashSet<>();
            for (Cache.Entry<Integer, String> e : test) {
                keys.add(e.getKey());
            }
            final Map<Integer, EntryProcessorResult<Integer>> map = test.invokeAll(keys, ep);
            System.out.println(map);

            for (Cache.Entry<Integer, String> e : test) {
                System.out.println(e);
            }
            System.err.println(invoke);
        }
    }
}
