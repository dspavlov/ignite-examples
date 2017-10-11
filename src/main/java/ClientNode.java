import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Created by dpavlov on 11.10.2017
 */
public class ClientNode {
    public static void main(String[] args) throws IOException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        ServerNode.setupCustomIp(cfg);
        cfg.setClientMode(true);
        try (final Ignite ignite = Ignition.start(cfg)) {
            final IgniteCache<Object, Object> orders = ignite.cache(ServerNode.ORDERS);

            final IgniteCompute compute = ignite.compute();
            System.out.println("Press any key to close client");
            System.in.read();
        }
    }
}
