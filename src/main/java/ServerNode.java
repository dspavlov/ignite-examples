import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Created by dpavlov on 11.10.2017.
 */
public class ServerNode {
    public static void main(String[] args) throws InterruptedException {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        try (final Ignite start = Ignition.start(cfg)) {
            Thread.sleep(1000000);
        }
    }
}
