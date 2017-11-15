import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 *
 */
public class Account {
    @QuerySqlField
    private final int id;

    @QuerySqlField
    private final String name;

    @QuerySqlField
    private long balance;

    @QuerySqlField
    public long ts;

    public Account(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public void setBalance(long balance) {
        this.balance = balance;
    }

    public long getBalance() {
        return balance;
    }
}
