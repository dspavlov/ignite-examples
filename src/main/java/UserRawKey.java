import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created by dpavlov on 17.10.2017.
 */
public class UserRawKey implements Serializable {
    @QuerySqlField(index = true, orderedGroups = {
        @QuerySqlField.Group(
            name = "raw_user_day_site_idx", order = 0),
        @QuerySqlField.Group(
            name = "raw_user_day_site_meetingnum_idx", order = 0)})
    private String day;

    @AffinityKeyMapped
    @QuerySqlField(index = true, orderedGroups = {
        @QuerySqlField.Group(
            name = "raw_user_day_site_idx", order = 1),
        @QuerySqlField.Group(
            name = "raw_user_day_site_meetingnum_idx", order = 1)})
    private String siteId;

    @QuerySqlField(index = true, orderedGroups = {
        @QuerySqlField.Group(
            name = "raw_user_day_site_meetingnum_idx", order = 2)})
    private String meetingNumber;

    @QuerySqlField(index = true)
    private String confId;

    @QuerySqlField
    private String uId;

    @QuerySqlField
    private String gId;

    public UserRawKey(String day, String siteId, String meetingNumber, String confId, String uId, String gId) {
        this.day = day;
        this.siteId = siteId;
        this.meetingNumber = meetingNumber;
        this.confId = confId;
        this.uId = uId;
        this.gId = gId;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        UserRawKey key = (UserRawKey)o;

        if (day != null ? !day.equals(key.day) : key.day != null)
            return false;
        if (siteId != null ? !siteId.equals(key.siteId) : key.siteId != null)
            return false;
        if (meetingNumber != null ? !meetingNumber.equals(key.meetingNumber) : key.meetingNumber != null)
            return false;
        if (confId != null ? !confId.equals(key.confId) : key.confId != null)
            return false;
        if (uId != null ? !uId.equals(key.uId) : key.uId != null)
            return false;
        return gId != null ? gId.equals(key.gId) : key.gId == null;
    }

    @Override public int hashCode() {
        int result = day != null ? day.hashCode() : 0;
        result = 31 * result + (siteId != null ? siteId.hashCode() : 0);
        result = 31 * result + (meetingNumber != null ? meetingNumber.hashCode() : 0);
        result = 31 * result + (confId != null ? confId.hashCode() : 0);
        result = 31 * result + (uId != null ? uId.hashCode() : 0);
        result = 31 * result + (gId != null ? gId.hashCode() : 0);
        return result;
    }
}
