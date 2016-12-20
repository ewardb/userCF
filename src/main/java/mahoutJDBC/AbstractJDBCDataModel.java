package mahoutJDBC;

/**
 * Created by wq on 16-12-6.
 */


import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.Retriever;
import org.apache.mahout.cf.taste.impl.common.jdbc.AbstractJDBCComponent;
import org.apache.mahout.cf.taste.impl.common.jdbc.ResultSetIterator;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
//import org.apache.mahout.cf.taste.impl.model.jdbc.ConnectionPoolDataSource;
import org.apache.mahout.cf.taste.model.JDBCDataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJDBCDataModel extends AbstractJDBCComponent implements JDBCDataModel {
    private static final Logger log = LoggerFactory.getLogger(AbstractJDBCDataModel.class);
    public static final String DEFAULT_PREFERENCE_TABLE = "taste_preferences";
    public static final String DEFAULT_USER_ID_COLUMN = "user_id";
    public static final String DEFAULT_ITEM_ID_COLUMN = "item_id";
    public static final String DEFAULT_PREFERENCE_COLUMN = "preference";
    public static final String DEFAULT_PREFERENCE_TIME_COLUMN = "timestamp";
    private final DataSource dataSource;
    private final String preferenceTable;
    private final String userIDColumn;
    private final String itemIDColumn;
    private final String preferenceColumn;
    private final String getPreferenceSQL;
    private final String getPreferenceTimeSQL;
    private final String getUserSQL;
    private final String getAllUsersSQL;
    private final String getNumItemsSQL;
    private final String getNumUsersSQL;
    private final String setPreferenceSQL;
    private final String removePreferenceSQL;
    private final String getUsersSQL;
    private final String getItemsSQL;
    private final String getPrefsForItemSQL;
    private final String getNumPreferenceForItemsSQL;
    private final String getMaxPreferenceSQL;
    private final String getMinPreferenceSQL;
    private int cachedNumUsers;
    private int cachedNumItems;
    private final Cache<Long, Integer> itemPrefCounts;
    private float maxPreference;
    private float minPreference;

    protected AbstractJDBCDataModel(DataSource dataSource, String getPreferenceSQL, String getPreferenceTimeSQL, String getUserSQL, String getAllUsersSQL, String getNumItemsSQL, String getNumUsersSQL, String setPreferenceSQL, String removePreferenceSQL, String getUsersSQL, String getItemsSQL, String getPrefsForItemSQL, String getNumPreferenceForItemSQL, String getNumPreferenceForItemsSQL, String getMaxPreferenceSQL, String getMinPreferenceSQL) {
        this(dataSource, "taste_preferences", "user_id", "item_id", "preference", getPreferenceSQL, getPreferenceTimeSQL, getUserSQL, getAllUsersSQL, getNumItemsSQL, getNumUsersSQL, setPreferenceSQL, removePreferenceSQL, getUsersSQL, getItemsSQL, getPrefsForItemSQL, getNumPreferenceForItemSQL, getNumPreferenceForItemsSQL, getMaxPreferenceSQL, getMinPreferenceSQL);
    }

    protected AbstractJDBCDataModel(DataSource dataSource, String preferenceTable, String userIDColumn, String itemIDColumn, String preferenceColumn, String getPreferenceSQL, String getPreferenceTimeSQL, String getUserSQL, String getAllUsersSQL, String getNumItemsSQL, String getNumUsersSQL, String setPreferenceSQL, String removePreferenceSQL, String getUsersSQL, String getItemsSQL, String getPrefsForItemSQL, String getNumPreferenceForItemSQL, String getNumPreferenceForItemsSQL, String getMaxPreferenceSQL, String getMinPreferenceSQL) {
        log.debug("Creating AbstractJDBCModel...");
        AbstractJDBCComponent.checkNotNullAndLog("preferenceTable", preferenceTable);
        AbstractJDBCComponent.checkNotNullAndLog("userIDColumn", userIDColumn);
        AbstractJDBCComponent.checkNotNullAndLog("itemIDColumn", itemIDColumn);
        AbstractJDBCComponent.checkNotNullAndLog("preferenceColumn", preferenceColumn);
        AbstractJDBCComponent.checkNotNullAndLog("dataSource", dataSource);
        AbstractJDBCComponent.checkNotNullAndLog("getUserSQL", getUserSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getAllUsersSQL", getAllUsersSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getPreferenceSQL", getPreferenceSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getNumItemsSQL", getNumItemsSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getNumUsersSQL", getNumUsersSQL);
        AbstractJDBCComponent.checkNotNullAndLog("setPreferenceSQL", setPreferenceSQL);
        AbstractJDBCComponent.checkNotNullAndLog("removePreferenceSQL", removePreferenceSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getUsersSQL", getUsersSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getItemsSQL", getItemsSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getPrefsForItemSQL", getPrefsForItemSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getNumPreferenceForItemSQL", getNumPreferenceForItemSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getNumPreferenceForItemsSQL", getNumPreferenceForItemsSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getMaxPreferenceSQL", getMaxPreferenceSQL);
        AbstractJDBCComponent.checkNotNullAndLog("getMinPreferenceSQL", getMinPreferenceSQL);
//        if(!(dataSource instanceof ConnectionPoolDataSource)) {
//            log.warn("You are not using ConnectionPoolDataSource. Make sure your DataSource pools connections to the database itself, or database performance will be severely reduced.");
//        }

        this.preferenceTable = preferenceTable;
        this.userIDColumn = userIDColumn;
        this.itemIDColumn = itemIDColumn;
        this.preferenceColumn = preferenceColumn;
        this.dataSource = dataSource;
        this.getPreferenceSQL = getPreferenceSQL;
        this.getPreferenceTimeSQL = getPreferenceTimeSQL;
        this.getUserSQL = getUserSQL;
        this.getAllUsersSQL = getAllUsersSQL;
        this.getNumItemsSQL = getNumItemsSQL;
        this.getNumUsersSQL = getNumUsersSQL;
        this.setPreferenceSQL = setPreferenceSQL;
        this.removePreferenceSQL = removePreferenceSQL;
        this.getUsersSQL = getUsersSQL;
        this.getItemsSQL = getItemsSQL;
        this.getPrefsForItemSQL = getPrefsForItemSQL;
        this.getNumPreferenceForItemsSQL = getNumPreferenceForItemsSQL;
        this.getMaxPreferenceSQL = getMaxPreferenceSQL;
        this.getMinPreferenceSQL = getMinPreferenceSQL;
        this.cachedNumUsers = -1;
        this.cachedNumItems = -1;
        this.itemPrefCounts = new Cache(new AbstractJDBCDataModel.ItemPrefCountRetriever(getNumPreferenceForItemSQL));
        this.maxPreference = 0.0F / 0.0F;
        this.minPreference = 0.0F / 0.0F;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public String getPreferenceTable() {
        return this.preferenceTable;
    }

    public String getUserIDColumn() {
        return this.userIDColumn;
    }

    public String getItemIDColumn() {
        return this.itemIDColumn;
    }

    public String getPreferenceColumn() {
        return this.preferenceColumn;
    }

    String getSetPreferenceSQL() {
        return this.setPreferenceSQL;
    }

    public LongPrimitiveIterator getUserIDs() throws TasteException {
        log.debug("Retrieving all users...");

        try {
            return new AbstractJDBCDataModel.ResultSetIDIterator(this.getUsersSQL);
        } catch (SQLException var2) {
            throw new TasteException(var2);
        }
    }

    public PreferenceArray getPreferencesFromUser(long userID) throws TasteException {
        log.debug("Retrieving user ID \'{}\'", Long.valueOf(userID));
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        GenericUserPreferenceArray var7;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.getUserSQL, 1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            this.setLongParameter(stmt, 1, userID);
            log.debug("Executing SQL query: {}", this.getUserSQL);
            rs = stmt.executeQuery();
            ArrayList sqle = new ArrayList();

            while(rs.next()) {
                sqle.add(this.buildPreference(rs));
            }

            if(sqle.isEmpty()) {
                throw new NoSuchUserException(userID);
            }

            var7 = new GenericUserPreferenceArray(sqle);
        } catch (SQLException var11) {
            log.warn("Exception while retrieving user", var11);
            throw new TasteException(var11);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return var7;
    }

    public FastByIDMap<PreferenceArray> exportWithPrefs() throws TasteException {
        log.debug("Exporting all data");
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        FastByIDMap result = new FastByIDMap();

        FastByIDMap nextUserID1;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.createStatement(1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            log.debug("Executing SQL query: {}", this.getAllUsersSQL);
            rs = stmt.executeQuery(this.getAllUsersSQL);
            Long sqle = null;

            ArrayList currentPrefs;
            long nextUserID;
            for(currentPrefs = new ArrayList(); rs.next(); sqle = Long.valueOf(nextUserID)) {
                nextUserID = this.getLongColumn(rs, 1);
                if(sqle != null && !sqle.equals(Long.valueOf(nextUserID)) && !currentPrefs.isEmpty()) {
                    result.put(sqle.longValue(), new GenericUserPreferenceArray(currentPrefs));
                    currentPrefs.clear();
                }

                currentPrefs.add(this.buildPreference(rs));
            }

            if(!currentPrefs.isEmpty()) {
                result.put(sqle.longValue(), new GenericUserPreferenceArray(currentPrefs));
            }

            nextUserID1 = result;
        } catch (SQLException var12) {
            log.warn("Exception while exporting all data", var12);
            throw new TasteException(var12);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return nextUserID1;
    }

    public FastByIDMap<FastIDSet> exportWithIDsOnly() throws TasteException {
        log.debug("Exporting all data");
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        FastByIDMap result = new FastByIDMap();

        FastByIDMap nextUserID1;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.createStatement(1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            log.debug("Executing SQL query: {}", this.getAllUsersSQL);
            rs = stmt.executeQuery(this.getAllUsersSQL);
            boolean sqle = false;
            long currentUserID = 0L;

            FastIDSet currentItemIDs;
            for(currentItemIDs = new FastIDSet(2); rs.next(); sqle = true) {
                long nextUserID = this.getLongColumn(rs, 1);
                if(sqle && currentUserID != nextUserID && !currentItemIDs.isEmpty()) {
                    result.put(currentUserID, currentItemIDs);
                    currentItemIDs = new FastIDSet(2);
                }

                currentItemIDs.add(this.getLongColumn(rs, 2));
                currentUserID = nextUserID;
            }

            if(!currentItemIDs.isEmpty()) {
                result.put(currentUserID, currentItemIDs);
            }

            nextUserID1 = result;
        } catch (SQLException var14) {
            log.warn("Exception while exporting all data", var14);
            throw new TasteException(var14);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return nextUserID1;
    }

    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        log.debug("Retrieving items for user ID \'{}\'", Long.valueOf(userID));
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        FastIDSet var7;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.getUserSQL, 1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            this.setLongParameter(stmt, 1, userID);
            log.debug("Executing SQL query: {}", this.getUserSQL);
            rs = stmt.executeQuery();
            FastIDSet sqle = new FastIDSet();

            while(rs.next()) {
                sqle.add(this.getLongColumn(rs, 2));
            }

            if(sqle.isEmpty()) {
                throw new NoSuchUserException(userID);
            }

            var7 = sqle;
        } catch (SQLException var11) {
            log.warn("Exception while retrieving item s", var11);
            throw new TasteException(var11);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return var7;
    }

    public Float getPreferenceValue(long userID, long itemID) throws TasteException {
        log.debug("Retrieving preferences for item ID \'{}\'", Long.valueOf(itemID));
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        Float sqle;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.getPreferenceSQL, 1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(1);
            this.setLongParameter(stmt, 1, userID);
            this.setLongParameter(stmt, 2, itemID);
            log.debug("Executing SQL query: {}", this.getPreferenceSQL);
            rs = stmt.executeQuery();
            if(!rs.next()) {
                sqle = null;
                return sqle;
            }

            sqle = Float.valueOf(rs.getFloat(1));
        } catch (SQLException var12) {
            log.warn("Exception while retrieving prefs for item", var12);
            throw new TasteException(var12);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return sqle;
    }

    public Long getPreferenceTime(long userID, long itemID) throws TasteException {
        if(this.getPreferenceTimeSQL == null) {
            return null;
        } else {
            log.debug("Retrieving preference time for item ID \'{}\'", Long.valueOf(itemID));
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            Long sqle;
            try {
                conn = this.dataSource.getConnection();
                stmt = conn.prepareStatement(this.getPreferenceSQL, 1003, 1007);
                stmt.setFetchDirection(1000);
                stmt.setFetchSize(1);
                this.setLongParameter(stmt, 1, userID);
                this.setLongParameter(stmt, 2, itemID);
                log.debug("Executing SQL query: {}", this.getPreferenceTimeSQL);
                rs = stmt.executeQuery();
                if(rs.next()) {
                    sqle = Long.valueOf(rs.getLong(1));
                    return sqle;
                }

                sqle = null;
            } catch (SQLException var12) {
                log.warn("Exception while retrieving time for item", var12);
                throw new TasteException(var12);
            } finally {
                IOUtils.quietClose(rs, stmt, conn);
            }

            return sqle;
        }
    }

    public LongPrimitiveIterator getItemIDs() throws TasteException {
        log.debug("Retrieving all items...");

        try {
            return new AbstractJDBCDataModel.ResultSetIDIterator(this.getItemsSQL);
        } catch (SQLException var2) {
            throw new TasteException(var2);
        }
    }

    public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
        List list = this.doGetPreferencesForItem(itemID);
        if(list.isEmpty()) {
            throw new NoSuchItemException(itemID);
        } else {
            return new GenericItemPreferenceArray(list);
        }
    }

    protected List<Preference> doGetPreferencesForItem(long itemID) throws TasteException {
        log.debug("Retrieving preferences for item ID \'{}\'", Long.valueOf(itemID));
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.getPrefsForItemSQL, 1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            this.setLongParameter(stmt, 1, itemID);
            log.debug("Executing SQL query: {}", this.getPrefsForItemSQL);
            rs = stmt.executeQuery();
            ArrayList sqle = new ArrayList();

            while(rs.next()) {
                sqle.add(this.buildPreference(rs));
            }

            ArrayList var7 = sqle;
            return var7;
        } catch (SQLException var11) {
            log.warn("Exception while retrieving prefs for item", var11);
            throw new TasteException(var11);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }
    }

    public int getNumItems() throws TasteException {
        if(this.cachedNumItems < 0) {
            this.cachedNumItems = this.getNumThings("items", this.getNumItemsSQL, new long[0]);
        }

        return this.cachedNumItems;
    }

    public int getNumUsers() throws TasteException {
        if(this.cachedNumUsers < 0) {
            this.cachedNumUsers = this.getNumThings("users", this.getNumUsersSQL, new long[0]);
        }

        return this.cachedNumUsers;
    }

    public int getNumUsersWithPreferenceFor(long... itemIDs) throws TasteException {
        Preconditions.checkArgument(itemIDs != null, "itemIDs is null");
        int length = itemIDs.length;
        Preconditions.checkArgument(length != 0 && length <= 2, "Illegal number of item IDs: " + length);
        return length == 1?((Integer)this.itemPrefCounts.get(Long.valueOf(itemIDs[0]))).intValue():this.getNumThings("user preferring items", this.getNumPreferenceForItemsSQL, itemIDs);
    }

    private int getNumThings(String name, String sql, long... args) throws TasteException {
        log.debug("Retrieving number of {} in model", name);
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        int sqle;
        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(sql, 1003, 1007);
            stmt.setFetchDirection(1000);
            stmt.setFetchSize(this.getFetchSize());
            if(args != null) {
                for(sqle = 1; sqle <= args.length; ++sqle) {
                    this.setLongParameter(stmt, sqle, args[sqle - 1]);
                }
            }

            log.debug("Executing SQL query: {}", sql);
            rs = stmt.executeQuery();
            rs.next();
            sqle = rs.getInt(1);
        } catch (SQLException var11) {
            log.warn("Exception while retrieving number of {}", name, var11);
            throw new TasteException(var11);
        } finally {
            IOUtils.quietClose(rs, stmt, conn);
        }

        return sqle;
    }

    public void setPreference(long userID, long itemID, float value) throws TasteException {
        Preconditions.checkArgument(!Float.isNaN(value), "NaN value");
        log.debug("Setting preference for user {}, item {}", Long.valueOf(userID), Long.valueOf(itemID));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.setPreferenceSQL);
            this.setLongParameter(stmt, 1, userID);
            this.setLongParameter(stmt, 2, itemID);
            stmt.setDouble(3, (double)value);
            stmt.setDouble(4, (double)value);
            log.debug("Executing SQL update: {}", this.setPreferenceSQL);
            stmt.executeUpdate();
        } catch (SQLException var12) {
            log.warn("Exception while setting preference", var12);
            throw new TasteException(var12);
        } finally {
            IOUtils.quietClose((ResultSet)null, stmt, conn);
        }

    }

    public void removePreference(long userID, long itemID) throws TasteException {
        log.debug("Removing preference for user \'{}\', item \'{}\'", Long.valueOf(userID), Long.valueOf(itemID));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = this.dataSource.getConnection();
            stmt = conn.prepareStatement(this.removePreferenceSQL);
            this.setLongParameter(stmt, 1, userID);
            this.setLongParameter(stmt, 2, itemID);
            log.debug("Executing SQL update: {}", this.removePreferenceSQL);
            stmt.executeUpdate();
        } catch (SQLException var11) {
            log.warn("Exception while removing preference", var11);
            throw new TasteException(var11);
        } finally {
            IOUtils.quietClose((ResultSet)null, stmt, conn);
        }

    }

    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        this.cachedNumUsers = -1;
        this.cachedNumItems = -1;
        this.minPreference = 0.0F / 0.0F;
        this.maxPreference = 0.0F / 0.0F;
        this.itemPrefCounts.clear();
    }

    public boolean hasPreferenceValues() {
        return true;
    }

    public float getMaxPreference() {
        if(Float.isNaN(this.maxPreference)) {
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                conn = this.dataSource.getConnection();
                stmt = conn.prepareStatement(this.getMaxPreferenceSQL);
                log.debug("Executing SQL query: {}", this.getMaxPreferenceSQL);
                rs = stmt.executeQuery();
                rs.next();
                this.maxPreference = rs.getFloat(1);
            } catch (SQLException var8) {
                log.warn("Exception while removing preference", var8);
            } finally {
                IOUtils.quietClose(rs, stmt, conn);
            }
        }

        return this.maxPreference;
    }

    public float getMinPreference() {
        if(Float.isNaN(this.minPreference)) {
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                conn = this.dataSource.getConnection();
                stmt = conn.prepareStatement(this.getMinPreferenceSQL);
                log.debug("Executing SQL query: {}", this.getMinPreferenceSQL);
                rs = stmt.executeQuery();
                rs.next();
                this.minPreference = rs.getFloat(1);
            } catch (SQLException var8) {
                log.warn("Exception while removing preference", var8);
            } finally {
                IOUtils.quietClose(rs, stmt, conn);
            }
        }

        return this.minPreference;
    }

    protected Preference buildPreference(ResultSet rs) throws SQLException {
        return new GenericPreference(this.getLongColumn(rs, 1), this.getLongColumn(rs, 2), rs.getFloat(3));
    }

    protected long getLongColumn(ResultSet rs, int position) throws SQLException {
        return rs.getLong(position);
    }

    protected void setLongParameter(PreparedStatement stmt, int position, long value) throws SQLException {
        stmt.setLong(position, value);
    }

    private final class ItemPrefCountRetriever implements Retriever<Long, Integer> {
        private final String getNumPreferenceForItemSQL;

        private ItemPrefCountRetriever(String getNumPreferenceForItemSQL) {
            this.getNumPreferenceForItemSQL = getNumPreferenceForItemSQL;
        }

        public Integer get(Long key) throws TasteException {
            return Integer.valueOf(AbstractJDBCDataModel.this.getNumThings("user preferring item", this.getNumPreferenceForItemSQL, new long[]{key.longValue()}));
        }
    }

    private final class ResultSetIDIterator extends ResultSetIterator<Long> implements LongPrimitiveIterator {
        private ResultSetIDIterator(String sql) throws SQLException {
            super(AbstractJDBCDataModel.this.dataSource, sql);
        }

        protected Long parseElement(ResultSet resultSet) throws SQLException {
            return Long.valueOf(AbstractJDBCDataModel.this.getLongColumn(resultSet, 1));
        }

        public long nextLong() {
            return ((Long)this.next()).longValue();
        }

        public long peek() {
            throw new UnsupportedOperationException();
        }
    }
}
