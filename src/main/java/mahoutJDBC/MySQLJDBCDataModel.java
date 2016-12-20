package mahoutJDBC;

/**
 * Created by wq on 16-12-6.
 */

import javax.sql.DataSource;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.jdbc.AbstractJDBCComponent;


public class MySQLJDBCDataModel extends AbstractJDBCDataModel {
    public MySQLJDBCDataModel() throws TasteException {
        this("jdbc/taste");
    }

    public MySQLJDBCDataModel(String dataSourceName) throws TasteException {
        this(AbstractJDBCComponent.lookupDataSource(dataSourceName), "taste_preferences", "user_id", "item_id", "preference", "timestamp");
    }

    public MySQLJDBCDataModel(DataSource dataSource) {
        this(dataSource, "taste_preferences", "user_id", "item_id", "preference", "timestamp");
    }

    public MySQLJDBCDataModel(DataSource dataSource, String preferenceTable, String userIDColumn, String itemIDColumn, String preferenceColumn, String timestampColumn) {
        super(dataSource, preferenceTable, userIDColumn, itemIDColumn, preferenceColumn, "SELECT " + preferenceColumn + " FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND " + itemIDColumn + "=?", "SELECT " + timestampColumn + " FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND " + itemIDColumn + "=?", "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable + " WHERE " + userIDColumn + "=? ORDER BY " + itemIDColumn, "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable + " ORDER BY " + userIDColumn + ", " + itemIDColumn, "SELECT COUNT(DISTINCT " + itemIDColumn + ") FROM " + preferenceTable, "SELECT COUNT(DISTINCT " + userIDColumn + ") FROM " + preferenceTable, "INSERT INTO " + preferenceTable + '(' + userIDColumn + ',' + itemIDColumn + ',' + preferenceColumn + ") VALUES (?,?,?) ON DUPLICATE KEY UPDATE " + preferenceColumn + "=?", "DELETE FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND " + itemIDColumn + "=?", "SELECT DISTINCT " + userIDColumn + " FROM " + preferenceTable + " ORDER BY " + userIDColumn, "SELECT DISTINCT " + itemIDColumn + " FROM " + preferenceTable + " ORDER BY " + itemIDColumn, "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable + " WHERE " + itemIDColumn + "=? ORDER BY " + userIDColumn, "SELECT COUNT(1) FROM " + preferenceTable + " WHERE " + itemIDColumn + "=?", "SELECT COUNT(1) FROM " + preferenceTable + " tp1 JOIN " + preferenceTable + " tp2 " + "USING (" + userIDColumn + ") WHERE tp1." + itemIDColumn + "=? and tp2." + itemIDColumn + "=?", "SELECT MAX(" + preferenceColumn + ") FROM " + preferenceTable, "SELECT MIN(" + preferenceColumn + ") FROM " + preferenceTable);
    }

    @Override
    public int getNumUsersWithPreferenceFor(long... itemIDs) throws TasteException {
        return super.getNumUsersWithPreferenceFor(itemIDs);
    }

    @Override
    public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) throws TasteException {
        return 0;
    }

    public int getNumUsersWithPreferenceFor(long itemID1) throws TasteException {
        return 0;
    }

    protected int getFetchSize() {
        return -2147483648;
    }












}
