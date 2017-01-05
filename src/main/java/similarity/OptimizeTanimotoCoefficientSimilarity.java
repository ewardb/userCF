package similarity;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.RefreshHelper;
import org.apache.mahout.cf.taste.impl.similarity.AbstractItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.PreferenceInferrer;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.util.Collection;

/**
 * Created by Qiang on 2017/1/5.
 */
public class OptimizeTanimotoCoefficientSimilarity extends AbstractItemSimilarity implements UserSimilarity {


    public OptimizeTanimotoCoefficientSimilarity(DataModel dataModel) {
        super(dataModel);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void setPreferenceInferrer(PreferenceInferrer inferrer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double userSimilarity(long userID1, long userID2) throws TasteException {

        DataModel dataModel = getDataModel();
        FastIDSet xPrefs = dataModel.getItemIDsFromUser(userID1);
        FastIDSet yPrefs = dataModel.getItemIDsFromUser(userID2);

        int xPrefsSize = xPrefs.size();
        int yPrefsSize = yPrefs.size();
        if (xPrefsSize == 0 && yPrefsSize == 0) {
            return Double.NaN;
        }
        if (xPrefsSize == 0 || yPrefsSize == 0) {
            return 0.0;
        }

        int intersectionSize =
                xPrefsSize < yPrefsSize ? yPrefs.intersectionSize(xPrefs) : xPrefs.intersectionSize(yPrefs);
        if (intersectionSize == 0) {
            return Double.NaN;
        }

        int unionSize = xPrefsSize + yPrefsSize - intersectionSize;

        return (double) intersectionSize / (double) unionSize;
    }

    @Override
    public double itemSimilarity(long itemID1, long itemID2) throws TasteException {
        int preferring1 = getDataModel().getNumUsersWithPreferenceFor(itemID1);
        return doItemSimilarity(itemID1, itemID2, preferring1);
    }

    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s) throws TasteException {
        int preferring1 = getDataModel().getNumUsersWithPreferenceFor(itemID1);
        int length = itemID2s.length;
        double[] result = new double[length];
        for (int i = 0; i < length; i++) {
            result[i] = doItemSimilarity(itemID1, itemID2s[i], preferring1);
        }
        return result;
    }

    private double doItemSimilarity(long itemID1, long itemID2, int preferring1) throws TasteException {
        DataModel dataModel = getDataModel();
        int preferring1and2 = dataModel.getNumUsersWithPreferenceFor(itemID1, itemID2);
        if (preferring1and2 == 0) {
            return Double.NaN;
        }
        int preferring2 = dataModel.getNumUsersWithPreferenceFor(itemID2);
        return (double) preferring1and2 / (double) (preferring1 + preferring2 - preferring1and2);
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        alreadyRefreshed = RefreshHelper.buildRefreshed(alreadyRefreshed);
        RefreshHelper.maybeRefresh(alreadyRefreshed, getDataModel());
    }

    @Override
    public String toString() {
        return "OptimizeTanimotoCoefficientSimilarity[dataModel:" + getDataModel() + ']';
    }
}
