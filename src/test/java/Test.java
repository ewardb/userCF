import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

/**
 * Created by wq on 16-11-24.
 */
public class Test {
    public static void main(String args[]){
        String path = System.getProperty("user.dir")+"/src/test/item.csv";

        try{
            DataModel dataModel = new FileDataModel(new File(path));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
            new UncenteredCosineSimilarity(dataModel);//余弦相似性！！！
            UserNeighborhood neighborhood = new NearestNUserNeighborhood(100,similarity,dataModel);
            Recommender recommender = new GenericUserBasedRecommender(dataModel,neighborhood,similarity);
            List<RecommendedItem> list = recommender.recommend(1,3);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
