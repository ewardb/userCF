package baseUserCF;

import DB.UtilDB;
import Entity.Rating;
import Kmeans.*;
import Kmeans.PreDataKin;
import Kmeans.UserModel;
import Kmeans.UserModelXY;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.model.MySQLJDBCIDMigrator;

import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.JDBCDataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import mahoutJDBC.MySQLJDBCDataModel;

import javax.sql.DataSource;
import java.io.File;
import java.util.*;

/**
 * Created by wq on 16-11-24.
 */
public class BaseUserCf {

    public static List<HashMap<Integer,List<Integer>>> splitDate(List<Rating> ratings){
        List<HashMap<Integer,List<Integer>>> lists = new ArrayList<>();
        HashMap<Integer,List<Integer>> train = new HashMap<>();
        HashMap<Integer,List<Integer>> test = new HashMap<>();
        //split data
        long t = System.currentTimeMillis();
        Random random = new Random(t);
        for(Rating rating:ratings){
            int userId = rating.getUserId();
            int movieId =  rating.getMovieId();
            //random.nextInt(8)  获得0-7的随机数
            if(random.nextInt(8)==7){
                if(test.containsKey(userId)){
                    //测试集中有这个用户 。。。所以 直接把movieId 添加就好了
                    test.get(userId).add(movieId);
                }else{
                    //这是  测试集中不存在这个用户。。。需要建立一个list 装movieId
                    List<Integer> list = new ArrayList<Integer>();
                    list.add(movieId);
                    test.put(userId,list);
                }
            }else{
                if(train.containsKey(userId)){
                    train.get(userId).add(movieId);
                }else{
                    List<Integer> list1 = new ArrayList<Integer>();
                    list1.add(movieId);
                    train.put(userId,list1);
                }
            }
        }
        lists.add(train);
        lists.add(test);
        System.out.println("hahahahaha");
        System.out.println("happy");

        return lists;
    }

    public double similarity(){
        try{
            DataModel dataModel = new FileDataModel(new File("ff"));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);
            new UncenteredCosineSimilarity(dataModel);//余弦相似性！！！
            UserNeighborhood neighborhood = new NearestNUserNeighborhood(100,similarity,dataModel);
            Recommender recommender = new GenericUserBasedRecommender(dataModel,neighborhood,similarity);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0.0;
    }


    public static void ss(){
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName("localhost");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setDatabaseName("userCf");
        JDBCDataModel dataModel = new MySQLJDBCDataModel(dataSource,"ratings","userID","movieId","rating","timestramp");
        DataModel model = dataModel;
        try{
            UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
            UserNeighborhood neighborhood = new NearestNUserNeighborhood(20,similarity,model);
            Recommender recommender = new GenericUserBasedRecommender(model,neighborhood,similarity);
            //gei yonghu 1 tuijian 3 tiaojilu
            Recommender recommender1 = new CachingRecommender(recommender);
            List<RecommendedItem> recommenders = recommender1.recommend(1,20);
            for(RecommendedItem recommendedItem:recommenders){
                System.out.println(recommendedItem);
            }
        }catch (TasteException e){
            e.printStackTrace();
        }
    }
    public static void main(String args[]){
//
        long t1  = System.currentTimeMillis();
        Kmeans kmeans = new Kmeans();
        UtilDB utilDB = new UtilDB();
        List<UserModel> list =  utilDB.getUserMovidType(); //97777
        List<UserModelXY> userModelXYList =  PreDataKin.createXandY(list);
        System.out.println(System.currentTimeMillis() - t1);
        int k = 10;
        List<Cluster> kCluster = kmeans.kmeansFunc(userModelXYList,k);
        System.out.println(System.currentTimeMillis() - t1);
        for(int i = 0 ; i< k ; i++){
            Cluster c = kCluster.get(i);
            System.out.print("用户(");
            for(UserModelXY p : c.getListPoint()){
                System.out.print(p.getId());
                System.out.print(" ");
            }
            System.out.println(") 属于第"+ c.getClusterid()+"类，大小为"+c.getListPoint().size());
        }
    }
}
