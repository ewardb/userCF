package Kmeans;

import DB.MysqlDB;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Qiang on 2016/12/24.
 */
public class PreDataKin {
    public static List<UserModelXY> createXandY(List<UserModel> list){
        List<UserModelXY> userModelXYList = new ArrayList<UserModelXY>();

        for(UserModel userModel : list){
            int id =  userModel.getId();
            HashMap<String,Integer> hashMap = userModel.getOptions();
            int count = userModel.getCount();
            double action =  hashMap.containsKey("Action") ? 100* hashMap.get("Action")/count : 0 ;
            double adventure = hashMap.containsKey("Adventure") ? 100* hashMap.get("Adventure")/count: 0 ;
            double animation= hashMap.containsKey("Animation") ? 100* hashMap.get("Animation")/count: 0 ;
            double children= hashMap.containsKey("Children's") ? 100* hashMap.get("Children's")/count: 0 ;
            double comedy= hashMap.containsKey("Comedy") ? 100* hashMap.get("Comedy")/count: 0 ;
            double crime= hashMap.containsKey("Crime") ? 100* hashMap.get("Crime")/count: 0 ;
            double documentary= hashMap.containsKey("Documentary") ? 100* hashMap.get("Documentary")/count: 0 ;
            double drama= hashMap.containsKey("Drama") ? 100* hashMap.get("Drama")/count: 0 ;
            double fantasy=hashMap.containsKey("Fantasy") ? 100* hashMap.get("Fantasy")/count: 0 ;
            double filmnoir= hashMap.containsKey("Film-Noir") ? 100* hashMap.get("Film-Noir")/count: 0 ;
            double horror=hashMap.containsKey("Horror") ? 100* hashMap.get("Horror")/count: 0 ;
            double musical=hashMap.containsKey("Musical") ? 100* hashMap.get("Musical")/count: 0 ;
            double mystery= hashMap.containsKey("Mystery") ? 100* hashMap.get("Mystery")/count: 0 ;
            double romance= hashMap.containsKey("Romance") ? 100* hashMap.get("Romance")/count: 0 ;
            double scifi=hashMap.containsKey("Sci-Fi") ? 100* hashMap.get("Sci-Fi")/count: 0 ;
            double thriller= hashMap.containsKey("Thriller") ? 100* hashMap.get("Thriller")/count: 0 ;
            double war= hashMap.containsKey("War") ? 100* hashMap.get("War")/count: 0 ;
            double western= hashMap.containsKey("Western") ? 100* hashMap.get("Western")/count: 0 ;
            UserModelXY userModelXY = new UserModelXY(id,action,adventure,animation,children,comedy,crime,documentary,drama,fantasy,filmnoir, horror, musical, mystery, romance,scifi,thriller, war,western);
            userModelXYList.add(userModelXY);
        }
        return userModelXYList;
    }

    public static void main(String args[]){

    }


}
