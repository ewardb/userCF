package DB;

import Entity.Rating;
import Kmeans.UserModel;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by wq on 16-11-24.
 */
public class UtilDB {
    Connection conn = null;
    PreparedStatement pstm = null;
    ResultSet rs = null;

    public List<Rating> getAllData(){
        conn = MysqlDB.getConnection();
        List<Rating> list = new ArrayList<Rating>();
        try{
            String sql = "select movieId,userId,rating from ratings";
            pstm = conn.prepareStatement(sql);
            rs = pstm.executeQuery();
            while(rs.next()){
                int movieId = rs.getInt("movieId");
                int userId = rs.getInt("userId");
                int rating = rs.getInt("rating");
                Rating rat = new Rating(movieId,userId,rating);
                list.add(rat);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            MysqlDB.close(rs,pstm,conn);
        }
        return list;
    }

    public List<UserModel> getUserMovidType(){
        List<Integer> userIdList = new ArrayList<Integer>();
        List<UserModel> typeList = new ArrayList<UserModel>();
        conn = MysqlDB.getConnection();
        try{
            String sql = "select id from users";
            pstm = conn.prepareStatement(sql);
            rs = pstm.executeQuery();
            while(rs.next()){
                Integer id = rs.getInt("id");
                userIdList.add(id);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        System.out.println(userIdList.size());
        //获取所有 用户 并分别获得他们看的电影 频率及看电影的总和。

        try{
            for(int i = 0 ; i<userIdList.size() ; i++){
                int userId = userIdList.get(i);
                String sql = "select type from movies where id in (select movieId from ratings where userId = ?)";
                pstm = conn.prepareStatement(sql);
                pstm.setInt(1,userId);
                UserModel userModel = new UserModel(userId);
                rs = pstm.executeQuery();
                int count = 0 ;
                while(rs.next()){
                    String type = rs.getString(1);
                    String []strs = type.split("\\|");
                    for(String str : strs){
                        HashMap<String,Integer> hashMap =  userModel.getOptions();
                        if(hashMap.containsKey(str)){
                            hashMap.put(str,hashMap.get(str)+1);
                        }else{
                            hashMap.put(str,1);
                        }
                        count++;
                    }
                }
                userModel.setCount(count);
                typeList.add(userModel);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return typeList;
    }

}
