package DB;

import Entity.Rating;

import java.sql.*;
import java.util.ArrayList;
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


}
