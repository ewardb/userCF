package DB;

//import baseUserCF.BaseUserCf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by wq on 16-11-23.
 */
public class ImportData {
    public static void main(String args[]){
//        String str = "121::Boys of St. Vincent, The (1993)::Drama";
//        String str1 = "1::Toy Story (1995)::Animation|Children's|Comedy";
//        String strs[] = str.split("::");
//        String strs[] = str1.split("::");
//        strs[2] = strs[2].contains("'") ? strs[2].replace("'","\\'") : strs[2];
        System.out.println("klk.");
//        System.out.println("*The ([0-9]{4}*".matches(strs[1]));
//        System.out.println(strs[1].matches(".*The \\([0-9]{4}.*"));
//        System.out.println(".*1995.*".matches(strs[1]));
//        int len = strs[1].length();
//        System.out.println(strs[1].substring(0, len - 12));

//        System.out.println(strs[1].matches(".*1995.*"));
//
//        System.out.println(strs[1]);
//        System.out.println("*The ([0-9]{4}*".matches(strs1[1]));
//        System.out.println(System.getProperty("user.dir")+"/src/main/java/mysql/movies.dat");
//        writeMovies2Database(System.getProperty("user.dir") + "/src/main/java/mysql/movies.dat");

//        writeUser2Database(System.getProperty("user.dir") + "/src/main/java/mysql/users.dat");
//        int id = 1 ;
//        String name = "1name";
//        String email = "1@qq.com";
//        String sql = "insert into user (id,name,email) values ("+id+","+"'" +name+"'"+",'"+email+"')" ;
//        System.out.println(sql);
//        writeRatings2Database(System.getProperty("user.dir") + "/src/main/java/mysql/ratings.dat");

//        System.out.println(System.getProperty("user.dir"));
//
//
//        InputStream ins = null;
//        try{
//
//            String ss = System.getProperty("user.dir");
//            ss = ss+"/src/main/resources";
//            ss = "ls "+ss;
//            Process pro =  Runtime.getRuntime().exec(ss);
//            ins = pro.getInputStream();
//            BufferedReader bf = new BufferedReader(new InputStreamReader(ins));
//            String line = null;
//            while((line = bf.readLine())!=null){
//                System.out.println(line);
//            }
//            int exitValue = pro.exitValue();
//            System.out.print("返回值"+exitValue);
//            pro.getInputStream().close();
//        }catch(Exception e){
//            e.printStackTrace();
//        }
        String ss = System.getProperty("user.dir");
        ss = ss+"\\src\\main\\resources";
//        writeUser2Database(ss+"/users.dat");
//        writeMovies2Database(ss+"\\movies.dat");
        writeRatings2Database(ss+"\\ratings.dat");
        System.out.println(ss);
//        BaseUserCf.splitDate(new UtilDB().getAllData());
    }

    public static void writeMovies2Database(String path){
        Connection conn = MysqlDB.getConnection();
        try{
            FileReader fileReader = new FileReader(path);
            BufferedReader bf = new BufferedReader(fileReader);
            String line = bf.readLine();
            //1::Toy Story (1995)::Animation|Children's|Comedy
            //11::American President, The (1995)::Comedy|Drama|Romance
            String namestrss [] = new String[2];
            //namestrss[1] 表示publishyear
            //namestrss[0] 表示name
            while(line!=null){
                String [] strs = line.split("::");
                if(strs[1].matches(".*,.*The \\([0-9]{4}.*")){
                    int len = strs[1].length();
                    namestrss[1] = strs[1].substring(len-5,len-1);
                    namestrss[0] = strs[1].substring(0,len-12);
                }else{
                    int len = strs[1].length();
                    namestrss[1] = strs[1].substring(len-5,len-1);
                    namestrss[0] = strs[1].substring(0,len-7);
                }
                namestrss[0] = namestrss[0].contains("'") ? namestrss[0].replace("'","\\'") : namestrss[0];
                strs[2] = strs[2].contains("'") ? strs[2].replace("'","\\'") : strs[2];
                String sql = "insert into movies (id,title,published_year,type)values("+Integer.parseInt(strs[0])+", '"+namestrss[0]+"', '"+namestrss[1]+"', '"+strs[2]+"')";
                conn.setAutoCommit(false);
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.execute();
                line = bf.readLine();
                conn.commit();
            }
        }catch (Exception e){
            try{
                conn.rollback();
            }catch (Exception ee){
                ee.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            try{
                conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void writeUser2Database(String path){
        Connection conn = MysqlDB.getConnection();
        try{
            FileReader fileReader = new FileReader(path);
            BufferedReader bf = new BufferedReader(fileReader);
            String line = bf.readLine();
//         1::F::1::10::48067
//          UserID::Gender::Age::Occupation::Zip-code
            while(line!=null){
                String [] strs = line.split("::");
                int id = Integer.valueOf(strs[0]);
                String gender = strs[1];
                int age = Integer.valueOf(strs[2]);
                String occupation = strs[3];
                String zipCode = strs[4];
                String sql = "insert into users (id,gender,age,occupation,zipCode) values ("+id+",'"+gender+"',"+age+",'"+occupation+"','"+zipCode+"')" ;
                conn.setAutoCommit(false);
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.execute();
                line = bf.readLine();
                conn.commit();
            }
        }catch (Exception e){
            try{
                conn.rollback();
            }catch (Exception ee){
                ee.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            try{
                conn.close();
            }catch (Exception ee){
                ee.printStackTrace();
            }
        }
    }

    public static void writeRatings2Database(String path){
        Connection conn = MysqlDB.getConnection();
        try{
            FileReader fileReader = new FileReader(path);
            BufferedReader bf = new BufferedReader(fileReader);
            String line = bf.readLine();
            String sql = "insert into ratings (userID, movieID, rating,timestamp) values (?, ?, ?, ?)";
            PreparedStatement ps = conn.prepareStatement(sql);
            final int batchSize = 200000;
            long count = 0 ;
            conn.setAutoCommit(false);
            while(line !=null){
                String [] strs = line.split("::");
                int userID = Integer.valueOf(strs[0]);
                int movieID =Integer.valueOf(strs[1]);
                int preference =Integer.valueOf(strs[2]);
                int timestamp =Integer.valueOf(strs[3]);
                ps.setInt(1,userID);
                ps.setInt(2,movieID);
                ps.setInt(3,preference);
                ps.setInt(4, timestamp);
                ps.addBatch();
                if(++count % batchSize == 0){
                    ps.executeBatch();
                    conn.commit();
                }
                line = bf.readLine();
            }
            ps.executeBatch();
            conn.commit();
        }catch (Exception e){
            try{
                conn.rollback();
            }catch (Exception ee){
                ee.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            try{
                conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}