package DB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.util.Properties;

/**
 * Created by wq on 16-11-23.
 */
public class MysqlDB {
    private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
    private static String driver;
    private static String url;
    private static String uname;
    private static String pwd;
    //
    static {
        try {
            Properties properties = new Properties();
            //把文件对象封装为 字节流（文件写在src resources下  或者 写道 src下！！）
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("DBconfig.properties");
            //把流 key value 的形式  加载到集合对象中。
            properties.load(inputStream);
            driver = properties.getProperty("driver");
            url = properties.getProperty("url");
            uname = properties.getProperty("uname");
            pwd = properties.getProperty("pwd");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection(){
        Connection conn = null;
        //1:注册驱动¯
        try {
            Class.forName(driver);
            //2:获取连接
            conn = DriverManager.getConnection(url, uname, pwd);
            threadLocal.set(conn);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException("链接数据库失败！！！",e);
        }
        return conn;
    }
    /**
     * 开启事务
     * @param conn
     * @throws SQLException
     */
    public static void startTransaction(Connection conn) throws SQLException{
        conn.setAutoCommit(false);
    }
    /**
     * 提交事务
     * @param conn
     * @throws SQLException
     */
    public static void commit(Connection conn) throws SQLException{
        conn.commit();
    }
    /**
     * 回滚事务
     * @param conn
     * @throws SQLException
     */
    public static void rollback(Connection conn){
        try {
            conn.rollback();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * 数据库的关闭
     * @param rs
     * @param pstm
     * @param conn
     */
    public static void close(ResultSet rs,Statement pstm,Connection conn){
        if(rs != null){
            try{
                rs.close();
            }catch(SQLException e){
                e.printStackTrace();
                throw new RuntimeException("关闭失败",e);
            }

        }
        if(pstm != null){
            try{
                pstm.close();
            }catch(SQLException e){
                e.printStackTrace();
                throw new RuntimeException("关闭失败",e);
            }

        }
        if(conn != null){
            try{
                conn.close();
            }catch(SQLException e){
                e.printStackTrace();
                throw new RuntimeException("关闭失败",e);
            }

        }
    }
}
