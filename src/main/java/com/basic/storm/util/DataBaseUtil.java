package com.basic.storm.util;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/9.
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class DataBaseUtil {
    private String driver;

    private String url;

    private String user;

    private String password;

    private Connection conn;

    private static volatile DataBaseUtil dataBaseUtil = null;

    public DataBaseUtil() {
        loadProperties("/conn.properties");
        setConnection();
    }

    public static DataBaseUtil getDataBaseUtilInstance(){
        if(null == dataBaseUtil)
        {
            synchronized (DataBaseUtil.class)
            {
                dataBaseUtil=new DataBaseUtil();
            }
        }
        return dataBaseUtil;
    }

    // handle the properties file to get the informations for connection
    private void loadProperties(String fileName) {
        Properties props = new Properties();
        try {
            InputStream in = Object. class .getResourceAsStream( fileName );
            props.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.driver = props.getProperty("driverClassName");
        this.url = props.getProperty("url");
        this.user = props.getProperty("username");
        this.password = props.getProperty("password");
    }

    private void setConnection() {
        try {
            Class.forName(driver);
            this.conn = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException classnotfoundexception) {
            System.err.println("db: " + classnotfoundexception.getMessage());
        } catch (SQLException sqlexception) {
            System.err.println("db.getconn(): " + sqlexception.getMessage());
        }
    }

    public Connection getConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                return this.conn;
            } else {
                setConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this.conn;
    }

    public void closeConnection() {
        try {
            if (st != null) {
                st.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 增加，修改，删除数据
     *
     * @param sql
     * @param sqlValue
     * @return
     * @throws Exception
     *             用法： sql="UPDATE user SET password = ? WHERE phone= ?"; String
     *             sql="INSERT INTO payee(name,card,openBank,addDate,addPhone)
     *             VALUES(?,?,?,?,?)"; delete ... db.executeUpdate(sql, new
     *             String[]{name,card,openBank,addDate,addPhone});
     */
    public int doExecute(String sql, String sqlValue[]) {
        int count = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            if (sqlValue != null) {
                for (int i = 0; i < sqlValue.length; i++)
                    ps.setString(i + 1, sqlValue[i]);
            }
            count = ps.executeUpdate();
        } catch (Exception e) {
        }
        return count;
    }

    /**
     * 提供更通用点的方法
     *
     * @param sql
     * @return
     */
    public int doExecute(String sql) {
        return doExecute(sql, null);
    }

    /**
     * 获得查询数据，返回一个ArrayList 得到的Arraylist 可以用 Map解析 用法： Arraylist list =
     * db.getList("select * from user where ***") for(int i=1;i<list.size;i++)
     * Map map = (Map)list.get(i); map.get("id"); 就是得到了里面的id了
     *
     */
    public ArrayList getList(String sql) {
        Statement st = null;
        ArrayList list = new ArrayList();
        try {
            // PreparedStatement ps = conn.prepareStatement(sql);
            // ResultSet rs=ps.executeQuery();
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            String cols[] = new String[count];
            for (int i = 0; i < cols.length; i++) {
                if (meta.getColumnName(i + 1) != null)
                    cols[i] = meta.getColumnName(i + 1);
                else
                    cols[i] = meta.getColumnLabel(i + 1);
            }
            HashMap map = null;
            String fieldValue = null;
            for (; rs.next(); list.add(map)) {
                map = new HashMap();
                for (int i = 0; i < cols.length; i++) {
                    int iType = meta.getColumnType(i + 1);
                    if (iType == 2 || iType == 3) {
                        if (meta.getScale(i + 1) == 0)
                            fieldValue = String.valueOf(rs.getLong(i + 1));
                        else
                            fieldValue = rs.getString(i + 1);
                    } else if (iType == 8)
                        fieldValue = String.valueOf(rs.getDouble(i + 1));
                    else if (iType == 6 || iType == 7)
                        fieldValue = String.valueOf(rs.getFloat(i + 1));
                    else
                        fieldValue = rs.getString(i + 1);
                    if (fieldValue == null)
                        fieldValue = "";
                    else
                        fieldValue = fieldValue.trim();
                    map.put(cols[i], fieldValue);// .toLowerCase()
                }
            }
        } catch (Exception e) {
        }
        return list;
    }

    /**
     * 获得记录数
     *
     * @param sql
     * @return 用法： String sql="select count(*) from bill where ... ;
     *         count=db.executeQuery(sql);
     */
    public int getCount(String sql) {
        // String sql="select count(*) from table where ...";
        ResultSet rs = null;
        PreparedStatement ps;
        int count = 0;
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            rs.next();
            count = rs.getInt(1);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 以下4个方法都可以用doExecute(),getList()替代
     *
     * @param args
     */
    private Statement st = null;
    private PreparedStatement preparedStatement=null;

    public void doInsert(String sql) {
        try {
            st = conn.createStatement();
            int i = st.executeUpdate(sql);
        } catch (SQLException sqlexception) {
            System.err.println("db.executeInset:" + sqlexception.getMessage());
        }
    }

    /**
     *
     * @param sql
     * @param fileds
     * @throws SQLException
     */
    public void doInsert(String sql,Object... fileds) throws SQLException {

        preparedStatement = conn.prepareStatement(sql);
        for(int i=0;i<fileds.length;i++){
            preparedStatement.setObject(i+1,fileds[i]);
        }
        int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
        preparedStatement.closeOnCompletion();
    }

    public void doDelete(String sql) {
        try {
            st = conn.createStatement();
            int i = st.executeUpdate(sql);
        } catch (SQLException sqlexception) {
            System.err.println("db.executeDelete:" + sqlexception.getMessage());
        }
    }

    public void doUpdate(String sql) {
        try {
            st = conn.createStatement();
            int i = st.executeUpdate(sql);
        } catch (SQLException sqlexception) {
            System.err.println("db.executeUpdate:" + sqlexception.getMessage());
        }
    }

    public ResultSet doSelect(String sql) {
        ResultSet rs = null;
        try {
            st = conn.createStatement(
                    java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            rs = st.executeQuery(sql);
        } catch (SQLException sqlexception) {
            System.err.println("db.executeQuery: " + sqlexception.getMessage());
        }
        return rs;
    }

    /**
     * 以下3个为保留方法
     */
    public void beginTransaction() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void commitTransaction() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollbackTransaction() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

