package mysqlutil;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * JDBC操作工具类, 提供注册驱动, 连接, 发送器, 动态绑定参数, 关闭资源等方法
 * jdbc连接参数的提取, 使用Properties进行优化(软编码)
 *
 * @author
 * @date 2020/6/19 19:41
 */
public class DBUtils {

    private static String driver;
    private static String url;
    private static String user;
    private static String password;

    static {
        // 借助静态代码块保证配置文件只读取一次就行
        // 创建Properties对象
        Properties prop = new Properties();
        try {
            // 加载配置文件, 调用load()方法
            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
            prop.load(DBUtils.class.getClassLoader().getResourceAsStream("db.properties"));
            // 从配置文件中获取数据为成员变量赋值
            driver = prop.getProperty("db.driver").trim();
            url = prop.getProperty("db.url").trim();
            user = prop.getProperty("db.user").trim();
            password = prop.getProperty("db.password").trim();
            // 加载驱动
            Class.forName(driver);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取特定消费者组，主题，分区下的偏移量
     *
     * @return offset
     */
    public static long queryOnlyOffset(String sql, Object... params) {
        Connection conn = getConn();
        long offset = 0;
        PreparedStatement preparedStatement = getPstmt(conn, sql);
        bindParam(preparedStatement, params);

        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong("sub_topic_partition_offset");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet, preparedStatement, conn);
        }

        return offset;
    }
    @Test
    public void testdb(){
        Properties prop = new Properties();
        try {
            // 加载配置文件, 调用load()方法
            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
            prop.load(DBUtils.class.getClassLoader().getResourceAsStream("db.properties"));
            // 从配置文件中获取数据为成员变量赋值
            driver = prop.getProperty("db.driver").trim();
            url = prop.getProperty("db.url").trim();
            user = prop.getProperty("db.user").trim();
            password = prop.getProperty("db.password").trim();
            // 加载驱动

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取特定消费者组，主题，分区下的偏移量
     *
     * @return offset
     */
    public static Map<TopicPartition, Long> queryOffset(String sql,String topic, Object... params) {
        Connection conn = getConn();
        PreparedStatement preparedStatement = getPstmt(conn, sql);
        bindParam(preparedStatement, params);

        ResultSet resultSet = null;
        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
        try {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long sub_topic_partition_offset = resultSet.getLong("sub_topic_partition_offset");
                int sub_topic_partition_id = resultSet.getInt("sub_topic_partition_id");
                TopicPartition topicPartition = new TopicPartition(topic, sub_topic_partition_id);
                offsets.put(topicPartition, sub_topic_partition_offset);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet, preparedStatement, conn);
        }

        return offsets;
    }

    /**
     * 动态绑定参数
     *
     * @param pstmt
     * @param params
     */
    public static void bindParam(PreparedStatement pstmt, Object... params) {
        try {
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 预处理发送器
     *
     * @param conn
     * @param sql
     * @return
     */
    public static PreparedStatement getPstmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    /**
     * 获取发送器的方法
     *
     * @param conn
     * @return
     */
    public static Statement getStmt(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stmt;
    }

    /**
     * 获取数据库连接的方法
     *
     * @return
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }



    /**
     * 根据特定消费者组，主题，分区，更新偏移量
     *offset
     * @param
     */
    public static void update(String sql, Object... params) {
        Connection conn = getConn();
        PreparedStatement preparedStatement = getPstmt(conn, sql);

        bindParam(preparedStatement,params);

        try {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(null, preparedStatement, conn);
        }

    }


    /**
     * 统一关闭资源
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String sql = "select dataTaskName from datataskinfor_bak";
        Connection conn = getConn();
        PreparedStatement preparedStatement = getPstmt(conn, sql);
        bindParam(preparedStatement, "");

        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String string = resultSet.getString("dataTaskName");
                System.out.println(string);
            }
        }catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}



