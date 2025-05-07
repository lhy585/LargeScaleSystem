//package regionserver;
//import java.io.IOException;
//import java.sql.*;
//
//public class JdbcUtilsTest {
//    public static void main(String[] args) {
//        // 测试本地 MySQL 连接
//        testLocalConnection();
//
//        // 测试使用自定义用户名和密码的连接
//        testCustomConnection("root", "040517cc");
//
//        // 测试远程 MySQL 连接
//        testRemoteConnection("127.0.0.1", "root", "040517cc");
//    }
//
//    public static void testLocalConnection() {
//        Connection conn = JdbcUtils.getConnection();
//        if (conn != null) {
//            System.out.println("Successfully connected to MySQL (local)!");
//        } else {
//            System.out.println("Failed to connect to MySQL (local).");
//        }
//    }
//
//    public static void testCustomConnection(String user, String password) {
//        Connection conn = JdbcUtils.getConnection(user, password);
//        if (conn != null) {
//            System.out.println("Successfully connected to MySQL (custom user)!");
//        } else {
//            System.out.println("Failed to connect to MySQL (custom user).");
//        }
//    }
//
//    public static void testRemoteConnection(String ip, String user, String password) {
//        Connection conn = JdbcUtils.getConnection(ip, user, password);
//        if (conn != null) {
//            System.out.println("Successfully connected to MySQL (remote)!");
//        } else {
//            System.out.println("Failed to connect to MySQL (remote).");
//        }
//    }
//}
