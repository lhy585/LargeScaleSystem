//package regionserver;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//public class ServerClientTestRunner {
//    private static Connection connection;
//    private static Statement statement;
//
//    public static void main(String[] args) {
//        System.out.println("Starting ServerClient tests...");
//
//        try {
//            // 初始化RegionServer
//            initRegionServer();
//
//            // 确保statement被正确设置
//            if (RegionServer.statement == null) {
//                // 如果RegionServer未初始化statement，则手动初始化
//                if (connection == null) {
//                    System.out.println("Initializing database connection...");
//                    connection = JdbcUtils.getConnection("root", "040517cc");
//                }
//                if (connection != null) {
//                    System.out.println("Creating statement...");
//                    statement = connection.createStatement();
//                    RegionServer.statement = statement; // 将statement赋值给RegionServer
//                } else {
//                    throw new SQLException("Failed to establish database connection.");
//                }
//            }
//
//            // 清理测试数据
//            cleanupDatabase();
//
//            // 运行所有测试
//            runAllTests();
//
//            // 关闭连接
//            closeConnection();
//
//            System.out.println("All tests completed.");
//        } catch (Exception e) {
//            System.err.println("Test failed with exception: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    private static void initRegionServer() throws SQLException {
//        // 初始化RegionServer
//        RegionServer.initRegionServer();
//
//        // 确保RegionServer.statement被正确设置
//        if (RegionServer.statement == null && connection != null) {
//            RegionServer.statement = connection.createStatement();
//        }
//    }
//
//    private static void cleanupDatabase() throws SQLException {
//        System.out.println("Cleaning up database...");
//        if (statement == null) {
//            throw new SQLException("Statement is not initialized");
//        }
//
//        // 删除已有数据库
//        String deleteDB = "DROP DATABASE IF EXISTS lss";
//        statement.execute(deleteDB);
//        // 重新创建数据库
//        String createDB = "CREATE DATABASE IF NOT EXISTS lss";
//        statement.execute(createDB);
//
//        // 使用新创建的数据库
//        String useDB = "USE lss";
//        statement.execute(useDB);
//    }
//
//    private static void runAllTests() throws SQLException {
//        System.out.println("\nRunning createTable test...");
//        testCreateTable();
//
//        System.out.println("\nRunning createTableFromCmd test...");
//        testCreateTableFromCmd();
//
//        System.out.println("\nRunning dropTable test...");
//        testDropTable();
//
//        System.out.println("\nRunning selectTable test...");
//        testSelectTable();
//
//        System.out.println("\nRunning executeCmd test...");
//        testExecuteCmd();
//
//        System.out.println("\nRunning extractTableName test...");
//        testExtractTableName();
//    }
//
//    private static void closeConnection() throws SQLException {
//        if (statement != null) {
//            statement.close();
//            statement = null;
//        }
//        if (connection != null) {
//            connection.close();
//            connection = null;
//        }
//    }
//
//    // 测试方法保持不变，但确保RegionServer.statement已被正确初始化
//    private static void testCreateTable() throws SQLException {
//        String sql = "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))";
//        boolean result = ServerClient.createTable("test_table", sql);
//
//        if (result) {
//            System.out.println("createTable test PASSED");
//
//            // 验证表是否存在
//            try {
//                statement.executeQuery("SELECT * FROM test_table");
//                System.out.println("Warning: Table exists but should be empty (no data)");
//            } catch (SQLException e) {
//                // 预期行为，表存在但没有数据
//            }
//        } else {
//            System.out.println("createTable test FAILED");
//        }
//    }
//
//    private static void testCreateTableFromCmd() throws SQLException {
//        String sql = "CREATE TABLE another_table (id INT, description TEXT)";
//        boolean result = ServerClient.createTable(sql);
//
//        if (result) {
//            System.out.println("createTableFromCmd test PASSED");
//
//            // 验证表是否存在
//            try {
//                statement.executeQuery("SELECT * FROM another_table");
//                System.out.println("Warning: Table exists but should be empty (no data)");
//            } catch (SQLException e) {
//                // 预期行为，表存在但没有数据
//            }
//        } else {
//            System.out.println("createTableFromCmd test FAILED");
//        }
//    }
//
//    private static void testDropTable() throws SQLException {
//        String createSql = "CREATE TABLE to_drop (id INT)";
//        statement.execute(createSql);
//
//        boolean result = ServerClient.dropTable("to_drop");
//
//        if (result) {
//            System.out.println("dropTable test PASSED");
//
//            // 验证表是否被删除
//            try {
//                statement.executeQuery("SELECT * FROM to_drop");
//                System.out.println("dropTable test FAILED - table still exists");
//            } catch (SQLException e) {
//                // 预期行为
//            }
//        } else {
//            System.out.println("dropTable test FAILED");
//        }
//    }
//
//    private static void testSelectTable() throws SQLException {
//        String createSql = "CREATE TABLE test_select (id INT, value VARCHAR(50))";
//        statement.execute(createSql);
//
//        String insertSql = "INSERT INTO test_select VALUES (1, 'test'), (2, 'data')";
//        statement.execute(insertSql);
//
//        String result = ServerClient.selectTable("SELECT * FROM test_select");
//
//        if (result.contains("1 test") && result.contains("2 data")) {
//            System.out.println("selectTable test PASSED");
//        } else {
//            System.out.println("selectTable test FAILED");
//            System.out.println("Result was: " + result);
//        }
//    }
//
//    private static void testExecuteCmd() throws SQLException {
//        String createSql = "CREATE TABLE test_execute (id INT)";
//        boolean result = ServerClient.executeCmd(createSql);
//
//        if (!result) {
//            System.out.println("executeCmd (create) test FAILED");
//            return;
//        }
//
//        String insertSql = "INSERT INTO test_execute VALUES (1), (2), (3)";
//        result = ServerClient.executeCmd(insertSql);
//
//        if (!result) {
//            System.out.println("executeCmd (insert) test FAILED");
//            return;
//        }
//
//        // 测试查询（只验证是否执行成功）
//        result = ServerClient.executeCmd("SELECT * FROM test_execute");
//        if (!result) {
//            System.out.println("executeCmd (select) test FAILED");
//            return;
//        }
//
//        // 验证查询结果
//        ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM test_execute");
//        rs.next();
//        int count = rs.getInt(1);
//        if (count == 3) {
//            System.out.println("executeCmd test PASSED");
//        } else {
//            System.out.println("executeCmd test FAILED - expected 3 rows, got " + count);
//        }
//    }
//
//    private static void testExtractTableName() throws SQLException {
//        // 假设有一个测试表
//        String createSql = "CREATE TABLE test_extract (id INT)";
//        statement.execute(createSql);
//
//        String sql = "CREATE TABLE another_test (id INT)";
//        String tableName = ServerClient.extractTableName(sql);
//
//        if (tableName != null && tableName.equals("another_test")) {
//            System.out.println("extractTableName test PASSED");
//        } else {
//            System.out.println("extractTableName test FAILED");
//            System.out.println("Extracted table name: " + tableName);
//        }
//    }
//}