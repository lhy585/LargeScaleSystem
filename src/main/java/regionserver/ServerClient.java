package regionserver;

import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

import java.sql.*;
import java.util.Objects;

import static regionserver.ServerMaster.addTable;
import static regionserver.ServerMaster.deleteTable;

public class ServerClient {
    private static ZooKeeperManager zooKeeperManager = new ZooKeeperManager();
    /**
     * 创建表
     * @param tableName 要创建的表名
     * @param sqlCmd 完整的CREATE TABLE SQL语句
     * @return 是否创建成功
     */
    //TODO:region server不和ZooKeeper做过多通信，ZooKeeper监控节点在就行，数据库连接没挂就好了
    // 其他的不管我那边操作了也测试通过了，增删改表region server都不管告知，这一部分有些删了就好
    // 只要：①数据库级别连接ZooKeeper；②数据库数据正常
    public static boolean createTable(String tableName, String sqlCmd) {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();

        try {
            // 创建本地表
            executeUpdate(sqlCmd);

            // 更新ZooKeeper节点
            String serverPath = RegionServer.serverPath;
            String currentValue = zooKeeperManager.getData(serverPath);
            if (currentValue != null && !currentValue.isEmpty()) {
                String newValue = addTable(currentValue, tableName);
                zooKeeperManager.setData(serverPath, newValue);
                return true;
            }
        } catch (Exception e) {
            System.out.println("创建表时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表（从SQL命令中解析表名）
     * @param sqlCmd 完整的CREATE TABLE SQL语句
     * @return 是否创建成功
     */
    public static boolean createTable(String sqlCmd){
        try {
            // 从SQL命令中解析表名
            String tableName = extractTableName(sqlCmd);
            if (tableName == null) {
                System.out.println("无法从SQL命令中解析表名");
                return false;
            }

            // 创建本地表
            executeUpdate(sqlCmd);

            // 更新ZooKeeper节点
            String serverValue = addTable(Objects.requireNonNull(zooKeeperManager.getData(RegionServer.serverPath)), tableName);
            System.out.println(serverValue);
            zooKeeperManager.setData(RegionServer.serverPath, serverValue);
            return true;
        } catch (Exception e) {
            System.out.println(e);
        }
        return false;
    }

    /**
     * 删除表
     * @param tableName 要删除的表名
     * @return 是否删除成功
     */
    public static boolean dropTable(String tableName) {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();

        try {
            // 删除本地表
            String sqlCmd = "DROP TABLE IF EXISTS " + tableName;
            executeUpdate(sqlCmd);

            // 更新ZooKeeper节点
            String serverPath = RegionServer.serverPath;
            String currentValue = zooKeeperManager.getData(serverPath);
            if (currentValue != null && !currentValue.isEmpty()) {
                String newValue = deleteTable(currentValue, tableName);
                zooKeeperManager.setData(serverPath, newValue);
                return true;
            }
        } catch (Exception e) {
            System.out.println("删除表时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 查询表数据
     * @param sqlCmd 查询SQL语句
     * @return 查询结果字符串
     */
    //TODO:这是唯一一个client会问你的
    // public static String selectTable(String ip, String sqlCmd){
    public static String selectTable(String sqlCmd){
        StringBuilder res = new StringBuilder();
        try {
            ResultSet r =  RegionServer.statement.executeQuery(sqlCmd);//TODO:要知道ip啊，不然不知道问哪个子数据库
            ResultSetMetaData rsmd = r.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while(r.next()){
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) res.append(" ");
                    res.append(r.getString(i));
                }
                res.append("\n");
            }
        } catch (Exception e) {
            System.out.println("查询时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return res.toString();
    }

    /**
     * 执行SQL命令
     * @param cmd SQL命令
     * @return 是否执行成功
     */
    //TODO:这是唯一一个master会问你的，有点混了，应该不是在ServerClient里面
    // ip必须要有，拿着ip取正确的数据库线程，然后执行就行，返回一个ture/false,执行就好了
    // 不用管具体什么操作，也不再组合什么语句了，直接返回执行结果，返回值为ResType，可能需要你判断一下操作符，就是第一个小字符串，也简单
    // 想要public static ResType executeCmd(String ip, String cmd)
    public static boolean executeCmd(String cmd){
        try {
            if (cmd.trim().toLowerCase().startsWith("select")) {
                // 如果是查询语句，使用executeQuery
                ResultSet r = RegionServer.statement.executeQuery(cmd);
                ResultSetMetaData rsmd = r.getMetaData();
                int columnCount = rsmd.getColumnCount();

                // 打印列名
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rsmd.getColumnName(i));
                    if (i < columnCount) System.out.print(" ");
                }
                System.out.println();

                // 打印数据
                while(r.next()){
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(r.getString(i));
                        if (i < columnCount) System.out.print(" ");
                    }
                    System.out.println();
                }
            } else {
                // 如果是更新语句，使用executeUpdate
                executeUpdate(cmd);
            }
            return true;
        } catch (Exception e){
            System.out.println("执行命令时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 执行更新操作（INSERT, UPDATE, DELETE, CREATE, DROP等）
     * @param sql SQL语句
     * @throws SQLException 如果SQL执行出错
     */
    public static void executeUpdate(String sql) throws SQLException {
        if (RegionServer.statement == null) {
            throw new SQLException("Statement is not initialized. Please ensure RegionServer is properly initialized.");
        }
        RegionServer.statement.executeUpdate(sql);
    }

    /**
     * 从SQL命令中提取表名
     * @param sql SQL命令
     * @return 表名，如果无法解析则返回null
     */
    static String extractTableName(String sql) {
        try {
            // 简单解析CREATE TABLE语句
            if (sql.trim().toLowerCase().startsWith("create table")) {
                String[] parts = sql.split("\\s+");
                for (int i = 0; i < parts.length; i++) {
                    if ("table".equals(parts[i]) && i + 1 < parts.length) {
                        String tableName = parts[i + 1];
                        // 去掉括号
                        int parenIndex = tableName.indexOf('(');
                        if (parenIndex > 0) {
                            tableName = tableName.substring(0, parenIndex);
                        }
                        return tableName;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("解析表名时出错: " + e.getMessage());
        }
        return null;
    }
}