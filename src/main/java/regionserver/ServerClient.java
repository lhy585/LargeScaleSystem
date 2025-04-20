package regionserver;

import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

import java.sql.*;
import java.util.Objects;

import static regionserver.ServerMaster.addTable;
import static regionserver.ServerMaster.deleteTable;

public class ServerClient {
    private static ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();
    /**
     * 创建表
     * @param tableName 要创建的表名
     * @param sqlCmd 完整的CREATE TABLE SQL语句
     * @return 是否创建成功
     */
    public static boolean createTable(String tableName, String sqlCmd) {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();

        try {
            // 创建本地表
            RegionServer.statement.execute(sqlCmd);

            // 更新ZooKeeper节点
            String serverPath = RegionServer.serverPath;
            String currentValue = zooKeeperManager.getRegionServer(serverPath);
            if (currentValue != null) {
                String newValue = addTable(currentValue, tableName);
                zooKeeperUtils.setData(serverPath, newValue);
                return true;
            }
        } catch (Exception e) {
            System.out.println("创建表时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public static boolean createTable(String sqlCmd){
//        int index = cmd.indexOf("@");
//        String sqlCmd = cmd.substring(0,index-1);
//        String slaveInfoString = cmd.substring(index+2);
//        String [] slaveInfo = slaveInfoString.split(",");

        String tableName = sqlCmd.split(" ")[2];
        tableName = tableName.substring(0,tableName.indexOf("("));
        System.out.println(sqlCmd);
        System.out.println(tableName);
//        for (String i: slaveInfo) {
//            System.out.println(i);
//        }

        try {
            // 创建本地表
            RegionServer.statement.execute(sqlCmd);
            // 创建zookeeperNode
            String serverValue = addTable(Objects.requireNonNull(zooKeeperUtils.getData(RegionServer.serverPath)), tableName);
            System.out.println(serverValue);
            zooKeeperUtils.setData(RegionServer.serverPath, serverValue);
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
        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();

        try {
            // 删除本地表
            String sqlCmd = "DROP TABLE IF EXISTS " + tableName;
            RegionServer.statement.execute(sqlCmd);

            // 更新ZooKeeper节点
            String serverPath = RegionServer.serverPath;
            String currentValue = zooKeeperUtils.getData(serverPath);
            if (currentValue != null) {
                String newValue = deleteTable(currentValue, tableName);
                zooKeeperUtils.setData(serverPath, newValue);
                return true;
            }
        } catch (Exception e) {
            System.out.println("删除表时出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

//    public static boolean dropTable(String cmd){
//        String tableName = cmd.split(" ")[2];
//        try {
//            // 删除本地表
//            RegionServer.statement.execute(cmd);
//            // 创建zookeeperNode
//            String serverValue = ServerMaster.deleteTable(Objects.requireNonNull(zooKeeperUtils.getData(RegionServer.serverPath)), tableName);
//            System.out.println(serverValue);
//            zooKeeperUtils.setData(RegionServer.serverPath, serverValue);
//            return true;
//        } catch (Exception e) {
//            System.out.println(e);
//        }
//        return false;
//    }

    public static String selectTable(String cmd){
        String res = "";
        try {
            ResultSet r =  RegionServer.statement.executeQuery(cmd);
            ResultSetMetaData rsmd = r.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while(r.next()){
                if (columnCount >= 1)
                    res +=  r.getString(1);
                for (int i = 2; i<= columnCount;i++){
                    res +=  " " + r.getString(i);
                }
                res += "\n";
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return res;
    }

    public static boolean executeCmd(String cmd){
        try {
            RegionServer.statement.execute(cmd);
            return true;
        } catch (Exception e){
            System.out.println(e);
        }
        return false;
    }

}