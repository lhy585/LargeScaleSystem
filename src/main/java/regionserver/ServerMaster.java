package regionserver;

import zookeeper.ZooKeeperUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Objects;

//TODO:只要public static ResType executeCmd(String ip, String cmd)
// 现在这个函数写在ServerClient里面了，要剪切过来
// 删除所有有关告知ZooKeeper表信息的函数
// PS：请不要修改region manager类的任何地方
//public class ServerMaster {
//    /**
//     * 从主服务器创建表的副本
//     * @param sourceServerName 源服务器名称
//     * @param sourceIp 源服务器IP
//     * @param sourcePort 源服务器端口
//     * @param sourceUser 源服务器用户名
//     * @param sourcePwd 源服务器密码
//     * @param tableName 要复制的表名
//     */
//    public static void dumpTable(String sourceServerName, String sourceIp, String sourcePort,
//                                 String sourceUser, String sourcePwd, String tableName) {
//        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();
//
//        // 从源服务器导出表结构
//        JdbcUtils.dumpRemoteSql(tableName, sourceIp, sourceUser, sourcePwd);
//        File file = new File("./sql/lss." + tableName + ".sql");
//
//        if (file.exists()) {
//            // 在本地创建从表
//            String[] mysqlCmd = {
//                    "mysql",
//                    "-u", RegionServer.mysqlUser,
//                    "-h", "localhost",
//                    "-p" + RegionServer.mysqlPwd,
//                    "-e", "source ./sql/lss." + tableName + ".sql"
//            };
//
//            try {
//                ProcessBuilder pb = new ProcessBuilder(mysqlCmd);
//                pb.redirectErrorStream(true);
//                Process process = pb.start();
//                int exitCode = process.waitFor();
//
//                if (exitCode != 0) {
//                    System.out.println("创建从表失败");
//                    return;
//                }
//
//                // 更新ZooKeeper节点 - 在当前服务器添加从表
//                String serverPath = RegionServer.serverPath;
//                String currentValue = zooKeeperUtils.getData(serverPath);
//                if (currentValue != null) {
//                    String newValue = addTable(currentValue, tableName + "_slave");
//                    zooKeeperUtils.setData(serverPath, newValue);
//                }
//
//                // 在源服务器节点中将从表升级为主表
//                String masterServerPath = "/lss/region_servers/" + sourceServerName;
//                String masterNodeData = zooKeeperUtils.getData(masterServerPath);
//                if (masterNodeData != null) {
//                    String[] parts = masterNodeData.split(",");
//                    for (int i = 6; i < parts.length; i++) {
//                        if (parts[i].equals(tableName + "_slave")) {
//                            parts[i] = tableName;
//                        }
//                    }
//
//                    // 更新表计数
//                    parts[5] = String.valueOf(parts.length - 6);
//
//                    // 重建节点值
//                    StringBuilder newMasterValue = new StringBuilder();
//                    for (String part : parts) {
//                        newMasterValue.append(part).append(",");
//                    }
//                    zooKeeperUtils.setData(masterServerPath,
//                            newMasterValue.substring(0, newMasterValue.length() - 1));
//                }
//            } catch (Exception e) {
//                System.out.println("表复制过程中出错: " + e.getMessage());
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("创建表导出文件失败");
//        }
//    }
//
//    /**
//     * 将表从一个服务器迁移到另一个服务器
//     * @param sourceServerName 源服务器名称
//     * @param sourceIp 源服务器IP
//     * @param sourcePort 源服务器端口
//     * @param sourceUser 源服务器用户名
//     * @param sourcePwd 源服务器密码
//     * @param tableName 要迁移的表名
//     */
//    public static void migrateTable(String sourceServerName, String sourceIp, String sourcePort,
//                                    String sourceUser, String sourcePwd, String tableName) {
//        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();
//
//        // 从源服务器导出表
//        JdbcUtils.dumpRemoteSql(tableName, sourceIp, sourceUser, sourcePwd);
//        File file = new File("./sql/lss." + tableName + ".sql");
//
//        if (file.exists()) {
//            // 在本地创建表
//            String[] mysqlCmd = {
//                    "mysql",
//                    "-u", RegionServer.mysqlUser,
//                    "-h", "localhost",
//                    "-p" + RegionServer.mysqlPwd,
//                    "-e", "source ./sql/lss." + tableName + ".sql"
//            };
//
//            try {
//                ProcessBuilder pb = new ProcessBuilder(mysqlCmd);
//                pb.redirectErrorStream(true);
//                Process process = pb.start();
//                int exitCode = process.waitFor();
//
//                if (exitCode != 0) {
//                    System.out.println("创建迁移表失败");
//                    return;
//                }
//
//                // 更新ZooKeeper - 添加到当前服务器
//                String serverPath = RegionServer.serverPath;
//                String currentValue = zooKeeperUtils.getData(serverPath);
//                if (currentValue != null) {
//                    String newValue = addTable(currentValue, tableName);
//                    zooKeeperUtils.setData(serverPath, newValue);
//                }
//
//                // 从源服务器删除表
//                Connection connection = JdbcUtils.getConnection(
//                        sourceIp, sourceUser, sourcePwd);
//                if (connection != null) {
//                    try (Statement statement = connection.createStatement()) {
//                        statement.execute("DROP TABLE IF EXISTS " + tableName);
//                    } finally {
//                        JdbcUtils.releaseResc(null, null, connection);
//                    }
//                }
//
//                // 更新ZooKeeper - 从源服务器删除表
//                String sourceServerPath = "/lss/region_servers/" + sourceServerName;
//                String sourceNodeData = zooKeeperUtils.getData(sourceServerPath);
//                if (sourceNodeData != null) {
//                    String newSourceValue = deleteTable(sourceNodeData, tableName);
//                    zooKeeperUtils.setData(sourceServerPath, newSourceValue);
//                }
//            } catch (Exception e) {
//                System.out.println("表迁移过程中出错: " + e.getMessage());
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("创建表导出文件失败");
//        }
//    }
//
//    /**
//     * 从ZooKeeper节点数据中删除表
//     * @param serverValue 服务器节点值
//     * @param tableName 要删除的表名
//     * @return 修改后的节点值
//     */
//    public static String deleteTable(String serverValue, String tableName) {
//        String[] parts = serverValue.split(",");
//        parts[5] = String.valueOf(Integer.parseInt(parts[5]) - 1); // 减少表计数
//        StringBuilder newValue = new StringBuilder();
//
//        for (String part : parts) {
//            if (!part.equals(tableName)) {
//                newValue.append(part).append(",");
//            }
//        }
//        return newValue.substring(0, newValue.length() - 1);
//    }
//
//    /**
//     * 向ZooKeeper节点数据中添加表
//     * @param serverValue 服务器节点值
//     * @param tableName 要添加的表名
//     * @return 修改后的节点值
//     */
//    public static String addTable(String serverValue, String tableName) {
//        String[] parts = serverValue.split(",");
//        parts[5] = String.valueOf(Integer.parseInt(parts[5]) + 1); // 增加表计数
//        StringBuilder newValue = new StringBuilder();
//
//        for (String part : parts) {
//            newValue.append(part).append(",");
//        }
//        return newValue.append(tableName).toString();
//    }
//}
public class ServerMaster {
    /**
     * 从主服务器创建表的副本
     * @param sourceServerName 源服务器名称
     * @param sourceIp 源服务器IP
     * @param sourcePort 源服务器端口
     * @param sourceUser 源服务器用户名
     * @param sourcePwd 源服务器密码
     * @param tableName 要复制的表名
     */
    public static void dumpTable(String sourceServerName, String sourceIp, String sourcePort,
                                 String sourceUser, String sourcePwd, String tableName) {
        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();

        // 从源服务器导出表结构
        JdbcUtils.dumpRemoteSql(tableName, sourceIp, sourceUser, sourcePwd);
        File file = new File("./sql/lss." + tableName + ".sql");

        if (file.exists()) {
            // 在本地创建从表
            String[] mysqlCmd = {
                    "mysql",
                    "-u", RegionServer.mysqlUser,
                    "-h", "localhost",
                    "-p" + RegionServer.mysqlPwd,
                    "-e", "source ./sql/lss." + tableName + ".sql"
            };

            try {
                ProcessBuilder pb = new ProcessBuilder(mysqlCmd);
                pb.redirectErrorStream(true);
                Process process = pb.start();
                int exitCode = process.waitFor();

                if (exitCode != 0) {
                    System.out.println("创建从表失败");
                    return;
                }

                // 更新ZooKeeper节点 - 在当前服务器添加从表
                String serverPath = RegionServer.serverPath;
                String currentValue = zooKeeperUtils.getData(serverPath);
                if (currentValue != null) {
                    String newValue = addTable(currentValue, tableName + "_slave");
                    zooKeeperUtils.setData(serverPath, newValue);
                }

                // 在源服务器节点中将从表升级为主表
                String masterServerPath = "/lss/region_servers/" + sourceServerName;
                String masterNodeData = zooKeeperUtils.getData(masterServerPath);
                if (masterNodeData != null) {
                    String[] parts = masterNodeData.split(",");
                    for (int i = 6; i < parts.length; i++) {
                        if (parts[i].equals(tableName + "_slave")) {
                            parts[i] = tableName;
                        }
                    }

                    // 更新表计数
                    parts[5] = String.valueOf(parts.length - 6);

                    // 重建节点值
                    StringBuilder newMasterValue = new StringBuilder();
                    for (String part : parts) {
                        newMasterValue.append(part).append(",");
                    }
                    zooKeeperUtils.setData(masterServerPath,
                            newMasterValue.substring(0, newMasterValue.length() - 1));
                }
            } catch (Exception e) {
                System.out.println("表复制过程中出错: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("创建表导出文件失败");
        }
    }

    public static void dumpTable(String cmd){
        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();
        int index = cmd.indexOf("@");
        String tableName = cmd.substring(cmd.indexOf(" " + 1),index - 1);
        String masterInfoString = cmd.substring(index+2);
        String [] masterInfo = masterInfoString.split(",");

        System.out.println(tableName);
        for (String i: masterInfo){
            System.out.println(i);
        }

        JdbcUtils.dumpRemoteSql(tableName, masterInfo[1], masterInfo[3], masterInfo[4]);
        File file = new File("./sql/lss." + tableName + ".sql");
        if (file.exists()){
            // 创建本地表
            String  str="mysql -u " + RegionServer.mysqlUser +
                    " -h " + "localhost" +
                    " -p" + RegionServer.mysqlPwd +
                    " < ./sql/lss." + tableName + ".sql";
            try {
                Runtime rt =Runtime.getRuntime();
                rt.exec("cmd /c " + str);
            } catch (IOException e){
                System.out.println(e);
                System.out.println(e.getStackTrace());
            }

            try {
                // 创建zookeeperNode
                String serverPath = RegionServer.serverPath;
                String remoteValue = addTable(Objects.requireNonNull(zooKeeperUtils.getData(serverPath)), tableName + "_slave");
                zooKeeperUtils.setData(serverPath, remoteValue);

                // 将原来副本升级为主表
                serverPath = "/lss/region_servers/" + masterInfo[0];
                String [] temp = zooKeeperUtils.getData(serverPath).split(",");

                for (int i = 6;i<temp.length;i++){
                    if (temp[i].equals(tableName + "_slave")){
                        temp[i] = tableName;
                    }
                }

                StringBuilder newValue = new StringBuilder();
                for (String i: temp){
                    newValue.append(i).append(",");
                }
                zooKeeperUtils.setData(serverPath, newValue.substring(0,newValue.toString().length()-1));

            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            System.out.println("创建副本失败");
        }
    }

    public static void migrateTable(String cmd){
        ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();
        int index = cmd.indexOf("@");
        String tableName = cmd.substring(cmd.indexOf(" " + 1),index - 1);
        String masterInfoString = cmd.substring(index+2);
        String [] masterInfo = masterInfoString.split(",");

        JdbcUtils.dumpRemoteSql(tableName, masterInfo[1], masterInfo[3], masterInfo[4]);
        File file = new File("./sql/lss." + tableName + ".sql");
        if (file.exists()){
            // 创建本地表
            String  str="mysql -u " + RegionServer.mysqlUser +
                    " -h " + "localhost" +
                    " -p" + RegionServer.mysqlPwd +
                    " < ./sql/lss." + tableName + ".sql";
            try {
                Runtime rt =Runtime.getRuntime();
                rt.exec("cmd /c " + str);
            } catch (IOException e){
                System.out.println(e);
                System.out.println(e.getStackTrace());
            }

            try {
                // 创建zookeeperNode
                String serverPath = RegionServer.serverPath;
                String remoteValue = addTable(Objects.requireNonNull(zooKeeperUtils.getData(serverPath)), tableName);
                zooKeeperUtils.setData(serverPath, remoteValue);

                // 将原来表删除
                // 删除本地表
                Connection connection = JdbcUtils.getConnection(masterInfo[1],masterInfo[3],masterInfo[4]);
                assert connection != null;
                Statement statement =  connection.createStatement();
                statement.execute("drop table if exists "+tableName);

                JdbcUtils.releaseResc(null,statement,connection);

                // 创建zookeeperNode
                serverPath = "/lss/region_servers/" + masterInfo[0];
                remoteValue = deleteTable(Objects.requireNonNull(zooKeeperUtils.getData(serverPath)), tableName + "_slave");
                zooKeeperUtils.setData(serverPath, remoteValue);

            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            System.out.println("创建副本失败");
        }
    }

    /**
     * 迁移表到新的region server
     * @param sourceRegionName 源region名称
     * @param targetRegionName 目标region名称
     * @param tableName 要迁移的表名
     * @return 是否迁移成功
     */
    public static boolean migrateTable(String sourceRegionName, String targetRegionName, String tableName) {
        try {
            ZooKeeperUtils zooKeeperUtils = new ZooKeeperUtils();

            // 1. 从源region server导出表数据
            String sourceServerPath = "/lss/region_servers/" + sourceRegionName;
            String sourceServerInfo = zooKeeperUtils.getData(sourceServerPath);
            String[] sourceInfo = sourceServerInfo.split(",");

            JdbcUtils.dumpRemoteSql(tableName, sourceInfo[1], sourceInfo[3], sourceInfo[4]);
            File file = new File("./sql/lss." + tableName + ".sql");

            if (!file.exists()) {
                System.out.println("创建副本失败: 导出文件不存在");
                return false;
            }

            // 2. 在目标region server创建表
            String targetServerPath = "/lss/region_servers/" + targetRegionName;
            String targetServerInfo = zooKeeperUtils.getData(targetServerPath);
            String[] targetInfo = targetServerInfo.split(",");

            String createCmd = "mysql -u " + targetInfo[3] +
                    " -h " + "localhost" +
                    " -p" + targetInfo[4] +
                    " < ./sql/lss." + tableName + ".sql";

            Runtime rt = Runtime.getRuntime();
            rt.exec("cmd /c " + createCmd);

            // 3. 更新zookeeper节点
            // 在目标region添加表
            String newTargetValue = addTable(targetServerInfo, tableName);
            zooKeeperUtils.setData(targetServerPath, newTargetValue);

            // 在源region删除表
            String newSourceValue = deleteTable(sourceServerInfo, tableName);
            zooKeeperUtils.setData(sourceServerPath, newSourceValue);

            // 4. 从源region server删除表
            Connection connection = JdbcUtils.getConnection(sourceInfo[1], sourceInfo[3], sourceInfo[4]);
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.execute("drop table if exists " + tableName);
                JdbcUtils.releaseResc(null, statement, connection);
            }

            return true;
        } catch (Exception e) {
            System.out.println("迁移表失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 从ZooKeeper节点数据中删除表
     * @param serverValue 服务器节点值
     * @param tableName 要删除的表名
     * @return 修改后的节点值
     */
    public static String deleteTable(String serverValue, String tableName) {
        if (serverValue == null || serverValue.isEmpty()) {
            return "";
        }

        String[] parts = serverValue.split(",");
        if (parts.length < 6) {
            return serverValue; // 无法处理不完整的节点数据
        }

        parts[5] = String.valueOf(Integer.parseInt(parts[5]) - 1); // 减少表计数
        StringBuilder newValue = new StringBuilder();
        for (String part : parts) {
            if (!part.equals(tableName)) {
                newValue.append(part).append(",");
            }
        }
        return newValue.length() > 0 ?
                newValue.substring(0, newValue.length() - 1) : "";
    }

    /**
     * 向ZooKeeper节点数据中添加表
     * @param serverValue 服务器节点值
     * @param tableName 要添加的表名
     * @return 修改后的节点值
     */
    public static String addTable(String serverValue, String tableName) {
        if (serverValue == null || serverValue.isEmpty()) {
            // 返回一个基本的节点结构，假设前6个字段是服务器基本信息
            return "server_name,ip,port,user,password,1," + tableName;
        }

        String[] parts = serverValue.split(",");
        if (parts.length < 6) {
            // 如果节点数据不完整，重建基本结构
            return serverValue + ",1," + tableName;
        }

        // 正常情况处理
        parts[5] = String.valueOf(Integer.parseInt(parts[5]) + 1); // 增加表计数
        StringBuilder newValue = new StringBuilder();
        for (String part : parts) {
            newValue.append(part).append(",");
        }
        return newValue.append(tableName).toString();
    }

    // 移动（剪切）表（给ip1,ip2,table_name) 在某ip复制表（ip1,ip2,table_name1,table_name2)
    // 删除表操作是client 不是master
}