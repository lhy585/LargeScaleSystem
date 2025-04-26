package master;

import socket.SqlSocket;
import socket.ParsedSqlResult;
import socket.SqlType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Master {
    //Master主程序：打开服务器，监听客户端并分配线程连接
    public static void main(String[] args) throws Exception {
        System.out.println("[Info]Region Master is initializing...");
        RegionManager.init();//初始化
        System.out.println("[Info]Initializing successfully!");
        // 启动两个监听线程
        new ClientListenerThread(5000).start(); // 监听Client
        new RegionServerListenerThread(5001).start(); // 监听RegionServer，TODO:测试的话,region server连接到5001端口
    }

    // 线程1：监听Clients连接
    private static class ClientListenerThread extends Thread {
        private final int port;

        public ClientListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Info] Listening for clients on port " + port + "...");
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new ClientHandler(clientSocket).start();
                }
            } catch (IOException e) {
                System.err.println("[Error] ClientListenerThread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // 线程2：监听RegionServers连接
    //TODO:Region Server和master合并在此处，是处理多个region server的总线程
    private static class RegionServerListenerThread extends Thread {
        //TODO:一些资源可以保留在private成员内，可能需要保留ip->socket/thread的map
        private final int port;

        public RegionServerListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            //TODO:申请一些资源或初始化
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Info] Listening for region servers on port " + port + "...");
                while (true) {
                    Socket regionSocket = serverSocket.accept();
                    new RegionServerHandler(regionSocket).start();
                }
            } catch (IOException e) {
                System.err.println("[Error] RegionServerListenerThread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }

        //TODO:一些涉及到ip的函数应该是写在这里的，在这个region servers的总线程的run()内调用
    }

    // 处理Client连接
    private static class ClientHandler extends Thread {
        private final SqlSocket sqlSocket;

        public ClientHandler(Socket socket) {
            this.sqlSocket = new SqlSocket(socket);
        }

        @Override
        public void run() {
            Socket socket = sqlSocket.getSocket();
            BufferedReader input = sqlSocket.getInput();
            PrintWriter output = sqlSocket.getOutput();

            System.out.println("[Info] New client connected: " + socket.getInetAddress() + ":" + socket.getPort());

            try {
                String sql;
                while (isSocketAlive(socket)) {
                    while ((sql = input.readLine()) == null) ;//持续接受并读取客户端输入
                    sqlSocket.parseSql(sql);//处理字符串
                    ParsedSqlResult parsedSqlResult = sqlSocket.getParsedSqlResult();
                    if(parsedSqlResult == null || parsedSqlResult.getType() == SqlType.UNKNOWN) {
                        continue;
                    }

                    List<String> tableNames = parsedSqlResult.getTableNames();
                    SqlType type = parsedSqlResult.getType();

                    for(String tableName : tableNames){
                        String region = RegionManager.zooKeeperManager.getRegionServer(tableName);
                        output.println("Table: " + tableName + " is in Region: " + region + ".");
                    }

                    Map<String, ResType> res;
                    switch (type) {
                        case CREATE:
                            res = createTable(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName) && res.get(tableName)==ResType.CREATE_TABLE_SUCCESS) {
                                    output.println("Create Table " + tableName + " successfully");
                                }else{
                                    output.println("Create Table " + tableName + " failed");
                                }
                            }
                            break;
                        case DROP:
                            res = dropTable(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName) && res.get(tableName)==ResType.DROP_TABLE_SUCCESS) {
                                    output.println("Drop Table " + tableName + " successfully");
                                }else{
                                    output.println("Drop Table " + tableName + " failed");
                                }
                            }
                            break;
                        case INSERT:
                            res = insert(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName)){
                                    ResType resType = res.get(tableName);
                                    if(resType==ResType.INSERT_SUCCESS){
                                        output.println("Insert into Table " + tableName + " successfully");
                                    }else if(resType==ResType.INSERT_FAILURE){
                                        output.println("Insert into Table " + tableName + " failed");
                                    }else{
                                        output.println("Insert into Table " + tableName + " doesn't exist");
                                    }
                                }
                            }
                            break;
                        case DELETE:
                            res = delete(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName)){
                                    ResType resType = res.get(tableName);
                                    if(resType==ResType.DELECT_SUCCESS){
                                        output.println("Delete from Table " + tableName + " successfully");
                                    }else if(resType==ResType.DELECT_FAILURE){
                                        output.println("Delete from Table " + tableName + " failed");
                                    }else{
                                        output.println("Delete from Table " + tableName + " doesn't exist");
                                    }
                                }
                            }
                            break;
                        case UPDATE:
                            res = update(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName)){
                                    ResType resType = res.get(tableName);
                                    if(resType==ResType.UPDATE_SUCCESS){
                                        output.println("Update Table " + tableName + " successfully");
                                    }else if(resType==ResType.UPDATE_FAILURE){
                                        output.println("Update Table " + tableName + " failed");
                                    }else{
                                        output.println("Update Table " + tableName + " doesn't exist");
                                    }
                                }
                            }
                            break;
                        case ALTER:
                            res = alter(tableNames, sql);
                            for(String tableName : tableNames){
                                if(res.containsKey(tableName)){
                                    ResType resType = res.get(tableName);
                                    if(resType==ResType.ALTER_SUCCESS){
                                        output.println("Alter Table " + tableName + " successfully");
                                    }else if(resType==ResType.ALTER_FAILURE){
                                        output.println("Alter Table " + tableName + " failed");
                                    }else{
                                        output.println("Alter Table " + tableName + " doesn't exist");
                                    }
                                }
                            }
                            break;
                        case SELECT:
                            SelectInfo selectInfo = select(tableNames, sql);
                            output.println(selectInfo.Serialize());
                            break;
                        case TRUNCATE:
                            res = truncate(tableNames, sql);
                            for(String tableName : tableNames){
                                if (res.containsKey(tableName)) {
                                    ResType resType = res.get(tableName);
                                    if(resType==ResType.TRUNCATE_SUCCESS){
                                        output.println("Truncate Table " + tableName + " successfully");
                                    }else{
                                        output.println("Truncate Table " + tableName + " doesn't exist");
                                    }
                                }
                            }
                        default:
                            break;
                    }
                }
            } catch (IOException e) {
                System.err.println("[Error] ClientHandler IOException: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    sqlSocket.destroy(); // 释放资源
                } catch (IOException e) {
                    System.err.println("[Error] Failed to destroy SqlSocket: " + e.getMessage());
                }
            }
        }

        // 创建表
        private static Map<String, ResType> createTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for(String tableName : tableNames) {
                List<ResType> ansList = RegionManager.createTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        // 删除表
        private static Map<String, ResType> dropTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for(String tableName : tableNames) {
                List<ResType> ansList = RegionManager.dropTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        // 插入数据
        private static SelectInfo select(List<String> tableNames, String sql) {
            return RegionManager.selectTable(tableNames, sql);
        }

        // 插入数据
        private static Map<String, ResType> insert(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.accTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        // 删除数据
        private static Map<String, ResType> delete(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.decTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        // 更新数据
        private static Map<String, ResType> update(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.updateTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        private static Map<String, ResType> alter(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.alterTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }

        // 清空数据表数据
        private static Map<String, ResType> truncate(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.truncateTableMasterAndSlave(tableName, sql);
                res.put(tableName, ansList.get(0));
                res.put(tableName + "_slave", ansList.get(1));
            }
            return res;
        }
    }

    // 处理RegionServer连接
    // TODO:此处是处理与某一个region server的连接
    private static class RegionServerHandler extends Thread {
        private final Socket socket;

        public RegionServerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("[Info] New region server connected: " + socket.getInetAddress() + ":" + socket.getPort());
            // TODO: 在此实现和某一个具体的region server通信
        }
        //TODO:这里涉及到某个表的删除，拿到表的数据等等，已经是和某一个具体的region server通信了
    }

    /**
     * 定期向Client/Region Server发送消息，通过发送的成功与否判断连接是否保持
     * @return true:连接保持;
     *         false:连接关闭
     */
    public static boolean isSocketAlive(Socket socket) {
        try {
            socket.sendUrgentData(0xFF); // 发送1字节紧急数据，对方关闭时会抛异常
            return true;
        } catch (Exception e) {
            return false; // 发送失败，说明连接断了
        }
    }
}