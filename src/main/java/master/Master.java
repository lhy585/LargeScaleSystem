package master;

import socket.SqlSocket;
import socket.ParsedSqlResult;
import socket.SqlType;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master {
    //Master主程序：打开服务器，监听客户端并分配线程连接
    public static void main(String[] args) throws Exception {
        System.out.println("[Info]Region Master is initializing...");
        RegionManager.init();//初始化
        System.out.println("[Info]Initializing successfully!");
        // 启动两个监听线程
        new ClientListenerThread(5000).start(); // 监听Client
        new RegionServerListenerThread(5001).start(); // 监听RegionServer
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
    private static class RegionServerListenerThread extends Thread {
        private final int port;

        public RegionServerListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
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
    }

    private static class ClientHandler extends Thread {
        private final SqlSocket sqlSocket;

        ClientHandler(Socket socket) {
            this.sqlSocket = new SqlSocket(socket);
        }

        @Override
        public void run() {//线程主逻辑
            Socket socket = sqlSocket.getSocket();
            BufferedReader input = sqlSocket.getInput();
            PrintWriter output = sqlSocket.getOutput();
            System.out.println("New client connected, address: " + socket.getInetAddress() + ", port: " + socket.getPort());
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
                    if (type == SqlType.CREATE) {//create需要寻找满足负载均衡的node
                        Map<String, Boolean> res = createTable(tableNames);
                        for (String tableName : tableNames) {
                            if(res.containsKey(tableName) && res.get(tableName)) {
                                output.println("Create Table" + tableName + " successfully");
                            }else{
                                output.println("Create Table" + tableName + " failed");
                            }
                        }
                    } else {
                        Map<String, Map<String, Integer>> regionsTablesInfo;
                        /*TODO:调用Zookeeper的函数获取表所在的节点
                        StringBuilder result = new StringBuilder();
                        for (String nodeInfo : tableInfo) {
                            String[] list = nodeInfo.split(",");
                            boolean flag = false;
                            for (String s : list) {
                                if (Objects.equals(s, tableName) || Objects.equals(s.split("_")[0], tableName)) {
                                    flag = true;
                                    break;
                                }
                            }
                            if (flag) {
                                System.out.println("TARGET REGION SERVER: " + nodeInfo);
                                if (result.toString().equals("")) {
                                    result.append(nodeInfo).append(";");
                                } else {
                                    result.append(nodeInfo);
                                }
                            }
                        }
                        printWriter.println(result);
                        TODO:将结果输出给Server
                         */
                    }
                }
                sqlSocket.destroy();//连接断开，释放资源
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    sqlSocket.destroy();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        private Map<String, Boolean> createTable(List<String> tableNames) throws Exception {
            RegionManager.regionsInfo = RegionManager.getRegionsInfo();//刷新重读，防止数据过期
            RegionManager.regionsLoad = RegionManager.getRegionsLoad();
            String leastRegionName = RegionManager.getLeastRegionName();
            Map<String, Boolean> res = new HashMap<>();
            for(String tableName : tableNames) {
                TableInform tableInform = new TableInform(tableName, 0);
                RegionManager.zooKeeperManager.addTable(leastRegionName, tableInform);
                System.out.println("New table created, name: " + tableName  + ", ip: " + leastRegionName);
                res.put(tableName, true);
            }
            RegionManager.regionsInfo = RegionManager.getRegionsInfo();//更新
            return res;
        }
    }

    private static class RegionServerHandler extends Thread {
        private final Socket socket;

        RegionServerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("[Info] New region server connected: " + socket.getInetAddress() + ":" + socket.getPort());
            // TODO: 在此实现心跳监听、上报表负载、数据同步等
        }
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