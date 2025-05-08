package master;

import socket.SqlSocket;
import socket.ParsedSqlResult;
import socket.SqlType;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets; // 确保字符编码一致性
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import zookeeper.ZooKeeperManager;

public class Master {
    //Master主程序：打开服务器，监听客户端并分配线程连接
    public static void main(String[] args) throws Exception {
        System.out.println("[Info]Region Master is initializing...");
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();
        RegionServerListenerThread regionServerListenerThread = new RegionServerListenerThread(5001, zooKeeperManager);
        RegionManager.init(regionServerListenerThread); //初始化
        System.out.println("[Info]Initializing successfully!");
        // 启动两个监听线程
        new ClientListenerThread(5000).start(); // 监听Client
        regionServerListenerThread.start();     // 监听RegionServer
        System.out.println("[Master] All services started.");
    }

    // 线程1：监听Clients连接
    private static class ClientListenerThread extends Thread {
        private final int port;

        public ClientListenerThread(int port) {
            this.port = port;
            this.setName("ClientListenerThread-" + port);
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
    static class RegionServerListenerThread extends Thread {
        private final int port;
        // ip是key
        private final Map<String, RegionServerHandler> regionHandlers;
        private final ZooKeeperManager zooKeeperManager;

        public RegionServerListenerThread(int port, ZooKeeperManager zooKeeperManager) {
            this.port = port;
            this.regionHandlers = new ConcurrentHashMap<>();
            this.zooKeeperManager = zooKeeperManager;
            this.setName("RegionServerListenerThread-" + port);
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Master] Listening for RegionServers on port " + port + "...");
                while (true) {
                    Socket regionSocket = null;
                    RegionServerHandler handler = null;
                    String connectionIp = null; // Socket 连接看到的 IP
                    String reportedZkIp = null; // RegionServer 报告的、在ZK中注册的IP

                    try {
                        regionSocket = serverSocket.accept();
                        connectionIp = regionSocket.getInetAddress().getHostAddress();
                        // 创建 Handler 管理这个 RegionServer 的连接
                        handler = new RegionServerHandler(regionSocket, zooKeeperManager);
                        reportedZkIp = handler.performRegistration();
                        if (reportedZkIp != null) {
                            RegionServerHandler oldHandler = regionHandlers.put(reportedZkIp, handler);
                            if (oldHandler != null) {
                                System.out.println("[Master Listener] Replaced existing handler for ZK IP: " + reportedZkIp);
                                oldHandler.closeConnection();
                            }
                            // 注册成功后再启动Handler的主线程
                            handler.startHandlerThread(reportedZkIp);
                        } else {
                            System.err.println("[Master Listener] Registration failed for connection from " + connectionIp + ". Closing socket.");
                            if (regionSocket != null && !regionSocket.isClosed()) {
                                try { regionSocket.close(); } catch (IOException ce) { /* ignore */ }
                            }
                            if (handler != null) handler.closeConnection();
                        }
                    } catch (IOException e) {
                        System.err.println("[Master Listener] Error handling incoming RegionServer connection from " + connectionIp + ": " + e.getMessage());
                        if (regionSocket != null && !regionSocket.isClosed()) {
                            try { regionSocket.close(); } catch (IOException ce) { /* ignore */ }
                        }
                        if (handler != null) handler.closeConnection();
                    }
                }
            } catch (IOException e) {
                System.err.println("[Master] RegionServerListenerThread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }

        public RegionServerHandler getRegionServerHandler(String zkIp) {
            return regionHandlers.get(zkIp);
        }

        public void removeRegionServer(String zkIp) {
            RegionServerHandler handler = regionHandlers.remove(zkIp);
            if (handler != null) {
                handler.closeConnection();
                System.out.println("[Master Listener] Removed handler for ZK IP: " + zkIp);
            }
        }

        public Set<String> getAllRegionServerIds() {
            return regionHandlers.keySet();
        }

//         public Socket getRegionServerSocket(String zkIp) {
//             RegionServerHandler handler = regionHandlers.get(zkIp);
//             return handler != null ? handler.getSocket() : null;
//         }
    }

    // 处理Client连接
    private static class ClientHandler extends Thread {
        private final SqlSocket sqlSocket;

        public ClientHandler(Socket socket) {
            this.sqlSocket = new SqlSocket(socket);
            this.setName("ClientHandler-" + socket.getRemoteSocketAddress());
        }

        @Override
        public void run() {
            Socket socket = sqlSocket.getSocket();
            BufferedReader input = sqlSocket.getInput();
            PrintWriter output = sqlSocket.getOutput();

            System.out.println("[Info] New client connected: " + socket.getInetAddress() + ":" + socket.getPort());

            try {
                String sql = input.readLine();

                if (sql == null || sql.trim().isEmpty()) {
                    System.out.println("[Master-ClientHandler] Client disconnected or sent empty command.");
                } else {
                    sqlSocket.parseSql(sql); //处理字符串 (日志在 Master 控制台)
                    ParsedSqlResult parsedSqlResult = sqlSocket.getParsedSqlResult();

                    if (parsedSqlResult == null || parsedSqlResult.getType() == SqlType.UNKNOWN) {
                        output.println("ERROR: Invalid SQL or unable to parse.");
                    } else {
                        List<String> tableNames = parsedSqlResult.getTableNames();
                        SqlType type = parsedSqlResult.getType();
                        Map<String, ResType> res;

                        switch (type) {
                            case CREATE:
                                res = createTable(tableNames, sql);
                                for (String tableName : tableNames) { // 为每个涉及的表发送响应行
                                    if (res.containsKey(tableName) && res.get(tableName) == ResType.CREATE_TABLE_SUCCESS) {
                                        output.println("Create Table " + tableName + " successfully");
                                    } else if (res.containsKey(tableName) && res.get(tableName) == ResType.CREATE_TABLE_ALREADY_EXISTS) {
                                        output.println("Create Table " + tableName + " failed: Already exists.");
                                    } else {
                                        output.println("Create Table " + tableName + " failed. Status: " + (res.get(tableName) != null ? res.get(tableName) : "UNKNOWN_ERROR"));
                                    }
                                }
                                break;
                            case DROP:
                                res = dropTable(tableNames, sql);
                                for (String tableName : tableNames) { // 发送多行响应
                                    if (res.containsKey(tableName) && res.get(tableName) == ResType.DROP_TABLE_SUCCESS) {
                                        output.println("Drop Table " + tableName + " successfully");
                                    } else if (res.containsKey(tableName) && res.get(tableName) == ResType.DROP_TABLE_NO_EXISTS){
                                        output.println("Drop Table " + tableName + " failed: Table does not exist.");
                                    } else {
                                        output.println("Drop Table " + tableName + " failed. Status: " + (res.get(tableName) != null ? res.get(tableName) : "UNKNOWN_ERROR"));
                                    }
                                }
                                break;
                            case INSERT:
                                res = insert(tableNames, sql);
                                for (String tableName : parsedSqlResult.getTableNames()) { // 发送多行响应
                                    outputDmlResponse(output, tableName, res, "INSERT");
                                }
                                // output.println(res); // 之前的简单响应方式被取代
                                break;
                            case DELETE:
                                res = delete(tableNames, sql);
                                for (String tableName : parsedSqlResult.getTableNames()) { // 发送多行响应
                                    outputDmlResponse(output, tableName, res, "DELETE");
                                }
                                break;
                            case UPDATE:
                                res = update(tableNames, sql);
                                for (String tableName : parsedSqlResult.getTableNames()) { // 发送多行响应
                                    outputDmlResponse(output, tableName, res, "UPDATE");
                                }
                                break;
                            case ALTER:
                                res = alter(tableNames, sql);
                                for (String tableName : parsedSqlResult.getTableNames()) { // 发送多行响应
                                    outputDmlResponse(output, tableName, res, "ALTER");
                                }
                                break;
                            case SELECT:
                                SelectInfo selectInfo = select(tableNames, sql);
                                String responseToClient = selectInfo.Serialize();
                                output.println(responseToClient); // 发送单行响应
                                break;
                            case TRUNCATE:
                                res = truncate(tableNames, sql);
                                for (String tableName : parsedSqlResult.getTableNames()) { // 发送多行响应
                                    outputDmlResponse(output, tableName, res, "TRUNCATE");
                                }
                                break;
                            default:
                                output.println("ERROR: Unsupported SQL command type: " + type);
                                break;
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("[Info] ClientHandler connection closed or error for " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
            } catch (Exception e) {
                System.err.println("[Error] ClientHandler unexpected exception for " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
                e.printStackTrace();
                if (output != null && isSocketAlive(socket)) {
                    output.println("ERROR: Internal server error while processing your request.");
                }
            } finally {
                // System.out.println("[Info] Closing connection for client: " + socket.getRemoteSocketAddress());
                try {
                    sqlSocket.destroy(); // 释放资源
                } catch (IOException e) {
                    System.err.println("[Error] Failed to destroy SqlSocket for " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
                }
            }
        }

        // 辅助方法，用于统一DML/DDL操作的响应格式
        private void outputDmlResponse(PrintWriter output, String tableName, Map<String, ResType> resMap, String operation) {
            if (resMap.containsKey(tableName)) { // 主副本
                ResType masterRes = resMap.get(tableName);
                output.println("Master Table " + tableName + " " + operation + ": " + masterRes);
            }
            if (resMap.containsKey(tableName + "_slave")) { // 从副本
                ResType slaveRes = resMap.get(tableName + "_slave");
                output.println("Slave Table " + tableName + "_slave" + " " + operation + ": " + slaveRes);
            }
            // 如果两个都不包含，可能表示该表未被操作或操作失败未记录
        }

        // 创建表
        private static Map<String, ResType> createTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.createTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.CREATE_TABLE_FAILURE);
                    res.put(tableName + "_slave", ResType.CREATE_TABLE_FAILURE);
                }
            }
            return res;
        }

        // 删除表
        private static Map<String, ResType> dropTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.dropTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.DROP_TABLE_FAILURE);
                    res.put(tableName + "_slave", ResType.DROP_TABLE_FAILURE);
                }
            }
            return res;
        }

        // 查找数据
        private static SelectInfo select(List<String> tableNames, String sql) {
            return RegionManager.selectTable(tableNames, sql);
        }

        // 插入数据
        private static Map<String, ResType> insert(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.accTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.INSERT_FAILURE);
                    res.put(tableName + "_slave", ResType.INSERT_FAILURE);
                }
            }
            return res;
        }

        // 删除数据
        private static Map<String, ResType> delete(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.decTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.DELECT_FAILURE);
                    res.put(tableName + "_slave", ResType.DELECT_FAILURE);
                }
            }
            return res;
        }

        // 更新数据
        private static Map<String, ResType> update(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.updateTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.UPDATE_FAILURE);
                    res.put(tableName + "_slave", ResType.UPDATE_FAILURE);
                }
            }
            return res;
        }

        private static Map<String, ResType> alter(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.alterTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.ALTER_FAILURE);
                    res.put(tableName + "_slave", ResType.ALTER_FAILURE);
                }
            }
            return res;
        }

        // 清空数据表数据
        private static Map<String, ResType> truncate(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.truncateTableMasterAndSlave(tableName, sql);
                if (ansList != null && ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.TRUNCATE_FAILURE);
                    res.put(tableName + "_slave", ResType.TRUNCATE_FAILURE);
                }
            }
            return res;
        }
    }

    // 处理RegionServer连接
    static class RegionServerHandler extends Thread { // 保持 private static
        private final Socket socket;
        private final ZooKeeperManager zooKeeperManager;
        private BufferedReader in;
        private PrintWriter out;
        private volatile boolean running = true;
        private String zkIpUsedForKey = null;

        public RegionServerHandler(Socket socket, ZooKeeperManager zooKeeperManager) {
            this.socket = socket;
            this.zooKeeperManager = zooKeeperManager;
        }

        public String performRegistration() throws IOException {
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

            // 等待 RegionServer 的注册消息
            String registerMsg = in.readLine();
            if (registerMsg == null || !"REGISTER_REGION_SERVER".equals(registerMsg)) {
                System.err.println("[Master] Invalid or missing registration command from " + socket.getRemoteSocketAddress());
                return null;
            }
            String reportedZkIp = in.readLine();
            // 读取RegionServer信息并存储
            if (reportedZkIp == null || reportedZkIp.trim().isEmpty()) {
                System.err.println("[Master] Invalid or missing ZK IP from RegionServer " + socket.getRemoteSocketAddress());
                return null;
            }
            this.zkIpUsedForKey = reportedZkIp.trim();
            System.out.println("[Master] RegionServer (Socket: " + socket.getInetAddress().getHostAddress() + ") registered with ZK IP " + this.zkIpUsedForKey);
            return this.zkIpUsedForKey;
        }

        public void startHandlerThread(String zkIp) {
            if (this.zkIpUsedForKey == null) this.zkIpUsedForKey = zkIp;
            this.setName("RegionServerHandler-" + this.zkIpUsedForKey);
            this.start();
        }

        @Override
        public void run() {
            // System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] run() method started.");
            if (in == null || out == null) {
                System.err.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Error: Input/Output streams not initialized. Handler terminating.");
                running = false;
                closeConnection();
                return;
            }
            // System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Entering listening loop for messages from RS.");
            try {
                String messageFromRegionServer;
                while (running && isSocketAlive(socket) && (messageFromRegionServer = in.readLine()) != null) {
                    System.out.println("[Master] Message from RegionServer (" + zkIpUsedForKey + "): " + messageFromRegionServer);
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[Master] RegionServerHandler for " + zkIpUsedForKey + " IOException: " + e.getMessage());
                }
            } finally {
                // System.out.println("[Master] RegionServerHandler for " + zkIpUsedForKey + " finishing.");
                running = false;
                closeConnection();
            }
        }

        // 新增方法，用于从外部发送命令 (由 RegionManager 调用)
        public void forwardCommand(String command) {
            if (out != null && isRunning()) {
                out.println(command);
            } else {
                System.err.println("[Master] Cannot forward command to RS (" + (zkIpUsedForKey != null ? zkIpUsedForKey : "N/A") + "): Handler not running or output stream unavailable.");
            }
        }

        public boolean isRunning() {
            return running && socket != null && socket.isConnected() && !socket.isClosed();
        }

        public void closeConnection() {
            running = false;
            try { if (in != null) in.close(); } catch (IOException e) { /* ignore */ }
            try { if (out != null) out.close(); } catch (Exception e) { /* ignore */ }
            try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException e) { /* ignore */ }
            in = null; out = null;
        }
    }

    /**
     * 定期向Client/Region Server发送消息，通过发送的成功与否判断连接是否保持
     *
     * @return true:连接保持;
     * false:连接关闭
     */
    public static boolean isSocketAlive(Socket socket) {
        if (socket == null || socket.isClosed() || !socket.isConnected()) {
            return false;
        }
        try {
            socket.sendUrgentData(0xFF); // 发送1字节紧急数据，对方关闭时会抛异常
            return true;
        } catch (IOException e) {
            return false; // 发送失败，说明连接断了
        } catch (Exception e) {
            System.err.println("[isSocketAlive] Unexpected error checking socket: " + e.getMessage());
            return false;
        }
    }
}