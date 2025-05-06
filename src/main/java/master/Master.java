package master;

import socket.SqlSocket;
import socket.ParsedSqlResult;
import socket.SqlType;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// Assuming RegionServer class is in regionserver package if needed for RegionServer.getIPAddress()
// If RegionServer.getIPAddress() is not used by Master's RegionServerHandler, this import might not be needed here.
// import regionserver.RegionServer;
import zookeeper.ZooKeeperManager;

public class Master {
    //Master主程序：打开服务器，监听客户端并分配线程连接
    public static void main(String[] args) throws Exception {
        System.out.println("[Info]Region Master is initializing...");

        // 1. Create ZooKeeperManager instance first, as it's needed by RegionServerListenerThread
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();

        // 2. Create RegionServerListenerThread instance
        // It needs to be created before RegionManager.init() so it can be passed.
        // It will be started after RegionManager is initialized.
        RegionServerListenerThread regionServerListenerThread = new RegionServerListenerThread(5001, zooKeeperManager);

        // 3. Initialize RegionManager, passing the RegionServerListenerThread instance
        // This allows RegionManager to send commands back to RegionServers.
        RegionManager.init(regionServerListenerThread); // Pass the instance

        System.out.println("[Info]Initializing successfully!");

        // 4. Start the listener threads
        new ClientListenerThread(5000).start(); // Start listening for Clients
        regionServerListenerThread.start();            // Start listening for RegionServers

        System.out.println("[Master] All services started.");
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
    // Made it static inner class, but can be non-static if preferred.
    // Ensured it's the same class RegionManager expects.
    static class RegionServerListenerThread extends Thread { // Ensure this matches the type in RegionManager
        private final int port;
        // Key 应该是 RegionServer 在 ZK 中注册的 IP
        private final Map<String, RegionServerHandler> regionHandlers;
        private final ZooKeeperManager zooKeeperManager;

        // 构造函数等...
        public RegionServerListenerThread(int port, ZooKeeperManager zooKeeperManager) {
            this.port = port;
            this.regionHandlers = new ConcurrentHashMap<>();
            this.zooKeeperManager = zooKeeperManager;
            this.setName("RegionServerListenerThread-" + port);
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Master Listener] Listening for RegionServers on port " + port + "...");
                while (true) {
                    Socket regionSocket = null;
                    RegionServerHandler handler = null;
                    String connectionIp = null; // Socket 连接看到的 IP
                    String reportedZkIp = null; // RegionServer 报告的 ZK IP

                    try {
                        regionSocket = serverSocket.accept();
                        connectionIp = regionSocket.getInetAddress().getHostAddress();
                        System.out.println("[Master Listener] Accepted connection from IP: " + connectionIp + ". Remote Port: " + regionSocket.getPort());

                        // 1. 创建 Handler 对象 (但先不启动线程)
                        handler = new RegionServerHandler(regionSocket, zooKeeperManager);

                        // 2. 让 Handler 执行注册流程，读取 RS 报告的 ZK IP
                        reportedZkIp = handler.performRegistration(); // Handler 中的新方法

                        if (reportedZkIp != null) {
                            // 3. 使用 RegionServer 报告的 ZK IP 作为 Key 存入 Map
                            System.out.println("[Master Listener] RegionServer reports ZK IP: " + reportedZkIp + ". Using this as handler key.");
                            RegionServerHandler oldHandler = regionHandlers.put(reportedZkIp, handler);
                            if (oldHandler != null) {
                                System.out.println("[Master Listener] Replaced existing handler for ZK IP: " + reportedZkIp);
                                oldHandler.closeConnection();
                            }
                            System.out.println("[Master Listener] Handler for ZK IP: " + reportedZkIp + " added. Current map keys: " + regionHandlers.keySet());

                            // 4. 注册成功后，再启动 Handler 的主线程
                            handler.startHandlerThread(reportedZkIp); // 启动线程，并传入 ZK IP 用于日志/命名
                            System.out.println("[Master Listener] Started handler thread for ZK IP: " + reportedZkIp);
                        } else {
                            // 如果注册失败（例如，没收到 ZK IP），关闭连接
                            System.err.println("[Master Listener] Registration failed for connection from " + connectionIp + ". Closing socket.");
                            if (regionSocket != null && !regionSocket.isClosed()) {
                                try { regionSocket.close(); } catch (IOException ce) { /* 忽略关闭错误 */ }
                            }
                            if (handler != null) handler.closeConnection(); // 确保资源释放
                        }
                    } catch (IOException e) {
                        // 处理 accept 或 performRegistration 中的 IO 错误
                        System.err.println("[Master Listener] Error handling connection from " + connectionIp + ": " + e.getMessage());
                        if (regionSocket != null && !regionSocket.isClosed()) {
                            try { regionSocket.close(); } catch (IOException ce) { /* 忽略关闭错误 */ }
                        }
                        if (handler != null) handler.closeConnection();
                    }
                } // 结束 while
            } catch (IOException e) {
                System.err.println("[Master Listener] Listener Thread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // getRegionServerHandler 现在应该接收 ZK IP 作为参数
        public RegionServerHandler getRegionServerHandler(String zkIp) {
            return regionHandlers.get(zkIp);
        }

        // removeRegionServer 也应该接收 ZK IP
        public void removeRegionServer(String zkIp) {
            System.out.println("[Master Listener] Removing handler for ZK IP: " + zkIp);
            RegionServerHandler handler = regionHandlers.remove(zkIp);
            if (handler != null) {
                handler.closeConnection();
            } else {
                System.out.println("[Master Listener] Handler for ZK IP: " + zkIp + " not found for removal.");
            }
        }

        public Set<String> getAllRegionServerIds() {
            // 返回的是 ZK IP 集合
            return regionHandlers.keySet();
        }
    }

    // 处理Client连接 (ClientHandler remains largely the same as your provided one)
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
                String sql;
                // Use isSocketAlive for loop condition or check readLine() != null
                while (isSocketAlive(socket) && (sql = input.readLine()) != null) {
                    // if(sql.equals("REGISTER"))continue; // REGISTER is for RegionServers, not clients
                    System.out.println("[Master-ClientHandler] Received SQL from client: " + sql);
                    sqlSocket.parseSql(sql);//处理字符串
                    ParsedSqlResult parsedSqlResult = sqlSocket.getParsedSqlResult();
                    if (parsedSqlResult == null || parsedSqlResult.getType() == SqlType.UNKNOWN) {
                        System.out.println("[Master-ClientHandler] SQL parsing failed or unknown type: " + sql);
                        output.println("ERROR: Invalid SQL or unable to parse."); // Inform client
                        continue;
                    }

                    List<String> tableNames = parsedSqlResult.getTableNames();
                    SqlType type = parsedSqlResult.getType();

                    // Log which region a table is on (for debugging)
                    // for (String tableName : tableNames) {
                    //    String region = RegionManager.zooKeeperManager.getRegionServer(tableName);
                    //    System.out.println("Table: " + tableName + " is in Region: " + region + ".");
                    // }

                    Map<String, ResType> res;
                    switch (type) {
                        case CREATE:
                            System.out.println("[Master-ClientHandler] Processing CREATE TABLE: " + sql);
                            res = createTable(tableNames, sql); // This calls RegionManager
                            // Respond to client based on RegionManager's result for each table.
                            // RegionManager's createTableMasterAndSlave now returns List<ResType>
                            // which should align with what client expect or be adapted.
                            // Assuming createTable method here adapts it.
                            for (String tableName : tableNames) {
                                if (res.containsKey(tableName) && res.get(tableName) == ResType.CREATE_TABLE_SUCCESS) {
                                    output.println("Create Table " + tableName + " successfully");
                                } else if (res.containsKey(tableName) && res.get(tableName) == ResType.CREATE_TABLE_ALREADY_EXISTS) {
                                    output.println("Create Table " + tableName + " failed: Already exists.");
                                }
                                else {
                                    output.println("Create Table " + tableName + " failed. Status: " + (res.get(tableName) != null ? res.get(tableName) : "UNKNOWN_ERROR"));
                                }
                            }
                            break;
                        case DROP:
                            System.out.println("[Master-ClientHandler] Processing DROP TABLE: " + sql);
                            res = dropTable(tableNames, sql);
                            for (String tableName : tableNames) {
                                if (res.containsKey(tableName) && res.get(tableName) == ResType.DROP_TABLE_SUCCESS) {
                                    output.println("Drop Table " + tableName + " successfully");
                                } else if (res.containsKey(tableName) && res.get(tableName) == ResType.DROP_TABLE_NO_EXISTS){
                                    output.println("Drop Table " + tableName + " failed: Table does not exist.");
                                }
                                else {
                                    output.println("Drop Table " + tableName + " failed. Status: " + (res.get(tableName) != null ? res.get(tableName) : "UNKNOWN_ERROR"));
                                }
                            }
                            break;
                        case INSERT:
                            System.out.println("[Master-ClientHandler] Processing INSERT: " + sql);
                            res = insert(tableNames, sql);
                            // Iterate over each table name from the original parsed list
                            for (String tableName : parsedSqlResult.getTableNames()) { // Use original table names for response
                                if (res.containsKey(tableName)) { // Check for master
                                    ResType masterRes = res.get(tableName);
                                    output.println("Master Table " + tableName + ": " + masterRes);
                                }
                                if (res.containsKey(tableName + "_slave")) { // Check for slave
                                    ResType slaveRes = res.get(tableName + "_slave");
                                    output.println("Slave Table " + tableName + "_slave" + ": " + slaveRes);
                                }
                                if (!res.containsKey(tableName) && !res.containsKey(tableName+"_slave")){
                                    output.println("Table " + tableName + ": Status UNKNOWN (not in response map)");
                                }
                            }
                            break;
                        // Add similar detailed logging and response for DELETE, UPDATE, ALTER, TRUNCATE
                        case DELETE:
                            System.out.println("[Master-ClientHandler] Processing DELETE: " + sql);
                            res = delete(tableNames, sql);
                            // Respond... (similar to INSERT)
                            for (String tableName : parsedSqlResult.getTableNames()) {
                                outputResponse(output, tableName, res, "DELETE");
                            }
                            break;
                        case UPDATE:
                            System.out.println("[Master-ClientHandler] Processing UPDATE: " + sql);
                            res = update(tableNames, sql);
                            // Respond...
                            for (String tableName : parsedSqlResult.getTableNames()) {
                                outputResponse(output, tableName, res, "UPDATE");
                            }
                            break;
                        case ALTER:
                            System.out.println("[Master-ClientHandler] Processing ALTER: " + sql);
                            res = alter(tableNames, sql);
                            // Respond...
                            for (String tableName : parsedSqlResult.getTableNames()) {
                                outputResponse(output, tableName, res, "ALTER");
                            }
                            break;
                        case SELECT:
                            System.out.println("[Master-ClientHandler] Processing SELECT: " + sql);
                            SelectInfo selectInfo = select(tableNames, sql); // Calls RegionManager
                            // selectInfo.Serialize() should return "FOUND ip:port\nactual_sql" or "NOT_FOUND" or "ERROR ..."
                            output.println(selectInfo.Serialize()); // Send this multi-line response
                            break;
                        case TRUNCATE:
                            System.out.println("[Master-ClientHandler] Processing TRUNCATE: " + sql);
                            res = truncate(tableNames, sql);
                            // Respond...
                            for (String tableName : parsedSqlResult.getTableNames()) {
                                outputResponse(output, tableName, res, "TRUNCATE");
                            }
                            break;
                        default:
                            output.println("ERROR: Unsupported SQL command type: " + type);
                            break;
                    }
                }
            } catch (IOException e) {
                // This often happens when client disconnects
                System.err.println("[Info] ClientHandler connection closed or error: " + socket.getRemoteSocketAddress() + " - " + e.getMessage());
            } catch (Exception e) {
                // Catch any other unexpected exceptions during processing
                System.err.println("[Error] ClientHandler unexpected exception: " + e.getMessage());
                e.printStackTrace();
                if (output != null && isSocketAlive(socket)) {
                    output.println("ERROR: Internal server error while processing your request.");
                }
            }
            finally {
                try {
                    System.out.println("[Info] Closing connection for client: " + socket.getRemoteSocketAddress());
                    sqlSocket.destroy(); // 释放资源
                } catch (IOException e) {
                    System.err.println("[Error] Failed to destroy SqlSocket: " + e.getMessage());
                }
            }
        }

        // Helper for consistent DML/DDL response
        private void outputResponse(PrintWriter output, String tableName, Map<String, ResType> resMap, String operation) {
            if (resMap.containsKey(tableName)) {
                ResType masterRes = resMap.get(tableName);
                output.println("Master Table " + tableName + " " + operation + ": " + masterRes);
            }
            if (resMap.containsKey(tableName + "_slave")) {
                ResType slaveRes = resMap.get(tableName + "_slave");
                output.println("Slave Table " + tableName + "_slave" + " " + operation + ": " + slaveRes);
            }
            if (!resMap.containsKey(tableName) && !resMap.containsKey(tableName+"_slave")){
                output.println("Table " + tableName + " " + operation + ": Status UNKNOWN (not in response map)");
            }
        }

        // Methods calling RegionManager (these remain mostly the same)
        private static Map<String, ResType> createTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.createTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) { // Ensure list has expected size
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    System.err.println("[Master-ClientHandler] createTableMasterAndSlave for " + tableName + " returned unexpected list size: " + ansList.size());
                    res.put(tableName, ResType.CREATE_TABLE_FAILURE); // Mark as failure
                    res.put(tableName + "_slave", ResType.CREATE_TABLE_FAILURE);
                }
            }
            return res;
        }

        private static Map<String, ResType> dropTable(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.dropTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    System.err.println("[Master-ClientHandler] dropTableMasterAndSlave for " + tableName + " returned unexpected list size: " + ansList.size());
                    res.put(tableName, ResType.DROP_TABLE_FAILURE);
                    res.put(tableName + "_slave", ResType.DROP_TABLE_FAILURE);
                }
            }
            return res;
        }

        private static SelectInfo select(List<String> tableNames, String sql) {
            return RegionManager.selectTable(tableNames, sql);
        }

        private static Map<String, ResType> insert(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.accTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    System.err.println("[Master-ClientHandler] accTableMasterAndSlave for " + tableName + " returned unexpected list size: " + ansList.size());
                    res.put(tableName, ResType.INSERT_FAILURE);
                    res.put(tableName + "_slave", ResType.INSERT_FAILURE);
                }
            }
            return res;
        }

        private static Map<String, ResType> delete(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.decTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.DELECT_FAILURE);
                    res.put(tableName + "_slave", ResType.DELECT_FAILURE);
                }
            }
            return res;
        }

        private static Map<String, ResType> update(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.updateTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) {
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
                if (ansList.size() >= 2) {
                    res.put(tableName, ansList.get(0));
                    res.put(tableName + "_slave", ansList.get(1));
                } else {
                    res.put(tableName, ResType.ALTER_FAILURE);
                    res.put(tableName + "_slave", ResType.ALTER_FAILURE);
                }
            }
            return res;
        }

        private static Map<String, ResType> truncate(List<String> tableNames, String sql) {
            Map<String, ResType> res = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                List<ResType> ansList = RegionManager.truncateTableMasterAndSlave(tableName, sql);
                if (ansList.size() >= 2) {
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
    static class RegionServerHandler extends Thread {
        private final Socket socket;
        private final ZooKeeperManager zooKeeperManager;
        private BufferedReader in;
        private PrintWriter out;
        private volatile boolean running = true;
        private String zkIpUsedForKey = null; // 用于日志记录

        // 构造函数不再需要 IP 参数
        public RegionServerHandler(Socket socket, ZooKeeperManager zooKeeperManager) {
            this.socket = socket;
            this.zooKeeperManager = zooKeeperManager;
        }

        /**
         * 执行初始注册握手。读取 REGISTER 命令和 ZK IP。
         * 必须在 Handler 线程启动或放入 Map 之前调用。
         * @return RegionServer 报告的 ZK IP，如果注册失败则返回 null。
         * @throws IOException 如果读取 Socket 失败。
         */
        public String performRegistration() throws IOException {
            try {
                // 初始化流用于读取注册信息
                in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
                out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true); // 如果需要发送确认

                String registerMsg = in.readLine();
                System.out.println("[Master-RegionServerHandler:Initial] Read line 1 (Command): " + registerMsg);
                if (registerMsg == null || !"REGISTER_REGION_SERVER".equals(registerMsg)) {
                    System.err.println("[Master-RegionServerHandler:Initial] Invalid or missing registration command.");
                    return null;
                }

                String reportedZkIp = in.readLine();
                System.out.println("[Master-RegionServerHandler:Initial] Read line 2 (Reported ZK IP): " + reportedZkIp);
                if (reportedZkIp == null || reportedZkIp.trim().isEmpty()) {
                    System.err.println("[Master-RegionServerHandler:Initial] Invalid or missing ZK IP from RegionServer.");
                    return null;
                }

                // 可以在这里添加对 reportedZkIp 格式的验证
                // ...

                System.out.println("[Master] RegionServer (Socket: " + socket.getInetAddress().getHostAddress() + ") reports ZK IP " + reportedZkIp.trim() + ". Registration protocol confirmed.");
                // 可选：发送确认信息给 RegionServer
                // out.println("REGISTER_ACK");

                return reportedZkIp.trim(); // 返回读取到的 ZK IP
            } catch (IOException e) {
                System.err.println("[Master-RegionServerHandler:Initial] IOException during registration reading: " + e.getMessage());
                throw e; // 向上抛出异常
            }
            // 注意：流 in 和 out 保持打开，供后续 run() 方法使用
        }

        /**
         * 在注册成功后启动 Handler 的主线程。
         * @param zkIp 用作此 Handler 的 Key 的 ZK IP。
         */
        public void startHandlerThread(String zkIp) {
            this.zkIpUsedForKey = zkIp; // 保存 ZK IP 用于日志
            this.setName("RegionServerHandler-" + zkIp); // 使用 ZK IP 命名线程
            this.start();
        }


        @Override
        public void run() {
            // 流 'in' 和 'out' 应该已经被 performRegistration 初始化了
            System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] run() method started.");
            if (in == null || out == null) {
                System.err.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Error: Input/Output streams not initialized before run(). Handler terminating.");
                running = false;
                closeConnection();
                return;
            }

            try {
                // 注册已完成，直接进入监听循环
                System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Entering listening loop for messages from RS.");
                String messageFromRegionServer;
                while (running && isSocketAlive(socket) && (messageFromRegionServer = in.readLine()) != null) {
                    System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Received from RS: " + messageFromRegionServer);
                    // 处理来自 RegionServer 的响应或心跳等
                }
                System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Exited listening loop.");

            } catch (IOException e) {
                // 处理 IO 异常
                if (running) {
                    System.err.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] IOException in run loop: " + e.getMessage());
                }
            } catch (Exception e) {
                System.err.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Unexpected Exception in run loop: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Handler thread finishing.");
                running = false;
                closeConnection();
                // 通知 ListenerThread 移除自己（需要传递 Listener 引用或使用其他机制）
                // 最好是由 ZK 触发的 delRegion 来调用 listener.removeRegionServer(this.zkIpUsedForKey)
            }
        }

        public boolean isRunning() {
            return running && socket != null && socket.isConnected() && !socket.isClosed();
        }

        public void forwardCommand(String commandToRegionServer) {
            if (out != null && isRunning()) { // 检查 isRunning 更可靠
                out.println(commandToRegionServer);
                // System.out.println("[Master-RegionServerHandler:" + zkIpUsedForKey + "] Forwarded: " + commandToRegionServer); // 可以减少日志量
            } else {
                System.err.println("[Master-RegionServerHandler:" + (zkIpUsedForKey != null ? zkIpUsedForKey : "Unknown IP") + "] Cannot forward command, handler not running or output stream null.");
            }
        }

        public void closeConnection() {
            // running = false; // 在调用者或 finally 中设置
            try {
                if (in != null) in.close();
            } catch (IOException e) { /* ignore */ }
            try {
                if (out != null) out.close();
            } catch (Exception e) { /* ignore */ } // PrintWriter close doesn't throw IOException
            try {
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (IOException e) { /* ignore */ }
            in = null; // Help GC
            out = null;
            // System.out.println("[Master-RegionServerHandler:" + (zkIpUsedForKey != null ? zkIpUsedForKey : socket.getInetAddress().getHostAddress()) + "] Connection closed.");
        }
    }

    public static boolean isSocketAlive(Socket socket) {
        if (socket == null || socket.isClosed() || !socket.isConnected()) {
            return false;
        }
        try {
            // Sending urgent data is a common way to check.
            // If it throws an exception, the connection is likely dead.
            socket.sendUrgentData(0xFF);
            return true;
        } catch (IOException e) {
            // An IOException here usually means the other side has closed the connection,
            // or there's a network issue.
            return false;
        } catch (Exception e) {
            // Other unexpected exceptions.
            System.err.println("[isSocketAlive] Unexpected error checking socket: " + e.getMessage());
            return false;
        }
    }
}