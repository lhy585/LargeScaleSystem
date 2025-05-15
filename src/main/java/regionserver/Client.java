package regionserver;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static master.RegionManager.MASTER_IP;

/**
 * RegionServer 的主进程.
 * 1. 初始化静态的 RegionServer 资源 (数据库连接, ZooKeeper连接, 监听Socket等).
 * 2. 启动一个线程来监听直接的客户端连接 (例如处理 SELECT).
 * 3. 连接到 Master (端口 5001) 进行注册并接收命令 (DDL/DML).
 */
public class Client {
    private static final String MASTER_HOST = MASTER_IP; // Master 的主机地址
    // Master 监听 RegionServer 连接的端口
    private static final int MASTER_REGION_SERVER_PORT = 5001;

    private Socket masterSocket;
    private BufferedWriter writerToMaster;
    private BufferedReader readerFromMaster;
    private volatile boolean running = true;

    public static void main(String[] args) {
        // 1. 初始化 RegionServer 静态资源
        try {
            RegionServer.initRegionServer();
        } catch (RuntimeException e) {
            System.err.println("[RegionServer Process] 初始化失败: " + e.getMessage());
            System.err.println("[RegionServer Process] 退出.");
            return; // 初始化失败则停止
        }

        // 2. 启动监听线程，处理直接的客户端连接 (在 RegionServer.clientListenPort 上)
        Thread listenerThread = new Thread(RegionServer::startListening);
        listenerThread.setDaemon(false); // 保持进程存活
        listenerThread.setName("RegionServer-ClientListener-" + RegionServer.clientListenPort);
        listenerThread.start();

        // 3. 连接到 Master 并处理命令
        Client regionServerProcess = new Client();
        try {
            regionServerProcess.start(); // 连接到 master 并开始监听命令
            System.out.println("[RegionServer Process] 运行中. 在端口 " + RegionServer.clientListenPort +
                    " 监听客户端连接，并已连接到 Master 的端口 " + MASTER_REGION_SERVER_PORT);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[RegionServer Process] 关闭钩子被触发.");
                regionServerProcess.stop(); // 停止与 Master 的连接
                RegionServer.shutdown();   // 关闭 RegionServer 的静态资源
            }));

            // 保持主线程存活 (监听线程和 Master 通信循环会保持进程运行)
            while (regionServerProcess.running) {
                try {
                    Thread.sleep(5000); // 定期检查状态
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // 重置中断状态
                    System.out.println("[RegionServer Process] 主线程被中断.");
                    regionServerProcess.running = false; // 退出循环
                }
            }

        } catch (IOException e) { // 从 start() 抛出的 IOException
            System.err.println("[RegionServer Process] Master 通信设置或运行时错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 确保资源被清理
            regionServerProcess.stop(); // 确保停止 Master 连接循环
            // RegionServer.shutdown();
            System.out.println("[RegionServer Process] 退出.");
        }
    }

    /**
     * 启动到 Master 的连接: 连接, 注册, 并监听命令.
     */
    public void start() throws IOException {
        connectToMaster();
        registerWithMaster();
        listenForMasterCommands(); // 开始监听循环 (在当前线程运行)
    }

    private void connectToMaster() throws IOException {
        System.out.println("[RegionServer Process] 正在连接到 Master " + MASTER_HOST + ":" + MASTER_REGION_SERVER_PORT + "...");
        masterSocket = new Socket(MASTER_HOST, MASTER_REGION_SERVER_PORT);
        // 确保使用 UTF-8 编码进行可靠通信
        writerToMaster = new BufferedWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8));
        readerFromMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8));
        System.out.println("[RegionServer Process] 已连接到 Master.");
    }

    /**
     * 向 Master 注册此 RegionServer.
     * 发送必要的身份或信息.
     */
    private void registerWithMaster() throws IOException {
        System.out.println("[RegionServer Process] 正在向 Master 注册...");
        // 协议: 第1行 = 命令, 第2行 = ZK 中使用的 IP
        writerToMaster.write("REGISTER_REGION_SERVER");
        writerToMaster.newLine();
        writerToMaster.write(RegionServer.ip); // 发送在 ZK 注册的 IP
        writerToMaster.newLine();
        writerToMaster.flush();
        System.out.println("[RegionServer Process] 注册信息已发送给 Master (包含 ZK IP: " + RegionServer.ip + ").");
    }


    /**
     * 监听来自 Master 的命令并处理它们.
     * 这个循环在此 Client 对象的主线程中运行.
     */
    private void listenForMasterCommands() {
        System.out.println("[RegionServer Process] 正在监听来自 Master 的命令...");
        try {
            String commandFromMaster;
            while (running && (commandFromMaster = readerFromMaster.readLine()) != null) {
                System.out.println("[RegionServer Process] 从 Master 收到命令: \"" + commandFromMaster + "\"");

                // 简单的心跳或测试命令检查
                if ("PING".equalsIgnoreCase(commandFromMaster)) {
                    sendResponseToMaster("PONG");
                    continue;
                }
                if ("QUIT".equalsIgnoreCase(commandFromMaster)) {
                    System.out.println("[RegionServer Process] 从 Master 收到 QUIT 命令.");
                    running = false;
                    break;
                }
                // 处理从 Master 接收到的 SQL 命令
                handleSqlCommand(commandFromMaster);

            }
        } catch (IOException e) {
            if (running) { // 只有在非主动停止时才记录错误
                System.err.println("[RegionServer Process] 监听 Master 命令时发生 IOException: " + e.getMessage());
                running = false; // 通信错误时停止运行
            }
        } finally {
            System.out.println("[RegionServer Process] 停止监听 Master 命令.");
            stop(); // 确保资源被关闭
        }
    }

    /**
     * 处理从 Master 收到的通用 SQL 命令.
     * 判断类型 (SELECT vs. 其他) 并使用 ServerClient 执行.
     * 向 Master 发回简单的 SUCCESS/FAILURE 响应.
     *
     * @param sqlCommand 从 Master 收到的 SQL 命令字符串.
     */
    private void handleSqlCommand(String sqlCommand) {
        String commandType = "UNKNOWN";
        boolean success = false;
        String resultData = null; // 用于 SELECT 结果

        try {
            String trimmedCommand = sqlCommand.trim().toLowerCase();
            // 基本的命令类型检测
            if (trimmedCommand.startsWith("select")) {
                commandType = "SELECT";
                resultData = ServerClient.selectTable(sqlCommand); // 执行 SELECT
                success = (resultData != null); // 只要没抛异常，即使结果为空也算成功
            } else if (trimmedCommand.startsWith("insert")) {
                commandType = "INSERT";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("update")) {
                commandType = "UPDATE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("delete")) {
                commandType = "DELETE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("create table")) {
                commandType = "CREATE_TABLE";
                success = ServerClient.createTable(sqlCommand); // 使用只执行命令的方法
            } else if (trimmedCommand.startsWith("drop table")) {
                commandType = "DROP_TABLE";
                String tableName = ServerClient.extractTableName(sqlCommand);
                if (tableName != null) {
                    success = ServerClient.dropTable(tableName); // 假设 dropTable 只需表名
                } else {
                    System.err.println("[RegionServer Process] Failed to extract table name for DROP: " + sqlCommand);
                    success = ServerClient.executeCmd(sqlCommand); // 尝试通用执行
                }
            } else if (trimmedCommand.startsWith("alter table")) {
                commandType = "ALTER_TABLE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("truncate")) {
                commandType = "TRUNCATE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("copy_table")) { // 处理自定义的 COPY_TABLE 命令
                commandType = "COPY_TABLE";
                success = handleCopyTableCommand(sqlCommand);
            }
            else if (trimmedCommand.startsWith("migrate_table_from_source ")) { // 注意末尾空格
                commandType = "MIGRATE_TABLE_FROM_SOURCE";
                success = handleFullMigrateCommand(sqlCommand); // 新的处理函数
            }
            else {
                System.err.println("[RegionServer Process] 收到无法识别的命令结构: " + sqlCommand);
                commandType = "EXECUTE_UNKNOWN";
                success = ServerClient.executeCmd(sqlCommand); // 尝试执行
            }

            // 向 Master 发送响应
            if ("SELECT".equals(commandType)) {
                if (success) {
                    sendResponseToMaster("SUCCESS SELECT");
                    if (resultData.isEmpty()) {
                        sendResponseToMaster("END_OF_DATA");
                    } else {
                        try (BufferedReader resultReader = new BufferedReader(new StringReader(resultData))) {
                            String line;
                            while ((line = resultReader.readLine()) != null) {
                                sendResponseToMaster(line);
                            }
                        }
                        sendResponseToMaster("END_OF_DATA");
                    }
                } else {
                    sendResponseToMaster("FAILURE SELECT");
                }
            } else {
                // 对于非 SELECT 命令
                if (success) {
                    sendResponseToMaster("SUCCESS " + commandType);
                } else {
                    sendResponseToMaster("FAILURE " + commandType);
                }
            }

        } catch (Exception e) {
            System.err.println("[RegionServer Process] 处理命令 (" + commandType + ") 时出错: " + sqlCommand);
            System.err.println("[RegionServer Process] 异常: " + e.getMessage());
            e.printStackTrace();
            sendResponseToMaster("FAILURE " + commandType + " Exception: " + e.getMessage());
        }
    }

    /**
     * 处理 Master 发送的自定义 COPY_TABLE 命令.
     * 格式: COPY_TABLE <source_host> <source_port> <source_user> <source_pwd> <source_table> <target_table>
     * @param command 命令字符串
     * @return 是否成功启动复制过程
     */
    private boolean handleCopyTableCommand(String command) {
        String prefix = "COPY_TABLE ";
        if (!command.startsWith(prefix)) {
            return false;
        }
        String[] parts = command.substring(prefix.length()).split("\\s+");
        if (parts.length != 6) {
            System.err.println("[RegionServer Process] Invalid COPY_TABLE command format: " + command);
            return false;
        }
        String sourceHost = parts[0];
        String sourcePort = parts[2];
        String sourceUser = parts[3];
        String sourcePwd = parts[1];
        String sourceTable = parts[4];
        String targetTable = parts[5]; // 这是在本 RegionServer 上创建的表名

        System.out.println("[RegionServer Process] Received request to copy table " + sourceTable + " from " + sourceHost + ":" + sourcePort + " to local table " + targetTable);
        boolean copySuccess = ServerMaster.dumpTable(
                sourceHost, // sourceRegionName (用于日志)
                sourceHost, sourcePort, sourceUser, sourcePwd,
                sourceTable,
                RegionServer.mysqlUser, // 本地目标数据库用户名
                RegionServer.mysqlPwd   // 本地目标数据库密码
        );

        if (copySuccess) {
            System.out.println("[RegionServer Process] Table copy initiated successfully for " + targetTable);
            // 在 ServerMaster.dumpTable 成功后，可以选择性地修改表名（如果需要）
            if (!sourceTable.equals(targetTable)) {
                System.out.println("[RegionServer Process] Renaming imported table " + sourceTable + " to " + targetTable);
                String renameSql = "RENAME TABLE `" + sourceTable + "` TO `" + targetTable + "`;";
                try {
                    ServerClient.executeCmd(renameSql); // 使用本地执行
                    System.out.println("[RegionServer Process] Table renamed successfully.");
                } catch (Exception e) {
                    System.err.println("[RegionServer Process] Failed to rename table after copy: " + e.getMessage());
                    // 即使重命名失败，复制也可能部分成功，但返回 false 表示整个操作未完全成功
                    return false;
                }
            }
        } else {
            System.err.println("[RegionServer Process] Table copy initiation failed for " + targetTable);
        }
        return copySuccess;
    }

    /**
     * 处理 Master 发送的完整迁移命令。
     * 格式: MIGRATE_TABLE_FROM_SOURCE <source_host_ip> <source_mysql_port> <source_mysql_user> <source_mysql_pwd> <table_name_to_migrate>
     */
    private boolean handleFullMigrateCommand(String command) {
        String prefix = "MIGRATE_TABLE_FROM_SOURCE ";
        if (!command.startsWith(prefix)) {
            System.err.println("[RegionServer Process] MIGRATE_TABLE_FROM_SOURCE 命令格式错误 (前缀不匹配): " + command);
            return false;
        }
        String params = command.substring(prefix.length());
        String[] parts = params.split("\\s+"); // 按空格分割参数
        if (parts.length != 5) {
            System.err.println("[RegionServer Process] MIGRATE_TABLE_FROM_SOURCE 命令参数数量错误 (期望5个): " + params);
            return false;
        }
        String sourceHost = parts[0];
        String sourcePort = parts[1];
        String sourceUser = parts[2];
        String sourcePwd = parts[3];
        String tableName = parts[4]; // 表在源和目标上名称相同

        System.out.println("[RegionServer Process] 收到完整迁移请求: 表 " + tableName + " 从源 " + sourceHost + ":" + sourcePort);

        // 调用 ServerMaster.executeCompleteMigrationSteps
        // 它会：1. dumpTable (从源拉取并导入本地) 2. 连接到源并删除源表
        return ServerMaster.executeCompleteMigrationSteps(
                sourceHost, sourcePort, sourceUser, sourcePwd, // 源数据库连接信息
                tableName,
                RegionServer.mysqlUser, // 当前 RegionServer (目标) 的 MySQL 用户名
                RegionServer.mysqlPwd   // 当前 RegionServer (目标) 的 MySQL 密码
        );
    }

    /**
     * 向 Master 发送响应消息.
     *
     * @param response 要发送的消息字符串.
     */
    private void sendResponseToMaster(String response) {
        if (writerToMaster != null) {
            try {
                writerToMaster.write(response);
                writerToMaster.newLine();
                writerToMaster.flush();
            } catch (IOException e) {
                System.err.println("[RegionServer Process] 向 Master 发送响应失败: " + e.getMessage());
                running = false; // 如果无法通信则停止
            }
        } else {
            System.err.println("[RegionServer Process] 无法发送响应，到 Master 的 writer 为 null.");
        }
    }

    /**
     * 停止客户端到 Master 的连接并将运行标志设为 false.
     */
    public void stop() {
        if (!running) return; // 防止重复停止
        System.out.println("[RegionServer Process] 正在停止到 Master 的连接...");
        running = false; // 信号循环退出
        try {
            if (writerToMaster != null) writerToMaster.close();
        } catch (Exception e) { /* 忽略 */ }
        try {
            if (readerFromMaster != null) readerFromMaster.close();
        } catch (IOException e) { /* 忽略 */ }
        try {
            if (masterSocket != null && !masterSocket.isClosed()) masterSocket.close();
        } catch (IOException e) {  }
        finally{ // 确保引用被清空
            writerToMaster = null;
            readerFromMaster = null;
            masterSocket = null;
            System.out.println("[RegionServer Process] 到 Master 的连接已关闭.");
        }
    }
}