package regionserver;

import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RegionServer {
    public static String ip;
    public static String masterCommPort = "5001"; // Master 监听 RegionServer 连接的端口
    public static String clientListenPort = "4001"; // 此 RegionServer 监听客户端直接连接的端口
    public static String mysqlUser;
    public static String mysqlPwd;
    public static String mysqlPort = "3306"; // 此 RegionServer 对应的 MySQL 实例端口

    public static Connection connection = null;
    public static Statement statement = null;

    public static ServerSocket serverSocket = null;
    public static ThreadPoolExecutor threadPoolExecutor = null;
    public static ZooKeeperManager zooKeeperManager = null; // ZK 管理器实例

    public static volatile boolean quitSignal = false; // 退出信号
    public static List<TableInform> tables; // 当前 RegionServer 负责的表信息 (可能由 Master/ZK 管理更佳)

    public static String DBpwd = "123456";
    public static String serverPath; // 此服务器在 ZK 中的路径

    static {
        ip = getIPAddress();
        mysqlUser = "root"; // 示例用户名
        mysqlPwd = DBpwd; // 示例密码 (生产环境应使用安全方式配置)
        tables = new ArrayList<>(); // 初始化列表
    }

    /**
     * 初始化 RegionServer 的静态资源: ZK 连接, 数据库连接, 监听 Socket, 线程池. 并向 ZooKeeper 注册.
     *
     * @return 初始化后的 ZooKeeperManager 实例.
     * @throws RuntimeException 如果关键初始化步骤失败.
     */
    public static ZooKeeperManager initRegionServer() {
        zooKeeperManager = new ZooKeeperManager();
        System.out.println("[RegionServer] 初始化...");

        // 初始化数据库连接
        try {
            System.out.println("[RegionServer] 正在设置 MySQL 连接...");
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            // 使用用户名和密码连接到 localhost (JdbcUtils内部处理)
            connection = JdbcUtils.getConnection(mysqlUser, mysqlPwd);
            if (connection != null) {
                statement = connection.createStatement();
                System.out.println("[RegionServer] MySQL 连接已建立.");
                clearMysqlData(); // 初始化时清理数据库
            } else {
                throw new SQLException("无法建立数据库连接.");
            }
        } catch (SQLException e) {
            System.err.println("[RegionServer] 致命错误: 初始化数据库连接失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("数据库初始化失败", e);
        }

        // 向 ZooKeeper 注册
        System.out.println("[RegionServer] 正在向 ZooKeeper 注册...");
        createZooKeeperNode(zooKeeperManager);

        // 创建监听 Socket 和线程池
        System.out.println("[RegionServer] 正在创建监听 Socket 和线程池...");
        createSocketAndThreadPool();

        System.out.println("[RegionServer] 初始化完成. 在端口 " + clientListenPort + " 上监听.");
        return zooKeeperManager;
    }

    /**
     * 清理此 RegionServer 使用的特定数据库 ('lss').
     * 应谨慎使用, 通常只在初始化或测试期间进行.
     */
    public static void clearMysqlData() {
        if (statement != null) {
            try {
                System.out.println("[RegionServer] 正在清理 MySQL 数据库 'lss'...");
                statement.execute("CREATE DATABASE IF NOT EXISTS lss");
                statement.execute("USE lss");
                System.out.println("[RegionServer] MySQL 数据库 'lss' 已清理/准备就绪.");
            } catch (SQLException e) {
                System.err.println("[RegionServer] 清理 MySQL 数据时出错: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.err.println("[RegionServer] Statement 未初始化. 无法清理 MySQL 数据.");
        }
    }

    /**
     * 使用 ZooKeeperManager.addRegionServer 在 ZK 中创建或更新代表此 RegionServer 的节点结构.
     *
     * @param zkManager ZooKeeperManager 实例.
     */
    public static void createZooKeeperNode(ZooKeeperManager zkManager) {
        try {
            System.out.println("[RegionServer] 使用 ZooKeeperManager.addRegionServer 向 ZooKeeper 注册...");

            // 调用管理器方法来创建 ZK 结构
            boolean success = zkManager.addRegionServer(
                    RegionServer.ip,                  // 本 RegionServer 的 IP
                    RegionServer.clientListenPort,    // 本 RS 监听客户端的端口 (例如 "4001")
                    new ArrayList<>(),                // 初始表列表 (空)
                    RegionServer.mysqlPwd,            // MySQL 密码
                    RegionServer.mysqlUser,           // MySQL 用户名
                    RegionServer.masterCommPort,      // Master 监听 RS 的端口 (例如 "5001")
                    RegionServer.mysqlPort            // 本 RS 的 MySQL 端口 (例如 "3306")
            );

            if (success) {
                System.out.println("[RegionServer] ZooKeeper 注册/更新成功 (通过 ZooKeeperManager) IP: " + RegionServer.ip);
                RegionServer.serverPath = "/lss/region_server/" + RegionServer.ip; // 保存 ZK 路径供参考
            } else {
                System.err.println("[RegionServer] 致命错误: ZooKeeperManager.addRegionServer 返回 false, IP: " + RegionServer.ip);
                throw new RuntimeException("ZooKeeper 注册失败 (addRegionServer 返回 false)");
            }

        } catch (Exception e) {
            System.err.println("[RegionServer] 致命错误: ZooKeeper 注册期间出错: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("ZooKeeper 注册失败", e);
        }
    }


    /**
     * 创建用于监听直接客户端连接的 ServerSocket, 并初始化用于处理这些连接的线程池.
     */
    public static void createSocketAndThreadPool() {
        try {
            ServerSocketFactory serverSocketFactory = ServerSocketFactory.getDefault();
            serverSocket = serverSocketFactory.createServerSocket(Integer.parseInt(clientListenPort));
//            serverSocket = new ServerSocket(Integer.parseInt(clientListenPort));
            System.out.println("[RegionServer] 服务器 Socket 已创建, 监听端口: " + clientListenPort);
        } catch (IOException e) {
            System.err.println("[RegionServer] 致命错误: 在端口 " + clientListenPort + " 创建服务器 Socket 时出错: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Socket 创建失败", e);
        }

        // 用于处理客户端请求的固定线程池
        threadPoolExecutor = new ThreadPoolExecutor(
                10, // 核心线程数
                50, // 最大线程数
                60L, // 保持活动时间
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(100), // 工作队列
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );
        System.out.println("[RegionServer] 线程池已创建.");
    }

    /**
     * 启动主循环以接受在 clientListenPort 上的入站客户端连接.
     * 每个连接由线程池中的 ServerThread 处理.
     * 这个方法应该在它自己的线程中运行.
     */
    public static void startListening() {
        if (serverSocket == null || threadPoolExecutor == null) {
            System.err.println("[RegionServer] 无法开始监听，Socket 或线程池未初始化.");
            return;
        }
        System.out.println("[RegionServer] 开始监听循环，处理端口 " + clientListenPort + " 上的客户端连接");
        while (!quitSignal && !serverSocket.isClosed()) {
            try {
                Socket clientConnSocket = serverSocket.accept();
                System.out.println("[RegionServer] 接收到来自以下地址的连接: " + clientConnSocket.getRemoteSocketAddress());
                // 传递本地 statement 和 tables 列表 (如果 ServerThread 逻辑需要)
                ServerThread clientHandler = new ServerThread(clientConnSocket, statement, (ArrayList<TableInform>) tables);
                threadPoolExecutor.submit(clientHandler);
            } catch (IOException e) {
                if (quitSignal || serverSocket.isClosed()) {
                    System.out.println("[RegionServer] 服务器 Socket 已关闭, 停止监听循环.");
                    break;
                }
                System.err.println("[RegionServer] 接受客户端连接时出错: " + e.getMessage());
                try {
                    Thread.sleep(100); // 避免错误时空转
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        System.out.println("[RegionServer] 监听循环结束.");
        // shutdown(); // 监听结束后不一定立即关闭整个服务，除非 quitSignal 被设置
    }


    /**
     * 获取本机主要的非环回 IP 地址的工具方法.
     *
     * @return IP 地址的字符串表示, 或 "127.0.0.1" 作为备选.
     */
    public static String getIPAddress() {
        try {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            while (nets.hasMoreElements()) {
                NetworkInterface netint = nets.nextElement();
                // 排除掉回环接口和未启用的接口
                if (!netint.isUp() || netint.isLoopback() || netint.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress addr = inetAddresses.nextElement();
                    if (addr instanceof Inet4Address) {
                        String ip = addr.getHostAddress();
                        if (ip.startsWith("10.")) {
                            return ip;
                        }
                    }
                }
            }
            return "127.0.0.1";
        } catch (Exception e) {
            System.err.println("[RegionServer] Error getting IP address: " + e.getMessage() + ". Falling back to 127.0.0.1");
            return "127.0.0.1"; // Fallback IP
        }
    }

    /**
     * 优雅地关闭 RegionServer 资源.
     */
    public static void shutdown() {
        if (quitSignal) return; // 防止重复关闭
        System.out.println("[RegionServer] 正在关闭...");
        quitSignal = true; // 信号监听循环停止

        // 关闭线程池
        if (threadPoolExecutor != null) {
            System.out.println("[RegionServer] 关闭线程池...");
            threadPoolExecutor.shutdown();
            try {
                if (!threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    threadPoolExecutor.shutdownNow();
                    if (!threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS))
                        System.err.println("[RegionServer] 线程池未能终止");
                }
            } catch (InterruptedException ie) {
                threadPoolExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            System.out.println("[RegionServer] 线程池已关闭.");
        }

        // 关闭服务器 Socket
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
                System.out.println("[RegionServer] 服务器 Socket 已关闭.");
            } catch (IOException e) {
                System.err.println("[RegionServer] 关闭服务器 Socket 时出错: " + e.getMessage());
            }
        }

        // 关闭数据库连接
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("[RegionServer] 数据库连接已关闭.");
            }
        } catch (SQLException e) {
            System.err.println("[RegionServer] 关闭数据库资源时出错: " + e.getMessage());
        }

        // 关闭 ZooKeeper 连接
        if (zooKeeperManager != null) {
            zooKeeperManager.close();
            System.out.println("[RegionServer] ZooKeeper 连接已关闭.");
        }
        System.out.println("[RegionServer] 关闭完成.");
    }
}