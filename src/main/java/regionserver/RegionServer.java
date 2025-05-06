package regionserver;

import java.io.IOException;
import java.net.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import javax.net.ServerSocketFactory;

import org.apache.zookeeper.KeeperException;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;

public class RegionServer {
    public static String ip;
    public static String masterCommPort = "5001"; // Port Master listens for RegionServers
    public static String clientListenPort = "4001"; // Port this RegionServer listens for direct clients
    public static String mysqlUser;
    public static String mysqlPwd;
    public static String mysqlPort = "3306"; // MySQL instance port

    public static Connection connection = null;
    public static Statement statement = null;

    public static ServerSocket serverSocket = null;
    public static ThreadPoolExecutor threadPoolExecutor = null;
    public static ZooKeeperManager zooKeeperManager = null; // Make ZK manager accessible if needed by ServerThread/Client

    public static volatile boolean quitSignal = false; // Renamed for clarity
    public static List<TableInform> tables; // This might be redundant if Master/ZK manages all metadata

    public static String serverPath; // ZK path for this server
    // serverValue is not explicitly needed here if ZK updates are handled by Master/ZKManager

    static {
        ip = getIPAddress();
        mysqlUser = "root"; // Example user
        mysqlPwd = "040517cc"; // Example password - Use secure methods in production
        tables = new ArrayList<>(); // Initialize list
    }

    /**
     * Initializes the RegionServer's static resources: ZK connection, DB connection,
     * listening socket, and thread pool. Registers with ZooKeeper.
     *
     * @return Initialized ZooKeeperManager instance.
     * @throws RuntimeException if initialization fails critically.
     */
    public static ZooKeeperManager initRegionServer() {
        zooKeeperManager = new ZooKeeperManager(); // Initialize ZK Manager first
        System.out.println("[RegionServer] Initializing...");

        // Initialize Database Connection
        try {
            System.out.println("[RegionServer] Setting up MySQL connection...");
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            connection = JdbcUtils.getConnection(mysqlUser, mysqlPwd); // Assuming getConnection takes port
            if (connection != null) {
                statement = connection.createStatement();
                System.out.println("[RegionServer] MySQL connection established.");
                clearMysqlData(); // Clear data on initialization
            } else {
                throw new SQLException("Failed to establish database connection.");
            }
        } catch (SQLException e) {
            System.err.println("[RegionServer] FATAL: Failed to initialize database connection: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Database initialization failed", e);
        }

        // Register with ZooKeeper
        System.out.println("[RegionServer] Registering with ZooKeeper...");
        createZooKeeperNode(zooKeeperManager);

        // Create Listening Socket and Thread Pool
        System.out.println("[RegionServer] Creating listening socket and thread pool...");
        createSocketAndThreadPool();

        System.out.println("[RegionServer] Initialization complete. Listening on port " + clientListenPort);
        return zooKeeperManager;
    }

    /**
     * Clears the specific database ('lss') used by this RegionServer.
     * Should be used carefully, typically only during initialization or testing.
     */
    public static void clearMysqlData() {
        if (statement != null) {
            try {
                System.out.println("[RegionServer] Clearing MySQL database 'lss'...");
                // Use the database before dropping/creating tables within it
                statement.execute("CREATE DATABASE IF NOT EXISTS lss");
                statement.execute("USE lss");
                // Example: Drop tables if they exist (more robust than dropping DB)
                // ResultSet rs = statement.executeQuery("SHOW TABLES");
                // List<String> tablesToDrop = new ArrayList<>();
                // while (rs.next()) {
                //     tablesToDrop.add(rs.getString(1));
                // }
                // rs.close();
                // for (String tableName : tablesToDrop) {
                //     System.out.println("[RegionServer] Dropping table: " + tableName);
                //     statement.execute("DROP TABLE IF EXISTS " + tableName);
                // }
                // Or, if really needed:
                // statement.execute("DROP DATABASE IF EXISTS lss");
                // statement.execute("CREATE DATABASE lss");
                // statement.execute("USE lss");
                System.out.println("[RegionServer] MySQL database 'lss' cleared/prepared.");
            } catch (SQLException e) {
                System.err.println("[RegionServer] Error clearing MySQL data: " + e.getMessage());
                e.printStackTrace();
                // Decide if this is fatal for initialization
            }
        } else {
            System.err.println("[RegionServer] Statement is not initialized. Cannot clear MySQL data.");
        }
    }

    /**
     * Creates or updates the ZooKeeper node representing this RegionServer.
     * Stores essential connection information (IP, client listening port, MySQL details).
     *
     * @param zkManager The ZooKeeperManager instance.
     */
    public static void createZooKeeperNode(ZooKeeperManager zkManager) {
        try {
            System.out.println("[RegionServer] Registering with ZooKeeper using ZooKeeperManager.addRegionServer...");

            // 确保 RegionServer.mysqlPort 包含正确的 MySQL 端口号
            boolean success = zkManager.addRegionServer(
                    RegionServer.ip,                  // IP of this RegionServer
                    RegionServer.clientListenPort,    // Port this RS listens on for clients (e.g., 4001)
                    new ArrayList<>(),                // Initial tables (empty)
                    RegionServer.mysqlPwd,            // MySQL password
                    RegionServer.mysqlUser,           // MySQL username
                    RegionServer.masterCommPort,      // Port Master listens on for RS connections (e.g., 5001)
                    RegionServer.mysqlPort            // MySQL port for this RegionServer's DB (e.g., 3306)
            );

            if (success) {
                System.out.println("[RegionServer] ZooKeeper registration/update successful via ZooKeeperManager for IP: " + RegionServer.ip);
                RegionServer.serverPath = "/lss/region_server/" + RegionServer.ip; // For reference
            } else {
                System.err.println("[RegionServer] FATAL: ZooKeeperManager.addRegionServer returned false for IP: " + RegionServer.ip);
                throw new RuntimeException("ZooKeeper registration failed (addRegionServer returned false)");
            }

        } catch (Exception e) { // Catch more general exceptions from addRegionServer
            System.err.println("[RegionServer] FATAL: Error during ZooKeeper registration: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("ZooKeeper registration failed", e);
        }
    }


    /**
     * Creates the ServerSocket for listening to direct client connections and
     * initializes the thread pool for handling them.
     */
    public static void createSocketAndThreadPool() {
        try {
            ServerSocketFactory serverSocketFactory = ServerSocketFactory.getDefault();
            // Listen on the designated client port
            serverSocket = serverSocketFactory.createServerSocket(Integer.parseInt(clientListenPort));
            System.out.println("[RegionServer] Server socket created, listening on port: " + clientListenPort);
        } catch (IOException e) {
            System.err.println("[RegionServer] FATAL: Error creating server socket on port " + clientListenPort + ": " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Socket creation failed", e);
        }

        // Fixed thread pool for handling client requests
        threadPoolExecutor = new ThreadPoolExecutor(
                10, // corePoolSize
                50, // maximumPoolSize
                60L, // keepAliveTime
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(100), // workQueue (use LinkedBlockingQueue for unbounded)
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy() // policy for rejected tasks (can adjust)
        );
        System.out.println("[RegionServer] Thread pool created.");
    }

    /**
     * Starts the main loop to accept incoming client connections on the clientListenPort.
     * Each connection is handled by a ServerThread via the thread pool.
     * This should run in its own thread.
     */
    public static void startListening() {
        if (serverSocket == null || threadPoolExecutor == null) {
            System.err.println("[RegionServer] Cannot start listening, socket or thread pool not initialized.");
            return;
        }
        System.out.println("[RegionServer] Starting listener loop for client connections on port " + clientListenPort);
        while (!quitSignal && !serverSocket.isClosed()) {
            try {
                Socket clientConnSocket = serverSocket.accept();
                System.out.println("[RegionServer] Accepted connection from: " + clientConnSocket.getRemoteSocketAddress());
                // Pass the local statement and tables list (if needed by ServerThread logic)
                ServerThread clientHandler = new ServerThread(clientConnSocket, statement, (ArrayList<TableInform>) tables);
                threadPoolExecutor.submit(clientHandler);
            } catch (IOException e) {
                if (quitSignal || serverSocket.isClosed()) {
                    System.out.println("[RegionServer] Server socket closed, stopping listener loop.");
                    break;
                }
                System.err.println("[RegionServer] Error accepting client connection: " + e.getMessage());
                // Avoid busy-waiting on error
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        System.out.println("[RegionServer] Listener loop finished.");
        shutdown(); // Clean up resources when loop ends
    }


    /**
     * Utility method to get the primary non-loopback IP address.
     *
     * @return String representation of the IP address, or "127.0.0.1" as fallback.
     */
    public static String getIPAddress() {
        try {
            // Prefer non-loopback IPv4 address
            NetworkInterface networkInterface = NetworkInterface.networkInterfaces()
                    .filter(ni -> {
                        try {
                            return ni.isUp() && !ni.isLoopback() && !ni.isVirtual();
                        } catch (SocketException e) {
                            return false;
                        }
                    })
                    .findFirst()
                    .orElse(null);

            if (networkInterface != null) {
                InetAddress inetAddress = networkInterface.getInterfaceAddresses().stream()
                        .map(InterfaceAddress::getAddress)
                        .filter(addr -> addr instanceof java.net.Inet4Address)
                        .findFirst()
                        .orElse(InetAddress.getLocalHost()); // Fallback if no IPv4 found on preferred interface
                return inetAddress.getHostAddress();
            } else {
                // Fallback if no suitable interface found
                return InetAddress.getLocalHost().getHostAddress();
            }
        } catch (Exception e) {
            System.err.println("[RegionServer] Error getting IP address: " + e.getMessage() + ". Falling back to 127.0.0.1");
            return "127.0.0.1"; // Fallback IP
        }
    }

    /**
     * Shuts down the RegionServer resources gracefully.
     */
    public static void shutdown() {
        System.out.println("[RegionServer] Shutting down...");
        quitSignal = true; // Signal listening loop to stop

        // Shutdown thread pool
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!threadPoolExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    threadPoolExecutor.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!threadPoolExecutor.awaitTermination(60, TimeUnit.SECONDS))
                        System.err.println("[RegionServer] Pool did not terminate");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                threadPoolExecutor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }

        // Close server socket
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
                System.out.println("[RegionServer] Server socket closed.");
            } catch (IOException e) {
                System.err.println("[RegionServer] Error closing server socket: " + e.getMessage());
            }
        }

        // Close database connection
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("[RegionServer] Database connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("[RegionServer] Error closing database resources: " + e.getMessage());
        }

        // Close ZooKeeper connection
        if (zooKeeperManager != null) {
            zooKeeperManager.close();
            System.out.println("[RegionServer] ZooKeeper connection closed.");
        }
        System.out.println("[RegionServer] Shutdown complete.");
    }

    // Utility method to get an available TCP port (use with caution, race conditions possible)
    // It's generally better to use a fixed, configured port.
    /*
    public static int getAvailableTcpPort() {
        for (int i = 4000; i <= 5000; i++) { // Scan a smaller range
            try (ServerSocket ss = new ServerSocket(i)) {
                ss.setReuseAddress(true); // Allow faster reuse
                return i;
            } catch (IOException e) {
                // Port likely in use
            }
        }
        System.err.println("[RegionServer] WARN: No available port found in range 4000-5000. Falling back to 0 (OS default).");
        return 0; // Let the OS pick a port if none found
    }
    */
}