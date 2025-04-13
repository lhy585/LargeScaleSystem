package regionserver;

import java.net.ServerSocket;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;

import java.io.IOException;
import java.net.*;
import java.io.BufferedReader;
import java.io.PrintWriter;

import zookeeper.ZooKeeperUtils;
import org.apache.curator.framework.CuratorFramework;

class TableInfo {
    public String tableName;
    public boolean slave;
    public String slaveIp;
    public String slavePort;
    public String slaveUsr;
    public String slavePwd;
    public String slaveSqlPort;

    TableInfo(String tableName) {
        this.slave = false;
        this.tableName = tableName;
        this.slaveUsr = "root";
        this.slavePwd = "123";
        this.slaveSqlPort = "3306";
    }

    public void setSlave(String ip, String port) {
        this.slave = true;
        this.slaveIp = ip;
        this.slavePort = port;
    }
}

public class RegionServer {
    public static String ip;
    public static String port;
    public static String mysqlUser;
    public static String mysqlPwd;

    public static Connection connection = null;
    public static Statement statement = null;

    public static Boolean quitSignal = false;
    public static ArrayList<TableInfo> tables;

    public static String serverPath;
    public static String serverValue;
    public static ZooKeeperUtils zooKeeperUtils;

    static {
        ip = getIPAddress();
        mysqlUser = "root";
        mysqlPwd = "040517cc";
        port = "1001";
        tables = new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[Info] Region Server is initializing...");
        initRegionServer(); // 初始化
        System.out.println("[Info] Initializing successfully!");

        // Start listener threads
        new ClientListenerThread(Integer.parseInt(port)).start(); // 启动监听线程
        new MasterListenerThread(Integer.parseInt(port) + 1).start();
    }

    public static void initRegionServer() throws Exception {
        zooKeeperUtils.connectZookeeper("localhost:2181");
        connection = JdbcUtils.getConnection(mysqlUser, mysqlPwd);
        try {
            assert connection != null;
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.println("[Error] SQL Exception: " + e.getMessage());
        }
        clearMysqlData();
        createZooKeeperNode(zooKeeperUtils);
    }

    public static void clearMysqlData() {
        if (connection != null && statement != null) {
            try {
                // Delete existing database
                String deleteDB = "drop database if exists lss";
                statement.execute(deleteDB);
                // Recreate database
                String createDB = "create database if not exists lss";
                statement.execute(createDB);

                // Use the newly created database
                String useDB = "use lss";
                statement.execute(useDB);
            } catch (Exception e) {
                System.out.println("[Error] Failed to clear MySQL data: " + e.getMessage());
            }
        }
    }

    public static void createZooKeeperNode(ZooKeeperUtils zooKeeperUtils) {
        try {
            List<String> serverNodes = zooKeeperUtils.getChildren("/lss/region_servers");

            int max = 0;
            for (String node : serverNodes) {
                int index = Integer.valueOf(node.substring(node.indexOf("_") + 1));
                if (index > max) {
                    max = index;
                }
            }

            serverPath = "/lss/region_servers/server_" + (max + 1);
            serverValue = ip + "," + port + "," + mysqlUser + "," + mysqlPwd + "," + "3306" + ",0";

            zooKeeperUtils.createNode(serverPath, serverValue);

        } catch (Exception e) {
            System.out.println("[Error] Failed to create ZooKeeper node: " + e.getMessage());
        }
    }

    public static String getIPAddress() {
        String res = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println("Local HostAddress: " + addr.getHostAddress());
            res = addr.getHostAddress();
            String hostname = addr.getHostName();
            System.out.println("Local host name: " + hostname);
        } catch (UnknownHostException e) {
            System.out.println("[Error] Failed to get IP address: " + e.getMessage());
        }
        return res;
    }

    // Thread 1: Listen for client connections
    private static class ClientListenerThread extends Thread {
        private final int port;

        public ClientListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Info] Listening for clients on port " + port + "...");
                while (!quitSignal) {
                    Socket clientSocket = serverSocket.accept();
                    new ClientHandler(clientSocket).start();
                }
            } catch (IOException e) {
                System.err.println("[Error] ClientListenerThread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // Thread 2: Listen for master connections
    private static class MasterListenerThread extends Thread {
        private final int port;

        public MasterListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("[Info] Listening for master on port " + port + "...");
                while (!quitSignal) {
                    Socket masterSocket = serverSocket.accept();
                    new MasterHandler(masterSocket).start();
                }
            } catch (IOException e) {
                System.err.println("[Error] MasterListenerThread failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static class ClientHandler extends Thread {
        private final Socket socket;
        private final ThreadPoolExecutor threadPoolExecutor;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.threadPoolExecutor = new ThreadPoolExecutor(60,
                    100,
                    20,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(20),
                    Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy());
        }

        @Override
        public void run() {
            System.out.println("[Info] New client connected: " + socket.getInetAddress() + ":" + socket.getPort());
            try {
                threadPoolExecutor.submit(new ServerThread(socket, statement, tables));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("[Error] Failed to close client socket: " + e.getMessage());
                }
            }
        }
    }

    private static class MasterHandler extends Thread {
        private final Socket socket;

        MasterHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("[Info] New master connection: " + socket.getInetAddress() + ":" + socket.getPort());
            // TODO: Implement heartbeat monitoring, load reporting, data synchronization, etc.
            try {
                while (isSocketAlive(socket)) {
                    // Handle master commands and heartbeats
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                System.out.println("[Error] Master connection error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("[Error] Failed to close master socket: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Check if socket connection is still alive
     * @return true: connection alive;
     *         false: connection closed
     */
    public static boolean isSocketAlive(Socket socket) {
        try {
            socket.sendUrgentData(0xFF); // Send 1 byte urgent data
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}