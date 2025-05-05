package regionserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

import org.apache.zookeeper.KeeperException;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;

class TableInfo {
    public String tableName;
    public boolean slave;
    public String slaveIp;
    public String slavePort;
    public String slaveUsr;
    public String slavePwd;
    public String slaveSqlPort;

    TableInfo(String tableName){
        this.slave = false;
        this.tableName = tableName;
        this.slaveUsr = "root";
        this.slavePwd = "123";
        this.slaveSqlPort = "3306";
    }
    public void setSlave(String ip, String port){
        this.slave = true;
        this.slaveIp = ip;
        this.slavePort = port;
    }
}

public class RegionServer implements Runnable {
    public static String ip;
    public static String port;
    public static String mysqlUser;
    public static String mysqlPwd;

    public static Connection connection = null;
    public static Statement statement = null;

    public static ServerSocket serverSocket = null;
    public static ThreadPoolExecutor threadPoolExecutor = null;

    public static Boolean quitSignal = false;
    public static List<TableInform> tables;

    public static String serverPath;
    public static String serverValue;

    static {
        ip = getIPAddress();
        mysqlUser = "root";
        mysqlPwd = "040517cc";
        port = "5001";
        tables = new ArrayList<>();
    }

    @Override
    public void run() {
        ZooKeeperManager zooKeeperManager = initRegionServer();

        // 主动连接 Master
        try (Socket masterSocket = new Socket("127.0.0.1", Integer.parseInt(port)); // 连接 Master
             BufferedReader in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
             PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true)) {

            System.out.println("[RegionServer] Connected to Master!");

            // 发送注册消息
            out.println("REGISTER_REGION_SERVER");
            out.flush();

            // 等待 Master 的响应
            String response = in.readLine();
            System.out.println("[RegionServer] Master response: " + response);

            // 持续接收 Master 的指令
            String command;
            while ((command = in.readLine()) != null) {
                System.out.println("[RegionServer] Received command: " + command);
                // 处理 Master 的指令（如 CREATE_TABLE, INSERT_DATA 等）
                if (command.equals("QUIT")) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("[RegionServer] Failed to connect to Master: " + e.getMessage());
        }
    }

    public static ZooKeeperManager initRegionServer() {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();
        System.out.println("Initializing RegionServer...");

        // 动态选择可用端口
        port = String.valueOf(getAvailableTcpPort());
        System.out.println("Selected port: " + port);

        // 确保每次初始化都创建新的连接和statement
        try {
            if (connection != null) {
                connection.close();
            }
            connection = JdbcUtils.getConnection(mysqlUser, mysqlPwd);
            if (connection != null) {
                statement = connection.createStatement();
            } else {
                throw new SQLException("Failed to establish database connection.");
            }
        } catch (SQLException e) {
            System.out.println("Failed to initialize database connection: " + e.getMessage());
            e.printStackTrace();
            // 根据需求决定是否抛出异常或继续
        }

        System.out.println("Clearing MySQL data...");
        clearMysqlData();

        System.out.println("Creating ZooKeeper node...");
        createZooKeeperNode(zooKeeperManager);

        System.out.println("Creating socket and thread pool...");
        createSocketAndThreadPool();

        return zooKeeperManager;
    }

    public static void clearMysqlData() {
        if (connection != null && statement != null) {
            try {
                // 删除已有数据库
                String deleteDB = "DROP DATABASE IF EXISTS lss";
                statement.execute(deleteDB);
                // 重新创建数据库
                String createDB = "CREATE DATABASE IF NOT EXISTS lss";
                statement.execute(createDB);

                // 使用新创建的数据库
                String useDB = "USE lss";
                statement.execute(useDB);
            } catch (Exception e) {
                System.out.println("Error clearing MySQL data: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("Connection or statement is not initialized. Cannot clear MySQL data.");
        }
    }

    public static void createZooKeeperNode(ZooKeeperManager zooKeeperManager) {
        try {
            System.out.println("Calling RegionServer.createZooKeeperNode");

            serverPath = "/lss/region_server/" + ip; // 修改路径以包含IP
            serverValue = ip + "," + port + "," + mysqlUser + "," + mysqlPwd + ",3306,0";

            // 检查节点是否存在
            if (zooKeeperManager.nodeExists(serverPath)) {
                System.out.println("ZooKeeper node already exists at path: " + serverPath + ". Continuing to use it.");

                // 获取现有节点的数据并更新tables列表
                String existingValue = zooKeeperManager.getData(serverPath);
                if (existingValue != null && !existingValue.isEmpty()) {
                    // 解析现有节点数据并更新tables
                    // 假设数据格式为 "ip,port,mysqlUser,mysqlPwd,port2master,port2regionserver"
                    // 需要根据实际数据格式调整解析逻辑
                    // 这里假设tables信息存储在其他路径，如 /lss/region_server/{ip}/table/
                    updateTablesFromZooKeeper(zooKeeperManager, ip);
                } else {
                    System.out.println("Warning: Existing ZooKeeper node has no data.");
                }
            } else {
                // 节点不存在，创建新节点
                zooKeeperManager.addRegionServer(ip, port, tables, mysqlPwd, mysqlUser, "2182", "3306");
                System.out.println("ZooKeeper node created successfully at path: " + serverPath);
            }
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("ZooKeeper node already exists at path: " + serverPath + ". Continuing to use it.");

            // 获取现有节点的数据并更新tables列表
            try {
                String existingValue = zooKeeperManager.getData(serverPath);
                if (existingValue != null && !existingValue.isEmpty()) {
                    // 解析现有节点数据并更新tables
                    updateTablesFromZooKeeper(zooKeeperManager, ip);
                } else {
                    System.out.println("Warning: Existing ZooKeeper node has no data.");
                }
            } catch (Exception ex) {
                System.out.println("Error retrieving data from existing ZooKeeper node: " + ex.getMessage());
                ex.printStackTrace();
            }
        } catch (Exception e) {
            System.out.println("Error creating ZooKeeper node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 根据现有的ZooKeeper节点数据更新tables列表
     * 需要根据实际的ZooKeeper数据结构进行调整
     */
    private static void updateTablesFromZooKeeper(ZooKeeperManager zooKeeperManager, String ip) throws Exception {
        // 假设tables存储在 /lss/region_server/{ip}/table/{tableName}/
        String tablesPath = "/lss/region_server/" + ip + "/table";
        List<String> tableNames = zooKeeperManager.getChildren(tablesPath);

        for (String tableName : tableNames) {
            // 这里可以根据需要进一步解析每个表的信息
            // 例如，读取 /lss/region_server/{ip}/table/{tableName}/payload 获取表的详细信息
            tables.add(new TableInform(tableName, 3));
            System.out.println("Added table from ZooKeeper: " + tableName);
        }
    }

    public static void createSocketAndThreadPool(){
        try {
            ServerSocketFactory serverSocketFactory  = ServerSocketFactory.getDefault();
            serverSocket = serverSocketFactory.createServerSocket(Integer.valueOf(port));
            System.out.println("Server socket created on port: " + port);
        } catch (IOException e) {
            System.out.println("Error creating server socket: " + e.getMessage());
            e.printStackTrace();
        }

        threadPoolExecutor = new ThreadPoolExecutor(60,
                100,
                20,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(20),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public static String getIPAddress(){
        String res = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            System.out.println("Local HostAddress: "+addr.getHostAddress());
            res = addr.getHostAddress();
            String hostname = addr.getHostName();
            System.out.println("Local host name: "+hostname);
        } catch(UnknownHostException e) {
            System.out.println("Error getting IP address: " + e.getMessage());
        }
        return res;
    }

    public static int getAvailableTcpPort() {
        for (int i = 1000; i <= 65535; i++) {
            try {
                new ServerSocket(i).close();
                return i;
            } catch (IOException e) {
                continue;
            }
        }
        return 0;
    }
}