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

import zookeeper.ZooKeeperManager;
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
    public static ArrayList<TableInfo> tables;

    public static String serverPath;
    public static String serverValue;

    static {
        ip = getIPAddress();
        mysqlUser = "root";
        mysqlPwd = "040517cc";
        port = "1001";
        tables = new ArrayList<>();
    }

    @Override
    public void run() {
        ZooKeeperManager zooKeeperManager = initRegionServer();

        // Start the command listener thread
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Scanner sc = new Scanner(System.in);
                while(true){
                    String cmd = sc.nextLine();
                    if (!cmd.equals("quit")) {
                        System.out.println(cmd);
                    } else {
                        quitSignal = true;
                        break;
                    }
                }
                sc.close();
            }
        });

        // Main server loop
        while(true) {
            try {
                Socket socket = serverSocket.accept();
                threadPoolExecutor.submit(new ServerThread(socket, statement, tables));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (quitSignal){
                break;
            }
        }
    }

    public static ZooKeeperManager initRegionServer() {
        ZooKeeperManager zooKeeperManager = new ZooKeeperManager();
        System.out.println("init region server");
        connection = JdbcUtils.getConnection("root", "040517cc");
        try {
            assert connection != null;
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.println(e);
        }
        System.out.println("clear mysql data");
        clearMysqlData();
        System.out.println("create zookeeper node");
        createZooKeeperNode(zooKeeperManager);
        System.out.println("create socket and thread pool");
        createSocketAndThreadPool();
        return zooKeeperManager;
    }

    public static void clearMysqlData() {
        if (connection != null && statement != null) {
            try {
                // 删除已有数据库
                String deleteDB = "drop database if exists lss";
                statement.execute(deleteDB);
                // 重新创建数据库
                String createDB = "create database if not exists lss";
                statement.execute(createDB);

                // 使用新创建的数据库
                String useDB = "use lss";
                statement.execute(useDB);
            } catch(Exception e) {
                System.out.println(e);
            }
        }
    }

    public static void createZooKeeperNode(ZooKeeperManager zooKeeperManager) {
        try {
            System.out.println("call RegionServer.createZooKeeperNode");

            serverPath = "/lss/region_server";
            serverValue = ip + "," + port + "," + mysqlUser + "," + mysqlPwd + "," + "3306" + ",0";

            zooKeeperManager.addRegionServer(ip, port, tables, mysqlUser, mysqlPwd, "2182", "3306");

            List<String> serverNodes = zooKeeperManager.getRegionServer(serverPath);

        } catch(Exception e) {
            System.out.println(e);
        }
    }

    public static void createSocketAndThreadPool(){
        ServerSocketFactory serverSocketFactory  = ServerSocketFactory.getDefault();
        try {
            serverSocket = serverSocketFactory.createServerSocket(Integer.valueOf(port));
        } catch (IOException e) {
            System.out.println(e);
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
            System.out.println(e);
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