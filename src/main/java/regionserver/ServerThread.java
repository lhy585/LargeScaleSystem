package regionserver;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;

import org.apache.curator.framework.CuratorFramework;

public class ServerThread implements Runnable {
    public Socket socket;
    public String serverIdentity = null;
    public BufferedReader reader = null;
    public ArrayList<TableInfo> tables;
    public Statement statement;
    public BufferedWriter writer = null;

    ServerThread(Socket socket, Statement statement, ArrayList<TableInfo> tables) {
        this.socket = socket;
        this.tables = tables;
        this.statement = statement;
    }

    @Override
    public void run() {
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            String line;

            // 处理初始连接身份识别
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("CONNECT ")) {
                    serverIdentity = line.substring(8);
                    System.out.println(serverIdentity + "已连接");
                    break;
                }
                if (line.equals("exit")) {
                    System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                    break;
                }
            }

            // 根据身份调用不同服务方法
            if (serverIdentity != null) {
                switch (serverIdentity) {
                    case "CLIENT":
                        serveClient();
                        break;
                    case "MASTER":
                        serveMaster();
                        break;
                    case "REGION SERVER":
                        serveRegionServer();
                        break;
                    default:
                        System.out.println("未知连接类型: " + serverIdentity);
                }
            }
        } catch (IOException e) {
            System.out.println("连接处理错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                System.out.println("关闭socket时出错: " + e.getMessage());
            }
        }
    }

    /**
     * 服务客户端请求
     */
    public void serveClient() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("收到客户端请求: " + line);

            if (line.equals("exit")) {
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            }

            try {
                String[] parts = line.split(" ", 3); // 分割命令、表名和剩余部分
                if (parts.length < 2) {
                    reactCmd(false, "无效命令格式");
                    continue;
                }

                String command = parts[0].toLowerCase();
                String tableName = parts[1];
                String rest = parts.length > 2 ? parts[2] : "";

                synchronized (this) {
                    switch (command) {
                        case "create":
                            System.out.println("处理创建表请求");
                            boolean createRes = ServerClient.createTable(tableName, line);
                            reactCmd(createRes, createRes ? "表创建成功" : "表创建失败");
                            break;
                        case "insert":
                        case "delete":
                            System.out.println("处理数据操作请求");
                            boolean executeRes = ServerClient.executeCmd(line);
                            reactCmd(executeRes, executeRes ? "操作执行成功" : "操作执行失败");
                            break;
                        case "select":
                            System.out.println("处理查询请求");
                            String queryResult = ServerClient.selectTable(line);
                            writer.write("SUCCESS:" + queryResult + "\n");
                            writer.flush();
                            break;
                        case "drop":
                            System.out.println("处理删除表请求");
                            boolean dropRes = ServerClient.dropTable(tableName);
                            reactCmd(dropRes, dropRes ? "表删除成功" : "表删除失败");
                            break;
                        default:
                            System.out.println("未知客户端指令: " + line);
                            reactCmd(false, "未知指令");
                    }
                }
            } catch (Exception e) {
                System.out.println("处理客户端请求时出错: " + e.getMessage());
                reactCmd(false, "处理请求时出错: " + e.getMessage());
            }
        }
    }

    /**
     * 服务Master节点请求
     */
    public void serveMaster() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("收到Master请求: " + line);

            if (line.equals("exit")) {
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            }

            try {
                String[] parts = line.split(" ", 3); // 分割命令和参数
                if (parts.length < 2) {
                    reactCmd(false, "无效命令格式");
                    continue;
                }

                String command = parts[0].toLowerCase();
                String params = parts[1];

                synchronized (this) {
                    switch (command) {
                        case "migrate":
                            System.out.println("处理表迁移请求");
                            // 参数格式: sourceServerName@ip:port:user:password:tableName
                            String[] migrateParams = params.split("@");
                            if (migrateParams.length == 2) {
                                String[] serverInfo = migrateParams[1].split(":");
                                if (serverInfo.length >= 4) {
                                    reactCmd(true, "表迁移请求已接收");
                                }
                            }
                            break;
                        case "dump":
                            System.out.println("处理创建副本请求");
                            // 参数格式: sourceServerName@ip:port:user:password:tableName
                            String[] dumpParams = params.split("@");
                            if (dumpParams.length == 2) {
                                String[] serverInfo = dumpParams[1].split(":");
                                if (serverInfo.length >= 4) {
                                    ServerMaster.dumpTable(
                                            dumpParams[0],  // sourceServerName
                                            serverInfo[0],  // ip
                                            serverInfo[1],  // port
                                            serverInfo[2],  // user
                                            serverInfo[3],  // password
                                            parts.length > 2 ? parts[2] : "" // tableName
                                    );
                                    reactCmd(true, "副本创建请求已接收");
                                }
                            }
                            break;
                        default:
                            System.out.println("未知Master指令: " + line);
                            reactCmd(false, "未知指令");
                    }
                }
            } catch (Exception e) {
                System.out.println("处理Master请求时出错: " + e.getMessage());
                reactCmd(false, "处理请求时出错: " + e.getMessage());
            }
        }
    }

    /**
     * 服务RegionServer节点请求
     */
    public void serveRegionServer() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("收到RegionServer请求: " + line);

            if (line.equals("exit")) {
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            }

            try {
                String[] parts = line.split(" ", 3); // 分割命令、表名和剩余部分
                if (parts.length < 2) {
                    reactCmd(false, "无效命令格式");
                    continue;
                }

                String command = parts[0].toLowerCase();
                String tableName = parts[1];
                String rest = parts.length > 2 ? parts[2] : "";

                synchronized (this) {
                    switch (command) {
                        case "create":
                            System.out.println("处理同步创建表请求");
                            // 实现同步创建表逻辑
                            reactCmd(true, "同步请求已接收");
                            break;
                        case "insert":
                        case "delete":
                            System.out.println("处理同步数据操作请求");
                            // 实现同步数据操作逻辑
                            reactCmd(true, "同步请求已接收");
                            break;
                        case "dump":
                            System.out.println("处理同步创建副本请求");
                            // 实现同步创建副本逻辑
                            reactCmd(true, "同步请求已接收");
                            break;
                        case "drop":
                            System.out.println("处理同步删除表请求");
                            // 实现同步删除表逻辑
                            reactCmd(true, "同步请求已接收");
                            break;
                        default:
                            System.out.println("未知RegionServer指令: " + line);
                            reactCmd(false, "未知指令");
                    }
                }
            } catch (Exception e) {
                System.out.println("处理RegionServer请求时出错: " + e.getMessage());
                reactCmd(false, "处理请求时出错: " + e.getMessage());
            }
        }
    }

    /**
     * 响应命令执行结果
     * @param success 是否成功
     * @param message 响应消息
     */
    private void reactCmd(boolean success, String message) throws IOException {
        writer.write((success ? "SUCCESS:" : "ERROR:") + message + "\n");
        writer.flush();
    }
}