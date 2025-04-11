package master;

import socket.MySocket;
import socket.ParsedSqlResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    //Master主程序：打开服务器，监听客户端并分配线程连接
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(5000);//接受客户端连接
        System.out.println("[Info]Master is initializing...");
        RegionManager.init();//初始化
        System.out.println("[Info]Initializing successfully!");
        while (true) {//持续监听
            Socket clientSocket = server.accept();
            ClientHandler clientHandler = new ClientHandler(clientSocket);
            Thread newClient = new Thread(clientHandler);
            newClient.start();
        }
    }

    private static class ClientHandler extends Thread {
        private final MySocket mySocket;

        ClientHandler(Socket socket) {
            this.mySocket = new MySocket(socket);
        }

        @Override
        public void run() {//线程主逻辑
            Socket socket = mySocket.getSocket();
            BufferedReader input = mySocket.getInput();
            System.out.println("New client connected, address: " + socket.getInetAddress() + ", port: " + socket.getPort());
            try {
                String sql;
                while(isSocketOn(socket)) {
                    while((sql = input.readLine())==null);//持续接受并读取客户端输入
                    mySocket.parseSql(sql);//处理字符串
                    ParsedSqlResult parsedSqlResult=mySocket.getParsedSqlResult();
                    String tableName = parsedSqlResult.getTableName();
                    if(tableName.equals("create")) {//create需要寻找满足负载均衡的node
                        /*TODO:调用Zookeeper的函数获取节点负载情况
                        System.out.println("Query result: " + RegionManager.loadBalance(tableInfo));
                        printWriter.println(RegionManager.loadBalance(tableInfo));
                         TODO:调用负载均衡函数，给Zookeeper返回该表合适的节点
                         */
                    }else{
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
                mySocket.destroy();//连接断开，释放资源
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    mySocket.destroy();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        /**
         * 定期向客户端发送消息，通过发送的成功与否判断连接是否保持
         * return true:连接保持
         *        false:连接关闭
         */
        public static boolean isSocketOn(Socket socket) {
            try {
                socket.sendUrgentData(0xFF); // 发送1字节紧急数据，对方关闭时会抛异常
                return true;
            } catch (Exception e) {
                return false; // 发送失败，说明连接断了
            }
        }
    }
}