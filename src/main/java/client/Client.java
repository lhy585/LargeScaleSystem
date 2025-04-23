package client;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Client {
    public static String ip="127.0.0.1";
    public static int port=5000;
    public static Map<String, String> map = new HashMap<>();
    public static void main(String[] args) throws Exception {
        map.put("t1","1.1.1.1");
        new communite(ip,port).start();
    }
    private static class communite extends Thread {
        public static String ip;
        public static int port;
        public communite(String ip, int port){
            this.ip=ip;
            this.port=port;
        }
        @Override
        public void run() {
            try {
                while(true){
                    StringBuilder sqlBuilder = new StringBuilder();
                    String line;
                    InputStream inputStream = System.in;
                    BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
                    System.out.print("mysql> ");
                    while ((line = input.readLine()) != null) {
                        sqlBuilder.append(line).append(" ");
                        if (line.trim().endsWith(";")) {
                            break;
                        }
                        System.out.print("    -> ");
                    }
                    String sql = sqlBuilder.toString().trim();
                    System.out.println("sql: " + sql);
                    if(sql.substring(0,sql.indexOf(' ')).equals("select")&&!sql.contains("join")){
                        String table_name=sql.substring(sql.indexOf("from"));
                        table_name=table_name.substring(table_name.indexOf(' ')+1,table_name.indexOf(';'));
                        System.out.println("table_name: "+table_name);
                        if(map.containsKey(table_name)){
                            //缓存中找到table_name
                            System.out.println(111);
                            //TODO: 与regionserver通信
                        }
                        else{
                            //缓存中未找到table_name
                            Socket socket = new Socket(ip, port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            // 发送消息到服务器
                            out.println(sql);
                            System.out.println("Message sent to server: " + sql);
                            // 接收服务器的响应
                            String ip = in.readLine();
                            String sql_response = in.readLine();
                            map.put(table_name,ip);
                            //TODO: 与regionserver通信
                        }
                    }
                    else{
                        Socket socket = new Socket(ip, port);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        // 发送消息到服务器
                        out.println(sql);
                        System.out.println("Message sent to server: " + sql);

                        // 接收服务器的响应
                        String response = in.readLine();
                        System.out.println("Response from server: " + response);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error occurred while connecting to the server: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
