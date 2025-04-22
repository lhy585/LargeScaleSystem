package client;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    public static String ip="10.162.126.54";
    public static int port=5000;
    public static void main(String[] args) throws Exception {
        String sql;
        InputStream inputStream = System.in;
        BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
        sql=input.readLine();
        System.out.println(sql);
        try (Socket socket = new Socket(ip, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送消息到服务器
            out.println(sql);
            System.out.println("Message sent to server: " + sql);

            // 接收服务器的响应
            String response = in.readLine();
            System.out.println("Response from server: " + response);

        } catch (IOException e) {
            System.err.println("Error occurred while connecting to the server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
