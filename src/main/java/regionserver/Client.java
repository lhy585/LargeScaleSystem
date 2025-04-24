package regionserver;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 5001);
        System.out.println("Connected to Master");

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

        writer.write("REGISTER_REGION_SERVER");
        writer.newLine();
        writer.flush();

        String sql;
        while ((sql = userInput.readLine()) != null) {
            writer.write(sql);
            writer.newLine();
            writer.flush();

            String response = reader.readLine();
            System.out.println("Master response: " + response);
        }
    }
}