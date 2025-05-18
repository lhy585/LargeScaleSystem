package client;

import master.SelectInfo;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static master.RegionManager.MASTER_IP;

public class Client {
    public static String masterIp = MASTER_IP;//TODO:ip
    public static int masterPort = 5000;
    public static Map<String, String> map = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // map.put("t1","1.1.1.1");
        new communite().start();
    }

    private static class communite extends Thread {
        public communite() {
        }

        @Override
        public void run() {
            try (BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                while (true) {
                    System.out.print("mysql> ");
                    StringBuilder sqlBuilder = new StringBuilder();
                    String line;
                    while ((line = consoleInput.readLine()) != null) {
                        if ("exit;".equalsIgnoreCase(line.trim())) {
                            System.out.println("Exiting client.");
                            return;
                        }
                        sqlBuilder.append(line).append(" ");
                        if (line.trim().endsWith(";")) {
                            break;
                        }
                        System.out.print("    -> ");
                    }
                    if (line == null && sqlBuilder.length() == 0) {
                        System.out.println("\nInput stream closed. Exiting.");
                        return;
                    }
                    String sql = sqlBuilder.toString().trim();
                    Statement statement = CCJSqlParserUtil.parse(sql);
                    if (statement instanceof Drop) {
                        List<String> tableNames;
                        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                        tableNames = tablesNamesFinder.getTableList(statement);
                        for(String i:tableNames){
                            map.remove(i);
                            map.remove(i+"_slave");
                        }
                    }
                    if (sql.isEmpty() || !sql.endsWith(";")) {
                        System.out.println("Invalid command or missing ';'");
                        continue;
                    }
                    System.out.println("debug sql: " + sql);
                    String firstWord = "";
                    int firstSpaceIndex = sql.indexOf(' ');
                    if (firstSpaceIndex > 0) {
                        firstWord = sql.substring(0, firstSpaceIndex).toLowerCase();
                    } else if (sql.endsWith(";")) {
                        firstWord = sql.substring(0, sql.length() - 1).toLowerCase();
                    }

                    // select语句 直接和region通信
                    if (firstWord.equals("select") && !sql.toLowerCase().contains(" join ")) {
                        // 解析表名
                        String table_name = extractTableNameFromSelect(sql);
                        if (table_name == null) {
                            System.out.println("Could not extract table name from SELECT query.");
                            continue;
                        }
                        System.out.println("debug table_name: " + table_name);
                        if (map.containsKey(table_name)) {
                            //缓存中找到table_name
                            String regionServerAddress = map.get(table_name);
                            System.out.println("debug cache hit for " + table_name + ", using address: " + regionServerAddress);

                            //TODO: 与regionserver通信
//                            String[] parts = regionServerAddress.split(":");
//                            if (parts.length != 2) {
//                                System.err.println("Invalid RegionServer address format in cache: " + regionServerAddress);
//                                map.remove(table_name);
//                                continue;
//                            }
                            String rsIp = regionServerAddress;
                            int rsPort = 4001;
//                            try {
//                                rsPort = Integer.parseInt(parts[1]);
//                            } catch (NumberFormatException e) {
//                                System.err.println("Invalid RegionServer port in cache: " + parts[1]);
//                                map.remove(table_name);
//                                continue;
//                            }

                            System.out.println("Connecting to RegionServer " + rsIp + ":" + rsPort + " for SELECT...");
                            try (Socket rsSocket = new Socket(rsIp, 4001);
                                 PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                 BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                rsOut.println(sql);
                                System.out.println("debug Message sent to RegionServer: " + sql);

                                System.out.println("--- RegionServer Response ---");
                                String responseLine;
                                boolean dataReceived = false;
                                ArrayList<String[]> dataList = new ArrayList<>();
                                // 读取数据行
                                while ((responseLine = rsIn.readLine()) != null) {
                                    if ("END_OF_DATA".equals(responseLine)) {
                                        break;
                                    }
                                    dataList.add(responseLine.split("\\s+"));
                                    dataReceived = true;
                                }
                                int[] count = new int[dataList.get(0).length];
                                for (int i = 0; i < dataList.size(); i++) {
                                    String[] temp = dataList.get(i);
                                    for (int y = 0; y < temp.length; y++) {
                                        if (count[y] < temp[y].length()) {
                                            count[y] = temp[y].length();
                                        }
                                    }
                                }
                                for (int i = 0; i < dataList.size(); i++) {
                                    //
                                    System.out.print("+");
                                    for (int j = 0; j < count.length; j++) {
                                        System.out.print("-");
                                        for (int t = 0; t < count[j]; t++) {
                                            System.out.print("-");
                                        }
                                        System.out.print("-");
                                        System.out.print("+");
                                    }
                                    System.out.println();
                                    //
                                    System.out.print("|");
                                    for (int j = 0; j < count.length; j++) {
                                        System.out.print(" ");
                                        System.out.print(dataList.get(i)[j]);
                                        for (int t = 0; t < count[j] - dataList.get(i)[j].length(); t++) {
                                            System.out.print(" ");
                                        }
                                        System.out.print(" ");
                                        System.out.print("|");
                                    }
                                    System.out.println();
                                }
                                System.out.print("+");
                                for (int j = 0; j < count.length; j++) {
                                    System.out.print("-");
                                    for (int t = 0; t < count[j]; t++) {
                                        System.out.print("-");
                                    }
                                    System.out.print("-");
                                    System.out.print("+");
                                }
                                System.out.println();
                                if (!dataReceived) {
                                    System.out.println("(No data rows received or connection closed prematurely)");
                                }
                                System.out.println("--- End of Response ---");
                            } catch (UnknownHostException e) {
                                System.err.println("Error: Unknown RegionServer host: " + rsIp);
                                map.remove(table_name);
                            } catch (IOException e) {
                                System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e.getMessage());
                                map.remove(table_name);
                                System.out.println("debug cache miss for " + table_name + ". Querying Master...");
                                String regionServerAddress2 = null;
                                String finalSqlForRs = null;

                                try (Socket masterSocket = new Socket(masterIp, masterPort); // 使用外部类的静态 masterIp, masterPort
                                     PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                     BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                    masterOut.println(sql); // 发送消息到服务器
                                    System.out.println("debug Message sent to Master: " + sql);

                                    String responseFromMaster = masterIn.readLine(); // 接收服务器的响应
                                    System.out.println("debug Master response line: " + responseFromMaster);

                                    if (responseFromMaster != null) {
                                        SelectInfo info = new SelectInfo(responseFromMaster);
                                        if (info.isValid) {
                                            regionServerAddress = info.ip;
                                            finalSqlForRs = info.sql;
//                                        map.put(table_name, regionServerAddress);
                                            System.out.println("debug Cached location for " + table_name + ": " + regionServerAddress);
                                        } else {
                                            System.out.println("Master reported table '" + table_name + "' not found or error processing SELECT.");
                                        }
                                    } else {
                                        System.err.println("Error: No response from Master.");
                                    }
                                } catch (IOException e2) {
                                    System.err.println("Error communicating with Master for SELECT lookup: " + e.getMessage());
                                } catch (Exception e2) {
                                    System.err.println("Error processing Master's SELECT response: " + e.getMessage());
                                }

                                if (regionServerAddress != null && finalSqlForRs != null) {
                                    System.out.println("Master found table. RegionServer at: " + regionServerAddress);
                                    System.out.println("Executing SQL on RegionServer: " + finalSqlForRs);
                                    String table=extractTableNameFromSelect(finalSqlForRs);
                                    System.out.println(finalSqlForRs);
                                    System.out.println(table);
                                    map.put(table,regionServerAddress);
//                                String[] parts = regionServerAddress.split(":");
//                                if (parts.length != 2) {
//                                    System.err.println("Invalid RegionServer address format from Master: " + regionServerAddress);
//                                    map.remove(table_name);
//                                    continue;
//                                }
                                    String rsIp2 = regionServerAddress;
                                    int rsPort2=4001;
//                                try {
//                                    rsPort = Integer.parseInt(parts[1]);
//                                } catch (NumberFormatException e) {
//                                    System.err.println("Invalid RegionServer port from Master: " + parts[1]);
//                                    map.remove(table_name);
//                                    continue;
//                                }

                                    System.out.println("Connecting to RegionServer " + rsIp2 + ":" + rsPort2 + " for SELECT...");
                                    try (Socket rsSocket = new Socket(rsIp2, rsPort2);
                                         PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                         BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                        rsOut.println(finalSqlForRs);
                                        System.out.println("debug Message sent to RegionServer: " + finalSqlForRs);

                                        System.out.println("--- RegionServer Response ---");
                                        String responseLine;
                                        ArrayList<String[]> dataList = new ArrayList<>();
                                        boolean dataReceived = false;
                                        // 读取数据行
                                        while ((responseLine = rsIn.readLine()) != null) {
                                            if ("END_OF_DATA".equals(responseLine)) {
                                                break;
                                            }
                                            dataList.add(responseLine.split("\\s+"));
                                            dataReceived = true;
                                        }
                                        int[] count = new int[dataList.get(0).length];
                                        for (int i = 0; i < dataList.size(); i++) {
                                            String[] temp = dataList.get(i);
                                            for (int y=0;y< temp.length;y++) {
                                                if (count[y] < temp[y].length()) {
                                                    count[y] = temp[y].length();
                                                }
                                            }
                                        }
                                        for(int i=0;i<dataList.size();i++){
                                            //
                                            System.out.print("+");
                                            for(int j=0;j<count.length;j++){
                                                System.out.print("-");
                                                for(int t=0;t<count[j];t++){
                                                    System.out.print("-");
                                                }
                                                System.out.print("-");
                                                System.out.print("+");
                                            }
                                            System.out.println();
                                            //
                                            System.out.print("|");
                                            for(int j=0;j<count.length;j++){
                                                System.out.print(" ");
                                                System.out.print(dataList.get(i)[j]);
                                                for(int t=0;t<count[j]-dataList.get(i)[j].length();t++){
                                                    System.out.print(" ");
                                                }
                                                System.out.print(" ");
                                                System.out.print("|");
                                            }
                                            System.out.println();
                                        }
                                        System.out.print("+");
                                        for(int j=0;j<count.length;j++){
                                            System.out.print("-");
                                            for(int t=0;t<count[j];t++){
                                                System.out.print("-");
                                            }
                                            System.out.print("-");
                                            System.out.print("+");
                                        }
                                        System.out.println();
                                        if (!dataReceived)
                                            System.out.println("(No data rows received or connection closed prematurely)");
                                        System.out.println("--- End of Response ---");
                                    } catch (UnknownHostException e2) {
                                        System.err.println("Error: Unknown RegionServer host: " + rsIp);
                                        map.remove(table_name);
                                    } catch (IOException e2) {
                                        System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e.getMessage());
                                        map.remove(table_name);
                                    }
                                }
                            }
                        }else if(map.containsKey(table_name+"_slave")) {
                            //缓存中找到table_name+"_slave"
                            sql = sql.replace(table_name, table_name + "_slave");
                            String regionServerAddress = map.get(table_name+"_slave");
                            System.out.println("debug cache hit for " + table_name + ", using address: " + regionServerAddress);

                            //TODO: 与regionserver通信
//                            String[] parts = regionServerAddress.split(":");
//                            if (parts.length != 2) {
//                                System.err.println("Invalid RegionServer address format in cache: " + regionServerAddress);
//                                map.remove(table_name);
//                                continue;
//                            }
                            String rsIp = regionServerAddress;
                            int rsPort = 4001;
//                            try {
//                                rsPort = Integer.parseInt(parts[1]);
//                            } catch (NumberFormatException e) {
//                                System.err.println("Invalid RegionServer port in cache: " + parts[1]);
//                                map.remove(table_name);
//                                continue;
//                            }

                            System.out.println("Connecting to RegionServer " + rsIp + ":" + rsPort + " for SELECT...");
                            try (Socket rsSocket = new Socket(rsIp, 4001);
                                 PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                 BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8))) {

                                rsOut.println(sql);
                                System.out.println("debug Message sent to RegionServer: " + sql);

                                System.out.println("--- RegionServer Response ---");
                                String responseLine;
                                boolean dataReceived = false;
                                ArrayList<String[]> dataList = new ArrayList<>();
                                // 读取数据行
                                while ((responseLine = rsIn.readLine()) != null) {
                                    if ("END_OF_DATA".equals(responseLine)) {
                                        break;
                                    }
                                    dataList.add(responseLine.split("\\s+"));
                                    dataReceived = true;
                                }
                                int[] count = new int[dataList.get(0).length];
                                for (int i = 0; i < dataList.size(); i++) {
                                    String[] temp = dataList.get(i);
                                    for (int y = 0; y < temp.length; y++) {
                                        if (count[y] < temp[y].length()) {
                                            count[y] = temp[y].length();
                                        }
                                    }
                                }
                                for (int i = 0; i < dataList.size(); i++) {
                                    //
                                    System.out.print("+");
                                    for (int j = 0; j < count.length; j++) {
                                        System.out.print("-");
                                        for (int t = 0; t < count[j]; t++) {
                                            System.out.print("-");
                                        }
                                        System.out.print("-");
                                        System.out.print("+");
                                    }
                                    System.out.println();
                                    //
                                    System.out.print("|");
                                    for (int j = 0; j < count.length; j++) {
                                        System.out.print(" ");
                                        System.out.print(dataList.get(i)[j]);
                                        for (int t = 0; t < count[j] - dataList.get(i)[j].length(); t++) {
                                            System.out.print(" ");
                                        }
                                        System.out.print(" ");
                                        System.out.print("|");
                                    }
                                    System.out.println();
                                }
                                System.out.print("+");
                                for (int j = 0; j < count.length; j++) {
                                    System.out.print("-");
                                    for (int t = 0; t < count[j]; t++) {
                                        System.out.print("-");
                                    }
                                    System.out.print("-");
                                    System.out.print("+");
                                }
                                System.out.println();
                                if (!dataReceived) {
                                    System.out.println("(No data rows received or connection closed prematurely)");
                                }
                                System.out.println("--- End of Response ---");
                            } catch (UnknownHostException e) {
                                System.err.println("Error: Unknown RegionServer host: " + rsIp);
                                map.remove(table_name+"_slave");
                            } catch (IOException e) {
                                System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e.getMessage());
                                map.remove(table_name+"_slave");
                                System.out.println("debug cache miss for " + table_name + "_slave. Querying Master...");
                                String regionServerAddress1 = null;
                                String finalSqlForRs = null;

                                try (Socket masterSocket = new Socket(masterIp, masterPort); // 使用外部类的静态 masterIp, masterPort
                                     PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                     BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                    sql=sql.replace(table_name,table_name+"_slave");
                                    masterOut.println(sql); // 发送消息到服务器
                                    System.out.println("debug Message sent to Master: " + sql);

                                    String responseFromMaster = masterIn.readLine(); // 接收服务器的响应
                                    System.out.println("debug Master response line: " + responseFromMaster);

                                    if (responseFromMaster != null) {
                                        SelectInfo info = new SelectInfo(responseFromMaster);
                                        if (info.isValid) {
                                            regionServerAddress = info.ip;
                                            finalSqlForRs = info.sql;
//                                        map.put(table_name, regionServerAddress);
                                            System.out.println("debug Cached location for " + table_name + ": " + regionServerAddress);
                                        } else {
                                            System.out.println("Master reported table '" + table_name + "' not found or error processing SELECT.");
                                        }
                                    } else {
                                        System.err.println("Error: No response from Master.");
                                    }
                                } catch (IOException e1) {
                                    System.err.println("Error communicating with Master for SELECT lookup: " + e.getMessage());
                                } catch (Exception e1) {
                                    System.err.println("Error processing Master's SELECT response: " + e.getMessage());
                                }

                                if (regionServerAddress != null && finalSqlForRs != null) {
                                    System.out.println("Master found table. RegionServer at: " + regionServerAddress);
                                    System.out.println("Executing SQL on RegionServer: " + finalSqlForRs);
                                    String table=extractTableNameFromSelect(finalSqlForRs);
                                    System.out.println(finalSqlForRs);
                                    System.out.println(table);
                                    map.put(table,regionServerAddress);
//                                String[] parts = regionServerAddress.split(":");
//                                if (parts.length != 2) {
//                                    System.err.println("Invalid RegionServer address format from Master: " + regionServerAddress);
//                                    map.remove(table_name);
//                                    continue;
//                                }
                                    String rsIp1 = regionServerAddress;
                                    int rsPort1=4001;
//                                try {
//                                    rsPort = Integer.parseInt(parts[1]);
//                                } catch (NumberFormatException e) {
//                                    System.err.println("Invalid RegionServer port from Master: " + parts[1]);
//                                    map.remove(table_name);
//                                    continue;
//                                }

                                    System.out.println("Connecting to RegionServer " + rsIp1 + ":" + rsPort1 + " for SELECT...");
                                    try (Socket rsSocket = new Socket(rsIp1, rsPort1);
                                         PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                         BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                        rsOut.println(finalSqlForRs);
                                        System.out.println("debug Message sent to RegionServer: " + finalSqlForRs);

                                        System.out.println("--- RegionServer Response ---");
                                        String responseLine;
                                        ArrayList<String[]> dataList = new ArrayList<>();
                                        boolean dataReceived = false;
                                        // 读取数据行
                                        while ((responseLine = rsIn.readLine()) != null) {
                                            if ("END_OF_DATA".equals(responseLine)) {
                                                break;
                                            }
                                            dataList.add(responseLine.split("\\s+"));
                                            dataReceived = true;
                                        }
                                        int[] count = new int[dataList.get(0).length];
                                        for (int i = 0; i < dataList.size(); i++) {
                                            String[] temp = dataList.get(i);
                                            for (int y=0;y< temp.length;y++) {
                                                if (count[y] < temp[y].length()) {
                                                    count[y] = temp[y].length();
                                                }
                                            }
                                        }
                                        for(int i=0;i<dataList.size();i++){
                                            //
                                            System.out.print("+");
                                            for(int j=0;j<count.length;j++){
                                                System.out.print("-");
                                                for(int t=0;t<count[j];t++){
                                                    System.out.print("-");
                                                }
                                                System.out.print("-");
                                                System.out.print("+");
                                            }
                                            System.out.println();
                                            //
                                            System.out.print("|");
                                            for(int j=0;j<count.length;j++){
                                                System.out.print(" ");
                                                System.out.print(dataList.get(i)[j]);
                                                for(int t=0;t<count[j]-dataList.get(i)[j].length();t++){
                                                    System.out.print(" ");
                                                }
                                                System.out.print(" ");
                                                System.out.print("|");
                                            }
                                            System.out.println();
                                        }
                                        System.out.print("+");
                                        for(int j=0;j<count.length;j++){
                                            System.out.print("-");
                                            for(int t=0;t<count[j];t++){
                                                System.out.print("-");
                                            }
                                            System.out.print("-");
                                            System.out.print("+");
                                        }
                                        System.out.println();
                                        if (!dataReceived)
                                            System.out.println("(No data rows received or connection closed prematurely)");
                                        System.out.println("--- End of Response ---");
                                    } catch (UnknownHostException e1) {
                                        System.err.println("Error: Unknown RegionServer host: " + rsIp);
                                        map.remove(table_name);
                                    } catch (IOException e1) {
                                        System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e1.getMessage());
                                        map.remove(table_name);
                                    }
                                }
                            }
                        }
                        else {
                            //TODO: 与regionserver通信
                            System.out.println("debug cache miss for " + table_name + ". Querying Master...");
                            String regionServerAddress = null;
                            String finalSqlForRs = null;

                            try (Socket masterSocket = new Socket(masterIp, masterPort); // 使用外部类的静态 masterIp, masterPort
                                 PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                 BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                masterOut.println(sql); // 发送消息到服务器
                                System.out.println("debug Message sent to Master: " + sql);

                                String responseFromMaster = masterIn.readLine(); // 接收服务器的响应
                                System.out.println("debug Master response line: " + responseFromMaster);

                                if (responseFromMaster != null) {
                                    SelectInfo info = new SelectInfo(responseFromMaster);
                                    if (info.isValid) {
                                        regionServerAddress = info.ip;
                                        finalSqlForRs = info.sql;
//                                        map.put(table_name, regionServerAddress);
                                        System.out.println("debug Cached location for " + table_name + ": " + regionServerAddress);
                                    } else {
                                        System.out.println("Master reported table '" + table_name + "' not found or error processing SELECT.");
                                    }
                                } else {
                                    System.err.println("Error: No response from Master.");
                                }
                            } catch (IOException e) {
                                System.err.println("Error communicating with Master for SELECT lookup: " + e.getMessage());
                            } catch (Exception e) {
                                System.err.println("Error processing Master's SELECT response: " + e.getMessage());
                            }

                            if (regionServerAddress != null && finalSqlForRs != null) {
                                System.out.println("Master found table. RegionServer at: " + regionServerAddress);
                                System.out.println("Executing SQL on RegionServer: " + finalSqlForRs);
                                String table=extractTableNameFromSelect(finalSqlForRs);
                                System.out.println(finalSqlForRs);
                                System.out.println(table);
                                map.put(table,regionServerAddress);
//                                String[] parts = regionServerAddress.split(":");
//                                if (parts.length != 2) {
//                                    System.err.println("Invalid RegionServer address format from Master: " + regionServerAddress);
//                                    map.remove(table_name);
//                                    continue;
//                                }
                                String rsIp = regionServerAddress;
                                int rsPort=4001;
//                                try {
//                                    rsPort = Integer.parseInt(parts[1]);
//                                } catch (NumberFormatException e) {
//                                    System.err.println("Invalid RegionServer port from Master: " + parts[1]);
//                                    map.remove(table_name);
//                                    continue;
//                                }

                                System.out.println("Connecting to RegionServer " + rsIp + ":" + rsPort + " for SELECT...");
                                try (Socket rsSocket = new Socket(rsIp, rsPort);
                                     PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                                     BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8))) {
                                    rsOut.println(finalSqlForRs);
                                    System.out.println("debug Message sent to RegionServer: " + finalSqlForRs);

                                    System.out.println("--- RegionServer Response ---");
                                    String responseLine;
                                    ArrayList<String[]> dataList = new ArrayList<>();
                                    boolean dataReceived = false;
                                    // 读取数据行
                                    while ((responseLine = rsIn.readLine()) != null) {
                                        if ("END_OF_DATA".equals(responseLine)) {
                                            break;
                                        }
                                        dataList.add(responseLine.split("\\s+"));
                                        dataReceived = true;
                                    }
                                    int[] count = new int[dataList.get(0).length];
                                    for (int i = 0; i < dataList.size(); i++) {
                                        String[] temp = dataList.get(i);
                                        for (int y=0;y< temp.length;y++) {
                                            if (count[y] < temp[y].length()) {
                                                count[y] = temp[y].length();
                                            }
                                        }
                                    }
                                    for(int i=0;i<dataList.size();i++){
                                        //
                                        System.out.print("+");
                                        for(int j=0;j<count.length;j++){
                                            System.out.print("-");
                                            for(int t=0;t<count[j];t++){
                                                System.out.print("-");
                                            }
                                            System.out.print("-");
                                            System.out.print("+");
                                        }
                                        System.out.println();
                                        //
                                        System.out.print("|");
                                        for(int j=0;j<count.length;j++){
                                            System.out.print(" ");
                                            System.out.print(dataList.get(i)[j]);
                                            for(int t=0;t<count[j]-dataList.get(i)[j].length();t++){
                                                System.out.print(" ");
                                            }
                                            System.out.print(" ");
                                            System.out.print("|");
                                        }
                                        System.out.println();
                                    }
                                    System.out.print("+");
                                    for(int j=0;j<count.length;j++){
                                        System.out.print("-");
                                        for(int t=0;t<count[j];t++){
                                            System.out.print("-");
                                        }
                                        System.out.print("-");
                                        System.out.print("+");
                                    }
                                    System.out.println();
                                    if (!dataReceived)
                                        System.out.println("(No data rows received or connection closed prematurely)");
                                    System.out.println("--- End of Response ---");
                                } catch (UnknownHostException e) {
                                    System.err.println("Error: Unknown RegionServer host: " + rsIp);
                                    map.remove(table_name);
                                } catch (IOException e) {
                                    System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e.getMessage());
                                    map.remove(table_name);
                                }
                            }
                        }
                    } else {
                        System.out.println("Sending command to Master...");
                        try (Socket masterSocket = new Socket(masterIp, masterPort); // 使用外部类的静态 masterIp, masterPort
                             PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                             BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8))) {
                            masterOut.println(sql); // 发送消息到服务器
                            System.out.println("debug Message sent to Master: " + sql);

                            System.out.println("--- Master Response ---");
                            // 接收服务器的响应
                            String responseLine;
                            while ((responseLine = masterIn.readLine()) != null) {
                                System.out.println(responseLine);
                            }
                            System.out.println("--- End of Response ---");
                        } catch (IOException e) {
                            System.err.println("Error communicating with Master: " + e.getMessage());
                        }
                    }
                    System.out.println("--------------------");
                }
            } catch (IOException e) {
                System.err.println("Error occurred while reading from console: " + e.getMessage());
                e.printStackTrace();
            } catch (JSQLParserException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("Communication thread finished.");
            }
        }

        private String extractTableNameFromSelect(String sql) {
            try {
                String sqlLower = sql.toLowerCase();
                int fromIndex = sqlLower.indexOf(" from ");
                if (fromIndex == -1) return null;

                String afterFrom = sql.substring(fromIndex + 6).trim();
                String[] parts = afterFrom.split("\\s+");
                if (parts.length > 0) {
                    String tableName = parts[0];
                    tableName = tableName.split("[\\s;(,]", 2)[0];
                    return tableName;
                }
            } catch (Exception e) {
                System.err.println("Error parsing table name from SELECT: " + e.getMessage());
            }
            return null;
        }
    }
    // create table t1(id int, name char(20))
}