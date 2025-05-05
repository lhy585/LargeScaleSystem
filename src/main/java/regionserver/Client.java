package regionserver;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import master.RegionManager;
import master.ResType;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

/**
 * RegionServer 的 Client，负责与 Master 通信，自动生成唯一 ID 并注册。
 */
public class Client {
    private static final String MASTER_HOST = "127.0.0.1"; // Master 的主机地址
    private static final int MASTER_PORT = 5000;         // Master 的端口号
    private static final AtomicInteger SERVER_COUNTER = new AtomicInteger(1); // 自增计数器

    private Socket socket;
    private BufferedWriter writer;
    private BufferedReader reader;
    private volatile boolean running = true;
    private String regionServerId; // 自动生成的 RegionServer ID

    public static void main(String[] args) {
        Client client = new Client();
        try {
            client.start();
            // 保持运行，直到手动终止
            while (true) {
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Client error: " + e.getMessage());
        } finally {
            client.stop();
        }
    }

    /**
     * 启动 Client，连接到 Master 并自动注册。
     */
    public void start() throws IOException {
        // 自动生成 RegionServer ID（如 RS_1, RS_2, ...）
        this.regionServerId = "RS_" + SERVER_COUNTER.getAndIncrement();
        System.out.println("Starting RegionServer Client with ID: " + regionServerId);

        connectToMaster();
        registerRegionServer();
        startListenerThread();
    }

    private void connectToMaster() throws IOException {
        socket = new Socket(MASTER_HOST, MASTER_PORT);
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        System.out.println("Connected to Master at " + MASTER_HOST + ":" + MASTER_PORT);
    }

    private void registerRegionServer() throws IOException {
        String registrationInfo = "REGISTER " + regionServerId + " " + getLocalHostAddress();
        writer.write(registrationInfo);
        writer.newLine();
        writer.flush();

        String response = reader.readLine();
        if ("REGISTERED".equals(response)) {
            System.out.println("Successfully registered with Master.");
        } else {
            throw new IOException("Failed to register: " + response);
        }
    }

    private void startListenerThread() {
        Thread listenerThread = new Thread(this::listenForRequests);
        listenerThread.setDaemon(true);
        listenerThread.start();
        System.out.println("Started listener thread for Master requests.");
    }

    /**
     * 获取本地的 IP 地址。
     *
     * @return 本地 IP 地址字符串
     */
    private String getLocalHostAddress() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    /**
     * 监听来自 Master 的请求，并根据请求执行相应的操作。
     */
    private void listenForRequests() {
        try {
            String request;
            while (running && (request = reader.readLine()) != null) {
                System.out.println("Received request from Master: " + request);

                if(request.contains("REGISTER"))continue;

                // 解析请求类型和参数
                String[] parts = request.split(" ", 2);
                if (parts.length < 1) {
                    System.err.println("Invalid request format: " + request);
                    continue;
                }

                String command = parts[0].toUpperCase();
                String params = parts.length > 1 ? parts[1] : "";

                switch (command) {
                    case "INSERT":
                        handleInsert(params);
                        break;
                    case "DELETE":
                        handleDelete(params);
                        break;
                    case "UPDATE":
                        handleUpdate(params);
                        break;
                    case "SELECT":
                        handleSelect(params);
                        break;
                    case "TRUNCATE":
                        handleTruncate(params);
                        break;
                    case "CREATE_TABLE":
                        handleCreateTable(params);
                        break;
                    case "DROP_TABLE":
                        handleDropTable(params);
                        break;
                    case "ALTER_TABLE":
                        handleAlterTable(params);
                        break;
                    default:
                        System.err.println("Unknown command from Master: " + command);
                        sendResponse("ERROR Unknown command");
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error while listening for requests: " + e.getMessage());
        } finally {
            stop();
        }
    }

    /**
     * 处理来自 Master 的 INSERT 请求。
     *
     * @param params 请求参数，通常包含 SQL 语句
     */
    private void handleInsert(String params) {
        try {
            // 假设 params 包含完整的 INSERT SQL 语句
            boolean success = ServerClient.executeCmd(params);
            if (success) {
                sendResponse("SUCCESS INSERT");
            } else {
                sendResponse("FAILURE INSERT");
            }
        } catch (Exception e) {
            System.err.println("Error handling INSERT: " + e.getMessage());
            sendResponse("FAILURE INSERT");
        }
    }

    /**
     * 处理来自 Master 的 DELETE 请求。
     *
     * @param params 请求参数，通常包含 SQL 语句
     */
    private void handleDelete(String params) {
        try {
            // 假设 params 包含完整的 DELETE SQL 语句
            boolean success = ServerClient.executeCmd(params);
            if (success) {
                sendResponse("SUCCESS DELETE");
            } else {
                sendResponse("FAILURE DELETE");
            }
        } catch (Exception e) {
            System.err.println("Error handling DELETE: " + e.getMessage());
            sendResponse("FAILURE DELETE");
        }
    }

    /**
     * 处理来自 Master 的 UPDATE 请求。
     *
     * @param params 请求参数，通常包含 SQL 语句
     */
    private void handleUpdate(String params) {
        try {
            // 假设 params 包含完整的 UPDATE SQL 语句
            boolean success = ServerClient.executeCmd(params);
            if (success) {
                sendResponse("SUCCESS UPDATE");
            } else {
                sendResponse("FAILURE UPDATE");
            }
        } catch (Exception e) {
            System.err.println("Error handling UPDATE: " + e.getMessage());
            sendResponse("FAILURE UPDATE");
        }
    }

    /**
     * 处理来自 Master 的 SELECT 请求。
     *
     * @param params 请求参数，通常包含 SQL 语句
     */
    private void handleSelect(String params) {
        try {
            // 假设 params 包含完整的 SELECT SQL 语句
            String result = ServerClient.selectTable(params); // 需要根据实际情况调整
            if (result != null) {
                sendResponse("SUCCESS SELECT " + result);
            } else {
                sendResponse("FAILURE SELECT");
            }
        } catch (Exception e) {
            System.err.println("Error handling SELECT: " + e.getMessage());
            sendResponse("FAILURE SELECT");
        }
    }

    /**
     * 处理来自 Master 的 TRUNCATE 请求。
     *
     * @param params 请求参数，通常包含表名
     */
    private void handleTruncate(String params) {
        try {
            // 假设 params 包含表名
            String[] parts = params.split(" ");
            if (parts.length < 1) {
                sendResponse("FAILURE TRUNCATE Invalid parameters");
                return;
            }
            String tableName = parts[0];
            String sql = "TRUNCATE TABLE " + tableName;
            boolean success = ServerClient.executeCmd(sql);
            if (success) {
                sendResponse("SUCCESS TRUNCATE");
            } else {
                sendResponse("FAILURE TRUNCATE");
            }
        } catch (Exception e) {
            System.err.println("Error handling TRUNCATE: " + e.getMessage());
            sendResponse("FAILURE TRUNCATE");
        }
    }

    /**
     * 处理来自 Master 的 CREATE_TABLE 请求。
     *
     * @param params 请求参数，通常包含表名和 SQL 语句
     */
    private void handleCreateTable(String params) {
        try {
            // 假设 params 包含表名和完整的 CREATE TABLE SQL 语句
            String[] parts = params.split(" ", 2);
            if (parts.length < 2) {
                sendResponse("FAILURE CREATE_TABLE Invalid parameters");
                return;
            }
            String tableName = parts[0];
            String sql = parts[1];
            List<ResType> result = RegionManager.createTableMasterAndSlave(tableName, sql);
            if (result.get(0) == ResType.CREATE_TABLE_SUCCESS && result.get(1) == ResType.CREATE_TABLE_SUCCESS) {
                sendResponse("SUCCESS CREATE_TABLE");
            } else {
                sendResponse("FAILURE CREATE_TABLE");
            }
        } catch (Exception e) {
            System.err.println("Error handling CREATE_TABLE: " + e.getMessage());
            sendResponse("FAILURE CREATE_TABLE");
        }
    }

    /**
     * 处理来自 Master 的 DROP_TABLE 请求。
     *
     * @param params 请求参数，通常包含表名
     */
    private void handleDropTable(String params) {
        try {
            // 假设 params 包含表名
            String tableName = params.trim();
            String sql = "DROP TABLE IF EXISTS " + tableName;
            ResType result = RegionManager.dropTable(tableName, sql);
            if (result == ResType.DROP_TABLE_SUCCESS) {
                sendResponse("SUCCESS DROP_TABLE");
            } else {
                sendResponse("FAILURE DROP_TABLE");
            }
        } catch (Exception e) {
            System.err.println("Error handling DROP_TABLE: " + e.getMessage());
            sendResponse("FAILURE DROP_TABLE");
        }
    }

    /**
     * 处理来自 Master 的 ALTER_TABLE 请求。
     *
     * @param params 请求参数，通常包含表名和 SQL 语句
     */
    private void handleAlterTable(String params) {
        try {
            // 假设 params 包含表名和完整的 ALTER TABLE SQL 语句
            String[] parts = params.split(" ", 2);
            if (parts.length < 2) {
                sendResponse("FAILURE ALTER_TABLE Invalid parameters");
                return;
            }
            String tableName = parts[0];
            String sql = parts[1];
            ResType result = RegionManager.alterTable(tableName, sql);
            if (result == ResType.ALTER_SUCCESS) {
                sendResponse("SUCCESS ALTER_TABLE");
            } else {
                sendResponse("FAILURE ALTER_TABLE");
            }
        } catch (Exception e) {
            System.err.println("Error handling ALTER_TABLE: " + e.getMessage());
            sendResponse("FAILURE ALTER_TABLE");
        }
    }

    /**
     * 向 Master 发送响应。
     *
     * @param response 响应消息
     */
    private void sendResponse(String response) {
        try {
            writer.write(response);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("Failed to send response to Master: " + e.getMessage());
        }
    }

    /**
     * 停止 RegionServer 的 Client。
     */
    public void stop() {
        running = false;
        try {
            if (writer != null) {
                writer.close();
            }
            if (reader != null) {
                reader.close();
            }
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error while stopping RegionServerClient: " + e.getMessage());
        }
        System.out.println("RegionServerClient stopped.");
    }


}