package regionserver;

import java.io.*;
import java.net.Socket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import master.ResType; // Assuming ResType is accessible or copied

/**
 * Main process for a RegionServer.
 * 1. Initializes static RegionServer resources (DB, ZK, listening socket).
 * 2. Starts a thread to listen for direct client connections (e.g., for SELECT).
 * 3. Connects to the Master (port 5001) to register and receive commands (DDL/DML).
 */
public class Client {
    private static final String MASTER_HOST = "127.0.0.1"; // Master's host address
    // Port Master listens for RegionServer connections
    private static final int MASTER_REGION_SERVER_PORT = 5001;

    private Socket masterSocket;
    private BufferedWriter writerToMaster;
    private BufferedReader readerFromMaster;
    private volatile boolean running = true;

    // Removed SERVER_COUNTER and regionServerId, registration uses ZK path (IP)

    public static void main(String[] args) {
        // 1. Initialize RegionServer static resources
        try {
            RegionServer.initRegionServer();
        } catch (RuntimeException e) {
            System.err.println("[RegionServer Process] Initialization failed: " + e.getMessage());
            System.err.println("[RegionServer Process] Exiting.");
            return; // Stop if initialization fails
        }

        // 2. Start the listener thread for direct client connections (on RegionServer.clientListenPort)
        Thread listenerThread = new Thread(RegionServer::startListening);
        listenerThread.setDaemon(false); // Keep process alive
        listenerThread.setName("RegionServer-ClientListener-" + RegionServer.clientListenPort);
        listenerThread.start();

        // 3. Connect to Master and handle commands
        Client regionServerProcess = new Client();
        try {
            regionServerProcess.start(); // Connects to master and starts listening for commands
            // Keep the main thread alive (or handle graceful shutdown)
            // The listenerThread and the master communication loop keep the process running.
            System.out.println("[RegionServer Process] Running. Listening for clients on " + RegionServer.clientListenPort +
                    " and connected to Master on " + MASTER_REGION_SERVER_PORT);

            // Add shutdown hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[RegionServer Process] Shutdown hook triggered.");
                regionServerProcess.stop();
                RegionServer.shutdown();
            }));

            // Keep main thread alive if necessary, or wait for listener/master threads
            // For example, wait for the listener thread:
            // listenerThread.join();
            // Or simply loop:
            while(regionServerProcess.running) {
                Thread.sleep(5000); // Keep main thread alive, checking status periodically
            }


        } catch (IOException | InterruptedException e) {
            System.err.println("[RegionServer Process] Error during Master communication setup or runtime: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Ensure resources are cleaned up if start() fails partially or loop exits unexpectedly
            regionServerProcess.stop(); // Stop master connection loop
            // RegionServer.shutdown() should be called by shutdown hook or here if hook isn't used
            // RegionServer.shutdown();
            System.out.println("[RegionServer Process] Exited.");
        }
    }

    /**
     * Starts the connection to the Master: connects, registers, and listens for commands.
     */
    public void start() throws IOException {
        connectToMaster();
        registerWithMaster();
        listenForMasterCommands(); // Start listening loop (runs in current thread)
    }

    private void connectToMaster() throws IOException {
        System.out.println("[RegionServer Process] Connecting to Master at " + MASTER_HOST + ":" + MASTER_REGION_SERVER_PORT + "...");
        masterSocket = new Socket(MASTER_HOST, MASTER_REGION_SERVER_PORT);
        // Ensure UTF-8 encoding for reliable communication
        writerToMaster = new BufferedWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8));
        readerFromMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8));
        System.out.println("[RegionServer Process] Connected to Master.");
    }

    /**
     * Registers this RegionServer with the Master.
     * Sends necessary identification or information.
     */
    private void registerWithMaster() throws IOException {
        System.out.println("[RegionServer Process] Registering with Master...");
        // 协议: 第1行 = 命令, 第2行 = ZK 中使用的 IP
        writerToMaster.write("REGISTER_REGION_SERVER");
        writerToMaster.newLine();
        writerToMaster.write(RegionServer.ip); // 发送在 ZK 注册的 IP
        writerToMaster.newLine();
        writerToMaster.flush();
        System.out.println("[RegionServer Process] Registration messages sent to Master (including ZK IP: " + RegionServer.ip + ").");
    }


    /**
     * Listens for commands from the Master and handles them.
     * This loop runs in the main thread of the Client object.
     */
    private void listenForMasterCommands() {
        System.out.println("[RegionServer Process] Listening for commands from Master...");
        try {
            String commandFromMaster;
            while (running && (commandFromMaster = readerFromMaster.readLine()) != null) {
                System.out.println("[RegionServer Process] Received command from Master: \"" + commandFromMaster + "\"");

                // Simple check for Master heartbeat or test command
                if ("PING".equalsIgnoreCase(commandFromMaster)) {
                    sendResponseToMaster("PONG");
                    continue;
                }
                if ("QUIT".equalsIgnoreCase(commandFromMaster)) {
                    System.out.println("[RegionServer Process] Received QUIT command from Master.");
                    running = false;
                    break;
                }

                // Assume command is SQL to be executed directly
                // Master should format the command appropriately (e.g., "CREATE TABLE ...", "INSERT INTO ...")
                handleSqlCommand(commandFromMaster);

            }
        } catch (IOException e) {
            if (running) { // Only log error if not intentionally stopping
                System.err.println("[RegionServer Process] IOException while listening for Master commands: " + e.getMessage());
                // Consider adding retry logic or shutting down based on the error
                running = false; // Stop running on communication error
            }
        } finally {
            System.out.println("[RegionServer Process] Stopped listening for Master commands.");
            stop(); // Ensure resources are closed if loop terminates
        }
    }

    /**
     * Handles a generic SQL command received from the Master.
     * Determines the type (SELECT vs. others) and executes it using ServerClient.
     * Sends a simple SUCCESS/FAILURE response back to the Master.
     *
     * @param sqlCommand The SQL command string from the Master.
     */
    private void handleSqlCommand(String sqlCommand) {
        String commandType = "UNKNOWN";
        boolean success = false;
        String resultData = null; // For SELECT results

        try {
            String trimmedCommand = sqlCommand.trim().toLowerCase();
            // Basic command type detection
            if (trimmedCommand.startsWith("select")) {
                commandType = "SELECT";
                resultData = ServerClient.selectTable(sqlCommand); // Execute SELECT
                success = (resultData != null); // Success if we got some result (even empty)
            } else if (trimmedCommand.startsWith("insert")) {
                commandType = "INSERT";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("update")) {
                commandType = "UPDATE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("delete")) {
                commandType = "DELETE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("create table")) {
                commandType = "CREATE_TABLE";
                success = ServerClient.createTable(sqlCommand); // Use the method that just executes
            } else if (trimmedCommand.startsWith("drop table")) {
                commandType = "DROP_TABLE";
                // Extract table name for the dropTable method signature if needed, otherwise use executeCmd
                String tableName = ServerClient.extractTableName(sqlCommand); // Helper might be needed if dropTable requires name
                if (tableName != null) {
                    success = ServerClient.dropTable(tableName); // Assumes dropTable takes only name
                } else {
                    success = ServerClient.executeCmd(sqlCommand); // Fallback to generic execution
                }
            } else if (trimmedCommand.startsWith("alter table")) {
                commandType = "ALTER_TABLE";
                success = ServerClient.executeCmd(sqlCommand);
            } else if (trimmedCommand.startsWith("truncate")) {
                commandType = "TRUNCATE";
                success = ServerClient.executeCmd(sqlCommand);
            } else {
                // Handle other SQL commands or potentially non-SQL instructions
                System.err.println("[RegionServer Process] Received unrecognized command structure: " + sqlCommand);
                commandType = "EXECUTE_UNKNOWN";
                success = ServerClient.executeCmd(sqlCommand); // Try executing anyway
            }

            // Send response back to Master
            if ("SELECT".equals(commandType)) {
                if (success) {
                    // Send SUCCESS marker followed by data lines, ending with a specific marker
                    sendResponseToMaster("SUCCESS SELECT");
                    if (resultData.isEmpty()) {
                        sendResponseToMaster("END_OF_SELECT_DATA"); // Indicate empty result
                    } else {
                        // Send each line of the result
                        try (BufferedReader resultReader = new BufferedReader(new StringReader(resultData))) {
                            String line;
                            while ((line = resultReader.readLine()) != null) {
                                sendResponseToMaster(line);
                            }
                        }
                        sendResponseToMaster("END_OF_SELECT_DATA"); // End marker
                    }
                } else {
                    sendResponseToMaster("FAILURE SELECT");
                }
            } else {
                // For non-SELECT commands
                if (success) {
                    sendResponseToMaster("SUCCESS " + commandType);
                } else {
                    sendResponseToMaster("FAILURE " + commandType);
                }
            }

        } catch (Exception e) {
            System.err.println("[RegionServer Process] Error handling command (" + commandType + "): " + sqlCommand);
            System.err.println("[RegionServer Process] Exception: " + e.getMessage());
            e.printStackTrace();
            // Send failure response
            sendResponseToMaster("FAILURE " + commandType + " Exception: " + e.getMessage());
        }
    }


    /**
     * Sends a response message back to the Master.
     *
     * @param response The message string to send.
     */
    private void sendResponseToMaster(String response) {
        if (writerToMaster != null) {
            try {
                writerToMaster.write(response);
                writerToMaster.newLine();
                writerToMaster.flush();
                // System.out.println("[RegionServer Process] Sent to Master: " + response); // Optional: Log sent messages
            } catch (IOException e) {
                System.err.println("[RegionServer Process] Failed to send response to Master: " + e.getMessage());
                // Consider implications: Master might timeout or mark this RS as failed
                running = false; // Stop if we can't communicate back
            }
        } else {
            System.err.println("[RegionServer Process] Cannot send response, writer to Master is null.");
        }
    }

    /**
     * Stops the client's connection to the Master and sets the running flag to false.
     */
    public void stop() {
        System.out.println("[RegionServer Process] Stopping connection to Master...");
        running = false; // Signal loops to exit
        try {
            // Close streams and socket connected to the Master
            if (writerToMaster != null) {
                writerToMaster.close();
            }
            if (readerFromMaster != null) {
                readerFromMaster.close();
            }
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
            System.out.println("[RegionServer Process] Connection to Master closed.");
        } catch (IOException e) {
            System.err.println("[RegionServer Process] Error while closing connection to Master: " + e.getMessage());
        } finally {
            writerToMaster = null;
            readerFromMaster = null;
            masterSocket = null;
        }
    }
}