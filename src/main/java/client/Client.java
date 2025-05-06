package client;

// Keep existing imports
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets; // Use standard charset

public class Client {
    // Master connection details (User Client port)
    public static String masterIp = "127.0.0.1";
    public static int masterPort = 5000;

    // Cache for table -> regionServerIP:regionServerPort mapping
    // Value format: "ip:port" (e.g., "192.168.1.10:4001")
    public static Map<String, String> tableLocationCache = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // Example cache entry (replace with actual IPs/Ports if known)
        // tableLocationCache.put("t1", "192.168.1.101:4001"); // Example

        // Start the communication thread
        new Communite(masterIp, masterPort).start();

        System.out.println("User Client started. Enter SQL commands ending with ';'. Type 'exit;' to quit.");
    }

    // Renamed thread class for clarity
    private static class Communite extends Thread {
        private final String masterServerIp;
        private final int masterServerPort;

        public Communite(String ip, int port) {
            this.masterServerIp = ip;
            this.masterServerPort = port;
        }

        @Override
        public void run() {
            // Use try-with-resources for console input if possible, or handle close()
            try (BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    System.out.print("mysql> ");
                    StringBuilder sqlBuilder = new StringBuilder();
                    String line;

                    // Read multi-line SQL input from console
                    while ((line = consoleInput.readLine()) != null) {
                        // Check for exit command
                        if ("exit;".equalsIgnoreCase(line.trim())) {
                            System.out.println("Exiting client.");
                            return; // Terminate the thread
                        }
                        sqlBuilder.append(line).append(" "); // Append line with space
                        if (line.trim().endsWith(";")) {
                            break; // End of SQL command
                        }
                        System.out.print("    -> "); // Prompt for more input
                    }

                    // If readLine returns null (e.g., EOF), exit
                    if (line == null && sqlBuilder.length() == 0) {
                        System.out.println("\nInput stream closed. Exiting.");
                        return;
                    }

                    String sql = sqlBuilder.toString().trim();
                    if (sql.isEmpty() || !sql.endsWith(";")) {
                        System.out.println("Invalid command or missing ';'");
                        continue;
                    }
                    System.out.println("Executing SQL: " + sql);


                    // --- SQL Processing Logic ---
                    try {
                        String firstWord = sql.substring(0, sql.indexOf(' ')).toLowerCase();

                        // Handle SELECT queries (potentially direct to RegionServer)
                        if (firstWord.equals("select") && !sql.toLowerCase().contains(" join ")) { // Simple SELECT check
                            String tableName = extractTableNameFromSelect(sql);
                            if (tableName == null) {
                                System.out.println("Could not extract table name from SELECT query.");
                                continue;
                            }
                            System.out.println("Detected SELECT on table: " + tableName);

                            String regionServerAddress = tableLocationCache.get(tableName);

                            if (regionServerAddress != null) {
                                // Cache hit: Connect directly to RegionServer
                                System.out.println("Cache hit for table '" + tableName + "'. Connecting directly to RegionServer at " + regionServerAddress);
                                executeSelectOnRegionServer(regionServerAddress, sql);
                            } else {
                                // Cache miss: Query Master for location, then connect to RegionServer
                                System.out.println("Cache miss for table '" + tableName + "'. Querying Master...");
                                queryMasterAndExecuteSelect(tableName, sql);
                            }
                        }
                        // Handle DDL/DML queries (send to Master)
                        else {
                            System.out.println("Sending DDL/DML/Other command to Master...");
                            sendCommandToMaster(sql);
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing SQL command: " + e.getMessage());
                        e.printStackTrace(); // More details for debugging
                    }
                    System.out.println("--------------------"); // Separator between commands
                } // End while(true) loop
            } catch (IOException e) {
                System.err.println("Error reading from console: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("Communication thread finished.");
            }
        } // End run()

        /**
         * Extracts the primary table name from a simple SELECT query.
         * Assumes format like "SELECT ... FROM tableName ..."
         */
        private String extractTableNameFromSelect(String sql) {
            try {
                String sqlLower = sql.toLowerCase();
                int fromIndex = sqlLower.indexOf(" from ");
                if (fromIndex == -1) return null;

                String afterFrom = sql.substring(fromIndex + 6).trim();
                String[] parts = afterFrom.split("\\s+"); // Split by whitespace
                if (parts.length > 0) {
                    String tableName = parts[0];
                    // Remove trailing semicolon or clauses like WHERE, GROUP BY etc.
                    tableName = tableName.split("[;,\\s(]", 2)[0];
                    return tableName;
                }
            } catch (Exception e) {
                System.err.println("Error parsing table name: " + e.getMessage());
            }
            return null;
        }

        /**
         * Connects directly to a RegionServer and executes a SELECT query.
         * Prints the results received.
         *
         * @param regionServerAddress "ip:port" string
         * @param sql                 The SELECT SQL query to execute
         */
        private void executeSelectOnRegionServer(String regionServerAddress, String sql) {
            String[] parts = regionServerAddress.split(":");
            if (parts.length != 2) {
                System.err.println("Invalid RegionServer address format in cache: " + regionServerAddress);
                // Optionally remove invalid entry: tableLocationCache.remove(extractTableNameFromSelect(sql));
                return;
            }
            String rsIp = parts[0];
            int rsPort;
            try {
                rsPort = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid RegionServer port in cache: " + parts[1]);
                return;
            }

            System.out.println("Connecting to RegionServer " + rsIp + ":" + rsPort + " for SELECT...");
            try (Socket rsSocket = new Socket(rsIp, rsPort);
                 PrintWriter rsOut = new PrintWriter(new OutputStreamWriter(rsSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader rsIn = new BufferedReader(new InputStreamReader(rsSocket.getInputStream(), StandardCharsets.UTF_8)))
            {
                // Send the SQL command
                rsOut.println(sql);
                System.out.println("Sent SELECT to RegionServer.");

                // Read the response lines
                System.out.println("--- RegionServer Response ---");
                String responseLine;
                boolean dataReceived = false;
                while ((responseLine = rsIn.readLine()) != null) {
                    // Use a clear end-of-data marker sent by the RegionServer's ServerThread
                    if ("END_OF_DATA".equals(responseLine)) {
                        break;
                    }
                    // Simple SUCCESS/ERROR check based on ServerThread's reactCmd
                    if (responseLine.startsWith("ERROR:")) {
                        System.out.println(responseLine); // Print error message from RS
                        dataReceived = true; // Mark as received to avoid "No response" message
                        break;
                    }
                    // Assume other lines are data rows
                    System.out.println(responseLine);
                    dataReceived = true;
                }
                if (!dataReceived) {
                    System.out.println("(No data rows received or connection closed prematurely)");
                }
                System.out.println("--- End of Response ---");

            } catch (UnknownHostException e) {
                System.err.println("Error: Unknown RegionServer host: " + rsIp);
                // Consider removing from cache if host is permanently unknown
                // tableLocationCache.remove(extractTableNameFromSelect(sql));
            } catch (IOException e) {
                System.err.println("Error communicating with RegionServer " + regionServerAddress + ": " + e.getMessage());
                // Consider removing from cache as the server might be down
                // tableLocationCache.remove(extractTableNameFromSelect(sql));
            }
        }


        /**
         * Queries the Master for table location, updates cache, and executes SELECT on RegionServer.
         *
         * @param tableName The table name being queried.
         * @param sql       The SELECT SQL query.
         */
        private void queryMasterAndExecuteSelect(String tableName, String sql) {
            System.out.println("Connecting to Master " + masterServerIp + ":" + masterServerPort + " to find table '" + tableName + "'...");
            try (Socket masterSocket = new Socket(masterServerIp, masterServerPort);
                 PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8)))
            {
                // Send the original SELECT SQL to the Master
                // Master's ClientHandler will parse it, find the region, and return location info
                masterOut.println(sql);
                System.out.println("Sent SELECT query to Master.");

                // Master's ClientHandler (modified) should respond with:
                // Line 1: "FOUND ip:port" or "NOT_FOUND" or "ERROR ..."
                // Line 2 (if found): The potentially modified SQL (e.g., with _slave table name)
                String statusLine = masterIn.readLine();
                System.out.println("Master status response: " + statusLine);

                if (statusLine != null && statusLine.startsWith("FOUND ")) {
                    String regionServerAddress = statusLine.substring(6).trim(); // Get "ip:port"
                    String modifiedSql = masterIn.readLine(); // Read the SQL Master wants us to execute

                    if (modifiedSql == null) {
                        System.err.println("Error: Master sent location but not the SQL to execute.");
                        return;
                    }

                    System.out.println("Master found table at: " + regionServerAddress);
                    System.out.println("Executing SQL from Master: " + modifiedSql);

                    // Cache the location
                    tableLocationCache.put(tableName, regionServerAddress);
                    System.out.println("Cached location for table '" + tableName + "'.");

                    // Execute the SELECT on the RegionServer using the received address and SQL
                    executeSelectOnRegionServer(regionServerAddress, modifiedSql);

                } else if (statusLine != null && statusLine.startsWith("NOT_FOUND")) {
                    System.out.println("Master reported table '" + tableName + "' not found.");
                    // Optional: Cache the fact that it's not found? (Negative caching)
                } else {
                    // Handle errors reported by the Master
                    System.err.println("Error received from Master: " + (statusLine == null ? "Connection closed" : statusLine));
                }

            } catch (IOException e) {
                System.err.println("Error communicating with Master: " + e.getMessage());
                // e.printStackTrace();
            }
        }


        /**
         * Sends a DDL/DML or other command directly to the Master.
         *
         * @param command The SQL command or instruction to send.
         */
        private void sendCommandToMaster(String command) {
            System.out.println("Connecting to Master " + masterServerIp + ":" + masterServerPort + "...");
            try (Socket masterSocket = new Socket(masterServerIp, masterServerPort);
                 PrintWriter masterOut = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8)))
            {
                // Send the command
                masterOut.println(command);
                System.out.println("Sent command to Master: " + command);

                // Read Master's response (Master's ClientHandler sends results/status)
                System.out.println("--- Master Response ---");
                String responseLine;
                while ((responseLine = masterIn.readLine()) != null) {
                    System.out.println(responseLine);
                }
                System.out.println("--- End of Response ---");

            } catch (IOException e) {
                System.err.println("Error communicating with Master: " + e.getMessage());
                // e.printStackTrace();
            }
        }

    } // End Communite class
} // End Client class