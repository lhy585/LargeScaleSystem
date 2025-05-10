package regionserver;

import java.io.*;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import zookeeper.TableInform; // Keep if tables list is used, otherwise remove

/**
 * Handles a single connection to the RegionServer's listening port.
 * Primarily expects direct connections from User Clients for SELECT queries.
 */
public class ServerThread implements Runnable {
    private final Socket socket;
    private final Statement statement; // Use the shared statement from RegionServer
    // private final ArrayList<TableInform> tables; // Likely unused now
    private BufferedReader reader = null;
    private BufferedWriter writer = null;

    // Pass the shared statement from RegionServer
    ServerThread(Socket socket, Statement statement, ArrayList<TableInform> tables) {
        this.socket = socket;
        this.statement = statement;
        // this.tables = tables; // Store if needed, otherwise remove
        System.out.println("[ServerThread " + Thread.currentThread().getId() + "] Handling connection from " + socket.getRemoteSocketAddress());
    }

    @Override
    public void run() {
        try {
            // Use UTF-8 for consistency
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));

            // Assume connection is from a User Client sending SQL (likely SELECT)
            serveUserClient();

        } catch (IOException e) {
            // Avoid logging errors if the socket was closed intentionally or by the client
            if (!socket.isClosed()) {
                System.err.println("[ServerThread " + Thread.currentThread().getId() + "] IOException: " + e.getMessage() + " for client " + socket.getRemoteSocketAddress());
                // e.printStackTrace(); // Uncomment for detailed debugging
            }
        } catch (Exception e) {
            System.err.println("[ServerThread " + Thread.currentThread().getId() + "] Unexpected Exception: " + e.getMessage() + " for client " + socket.getRemoteSocketAddress());
            e.printStackTrace(); // Log unexpected errors
            try {
                reactCmd(false, "Internal server error: " + e.getMessage());
            } catch (IOException ignored) {} // Best effort to inform client
        }
        finally {
            try {
                if (reader != null) reader.close();
                if (writer != null) writer.close();
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
                System.out.println("[ServerThread " + Thread.currentThread().getId() + "] Connection closed for " + socket.getRemoteSocketAddress());
            } catch (IOException e) {
                System.err.println("[ServerThread " + Thread.currentThread().getId() + "] Error closing resources: " + e.getMessage());
            }
        }
    }

    /**
     * Services requests assuming the connection is from a User Client.
     * Expects SQL commands, primarily SELECT.
     */
    public void serveUserClient() throws IOException {
        String sqlCommand;
        while ((sqlCommand = reader.readLine()) != null) {
            System.out.println("[ServerThread " + Thread.currentThread().getId() + "] Received SQL from Client " + socket.getRemoteSocketAddress() + ": " + sqlCommand);

            if ("exit".equalsIgnoreCase(sqlCommand.trim())) {
                System.out.println("[ServerThread " + Thread.currentThread().getId() + "] Client " + socket.getRemoteSocketAddress() + " requested exit.");
                break;
            }

            // Process SQL command (mainly expect SELECT here)
            String trimmedCommand = sqlCommand.trim().toLowerCase();
            if (trimmedCommand.startsWith("select")) {
                handleDirectSelect(sqlCommand);
            } else {
                System.err.println("[ServerThread " + Thread.currentThread().getId() + "] Received non-SELECT command from direct client connection: " + sqlCommand);
                reactCmd(false, "ERROR: This connection only supports SELECT queries.");
                // Optionally, could execute other commands if design allows, but
                // generally DDL/DML go through the Master.
                // boolean success = ServerClient.executeCmd(sqlCommand);
                // reactCmd(success, success ? "Command executed." : "Command failed.");
            }
        }
    }

    /**
     * Handles a SELECT query received directly from a client.
     * Executes the query and sends results back row by row.
     *
     * @param sqlSelectCommand The SELECT SQL command.
     * @throws IOException If communication fails.
     */
    private void handleDirectSelect(String sqlSelectCommand) throws IOException {
        ResultSet rs = null;
        try {
            if (statement == null || statement.isClosed()) {
                throw new SQLException("Database statement is not available.");
            }
            // Synchronize if statement is not thread-safe, though typically it is for execution.
            // synchronized(statement) { // Usually not needed for executeQuery
            rs = statement.executeQuery(sqlSelectCommand);
            // }

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            StringBuilder row1 = new StringBuilder();
            for(int i=1;i<=columnCount;i++){
                row1.append(rsmd.getColumnName(i));
                if(i<columnCount){
                    row1.append("\t");
                }
            }
            writer.write(row1.toString() + "\n"); // Send one row per line
            // Send data rows
            int rowCount = 1;
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    // Get string representation, handle NULLs gracefully
                    String value = rs.getString(i);
                    row.append(value == null ? "NULL" : value);
                    if (i < columnCount) {
                        row.append("\t"); // Use a clear delimiter (e.g., tab)
                    }
                }
                writer.write(row.toString() + "\n"); // Send one row per line
                rowCount++;
            }
            // Send an end-of-data marker
            writer.write("END_OF_DATA\n");
            writer.flush();
            System.out.println("[ServerThread " + Thread.currentThread().getId() + "] Sent " + rowCount + " rows for query.");

        } catch (SQLException e) {
            System.err.println("[ServerThread " + Thread.currentThread().getId() + "] SQLException executing SELECT: " + e.getMessage());
            // Inform the client about the error
            reactCmd(false, "ERROR executing query: " + e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* ignore */ }
            }
        }
    }

    /**
     * Responds to the connected client/master/server.
     * Prefixes with SUCCESS: or ERROR:.
     * @param success Whether the operation was successful.
     * @param message The message to send.
     */
    private void reactCmd(boolean success, String message) throws IOException {
        if (writer != null && !socket.isClosed()) {
            writer.write((success ? "SUCCESS: " : "ERROR: ") + message + "\n");
            writer.flush();
        }
    }
}