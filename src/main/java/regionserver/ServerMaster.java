package regionserver;

import zookeeper.ZooKeeperManager;
// import zookeeper.ZooKeeperUtils; // Not directly used by ServerMaster if zkManager provides all needed methods

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException; // Added for clarity
import java.sql.Statement;

// Explicit imports to help with potential type resolution issues for InputStream
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

/**
 * Contains static methods likely called remotely by the Master (e.g., via RPC or commands)
 * to perform administrative tasks like migrating or dumping tables between RegionServers.
 * Interacts with local system commands (mysqldump, mysql) and ZooKeeper.
 */
public class ServerMaster {

    // Define the base path consistently
    private static final String ZK_REGION_SERVER_BASE_PATH = "/lss/region_server";

    // NOTE: Using external commands like 'mysql' and 'mysqldump' has dependencies
    // and security implications. Consider pure Java/JDBC alternatives if possible.

    /**
     * Copies (dumps) a table from a source RegionServer to this (target) RegionServer.
     * Assumes this code is running in the context of the *target* RegionServer process,
     * or has access to execute commands and connect to the target MySQL.
     * The Master would typically *instruct* the target RS to perform this pull.
     *
     * @param sourceRegionName ZK node name (e.g., IP address) of the source server.
     * @param sourceMysqlHost  IP/hostname of the source MySQL instance.
     * @param sourceMysqlPort  Port of the source MySQL instance.
     * @param sourceMysqlUser  Username for source MySQL.
     * @param sourceMysqlPwd   Password for source MySQL.
     * @param tableName        The name of the table to dump.
     * @param targetMysqlUser  Username for the target (local) MySQL.
     * @param targetMysqlPwd   Password for the target (local) MySQL.
     * @return true if the dump and import seem successful, false otherwise.
     */
    public static boolean dumpTable(String sourceRegionName, String sourceMysqlHost, String sourceMysqlPort, String sourceMysqlUser, String sourceMysqlPwd, String tableName, String targetMysqlUser, String targetMysqlPwd) {
        System.out.println("[ServerMaster] Dumping table '" + tableName + "' from " + sourceRegionName + " (" + sourceMysqlHost + ":" + sourceMysqlPort + ")");
        String dumpFileName = "./sql_dump_" + tableName + "_" + System.currentTimeMillis() + ".sql"; // Unique temp file
        File dumpFile = new File(dumpFileName);

        try {
            String osName = System.getProperty("os.name").toLowerCase();
            // 1. Dump table from source MySQL using mysqldump
            String dumpCmdStr = String.format(
                    "mysqldump -h %s -P %s -u %s -p%s --databases lss --tables %s --result-file=%s",
                    sourceMysqlHost, sourceMysqlPort, sourceMysqlUser, sourceMysqlPwd, tableName, dumpFileName
            );
            System.out.println("[ServerMaster] Executing dump command (password masked): " + dumpCmdStr.replace("-p"+sourceMysqlPwd, "-p****"));
            Process dumpProcess = Runtime.getRuntime().exec(dumpCmdStr); // Direct execution usually fine

            // Start stream readers immediately
            readStream(dumpProcess.getErrorStream(), "Dump Error Stream");
            readStream(dumpProcess.getInputStream(), "Dump Output Stream");
            int dumpExitCode = dumpProcess.waitFor();


            if (dumpExitCode != 0 || !dumpFile.exists() || dumpFile.length() == 0) {
                System.err.println("[ServerMaster] Dump failed. Exit code: " + dumpExitCode + ". File exists: " + dumpFile.exists() + ", Size: " + (dumpFile.exists() ? dumpFile.length() : 0));
                // Streams already being read by threads
                return false;
            }
            System.out.println("[ServerMaster] Dump successful to file: " + dumpFileName);

            // 2. Import the dump file into the target (local) MySQL
            String importCmdStr = String.format(
                    "mysql -h localhost -P %s -u %s -p%s lss < %s",
                    RegionServer.mysqlPort, targetMysqlUser, targetMysqlPwd, dumpFileName
            );
            System.out.println("[ServerMaster] Executing import command (password masked)... " + importCmdStr.replace("-p"+targetMysqlPwd, "-p****"));
            Process importProcess;
            if (osName.contains("win")) {
                importProcess = Runtime.getRuntime().exec(new String[]{"cmd", "/c", importCmdStr});
            } else {
                // For Linux/macOS, use sh -c
                importProcess = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", importCmdStr});
            }

            // Start stream readers immediately
            readStream(importProcess.getErrorStream(), "Import Error Stream");
            readStream(importProcess.getInputStream(), "Import Output Stream");
            int importExitCode = importProcess.waitFor();

            if (importExitCode != 0) {
                System.err.println("[ServerMaster] Import failed. Exit code: " + importExitCode);
                return false;
            }
            System.out.println("[ServerMaster] Import successful for table: " + tableName);
            return true;

        } catch (InterruptedException | IOException e) {
            System.err.println("[ServerMaster] Error during dump/import process for table '" + tableName + "': " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            // Clean up the dump file
            if (dumpFile.exists()) {
                if (!dumpFile.delete()) {
                    System.err.println("[ServerMaster] Warning: Failed to delete temporary dump file: " + dumpFileName);
                }
            }
        }
    }


    /**
     * Migrates a table by dumping from source, importing to target, and dropping from source.
     *
     * @param sourceRegionName ZK node name (e.g., IP) of the source server.
     * @param targetRegionName ZK node name (e.g., IP) of the target server (where this runs).
     * @param tableName        The name of the table to migrate.
     * @param zkManager        An instance of ZooKeeperManager for metadata updates.
     * @return true if migration appears successful, false otherwise.
     */
    public static boolean migrateTable(String sourceRegionName, String targetRegionName, String tableName, ZooKeeperManager zkManager) {
        System.out.println("[ServerMaster] Migrating table '" + tableName + "' from " + sourceRegionName + " to " + targetRegionName);

        String sourceServerBasePath = ZK_REGION_SERVER_BASE_PATH + "/" + sourceRegionName;

        try {
            // 1. Get source server connection details from ZooKeeper
            if (!zkManager.nodeExists(sourceServerBasePath)) {
                System.err.println("[ServerMaster] Source RegionServer node not found in ZooKeeper: " + sourceServerBasePath);
                return false;
            }

            // Fetch individual details from ZK sub-nodes based on ZooKeeperManager.addRegionServer structure
            String sourceMysqlUser = zkManager.getData(sourceServerBasePath + "/username");
            String sourceMysqlPwd = zkManager.getData(sourceServerBasePath + "/password");
            // Assuming MySQL port is stored in '/port2regionserver' node by the RS calling zkManager.addRegionServer
            // THIS REQUIRES THE PREREQUISITE CHANGE MENTIONED ABOVE in RegionServer.java
            String sourceMysqlPort = zkManager.getData(sourceServerBasePath + "/port2regionserver");

            if (sourceMysqlUser == null || sourceMysqlPwd == null || sourceMysqlPort == null) {
                System.err.println("[ServerMaster] Incomplete source server DB details in ZooKeeper for " + sourceRegionName);
                System.err.println("User: " + sourceMysqlUser + ", Pwd: (masked), Port: " + sourceMysqlPort);
                return false;
            }
            String sourceMysqlHost = sourceRegionName; // Assuming ZK node name is the host IP/name

            // 2. Dump from source and Import to target (local)
            boolean dumpImportSuccess = dumpTable(
                    sourceRegionName, sourceMysqlHost, sourceMysqlPort, sourceMysqlUser, sourceMysqlPwd, tableName,
                    RegionServer.mysqlUser, RegionServer.mysqlPwd // Use local MySQL credentials for import (target)
            );

            if (!dumpImportSuccess) {
                System.err.println("[ServerMaster] Dump/Import step failed during migration.");
                return false;
            }

            System.out.println("[ServerMaster] Dump/Import successful. Master must update ZooKeeper metadata for table '" + tableName + "' (move from " + sourceRegionName + " to " + targetRegionName + ").");

            // 4. Drop table from the source Region Server (Master should trigger this AFTER ZK update)
            System.out.println("[ServerMaster] Requesting Master to trigger DROP TABLE '" + tableName + "' on source server " + sourceRegionName + " after metadata update.");
            Connection sourceConn = null;
            Statement sourceStmt = null;
            try {
                System.out.println("[ServerMaster] Connecting to source MySQL ("+sourceMysqlHost+":"+sourceMysqlPort+") to drop table...");
                // Assuming JdbcUtils.getConnection can take host and port
                sourceConn = JdbcUtils.getConnection(sourceMysqlUser, sourceMysqlPwd);
                if (sourceConn != null) {
                    sourceStmt = sourceConn.createStatement();
                    String dropSql = "DROP TABLE IF EXISTS `" + tableName + "`"; // Use backticks for safety
                    System.out.println("[ServerMaster] Executing on source ("+sourceRegionName+"): " + dropSql);
                    sourceStmt.executeUpdate(dropSql);
                    System.out.println("[ServerMaster] Table dropped from source successfully.");
                } else {
                    System.err.println("[ServerMaster] Could not connect to source MySQL to drop table.");
                }
            } catch (SQLException e) {
                System.err.println("[ServerMaster] Error dropping table from source MySQL: " + e.getMessage());
                // Migration technically succeeded in moving data, but cleanup failed. This is an inconsistency.
            } finally {
                // Ensure JdbcUtils.releaseResc handles null statement/connection
                if (sourceStmt != null) try { sourceStmt.close(); } catch (SQLException e) { e.printStackTrace(); }
                if (sourceConn != null) try { sourceConn.close(); } catch (SQLException e) { e.printStackTrace(); }
                // Or if JdbcUtils.releaseResc is robust:
                // JdbcUtils.releaseResc(null, sourceStmt, sourceConn);
            }

            System.out.println("[ServerMaster] Migration process completed for table: " + tableName);
            return true; // Data is migrated. Source drop failure is a separate issue.

        } catch (Exception e) { // Catching general exception from zkManager.getData or other issues
            System.err.println("[ServerMaster] Critical error during migration for table '" + tableName + "': " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }


    /**
     * Helper method to read from process streams asynchronously to prevent blocking.
     *
     * @param stream The InputStream (e.g., from Process.getErrorStream() or Process.getInputStream()).
     * @param streamName A descriptive name for the stream (e.g., "Dump Error Stream").
     */
    private static void readStream(InputStream stream, String streamName) {
        new Thread(() -> {
            // Using java.io.InputStreamReader and java.io.BufferedReader
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                String line;
                // System.out.println("--- " + streamName + " Start ---"); // Optional: for debugging stream start/end
                while ((line = reader.readLine()) != null) {
                    System.out.println("[" + streamName + "] " + line);
                }
                // System.out.println("--- " + streamName + " End ---");
            } catch (IOException e) {
                // This can happen if the process closes the stream abruptly
                System.err.println("Error reading stream " + streamName + ": " + e.getMessage());
            }
        }).start();
    }

    // Deprecated methods addTableToValue and deleteTableFromValue remain unchanged.
    // As noted, they are fragile and ideally metadata is managed differently.
    @Deprecated
    public static String addTableToValue(String serverValue, String tableName) {
        return serverValue + "," + tableName;
    }

    @Deprecated
    public static String deleteTableFromValue(String serverValue, String tableName) {
        return serverValue.replace("," + tableName, "");
    }
}