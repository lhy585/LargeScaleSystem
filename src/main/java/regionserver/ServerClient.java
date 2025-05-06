package regionserver;

import java.sql.*;
import java.util.Objects;

// Removed ZooKeeper imports

/**
 * Provides methods to interact with the local RegionServer's database.
 * Executes SQL commands received from the Master (via regionserver.Client)
 * or directly from User Clients (via ServerThread for SELECT).
 * Does NOT interact with ZooKeeper.
 */
public class ServerClient {

    // Removed ZooKeeperManager instance

    /**
     * Creates a table in the local database.
     *
     * @param tableName Ignored in this implementation, SQL command contains the name.
     * @param sqlCmd    The full CREATE TABLE SQL statement.
     * @return true if execution succeeds, false otherwise.
     * @deprecated Use createTable(String sqlCmd) instead.
     */
    @Deprecated
    public static boolean createTable(String tableName, String sqlCmd) {
        System.out.println("[ServerClient] Executing CREATE (deprecated): " + sqlCmd);
        return executeCmd(sqlCmd); // Just execute the command
    }

    /**
     * Creates a table in the local database by executing the provided SQL.
     *
     * @param sqlCmd The full CREATE TABLE SQL statement.
     * @return true if execution succeeds, false otherwise.
     */
    public static boolean createTable(String sqlCmd) {
        System.out.println("[ServerClient] Executing CREATE: " + sqlCmd);
        return executeCmd(sqlCmd); // Just execute the command
    }

    /**
     * Drops a table from the local database.
     *
     * @param tableName The name of the table to drop.
     * @return true if execution succeeds, false otherwise.
     */
    public static boolean dropTable(String tableName) {
        String sqlCmd = "DROP TABLE IF EXISTS `" + tableName + "`"; // Use backticks for safety
        System.out.println("[ServerClient] Executing DROP: " + sqlCmd);
        return executeCmd(sqlCmd);
    }

    /**
     * Executes a SELECT query on the local database and returns the result as a formatted string.
     * Each row is a line, columns separated by tabs. Includes header row.
     *
     * @param sqlCmd The SELECT SQL statement.
     * @return A string containing the query results, or null on error.
     * Returns empty string "" if query runs successfully but yields no rows.
     */
    public static String selectTable(String sqlCmd) {
        System.out.println("[ServerClient] Executing SELECT: " + sqlCmd);
        StringBuilder res = new StringBuilder();
        ResultSet rs = null;
        try {
            if (RegionServer.statement == null || RegionServer.statement.isClosed()) {
                throw new SQLException("Database statement is not available.");
            }

            // Ensure it's actually a select command
            if (!sqlCmd.trim().toLowerCase().startsWith("select")) {
                System.err.println("[ServerClient] Non-SELECT command passed to selectTable: " + sqlCmd);
                return null; // Or throw exception
            }

            rs = RegionServer.statement.executeQuery(sqlCmd);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // Add header row (Column names)
            for (int i = 1; i <= columnCount; i++) {
                res.append(rsmd.getColumnName(i));
                if (i < columnCount) res.append("\t");
            }
            res.append("\n");

            // Add data rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    res.append(value == null ? "NULL" : value); // Handle NULLs
                    if (i < columnCount) res.append("\t");
                }
                res.append("\n");
            }
            return res.toString();

        } catch (SQLException e) {
            System.err.println("[ServerClient] SQLException during SELECT: " + e.getMessage() + " SQL: " + sqlCmd);
            // e.printStackTrace(); // Uncomment for full stack trace
            return null; // Indicate error
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* ignored */ }
            }
        }
    }

    /**
     * Executes a generic SQL command (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE etc.).
     * Determines if it's a query (SELECT - though should use selectTable) or update.
     *
     * @param cmd The SQL command string.
     * @return true if the command executes successfully, false otherwise.
     */
    public static boolean executeCmd(String cmd) {
        String trimmedCmd = cmd.trim().toLowerCase();
        System.out.println("[ServerClient] Executing CMD: " + cmd);
        try {
            if (RegionServer.statement == null || RegionServer.statement.isClosed()) {
                throw new SQLException("Database statement is not available.");
            }

            if (trimmedCmd.startsWith("select")) {
                System.err.println("[ServerClient] Warning: SELECT statement passed to executeCmd. Use selectTable instead. Executing anyway.");
                ResultSet rs = RegionServer.statement.executeQuery(cmd);
                // Consume the result set to avoid potential issues, though we don't return data here.
                while(rs.next()){}
                rs.close();
            } else {
                // For INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE
                RegionServer.statement.executeUpdate(cmd);
            }
            return true; // Success
        } catch (SQLException e) {
            System.err.println("[ServerClient] SQLException during executeCmd: " + e.getMessage() + " SQL: " + cmd);
            // e.printStackTrace(); // Uncomment for full stack trace
            return false; // Failure
        }
    }

    /**
     * Executes an update operation (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
     * Kept for potential direct use, but executeCmd is more general.
     *
     * @param sql The SQL statement.
     * @throws SQLException If the SQL execution fails.
     * @deprecated Use executeCmd(String cmd) instead.
     */
    @Deprecated
    public static void executeUpdate(String sql) throws SQLException {
        System.out.println("[ServerClient] Executing UPDATE (deprecated): " + sql);
        if (RegionServer.statement == null || RegionServer.statement.isClosed()) {
            throw new SQLException("Statement is not initialized.");
        }
        RegionServer.statement.executeUpdate(sql);
    }

    /**
     * Utility method to extract table name from common SQL commands (basic parsing).
     * Useful if a specific method needs the table name separately from the full SQL.
     *
     * @param sql The SQL command string.
     * @return The extracted table name (without quotes/backticks), or null if parsing fails.
     */
    static String extractTableName(String sql) {
        String sqlLower = sql.trim().toLowerCase();
        String[] parts = sql.trim().split("\\s+"); // Split by whitespace

        try {
            if (sqlLower.startsWith("create table")) {
                if (parts.length > 2) return cleanName(parts[2]);
            } else if (sqlLower.startsWith("drop table")) {
                if (parts.length > 2) return cleanName(parts[2]);
            } else if (sqlLower.startsWith("alter table")) {
                if (parts.length > 2) return cleanName(parts[2]);
            } else if (sqlLower.startsWith("insert into")) {
                if (parts.length > 2) return cleanName(parts[2]);
            } else if (sqlLower.startsWith("update")) {
                if (parts.length > 1) return cleanName(parts[1]);
            } else if (sqlLower.startsWith("delete from")) {
                if (parts.length > 2) return cleanName(parts[2]);
            } else if (sqlLower.startsWith("truncate table")) {
                if (parts.length > 2) return cleanName(parts[2]);
            }
        } catch (Exception e) {
            System.err.println("[ServerClient] Error parsing table name from SQL: " + sql + " - " + e.getMessage());
            return null;
        }
        // Add more cases if needed
        return null; // Indicate parsing failure
    }

    /** Helper to remove potential quotes or backticks and parenthesis from table names */
    private static String cleanName(String name) {
        name = name.replace("`", "").replace("\"", "").replace("'", "");
        int parenIndex = name.indexOf('(');
        if (parenIndex > 0) {
            name = name.substring(0, parenIndex);
        }
        return name.trim();
    }
}