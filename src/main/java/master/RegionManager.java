package master; // Assuming RegionManager is in the master package

import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException; // Added for command sending exception

import regionserver.JdbcUtils; // Assuming needed for potential migration logic details
import socket.SqlSocket; // Assuming needed for potential migration logic details
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

// Import necessary Master components for communication
import master.Master.RegionServerListenerThread; // To access handlers
import master.Master.RegionServerHandler;      // The handler itself


public class RegionManager {
    public static ZooKeeperManager zooKeeperManager;
    public static ZooKeeperUtils zooKeeperUtils = null;
    public static String masterInfo = null;//master node数据

    //region name (IP) ->table names, table name->table load
    public static Map<String, Map<String, Integer>> regionsInfo = null;

    //region name (IP) ->region load
    public static Map<String, Integer> regionsLoad = null;

    public static List<String> toBeCopiedTable = null;

    public static Integer loadsSum = 0, loadsAvg = 0;

    // *** ADDED: Reference to the Master's component responsible for RS communication ***
    // This needs to be set by the Master during initialization.
    public static RegionServerListenerThread regionServerListener = null;

    // Information synchronization, failure recovery, etc.

    /**
     * Initialization, get node data from ZooKeeper.
     */
    public static void init(RegionServerListenerThread listener) throws Exception { // Accept listener reference
        zooKeeperManager = new ZooKeeperManager();
        zooKeeperManager.setWatch("/lss/region_server"); // Watch the parent node for RS joining/leaving
        zooKeeperUtils = zooKeeperManager.zooKeeperUtils;
        regionsInfo = getRegionsInfo();
        toBeCopiedTable = new ArrayList<>();
        sortAndUpdate(); // Initial sort and load calculation
        masterInfo = zooKeeperUtils.getData("/lss/master"); // Get master info if needed

        // Store the reference for sending commands
        regionServerListener = listener;

        // *** Set initial watches on existing region servers ***
        if (regionsInfo != null) {
            for (String regionName : regionsInfo.keySet()) {
                try {
                    // Watch the ephemeral node for disconnections
                    String existPath = "/lss/region_server/" + regionName + "/exist";
                    if (zooKeeperUtils.nodeExists(existPath)) {
                        zooKeeperUtils.setWatch(existPath); // Watch for deletion
                    }
                    // Watch the data node for messages (if using ZK message passing)
                    String dataPath = "/lss/region_server/" + regionName + "/data";
                    if (zooKeeperUtils.nodeExists(dataPath)) {
                        zooKeeperUtils.setWatch(dataPath); // Watch for data changes
                    }
                } catch (Exception e) {
                    System.err.println("[RegionManager] Warning: Failed to set initial watch on existing region: " + regionName + " - " + e.getMessage());
                }
            }
        }
    }

    /**
     * Get information about all current region nodes.
     * Records table names and their loads for each region server.
     */
    public static Map<String, Map<String, Integer>> getRegionsInfo() throws Exception {
        Map<String, Map<String, Integer>> newRegionsInfo = new LinkedHashMap<>();
        List<String> regionNames = zooKeeperUtils.getChildren("/lss/region_server"); // Get all Region IPs/names

        for (String regionName : regionNames) {
            Map<String, Integer> tablesInfo = new LinkedHashMap<>();
            String tableBasePath = "/lss/region_server/" + regionName + "/table";

            // Check if the base table path exists before getting children
            if (zooKeeperUtils.nodeExists(tableBasePath)) {
                List<String> tableNames = zooKeeperUtils.getChildren(tableBasePath);
                for (String tableName : tableNames) {
                    String payloadPath = tableBasePath + "/" + tableName + "/payload";
                    try {
                        // Check if payload node exists before trying to get data
                        if (zooKeeperUtils.nodeExists(payloadPath)) {
                            String payloadData = zooKeeperUtils.getData(payloadPath);
                            Integer load = Integer.valueOf(payloadData);
                            tablesInfo.put(tableName, load);
                        } else {
                            System.err.println("[RegionManager] Warning: Payload node missing for table " + tableName + " on region " + regionName);
                            tablesInfo.put(tableName, 0); // Assign default load 0
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("[RegionManager] Warning: Invalid payload format for table " + tableName + " on region " + regionName + ". Setting load to 0.");
                        tablesInfo.put(tableName, 0);
                    } catch (Exception e) {
                        System.err.println("[RegionManager] Error reading payload for table " + tableName + " on region " + regionName + ": " + e.getMessage());
                        // Optionally skip the table or assign default load
                        tablesInfo.put(tableName, 0);
                    }
                }
            } else {
                System.out.println("[RegionManager] Info: Region " + regionName + " currently has no '/table' node (likely new or no tables yet).");
            }
            newRegionsInfo.put(regionName, sortLoadDsc(tablesInfo)); // Sort tables by load descending
        }
        return newRegionsInfo;
    }

    // --- getRegionsLoad, getLoadsSum remain the same ---
    public static Map<String, Integer> getRegionsLoad() {
        Map<String, Integer> newRegionsLoad = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : regionsInfo.entrySet()) {
            String regionName = entry.getKey();
            Map<String, Integer> tablesInfo = entry.getValue();
            Integer regionLoad = 0;
            for (Map.Entry<String, Integer> tableInfo : tablesInfo.entrySet()) {
                Integer tableLoad = tableInfo.getValue();
                regionLoad += tableLoad;
            }
            newRegionsLoad.put(regionName, regionLoad);
        }
        // Sort regions by load ascending
        return sortLoadAsc(newRegionsLoad);
    }
    public static Integer getLoadsSum() {
        Integer sum = 0;
        for (Map.Entry<String,Integer> entry : regionsLoad.entrySet()) {
            sum += entry.getValue();
        }
        return sum;
    }
    // ---

    /**
     * Sort regionInfo, update regionLoad, loadSum, and loadAvg.
     * Also attempts to recover missing master/slave tables.
     */
    public static void sortAndUpdate() {
        try {
            // Refresh regionsInfo from ZK before sorting and recovery
            regionsInfo = getRegionsInfo();
        } catch (Exception e) {
            System.err.println("[RegionManager] CRITICAL: Failed to refresh regionsInfo from ZooKeeper during sortAndUpdate: " + e.getMessage());
            // Decide how to proceed: use potentially stale data, or abort?
            // For now, proceed with potentially stale data, but log prominently.
            e.printStackTrace();
        }
        regularRecoverData(); // Attempt recovery based on current info
        Map<String, Map<String, Integer>> sortedRegionsInfo = new LinkedHashMap<>();
        for (String regionName : regionsInfo.keySet()) {
            sortedRegionsInfo.put(regionName, sortLoadDsc(regionsInfo.get(regionName)));
        }
        regionsInfo = sortedRegionsInfo;
        regionsLoad = getRegionsLoad(); // Recalculate loads based on potentially recovered info
        loadsSum = getLoadsSum();
        if (!regionsInfo.isEmpty()) {
            loadsAvg = loadsSum / regionsInfo.size();
        } else {
            loadsAvg = 0;
        }
    }

    /**
     * Attempts to recreate missing master or slave tables based on the existing counterpart.
     */
    private static void regularRecoverData() {
        List<String> recovered = new ArrayList<>();
        for (String tableName : new ArrayList<>(toBeCopiedTable)) { // Iterate copy for safety
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";

            // Use findTable (which checks ZK) to determine existence
            ResType masterStatus = findTable(masterTableName);
            ResType slaveStatus = findTable(slaveTableName);

            if (masterStatus == ResType.FIND_TABLE_NO_EXISTS && slaveStatus == ResType.FIND_SUCCESS) {
                System.out.println("[RegionManager] Recovering missing master table: " + masterTableName + " from slave: " + slaveTableName);
                if (addSameTable(masterTableName, slaveTableName)) {
                    recovered.add(tableName); // Mark for removal from toBeCopiedTable
                }
            } else if (slaveStatus == ResType.FIND_TABLE_NO_EXISTS && masterStatus == ResType.FIND_SUCCESS) {
                System.out.println("[RegionManager] Recovering missing slave table: " + slaveTableName + " from master: " + masterTableName);
                if (addSameTable(slaveTableName, masterTableName)) {
                    recovered.add(tableName); // Mark for removal from toBeCopiedTable
                }
            } else if (masterStatus == ResType.FIND_SUCCESS && slaveStatus == ResType.FIND_SUCCESS) {
                // Both exist, no longer needs copying
                recovered.add(tableName);
            } else {
                // Both missing or other error, cannot recover currently
                System.out.println("[RegionManager] Cannot recover " + tableName + ", master exists: " + (masterStatus == ResType.FIND_SUCCESS) + ", slave exists: " + (slaveStatus == ResType.FIND_SUCCESS));
            }
        }
        toBeCopiedTable.removeAll(recovered);
    }

    /**
     * Adds a new table (addTableName) by copying data from an existing template table (templateTableName).
     * Selects the least loaded appropriate region server for the new table.
     *
     * @param addTableName      The name of the table to create.
     * @param templateTableName The name of the existing table to copy data from.
     * @return true if the ZK update and command sending were initiated, false otherwise.
     */
    private static boolean addSameTable(String addTableName, String templateTableName) {
        // Find the region server hosting the template table
        String templateRegionName = zooKeeperManager.getRegionServer(templateTableName);
        if (templateRegionName == null || !regionsInfo.containsKey(templateRegionName) || !regionsInfo.get(templateRegionName).containsKey(templateTableName)) {
            System.err.println("[RegionManager] Cannot add table " + addTableName + ": Template table " + templateTableName + " or its region not found in current info.");
            return false;
        }
        Integer load = regionsInfo.get(templateRegionName).get(templateTableName);

        // Find the least loaded region server that can host the new table
        String addRegionName = getLeastRegionName(templateTableName); // Ensures master/slave separation
        if (addRegionName == null) {
            System.err.println("[RegionManager] Cannot add table " + addTableName + ": No suitable region server found.");
            return false;
        }

        // Update local info first (optimistic)
        if (!regionsInfo.containsKey(addRegionName)) {
            regionsInfo.put(addRegionName, new LinkedHashMap<>());
        }
        regionsInfo.get(addRegionName).put(addTableName, load); // Add with template's load

        // Notify ZooKeeper about the new table location
        boolean zkSuccess = zooKeeperManager.addTable(addRegionName, new TableInform(addTableName, load));
        if (!zkSuccess) {
            System.err.println("[RegionManager] Failed to add table " + addTableName + " to ZooKeeper for region " + addRegionName);
            // Rollback local info change?
            if (regionsInfo.containsKey(addRegionName)) {
                regionsInfo.get(addRegionName).remove(addTableName);
            }
            return false;
        }

        // *** TODO Implementation: Trigger the actual data copy ***
        // This involves telling the 'addRegionName' server to copy data from 'templateRegionName'.
        // We can use the ServerMaster.dumpTable logic.
        // The Master needs to get connection details for both servers from ZK.
        boolean commandSent = sendCopyTableCommand(templateRegionName, addRegionName, templateTableName, addTableName);
        if (!commandSent) {
            System.err.println("[RegionManager] Failed to send copy command from " + templateRegionName + " to " + addRegionName + " for table " + addTableName);
            // Consider cleanup: remove ZK node, rollback local info? Complex recovery needed.
            zooKeeperManager.deleteTable(addTableName); // Attempt to remove ZK entry
            if (regionsInfo.containsKey(addRegionName)) {
                regionsInfo.get(addRegionName).remove(addTableName);
            }
            return false;
        }

        System.out.println("[RegionManager] Initiated copy for table " + addTableName + " on region " + addRegionName + " from template " + templateTableName + " on region " + templateRegionName);
        // Don't call sortAndUpdate here, let the caller handle it after potentially multiple operations.
        return true;
    }


    // --- sortLoadAsc, sortLoadDsc, getLeastRegionName, getLargestRegionName, getLargerTableInfo remain the same ---
    public static Map<String, Integer> sortLoadAsc(Map<String, Integer> loads){
        return loads.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue()) // Ascending
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new // Keep order
                ));
    }
    public static Map<String, Integer> sortLoadDsc(Map<String, Integer> loads){
        return loads.entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())// Descending
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new // Keep order
                ));
    }
    public static String getLeastRegionName(String tableName) {
        // sortAndUpdate(); // Avoid recursive calls, ensure caller sorts if needed
        String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
        String slaveTableName = masterTableName + "_slave";

        for (Map.Entry<String, Integer> entry : regionsLoad.entrySet()) { // regionsLoad is sorted ascending
            String regionServer = entry.getKey();
            Map<String, Integer> tables = regionsInfo.get(regionServer);
            if (tables == null) continue; // Skip if region has no info (shouldn't happen if sync is correct)
            if (!tables.containsKey(masterTableName) && !tables.containsKey(slaveTableName)) {
                return regionServer;
            }
        }
        return null; // No suitable region found
    }
    public static String getLargestRegionName(Map<String, Integer> existTablesInfo){
        // sortAndUpdate(); // Avoid recursive calls
        if (!regionsLoad.isEmpty()) {
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(regionsLoad.entrySet()); // Already sorted Asc
            for(int i = entries.size() - 1; i>=0; i--){ // Iterate from largest load
                Map.Entry<String, Integer> largestRegion = entries.get(i);
                String regionName = largestRegion.getKey();
                Map<String, Integer> tables = regionsInfo.get(regionName);
                if (tables == null || tables.isEmpty()) continue; // Skip empty regions
                if(getLargerTableInfo(regionName, 0, existTablesInfo)!=null){ // Check if any table can be moved
                    return regionName;
                }
            }
        }
        return null;
    }
    public static Map<String, Integer> getLargerTableInfo(String regionName, Integer load, Map<String, Integer> existTablesInfo) {
        Map<String, Integer> tablesInfo = regionsInfo.get(regionName);
        if (tablesInfo == null || tablesInfo.isEmpty()) {
            return null;
        }
        // tablesInfo is sorted descending by load
        Map<String, Integer> result = null;
        int bestLoadFound = -1; // Track the load of the best candidate found so far

        for (Map.Entry<String, Integer> entry : tablesInfo.entrySet()) {
            Integer tableLoad = entry.getValue();
            String tableName = entry.getKey();
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";

            // Check if the counterpart already exists in the target region's map
            boolean counterpartExists = false;
            if (existTablesInfo != null) {
                counterpartExists = existTablesInfo.containsKey(masterTableName) || existTablesInfo.containsKey(slaveTableName);
            }

            if (!counterpartExists) { // Can move this table
                if (tableLoad >= load) { // Meets minimum load requirement
                    // If it's the first suitable table, or smaller than the previous best (but still >= load)
                    if (result == null || tableLoad < bestLoadFound) {
                        result = new LinkedHashMap<>();
                        result.put(tableName, tableLoad);
                        bestLoadFound = tableLoad; // Update best load found
                    }
                } else {
                    // Since tablesInfo is sorted descending, if we encounter a table with load < required load,
                    // no subsequent table will meet the requirement either.
                    break; // Optimization: stop searching
                }
            }
        }
        // If no table >= load was found, but we need *any* table, return the largest available one
        if (result == null && load == 0 && !tablesInfo.isEmpty()) {
            for (Map.Entry<String, Integer> entry : tablesInfo.entrySet()) {
                String tableName = entry.getKey();
                Integer tableLoad = entry.getValue();
                String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
                String slaveTableName = masterTableName + "_slave";
                boolean counterpartExists = false;
                if (existTablesInfo != null) {
                    counterpartExists = existTablesInfo.containsKey(masterTableName) || existTablesInfo.containsKey(slaveTableName);
                }
                if (!counterpartExists) {
                    result = new LinkedHashMap<>();
                    result.put(tableName, tableLoad);
                    break; // Return the first available (largest) one
                }
            }
        }

        return result;
    }
    // ---

    // --- ZK Watch Callbacks (addRegion, delRegion) ---
    /**
     * Handles the addition of a new region server detected by ZooKeeper watcher.
     * Balances load by migrating tables from the most loaded servers to the new server.
     */
    public static ResType addRegion(List<String> nowRegionNames) {
        System.out.println("[RegionManager] Detected RegionServer change. Current list: " + nowRegionNames);
        String addRegionName = null;
        for (String regionName : nowRegionNames) {
            if (!regionsInfo.containsKey(regionName)) {
                addRegionName = regionName;
                // *** Set watches on the new region server ***
                try {
                    String existPath = "/lss/region_server/" + regionName + "/exist";
                    if (zooKeeperUtils.nodeExists(existPath)) {
                        zooKeeperUtils.setWatch(existPath); // Watch for deletion
                    }
                    String dataPath = "/lss/region_server/" + regionName + "/data";
                    if (zooKeeperUtils.nodeExists(dataPath)) {
                        zooKeeperUtils.setWatch(dataPath); // Watch for data changes
                    }
                } catch (Exception e) {
                    System.err.println("[RegionManager] Warning: Failed to set initial watch on new region: " + regionName + " - " + e.getMessage());
                }
                break;
            }
        }

        if (addRegionName == null) {
            System.out.println("[RegionManager] No new region detected or already processed.");
            return ResType.ADD_REGION_ALREADY_EXISTS; // Or some other status like NO_CHANGE
        }
        if (regionsInfo.containsKey(addRegionName)) { // Double check just in case
            System.out.println("[RegionManager] Region " + addRegionName + " already exists in local info.");
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }

        System.out.println("[RegionManager] Adding new region: " + addRegionName);
        // Add the new region with an empty table list to our local info first
        regionsInfo.put(addRegionName, new LinkedHashMap<>());
        sortAndUpdate(); // Recalculate average load including the new server

        Integer avgLoad = loadsAvg; // Use the recalculated average
        Map<String, Integer> addTablesInfo = new LinkedHashMap<>();
        Integer addRegionLoadSum = 0;

        System.out.println("[RegionManager] Balancing load for new region " + addRegionName + ". Target avg load: " + avgLoad);

        while (addRegionLoadSum < avgLoad) {
            sortAndUpdate(); // Update loads after potential previous migration
            String largestRegionName = getLargestRegionName(addTablesInfo); // Find suitable source region

            if (largestRegionName == null) {
                System.out.println("[RegionManager] No suitable source region found to migrate tables from.");
                break; // Cannot find any more tables to migrate
            }

            Map<String, Integer> largestRegionTables = regionsInfo.get(largestRegionName);
            if (largestRegionTables == null || largestRegionTables.size() <= 1) { // Leave at least one table? Or allow empty? Let's allow moving the last table.
                if (largestRegionTables == null || largestRegionTables.isEmpty()) {
                    System.out.println("[RegionManager] Source region " + largestRegionName + " has no tables to migrate.");
                    // This shouldn't happen if getLargestRegionName worked correctly, but check anyway.
                    // We need to remove this region from consideration if it's truly empty and try again.
                    // This indicates a potential logic issue needing investigation. For now, break.
                    break;
                }
            }

            // Determine the load to aim for in the move
            Integer currentLargestLoad = regionsLoad.get(largestRegionName);
            Integer idealLoadToMove = Math.max(0, Math.min(avgLoad - addRegionLoadSum, currentLargestLoad - avgLoad));

            // Find the best table to move (closest to idealLoadToMove, without causing counterpart conflict)
            Map<String, Integer> tableToMoveInfo = getLargerTableInfo(largestRegionName, idealLoadToMove, addTablesInfo);

            if (tableToMoveInfo == null) {
                // If no table >= idealLoadToMove found, try finding the smallest possible table > 0 to move
                tableToMoveInfo = getLargerTableInfo(largestRegionName, 0, addTablesInfo); // Find smallest > 0
                if (tableToMoveInfo == null && largestRegionTables != null && !largestRegionTables.isEmpty()) {
                    // Still null? Try the absolute largest if load target was 0
                    tableToMoveInfo = getLargerTableInfo(largestRegionName, 0, addTablesInfo); // Re-check logic
                }
            }


            if (tableToMoveInfo == null) {
                System.out.println("[RegionManager] No suitable table found to migrate from " + largestRegionName + " to " + addRegionName);
                break; // Cannot find any table to move from this source
            }

            String tableNameToMove = tableToMoveInfo.keySet().iterator().next();
            Integer tableLoadToMove = tableToMoveInfo.get(tableNameToMove);

            System.out.println("[RegionManager] Selected table '" + tableNameToMove + "' (load " + tableLoadToMove + ") from " + largestRegionName + " to migrate to " + addRegionName);

            // *** TODO Implementation: Trigger actual table migration ***
            // Use ServerMaster.migrateTable logic (or similar command)
            boolean migrateCommandSent = sendMigrateTableCommand(largestRegionName, addRegionName, tableNameToMove);

            if (migrateCommandSent) {
                System.out.println("[RegionManager] Migration command sent for table " + tableNameToMove);
                // Update local info optimistically (assuming migration will succeed)
                // Remove from source
                regionsInfo.get(largestRegionName).remove(tableNameToMove);
                // Add to target
                addTablesInfo.put(tableNameToMove, tableLoadToMove);
                addRegionLoadSum += tableLoadToMove;

                // IMPORTANT: ZK update for table location is handled by the migration command/process itself or Master needs to confirm and do it.
                // If sendMigrateTableCommand handles ZK update: okay.
                // If not: Master needs to be notified upon success to update ZK.

            } else {
                System.err.println("[RegionManager] Failed to send migration command for table " + tableNameToMove + ". Stopping balancing for now.");
                // Don't update local info if command failed
                break; // Stop balancing if command fails
            }
        } // End while loop

        // Update the final table list for the added region in the main map
        regionsInfo.put(addRegionName, addTablesInfo);
        sortAndUpdate(); // Final update after balancing finishes
        System.out.println("[RegionManager] Finished load balancing for new region " + addRegionName + ". Final load: " + regionsLoad.getOrDefault(addRegionName, 0));
        return ResType.ADD_REGION_SUCCESS;
    }

    /**
     * Handles the deletion of a region server detected by ZooKeeper watcher.
     * Redistributes tables from the deleted server to the least loaded available servers.
     */
    public static ResType delRegion(String delRegionName) {
        System.out.println("[RegionManager] Detected deletion/disconnection of region: " + delRegionName);
        if (!regionsInfo.containsKey(delRegionName)) {
            System.err.println("[RegionManager] Region " + delRegionName + " not found in local info. Already processed or error.");
            return ResType.DROP_REGION_NO_EXISTS;
        }

        Map<String, Integer> delTablesInfo = new HashMap<>(regionsInfo.get(delRegionName)); // Copy tables to redistribute
        regionsInfo.remove(delRegionName); // Remove from active regions immediately

        System.out.println("[RegionManager] Redistributing " + delTablesInfo.size() + " tables from deleted region " + delRegionName);

        if (regionsInfo.isEmpty()) {
            System.err.println("[RegionManager] CRITICAL: No remaining region servers to redistribute tables to!");
            // Lost data? Need manual recovery or backup strategy.
            return ResType.DROP_REGION_FAILURE; // Indicate failure
        }

        // Sort tables to potentially redistribute larger ones first? Or just iterate? Iterate for now.
        for (Map.Entry<String, Integer> tableEntry : delTablesInfo.entrySet()) {
            String tableName = tableEntry.getKey();
            Integer tableLoad = tableEntry.getValue();

            sortAndUpdate(); // Recalculate loads before finding target for each table
            String leastRegionName = getLeastRegionName(tableName); // Find best target, respecting master/slave constraints

            if (leastRegionName == null) {
                System.err.println("[RegionManager] CRITICAL: Could not find a suitable region server to migrate table '" + tableName + "' to! Data might be lost.");
                // Skip this table, maybe try again later? Log for manual intervention.
                continue; // Move to next table
            }

            System.out.println("[RegionManager] Migrating table '" + tableName + "' (load " + tableLoad + ") from deleted " + delRegionName + " to " + leastRegionName);

            // Update local info optimistically
            if (!regionsInfo.containsKey(leastRegionName)) {
                regionsInfo.put(leastRegionName, new LinkedHashMap<>());
            }
            regionsInfo.get(leastRegionName).put(tableName, tableLoad);

            // Update ZooKeeper (add table to new region)
            boolean zkSuccess = zooKeeperManager.addTable(leastRegionName, new TableInform(tableName, tableLoad));
            if (!zkSuccess) {
                System.err.println("[RegionManager] Failed to update ZooKeeper for migrated table " + tableName + " to region " + leastRegionName);
                // Rollback local info? Requires careful handling.
                if (regionsInfo.containsKey(leastRegionName)) {
                    regionsInfo.get(leastRegionName).remove(tableName);
                }
                continue; // Skip trying to trigger data move if ZK failed
            }

            // *** TODO Implementation: Trigger actual data copy ***
            // This is tricky because the source server is GONE.
            // We need a backup/replication strategy. If slaves exist, maybe promote one?
            // If no slave, data might be lost unless recovered from backup/log.
            // For now, assume a slave might exist OR we rely on backups.
            // If a slave exists, we just updated ZK - the system might self-heal via regularRecoverData later.
            // If no slave, we log the issue.
            String masterTable = tableName.endsWith("_slave") ? tableName.substring(0,tableName.length()-6) : tableName;
            String slaveTable = masterTable + "_slave";
            if (findTable(slaveTable) != ResType.FIND_SUCCESS && tableName.equals(masterTable)) {
                System.err.println("[RegionManager] WARNING: Master table '" + tableName + "' from deleted region " + delRegionName + " has no available slave. Data must be restored from backup to region " + leastRegionName);
            } else if (findTable(masterTable) != ResType.FIND_SUCCESS && tableName.equals(slaveTable)) {
                System.err.println("[RegionManager] WARNING: Slave table '" + tableName + "' from deleted region " + delRegionName + " has no available master. Data must be restored from backup to region " + leastRegionName);
            } else {
                System.out.println("[RegionManager] Table '" + tableName + "' assigned to region " + leastRegionName + ". Replication/recovery should handle data restoration if needed.");
            }

            // No explicit data move command here as source is gone. ZK update is key.

        } // End for loop

        sortAndUpdate(); // Update loads after redistribution
        System.out.println("[RegionManager] Finished redistributing tables from deleted region " + delRegionName);
        return ResType.DROP_REGION_SUCCESS;
    }
    // ---

    // --- Table Operations (CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ALTER, TRUNCATE) ---

    /**
     * Creates a table and its slave replica on different region servers.
     */
    public static List<ResType> createTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.CREATE_TABLE_FAILURE, ResType.CREATE_TABLE_FAILURE)); // Default to failure

        // Check if master already exists
        if (checkList.get(0) == ResType.FIND_SUCCESS) {
            results.set(0, ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        // Check if slave already exists
        if (checkList.get(1) == ResType.FIND_SUCCESS) {
            results.set(1, ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        // If either already exists, return immediately
        if (results.get(0) == ResType.CREATE_TABLE_ALREADY_EXISTS || results.get(1) == ResType.CREATE_TABLE_ALREADY_EXISTS) {
            return results;
        }

        sortAndUpdate(); // Ensure loads are up-to-date before selecting regions

        // Find region for master table
        String masterRegionName = getLeastRegionName(tableName + "_slave"); // Ensure it doesn't clash with potential future slave
        if (masterRegionName != null) {
            results.set(0, createTable(masterRegionName, tableName, sql));
        } else {
            System.err.println("[RegionManager] Cannot create master table " + tableName + ": No suitable region found.");
        }

        // Find region for slave table (must be different from master)
        String slaveTableName = tableName + "_slave";
        String slaveRegionName = getLeastRegionName(tableName); // Ensure it doesn't clash with master
        if (slaveRegionName != null) {
            // Modify SQL for slave table name
            String slaveSql = sql.replaceFirst("(?i)create table\\s+`?" + tableName + "`?", "CREATE TABLE `" + slaveTableName + "`"); // Basic replace
            results.set(1, createTable(slaveRegionName, slaveTableName, slaveSql));

            // If slave creation succeeded, trigger copy from master
            if (results.get(0) == ResType.CREATE_TABLE_SUCCESS && results.get(1) == ResType.CREATE_TABLE_SUCCESS) {
                sendCopyTableCommand(masterRegionName, slaveRegionName, tableName, slaveTableName);
            }

        } else {
            System.err.println("[RegionManager] Cannot create slave table " + slaveTableName + ": No suitable region found.");
        }


        // If slave failed but master succeeded, add slave to recovery list
        if (results.get(0) == ResType.CREATE_TABLE_SUCCESS && results.get(1) != ResType.CREATE_TABLE_SUCCESS) {
            toBeCopiedTable.add(slaveTableName);
            System.out.println("[RegionManager] Slave creation failed for " + slaveTableName + ", added to recovery list.");
        }
        // If master failed but slave somehow succeeded (unlikely), add master to recovery?
        else if (results.get(0) != ResType.CREATE_TABLE_SUCCESS && results.get(1) == ResType.CREATE_TABLE_SUCCESS) {
            toBeCopiedTable.add(tableName);
            System.out.println("[RegionManager] Master creation failed for " + tableName + ", added to recovery list.");
        }

        sortAndUpdate(); // Update info after operations
        return results;
    }

    /**
     * Helper to create a single table on a specific region server.
     * Updates ZK and sends command to RegionServer.
     */
    private static ResType createTable(String regionName, String tableName, String sql) {
        System.out.println("[RegionManager] Attempting to create table " + tableName + " on region " + regionName);
        // Update ZK first
        if (zooKeeperManager.addTable(regionName, new TableInform(tableName, 0))) { // Initial load 0
            // Update local cache
            if (!regionsInfo.containsKey(regionName)) regionsInfo.put(regionName, new LinkedHashMap<>());
            regionsInfo.get(regionName).put(tableName, 0);

            // Send command to RegionServer
            boolean success = sendSqlCommandToRegion(regionName, sql);
            if (success) {
                System.out.println("[RegionManager] Successfully sent CREATE command for table " + tableName + " to region " + regionName);
                return ResType.CREATE_TABLE_SUCCESS;
            } else {
                System.err.println("[RegionManager] Failed to send CREATE command for table " + tableName + " to region " + regionName);
                // Rollback ZK and local cache?
                zooKeeperManager.deleteTable(tableName);
                if(regionsInfo.containsKey(regionName)) regionsInfo.get(regionName).remove(tableName);
                return ResType.CREATE_TABLE_FAILURE;
            }
        } else {
            System.err.println("[RegionManager] Failed to add table " + tableName + " to ZooKeeper for region " + regionName);
            return ResType.CREATE_TABLE_FAILURE;
        }
    }


    /**
     * Drops a table and its slave replica from their respective region servers.
     */
    public static List<ResType> dropTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.DROP_TABLE_FAILURE, ResType.DROP_TABLE_FAILURE));

        if (checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS && checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS) {
            results.set(0, ResType.DROP_TABLE_NO_EXISTS);
            results.set(1, ResType.DROP_TABLE_NO_EXISTS);
            return results; // Both don't exist
        }

        // Drop master if it exists
        if (checkList.get(0) == ResType.FIND_SUCCESS) {
            results.set(0, dropTable(tableName, sql));
        } else {
            results.set(0, ResType.DROP_TABLE_NO_EXISTS); // Doesn't exist, so "success" in terms of goal
            toBeCopiedTable.remove(tableName); // Remove from recovery if it was there
        }

        // Drop slave if it exists
        String slaveTableName = tableName + "_slave";
        String slaveSql = sql.replaceFirst("(?i)drop table\\s+(if exists\\s+)?`?" + tableName + "`?", "DROP TABLE IF EXISTS `" + slaveTableName + "`");
        if (checkList.get(1) == ResType.FIND_SUCCESS) {
            results.set(1, dropTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.DROP_TABLE_NO_EXISTS); // Doesn't exist
            toBeCopiedTable.remove(slaveTableName); // Remove from recovery
        }

        sortAndUpdate(); // Update info
        return results;
    }

    /**
     * Helper to drop a single table from its region server.
     * Updates ZK and sends command to RegionServer.
     */
    public static ResType dropTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) {
            System.out.println("[RegionManager] Cannot drop table " + tableName + ": Not found in ZooKeeper.");
            return ResType.DROP_TABLE_NO_EXISTS; // Or FAILURE if expected to exist
        }
        System.out.println("[RegionManager] Attempting to drop table " + tableName + " from region " + regionName);

        // Send command to RegionServer FIRST
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);
        if (commandSuccess) {
            System.out.println("[RegionManager] Successfully sent DROP command for table " + tableName + " to region " + regionName);
            // If command succeeded, update ZK and local cache
            if (zooKeeperManager.deleteTable(tableName)) {
                if (regionsInfo.containsKey(regionName)) {
                    regionsInfo.get(regionName).remove(tableName);
                }
                return ResType.DROP_TABLE_SUCCESS;
            } else {
                System.err.println("[RegionManager] Failed to delete table " + tableName + " from ZooKeeper after successful drop command.");
                // Data is dropped, but ZK is inconsistent. Requires attention.
                return ResType.DROP_TABLE_FAILURE; // Indicate ZK failure
            }
        } else {
            System.err.println("[RegionManager] Failed to send DROP command for table " + tableName + " to region " + regionName);
            return ResType.DROP_TABLE_FAILURE;
        }
    }


    /**
     * Selects the best region server (master or slave) to handle a SELECT query.
     * Checks for table existence and recovers if necessary.
     * Returns information needed by the client to connect directly.
     */
    public static SelectInfo selectTable(List<String> tableNames, String sql) {
        System.out.println("[RegionManager] Processing SELECT for tables: " + tableNames);
        // 1. Check existence and trigger recovery if needed
        if (!checkAndRecoverData(tableNames)) {
            System.err.println("[RegionManager] SELECT failed: One or more required tables/replicas could not be found or recovered.");
            return new SelectInfo(false); // Indicate failure
        }

        // 2. Find regions hosting the tables (prefer master, fallback to slave)
        Map<String, String> tableToRegionMap = new HashMap<>(); // tableName -> regionName
        Map<String, String> tableToActualNameMap = new HashMap<>(); // originalName -> actualNameInRegion (e.g., t1 -> t1_slave)
        boolean foundAll = true;
        for (String tableName : tableNames) {
            String masterRegion = zooKeeperManager.getRegionServer(tableName);
            String slaveRegion = zooKeeperManager.getRegionServer(tableName + "_slave");

            if (masterRegion != null) {
                tableToRegionMap.put(tableName, masterRegion);
                tableToActualNameMap.put(tableName, tableName);
            } else if (slaveRegion != null) {
                tableToRegionMap.put(tableName, slaveRegion);
                tableToActualNameMap.put(tableName, tableName + "_slave");
                System.out.println("[RegionManager] Using slave table " + tableName + "_slave" + " on region " + slaveRegion + " for SELECT.");
            } else {
                System.err.println("[RegionManager] SELECT failed: Could not find region for table " + tableName + " even after recovery check.");
                foundAll = false;
                break; // No point continuing if a table is missing
            }
        }

        if (!foundAll) {
            return new SelectInfo(false);
        }

        // 3. Choose the target region (e.g., least loaded region among those involved)
        // Simple strategy: Use the region of the first table. More complex strategies possible.
        String targetRegionName = tableToRegionMap.get(tableNames.get(0));

        // 4. Modify SQL if slave tables are being used
        String finalSql = sql;
        for (String originalName : tableToActualNameMap.keySet()) {
            String actualName = tableToActualNameMap.get(originalName);
            if (!originalName.equals(actualName)) {
                // Careful regex to replace whole word only
                finalSql = finalSql.replaceAll("\\b" + originalName + "\\b", actualName);
            }
        }
        System.out.println("[RegionManager] Final SQL for region " + targetRegionName + ": " + finalSql);


        // 5. Get target region connection info (IP:ClientPort)
        String targetRegionInfo = getRegionConnectionString(targetRegionName);
        if (targetRegionInfo == null) {
            System.err.println("[RegionManager] SELECT failed: Could not get connection string for target region " + targetRegionName);
            return new SelectInfo(false);
        }


        // Return SelectInfo containing the target address and final SQL
        return new SelectInfo(true, targetRegionInfo, finalSql);
    }

    /**
     * Checks if master/slave pairs exist for the given tables.
     * Attempts recovery by copying if one part is missing.
     *
     * @param tableNames List of original table names.
     * @return true if all tables (or their recoverable counterparts) exist, false otherwise.
     */
    private static boolean checkAndRecoverData(List<String> tableNames) {
        boolean allOk = true;
        for (String tableName : tableNames) {
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";

            ResType masterStatus = findTable(masterTableName);
            ResType slaveStatus = findTable(slaveTableName);

            if (masterStatus == ResType.FIND_TABLE_NO_EXISTS && slaveStatus == ResType.FIND_TABLE_NO_EXISTS) {
                System.err.println("[RegionManager] Check failed: Neither master (" + masterTableName + ") nor slave (" + slaveTableName + ") found.");
                allOk = false;
                break; // Cannot proceed if both are missing
            } else if (masterStatus == ResType.FIND_TABLE_NO_EXISTS) { // Master missing, slave exists
                System.out.println("[RegionManager] Check: Master " + masterTableName + " missing, attempting recovery from slave " + slaveTableName);
                if (!addSameTable(masterTableName, slaveTableName)) {
                    System.err.println("[RegionManager] Check failed: Recovery of master " + masterTableName + " failed.");
                    allOk = false;
                    break;
                }
                // Re-check after recovery attempt
                if (findTable(masterTableName) == ResType.FIND_TABLE_NO_EXISTS) {
                    System.err.println("[RegionManager] Check failed: Master " + masterTableName + " still missing after recovery attempt.");
                    allOk = false;
                    break;
                }
            } else if (slaveStatus == ResType.FIND_TABLE_NO_EXISTS) { // Slave missing, master exists
                System.out.println("[RegionManager] Check: Slave " + slaveTableName + " missing, attempting recovery from master " + masterTableName);
                if (!addSameTable(slaveTableName, masterTableName)) {
                    System.err.println("[RegionManager] Check failed: Recovery of slave " + slaveTableName + " failed.");
                    // Slave missing might be acceptable for SELECT depending on policy, but let's assume not for now.
                    // Or maybe just log a warning and continue? For now, treat as failure.
                    allOk = false;
                    break;
                }
                // Re-check after recovery attempt
                if (findTable(slaveTableName) == ResType.FIND_TABLE_NO_EXISTS) {
                    System.err.println("[RegionManager] Check failed: Slave " + slaveTableName + " still missing after recovery attempt.");
                    allOk = false;
                    break;
                }
            }
            // If both exist, continue to the next table
        }
        if (allOk) {
            System.out.println("[RegionManager] Check & Recovery completed successfully for tables: " + tableNames);
        }
        // sortAndUpdate(); // Update local info after potential recovery
        return allOk;
    }


    // --- getRegionsWithLoad, getLargestLoadRegion, replaceSql are simplified or replaced by logic in selectTable ---


    /**
     * Handles INSERT operations for a table and its slave.
     */
    public static List<ResType> accTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.INSERT_FAILURE, ResType.INSERT_FAILURE));
        boolean masterExists = checkList.get(0) == ResType.FIND_SUCCESS;
        boolean slaveExists = checkList.get(1) == ResType.FIND_SUCCESS;

        if (!masterExists && !slaveExists) {
            results.set(0, ResType.INSERT_TABLE_NO_EXISTS);
            results.set(1, ResType.INSERT_TABLE_NO_EXISTS);
            return results;
        }

        // Execute on master if it exists
        if (masterExists) {
            results.set(0, accTable(tableName, sql));
        } else {
            results.set(0, ResType.INSERT_TABLE_NO_EXISTS); // Master doesn't exist
        }

        // Execute on slave if it exists
        String slaveTableName = tableName + "_slave";
        String slaveSql = sql.replaceFirst("(?i)insert into\\s+`?" + tableName + "`?", "INSERT INTO `" + slaveTableName + "`");
        if (slaveExists) {
            results.set(1, accTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.INSERT_TABLE_NO_EXISTS); // Slave doesn't exist
        }

        // If one failed, mark the other for recovery? Or just report failure? Reporting failure is simpler.
        if (results.get(0) != ResType.INSERT_SUCCESS && results.get(1) == ResType.INSERT_SUCCESS) {
            // Master failed, slave succeeded - inconsistency
            System.err.println("[RegionManager] Inconsistency: INSERT failed on master " + tableName + " but succeeded on slave " + slaveTableName);
        } else if (results.get(0) == ResType.INSERT_SUCCESS && results.get(1) != ResType.INSERT_SUCCESS) {
            // Master succeeded, slave failed
            System.err.println("[RegionManager] Inconsistency: INSERT succeeded on master " + tableName + " but failed on slave " + slaveTableName);
            // Add slave to recovery list?
            // toBeCopiedTable.add(slaveTableName);
        }

        sortAndUpdate();
        return results;
    }

    /** Helper for single table INSERT */
    private static ResType accTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.INSERT_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending INSERT for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            // Update payload in ZK
            if (zooKeeperManager.accTablePayload(tableName)) {
                // Update local cache
                if (regionsInfo.containsKey(regionName) && regionsInfo.get(regionName).containsKey(tableName)) {
                    regionsInfo.get(regionName).compute(tableName, (k, v) -> (v == null) ? 1 : v + 1);
                }
                return ResType.INSERT_SUCCESS;
            } else {
                System.err.println("[RegionManager] INSERT command succeeded for " + tableName + " but failed to update ZK payload.");
                return ResType.INSERT_FAILURE; // Indicate ZK failure
            }
        } else {
            System.err.println("[RegionManager] Failed to send INSERT command for table " + tableName + " to region " + regionName);
            return ResType.INSERT_FAILURE;
        }
    }


    // --- DELETE ---
    public static List<ResType> decTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.DELECT_FAILURE, ResType.DELECT_FAILURE));
        boolean masterExists = checkList.get(0) == ResType.FIND_SUCCESS;
        boolean slaveExists = checkList.get(1) == ResType.FIND_SUCCESS;

        if (!masterExists && !slaveExists) {
            results.set(0, ResType.DELECT_TABLE_NO_EXISTS);
            results.set(1, ResType.DELECT_TABLE_NO_EXISTS);
            return results;
        }

        // Execute on master
        if (masterExists) {
            results.set(0, decTable(tableName, sql));
        } else {
            results.set(0, ResType.DELECT_TABLE_NO_EXISTS);
        }

        // Execute on slave
        String slaveTableName = tableName + "_slave";
        // Basic replace for DELETE FROM `tableName` ...
        String slaveSql = sql.replaceFirst("(?i)delete from\\s+`?" + tableName + "`?", "DELETE FROM `" + slaveTableName + "`");
        if (slaveExists) {
            results.set(1, decTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.DELECT_TABLE_NO_EXISTS);
        }

        // Handle inconsistencies similar to INSERT
        if (results.get(0) != ResType.DELECT_SUCCESS && results.get(1) == ResType.DELECT_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: DELETE failed on master " + tableName + " but succeeded on slave " + slaveTableName);
        } else if (results.get(0) == ResType.DELECT_SUCCESS && results.get(1) != ResType.DELECT_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: DELETE succeeded on master " + tableName + " but failed on slave " + slaveTableName);
        }

        sortAndUpdate();
        return results;
    }

    /** Helper for single table DELETE */
    private static ResType decTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.DELECT_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending DELETE for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            // Update payload in ZK (assuming DELETE reduces load)
            // NOTE: Accurately tracking load changes from DELETE/UPDATE is complex.
            // Decrementing might not be correct. Let's skip ZK load update for DELETE for now,
            // unless a specific row count is returned.
            // if (zooKeeperManager.decTablePayload(tableName)) { ... }
            return ResType.DELECT_SUCCESS;
        } else {
            System.err.println("[RegionManager] Failed to send DELETE command for table " + tableName + " to region " + regionName);
            return ResType.DELECT_FAILURE;
        }
    }


    // --- UPDATE ---
    public static List<ResType> updateTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.UPDATE_FAILURE, ResType.UPDATE_FAILURE));
        boolean masterExists = checkList.get(0) == ResType.FIND_SUCCESS;
        boolean slaveExists = checkList.get(1) == ResType.FIND_SUCCESS;

        if (!masterExists && !slaveExists) {
            results.set(0, ResType.UPDATE_TABLE_NO_EXISTS);
            results.set(1, ResType.UPDATE_TABLE_NO_EXISTS);
            return results;
        }

        // Execute on master
        if (masterExists) {
            results.set(0, updateTable(tableName, sql));
        } else {
            results.set(0, ResType.UPDATE_TABLE_NO_EXISTS);
        }

        // Execute on slave
        String slaveTableName = tableName + "_slave";
        // Basic replace for UPDATE `tableName` SET...
        String slaveSql = sql.replaceFirst("(?i)update\\s+`?" + tableName + "`?", "UPDATE `" + slaveTableName + "`");
        if (slaveExists) {
            results.set(1, updateTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.UPDATE_TABLE_NO_EXISTS);
        }

        // Handle inconsistencies
        if (results.get(0) != ResType.UPDATE_SUCCESS && results.get(1) == ResType.UPDATE_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: UPDATE failed on master " + tableName + " but succeeded on slave " + slaveTableName);
        } else if (results.get(0) == ResType.UPDATE_SUCCESS && results.get(1) != ResType.UPDATE_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: UPDATE succeeded on master " + tableName + " but failed on slave " + slaveTableName);
        }

        // sortAndUpdate(); // Load doesn't change reliably with UPDATE, no sort needed here
        return results;
    }

    /** Helper for single table UPDATE */
    private static ResType updateTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.UPDATE_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending UPDATE for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            // Load calculation for UPDATE is complex. Don't update ZK payload here.
            return ResType.UPDATE_SUCCESS;
        } else {
            System.err.println("[RegionManager] Failed to send UPDATE command for table " + tableName + " to region " + regionName);
            return ResType.UPDATE_FAILURE;
        }
    }


    // --- ALTER ---
    public static List<ResType> alterTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.ALTER_FAILURE, ResType.ALTER_FAILURE));
        boolean masterExists = checkList.get(0) == ResType.FIND_SUCCESS;
        boolean slaveExists = checkList.get(1) == ResType.FIND_SUCCESS;

        if (!masterExists && !slaveExists) {
            results.set(0, ResType.ALTER_TABLE_NO_EXISTS);
            results.set(1, ResType.ALTER_TABLE_NO_EXISTS);
            return results;
        }

        // Execute on master
        if (masterExists) {
            results.set(0, alterTable(tableName, sql));
        } else {
            results.set(0, ResType.ALTER_TABLE_NO_EXISTS);
        }

        // Execute on slave
        String slaveTableName = tableName + "_slave";
        // Basic replace for ALTER TABLE `tableName` ...
        String slaveSql = sql.replaceFirst("(?i)alter table\\s+`?" + tableName + "`?", "ALTER TABLE `" + slaveTableName + "`");
        if (slaveExists) {
            results.set(1, alterTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.ALTER_TABLE_NO_EXISTS);
        }

        // Handle inconsistencies
        if (results.get(0) != ResType.ALTER_SUCCESS && results.get(1) == ResType.ALTER_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: ALTER failed on master " + tableName + " but succeeded on slave " + slaveTableName);
        } else if (results.get(0) == ResType.ALTER_SUCCESS && results.get(1) != ResType.ALTER_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: ALTER succeeded on master " + tableName + " but failed on slave " + slaveTableName);
        }

        // sortAndUpdate(); // Load unlikely to change with ALTER, no sort needed
        return results;
    }

    /** Helper for single table ALTER */
    public static ResType alterTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.ALTER_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending ALTER for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            // Load calculation for ALTER is complex. Don't update ZK payload.
            return ResType.ALTER_SUCCESS;
        } else {
            System.err.println("[RegionManager] Failed to send ALTER command for table " + tableName + " to region " + regionName);
            return ResType.ALTER_FAILURE;
        }
    }

    // --- TRUNCATE ---
    public static List<ResType> truncateTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.TRUNCATE_FAILURE, ResType.TRUNCATE_FAILURE));
        boolean masterExists = checkList.get(0) == ResType.FIND_SUCCESS;
        boolean slaveExists = checkList.get(1) == ResType.FIND_SUCCESS;

        if (!masterExists && !slaveExists) {
            results.set(0, ResType.TRUNCATE_TABLE_NO_EXISTS);
            results.set(1, ResType.TRUNCATE_TABLE_NO_EXISTS);
            return results;
        }

        // Execute on master
        if (masterExists) {
            results.set(0, truncateTable(tableName, sql));
        } else {
            results.set(0, ResType.TRUNCATE_TABLE_NO_EXISTS);
        }

        // Execute on slave
        String slaveTableName = tableName + "_slave";
        // Basic replace for TRUNCATE TABLE `tableName` ...
        String slaveSql = sql.replaceFirst("(?i)truncate table\\s+`?" + tableName + "`?", "TRUNCATE TABLE `" + slaveTableName + "`");
        if (slaveExists) {
            results.set(1, truncateTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.TRUNCATE_TABLE_NO_EXISTS);
        }

        // Handle inconsistencies
        if (results.get(0) != ResType.TRUNCATE_SUCCESS && results.get(1) == ResType.TRUNCATE_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: TRUNCATE failed on master " + tableName + " but succeeded on slave " + slaveTableName);
        } else if (results.get(0) == ResType.TRUNCATE_SUCCESS && results.get(1) != ResType.TRUNCATE_SUCCESS) {
            System.err.println("[RegionManager] Inconsistency: TRUNCATE succeeded on master " + tableName + " but failed on slave " + slaveTableName);
        }

        sortAndUpdate(); // Load resets to 0 after truncate
        return results;
    }


    /** Helper for single table TRUNCATE */
    public static ResType truncateTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.TRUNCATE_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending TRUNCATE for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            // Update payload in ZK to 0
            if (zooKeeperManager.setTablePayload(tableName, 0)) {
                // Update local cache
                if (regionsInfo.containsKey(regionName) && regionsInfo.get(regionName).containsKey(tableName)) {
                    regionsInfo.get(regionName).put(tableName, 0);
                }
                return ResType.TRUNCATE_SUCCESS;
            } else {
                System.err.println("[RegionManager] TRUNCATE command succeeded for " + tableName + " but failed to update ZK payload.");
                return ResType.TRUNCATE_FAILURE; // Indicate ZK failure
            }
        } else {
            System.err.println("[RegionManager] Failed to send TRUNCATE command for table " + tableName + " to region " + regionName);
            return ResType.TRUNCATE_FAILURE;
        }
    }

    // --- Find Operations ---
    public static List<ResType> findTableMasterAndSlave(String tableName) {
        List<ResType> res = new ArrayList<>();
        res.add(findTable(tableName));
        res.add(findTable(tableName + "_slave"));
        return res;
    }

    /** Checks ZK for table existence */
    private static ResType findTable(String tableName) {
        if (zooKeeperManager.getRegionServer(tableName) == null) {
            return ResType.FIND_TABLE_NO_EXISTS;
        } else {
            return ResType.FIND_SUCCESS;
        }
    }


    // --- Communication Helpers ---

    /**
     * Sends a generic SQL command to the specified RegionServer via the Master's handler.
     * Assumes the Master maintains handlers mapped by regionName (IP).
     *
     * @param regionName The IP or identifier of the target RegionServer.
     * @param sqlCommand The SQL command to send.
     * @return true if the command was sent successfully (doesn't guarantee execution success on RS).
     */
    private static boolean sendSqlCommandToRegion(String regionName, String sqlCommand) {
        if (regionServerListener == null) {
            System.err.println("[RegionManager] Cannot send command: RegionServerListener reference not set.");
            return false;
        }

        RegionServerHandler handler = null;
        int maxRetries = 5; // 最多重试次数 (例如 5 次)
        long retryDelayMs = 300; // 每次重试间隔 (例如 300 毫秒)

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            handler = regionServerListener.getRegionServerHandler(regionName); // 根据 IP 获取 handler

            // 检查 handler 是否存在并且处于运行状态 (需要给 RegionServerHandler 添加 isRunning 方法)
            if (handler != null && handler.isRunning()) {
                break; // 找到了活跃的 handler，退出循环
            }

            // 如果没找到或 handler 未运行，并且还未达到最大重试次数
            if (attempt < maxRetries) {
                System.out.println("[RegionManager] Handler for region " + regionName + " not found/ready (attempt " + attempt + "/" + maxRetries + "). Retrying in " + retryDelayMs + "ms...");
                try {
                    Thread.sleep(retryDelayMs); // 等待一段时间
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // 重置中断状态
                    System.err.println("[RegionManager] Send command retry interrupted.");
                    return false; // 如果线程被中断，则停止发送
                }
            }
        }

        // 经过重试后，再次检查 handler
        if (handler == null || !handler.isRunning()) {
            System.err.println("[RegionManager] Cannot send command: No active handler found for region " + regionName + " after " + maxRetries + " attempts.");
            return false;
        }

        // 如果找到了活跃的 handler，发送命令
        try {
            handler.forwardCommand(sqlCommand); // 通过 handler 将命令发送给 RegionServer
            // TODO: 这里仍然缺少等待 RegionServer 返回执行结果的逻辑。
            // 目前只是假设发送成功即成功。
            System.out.println("[RegionManager] Command successfully sent to handler for region " + regionName);
            return true; // 暂时认为发送即成功
        } catch (Exception e) {
            System.err.println("[RegionManager] Error sending command to region " + regionName + " via handler: " + e.getMessage());
            return false;
        }
    }

    /**
     * Sends a command to the target RegionServer instructing it to copy a table from the source.
     * This requires defining a specific command protocol.
     * Example: "COPY_TABLE <source_host> <source_port> <source_user> <source_pwd> <source_table> <target_table>"
     *
     * @param sourceRegionName Name/IP of the source region.
     * @param targetRegionName Name/IP of the target region (where command is sent).
     * @param sourceTableName  Table name on the source.
     * @param targetTableName  Desired table name on the target.
     * @return true if the command was sent successfully.
     */
    private static boolean sendCopyTableCommand(String sourceRegionName, String targetRegionName, String sourceTableName, String targetTableName) {
        // 1. Get source connection details from ZK
        String sourceConnectionString = getRegionConnectionString(sourceRegionName, true); // Get MySQL details
        if (sourceConnectionString == null) {
            System.err.println("[RegionManager] Cannot send COPY command: Failed to get source connection details for " + sourceRegionName);
            return false;
        }
        String[] sourceDetails = sourceConnectionString.split(","); // ip,port,user,pwd

        // 2. Get target handler
        if (regionServerListener == null) {
            System.err.println("[RegionManager] Cannot send COPY command: RegionServerListener reference not set.");
            return false;
        }
        RegionServerHandler targetHandler = regionServerListener.getRegionServerHandler(targetRegionName);
        if (targetHandler == null) {
            System.err.println("[RegionManager] Cannot send COPY command: No active handler found for target region " + targetRegionName);
            return false;
        }

        // 3. Construct the command (DEFINE YOUR PROTOCOL HERE)
        // Example: COPY <source_ip> <source_mysql_port> <source_user> <source_pwd> <source_table_name> <target_table_name>
        String command = String.format("COPY_TABLE %s %s %s %s %s %s",
                sourceRegionName, // source host IP/name
                sourceDetails[3], // source mysql port
                sourceDetails[1], // source mysql user
                sourceDetails[2], // source mysql password (SECURITY RISK!)
                sourceTableName,
                targetTableName);

        // 4. Send the command
        try {
            System.out.println("[RegionManager] Sending command to target " + targetRegionName + ": " + command.replaceAll(sourceDetails[2], "****")); // Mask password in log
            targetHandler.forwardCommand(command);
            // TODO: Wait for response from target RS confirming copy initiated/completed?
            return true; // Assume send success for now
        } catch (Exception e) {
            System.err.println("[RegionManager] Error sending COPY command to region " + targetRegionName + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Sends a command to initiate table migration. Could involve multiple steps/commands.
     * Example: Tell target to pull, then tell source to drop.
     *
     * @param sourceRegionName Name/IP of the source.
     * @param targetRegionName Name/IP of the target.
     * @param tableName        Table to migrate.
     * @return true if the initial command was sent successfully.
     */
    private static boolean sendMigrateTableCommand(String sourceRegionName, String targetRegionName, String tableName) {
        // This could be similar to sendCopyTableCommand, but the target RS, after copying,
        // might need to notify the Master, which then tells the source RS to drop the table.
        // Or, the Master orchestrates fully.
        // Let's use a simplified approach: tell the target to copy (like replication).
        // The Master will handle ZK update and telling source to drop later.
        System.out.println("[RegionManager] Initiating migration step 1: Copying " + tableName + " from " + sourceRegionName + " to " + targetRegionName);
        boolean copySent = sendCopyTableCommand(sourceRegionName, targetRegionName, tableName, tableName); // Copy with same name

        // Master needs to:
        // 1. Wait for confirmation of copy success from targetRegionName (via ZK data node or socket response).
        // 2. Update ZK: remove table from sourceRegionName node, add to targetRegionName node.
        // 3. Send DROP command to sourceRegionName for the table.

        return copySent; // Return success if the initial copy command was sent.
    }

    /**
     * Helper to get the connection string for a region server, needed by clients.
     * Retrieves info stored by RegionServer.createZooKeeperNode / ZKManager.addRegionServer.
     *
     * @param regionName IP/Name of the region server.
     * @param includeDbDetails If true, returns ip,dbPort,dbUser,dbPwd. If false, returns ip:clientListenPort.
     * @return Connection string or null if not found/error.
     */
    private static String getRegionConnectionString(String regionName, boolean includeDbDetails) {
        String clientListenPortPath = "/lss/region_server/" + regionName + "/port"; // Port RS listens for clients (e.g., 4001)
        String dbUserPath = "/lss/region_server/" + regionName + "/username";
        String dbPwdPath = "/lss/region_server/" + regionName + "/password";
        // Assuming MySQL port is standard or stored elsewhere if variable per RS
        // Let's assume it's needed for DB details. ZKManager stores it under /port2regionserver? No, that's clientListenPort.
        // The ZK structure from addRegionServer is complex. Let's assume standard port 3306 if not explicitly stored easily.
        // Let's refine based on ZKManager.addRegionServer:
        // port -> clientListenPort (e.g., 4001)
        // port2regionserver -> also clientListenPort (e.g., 4001) according to current RS code call
        // So, MySQL port isn't directly stored per-RS in that structure easily accessible way. Assume standard 3306 for now.

        try {
            if (!zooKeeperUtils.nodeExists("/lss/region_server/" + regionName)) {
                System.err.println("[RegionManager] Region node not found: " + regionName);
                return null;
            }

            String clientPort = zooKeeperUtils.getData(clientListenPortPath);

            if (includeDbDetails) {
                String dbUser = zooKeeperUtils.getData(dbUserPath);
                String dbPwd = zooKeeperUtils.getData(dbPwdPath);
                String dbPort = "3306"; // Default MySQL port, adjust if stored differently
                return String.format("%s,%s,%s,%s", regionName, dbPort, dbUser, dbPwd); // Format: host,dbPort,dbUser,dbPwd
            } else {
                return regionName + ":" + clientPort; // Format: ip:clientListenPort
            }
        } catch (Exception e) {
            System.err.println("[RegionManager] Error getting connection info for region " + regionName + ": " + e.getMessage());
            return null;
        }
    }
    // Overload for simpler client connection string retrieval
    private static String getRegionConnectionString(String regionName) {
        return getRegionConnectionString(regionName, false);
    }


} // End of RegionManager class