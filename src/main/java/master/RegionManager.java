/*
    维护region server信息
    相关操作:① 从ZooKeeper获取节点信息；
            ② 调用region server的接口，迁移region servers上的数据
    提供接口：为ZooKeeper提供节点负载分配策略
 */
package master;

import master.Master.RegionServerHandler;
import master.Master.RegionServerListenerThread;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

import java.util.*;
import java.util.stream.Collectors;

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

    public static RegionServerListenerThread regionServerListener = null;

    //信息同步，损毁迁移等

    /**
     * 初始化，从ZooKeeper处获取nodes数据
     * --测试通过--
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
     * 获取当前所有region node的情况
     * 记录region server下的表名以及表对应的load
     * --测试通过--
     */
    public static Map<String, Map<String, Integer>> getRegionsInfo() throws Exception {
        Map<String, Map<String, Integer>> newRegionsInfo = new LinkedHashMap<>();
        List<String> regionNames = zooKeeperUtils.getChildren("/lss/region_server"); // Get all Region IPs/names

        for (String regionName : regionNames) {
            Map<String, Integer> tablesInfo = new LinkedHashMap<>();
            String tableBasePath = "/lss/region_server/" + regionName + "/table";

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

    /**
     * 计算region server层级的load
     * @return Map<String, Integer>
     *         String为region server对应的ip+port(从ZooKeeper处获得）
     *         Integer为该region server对应的负载
     * --测试通过--
     */
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
        //排序，按负载升序排序优先处理，负载小的region server先接收load最大的table
        return sortLoadAsc(newRegionsLoad);
    }

    /**
     * 获取总负荷，用于平衡不同region server的负荷
     * @return regions servers的总负荷
     * --测试通过--
     */
    public static Integer getLoadsSum() {
        Integer sum = 0;
        for (Map.Entry<String,Integer> entry : regionsLoad.entrySet()) {
            sum += entry.getValue();
        }
        return sum;
    }

    /**
     * 排序regionInfo,同时根据regionInfo更新regionLoad、loadSum和loadAvg
     * --测试通过--
     */
    public static void sortAndUpdate() {
        try {
            regionsInfo = getRegionsInfo();
        } catch (Exception e) {
            System.err.println("[RegionManager] CRITICAL: Failed to refresh regionsInfo from ZooKeeper during sortAndUpdate: " + e.getMessage());
            e.printStackTrace();
        }
        regularRecoverData();
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

    private static void regularRecoverData() {
        List<String> recovered = new ArrayList<>();
        for (String tableName : new ArrayList<>(toBeCopiedTable)) {
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";

            ResType masterStatus = findTable(masterTableName);
            ResType slaveStatus = findTable(slaveTableName);

            if (masterStatus == ResType.FIND_TABLE_NO_EXISTS && slaveStatus == ResType.FIND_SUCCESS) { //母本丢失
                System.out.println("[RegionManager] Recovering missing master table: " + masterTableName + " from slave: " + slaveTableName);
                if (addSameTable(masterTableName, slaveTableName)) {
                    recovered.add(tableName);
                }
            } else if (slaveStatus == ResType.FIND_TABLE_NO_EXISTS && masterStatus == ResType.FIND_SUCCESS) { //副本丢失
                System.out.println("[RegionManager] Recovering missing slave table: " + slaveTableName + " from master: " + masterTableName);
                if (addSameTable(slaveTableName, masterTableName)) {
                    recovered.add(tableName);
                }
            } else if (masterStatus == ResType.FIND_SUCCESS && slaveStatus == ResType.FIND_SUCCESS) {
                recovered.add(tableName);
            } else {
                System.out.println("[RegionManager] Cannot recover " + tableName + ", master exists: " + (masterStatus == ResType.FIND_SUCCESS) + ", slave exists: " + (slaveStatus == ResType.FIND_SUCCESS));
            }
        }
        toBeCopiedTable.removeAll(recovered);
    }

    private static boolean addSameTable(String addTableName, String templateTableName) {
        //从被拷贝处取信息
        String templateRegionName = zooKeeperManager.getRegionServer(templateTableName);
        if (templateRegionName == null || !regionsInfo.containsKey(templateRegionName) || !regionsInfo.get(templateRegionName).containsKey(templateTableName)) {
            System.err.println("[RegionManager] Cannot add table " + addTableName + ": Template table " + templateTableName + " or its region not found in current info.");
            return false;
        }
        Integer load = regionsInfo.get(templateRegionName).get(templateTableName);
        //新建
        String addRegionName = getLeastRegionName(templateTableName);
        if (addRegionName == null) {
            System.err.println("[RegionManager] Cannot add table " + addTableName + ": No suitable region server found.");
            return false;
        }

        if (!regionsInfo.containsKey(addRegionName)) {
            regionsInfo.put(addRegionName, new LinkedHashMap<>());
        }
        regionsInfo.get(addRegionName).put(addTableName, load);
        //通知ZooKeeper
        boolean zkSuccess = zooKeeperManager.addTable(addRegionName, new TableInform(addTableName, load));
        if (!zkSuccess) {
            System.err.println("[RegionManager] Failed to add table " + addTableName + " to ZooKeeper for region " + addRegionName);
            if (regionsInfo.containsKey(addRegionName)) {
                regionsInfo.get(addRegionName).remove(addTableName);
            }
            return false;
        }

        //TODO:从slaveRegionName复制表到regionName
        boolean commandSent = sendCopyTableCommand(templateRegionName, addRegionName, templateTableName, addTableName);
        if (!commandSent) {
            System.err.println("[RegionManager] Failed to send copy command from " + templateRegionName + " to " + addRegionName + " for table " + addTableName);
            zooKeeperManager.deleteTable(addTableName);
            if (regionsInfo.containsKey(addRegionName)) {
                regionsInfo.get(addRegionName).remove(addTableName);
            }
            return false;
        }

        System.out.println("[RegionManager] Initiated copy for table " + addTableName + " on region " + addRegionName + " from template " + templateTableName + " on region " + templateRegionName);
        return true;
    }

    //region servers均衡策略

    /**
     * 排序，按负载降序排序优先处理，
     * 便于负载小的region server先接收load最大的table
     * --测试通过--
     */
    public static Map<String, Integer> sortLoadAsc(Map<String, Integer> loads){
        return loads.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * 降序排序
     * --测试通过--
     */
    public static Map<String, Integer> sortLoadDsc(Map<String, Integer> loads){
        return loads.entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * 寻找负载最小的不含有某个表名的region server名字
     * @return 如果有节点，则返回最小节点对应的名字；如果没有节点，返回null
     * --测试通过--
     */
    public static String getLeastRegionName(String tableName) {
        String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
        String slaveTableName = masterTableName + "_slave";
        System.out.println("[RegionManager] Finding least loaded region avoiding master '" + masterTableName + "' and slave '" + slaveTableName + "'.");
        System.out.println("[RegionManager] Current regionsLoad (sorted asc): " + regionsLoad);

        for (Map.Entry<String, Integer> entry : regionsLoad.entrySet()) {
            String regionServer = entry.getKey();
            Map<String, Integer> tables = regionsInfo.get(regionServer);
            System.out.println("[RegionManager] Checking candidate region: " + regionServer + " (Load: " + entry.getValue() + ")");

            if (tables == null) {
                System.out.println("[RegionManager]   - Skipping region " + regionServer + ": No table info found in local cache.");
                continue;
            }

            boolean hasMaster = tables.containsKey(masterTableName);
            boolean hasSlave = tables.containsKey(slaveTableName);
            // *** ADD LOGGING ***
            System.out.println("[RegionManager]   - Region " + regionServer + " contains '" + masterTableName + "': " + hasMaster);
            System.out.println("[RegionManager]   - Region " + regionServer + " contains '" + slaveTableName + "': " + hasSlave);
            System.out.println("[RegionManager]   - Tables on " + regionServer + ": " + tables.keySet());


            if (!hasMaster && !hasSlave) {
                System.out.println("[RegionManager]   - Found suitable region: " + regionServer);
                return regionServer;
            } else {
                System.out.println("[RegionManager]   - Region " + regionServer + " is unsuitable.");
            }
        }
        System.err.println("[RegionManager] No suitable region found for placing counterpart of " + tableName);
        return null;
    }

    /**
     * 寻找负载最大的region server名字,同时保证该region server存在可以迁移的不重复的表
     * 用于addRegion时平衡负荷，新region server先从最大的region server里面选择表
     * --测试通过--
     */
    public static String getLargestRegionName(Map<String, Integer> existTablesInfo){
        // sortAndUpdate(); // Avoid recursive calls
        if (!regionsLoad.isEmpty()) {
            // regionsLoad是排好序的，直接最后一个
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(regionsLoad.entrySet());
            for(int i = entries.size() - 1; i>=0; i--){
                Map.Entry<String, Integer> largestRegion = entries.get(i);
                String regionName = largestRegion.getKey();
                Map<String, Integer> tables = regionsInfo.get(regionName);
                if (tables == null || tables.isEmpty()) continue;
                if(getLargerTableInfo(regionName, 0, existTablesInfo)!=null){
                    return regionName;
                }
            }
        }
        return null;
    }

    /**
     * 寻找大于等于指定负荷且符合最小的表，同时该表(母本/副本)不能出现在Map中
     * @return 如果有表，则返回表对应的Info; 如果没有节点,返回null
     * --测试通过--
     */
    public static Map<String, Integer> getLargerTableInfo(String regionName, Integer load, Map<String, Integer> existTablesInfo) {
        Map<String, Integer> tablesInfo = regionsInfo.get(regionName);
        if (tablesInfo == null || tablesInfo.isEmpty()) {
            return null;
        }
        Map<String, Integer> result = null;
        for (Map.Entry<String, Integer> entry : tablesInfo.entrySet()) {
            Integer tableLoad = entry.getValue();
            String tableName = entry.getKey();
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";
            if (!existTablesInfo.containsKey(masterTableName) && !existTablesInfo.containsKey(slaveTableName)) {//母本和副本不出现在同一个region server里面
                // 返回负荷最大的表或者满足条件且负荷较小的表
                if (result == null || tableLoad>=load) {
                    result = new LinkedHashMap<>();
                    result.put(tableName, tableLoad);
                }else{
                    break;
                }
            }
        }
        return result;
    }

    //ZooKeeper在region servers有变动的时候，调用下方函数

    /**
     * 新添加一个region server(我们认为是空的，所以无需记录或传递新region server内的数据）
     * 将其他region server上的数据表匀给该region server上
     */
    public static ResType addRegion(List<String> nowRegionNames) {
        System.out.println("[RegionManager] Detected RegionServer change. Current list: " + nowRegionNames);
        String addRegionName = null;
        for (String regionName : nowRegionNames) {
            if (!regionsInfo.containsKey(regionName)) {
                addRegionName = regionName;
                try {
                    String existPath = "/lss/region_server/" + regionName + "/exist";
                    if (zooKeeperUtils.nodeExists(existPath)) {
                        zooKeeperUtils.setWatch(existPath);
                    }
                    String dataPath = "/lss/region_server/" + regionName + "/data";
                    if (zooKeeperUtils.nodeExists(dataPath)) {
                        zooKeeperUtils.setWatch(dataPath);
                    }
                } catch (Exception e) {
                    System.err.println("[RegionManager] Warning: Failed to set initial watch on new region: " + regionName + " - " + e.getMessage());
                }
                break;
            }
        }

        if (addRegionName == null) {
            System.out.println("[RegionManager] No new region detected or already processed.");
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }
        if (regionsInfo.containsKey(addRegionName)) {
            System.out.println("[RegionManager] Region " + addRegionName + " already exists in local info.");
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }

        System.out.println("[RegionManager] Adding new region: " + addRegionName);
        regionsInfo.put(addRegionName, new LinkedHashMap<>());
        sortAndUpdate();

        Integer avgLoad = loadsAvg; // Use the recalculated average
        Map<String, Integer> addTablesInfo = new LinkedHashMap<>();
        Integer addRegionLoadSum = 0;

        System.out.println("[RegionManager] Balancing load for new region " + addRegionName + ". Target avg load: " + avgLoad);

        while (addRegionLoadSum <= avgLoad) {
            sortAndUpdate();
            String largestRegionName = getLargestRegionName(addTablesInfo);

            if (largestRegionName == null) {
                System.out.println("[RegionManager] No suitable source region found to migrate tables from.");
                break;
            }

            Map<String, Integer> largestRegionTables = regionsInfo.get(largestRegionName);
            if (largestRegionTables == null || largestRegionTables.size() <= 1) {
                if (largestRegionTables == null || largestRegionTables.isEmpty()) {
                    System.out.println("[RegionManager] Source region " + largestRegionName + " has no tables to migrate.");
                    break;
                }
            }

            Integer currentLargestLoad = regionsLoad.get(largestRegionName);
            Integer idealLoadToMove = Math.max(0, Math.min(avgLoad - addRegionLoadSum, currentLargestLoad - avgLoad));

            Map<String, Integer> tableToMoveInfo = getLargerTableInfo(largestRegionName, idealLoadToMove, addTablesInfo);

            if (tableToMoveInfo == null) {
                tableToMoveInfo = getLargerTableInfo(largestRegionName, 0, addTablesInfo);
                if (tableToMoveInfo == null && largestRegionTables != null && !largestRegionTables.isEmpty()) {
                    tableToMoveInfo = getLargerTableInfo(largestRegionName, 0, addTablesInfo);
                }
            }

            if (tableToMoveInfo == null) {
                System.out.println("[RegionManager] No suitable table found to migrate from " + largestRegionName + " to " + addRegionName);
                break; // Cannot find any table to move from this source
            }

            String tableNameToMove = tableToMoveInfo.keySet().iterator().next();
            Integer tableLoadToMove = tableToMoveInfo.get(tableNameToMove);

            System.out.println("[RegionManager] Selected table '" + tableNameToMove + "' (load " + tableLoadToMove + ") from " + largestRegionName + " to migrate to " + addRegionName);

            boolean migrateCommandSent = sendMigrateTableCommand(largestRegionName, addRegionName, tableNameToMove);
            if (migrateCommandSent) {
                System.out.println("[RegionManager] Migration command sent for table " + tableNameToMove);
            } else {
                System.err.println("[RegionManager] Failed to send migration command for table " + tableNameToMove + ". Stopping balancing for now.");
            }
            //处理表迁出的region server
            regionsInfo.get(largestRegionName).remove(tableNameToMove);
            //处理表迁入的region server
            addTablesInfo.put(tableNameToMove, tableLoadToMove);
            addRegionLoadSum += tableLoadToMove;
            zooKeeperManager.addTable(addRegionName, new TableInform(tableNameToMove, tableLoadToMove));
        }
        regionsInfo.put(addRegionName, addTablesInfo);
        sortAndUpdate();
        System.out.println("[RegionManager] Finished load balancing for new region " + addRegionName + ". Final load: " + regionsLoad.getOrDefault(addRegionName, 0));
        return ResType.ADD_REGION_SUCCESS;
    }

    /**
     * 为某一个被删除的region server善后
     * 将该region server上的数据表重分配到其他region server上
     */
    public static ResType delRegion(String delRegionName) {
        System.out.println("[RegionManager] Detected deletion/disconnection of region: " + delRegionName);
        if (!regionsInfo.containsKey(delRegionName)) {
            System.err.println("[RegionManager] Region " + delRegionName + " not found in local info. Already processed or error.");
            return ResType.DROP_REGION_NO_EXISTS;
        }
        Map<String, Integer> delTablesInfo = new HashMap<>(regionsInfo.get(delRegionName)); // Copy tables to redistribute
        regionsInfo.remove(delRegionName);//首先移除被删除region server，防止参与分配tables
        System.out.println("[RegionManager] Redistributing " + delTablesInfo.size() + " tables from deleted region " + delRegionName);

        if (regionsInfo.isEmpty()) {
            System.err.println("[RegionManager] CRITICAL: No remaining region servers to redistribute tables to!");
            return ResType.DROP_REGION_FAILURE;
        }
        for (Map.Entry<String, Integer> tableEntry : delTablesInfo.entrySet()) {
            sortAndUpdate();
            String tableName = tableEntry.getKey();
            Integer tableLoad = tableEntry.getValue();
            String leastRegionName = getLeastRegionName(tableName); //防止同一region server存储了母本和副本

            if (leastRegionName == null) {
                System.err.println("[RegionManager] CRITICAL: Could not find a suitable region server to migrate table '" + tableName + "' to! Data might be lost.");
                continue;
            }
            System.out.println("[RegionManager] Migrating table '" + tableName + "' (load " + tableLoad + ") from deleted " + delRegionName + " to " + leastRegionName);
            if (!regionsInfo.containsKey(leastRegionName)) {
                regionsInfo.put(leastRegionName, new LinkedHashMap<>());
            }

            boolean migrateCommandSent = sendMigrateTableCommand(delRegionName, leastRegionName, tableName);
            if (migrateCommandSent) {
                System.out.println("[RegionManager] Migration command sent for table " + tableName);
            } else {
                System.err.println("[RegionManager] Failed to send migration command for table " + tableName + ". Stopping balancing for now.");
            }

            Map<String,Integer> tablesInfo = regionsInfo.get(leastRegionName);
            tablesInfo.put(tableName, tableLoad);
            regionsInfo.put(leastRegionName, tablesInfo);

            boolean zkSuccess = zooKeeperManager.addTable(leastRegionName, new TableInform(tableName, tableLoad));
            if (!zkSuccess) {
                System.err.println("[RegionManager] Failed to update ZooKeeper for migrated table " + tableName + " to region " + leastRegionName);
            }else{
                System.out.println("[RegionManager] Update ZooKeeper for migrated table " + tableName + " to region " + leastRegionName + " successfully updated.");
            }
        }

        sortAndUpdate();
        System.out.println("[RegionManager] Finished redistributing tables from deleted region " + delRegionName);
        return ResType.DROP_REGION_SUCCESS;
    }

    /**
     * 创建新表，同时在不同的region server上创建一份副本
     *
     */
    public static List<ResType> createTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.CREATE_TABLE_FAILURE, ResType.CREATE_TABLE_FAILURE));
        if (checkList.get(0) == ResType.FIND_SUCCESS) {
            results.set(0, ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        if (checkList.get(1) == ResType.FIND_SUCCESS) {
            results.set(1, ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        if (results.get(0) == ResType.CREATE_TABLE_ALREADY_EXISTS || results.get(1) == ResType.CREATE_TABLE_ALREADY_EXISTS) {
            return results;
        }

        sortAndUpdate();

        // Find region for master table
        String masterRegionName = getLeastRegionName(tableName + "_slave"); // Ensure it doesn't clash with potential future slave
        if (masterRegionName != null) {
            results.set(0, createTable(masterRegionName, tableName, sql));
        } else {
            System.err.println("[RegionManager] Cannot create master table " + tableName + ": No suitable region found.");
        }
        String slaveTableName = tableName + "_slave";
        String slaveRegionName = getLeastRegionName(tableName); // Ensure it doesn't clash with master
        if (slaveRegionName != null) {
            String slaveSql = sql.replaceFirst("(?i)create table\\s+`?" + tableName + "`?", "CREATE TABLE `" + slaveTableName + "`"); // Basic replace
            results.set(1, createTable(slaveRegionName, slaveTableName, slaveSql));
            if (results.get(0) == ResType.CREATE_TABLE_SUCCESS && results.get(1) == ResType.CREATE_TABLE_SUCCESS) {
                sendCopyTableCommand(masterRegionName, slaveRegionName, tableName, slaveTableName);
            }
        } else {
            System.err.println("[RegionManager] Cannot create slave table " + slaveTableName + ": No suitable region found.");
        }
        if (results.get(0) == ResType.CREATE_TABLE_SUCCESS && results.get(1) != ResType.CREATE_TABLE_SUCCESS) {
            toBeCopiedTable.add(slaveTableName);
            System.out.println("[RegionManager] Slave creation failed for " + slaveTableName + ", added to recovery list.");
        }

        if (results.get(0) != ResType.CREATE_TABLE_SUCCESS && results.get(1) == ResType.CREATE_TABLE_SUCCESS) {
            toBeCopiedTable.add(tableName);
            System.out.println("[RegionManager] Master creation failed for " + tableName + ", added to recovery list.");
        }
        sortAndUpdate();//更新
        return results;
    }

    private static ResType createTable(String regionName, String tableName, String sql) {
        System.out.println("[RegionManager] Attempting to create table " + tableName + " on region " + regionName);
        if (zooKeeperManager.addTable(regionName, new TableInform(tableName, 0))) {

            if (!regionsInfo.containsKey(regionName)) regionsInfo.put(regionName, new LinkedHashMap<>());
            regionsInfo.get(regionName).put(tableName, 0);
            //TODO:实际操作sql语句
            boolean success = sendSqlCommandToRegion(regionName, sql);
            if (success) {
                System.out.println("[RegionManager] Successfully sent CREATE command for table " + tableName + " to region " + regionName);
                return ResType.CREATE_TABLE_SUCCESS;
            } else {
                System.err.println("[RegionManager] Failed to send CREATE command for table " + tableName + " to region " + regionName);
                zooKeeperManager.deleteTable(tableName);
                if(regionsInfo.containsKey(regionName)) regionsInfo.get(regionName).remove(tableName);
                return ResType.CREATE_TABLE_FAILURE;
            }
        } else {
            System.err.println("[RegionManager] Failed to add table " + tableName + " to ZooKeeper for region " + regionName);
            return ResType.CREATE_TABLE_FAILURE;
        }
    }

    public static List<ResType> dropTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> results = new ArrayList<>(Arrays.asList(ResType.DROP_TABLE_FAILURE, ResType.DROP_TABLE_FAILURE));
        if (checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS && checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS) {
            results.set(0, ResType.DROP_TABLE_NO_EXISTS);
            results.set(1, ResType.DROP_TABLE_NO_EXISTS);
            return results;
        }
        if (checkList.get(0) == ResType.FIND_SUCCESS) {
            results.set(0, dropTable(tableName, sql));
        } else {
            results.set(0, ResType.DROP_TABLE_NO_EXISTS); // Doesn't exist, so "success" in terms of goal
            toBeCopiedTable.remove(tableName); // Remove from recovery if it was there
        }

        String slaveTableName = tableName + "_slave";
        String slaveSql = sql.replaceFirst("(?i)drop table\\s+(if exists\\s+)?`?" + tableName + "`?", "DROP TABLE IF EXISTS `" + slaveTableName + "`");
        if (checkList.get(1) == ResType.FIND_SUCCESS) {
            results.set(1, dropTable(slaveTableName, slaveSql));
        } else {
            results.set(1, ResType.DROP_TABLE_NO_EXISTS); // Doesn't exist
            toBeCopiedTable.remove(slaveTableName); // Remove from recovery
        }

        sortAndUpdate();// 更新
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
            return ResType.DROP_TABLE_NO_EXISTS;
        }
        System.out.println("[RegionManager] Attempting to drop table " + tableName + " from region " + regionName);

        //TODO:实际操作sql语句
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);
        if (commandSuccess) {
            System.out.println("[RegionManager] Successfully sent DROP command for table " + tableName + " to region " + regionName);
            if (zooKeeperManager.deleteTable(tableName)) {
                if (regionsInfo.containsKey(regionName)) {
                    regionsInfo.get(regionName).remove(tableName);
                }
                return ResType.DROP_TABLE_SUCCESS;
            } else {
                System.err.println("[RegionManager] Failed to delete table " + tableName + " from ZooKeeper after successful drop command.");
                return ResType.DROP_TABLE_FAILURE;
            }
        } else {
            System.err.println("[RegionManager] Failed to send DROP command for table " + tableName + " to region " + regionName);
            return ResType.DROP_TABLE_FAILURE;
        }
    }

    public static SelectInfo selectTable(List<String> tableNames, String sql) {
        System.out.println("[RegionManager] Processing SELECT for tables: " + tableNames);
        if (!checkAndRecoverData(tableNames)) {
            return new SelectInfo(false);
        }
        Map<String, String> tableToRegionMap = new HashMap<>();
        Map<String, String> tableToActualNameMap = new HashMap<>();
        boolean foundAll = true;
        for (String tableName : tableNames) {
            String masterRegion = zooKeeperManager.getRegionServer(tableName); // 返回 IP
            String slaveRegion = zooKeeperManager.getRegionServer(tableName + "_slave");

            if (masterRegion != null) {
                tableToRegionMap.put(tableName, masterRegion);
                tableToActualNameMap.put(tableName, tableName);
            } else if (slaveRegion != null) {
                tableToRegionMap.put(tableName, slaveRegion);
                tableToActualNameMap.put(tableName, tableName + "_slave");
                System.out.println("[RegionManager] Using slave table " + (tableName + "_slave") + " on region " + slaveRegion + " for SELECT.");
            } else {
                System.err.println("[RegionManager] SELECT failed: Could not find region for table " + tableName + " even after recovery check.");
                foundAll = false;
                break;
            }
        }

        if (!foundAll || tableNames.isEmpty()) {
            System.err.println("[RegionManager] SELECT failed: Not all tables found or initial table list was empty.");
            return new SelectInfo(false);
        }

        String targetRegionName = tableToRegionMap.get(tableNames.get(0)); // targetRegionName 是 IP
        if (targetRegionName == null) {
            System.err.println("[RegionManager] SELECT failed: Target region name for table " + tableNames.get(0) + " is null.");
            return new SelectInfo(false);
        }

        String finalSql = sql;
        for (String originalName : tableNames) {
            if (tableToActualNameMap.containsKey(originalName)) {
                String actualNameInRegion = tableToActualNameMap.get(originalName);
                if (!originalName.equals(actualNameInRegion)) {
                    finalSql = finalSql.replaceAll("(?i)\\b" + originalName + "\\b", actualNameInRegion);
                }
            }
        }
        System.out.println("[RegionManager] Final SQL for region " + targetRegionName + ": " + finalSql);
        String targetRegionAddressWithPort = getRegionConnectionString(targetRegionName);
        if (targetRegionAddressWithPort == null) {
            System.err.println("[RegionManager] SELECT failed: Could not get connection string for target region " + targetRegionName);
            return new SelectInfo(false);
        }
        return new SelectInfo(true, targetRegionAddressWithPort, finalSql);
    }

    private static boolean checkAndRecoverData(List<String> tableNames) {
        System.out.println("[RegionManager] Starting Check & Recover for tables: " + tableNames);
        sortAndUpdate();

        boolean allOk = true;
        for (String tableName : tableNames) {
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";

            ResType masterStatus = findTable(masterTableName);
            ResType slaveStatus = findTable(slaveTableName);

            if (masterStatus == ResType.FIND_TABLE_NO_EXISTS && slaveStatus == ResType.FIND_TABLE_NO_EXISTS) {
                System.err.println("[RegionManager] Check failed: Neither master (" + masterTableName + ") nor slave (" + slaveTableName + ") found.");
                allOk = false;
                break;//不可能成功,因为缺表
            } else if (masterStatus == ResType.FIND_TABLE_NO_EXISTS) {
                System.out.println("[RegionManager] Check: Master " + masterTableName + " missing, attempting recovery from slave " + slaveTableName);
                if (!addSameTable(masterTableName, slaveTableName)) {
                    System.err.println("[RegionManager] Check failed: Recovery initiation of master " + masterTableName + " failed (e.g., no suitable region).");
                    allOk = false;
                    break;
                } else {
                    System.out.println("[RegionManager] Check: Recovery initiated for master " + masterTableName);
                }
            } else if (slaveStatus == ResType.FIND_TABLE_NO_EXISTS) {
                System.out.println("[RegionManager] Check: Slave " + slaveTableName + " missing, attempting recovery from master " + masterTableName);
                if (!addSameTable(slaveTableName, masterTableName)) {
                    System.err.println("[RegionManager] Check failed: Recovery initiation of slave " + slaveTableName + " failed (e.g., no suitable region).");
                    allOk = false; // Treat missing slave as failure for now
                    break;
                } else {
                    System.out.println("[RegionManager] Check: Recovery initiated for slave " + slaveTableName);
                }
            }
        }
        if (allOk) {
            System.out.println("[RegionManager] Check & Recovery completed for tables: " + tableNames + ". Result: " + allOk);
        }
        if (!allOk) sortAndUpdate();
        return allOk;
    }

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

    public static ResType truncateTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if (regionName == null) return ResType.TRUNCATE_TABLE_NO_EXISTS;

        System.out.println("[RegionManager] Sending TRUNCATE for table " + tableName + " to region " + regionName);
        boolean commandSuccess = sendSqlCommandToRegion(regionName, sql);

        if (commandSuccess) {
            if (zooKeeperManager.setTablePayload(tableName, 0)) {
                if (regionsInfo.containsKey(regionName) && regionsInfo.get(regionName).containsKey(tableName)) {
                    regionsInfo.get(regionName).put(tableName, 0);
                }
                return ResType.TRUNCATE_SUCCESS;
            } else {
                System.err.println("[RegionManager] TRUNCATE command succeeded for " + tableName + " but failed to update ZK payload.");
                return ResType.TRUNCATE_FAILURE;
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
            System.out.println("[RegionManager] Command successfully sent to handler for region " + regionName);
            return true;
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
        System.out.println("[RegionManager] 准备发送 MIGRATE_TABLE_FROM_SOURCE 命令给目标 Region " + targetRegionName +
                " 以迁移表 " + tableName + " 从源 " + sourceRegionName);

        // 1. 从 ZK 获取源 RegionServer 的 MySQL 连接详细信息
        String sourceDbDetailsString = getRegionConnectionString(sourceRegionName, true); // true 表示获取DB细节
        if (sourceDbDetailsString == null) {
            System.err.println("[RegionManager] 无法发送 MIGRATE 命令: 获取源 " + sourceRegionName + " 的DB连接信息失败。");
            return false;
        }
        String[] sourceDetails = sourceDbDetailsString.split(","); // 期望格式: host,dbPort,dbUser,dbPwd
        if (sourceDetails.length < 4) {
            System.err.println("[RegionManager] 源 " + sourceRegionName + " 的DB连接信息格式不正确: " + sourceDbDetailsString);
            return false;
        }
        // sourceDetails[0] 是 sourceRegionName (IP)
        // sourceDetails[1] 是 sourceMysqlPort
        // sourceDetails[2] 是 sourceMysqlUser
        // sourceDetails[3] 是 sourceMysqlPwd

        // 2. 构建 "MIGRATE_TABLE_FROM_SOURCE" 命令
        // 格式: MIGRATE_TABLE_FROM_SOURCE <source_host_ip> <source_mysql_port> <source_mysql_user> <source_mysql_pwd> <table_name_to_migrate>
        String command = String.format("MIGRATE_TABLE_FROM_SOURCE %s %s %s %s %s",
                sourceDetails[0], // sourceRegionName (作为 host_ip)
                sourceDetails[1], // sourceMysqlPort
                sourceDetails[2], // sourceMysqlUser
                sourceDetails[3], // sourceMysqlPwd (安全风险)
                tableName);

        // 3. 将此命令发送给 *目标* RegionServer (targetRegionName)
        boolean commandSent = sendSqlCommandToRegion(targetRegionName, command); // sendSqlCommandToRegion 已包含重试

        if (commandSent) {
            System.out.println("[RegionManager] MIGRATE_TABLE_FROM_SOURCE 命令已成功发送给 Region " + targetRegionName + "。");
            // 注意: 这只表示命令已发送。实际迁移成功与否需要目标RegionServer反馈。
        } else {
            System.err.println("[RegionManager] MIGRATE_TABLE_FROM_SOURCE 命令发送给 Region " + targetRegionName + " 失败。");
        }
        return commandSent;
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
}