/*
    维护region server信息
    相关操作:① 从ZooKeeper获取节点信息；
            ② 调用region server的接口，迁移region servers上的数据
    提供接口：为ZooKeeper提供节点负载分配策略
 */
package master;

import master.Master.RegionServerHandler;
import master.Master.RegionServerListenerThread;
import regionserver.RegionServer;
import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

import java.io.*;
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

    public static Integer loadsSum = 0;

    public static RegionServerListenerThread regionServerListener = null;

    //全局变量
    public static String MASTER_IP = "10.193.103.92";
    public static int MAX_LOOP_NUM = 50;
    public static final String TARGET_DB_NAME = "lss"; // 目标数据库名

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
                    System.err.println("[RegionManager] Warning: Failed to set initial watch on existing region: " +
                            regionName + " - " + e.getMessage());
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
        Map<String, Map<String, Integer>> newLegionsInfo = new LinkedHashMap<>();
        List<String> regionNames = zooKeeperUtils.getChildren("/lss/region_server");//获取下所有Region节点
        for(String regionName : regionNames){//获取节点上的数据
            Map<String, Integer> tablesInfo = new LinkedHashMap<>();
            List<String> tableNames = zooKeeperUtils.getChildren("/lss/region_server/" + regionName + "/table");
            for(String tableName : tableNames){
                Integer load = Integer.valueOf(zooKeeperUtils.getData("/lss/region_server/" + regionName + "/table/" + tableName + "/payload"));
                tablesInfo.put(tableName, load);
            }
            newLegionsInfo.put(regionName, sortLoadDsc(tablesInfo));//给某个region server中的tables排序，按负载降序排序优先处理，
        }
        return newLegionsInfo;
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
    public static void sortAndUpdate(){
        Map<String, Map<String, Integer>> sortedRegionsInfo = new LinkedHashMap<>();
        for(String regionName : regionsInfo.keySet()){
            sortedRegionsInfo.put(regionName, sortLoadDsc(regionsInfo.get(regionName)));
        }
        regionsInfo = sortedRegionsInfo;
        regionsLoad = getRegionsLoad();
        loadsSum = getLoadsSum();
    }

    private static void recoverData(){
        List<String> temp = new ArrayList<>(toBeCopiedTable);
        for(String tableName : temp){
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";
            List<ResType> checkList = findTableMasterAndSlave(masterTableName);
            if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS
                    && checkList.get(1) == ResType.FIND_SUCCESS){//母本丢失
                if(addSameTable(masterTableName, slaveTableName)) {
                    toBeCopiedTable.remove(tableName);
                }
            }else if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){//副本丢失
                if(addSameTable(slaveTableName, masterTableName)) {
                    toBeCopiedTable.remove(tableName);
                }
            }
        }
    }

    private static boolean addSameTable(String addTableName, String templateTableName){
        //从被拷贝处取信息
        String templateRegionName = zooKeeperManager.getRegionServer(templateTableName);
        Integer load = regionsInfo.get(templateRegionName).get(templateTableName);
        //新建
        String addRegionName = getLeastRegionName(templateTableName);
        if(addRegionName == null){
            return false;
        }
        Map<String, Integer> tableInfos = regionsInfo.get(addRegionName);
        tableInfos.put(addTableName, load);
        //通知ZooKeeper
        zooKeeperManager.addTable(addRegionName, new TableInform(addTableName,load));
        sortAndUpdate();
        copyTable(templateRegionName,addRegionName,templateTableName,addTableName);
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
                .sorted(Map.Entry.comparingByValue()) // 升序
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new // 保证有序
                ));
    }

    /**
     * 降序排序
     * --测试通过--
     */
    public static Map<String, Integer> sortLoadDsc(Map<String, Integer> loads){
        return loads.entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())// 降序
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new // 保证有序
                ));
    }

    /**
     * 寻找负载最小的不含有某个表名的region server名字
     * @return 如果有节点，则返回最小节点对应的名字；如果没有节点，返回null
     * --测试通过--
     */

    public static String getLeastRegionName(String tableName) {
        sortAndUpdate();
        String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
        String slaveTableName = masterTableName + "_slave";

        for (Map.Entry<String, Integer> entry : regionsLoad.entrySet()) {
            String regionServer = entry.getKey();
            Map<String, Integer> tables = regionsInfo.get(regionServer);
            if (!tables.containsKey(masterTableName) && !tables.containsKey(slaveTableName)) {
                return regionServer;
            }
        }
        return null;
    }

    /**
     * 寻找负载最大的region server名字,同时保证该region server存在可以迁移的不重复的表
     * 用于addRegion时平衡负荷，新region server先从最大的region server里面选择表
     * --测试通过--
     */
    public static String getLargestRegionName(Map<String, Integer> existTablesInfo){
        sortAndUpdate();
        if (!regionsLoad.isEmpty()) {
            // regionsLoad是排好序的，直接最后一个
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(regionsLoad.entrySet());
            for(int i = entries.size() - 1; i>=0; i--){
                Map.Entry<String, Integer> largestRegion = entries.get(i);
                String regionName = largestRegion.getKey();
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
        if(nowRegionNames.size()==regionsInfo.size()){
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }
        String addRegionName = null;
        for (String regionName : nowRegionNames) {
            if (!regionsInfo.containsKey(regionName)) {
                addRegionName = regionName;
                break;
            }
        }
        if (regionsInfo.containsKey(addRegionName)) {
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }
        sortAndUpdate();
        Integer avgLoad = getLoadsSum() / (regionsInfo.size() + 1);//为它提前考虑

        Map<String, Integer> addTablesInfo = new LinkedHashMap<>();
        Integer addRegionLoadSum = 0;
        int count = MAX_LOOP_NUM;
        while (addRegionLoadSum < avgLoad && count-->0) {
            sortAndUpdate();
            String largestRegionName = getLargestRegionName(addTablesInfo);
            Map<String, Integer> largerRegionTables = regionsInfo.get(largestRegionName);
            if (largerRegionTables == null || largerRegionTables.size() <= 1) {// 说明这个region不能再迁了
                break;
            }
            Integer aimLoad = Math.min(avgLoad - addRegionLoadSum, regionsLoad.get(largestRegionName) - avgLoad);
            Map<String, Integer> largestTableInfo = getLargerTableInfo(largestRegionName, aimLoad, addTablesInfo);
            String tableNameToMove;
            if (largestTableInfo != null) {
                tableNameToMove = largestTableInfo.keySet().iterator().next();
            } else {
                break;
            }
            Integer tableLoadToMove = largestTableInfo.get(tableNameToMove);

            // 把表从largestRegion迁到targetRegion
            migrateTable(largestRegionName,addRegionName,tableNameToMove);
            //处理表迁出的region server
            largerRegionTables.remove(tableNameToMove);
            regionsInfo.put(largestRegionName, largerRegionTables);
            //处理表迁入的region server
            addRegionLoadSum += tableLoadToMove;
            addTablesInfo.put(tableNameToMove, tableLoadToMove);
            zooKeeperManager.deleteTable(tableNameToMove);
            zooKeeperManager.addTable(addRegionName,new TableInform(tableNameToMove,tableLoadToMove));
        }
        regionsInfo.put(addRegionName, addTablesInfo);
        sortAndUpdate();
        recoverData();
        return ResType.ADD_REGION_SUCCESS;
    }

    /**
     * 为某一个掉线的region server善后
     * 将该region server上的数据表重分配到其他region server上
     */
    public static ResType delRegion(String delRegionName) {
        if (!regionsInfo.containsKey(delRegionName)) {
            return ResType.DROP_REGION_NO_EXISTS;
        }
        Map<String, Integer> delTablesInfo = regionsInfo.get(delRegionName);
        regionsInfo.remove(delRegionName);//首先移除被删除region server，防止参与分配tables
        if(regionsInfo.size()==1){
            for(Map.Entry<String, Integer> entry : delTablesInfo.entrySet()){
                toBeCopiedTable.add(entry.getKey());
            }
            return ResType.DROP_REGION_SUCCESS;
        }
        for(String tableName : delTablesInfo.keySet()){
            sortAndUpdate();
            String leastRegionName = getLeastRegionName(tableName);//防止同一region server存储了母本和副本
            if(leastRegionName==null){
                toBeCopiedTable.add(tableName);
                continue;
            }
            Map<String, Integer> leastTablesInfo = regionsInfo.get(leastRegionName);
            Integer tableLoad = delTablesInfo.get(tableName);
            leastTablesInfo.put(tableName, tableLoad);
            regionsInfo.put(leastRegionName, leastTablesInfo);

            List<String> backInfos = getBackRegion(tableName);
            String backTableName = backInfos.get(0);
            String backRegionName = backInfos.get(1);
            copyTable(backRegionName,leastRegionName,backTableName,tableName);
            zooKeeperManager.addTable(leastRegionName, new TableInform(tableName,tableLoad));
        }
        sortAndUpdate();
        return ResType.DROP_REGION_SUCCESS;
    }


    public static List<String> getBackRegion(String tableName) {
        String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
        String slaveTableName = masterTableName + "_slave";
        List<String> backInfos = new ArrayList<>();
        if(tableName.equals(masterTableName)){//主表丢失找副本
            backInfos.add(slaveTableName);
            String regionName = zooKeeperManager.getRegionServer(slaveTableName);
            backInfos.add(regionName);
        }else{
            backInfos.add(masterTableName);
            String regionName = zooKeeperManager.getRegionServer(masterTableName);
            backInfos.add(regionName);
        }
        return backInfos;
    }

    /**
     * 创建新表，同时在不同的region server上创建一份副本
     */
    public static List<ResType> createTableMasterAndSlave(String tableName,String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.CREATE_TABLE_FAILURE);
        }else{
            res.add(ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.CREATE_TABLE_FAILURE);
        }else{
            res.add(ResType.CREATE_TABLE_ALREADY_EXISTS);
        }
        if(res.get(0) == ResType.CREATE_TABLE_ALREADY_EXISTS || res.get(1) == ResType.CREATE_TABLE_ALREADY_EXISTS){
            return res;
        }else {
            res = new ArrayList<>();
        }

        Iterator<Map.Entry<String, Integer>> iterator = regionsLoad.entrySet().iterator();

        if (iterator.hasNext()) {
            String masterRegionName = iterator.next().getKey(); //第一个region server用于存放master
            res.add(createTable(masterRegionName, tableName, sql));
            System.out.println("[RegionManager] create table "+tableName+" at region "+masterRegionName);
        }else{
            System.out.println("[RegionManager] fail create master_table");
            res.add(ResType.CREATE_TABLE_FAILURE);
        }
        String slaveTableName = tableName + "_slave";
        if (iterator.hasNext()) {
            String slaveRegionName = iterator.next().getKey(); //第二个region server用于存放slave
            sql = sql.replace(tableName, slaveTableName);
            res.add(createTable(slaveRegionName, slaveTableName, sql));
            System.out.println("[RegionManager] create table "+slaveTableName+" at region "+slaveRegionName);
        }else{
            System.out.println("[RegionManager] fail create slave_table");
            res.add(ResType.CREATE_TABLE_FAILURE);
        }

        if(res.get(0) == ResType.CREATE_TABLE_SUCCESS && res.get(1) == ResType.CREATE_TABLE_FAILURE){
            toBeCopiedTable.add(slaveTableName);
        }
        sortAndUpdate();//更新
        return res;
    }

    private static ResType createTable(String regionName, String tableName, String sql) {
        if(zooKeeperManager.addTable(regionName, new TableInform(tableName,0))){
            regionsInfo.get(regionName).put(tableName, 0);
            sendSqlCommandToRegion(regionName,sql);
            return ResType.CREATE_TABLE_SUCCESS;
        }else{
            return ResType.CREATE_TABLE_FAILURE;
        }
    }

    public static List<ResType> dropTableMasterAndSlave(String tableName, String sql) {
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.DROP_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.DROP_TABLE_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.DROP_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.DROP_TABLE_FAILURE);
        }
        if(res.get(0) == ResType.DROP_TABLE_NO_EXISTS && res.get(1) == ResType.DROP_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.DROP_TABLE_FAILURE) {
            res.set(0, dropTable(tableName, sql));
        }else{
            toBeCopiedTable.remove(tableName);
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.DROP_TABLE_FAILURE) {
            res.set(1, dropTable(slaveTableName, sql));
        }else{
            toBeCopiedTable.remove(slaveTableName);
        }

        sortAndUpdate();//更新
        return res;
    }

    private static ResType dropTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if(zooKeeperManager.deleteTable(tableName)) {
            regionsInfo.get(regionName).remove(tableName);
            sendSqlCommandToRegion(regionName,sql);
            return ResType.DROP_TABLE_SUCCESS;
        }else{
            return ResType.DROP_TABLE_FAILURE;
        }
    }

    public static SelectInfo selectTable(List<String> tableNames, String sql) {
        if(!checkAndRecoverData(tableNames)){
            return new SelectInfo(false);
        }
        Map<String, Integer> regionsWithLoad = getRegionsWithLoad(tableNames);
        String regionName = getLargestLoadRegion(regionsWithLoad);
        String retSql = replaceSql(regionName, tableNames, sql);
        return new SelectInfo(true, regionName, retSql);
    }

    private static boolean checkAndRecoverData(String tableName){
        List<String> tableNames = new ArrayList<>();
        tableNames.add(tableName);
        return checkAndRecoverData(tableNames);
    }

    private static boolean checkAndRecoverData(List<String> tableNames) {
        for(String tableName : tableNames){
            String slaveTableName = tableName + "_slave";
            List<ResType> checkList = findTableMasterAndSlave(tableName);
            if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS
                    && checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
                return false;//不可能成功,因为缺表
            }else if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){//母本丢失
                boolean flag = addSameTable(tableName, slaveTableName);
                if(!flag && !toBeCopiedTable.contains(tableName)) {
                    toBeCopiedTable.add(tableName);
                }else if(flag) {
                    toBeCopiedTable.remove(tableName);
                }
            }else if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){//副本丢失
                boolean flag = addSameTable(slaveTableName, tableName);
                if(!flag && !toBeCopiedTable.contains(slaveTableName)) {
                    toBeCopiedTable.add(slaveTableName);
                }else if(flag) {
                    toBeCopiedTable.remove(slaveTableName);
                }
            }
        }
        sortAndUpdate();
        return true;
    }

    private static Map<String, Integer> getRegionsWithLoad(List<String> tableNames){
        Map<String, Integer> regionsWithLoad = new LinkedHashMap<>();
        for(String tableName : tableNames){
            addLoad(regionsWithLoad, tableName);

            String slaveTableName = tableName + "_slave";
            addLoad(regionsWithLoad, slaveTableName);
        }
        return regionsWithLoad;
    }

    private static void addLoad(Map<String, Integer> regionsWithLoad, String tableName) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        if(regionName != null){
            Integer load = regionsInfo.get(regionName).get(tableName);
            if(regionsWithLoad.containsKey(regionName)) {
                load += regionsWithLoad.get(regionName);
            }
            regionsWithLoad.put(regionName, load);
        }
    }

    private static String getLargestLoadRegion(Map<String, Integer> regionsWithLoad){
        Integer maxLoad = 0;
        String regionName = "";
        for(Map.Entry<String, Integer> entry : regionsWithLoad.entrySet()){
            if(entry.getValue() >= maxLoad){
                maxLoad = entry.getValue();
                regionName = entry.getKey();
            }
        }
        return regionName;
    }

    private static String replaceSql(String regionName, List<String> tableNames, String sql) {
        Map<String, Integer> tables = regionsInfo.get(regionName);
        for(String tableName : tableNames){
            String slaveTableName = tableName + "_slave";
            if(!tables.containsKey(tableName)) {
                if (tables.containsKey(slaveTableName)) {
                    sql = sql.replace(tableName, slaveTableName);
                }else{
                    String delRegion = zooKeeperManager.getRegionServer(tableName);
                    Integer load = regionsInfo.get(delRegion).get(tableName);
                    //处理被移除表的region
                    Map<String, Integer> delRegionInfo = regionsInfo.get(delRegion);
                    delRegionInfo.remove(tableName);
                    regionsInfo.put(delRegion, delRegionInfo);
                    zooKeeperManager.deleteTable(tableName);
                    //处理移入表的region
                    tables.put(tableName, load);
                    zooKeeperManager.addTable(regionName, new TableInform(tableName,load));
                    migrateTable(delRegion,regionName,tableName);
                }
            }
        }
        regionsInfo.put(regionName, tables);
        return sql;
    }

    public static List<ResType> accTableMasterAndSlave(String tableName, String sql) {
        checkAndRecoverData(tableName);
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.INSERT_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.INSERT_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.INSERT_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.INSERT_FAILURE);
        }
        if(res.get(0) == ResType.INSERT_TABLE_NO_EXISTS && res.get(1) == ResType.INSERT_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.INSERT_FAILURE) {
            res.set(0, accTable(tableName, sql));
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.INSERT_FAILURE) {
            res.set(1, accTable(slaveTableName, sql));
        }

        sortAndUpdate();
        return res;
    }

    private static ResType accTable(String tableName, String sql) {
        if(zooKeeperManager.accTablePayload(tableName)){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)+1);
            sendSqlCommandToRegion(regionName, sql);
            return ResType.INSERT_SUCCESS;
        }else{
            return ResType.INSERT_FAILURE;
        }
    }

    public static List<ResType> decTableMasterAndSlave(String tableName, String sql) {
        checkAndRecoverData(tableName);
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.DELECT_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.DELECT_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.DELECT_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.DELECT_FAILURE);
        }
        if(res.get(0) == ResType.DELECT_TABLE_NO_EXISTS && res.get(1) == ResType.DELECT_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.DELECT_FAILURE) {
            res.set(0, decTable(tableName, sql));
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.DELECT_FAILURE) {
            res.set(1, decTable(slaveTableName, sql));
        }

        sortAndUpdate();
        return res;
    }

    private static ResType decTable(String tableName, String sql) {
        if(zooKeeperManager.decTablePayload(tableName)){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)-1);
            sendSqlCommandToRegion(regionName, sql);
            return ResType.DELECT_SUCCESS;
        }else{
            return ResType.DELECT_FAILURE;
        }
    }

    public static List<ResType> updateTableMasterAndSlave(String tableName, String sql) {
        checkAndRecoverData(tableName);
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.UPDATE_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.UPDATE_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.UPDATE_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.UPDATE_FAILURE);
        }
        if(res.get(0) == ResType.UPDATE_TABLE_NO_EXISTS && res.get(1) == ResType.UPDATE_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.UPDATE_FAILURE) {
            res.set(0, updateTable(tableName, sql));
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.UPDATE_FAILURE) {
            res.set(1, updateTable(slaveTableName, sql));
        }

        sortAndUpdate();
        return res;
    }

    private static ResType updateTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)-1);
        sendSqlCommandToRegion(regionName, sql);
        return ResType.UPDATE_SUCCESS;
    }

    public static List<ResType> alterTableMasterAndSlave(String tableName, String sql) {
        checkAndRecoverData(tableName);
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.ALTER_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.ALTER_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.ALTER_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.ALTER_FAILURE);
        }
        if(res.get(0) == ResType.ALTER_TABLE_NO_EXISTS && res.get(1) == ResType.ALTER_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.ALTER_FAILURE) {
            res.set(0, alterTable(tableName, sql));
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.ALTER_FAILURE) {
            res.set(1, alterTable(slaveTableName, sql));
        }

        sortAndUpdate();
        return res;
    }

    private static ResType alterTable(String tableName, String sql) {
        String regionName = zooKeeperManager.getRegionServer(tableName);
        regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)-1);
        sendSqlCommandToRegion(regionName,sql);
        return ResType.ALTER_SUCCESS;
    }

    public static List<ResType> findTableMasterAndSlave(String tableName) {
        List<ResType> res = new ArrayList<>();
        res.add(findTable(tableName));
        res.add(findTable(tableName + "_slave"));
        return res;
    }

    private static ResType findTable(String tableName) {
        if(zooKeeperManager.getRegionServer(tableName)==null){
            return ResType.FIND_TABLE_NO_EXISTS;
        }else{
            return ResType.FIND_SUCCESS;
        }
    }

    public static List<ResType> truncateTableMasterAndSlave(String tableName, String sql) {
        checkAndRecoverData(tableName);
        List<ResType> checkList = findTableMasterAndSlave(tableName);
        List<ResType> res = new ArrayList<>();
        if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.TRUNCATE_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.TRUNCATE_FAILURE);
        }
        if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
            res.add(ResType.TRUNCATE_TABLE_NO_EXISTS);
        }else{
            res.add(ResType.TRUNCATE_FAILURE);
        }
        if(res.get(0) == ResType.TRUNCATE_TABLE_NO_EXISTS && res.get(1) == ResType.TRUNCATE_TABLE_NO_EXISTS){
            return res;
        }

        if(res.get(0) == ResType.TRUNCATE_FAILURE) {
            res.set(0, truncateTable(tableName, sql));
        }

        String slaveTableName = tableName + "_slave";
        sql = sql.replace(tableName, slaveTableName);
        if(res.get(1) == ResType.TRUNCATE_FAILURE) {
            res.set(1, truncateTable(slaveTableName, sql));
        }
        sortAndUpdate();
        return res;
    }

    private static ResType truncateTable(String tableName, String sql) {
        if(zooKeeperManager.setTablePayload(tableName, 0)){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, 0);
            sendSqlCommandToRegion(regionName,sql);
            return ResType.TRUNCATE_SUCCESS;
        }else{
            return ResType.TRUNCATE_FAILURE;
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
                System.out.println("[RegionManager] Handler for region " + regionName + " not found/ready (attempt "
                        + attempt + "/" + maxRetries + "). Retrying in " + retryDelayMs + "ms...");
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
            System.err.println("[RegionManager] Cannot send command: No active handler found for region " + regionName
                    + " after " + maxRetries + " attempts.");
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
     * Example: "COPY_TABLE <src_host> <src_port> <src_user> <src_pwd> <table_name> <dst_table> <dst_table> <dst_table>"
     *
     * @param sourceRegionName Name/IP of the source region.
     * @param targetRegionName Name/IP of the target region (where command is sent).
     * @param sourceTableName  Table name on the source.
     * @param targetTableName  Desired table name on the target.
     * @return true if the command was sent successfully.
     */
    public static boolean copyTable(String sourceRegionName, String targetRegionName, String sourceTableName, String targetTableName) {
        // 1. Get source connection details from ZK
        String sourceConnectionString = getRegionConnectionString(sourceRegionName, true); // Get MySQL details
        if (sourceConnectionString == null) {
            System.err.println("[RegionManager] Cannot COPY: Failed to get source connection details for " + sourceRegionName);
            return false;
        }else{
            System.out.println("[RegionManager] src: " + sourceConnectionString);
        }
        String[] sourceDetails = sourceConnectionString.split(","); // ip,port,user,pwd

        String dstConnectionString = getRegionConnectionString(targetRegionName, true); // Get MySQL details
        if (dstConnectionString == null) {
            System.err.println("[RegionManager] Cannot COPY: Failed to get source connection details for " + targetRegionName);
            return false;
        }else{
            System.out.println("[RegionManager] dst: " + dstConnectionString);
        }
        String[] dstDetails = dstConnectionString.split(","); // ip,port,user,pwd

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
        // Example: COPY_TABLE <src_ip> <src_port> <src_user> <src_pwd> <source_table_name> <dst_ip> <dst_port> <dst_user> <dst_pwd> <target_table_name>
        String command = String.format("COPY_TABLE %s %s %s %s %s %s %s %s %s %S",
                sourceRegionName,
                sourceDetails[1],
                sourceDetails[2],
                sourceDetails[3],
                sourceTableName,
                targetRegionName,
                dstDetails[1],
                dstDetails[2],
                dstDetails[3],
                targetTableName);

        // 4. Process the command
        try {
            if (handleCopyTableCommand(command)) {
                System.out.println("[RegionManager] COPY_TABLE_SUCCESS!");
            } else {
                System.err.println("[RegionManager] COPY_TABLE_FAILURE.");
            }
            return true; // Assume send success for now
        } catch (Exception e) {
            System.err.println("[RegionManager] Error sending COPY command to region " + targetRegionName + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * 处理 Master 发送的自定义 COPY_TABLE 命令.
     * 格式: COPY_TABLE <src_host> <src_port> <src_user> <src_pwd> <src_table> <dst_host> <dst_port> <dst_user> <dst_pwd> <dst_table>
     * @param command 命令字符串
     * @return 是否成功启动复制过程
     */
    private static boolean handleCopyTableCommand(String command) {
        String prefix = "COPY_TABLE ";
        if (!command.startsWith(prefix)) {
            return false;
        }
        String[] parts = command.substring(prefix.length()).split("\\s+");
        if (parts.length != 10) {
            System.err.println("[RegionServer Process] Invalid COPY_TABLE command format: " + command);
            return false;
        }
        String srcHost = parts[0];
        String srcPort = parts[1];
        String srcUser = parts[2];
        String srcPwd = parts[3];
        String srcTable = parts[4];

        String dstHost = parts[5];
        String dstPort = parts[6];
        String dstUser = parts[7];
        String dstPwd = parts[8];
        String dstTable = parts[9];


        System.out.println("[RegionServer Process] Received request to copy table " + srcTable
                + " from " + srcHost + ":" + srcPort + " to local table " + dstTable);
        boolean copySuccess = copyTableBetweenMySQL(
                srcHost, srcPort, srcUser, srcPwd, srcTable,
                dstHost, dstPort, dstUser, dstPwd
        );

        if (copySuccess) {
            System.out.println("[RegionServer Process] Table copy initiated successfully for " + dstTable);
            // 在 ServerMaster.dumpTable 成功后，可以选择性地修改表名（如果需要）
            if (!srcTable.equals(dstTable)) {
                System.out.println("[RegionServer Process] Renaming imported table " + srcTable + " to " + dstTable);
                String renameSql = "RENAME TABLE `" + srcTable + "` TO `" + dstTable + "`;";
                try {
                    sendSqlCommandToRegion(dstHost,renameSql); // 使用本地执行
                    System.out.println("[RegionServer Process] Table renamed successfully.");
                } catch (Exception e) {
                    System.err.println("[RegionServer Process] Failed to rename table after copy: " + e.getMessage());
                    // 即使重命名失败，复制也可能部分成功，但返回 false 表示整个操作未完全成功
                    return false;
                }
            }
        } else {
            System.err.println("[RegionServer Process] Table copy initiation failed for " + dstTable);
        }
        return copySuccess;
    }

    /**
     * Sends a command to initiate table migration. Could involve multiple steps/commands.
     * Example: Tell target to pull, then tell source to drop.
     * @param srcRegionName Name/IP of the source.
     * @param dstRegionName Name/IP of the target.
     * @param tableName     Table to migrate.
     */
    private static void migrateTable(String srcRegionName, String dstRegionName, String tableName) {
        // 1. Get source connection details from ZK
        String srcConnectionString = getRegionConnectionString(srcRegionName, true); // Get MySQL details
        if (srcConnectionString == null) {
            System.err.println("[RegionManager] Cannot MIGRATE: Failed to get source connection details for " + srcRegionName);
            return ;
        }else{
            System.out.println("[RegionManager] src: " + srcConnectionString);
        }
        String[] srcDetails = srcConnectionString.split(","); // ip,port,user,pwd

        String dstConnectionString = getRegionConnectionString(dstRegionName, true); // Get MySQL details
        if (dstConnectionString == null) {
            System.err.println("[RegionManager] Cannot MIGRATE:: Failed to get source connection details for " + dstRegionName);
            return ;
        }else{
            System.out.println("[RegionManager] dst: " + dstConnectionString);
        }
        String[] dstDetails = dstConnectionString.split(","); // ip,port,user,pwd

        // 2. 构建 "MIGRATE_TABLE_FROM_SOURCE" 命令
        // 格式: MIGRATE_TABLE_FROM_SOURCE <src_ip> <src_port> <src_user> <src_pwd> <table_name> <dst_ip> <dst_port> <dst_user> <dst_pwd>
        String command = String.format("MIGRATE_TABLE_FROM_SOURCE %s %s %s %s %s %s %s %s %s",
                srcDetails[0], // sourceRegionName (作为 host_ip)
                srcDetails[1], // sourceMysqlPort
                srcDetails[2], // sourceMysqlUser
                srcDetails[3], // sourceMysqlPwd (安全风险)
                tableName,
                dstDetails[0],
                dstDetails[1],
                dstDetails[2],
                dstDetails[3]);

        if (handleFullMigrateCommand(command)) {
            System.out.println("[RegionManager] MIGRATE_TABLE_SUCCESS!");
        } else {
            System.err.println("[RegionManager] MIGRATE_TABLE_FAILURE.");
        }
    }

    /**
     * 处理 Master 发送的完整迁移命令。
     * 格式: MIGRATE_TABLE_FROM_SOURCE <src_host> <src_port> <src_user> <src_pwd> <table_name> <dst_host> <dst_port> <dst_user> <dst_pwd>
     */
    private static boolean handleFullMigrateCommand(String command) {
        String prefix = "MIGRATE_TABLE_FROM_SOURCE ";
        if (!command.startsWith(prefix)) {
            System.err.println("[RegionServer Process] MIGRATE_TABLE_FROM_SOURCE 命令格式错误 (前缀不匹配): " + command);
            return false;
        }
        String params = command.substring(prefix.length());
        String[] parts = params.split("\\s+"); // 按空格分割参数
        if (parts.length != 9) {
            System.err.println("[RegionServer Process] MIGRATE_TABLE_FROM_SOURCE 命令参数数量错误 (期望9个): " + params);
            return false;
        }
        String srcHost = parts[0];
        String srcPort = parts[1];
        String srcUser = parts[2];
        String srcPwd = parts[3];
        String tableName = parts[4];
        String dstHost = parts[5];
        String dstPort = parts[6];
        String dstUser = parts[7];
        String dstPwd = parts[8];

        System.out.println("[RegionServer Process] 收到完整迁移请求: 表 " + tableName + " 从源 " + srcHost + ":" + srcPort);

        // 调用 ServerMaster.executeCompleteMigrationSteps
        // 它会：1. dumpTable (从源拉取并导入本地) 2. 连接到源并删除源表
        return executeCompleteMigrationSteps(
                srcHost, srcPort, srcUser, srcPwd, // 源数据库连接信息
                tableName,
                dstHost, dstPort, dstUser, dstPwd
        );
    }

    /**
     * (此方法在RegionServer端执行，由Master通过命令触发)
     * 将数据表从src Server剪切一份表数据给 dst Region Server
     * 注意：ZK元数据更新由Master负责。此方法只负责数据层面迁移。
     */
    public static boolean executeCompleteMigrationSteps(String srcHost, String srcPort,
                                                        String srcUser, String srcPwd,
                                                        String tableName,
                                                        String dstHost, String dstPort,
                                                        String dstUser, String dstPwd) {
        System.out.println("[ServerMaster@"+ RegionServer.ip+"] 开始执行完整迁移步骤，表: " + tableName + " 从源: " + srcHost);

        // 从src Server复制一份表数据给 dst Region Server
        boolean dumpImportSuccess = copyTableBetweenMySQL(srcHost, srcPort, srcUser, srcPwd,
                tableName, dstHost, dstPort, dstUser, dstPwd);

        if (!dumpImportSuccess) {
            System.err.println("[RegionManager] 完整迁移失败：数据复制步骤 (dumpTable) 失败，表: " + tableName);
            return false;
        }
        System.out.println("[RegionManager] 完整迁移：数据复制成功，表: " + tableName);

        String dropSql = "DROP TABLE " + tableName ;
        System.out.println("[RegionManager] 在源 (" + srcHost + ") 上执行: " + dropSql);
        sendSqlCommandToRegion(srcHost, dropSql);
        System.out.println("[RegionManager] 表 '" + tableName + "' 已成功从源 " + srcHost + " 的数据库中删除。");
        return true; // 整个迁移步骤成功
    }

    public static boolean copyTableBetweenMySQL(String srcHost, String srcPort, String srcUser, String srcPwd,
                                                String tableName, String dstHost, String dstPort, String dstUser, String dstPwd) {
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("mysqldump_", ".sql");

            // mysqldump 命令，写入本地文件
            List<String> dumpCmd = Arrays.asList(
                    "mysqldump",
                    "-h", srcHost,
                    "-P", srcPort,
                    "-u", srcUser,
                    "-p" + srcPwd,
                    "--single-transaction",
                    "--skip-tz-utc",
                    TARGET_DB_NAME,
                    tableName,
                    "--result-file=" + tmpFile.getAbsolutePath()
            );

            System.out.println("执行 mysqldump 命令: " + String.join(" ", dumpCmd));
            ProcessBuilder dumpBuilder = new ProcessBuilder(dumpCmd);
            dumpBuilder.redirectErrorStream(true);
            Process dumpProcess = dumpBuilder.start();

            // 打印 mysqldump 输出（方便调试）
            printStreamAsync("mysqldump", dumpProcess.getInputStream());

            int dumpExit = dumpProcess.waitFor();
            if (dumpExit != 0) {
                System.err.println("mysqldump 执行失败，退出码：" + dumpExit);
                return false;
            }

            System.out.println("mysqldump 成功，文件路径：" + tmpFile.getAbsolutePath());

            // mysql 导入命令，从文件导入
            List<String> importCmd = Arrays.asList(
                    "mysql",
                    "-h", dstHost,
                    "-P", dstPort,
                    "-u", dstUser,
                    "-p" + dstPwd,
                    TARGET_DB_NAME,
                    "-e", "source " + tmpFile.getAbsolutePath()
            );

            System.out.println("执行 mysql 导入命令: " + String.join(" ", importCmd));
            ProcessBuilder importBuilder = new ProcessBuilder(importCmd);
            importBuilder.redirectErrorStream(true);
            Process importProcess = importBuilder.start();

            printStreamAsync("mysql", importProcess.getInputStream());

            int importExit = importProcess.waitFor();
            if (importExit != 0) {
                System.err.println("mysql 导入失败，退出码：" + importExit);
                return false;
            }

            System.out.println("✅ 成功将表 " + tableName + " 从 " + srcHost + " 复制到 " + dstHost);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (tmpFile != null && tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    private static void printStreamAsync(String prefix, InputStream inputStream) {
        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[" + prefix + "] " + line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
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
}