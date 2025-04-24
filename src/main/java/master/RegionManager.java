/*
    维护region server信息
    相关操作:① 从ZooKeeper获取节点信息；
            ② 调用region server的接口，迁移region servers上的数据
    提供接口：为ZooKeeper提供节点负载分配策略
 */
package master;

import java.util.*;
import java.util.stream.Collectors;

import zookeeper.TableInform;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

public class RegionManager{
    public static ZooKeeperManager zooKeeperManager;
    public static ZooKeeperUtils zooKeeperUtils = null;
    public static String masterInfo = null;//master node数据

    //region name->table names, table name->table load
    public static Map<String, Map<String, Integer>> regionsInfo = null;

    //region name->region load
    public static Map<String, Integer> regionsLoad = null;

    public static List<String> toBeCopiedTable = null;

    public static Integer loadsSum = 0, loadsAvg = 0;

    //信息同步，损毁迁移等

    /**
     * 初始化，从ZooKeeper处获取nodes数据
     * --测试通过--
     */
    public static void init() throws Exception {
        zooKeeperManager = new ZooKeeperManager();
        zooKeeperManager.setWatch("/lss/region_server");
        zooKeeperUtils = zooKeeperManager.zooKeeperUtils;
        regionsInfo = getRegionsInfo();
        toBeCopiedTable = new ArrayList<>();
        sortAndUpdate();
        masterInfo = zooKeeperUtils.getData("/lss/master");
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
        regularRecoverData();
        Map<String, Map<String, Integer>> sortedRegionsInfo = new LinkedHashMap<>();
        for(String regionName : regionsInfo.keySet()){
            sortedRegionsInfo.put(regionName, sortLoadDsc(regionsInfo.get(regionName)));
        }
        regionsInfo = sortedRegionsInfo;
        regionsLoad = getRegionsLoad();
        loadsSum = getLoadsSum();
        loadsAvg = loadsSum/regionsInfo.size();
    }

    private static void regularRecoverData(){
        for(String tableName : toBeCopiedTable){
            String masterTableName = tableName.endsWith("_slave") ? tableName.substring(0, tableName.length() - 6) : tableName;
            String slaveTableName = masterTableName + "_slave";
            List<ResType> checkList = findTableMasterAndSlave(masterTableName);
            if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS
                && checkList.get(1) == ResType.FIND_SUCCESS){//母本丢失
                addSameTable(masterTableName, slaveTableName);
                toBeCopiedTable.remove(tableName);
            }else if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){//副本丢失
                addSameTable(slaveTableName, masterTableName);
                toBeCopiedTable.remove(tableName);
            }
        }
    }

    private static void addSameTable(String addTableName, String templateTableName){
        //从被拷贝处取信息
        String templateRegionName = zooKeeperManager.getRegionServer(templateTableName);
        Integer load = regionsInfo.get(templateRegionName).get(templateTableName);
        //新建
        String addRegionName = getLeastRegionName(templateTableName);
        regionsInfo.get(addRegionName).put(addTableName, load);
        //通知ZooKeeper
        zooKeeperManager.addTable(addRegionName, new TableInform(addTableName,load));
        sortAndUpdate();
        //TODO:从slaveRegionName复制表到regionName
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
        while (addRegionLoadSum < avgLoad) {
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

            // TODO: 这里写迁移逻辑，把表从largestRegion迁到targetRegion

            //处理表迁出的region server
            largerRegionTables.remove(tableNameToMove);
            regionsInfo.put(largestRegionName, largerRegionTables);
            //处理表迁入的region server
            addRegionLoadSum += tableLoadToMove;
            addTablesInfo.put(tableNameToMove, tableLoadToMove);
        }
        regionsInfo.put(addRegionName, addTablesInfo);
        sortAndUpdate();
        return ResType.ADD_REGION_SUCCESS;
    }

    /**
     * 为某一个被删除的region server善后
     * 将该region server上的数据表重分配到其他region server上
     */
    public static ResType delRegion(String delRegionName) {
        if (!regionsInfo.containsKey(delRegionName)) {
            return ResType.DROP_REGION_NO_EXISTS;
        }
        Map<String, Integer> delTablesInfo = regionsInfo.get(delRegionName);
        regionsInfo.remove(delRegionName);//首先移除被删除region server，防止参与分配tables
        for(String tableName : delTablesInfo.keySet()){
            sortAndUpdate();
            String leastRegionName = getLeastRegionName(tableName);//防止同一region server存储了母本和副本
            Map<String, Integer> leastTablesInfo = regionsInfo.get(leastRegionName);
            Integer tableLoad = delTablesInfo.get(tableName);
            leastTablesInfo.put(tableName, tableLoad);
            regionsInfo.put(leastRegionName, leastTablesInfo);
            //TODO:迁移操作
        }
        sortAndUpdate();
        return ResType.DROP_REGION_SUCCESS;
    }

    /**
     * 创建新表，同时在不同的region server上创建一份副本
     *
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

        Iterator<Map.Entry<String, Map<String, Integer>>> iterator = regionsInfo.entrySet().iterator();

        if (iterator.hasNext()) {
            String masterRegionName = iterator.next().getKey(); //第一个region server用于存放master
            res.add(createTable(masterRegionName, tableName, sql));
        }else{
            res.add(ResType.CREATE_TABLE_FAILURE);
        }
        String slaveTableName = tableName + "_slave";
        if (iterator.hasNext()) {
            String slaveRegionName = iterator.next().getKey(); //第二个region server用于存放slave
            sql = sql.replace(tableName, slaveTableName);
            res.add(createTable(slaveRegionName, slaveTableName, sql));
        }else{
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
            //TODO:实际操作sql语句
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
            //TODO:实际操作sql语句
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

    private static boolean checkAndRecoverData(List<String> tableNames) {
        for(String tableName : tableNames){
            String slaveTableName = tableName + "_slave";
            List<ResType> checkList = findTableMasterAndSlave(tableName);
            if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS
                    && checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){
                return false;//不可能成功,因为缺表
            }else if(checkList.get(0) == ResType.FIND_TABLE_NO_EXISTS){//母本丢失
                //从副本处取信息
                String slaveRegionName = zooKeeperManager.getRegionServer(slaveTableName);
                Integer load = regionsInfo.get(slaveRegionName).get(slaveTableName);
                //新建母本
                String regionName = getLeastRegionName(slaveTableName);
                regionsInfo.get(regionName).put(tableName, load);
                //TODO:从slaveRegionName复制表到regionName
            }else if(checkList.get(1) == ResType.FIND_TABLE_NO_EXISTS){//副本丢失
                //从母本处取信息
                String regionName = zooKeeperManager.getRegionServer(tableName);
                Integer load = regionsInfo.get(regionName).get(tableName);
                //新建副本
                String slaveRegionName = getLeastRegionName(tableName);
                regionsInfo.get(slaveRegionName).put(slaveTableName, load);
                //TODO:从regionName复制表到slaveRegionName
            }
        }
        sortAndUpdate();
        return true;
    }

    private static Map<String, Integer> getRegionsWithLoad(List<String> tableNames){
        Map<String, Integer> regionsWithLoad = new LinkedHashMap<>();
        for(String tableName : tableNames){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            if(regionName != null){
                Integer load = regionsInfo.get(regionName).get(tableName);
                if(regionsWithLoad.containsKey(regionName)) {
                    load += regionsWithLoad.get(regionName);
                }
                regionsWithLoad.put(regionName, load);
            }

            String slaveTableName = tableName + "_slave";
            String slaveRegionName = zooKeeperManager.getRegionServer(slaveTableName);
            if(slaveRegionName != null && regionName == null) {
                Integer load = regionsInfo.get(slaveRegionName).get(slaveTableName);
                if(regionsWithLoad.containsKey(slaveRegionName)) {
                    load += regionsWithLoad.get(slaveRegionName);
                }
                regionsWithLoad.put(slaveRegionName, load);
            }
        }
        return regionsWithLoad;
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
                    regionsInfo.get(delRegion).remove(tableName);
                    zooKeeperManager.deleteTable(tableName);
                    regionsInfo.get(regionName).put(tableName, load);
                    zooKeeperManager.addTable(regionName, new TableInform(tableName, load));
                    //TODO:迁移表
                }
            }
        }
        return sql;
    }

    public static List<ResType> accTableMasterAndSlave(String tableName, String sql) {
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
            //TODO:实际操作sql语句
            return ResType.INSERT_SUCCESS;
        }else{
            return ResType.INSERT_FAILURE;
        }
    }

    public static List<ResType> decTableMasterAndSlave(String tableName, String sql) {
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
            //TODO:实际操作sql语句
            return ResType.DELECT_SUCCESS;
        }else{
            return ResType.DELECT_FAILURE;
        }
    }

    public static List<ResType> updateTableMasterAndSlave(String tableName, String sql) {
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
        //TODO:实际操作sql语句
        return ResType.UPDATE_SUCCESS;
    }

    public static List<ResType> alterTableMasterAndSlave(String tableName, String sql) {
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
        //TODO:实际操作sql语句
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
            //TODO:实际操作sql语句
            return ResType.TRUNCATE_SUCCESS;
        }else{
            return ResType.TRUNCATE_FAILURE;
        }
    }
}