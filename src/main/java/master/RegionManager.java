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

    public static Integer loadsSum = 0, loadsAvg = 0;

    /**
     * 初始化，从ZooKeeper处获取nodes数据
     */
    public static void init() throws Exception {
        zooKeeperManager = new ZooKeeperManager();
        zooKeeperUtils = zooKeeperManager.zooKeeperUtils;
        regionsInfo = getRegionsInfo();
        update();
        masterInfo = zooKeeperUtils.getData("/lss/master");
    }

    /**
     * 获取当前所有region node的情况
     * 记录region server下的表名以及表对应的load
     */
    public static Map<String, Map<String, Integer>> getRegionsInfo() throws Exception {
        Map<String, Map<String, Integer>> newLegionsInfo = new LinkedHashMap<>();
        List<String> regionNames = zooKeeperUtils.getChildren("/lss/region_servers");//获取下所有Region节点
        for(String regionName : regionNames){//获取节点上的数据
            Map<String, Integer> tablesInfo = new LinkedHashMap<>();
            List<String> tableNames = zooKeeperUtils.getChildren("/lss/region_servers/" + regionName);
            for(String tableName : tableNames){
                Integer load = Integer.valueOf(zooKeeperUtils.getData("/lss/region_servers/" + regionName + "table/" + tableName));
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

    /** 获取总负荷，用于平衡不同region server的负荷
     * @return regions servers的总负荷
     */
    public static Integer getLoadsSum() {
        Integer sum = 0;
        for (Map.Entry<String,Integer> entry : regionsLoad.entrySet()) {
            sum += entry.getValue();
        }
        return sum;
    }

    public static void update(){
        Map<String, Map<String, Integer>> sortedRegionsInfo = new LinkedHashMap<>();
        for(String regionName : regionsInfo.keySet()){
            sortedRegionsInfo.put(regionName, sortLoadDsc(regionsInfo.get(regionName)));
        }
        regionsInfo = sortedRegionsInfo;
        regionsLoad = getRegionsLoad();
        loadsSum = getLoadsSum();
        loadsAvg = loadsSum/regionsInfo.size();
    }
    /**
     * 排序，按负载降序排序优先处理，
     * 便于负载小的region server先接收load最大的table
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

    //降序排序
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
     * 寻找负载最小的region server名字
     * @return 如果有节点，则返回最小节点对应的名字；如果没有节点，返回null
     */
    public static String getLeastRegionName() {
        if (!regionsLoad.isEmpty()) {
            // regionsLoad是排好序的，直接取第一个
            Map.Entry<String, Integer> leastRegionLoad = regionsLoad.entrySet().stream().findFirst().get();
            return leastRegionLoad.getKey();
        }else{
            return null;
        }

    }

    //寻找负载最大的region server名字
    public static String getLargestRegionName(){
        if (!regionsLoad.isEmpty()) {
            // regionsLoad是排好序的，直接最后一个
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(regionsLoad.entrySet());
            Map.Entry<String, Integer> largestRegion = entries.get(entries.size() - 1);
            return largestRegion.getKey();
        }else {
            return null;
        }
    }

    /**
     * 寻找大于给定负荷的最小负荷的表
     * @return 如果有表，则返回表对应的Info; 如果没有节点,返回null
     */
    public static Map<String, Integer> getLargerTableInfo(String regionName, Integer load) {
        Map<String, Integer> tablesInfo = regionsInfo.get(regionName);
        if (tablesInfo == null || tablesInfo.isEmpty()) {
            return null;
        }
        Map<String, Integer> result = null;
        for (Map.Entry<String, Integer> entry : tablesInfo.entrySet()) {
            Integer tableLoad = entry.getValue();
            if (tableLoad >= load) {
                String tableName = entry.getKey();
                // 如果第一次符合，或者找到更小的满足条件的表
                if (result == null || tableLoad < result.values().iterator().next()) {
                    result = new LinkedHashMap<>();
                    result.put(tableName, tableLoad);
                }
            }
        }
        if(result != null){
            tablesInfo.remove( result.keySet().iterator().next());
            update();
        }
        return result;
    }

    //ZooKeeper在region servers有变动的时候，调用下方函数

    /**
     * 新添加一个region server(我们认为是空的，所以无需记录或传递新region server内的数据）
     * 将其他region server上的数据表匀给该region server上
     */
    public static ResType addRegion(String addRegionName) {
        if(!regionsInfo.containsKey(addRegionName)){
            return ResType.ADD_REGION_ALREADY_EXISTS;
        }
        update();
        loadsAvg = getLoadsSum()/(regionsInfo.size()+1);//为它提前考虑

        Map<String, Integer> addTablesInfo = new LinkedHashMap<>();
        Integer addRegionloadSum = 0;
        while (addRegionloadSum < loadsAvg) {
            update();
            String largestRegionName = getLargestRegionName();
            Map<String, Integer> largerRegionTables = regionsInfo.get(largestRegionName);
            if (largerRegionTables == null || largerRegionTables.size() <= 1){// 说明这个region不能再迁了
                break;
            }
            Integer aimLoad = Math.max(loadsAvg - addRegionloadSum, regionsLoad.get(largestRegionName) - loadsAvg);
            Map<String, Integer> largestTableInfo = getLargerTableInfo(largestRegionName, aimLoad);
            String tableNameToMove;
            if (largestTableInfo != null) {
                tableNameToMove = largestTableInfo.keySet().iterator().next();
            }else{
                break;
            }
            Integer tableLoadToMove = largestTableInfo.get(tableNameToMove);

            // TODO: 这里写迁移逻辑，把表从largestRegion迁到targetRegion

            //处理表迁出的region server
            largerRegionTables.remove(tableNameToMove);
            regionsInfo.put(largestRegionName, largerRegionTables);
            //处理表迁入的region server
            addRegionloadSum += tableLoadToMove;
            addTablesInfo.put(tableNameToMove, tableLoadToMove);
        }
        regionsInfo.put(addRegionName, addTablesInfo);
        update();
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
        regionsInfo.remove(delRegionName);
        for(String tableName : delTablesInfo.keySet()){
            update();
            String leastRegionName = getLeastRegionName();
            Map<String, Integer> leastTablesInfo = regionsInfo.get(leastRegionName);
            leastTablesInfo.put(tableName, delTablesInfo.get(tableName));
        }
        update();
        return ResType.DROP_REGION_SUCCESS;
    }

    public static List<ResType> addTableMasterAndSlave(String tableName) {
        update();//更新信息
        Iterator<Map.Entry<String, Map<String, Integer>>> iterator = regionsInfo.entrySet().iterator();

        List<ResType> res = new ArrayList<>();
        if (iterator.hasNext()) {
            String masterRegionName = iterator.next().getKey(); //第一个region server用于存放master
            res.add(addTable(masterRegionName, tableName));
        }else{
            res.add(ResType.CREATE_TABLE_FAILURE);
        }
        if (iterator.hasNext()) {
            String slaveRegionName = iterator.next().getKey(); //第二个region server用于存放slave
            res.add(addTable(slaveRegionName, tableName + "_slave"));
        }else{
            res.add(ResType.CREATE_TABLE_FAILURE);
        }
        update();//更新
        return res;
    }

    private static ResType addTable(String regionName, String tableName) {
        if(zooKeeperManager.addTable(regionName, new TableInform(tableName,0))){
            regionsInfo.get(regionName).put(tableName, 0);
            return ResType.CREATE_TABLE_SUCCESS;
        }else{
            return ResType.CREATE_TABLE_FAILURE;
        }
    }

    public static List<ResType> dropTableMasterAndSlave(String tableName) {
        update();//更新信息

        List<ResType> res = new ArrayList<>();
        res.add(dropTable(tableName));
        res.add(dropTable(tableName + "_slave"));

        update();//更新
        return res;
    }

    private static ResType dropTable(String tableName) {
        if(zooKeeperManager.deleteTable(tableName)) {
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).remove(tableName);
            return ResType.DROP_TABLE_SUCCESS;
        }else{
            return ResType.DROP_TABLE_FAILURE;
        }
    }

    public static List<ResType> accTableMasterAndSlave(String tableName) {
        List<ResType> res = new ArrayList<>();
        res.add(accTable(tableName));
        res.add(accTable(tableName + "_slave"));
        update();
        return res;
    }

    private static ResType accTable(String tableName) {
        if(zooKeeperManager.accTablePayload(tableName)){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)+1);
            return ResType.INSERT_SUCCESS;
        }else{
            if(zooKeeperManager.getRegionServer(tableName)==null){
                return ResType.INSERT_TABLE_NO_EXISTS;
            }else{
                return ResType.INSERT_FAILURE;
            }
        }
    }

    public static List<ResType> decTableMasterAndSlave(String tableName) {
        List<ResType> res = new ArrayList<>();
        res.add(decTable(tableName));
        res.add(decTable(tableName + "_slave"));
        update();
        return res;
    }

    private static ResType decTable(String tableName) {
        if(zooKeeperManager.decTablePayload(tableName)){
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, regionsInfo.get(regionName).get(tableName)-1);
            return ResType.DELECT_SUCCESS;
        }else{
            if(zooKeeperManager.getRegionServer(tableName)==null){
                return ResType.DELECT_TABLE_NO_EXISTS;
            }else{
                return ResType.DELECT_FAILURE;
            }
        }
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

    public static List<ResType> truncateTableMasterAndSlave(String tableName) {
        List<ResType> res = new ArrayList<>();
        res.add(truncateTable(tableName));
        res.add(truncateTable(tableName + "_slave"));
        update();
        return res;
    }

    private static ResType truncateTable(String tableName) {
        if(zooKeeperManager.decTablePayload(tableName)){//TODO:设置ZooKeeper负载的函数
            String regionName = zooKeeperManager.getRegionServer(tableName);
            regionsInfo.get(regionName).put(tableName, 0);
            return ResType.TRUNCATE_SUCCESS;
        }else{
            if(zooKeeperManager.getRegionServer(tableName)==null){
                return ResType.TRUNCATE_TABLE_NO_EXISTS;
            }else{
                return ResType.TRUNCATE_FAILURE;
            }
        }
    }
}