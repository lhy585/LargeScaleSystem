/*
    维护region server信息
    相关操作:① 从ZooKeeper获取节点信息；
            ② 调用region server的接口，迁移region servers上的数据
    提供接口：为ZooKeeper提供节点负载分配策略
 */
package master;

import java.util.*;
import java.util.stream.Collectors;

import zookeeper.ZooKeeperUtils;

public class RegionManager{
    public static String zooKeeperAddress = "192.168.246.136:2181";
    public static ZooKeeperUtils zooKeeperUtils = null;
    public static String masterInfo = null;//master node数据

    //region name->table names, table name->table load
    public static Map<String, List<Map<String, Integer>>> regionsInfo = null;

    //region name->region load
    public static Map<String, Integer> regionsLoad = null;

    /**
     * 初始化，从ZooKeeper处获取nodes数据
     */
    public static void init() throws Exception {
        zooKeeperUtils = new ZooKeeperUtils();
        zooKeeperUtils.connectZookeeper(zooKeeperAddress);
        regionsInfo = getRegionsInfo();
        regionsLoad = getRegionsLoad();
        masterInfo = zooKeeperUtils.getData("/lss/master");
    }

    /**
     * 获取当前所有region node的情况
     * 记录region server下的表名以及表对应的load
     */
    public static Map<String, List<Map<String, Integer>>> getRegionsInfo() throws Exception {
        Map<String, List<Map<String, Integer>>> newLegionsInfo = new HashMap<>();
        List<String> regionNames = zooKeeperUtils.getChildren("/lss/region_servers");//获取下所有Region节点
        for(String regionName : regionNames){//获取节点上的数据
            List<Map<String, Integer>> tablesInfo = new ArrayList<>();
            List<String> tableNames = zooKeeperUtils.getChildren("/lss/region_servers/" + regionName);
            for(String tableName : tableNames){
                Integer load = Integer.valueOf(zooKeeperUtils.getData("/lss/region_servers/" + regionName + "table/" + tableName));
                tablesInfo.add(Map.of(tableName, load));
            }
            newLegionsInfo.put(regionName, tableLoadAsc(tablesInfo));//给某个region server中的tables排序，按负载降序排序优先处理，
        }
        return newLegionsInfo;
    }

    /**
     * 给某个region server中的tables排序，按负载降序排序优先处理，
     * 便于load最大的table被负载小的region server先接收
     */
    public static List<Map<String, Integer>> tableLoadAsc(List<Map<String, Integer>> tablesInfo){
        tablesInfo.sort((m1, m2) -> {//按表load从高到低排序
            Integer v1 = m1.values().iterator().next();
            Integer v2 = m2.values().iterator().next();
            return Integer.compare(v2, v1);
        });
        return tablesInfo;
    }

    /**
     * 计算region server层级的load
     * @return Map<String, Integer>
     *         String为region server对应的ip+port(从ZooKeeper处获得）
     *         Integer为该region server对应的负载
     */
    public static Map<String, Integer> getRegionsLoad() {
        Map<String, Integer> newRegionsLoad = new HashMap<>();
        for (Map.Entry<String, List<Map<String, Integer>>> entry : regionsInfo.entrySet()) {
            String regionName = entry.getKey();
            List<Map<String, Integer>> tablesInfo = entry.getValue();
            Integer regionLoad = 0;
            for (Map<String, Integer> tableInfo : tablesInfo) {
                for (Integer tableLoad : tableInfo.values()) {
                    regionLoad += tableLoad;
                }
            }
            newRegionsLoad.put(regionName, regionLoad);
        }
        //排序，按负载升序排序优先处理，负载小的region server先接收load最大的table
        return regionsLoadDes(newRegionsLoad);
    }

    /**
     * 给region servers排序，按负载升序排序优先处理，
     * 便于负载小的region server先接收load最大的table
     */
    public static Map<String, Integer> regionsLoadDes(Map<String, Integer> regionsLoad){
        return regionsLoad.entrySet()
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
     * 寻找最小负荷的节点
     * @return 如果有节点，则返回最小节点对应的信息; 如果没有节点,返回null
     */
    public static String getLeastRegionName() {
        if(!regionsInfo.isEmpty()) {
            //每次都是排好序的，所以直接取第一个即可
            Map.Entry<String, List<Map<String, Integer>>> leastRegionInfo = regionsInfo.entrySet().stream().findFirst().get();
            return leastRegionInfo.getKey();
        }
        return null;
    }

    public static List<Map<String, Integer>> getLeastRegionData() {
        if(!regionsInfo.isEmpty()) {
            //每次都是排好序的，所以直接取第一个即可
            Map.Entry<String, List<Map<String, Integer>>> leastRegionInfo = regionsInfo.entrySet().stream().findFirst().get();
            return leastRegionInfo.getValue();
        }
        return null;
    }

    /**
     * 寻找最大负荷的表
     * @return 如果有表，则返回最大表对应的Info; 如果没有节点,返回null
     */
    public static Map<String, Integer> getLargestTableInfo(String regionName) {
        List<Map<String, Integer>> tablesInfo = regionsInfo.get(regionName);
        if(!tablesInfo.isEmpty()) {
            //每次都是排好序的，所以直接取第一个即可
            return tablesInfo.get(0);
        }
        return null;
    }

    /**
     * ZooKeeper在节点掉线或新增时，均调用该函数
     * 该函数会自动判断掉线或新增情况
     */
    public static void add() throws Exception {
        //更新数据
        Map<String, List<Map<String, Integer>>> newRegionsInfo = getRegionsInfo();
        Map<String, List<Map<String, Integer>>> lastRegionsInfo = regionsInfo;
        regionsInfo = new HashMap<>();
        //获取变更情况，包括删除和新增的region server清况
        Map<String, List<Map<String, Integer>>> addRegionsInfo = new HashMap<>(), delRegionsInfo;
        for(Map.Entry<String, List<Map<String, Integer>>> regionInfo : newRegionsInfo.entrySet()){
            String regionName = regionInfo.getKey();
            List<Map<String, Integer>> tablesInfo = regionInfo.getValue();
            if(!lastRegionsInfo.containsKey(regionName)){
                addRegionsInfo.put(regionName, tablesInfo);
            }else{
                regionsInfo.put(regionName, tablesInfo);
                lastRegionsInfo.remove(regionName);
            }
        }
        delRegionsInfo = lastRegionsInfo;
        //如果既有新加的region server也有删除的region server,可以用一个新增的region server承载一个删除的region server的数据
        for(Map.Entry<String, List<Map<String, Integer>>> addRegionInfo : addRegionsInfo.entrySet()){
            String regionName = addRegionInfo.getKey();
            if(!delRegionsInfo.isEmpty()){
                Map.Entry<String, List<Map<String, Integer>>> delRegionInfo = delRegionsInfo.entrySet().stream().findFirst().get();
                regionsInfo.put(regionName, delRegionInfo.getValue());//TODO:此处需要写日志，用于告知region server
                delRegionsInfo.remove(delRegionInfo.getKey());
            }else{
                break;
            }
        }
        //一对一处理过后，剩下的需要处理的只有可能全部是新增的region servers或者全部是删除的region servers
        if (!addRegionsInfo.isEmpty()) {//减少其他region server的负载
            for (Map.Entry<String, List<Map<String, Integer>>> addRegionInfo : addRegionsInfo.entrySet()) {
                String addRegionName = addRegionInfo.getKey();
                addRegion(addRegionName);//TODO:此处需要写日志，用于告知region server
            }
        } else if(!delRegionsInfo.isEmpty()){
            for (Map.Entry<String, List<Map<String, Integer>>> delRegionInfo : delRegionsInfo.entrySet()) {
                List<Map<String, Integer>> delRegionData = delRegionInfo.getValue();
                delRegion(delRegionData);//TODO:此处需要写日志，用于告知region server
            }
        }
    }

    /**
     * 新添加一个region server(我们认为是空的，所以无需记录或传递新region server内的数据）
     * 将其他region server上的数据表匀给该region server上
     */
    public static void addRegion(String addRegionName) {
        Integer load = 0;
        while(true){
            //String largestRegionName = ge
            //List<Map<String, Integer>>> largestRegion = getLeastRegion();
        }
    }

    /**
     * 为某一个被删除的region server善后
     * 将该region server上的数据表重分配到其他region server上
     */
    public static void delRegion(List<Map<String, Integer>> delRegionData) {

    }
}