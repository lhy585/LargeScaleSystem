/*
    维护region server信息
    相关操作:① 从ZooKeeper获取节点信息；
            ② 调用region server的接口，迁移region servers上的数据
    提供接口：为ZooKeeper提供节点负载分配策略
 */
package master;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import zookeeper.ZooKeeperManager;
import zookeeper.ZooKeeperUtils;

public class RegionManager{
    public static String zooKeeperAddress = "192.168.246.136:2181";
    public static ZooKeeperManager zooKeeperManager;
    public static ZooKeeperUtils zooKeeperUtils = null;
    public static String masterInfo = null;//master node数据

    //region name->table names, table name->table load
    public static Map<String, Map<String, Integer>> regionsInfo = null;

    //region name->region load
    public static Map<String, Integer> regionsLoad = null;

    public static Integer loadsSum = 0;

    /**
     * 初始化，从ZooKeeper处获取nodes数据
     */
    public static void init() throws Exception {
        zooKeeperManager = new ZooKeeperManager();
        zooKeeperUtils = zooKeeperManager.zooKeeperUtils;
        regionsInfo = getRegionsInfo();
        regionsLoad = getRegionsLoad();
        loadsSum = getLoadsSum();
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

    public static Integer getLoadsSum() {
        Integer sum = 0;
        for (Map.Entry<String,Integer> entry : regionsLoad.entrySet()) {
            sum += entry.getValue();
        }
        return sum;
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
     * 寻找最小负荷的节点
     * @return 如果有节点，则返回最小节点对应的信息; 如果没有节点,返回null
     */
    public static String getLeastRegionName() {
        if(!regionsInfo.isEmpty()) {
            //每次都是排好序的，所以直接取第一个即可
            Map.Entry<String, Map<String, Integer>> leastRegionInfo = regionsInfo.entrySet().stream().findFirst().get();
            return leastRegionInfo.getKey();
        }
        return null;
    }

    public static Map<String, Integer> getLeastRegionData() {
        if(!regionsInfo.isEmpty()) {
            //每次都是排好序的，所以直接取第一个即可
            Map.Entry<String, Map<String, Integer>> leastRegionInfo = regionsInfo.entrySet().stream().findFirst().get();
            return leastRegionInfo.getValue();
        }
        return null;
    }

    /**
     * 寻找最大负荷的表
     * @return 如果有表，则返回最大表对应的Info; 如果没有节点,返回null
     */
    public static Map<String, Integer> getLargestTableInfo(String regionName) {
        Map<String, Integer> tablesInfo = regionsInfo.get(regionName);
        if(tablesInfo != null && !tablesInfo.isEmpty()) {
            Map<String, Integer> tableInfo = new LinkedHashMap<>();
            Map.Entry<String, Integer> largestTableInfo = tablesInfo.entrySet().iterator().next();//每次都是排好序的，所以直接取第一个即可
            tableInfo.put(largestTableInfo.getKey(), largestTableInfo.getValue());
            return tableInfo;
        }
        return null;
    }

    //ZooKeeper在region servers有变动的时候，调用下方函数

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
    public static void delRegion(Map<String, Integer> delRegionData) {

    }
}