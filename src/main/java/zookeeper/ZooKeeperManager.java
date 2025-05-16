package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.List;

import static master.RegionManager.MASTER_IP;

public class ZooKeeperManager {
    public ZooKeeperUtils zooKeeperUtils;
    public String address = MASTER_IP; // 最好从配置文件读取 TODO:ip
    public String port = "2181";    // 最好从配置文件读取

    public ZooKeeperManager() {
        try {
            // zooKeeperUtils 的构造函数现在不带参数，或使用默认参数
            zooKeeperUtils = new ZooKeeperUtils(); // Assumes default constructor connects
            // zooKeeperUtils.connectZookeeper(address+":"+port); // 如果构造函数不自动连接，则取消此行注释
        } catch (Exception e) {
            e.printStackTrace();
            // 初始化失败时抛出异常，让上层知道
            throw new RuntimeException("Failed to initialize ZooKeeperManager", e);
        }
    }

    //regionserver连接到zookeeper时由regionserver调用***
    public boolean addRegionServer(String ip, String clientListenPort, List<TableInform> tables, String password, String username, String portToMasterListenOn, String mysqlPortForThisRS) {
        try {
            String regionBasePath = "/lss/region_server";
            String regionPath = regionBasePath + "/" + ip;
            String initialDataForParentNode = ip; // 或者更详细的描述性数据

            // 确保 /lss/region_server 存在
            zooKeeperUtils.ensurePathExists(regionBasePath);

            // 1. 处理主RegionServer节点 /lss/region_server/ip
            if (zooKeeperUtils.nodeExists(regionPath)) {
                System.out.println("[ZKManager] RegionServer node " + regionPath + " already exists. Verifying/Updating data and sub-nodes.");
                // 如果已存在，可以选择更新其数据，或按用户要求“不动原有的”则仅更新其子节点
                // zooKeeperUtils.setData(regionPath, initialDataForParentNode); // 例如更新数据
            } else {
                System.out.println("[ZKManager] Creating RegionServer node " + regionPath);
                zooKeeperUtils.createNode(regionPath, initialDataForParentNode);
            }

            // 2. 创建/更新子节点
            // 使用 CreateMode.PERSISTENT for persistent nodes
            // 使用 CreateMode.EPHEMERAL for ephemeral nodes

            String dataNodePath = regionPath + "/data";
            String initialDataContent = "master(received):\nregion(received):"; // 确保状态是received，表示没有待处理消息
            zooKeeperUtils.createOrSetData(dataNodePath, initialDataContent, CreateMode.PERSISTENT);

            String existNodePath = regionPath + "/exist";
            // 对于临时节点，如果因为某些原因（如上次会话未完全清理或路径被持久节点占用）已存在，
            // createOrSetData 会尝试处理。会话结束时它应该自动消失。
            zooKeeperUtils.createOrSetData(existNodePath, "alive", CreateMode.EPHEMERAL);


            zooKeeperUtils.createOrSetData(regionPath + "/port", clientListenPort, CreateMode.PERSISTENT); // RS 监听客户端的端口
            zooKeeperUtils.createOrSetData(regionPath + "/password", password, CreateMode.PERSISTENT);
            zooKeeperUtils.createOrSetData(regionPath + "/username", username, CreateMode.PERSISTENT);
            zooKeeperUtils.createOrSetData(regionPath + "/port2master", portToMasterListenOn, CreateMode.PERSISTENT); // Master监听RS的端口
            zooKeeperUtils.createOrSetData(regionPath + "/mysql_port", mysqlPortForThisRS, CreateMode.PERSISTENT); // 存储MySQL端口


            String tableRootPath = regionPath + "/table";
            zooKeeperUtils.createOrSetData(tableRootPath, "", CreateMode.PERSISTENT); // 父表节点

            // 设置监视点
            // 监视父节点以检测新的RegionServer添加/删除 (由 Master 的 RegionManager.init 设置)
            // zooKeeperUtils.setWatch(regionBasePath);
            // 监视 /data 节点以进行通信 (由 Master 的 RegionManager.init/addRegion 设置)
            zooKeeperUtils.setWatch(dataNodePath);
            // 监视 /exist 节点以检测RegionServer故障 (由 Master 的 RegionManager.init/addRegion 设置)
            zooKeeperUtils.setWatch(existNodePath);


            // tables 参数在RegionServer初次启动时通常为空，由Master后续添加
            for (TableInform table : tables) {
                String tablePath = tableRootPath + "/" + table.name;
                zooKeeperUtils.createOrSetData(tablePath, "", CreateMode.PERSISTENT);
                zooKeeperUtils.createOrSetData(tablePath + "/payload", table.payload.toString(), CreateMode.PERSISTENT);
            }

            System.out.println("[ZKManager] addRegionServer for " + ip + " completed successfully.");
            return true;

        } catch (KeeperException.NodeExistsException nee) {
            // 这个异常理论上应该被 createOrSetData 内部处理掉大部分，除非是根深蒂固的结构问题
            System.err.println("[ZKManager] NodeExistsException during addRegionServer for " + ip + ", though it should have been handled: " + nee.getMessage());
            nee.printStackTrace();
            return false;
        }
        catch (Exception e) {
            System.err.println("[ZKManager] Error in addRegionServer for " + ip + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // ... 其他 ZooKeeperManager 方法保持不变或按需调整 ...
    // 例如，getRegionServer, deleteRegionServer, addTable, deleteTable 等
    // 确保它们与上面定义的 ZK 结构一致。
    // 特别是 getRegionServer(String table_name) 需要正确遍历新的结构。
    // 之前 getRegionConnectionString 中获取 MySQL 端口的逻辑也需要更新为读取 /mysql_port

    public String getRegionServer(String table_name){
        try{
            List<String> serverIps = zooKeeperUtils.getChildren("/lss/region_server");
            for(String serverIp : serverIps){
                String tablePath = "/lss/region_server/" + serverIp + "/table/" + table_name;
                if(zooKeeperUtils.nodeExists(tablePath)){
                    return serverIp;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public boolean deleteRegionServer(String ip){
        try{
            // 删除前可以先关闭对应的 RegionServerHandler (在 Master 中)
            zooKeeperUtils.deleteNodeRecursively("/lss/region_server/"+ip);
            System.out.println("[ZKManager] Deleted RegionServer node for IP: " + ip);
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public void close(){
        try{
            zooKeeperUtils.closeConnection();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void setWatch(String path){ // 这个方法应该在需要设置watch的地方被调用，例如 Master 的 RegionManager
        try{
            System.out.println("[ZKManager] Setting watch on path: " + path);
            zooKeeperUtils.setWatch(path);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public boolean addTable(String regionIp, TableInform table){ // regionIp 是服务器的 IP
        try{
            String tableBasePath = "/lss/region_server/"+regionIp+"/table/";
            zooKeeperUtils.createOrSetData(tableBasePath + table.name,"", CreateMode.PERSISTENT);
            zooKeeperUtils.createOrSetData(tableBasePath + table.name+"/payload",table.payload.toString(), CreateMode.PERSISTENT);
            System.out.println("[ZKManager] Added table " + table.name + " to region " + regionIp);
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public boolean deleteTable(String table_name){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("[ZKManager] Delete failed: table ["+table_name+"] does not exist in any region.");
                return false; // Table not found, so can't delete
            }
            zooKeeperUtils.deleteNodeRecursively("/lss/region_server/"+ip+"/table/"+table_name);
            System.out.println("[ZKManager] Deleted table " + table_name + " from region " + ip);
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    // accTablePayload, decTablePayload, setTablePayload, setMasterData, setRegionData, getData, nodeExists, setData, getChildren
    // 这些方法应该继续工作，但要确保路径和逻辑与新的 createOrSetData 兼容。
    // 例如，accTablePayload 需要读取、修改、然后用 setData 写回。
    public boolean accTablePayload(String table_name){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("[ZKManager] AccPayload failed: table ["+table_name+"] does not exist");
                return false;
            }
            String payloadPath = "/lss/region_server/"+ip+"/table/"+table_name+"/payload";
            int payload=Integer.parseInt(zooKeeperUtils.getData(payloadPath));
            payload++;
            zooKeeperUtils.setData(payloadPath,String.valueOf(payload));
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public boolean decTablePayload(String table_name){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("[ZKManager] DecPayload failed: table ["+table_name+"] does not exist");
                return false;
            }
            String payloadPath = "/lss/region_server/"+ip+"/table/"+table_name+"/payload";
            int payload=Integer.parseInt(zooKeeperUtils.getData(payloadPath));
            payload--;
            if(payload<0){
                System.out.println("[ZKManager] DecPayload failed: payload of table ["+table_name+"] cannot be negative");
                return false;
            }
            zooKeeperUtils.setData(payloadPath,String.valueOf(payload));
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public boolean setTablePayload(String table_name,int payload){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("[ZKManager] SetPayload failed: table ["+table_name+"] does not exist");
                return false;
            }
            if(payload<0){
                System.out.println("[ZKManager] SetPayload failed: payload of table ["+table_name+"] cannot be negative");
                return false;
            }
            zooKeeperUtils.setData("/lss/region_server/"+ip+"/table/"+table_name+"/payload",String.valueOf(payload));
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public boolean setMasterData(String ip, String new_data){
        try{
            String path="/lss/region_server/"+ip+"/data";
            String currentZkData = zooKeeperUtils.getData(path);
            String[] lines = currentZkData.split("\n", 2);
            // String masterLine = lines[0]; // Not needed directly for setting
            String regionLine = (lines.length > 1) ? lines[1] : "region(received):"; // Preserve region line

            // No need to check label, just overwrite Master's part
            System.out.println("[ZKManager] Master setting data for " + ip + ": " + new_data);
            zooKeeperUtils.setData(path,"master(unreceived):"+new_data+"\n"+regionLine);
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public boolean setRegionData(String ip,String new_data){
        try{
            String path="/lss/region_server/"+ip+"/data";
            String currentZkData = zooKeeperUtils.getData(path);
            String[] lines = currentZkData.split("\n", 2);
            String masterLine = lines[0]; // Preserve master line
            // String regionLine = (lines.length > 1) ? lines[1] : "region(received):"; // Not needed

            System.out.println("[ZKManager] Region " + ip + " setting data: " + new_data);
            zooKeeperUtils.setData(path,masterLine+"\n"+"region(unreceived):"+new_data);
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 从指定的 RegionServer 的 ZooKeeper 记录中删除一个表及其元数据。
     * @param regionIp 要从中删除表的 RegionServer 的 IP 地址。
     * @param tableName 要删除的表名。
     * @return 如果成功删除或节点本就不存在，则返回 true；否则返回 false。
     */
    public boolean deleteTableFromSpecificRegion(String regionIp, String tableName) {
        try {
            String tablePath = "/lss/region_server/" + regionIp + "/table/" + tableName;
            if (zooKeeperUtils.nodeExists(tablePath)) {
                zooKeeperUtils.deleteNodeRecursively(tablePath);
                System.out.println("[ZKManager] 已从 Region " + regionIp + " 的 ZK 记录中删除表: " + tableName);
                return true;
            } else {
                System.out.println("[ZKManager] 在 Region " + regionIp + " 的 ZK 记录中未找到表 " + tableName + " (用于删除)。");
                return true; // 如果节点本就不存在，也视为成功（目标已达成）
            }
        } catch (Exception e) {
            System.err.println("[ZKManager] 从 Region " + regionIp + " 的 ZK 记录中删除表 " + tableName + " 失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public String getData(String serverPath) {
        try {
            return zooKeeperUtils.getData(serverPath);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean nodeExists(String serverPath) {
        try {
            return zooKeeperUtils.nodeExists(serverPath);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}