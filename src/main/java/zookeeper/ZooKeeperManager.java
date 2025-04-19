package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;
public class ZooKeeperManager {
    public ZooKeeperUtils zooKeeperUtils;
    public String address="127.0.0.1";
    public String port="2181";
    public ZooKeeperManager(){
        try{
            zooKeeperUtils = new ZooKeeperUtils();
            zooKeeperUtils.connectZookeeper(address+":"+port);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    //regionserver连接到zookeeper时由regionserver调用***
    public boolean addRegionServer(String ip,String port, List<TableInform> tables, String password, String username, String port2master, String port2regionserver){
        try{
            String name="/lss/region_server/"+ip;
            zooKeeperUtils.createNode(name,ip);
            zooKeeperUtils.createTempNode(name+"/exist","123");
            zooKeeperUtils.createNode(name+"/port",port);
            zooKeeperUtils.createNode(name+"/password",password);
            zooKeeperUtils.createNode(name+"/username",username);
            zooKeeperUtils.createNode(name+"/port2master",port2master);
            zooKeeperUtils.createNode(name+"/port2regionserver",port2regionserver);
            zooKeeperUtils.createNode(name+"/table","");
            for(int i=0;i< tables.size();i++){
                zooKeeperUtils.createNode(name+"/table/"+tables.get(i).name,"");
                zooKeeperUtils.createNode(name+"/table/"+tables.get(i).name+"/payload",tables.get(i).payload.toString());
            }
            System.out.println("add region "+ip);
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getRegionServer(String table_name){
        try{
            List<String> server=zooKeeperUtils.getChildren("/lss/region_server");
            for(int i=0;i<server.size();i++){
                List<String> tables=zooKeeperUtils.getChildren("/lss/region_server/"+server.get(i)+"/table");
                for(int j=0;j<tables.size();j++){
                    if(table_name.equals(tables.get(j))){
                        return server.get(i);
                    }
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
            zooKeeperUtils.deleteNodeRecursively("/lss/region_server/"+ip);
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
    public void setWatch(String path){
        try{
            zooKeeperUtils.setWatch(path);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public boolean addTable(String ip, TableInform table){
        try{
            zooKeeperUtils.createNode("/lss/region_server/"+ip+"/table/"+table.name,"");
            zooKeeperUtils.createNode("/lss/region_server/"+ip+"/table/"+table.name+"/payload",table.payload.toString());
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean deleteTable(String table_name){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("the table ["+table_name+"] does not exist");
            }
            zooKeeperUtils.deleteNodeRecursively("/lss/region_server/"+ip+"/table/"+table_name);
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;

        }
    }
    public boolean accTablePayload(String table_name){
        try{
            String ip=getRegionServer(table_name);
            if(ip==null){
                System.out.println("the table ["+table_name+"] does not exist");
            }

            int payload=Integer.parseInt(zooKeeperUtils.getData("/lss/region_server/"+ip+"/table/"+table_name+"/payload"));
            payload++;
            zooKeeperUtils.setData("/lss/region_server/"+ip+"/table/"+table_name+"/payload",String.valueOf(payload));
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
                System.out.println("the table ["+table_name+"] does not exist");
            }

            int payload=Integer.parseInt(zooKeeperUtils.getData("/lss/region_server/"+ip+"/table/"+table_name+"/payload"));
            payload--;
            if(payload<0){
                System.out.println("the payload of table ["+table_name+"] cannot be negative");
                return false;
            }
            zooKeeperUtils.setData("/lss/region_server/"+ip+"/table/"+table_name+"/payload",String.valueOf(payload));
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
                System.out.println("the table ["+table_name+"] does not exist");
            }
            if(payload<0){
                System.out.println("the payload of table ["+table_name+"] cannot be negative");
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
}
