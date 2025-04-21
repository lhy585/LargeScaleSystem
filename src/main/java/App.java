import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import zookeeper.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main(String [] args){
        try{
//            System.out.println(zookeeper.getData("/lss/master/ip"));
            TableInform tableInform=new TableInform("table1",0);
            TableInform tableInform2=new TableInform("table2",0);
            List<TableInform> tableInforms=new ArrayList<>();
            List<TableInform> tableInforms2=new ArrayList<>();
            tableInforms.add(tableInform);
            tableInforms2.add(tableInform2);
            ZooKeeperManager master = new ZooKeeperManager(); //master
            master.setWatch("/lss/region_server");
            System.out.println("client1 connected");
            ZooKeeperManager zooKeeperManager=new ZooKeeperManager(); //client
            zooKeeperManager.addRegionServer("1.1.1.1","2182",tableInforms,"123","123","1234","1234");
            System.out.println("client2 connected");
            ZooKeeperManager zooKeeperManager1 = new ZooKeeperManager(); //client
            zooKeeperManager1.addRegionServer("1.1.1.2","2182",tableInforms2,"123","123","1234","1234");
            System.out.println(111);
//            System.out.println("table1 is at server ip = "+master.getRegionServer("table1"));
//            System.out.println("table2 is at server ip = "+master.getRegionServer("table2"));
////            System.out.println("client2 disconnected");
////            zooKeeperManager1.close();
////            zooKeeperManager.close();
//            zooKeeperManager.addTable("1.1.1.1",tableInform2);
//            if(zooKeeperManager.accTablePayload("table1")){
//                System.out.println("acc success");
//            }
//            if(zooKeeperManager.accTablePayload("table1")){
//                System.out.println("acc success");
//            }
//            if(zooKeeperManager.accTablePayload("table1")){
//                System.out.println("acc success");
//            }
//            zooKeeperManager.decTablePayload("table1");
//            zooKeeperManager.deleteTable("table2");
            zooKeeperManager1.setRegionData("1.1.1.2","i am 1.1.1.2");
            master.setMasterData("1.1.1.2","i am master");
            master.setMasterData("1.1.1.2","i am master new");
            while(true){

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
