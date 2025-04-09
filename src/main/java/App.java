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
            List<TableInform> tableInforms=new ArrayList<>();
            tableInforms.add(tableInform);
            ZooKeeperManager master = new ZooKeeperManager(); //master
            master.setWatch("/lss/region_server");
            System.out.println("client1 connected");
            ZooKeeperManager zooKeeperManager=new ZooKeeperManager(); //client
            zooKeeperManager.addRegionServer("1.1.1.1","2182",tableInforms,"123","123","1234","1234");
            System.out.println("client2 connected");
            ZooKeeperManager zooKeeperManager1 = new ZooKeeperManager(); //client
            zooKeeperManager1.addRegionServer("1.1.1.2","2182",tableInforms,"123","123","1234","1234");
            System.out.println(111);
            System.out.println("client2 disconnected");
            zooKeeperManager1.close();
//            zooKeeperManager.close();
            while(true){

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
