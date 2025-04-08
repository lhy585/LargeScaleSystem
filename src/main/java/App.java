import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import zookeeper.*;

import java.io.File;
import java.util.List;

public class App {
    public static void main(String [] args){
        try{
            ZooKeeperUtils zookeeper = new ZooKeeperUtils();
            zookeeper.connectZookeeper("127.0.0.1:2181");
//            zookeeper.createNode("/lss/master","master");
//            zookeeper.createNode("/lss/master/ip","127.0.0.1");
//            zookeeper.createNode("/lss/master/port","2181");
            System.out.println(zookeeper.getData("/lss/master/ip"));
            List<String> children = zookeeper.getChildren("/lss/master/ip");
            System.out.println(children);
            System.out.println(111);
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
