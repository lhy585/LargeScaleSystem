package zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperUtils implements Watcher{

	private ZooKeeper zookeeper;
	/**
	 * 超时时间
	 */
	private static final int SESSION_TIME_OUT = 2000;
	private CountDownLatch countDownLatch = new CountDownLatch(1);

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			System.out.println("Watch received event");
			countDownLatch.countDown();
		}
//		System.out.println(event);
		if(event.getType() == Event.EventType.NodeChildrenChanged){
			System.out.println("Detected a client has created: " + event.getPath());
			try{
				this.setWatch(event.getPath());
				List<String>regions= getChildren(event.getPath());
				for(int i=0;i<regions.size();i++){
					System.out.println("set watch path is "+event.getPath()+"/"+regions.get(i)+"/exist");
					setWatch(event.getPath()+"/"+regions.get(i)+"/exist");
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			//TODO:在此处调用master的处理新注册的regionserver的函数
			//当前所有的regionserver为List<String>regions，仅记录regionserver的ip

		}
		if(event.getType() == Event.EventType.NodeDeleted){
			String ip=event.getPath();
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(0,ip.indexOf('/'));
			System.out.println("Detected a client has disconnected: " + ip);
			//TODO:在此处调用master的处理regionserver掉线的函数
			//掉线的region为ip

			try{
				deleteNodeRecursively("/lss/region_server/"+ip);
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * 连接Zookeeper
     */
	public void connectZookeeper(String host) throws Exception{
		zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
		countDownLatch.await();
		System.out.println("[Info]Zookeeper connection success.");
	}

	/**
	 * 创建节点
     */
	public String createNode(String path,String data) throws Exception{
		return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public String createTempNode(String path,String data) throws Exception{
		return this.zookeeper.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
	}

	/**
	 * 获取路径下所有子节点
     */
	public List<String> getChildren(String path) throws KeeperException, InterruptedException{
		List<String> children = zookeeper.getChildren(path, false);
		return children;
	}

	public void setWatch(String path) throws Exception{
		zookeeper.getChildren(path,true);
	}

	/**
	 * 获取节点上面的数据
     */
	public String getData(String path) throws KeeperException, InterruptedException{
		byte[] data = zookeeper.getData(path, false, null);
		if (data == null) {
			return "";
		}
		return new String(data);
	}

	/**
	 * 设置节点信息
	 * @param path  路径
	 * @param data  数据
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat setData(String path,String data) throws KeeperException, InterruptedException{
		Stat stat = zookeeper.setData(path, data.getBytes(), -1);
		return stat;
	}

	/**
	 * 删除节点
	 * @param path
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void deleteNode(String path) throws InterruptedException, KeeperException{
		zookeeper.delete(path, -1);
	}
	public void deleteNodeRecursively(String path) throws Exception {
		// 获取当前节点的所有子节点
		List<String> children = zookeeper.getChildren(path,false);

		// 递归删除所有子节点
		for (String child : children) {
			deleteNodeRecursively(path + "/" + child);  // 删除子节点
		}

		// 删除当前节点
		zookeeper.delete(path,-1);
//		System.out.println("Deleted node: " + path);
	}
	/**
	 * 获取创建时间
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getCTime(String path) throws KeeperException, InterruptedException{
		Stat stat = zookeeper.exists(path, false);
		return String.valueOf(stat.getCtime());
	}

	/**
	 * 获取某个路径下孩子的数量
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Integer getChildrenNum(String path) throws KeeperException, InterruptedException{
		int childenNum = zookeeper.getChildren(path, false).size();
		return childenNum;
	}
	/**
	 * 关闭连接
	 * @throws InterruptedException
	 */
	public void closeConnection() throws InterruptedException{
		if (zookeeper != null) {
			zookeeper.close();
		}
	}

}