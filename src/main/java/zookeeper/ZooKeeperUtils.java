package zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import master.RegionManager;
import master.ResType;
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
	
	// 构造ZooKeeperUtils对象的时候初始化连接
	public ZooKeeperUtils(String host) {
		try {
			connectZookeeper(host);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public ZooKeeperUtils() {
		try {
			connectZookeeper("localhost:2181");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
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
				List<String> regions = getChildren(event.getPath());
				for(int i=0;i<regions.size();i++){
					System.out.println("set watch path is "+event.getPath()+"/"+regions.get(i)+"/exist");
					setWatch(event.getPath()+"/"+regions.get(i)+"/exist");
					setWatch(event.getPath()+"/"+regions.get(i)+"/data");
				}
				ResType res = RegionManager.addRegion(regions);
				switch (res){
					case ADD_REGION_ALREADY_EXISTS:
						System.out.println("Add region already exists.");
						break;
					case ADD_REGION_SUCCESS:
						System.out.println("Add region successfully.");
						break;
					case ADD_REGION_FAILURE:
						System.out.println("Add region failed.");
						break;
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}

		}
		if(event.getType() == Event.EventType.NodeDeleted){
			String ip=event.getPath();
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(0,ip.indexOf('/'));
			System.out.println("Detected a client has disconnected: " + ip);
			ResType res = RegionManager.delRegion(ip);
			switch (res){
				case DROP_REGION_NO_EXISTS:
					System.out.println("Del region doesn't exist.");
					break;
				case DROP_REGION_SUCCESS:
					System.out.println("Del region successfully.");
					break;
				case DROP_REGION_FAILURE:
					System.out.println("Del region failed.");
					break;
			}
			try{
				deleteNodeRecursively("/lss/region_server/"+ip);
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		if(event.getType() == Event.EventType.NodeDataChanged ){
			String ip=event.getPath();
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(ip.indexOf('/')+1);
			ip=ip.substring(0,ip.indexOf('/'));
			try{
				String data;
				if((data=getMasterData(event.getPath()))!=null){
					//TODO: 地址为ip的region给master发送消息data

				}
				else{
					data=getRegionData(event.getPath());
					//TODO: master给地址为ip的region发送消息data

				}
			}
			catch(Exception e){
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
		System.out.println("call ZooKeeperUtils.createNode");
		return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public String createTempNode(String path,String data) throws Exception{
		return this.zookeeper.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
	}

	/**
	 * 获取路径下所有子节点
     */
	public List<String> getChildren(String path) throws KeeperException, InterruptedException{
		System.out.println("call ZooKeeperUtils.getchildren " + path);
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
		System.out.println("call ZooKeeperUtils.getData " + path);
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
		System.out.println("call ZooKeeperUtils.setData " + path);
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
		System.out.println("call close Connection");
		if (zookeeper != null) {
			zookeeper.close();
		}
	}
	//从path中取出master的信息。
	public String getMasterData(String path){
		try{
			String data=getData(path);
			String reserve_data=data.substring(data.indexOf('\n'));
			data=data.substring(0,data.indexOf('\n'));
			String label=data.substring(data.indexOf('('),data.indexOf(')'));


			data=data.substring(data.indexOf(':')+1);
			if(label.equals("received")){
				System.out.println("当前master的信息已接收，不再重复接收");
				return null;
			}
			else{
				System.out.println("master传递的信息为: "+data);
				setData(path,"master(received):"+data+reserve_data);
				return data;
			}
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
//		return null;
	}
	public String getRegionData(String path){
		try{
			String data=getData(path);
			String reserve_data=data.substring(0,data.indexOf('\n'));
			data=data.substring(data.indexOf('\n')+1);
			String label=data.substring(data.indexOf('('),data.indexOf(')'));
			data=data.substring(data.indexOf(':')+1);
			if(label.equals("received")){
				System.out.println("当前region的信息已接收，不再重复接收");
				return null;
			}
			else{
				System.out.println("region传递的信息为: "+data);
				setData(path,reserve_data+"region(received):"+data);
				return data;
			}
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

}
