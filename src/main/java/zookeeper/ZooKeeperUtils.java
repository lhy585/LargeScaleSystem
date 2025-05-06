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

public class ZooKeeperUtils implements Watcher {

	private ZooKeeper zookeeper;
	/**
	 * 超时时间
	 */
	private static final int SESSION_TIME_OUT = 2000; // 建议适当调高，例如 5000ms
	private CountDownLatch countDownLatch = new CountDownLatch(1);

	// 构造函数等保持不变...
	public ZooKeeperUtils(String host) {
		try {
			connectZookeeper(host);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public ZooKeeperUtils() {
		try {
			// 确保这里连接的是正确的 ZK 地址
			connectZookeeper("127.0.0.1:2181"); // 或者从配置读取
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			System.out.println("Watch received event: SyncConnected"); // 更清晰的日志
			countDownLatch.countDown();
		}
		// ... 其他的 process 逻辑保持不变
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
					default: // 处理其他可能的枚举值
						System.out.println("Add region returned: " + res);
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		if(event.getType() == Event.EventType.NodeDeleted){
			String path = event.getPath();
			// 从路径中提取IP应该更健壮
			// 例如: /lss/region_server/192.168.140.1/exist -> 192.168.140.1
			String prefix = "/lss/region_server/";
			if (path.startsWith(prefix) && path.contains("/exist")) {
				String ip = path.substring(prefix.length(), path.indexOf("/exist"));
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
					default: // 处理其他可能的枚举值
						System.out.println("Del region returned: " + res);
				}
				// 删除父节点 /lss/region_server/ip 的操作应该由RegionManager决定，
				// 或者在 ZK 会话超时后自动清理（如果父节点也是临时的，但这里不是）
				// 如果 RegionManager.delRegion 负责清理，这里就不需要重复删除
			} else {
				System.out.println("NodeDeleted event for unexpected path: " + path);
			}
		}
		if(event.getType() == Event.EventType.NodeDataChanged ){
			String path = event.getPath();
			// 提取IP
			String prefix = "/lss/region_server/";
			if (path.startsWith(prefix) && path.contains("/data")) {
				String ip = path.substring(prefix.length(), path.indexOf("/data"));
				try{
					String data;
					if((data=getMasterData(path))!=null){ // 假设 getMasterData 返回 null 如果是对方发的消息
						//TODO: 地址为ip的region给master发送消息data
						System.out.println("[Watcher] Master should process data from RegionServer " + ip + ": " + data);
					}
					else if ((data=getRegionData(path))!=null){ // 假设 getRegionData 返回 null 如果是对方发的消息
						//TODO: master给地址为ip的region发送消息data
						System.out.println("[Watcher] RegionServer " + ip + " should process data from Master: " + data);
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}
			} else {
				System.out.println("NodeDataChanged event for unexpected path: " + path);
			}
		}
	}


	/**
	 * 连接Zookeeper
	 */
	public void connectZookeeper(String host) throws Exception {
		zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
		boolean connected = countDownLatch.await(SESSION_TIME_OUT + 1000, java.util.concurrent.TimeUnit.MILLISECONDS); // Add timeout
		if (!connected) {
			System.err.println("[Error] Timed out waiting for ZooKeeper connection to " + host);
			throw new RuntimeException("Timed out waiting for ZooKeeper connection");
		}
		System.out.println("[Info]Zookeeper connection success.");
	}

	/**
	 * 递归确保路径存在 (创建所有不存在的父路径)
	 */
	public void ensurePathExists(String path) throws KeeperException, InterruptedException {
		if (path == null || path.isEmpty() || path.equals("/")) {
			return;
		}
		if (nodeExists(path)) {
			return;
		}
		// 创建父路径
		int lastSlash = path.lastIndexOf('/');
		if (lastSlash > 0) { // 不是根目录也不是根目录下的直接子节点
			ensurePathExists(path.substring(0, lastSlash));
		}
		// 创建当前节点
		try {
			System.out.println("[ZooKeeperUtils] Creating ZK path: " + path);
			this.zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException nee) {
			// 如果在检查和创建之间，其他线程/进程创建了它，忽略
		}
	}


	/**
	 * 创建节点 (如果不存在) 或设置数据 (如果存在).
	 * 对于临时节点，如果已存在持久节点，会尝试删除再创建。
	 */
	public void createOrSetData(String path, String data, CreateMode mode) throws Exception {
		ensurePathExists(path.substring(0, path.lastIndexOf('/'))); // 确保父路径存在

		if (mode == CreateMode.EPHEMERAL || mode == CreateMode.EPHEMERAL_SEQUENTIAL) {
			try {
				// 对于临时节点，我们总是尝试创建它，会话结束时它会自动消失。
				// 如果因为某种原因（例如，一个持久节点占用了路径）而创建失败，下面的catch会处理。
				createTempNode(path, data);
			} catch (KeeperException.NodeExistsException e) {
				System.out.println("[ZooKeeperUtils] WARN: Ephemeral node " + path + " encountered NodeExists. Trying to delete existing and recreate.");
				try {
					// 可能是因为一个持久节点错误地存在于此
					// 或者 ZK 会话恢复时的一个罕见情况
					deleteNode(path); // 尝试删除任何已存在的节点 (可能是持久的)
					createTempNode(path, data); // 再次尝试创建临时节点
				} catch (Exception ex) {
					System.err.println("[ZooKeeperUtils] ERROR: Failed to delete and recreate ephemeral node " + path + ": " + ex.getMessage());
					throw e; // 重新抛出原始的 NodeExistsException 或新的异常
				}
			} catch (Exception e) { // 其他来自 createTempNode 的异常
				System.err.println("[ZooKeeperUtils] ERROR: Failed to create ephemeral node " + path + ": " + e.getMessage());
				throw e;
			}
		} else { // Persistent or PersistentSequential
			if (nodeExists(path)) {
				try {
					setData(path, data);
				} catch (KeeperException.NoNodeException nne) {
					// 节点在检查存在和设置数据之间被删除了，尝试创建
					createNode(path, data);
				}
			} else {
				createNode(path, data);
			}
		}
	}

	// ... 其他方法 createNode, createTempNode, getChildren, setWatch, getData, setData, deleteNode 等保持不变 ...
	public String createNode(String path,String data) throws Exception{
		System.out.println("call ZooKeeperUtils.createNode " + path);
		return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public String createTempNode(String path,String data) throws Exception{
		// 确保父路径存在
		ensurePathExists(path.substring(0, path.lastIndexOf('/')));
		System.out.println("call ZooKeeperUtils.createTempNode " + path);
		return this.zookeeper.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
	}
	public List<String> getChildren(String path) throws KeeperException, InterruptedException{
		System.out.println("call ZooKeeperUtils.getchildren " + path);
		List<String> children = zookeeper.getChildren(path, false);
		return children;
	}
	public void setWatch(String path) throws Exception{
		// 对于数据节点，我们通常监视 NodeDataChanged 或 NodeDeleted
		// 对于父节点，我们通常监视 NodeChildrenChanged
		// 这里的实现是监视子节点变化
		if (nodeExists(path)) { // 只有当节点存在时才能设置watch
			zookeeper.getChildren(path,true);
			zookeeper.exists(path, true); // 也监视节点本身(删除/数据改变)
		} else {
			System.out.println("[ZooKeeperUtils] WARN: Cannot set watch on non-existent path: " + path);
		}
	}
	public String getData(String path) throws KeeperException, InterruptedException{
		System.out.println("call ZooKeeperUtils.getData " + path);
		byte[] data = zookeeper.getData(path, false, null);
		if (data == null) {
			return "";
		}
		return new String(data);
	}
	public Stat setData(String path,String data) throws KeeperException, InterruptedException{
		System.out.println("call ZooKeeperUtils.setData " + path);
		Stat stat = zookeeper.setData(path, data.getBytes(), -1);
		return stat;
	}
	public void deleteNode(String path) throws InterruptedException, KeeperException{
		zookeeper.delete(path, -1);
	}
	public void deleteNodeRecursively(String path) throws Exception {
		List<String> children = zookeeper.getChildren(path,false);
		for (String child : children) {
			deleteNodeRecursively(path + "/" + child);
		}
		zookeeper.delete(path,-1);
	}
	public String getCTime(String path) throws KeeperException, InterruptedException{
		Stat stat = zookeeper.exists(path, false);
		return String.valueOf(stat.getCtime());
	}
	public Integer getChildrenNum(String path) throws KeeperException, InterruptedException{
		int childenNum = zookeeper.getChildren(path, false).size();
		return childenNum;
	}
	public void closeConnection() throws InterruptedException{
		System.out.println("call close Connection");
		if (zookeeper != null) {
			zookeeper.close();
		}
	}
	public String getMasterData(String path){
		try{
			String data=getData(path);
			String[] lines = data.split("\n", 2);
			String masterLine = lines[0];
			String regionLine = (lines.length > 1) ? lines[1] : "region(received):"; // 默认

			if(masterLine.startsWith("master(received):")){
				System.out.println("当前master的信息已接收，不再重复接收");
				return null;
			}
			else if (masterLine.startsWith("master(unreceived):")){
				String masterData = masterLine.substring("master(unreceived):".length());
				System.out.println("master传递的信息为: "+masterData);
				setData(path,"master(received):"+masterData+"\n"+regionLine);
				return masterData;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	public String getRegionData(String path){
		try{
			String data=getData(path);
			String[] lines = data.split("\n", 2);
			String masterLine = lines[0];
			String regionLine = (lines.length > 1) ? lines[1] : "region(received):";

			if(regionLine.startsWith("region(received):")){
				System.out.println("当前region的信息已接收，不再重复接收");
				return null;
			}
			else if (regionLine.startsWith("region(unreceived):")) {
				String regionData = regionLine.substring("region(unreceived):".length());
				System.out.println("region传递的信息为: "+regionData);
				setData(path,masterLine+"\n"+"region(received):"+regionData);
				return regionData;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	public boolean nodeExists(String path) {
		try {
			Stat stat = zookeeper.exists(path, false); // false 表示不设置默认监视器
			return stat != null;
		} catch (KeeperException | InterruptedException e) {
			// 连接问题等可能导致异常，此时视为节点不存在或无法确认
			System.err.println("[ZooKeeperUtils] Error checking node existence for " + path + ": " + e.getMessage());
			return false;
		}
	}
}