package zookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 同步调用（创建节点）
 * */
public class createNodeSync implements Watcher{

	private static ZooKeeper zookeeper;
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new createNodeSync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
		//path:创建节点完整路径，data：数据内容，acl（权限信息），createMode：临时节点/持久节点
		try {
			String path = zookeeper.create( "/node_2", "666".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("return path:"+path);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("收到事件："+event);
		if(event.getState()==KeeperState.SyncConnected){
			doSomething();
		}
	}

}
