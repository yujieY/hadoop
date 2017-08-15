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
 * 同步调用（删除节点）
 * */
public class deleteNodeSync implements Watcher{

	private static ZooKeeper zookeeper;
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new deleteNodeSync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
		try {
			//version=-1:不校验版本信息 
			zookeeper.delete("/node_3", -1);
			System.out.println("节点删除成功");
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
