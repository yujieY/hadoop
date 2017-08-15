package zookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 同步调用（获取节点数据）
 * */
public class getDataSync implements Watcher{

	private static ZooKeeper zookeeper;
	private static Stat stat;
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new getDataSync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
		try {
			byte[] data = zookeeper.getData("/node_1", true, stat);
			String dataStr = new String(data);
			System.out.println("return data:"+dataStr);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
//		System.out.println("收到事件："+event);
		//当前zookeeper的状态
		if(event.getState()==KeeperState.SyncConnected){
			//当连接刚刚建立时，会得到事件通知event，但是此时event.getType()和event.getPath()均为空。if保证doSometing的内容只执行一次
			if(event.getType()==EventType.None && null==event.getPath()){
			doSomething();
			}else{
				if(event.getType()==EventType.NodeDataChanged){
					try {
						System.out.println("changed data:"+new String(zookeeper.getData(event.getPath(),true, stat)));
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

}
