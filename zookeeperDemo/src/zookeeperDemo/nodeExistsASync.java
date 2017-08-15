package zookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.AsyncCallback;
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
 * 异步调用（判断节点是否存在）
 * */
public class nodeExistsASync implements Watcher{

	private static ZooKeeper zookeeper;
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new nodeExistsASync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
		zookeeper.exists("/node_1", true,new IStateCallback(),"查看节点状态");
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		//当前zookeeper的状态
		if(event.getState()==KeeperState.SyncConnected){
			//当连接刚刚建立时，会得到事件通知event，但是此时event.getType()和event.getPath()均为空。if保证doSometing的内容只执行一次
			if(event.getType()==EventType.None && null==event.getPath()){
			doSomething();
			}else{
				try{
					if(event.getType()==EventType.NodeCreated){
						System.out.println(event.getPath()+ " created");
						System.out.println(zookeeper.exists(event.getPath(),true ));
					}else if(event.getType()==EventType.NodeDataChanged){
						System.out.println(event.getPath()+ " updated");
						System.out.println(zookeeper.exists(event.getPath(),true ));
					}else if(event.getType()==EventType.NodeDeleted){
						System.out.println(event.getPath()+ " deleted");
						System.out.println(zookeeper.exists(event.getPath(),true ));
					}
				}catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	static class IStateCallback implements AsyncCallback.StatCallback{

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			// TODO Auto-generated method stub
			System.out.println("rc="+rc);
			System.out.println("ctx="+ctx);
			System.out.println("stat="+stat);
		}
		
	}
}
