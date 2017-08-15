package zookeeperDemo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 异步调用(获取子节点)
 * */
public class getChildrenASync implements Watcher{

	private static ZooKeeper zookeeper; 
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new getChildrenASync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething() throws KeeperException, InterruptedException{
		//异步获取没有返回值
		zookeeper.getChildren("/", true,new IChildren2Callback(),"查询");
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
//		System.out.println("收到事件："+event);
		//当前zookeeper的状态
		if(event.getState()==KeeperState.SyncConnected){
			//当连接刚刚建立时，会得到事件通知event，但是此时event.getType()和event.getPath()均为空。if保证doSometing的内容只执行一次
			if(event.getType()==EventType.None && null==event.getPath()){
			try {
				doSomething();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}else{
				if(event.getType()==EventType.NodeChildrenChanged){
					try {
						System.out.println(zookeeper.getChildren(event.getPath(),true));
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

	static class IChildren2Callback implements AsyncCallback.Children2Callback{

		@Override
		//ctx；异步调用上下文，可以是任意值
		public void processResult(int rc, String path, Object ctx,
				List<String> children, Stat stat) {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("rc="+rc).append("\n");
			sb.append("path="+path).append("\n");
			sb.append("children="+children).append("\n");
			sb.append("stat="+stat).append("\n");
			System.out.println(sb.toString());
		}
		
	}
}
