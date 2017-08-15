package zookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.AsyncCallback;
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
public class deleteNodeASync implements Watcher{

	private static ZooKeeper zookeeper;
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000, new deleteNodeASync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
		//version=-1:不校验版本信息 
		zookeeper.delete("/node_4", -1,new IVoidCallback(),"删除");
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("收到事件："+event);
		if(event.getState()==KeeperState.SyncConnected){
			doSomething();
		}
	}

	static class IVoidCallback implements AsyncCallback.VoidCallback{

		@Override
		public void processResult(int rc, String path, Object ctx) {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("rc="+rc).append("\n");
			sb.append("path="+path).append("\n");
			sb.append("ctx="+ctx).append("\n");
			System.out.println(sb.toString());
		}
		
	}
}
