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
 * 异步调用（创建节点）
 * */
public class createNodeASync implements Watcher {

	private static ZooKeeper zookeeper;

	public static void main(String[] args) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000,
				new createNodeASync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}

	private void doSomething() throws KeeperException, InterruptedException {
		zookeeper.create("/node_2", "666".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,new IStringCallback(),"创建");
//			System.out.println("return path:" + path);
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("收到事件：" + event);
		if (event.getState() == KeeperState.SyncConnected) {
			try {
				doSomething();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static class IStringCallback implements AsyncCallback.StringCallback {

		@Override
		//rc：返回码（异步调用成功返回0），path：需要创建节点的完整路径，ctx：异步调用的上下文（即“创建”），name：服务端返回已经创建节点的真实路径
		public void processResult(int rc, String path, Object ctx, String name) {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("rc="+rc).append("\n");
			sb.append("path="+path).append("\n");
			sb.append("ctx="+ctx).append("\n");
			sb.append("name="+name).append("\n");
			System.out.println(sb.toString());
		}

	}
}
