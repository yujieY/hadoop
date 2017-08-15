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
 * 异步调用（更新节点数据）
 * */
public class updateNodeASync implements Watcher {

	private static ZooKeeper zookeeper;
	private static Stat stat;

	public static void main(String[] args) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000,
				new updateNodeASync());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}

	private void doSomething() throws KeeperException, InterruptedException {
		zookeeper.setData("/node_2", "443".getBytes(), -1, new IStatCallback(), "更新数据");
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		// System.out.println("收到事件："+event);
		// 当前zookeeper的状态
		if (event.getState() == KeeperState.SyncConnected) {
			// 当连接刚刚建立时，会得到事件通知event，但是此时event.getType()和event.getPath()均为空。if保证doSometing的内容只执行一次
			if (event.getType() == EventType.None && null == event.getPath()) {
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
	}

	static class IStatCallback implements AsyncCallback.StatCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("rc=" + rc).append("\n");
			sb.append("path=" + path).append("\n");
			sb.append("ctx=" + ctx).append("\n");
			sb.append("stat=" + stat).append("\n");
			System.out.println(sb.toString());
		}

	}
}
