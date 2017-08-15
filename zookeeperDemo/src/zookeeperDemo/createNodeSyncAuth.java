package zookeeperDemo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/**
 * 同步调用（权限设置创建节点）
 * */
public class createNodeSyncAuth implements Watcher {

	private static ZooKeeper zookeeper;
	private boolean doSometingDone = false;

	public static void main(String[] args) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		zookeeper = new ZooKeeper("172.24.68.100:12181", 5000,
				new createNodeSyncAuth());
		System.out.println(zookeeper.getState());
		Thread.sleep(Integer.MAX_VALUE);
	}

	/**
	 * 权限模式（Scheme）：ip，digest（基于用户名密码） 授权对象（ID） ip权限模式：具体的ip地址
	 * digest权限模式：username:Base64(SHA-1(username:password)) --SHA-1加密>Base64位编码
	 * 权限（permission）：create（C），delete（D），read（R），write（W），admin（A）
	 * 注：单个权限，完全权限，复合权限
	 * 
	 * 权限组合：scheme+ID+permisssion
	 * 
	 * @throws NoSuchAlgorithmException
	 * */

	private void doSomething() throws NoSuchAlgorithmException {
		// path:创建节点完整路径，data：数据内容，acl（权限信息），createMode：临时节点/持久节点
		try {

			ACL aclIp = new ACL(Perms.ALL, new Id("ip", "172.24.68.100"));
			ACL aclDigest = new ACL(Perms.READ | Perms.WRITE, new Id("digest",
					DigestAuthenticationProvider.generateDigest("zk:123456")));
			System.out.println("生成的密文为："
					+ DigestAuthenticationProvider.generateDigest("zk:123456"));
			ArrayList<ACL> acls = new ArrayList<ACL>();
			acls.add(aclDigest);
			acls.add(aclIp);

			/**
			 * zookeeper预设的授权模式 Ids.OPEN_ACL_UNSAFE(全开放)
			 * Ids.READ_ACL_UNSAFE(任何人可对其进行读取操作)
			 * Ids.CREATOR_ALL_ACL(前提：添加zookeeper.addAuthInfo("ip",
			 * "172.24.68.101");)（系统将用addAuthInfo注册的信息作为新创建节点的授权信息）
			 */
			String path = zookeeper.create("/node_3", "666".getBytes(), acls,
					CreateMode.PERSISTENT);
			System.out.println("return path:" + path);

			doSometingDone = true;
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("收到事件：" + event);
		if (event.getState() == KeeperState.SyncConnected) {
			if (!doSometingDone && event.getType() == EventType.None && null == event.getPath()) {
				try {
					// 如果测试环境为广域网或局域网，并且网络状况不好，可能会导致zookeeper客户端断线重连，doSomething可能会多次执行
					// 可考虑设置布尔变量doSomethingDone
					doSomething();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
