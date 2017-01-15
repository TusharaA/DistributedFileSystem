package gash.router.server.replication;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.database.DatabaseHandler;
import gash.router.database.datatypes.FluffyFile;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class DataReplicationManager {
	protected static Logger logger = LoggerFactory.getLogger("DataReplicationManager");
	protected static AtomicReference<DataReplicationManager> instance = new AtomicReference<DataReplicationManager>();
	public static ConcurrentHashMap<String, UpdateFileInfo> fileUpdateTracker = new ConcurrentHashMap<String, UpdateFileInfo>();
	public static DataReplicationManager initDataReplicationManager() {
		instance.compareAndSet(null, new DataReplicationManager());
		System.out.println(" --- Initializing Data Replication Manager --- ");
		return instance.get();
	}

	public static DataReplicationManager getInstance() throws Exception {
		if (instance != null && instance.get() != null) {
			return instance.get();
		}
		throw new Exception(" Data Replication Manager not started ");
	}


	public void broadcastReplication(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = MessageGenerator.getInstance().generateReplicationRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}

	public void broadcastDeletion(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = MessageGenerator.getInstance().generateDeletionRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}

	public void broadcastUpdateReplication(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = MessageGenerator.getInstance().generateUpdateReplicationRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}
	
	public void broadcastUpdateDeletion(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = MessageGenerator.getInstance().generateUpdateDeletionRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}
	
	public void newNodeReplication(Integer nodeId,Channel channel){
		System.out.println("Entered DataReplicationManager::newNodeReplication::Channel open?::"+(channel!=null));
		try {
			List<FluffyFile> filesFromRethinkDB = DatabaseHandler.getAllFileContentsFromRethink();
			List<FluffyFile> filesFromRiakDB = DatabaseHandler.getAllFileContentsFromRiak();
			filesFromRethinkDB.addAll(filesFromRiakDB);
			System.out.println("Returned from Database handler::"+filesFromRethinkDB.size());
			for(FluffyFile record : filesFromRethinkDB){
				WorkMessage replicateFileMsg = MessageGenerator.getInstance().generateReplicationRequestMsg(nodeId,record);
				QueueManager.getInstance().enqueueOutboundWorkWrite(replicateFileMsg, channel);
				
			}
		} catch (Exception e) {
			System.out.println("Exception at newNodeReplication");
		}
	}
}
