package gash.router.server.message.generator;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.container.RoutingConf;
import gash.router.database.datatypes.FluffyFile;
import pipe.common.Common.Header;
import pipe.election.Election.AllNodeInfo;
import pipe.election.Election.NewNodeMessage;
import pipe.election.Election.NewNodeMsgType;
import pipe.election.Election.NodeInfo;
import pipe.work.Work.READ_STEAL;
import pipe.work.Work.Steal;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

public class MessageGenerator {
	private static RoutingConf conf;

	protected static Logger logger = LoggerFactory.getLogger(MessageGenerator.class);
	protected static AtomicReference<MessageGenerator> instance = new AtomicReference<MessageGenerator>();

	public static MessageGenerator initGenerator() {
		instance.compareAndSet(null, new MessageGenerator());
		return instance.get();
	}

	private MessageGenerator() {

	}

	public static MessageGenerator getInstance(){
		MessageGenerator messageGenerator = instance.get();
		if(messageGenerator == null){
			logger.error(" Error while getting instance of MessageGenerator ");
		}
		return messageGenerator;
	}

	public static void setRoutingConf(RoutingConf routingConf) {
		MessageGenerator.conf = routingConf;
	}

	public CommandMessage generateClientResponseMsg(String msg){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setMessage(msg);

		return rb.build();
	}

	public WorkMessage generateReplicationRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);

		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		tb.setChunk(message.getFiletask().getChunk());
		tb.setChunkNo(message.getFiletask().getChunkNo());

		wb.setFiletask(tb);

		wb.setWorktype(Worktype.REPLICATE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateUpdateReplicationRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		tb.setChunk(message.getFiletask().getChunk());
		tb.setChunkNo(message.getFiletask().getChunkNo());

		wb.setFiletask(tb);

		wb.setWorktype(Worktype.UPDATE_REPLICATE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateDeletionRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);

		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		wb.setFiletask(tb);

		wb.setWorktype(Worktype.DELETE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateUpdateDeletionRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		wb.setFiletask(tb);

		wb.setWorktype(Worktype.UPDATE_DELETE_REQUEST);
		return wb.build();
	}

	public WorkMessage generateReadRequestMessage(CommandMessage commandMessage, String clientID, int nodeId){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setDestination(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setFiletask(commandMessage.getFiletask());

		wb.setSecret(1234);
		wb.setRequestId(clientID);
		wb.setWorktype(Worktype.READ_REQUEST);
		return wb.build();
	}

	public WorkMessage generateReadRequestMessage(CommandMessage commandMessage, String clientID, int nodeId, int chunkId){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setDestination(nodeId);
		hb.setTime(System.currentTimeMillis());

		FileTask.Builder fb = FileTask.newBuilder();
		fb.setFilename(commandMessage.getFiletask().getFilename());
		fb.setChunkNo(chunkId);
		fb.setFileTaskType(FileTaskType.READ);
		fb.setSender(commandMessage.getFiletask().getSender());
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setFiletask(fb.build());

		wb.setSecret(1234);
		wb.setRequestId(clientID);
		wb.setWorktype(Worktype.READ_REQUEST);
		return wb.build();
	}

	public WorkMessage generateReadRequestResponseMessage(FileTask filetask, ByteString line, int chunkId, int totalChunks, String requestId, int nodeId, String filename){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(nodeId);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());

		wb.setSecret(1234);
		wb.setRequestId(requestId);
		wb.setWorktype(Worktype.READ_REQUEST_RESPONSE);

		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(filename);
		ft.setChunkNo(chunkId);
		ft.setChunk(line);
		ft.setChunkCounts(totalChunks);
		ft.setFileTaskType(FileTaskType.READ);
		ft.setSender(filetask.getSender());
		//ft.setFilename(ft.getFilename());
		wb.setFiletask(ft.build());

		return wb.build();
	}


	public CommandMessage forwardChunkToClient(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb.build());
		cb.setFiletask(message.getFiletask());
		cb.setMessage("Fowarded chunk success : " + message.getFiletask().getChunkNo());

		return cb.build();
	}

	public WorkMessage generateReplicationAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.REPLICATE_RESPONSE);
		wb.setSecret(1234);

		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}

	public WorkMessage generateDeletionAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.DELETE_RESPONSE);
		wb.setSecret(1234);

		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}
	
	public WorkMessage generateUpdateDeletionAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.UPDATE_DELETE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}

	public WorkMessage generateUpdateReplicationAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.UPDATE_REPLICATE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}
	
	public WorkMessage generateWorkReadStealMessage() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_REQUEST);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSteal(sb.build());
		wb.setSecret(1234);
		return wb.build();
	}

	public WorkMessage generateWorkReadStealResponseMsg(WorkMessage workMessage) {
		WorkMessage.Builder wb = WorkMessage.newBuilder(workMessage);
		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_RESPONSE);
		wb.setSteal(sb.build());
		System.out.println("SEnding chunkId: " + workMessage.getFiletask().getChunkNo());
		/*		Header.Builder hb = Header.newBuilder();
		//System.out.println("Setting main server nodeId as header for steal read id: " + workMessage.getHeader().getDestination());
		try {
			System.out.println("Setting main server nodeId as header for steal read id: " + workMessage.getHeader().getNodeId());
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hb.setNodeId(workMessage.getHeader().getNodeId());
		hb.setTime(System.currentTimeMillis());

		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_RESPONSE);

		//System.out.println("SEnding filename: " + workMessage.getFiletask().getFilename());
		System.out.println("SEnding chunkId: " + workMessage.getFiletask().getChunkNo());
		//System.out.println("SEnding chunkCount: " + workMessage.getFiletask().getChunkCounts());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileTask.Builder ft  = FileTask.newBuilder();
		ft.setFilename(workMessage.getFiletask().getFilename());
		ft.setChunkNo(workMessage.getFiletask().getChunkNo());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setFiletask(ft.build());
		wb.setHeader(hb.build());
		wb.setSteal(sb.build());
		wb.setSecret(1234);*/
		return wb.build();
	}

	
	/**************************Starting of new node MessageGenrator function*************************/
	
	/**
	 * 
	 * @param leaderNodeId
	 * @param leaderHost
	 * @param leaderPort
	 * @return
	 */
	public WorkMessage generateLeaderInfoMsg(int leaderNodeId,String leaderHost, int leaderPort) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		NodeInfo.Builder leaderNodeInfo = NodeInfo.newBuilder();
		leaderNodeInfo.setNodeId(leaderNodeId);
		leaderNodeInfo.setPortNo(leaderPort);
		leaderNodeInfo.setHostAddr(leaderHost);	

		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.LEADER_INFO);
		newNodeMsg.setNodeInfo(leaderNodeInfo);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setNewNodeMessage(newNodeMsg);
		wb.setSecret(1234);
		wb.setNewNode(true);
		return wb.build();

	}

	public WorkMessage generateIamTheLeader(int myId, int myPort) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		NodeInfo.Builder leaderNodeInfo = NodeInfo.newBuilder();
		leaderNodeInfo.setNodeId(myId);
		leaderNodeInfo.setPortNo(myPort);

		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.I_AM_THE_LEADER);
		newNodeMsg.setNodeInfo(leaderNodeInfo);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setNewNodeMessage(newNodeMsg);
		wb.setSecret(1234);
		wb.setNewNode(true);
		return wb.build();
	}

	public WorkMessage generateIamNewNodeMsg() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(conf.getNodeId());
		myInfo.setPortNo(conf.getWorkPort());


		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.I_AM_NEW_NODE);
		newNodeMsg.setNodeInfo(myInfo);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setNewNodeMessage(newNodeMsg);
		wb.setSecret(1234);
		wb.setNewNode(true);
		
		return wb.build();
	}

	// message that is sent to New node to replicate data
	public WorkMessage generateReplicationRequestMsg(Integer nodeId,FluffyFile file){

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);

		FileTask.Builder tb = FileTask.newBuilder();
		tb.setChunk(ByteString.copyFrom(file.getFile()));
		tb.setChunkNo(file.getChunkId());
		tb.setChunkCounts(file.getTotalChunks());
		tb.setFileTaskType(FileTaskType.WRITE);
		tb.setFilename(file.getFilename());

		wb.setFiletask(tb);

		wb.setWorktype(Worktype.REPLICATE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateNewNodeAreYouDoneMsg() {
		
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);

		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(conf.getNodeId());
		myInfo.setPortNo(conf.getWorkPort());
		
		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.ARE_YOU_DONE);
		newNodeMsg.setNodeInfo(myInfo);
		wb.setNewNodeMessage(newNodeMsg);
		wb.setNewNode(true);
		return wb.build();
	}
	
	public WorkMessage generateAmDoneMessage() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);

		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(conf.getNodeId());
		myInfo.setPortNo(conf.getWorkPort());
		
		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.I_AM_DONE);
		newNodeMsg.setNodeInfo(myInfo);
		wb.setNewNodeMessage(newNodeMsg);
		wb.setNewNode(true);
		return wb.build();
	}
	
	public NodeInfo generateNodeInfoObject(Integer nodeId, String hostAddress, Integer portNo) {
		
		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(nodeId);
		myInfo.setHostAddr(hostAddress);
		myInfo.setPortNo(portNo);
		
		return myInfo.build();
	}
	
	public WorkMessage generateAllNodeInfoMsg(ArrayList<NodeInfo> nodesList) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		AllNodeInfo.Builder allNodeInfo = AllNodeInfo.newBuilder();
		allNodeInfo.addAllNodesInfo(nodesList);
		
		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.ALL_NODE_INFO);
		newNodeMsg.setAllNodeInfo(allNodeInfo);
		wb.setNewNodeMessage(newNodeMsg);
		wb.setNewNode(true);
		return wb.build();
	}

	public WorkMessage generateHeyThereMessage() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(conf.getNodeId());
		myInfo.setPortNo(conf.getWorkPort());
		
		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.HEY_THERE);
		newNodeMsg.setNodeInfo(myInfo);
		wb.setNewNodeMessage(newNodeMsg);
		wb.setNewNode(true);
		return wb.build();
	}
	

	public WorkMessage generateNewNodeInitiationMsg() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		NodeInfo.Builder myInfo = NodeInfo.newBuilder();
		myInfo.setNodeId(conf.getNodeId());
		myInfo.setPortNo(conf.getWorkPort());
		//myInfo.setHostAddr("127.0.0.1");
		
		NewNodeMessage.Builder newNodeMsg = NewNodeMessage.newBuilder();
		newNodeMsg.setMsgType(NewNodeMsgType.MY_INFO);
		newNodeMsg.setNodeInfo(myInfo);
		wb.setNewNodeMessage(newNodeMsg);
		wb.setNewNode(true);
		return wb.build();
	}
	/**************************Ending of new node MessageGenrator function*************************/

}

