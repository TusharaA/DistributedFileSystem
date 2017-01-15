package gash.router.server.workChainHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.QueueManager;
import gash.router.server.replication.DataReplicationManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.election.Election.NewNodeMsgType;
import pipe.election.Election.NodeInfo;
import pipe.work.Work.WorkMessage;

public class NewNodeChainHandlerV2 implements IWorkChainHandler {
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(NewNodeChainHandlerV2.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	private Channel connectToChannel(String host, int port, ServerState state) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
		WorkInit workInit = new WorkInit(state, false);

		try {
			b.group(nioEventLoopGroup).channel(NioSocketChannel.class).handler(workInit);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			// Make the connection attempt.
		} catch (Exception e) {
			logger.error("Could not connect to the host " + host);
			return null;
		}
		return b.connect(host, port).syncUninterruptibly().channel();

	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		
		if(workMessage.hasNewNode()){
			// always enter this
			System.out.println("Entered New Node chain handler");
			if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.MY_INFO){
				System.out.println("Entered New Node chain handler:::MY_INFO");
				int leaderId = state.getElectionCtx().getLeaderId();
				int myId = state.getConf().getNodeId();
				int myPort = state.getConf().getWorkPort();
				if(leaderId == myId) {
					WorkMessage leaderInfoMsg = MessageGenerator.getInstance().generateIamTheLeader(myId, myPort);
					if(channel != null && channel.isOpen())
						channel.writeAndFlush(leaderInfoMsg);
					else 
						System.out.println("New Node Channel got closed:while sending are you leader Info, i.e my info");
				}
				else {
					EdgeInfo edgeInfo = state.getEmon().getOutBoundEdgesList().getNode(state.getElectionCtx().getLeaderId());
					String leaderHost = edgeInfo.getHost(); //get from EMON
					int leaderPort = edgeInfo.getPort(); // get from EMON
					System.out.println("Leader is: " + leaderHost + " port: " + leaderPort);
					WorkMessage leaderInfoMsg = MessageGenerator.getInstance().generateLeaderInfoMsg(state.getElectionCtx().getLeaderId(), leaderHost, leaderPort);
					if(channel != null && channel.isOpen())
						channel.writeAndFlush(leaderInfoMsg);
					else 
						System.out.println("New Node Channel got closed:while sending are you leader Info");
				}

			} else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.I_AM_DONE){
				// I am a leader as I have got a new Node message from new Node saying that it has completed 
				// the replication process and that it is ready to communicate with the other nodes
				System.out.println("I am a leader as I have got a new Node message from new Node saying that it has completed "
						+ "the replication process and that it is ready to communicate with the other nodes");

				// I need to send all node info to the new slave for communication with other servers
				ArrayList<NodeInfo> nodeList = new ArrayList<NodeInfo>();
				NodeInfo nodeInfo;

				for(EdgeInfo ei : state.getEmon().getOutboundEdges().getEdgeListMap().values()){
					nodeInfo = MessageGenerator.getInstance().generateNodeInfoObject(ei.getRef(), ei.getHost(), ei.getPort());
					nodeList.add(nodeInfo);
				}
				
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createOutboundIfNew(workMessage.getNewNodeMessage().getNodeInfo().getNodeId(), addr.getAddress().toString(),workMessage.getNewNodeMessage().getNodeInfo().getPortNo());				
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true); 

				WorkMessage allNodeInfoMessage = MessageGenerator.getInstance().generateAllNodeInfoMsg(nodeList);
				if(channel != null && channel.isOpen())
					channel.writeAndFlush(allNodeInfoMessage);
				else 
					System.out.println("New Node Channel got closed: while sending all node info");

			} else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.I_AM_NEW_NODE){
				// I am a leader and I receveid this message
				System.out.println("I am the leader and I have got the 'I_AM_NEW_NODE' message: " + state.getConf().getNodeId());
				if(state.getConf().getNodeId() == state.getElectionCtx().getLeaderId()){
					try {
						System.out.println("Start new node replication");
						DataReplicationManager.getInstance().newNodeReplication(workMessage.getHeader().getNodeId(),channel);

						// done with replication message sending to new node
						System.out.println("done with replication message sending to new node");

						// Now I need to send a message to the new node to ask if the node
						// is done with the node replication
						WorkMessage areYouDoneMsg = MessageGenerator.getInstance().generateNewNodeAreYouDoneMsg();
						if(channel != null && channel.isOpen())
							channel.writeAndFlush(areYouDoneMsg);
						else 
							System.out.println("New Node Channel got closed while sending are you done");
					}
					catch (Exception e) {
						System.out.println("Exception occured when trying to replication to new node: " + e.getMessage());
					}
				}
				else {
					System.out.println("I am not a leader and cannot do replication to new node");
				}
			}else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.ARE_YOU_DONE){
				// I am a new node and got the are you done message
				System.out.println("I am a new node and got the are you done message");
				while(true) {
					if(QueueManager.getInstance().isInboundWorkWriteQEmpty()) {
						WorkMessage iAmDoneWithReplicaitonMsg = MessageGenerator.getInstance().generateAmDoneMessage();
						if(channel != null && channel.isOpen()) {
							System.out.println("I am ready to become a slave");
							channel.writeAndFlush(iAmDoneWithReplicaitonMsg);
							System.out.println("Sent I am ready message to slave");
							break;
						}
						else 
							System.out.println("New Node Channel got closed while sending are you done");
					}
					else {
						// TO - DO remove this
						System.out.println("Replication work for new node in progress");
					}
				}

			}
			else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.I_AM_THE_LEADER){
				
				System.out.println("My friend is the leader: ");
				
				//Add to you OB edges
				//once channel gets created in Emon .. send I_am_new_node msg to Leader
				int nodeId = workMessage.getNewNodeMessage().getNodeInfo().getNodeId();

				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;
				String host = addr.getAddress().toString();

				int portNo = workMessage.getNewNodeMessage().getNodeInfo().getPortNo();

				System.out.println("my friend i.e leader: " + nodeId + " host: " + host + " port: " + portNo);

				WorkMessage amNewNodeMsg = MessageGenerator.getInstance().generateIamNewNodeMsg();
				if(channel != null && channel.isOpen()) {
					channel.writeAndFlush(amNewNodeMsg);
					System.out.println("send message to friend for replicatioon");
				}
				else 
					System.out.println("Leader Channel got closed:while sending i am new node");
				
				state.getEmon().createOutboundIfNew(nodeId,host,portNo);				
				state.getEmon().getOutBoundEdgesList().getNode(nodeId).setChannel(channel);
				state.getEmon().getOutBoundEdgesList().getNode(nodeId).setActive(true);
			}
			else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.LEADER_INFO){
				//Add to you OB edges
				//once channel gets created in Emon .. send I_am_new_node msg to Leader
				int nodeId = workMessage.getNewNodeMessage().getNodeInfo().getNodeId();
				String host = workMessage.getNewNodeMessage().getNodeInfo().getHostAddr();
				int portNo = workMessage.getNewNodeMessage().getNodeInfo().getPortNo();

				Channel ch = connectToChannel(host,portNo,state);

				state.getEmon().createOutboundIfNew(nodeId,host,portNo);				
				state.getEmon().getOutBoundEdgesList().getNode(nodeId).setChannel(ch);
				state.getEmon().getOutBoundEdgesList().getNode(nodeId).setActive(true);

				WorkMessage amNewNodeMsg = MessageGenerator.getInstance().generateIamNewNodeMsg();
				if(ch != null && ch.isOpen())
					ch.writeAndFlush(amNewNodeMsg);
				else 
					System.out.println("Leader Channel got closed:while sending i am new node");

			}else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.ALL_NODE_INFO){
				logger.info("New node received details about cluster : " + workMessage.getHeader().getNodeId());	
				int listSize = workMessage.getNewNodeMessage().getAllNodeInfo().getNodesInfoList().size();
				for (int i = 0; i < listSize ; i++) {
					// For all the nodes get the node info and create channels
					int nodeId = workMessage.getNewNodeMessage().getAllNodeInfo().getNodesInfoList().get(i).getNodeId();
					String hostAddress = workMessage.getNewNodeMessage().getAllNodeInfo().getNodesInfoList().get(i).getHostAddr();
					int portNo = workMessage.getNewNodeMessage().getAllNodeInfo().getNodesInfoList().get(i).getPortNo();

					WorkMessage heyThereMsg = MessageGenerator.getInstance().generateHeyThereMessage();

					Channel ch = connectToChannel(hostAddress, portNo, state);
					if(ch != null && ch.isOpen())
						ch.writeAndFlush(heyThereMsg);
					else 
						System.out.println("Leader Channel got closed:while sending i am new node");

					state.getEmon().createOutboundIfNew(nodeId, hostAddress, portNo);
					state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(ch);
					state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true);
				}
				
				/*
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createOutboundIfNew(workMessage.getHeader().getNodeId(), addr.getAddress().toString(), workMessage.getNewNodeMessage().getPortNo());				
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true); 
				*/
			}
			
			else if(workMessage.getNewNodeMessage().getMsgType() == NewNodeMsgType.HEY_THERE) {
				state.getElectionCtx().setAmReady(true);
				int nodeId = workMessage.getNewNodeMessage().getNodeInfo().getNodeId();
				String hostAddress = workMessage.getNewNodeMessage().getNodeInfo().getHostAddr();
				int portNo = workMessage.getNewNodeMessage().getNodeInfo().getPortNo();

				state.getEmon().createOutboundIfNew(nodeId, hostAddress, portNo);
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);
				state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true);

				System.out.println("Done: I have got a new friend");
			}
			else {
				System.out.println("Unknown new node message type sent: " + workMessage.getNewNodeMessage().getMsgType());
			}
		}
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}
}