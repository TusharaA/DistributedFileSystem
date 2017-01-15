/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
//
//C
//changes
package gash.router.server.edges;

//change
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.router.server.election.RaftElectionContext;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");
	public static ConcurrentHashMap<Integer, Channel> node2ChannelMap = new ConcurrentHashMap<Integer, Channel>();
	public static HashSet<Integer> loadNodeSet = new HashSet<Integer>();
	public static ConcurrentHashMap<String, InternalChannelNode> clientChannelMapInfo = new ConcurrentHashMap<String, InternalChannelNode>();
	private static Queue<Integer> workStealQueue = new LinkedBlockingQueue<Integer>();
	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private RaftElectionContext electionCtx;
	private boolean forever = true;
	private ArrayList<InetAddress> activeIps;


	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");
		this.electionCtx = state.getElectionCtx();
		this.electionCtx.setEmon(this);
		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);
		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				System.out.println("Node id " + e.getId() + " Host " + e.getHost());
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		RaftMessageBuilder.setRoutingConf(state.getConf());

		if (state.getConf().isNewNode()) {
			electionCtx.setAmReady(false);
		System.out.println("I am a new node:: sending a message to friend");
			for(EdgeInfo ei : outboundEdges.getEdgeListMap().values()){
				System.out.println("No routing entries..possibly a new node");
				try {
					Channel detectedChannel = null;
					try {
						Channel newChannel = connectToChannel(ei.getHost(), ei.getPort(), this.state);

						if (newChannel.isOpen() && newChannel != null) {

							System.out.println("Connected to channel : " + ei.getHost());
							WorkMessage wm = MessageGenerator.getInstance().generateNewNodeInitiationMsg();

							ChannelFuture cf = newChannel.write(wm);
							newChannel.flush();
							cf.awaitUninterruptibly();
							if (cf.isDone() && !cf.isSuccess()) {
								logger.info("Failed to write the message to the channel ");
							}
							if (detectedChannel == null) {
								logger.info("Setting up new channel to : "+ei.getHost());
								detectedChannel = newChannel;
							}
						}
					} catch (Exception e) {
						System.out.println("Connection unsuccessful: " + e.getMessage());
						System.out.println("Connection unsuccessful: " + ei.getHost());
					}
					if (detectedChannel != null) {
						WorkMessage whoIsTheLeaderMessage = RaftMessageBuilder.buildWhoIsTheLeaderMessage();
						logger.info("Trying to find the current leader in the cluster");
						ChannelFuture cf = detectedChannel.write(whoIsTheLeaderMessage);
						detectedChannel.flush();
						cf.awaitUninterruptibly();
						if (cf.isDone() && !cf.isSuccess()) {
							logger.info("Failed to write the message to the channel ");
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
		
		NodeMonitor nodeMonitor = new NodeMonitor();
		Thread thread = new Thread(nodeMonitor);
		thread.start();
	}

	
	
	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}

	public EdgeList getOutBoundEdgesList(){
		return outboundEdges;
	}

	public EdgeList getInBoundEdgesList(){
		return inboundEdges;
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setSecret(1234);

		return wb.build();
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						WorkMessage wm = createHB(ei);
						ei.getChannel().writeAndFlush(wm);
					} else if(ei.getChannel() == null){
						Channel channel = connectToChannel(ei.getHost(), ei.getPort());
						ei.setChannel(channel);
						ei.setActive(true);
						if (channel == null) {
							logger.info("trying to connect to node " + ei.getRef());
						}
					}
				}
				if(!state.getConf().isNewNode())
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						if(electionCtx.getTerm()== 0){
							WorkMessage whoIsTheLeaderMsg = RaftMessageBuilder.buildWhoIsTheLeaderMessage();
							broadcast(whoIsTheLeaderMsg);
						}
					}
				}
				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
	}

	private Channel connectToChannel(String host, int port) {
		Bootstrap b = new Bootstrap();
		Channel ch = null;
		try {
			b.group(new NioEventLoopGroup());
			b.channel(NioSocketChannel.class);
			b.handler(new WorkInit(state, false));

			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			ch = b.connect(host, port).syncUninterruptibly().channel();
			Thread.sleep(dt);
		} catch (Exception e) {
				//e.printStackTrace();
				//logger.info("trying to connect to node"+host);
			
		}
		return ch;

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

	public EdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public void setOutboundEdges(EdgeList outboundEdgesInfo) {
		outboundEdges = outboundEdgesInfo;
	}

	public EdgeList getInboundEdges() {
		return inboundEdges;
	}

	public void setInboundEdges(EdgeList inboundEdgesInfo) {
		inboundEdges = inboundEdgesInfo;
	}

	public void broadcast(WorkMessage msg){
		for(EdgeInfo ei:this.outboundEdges.map.values()){
			if (ei.isActive() && ei.getChannel() != null) 
				ei.getChannel().writeAndFlush(msg);

		}
		for(EdgeInfo ei:this.inboundEdges.map.values()){
			if (ei.isActive() && ei.getChannel() != null) 
				ei.getChannel().writeAndFlush(msg);

		}
	}
	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}

	public int getActiveChannels(){
		int activeChannel = 0;
		for(int node: node2ChannelMap.keySet()) {
			//System.out.println("Node : " + node + " Channel: " + node2ChannelMap.get(node));
			if(node2ChannelMap.get(node)!=null)
				activeChannel++;
		}
		return activeChannel;
	}

	public static String clientInfoMap(InternalChannelNode commandMessageNode) {
		UUID uuid = UUID.randomUUID();
		String clientUidString = uuid.toString();
		clientChannelMapInfo.put(clientUidString, commandMessageNode);
		return clientUidString;
	}

	public static synchronized InternalChannelNode getClientChannelFromMap(String clientId) {

		if (clientChannelMapInfo.containsKey(clientId) && clientChannelMapInfo.get(clientId) != null) {
			return clientChannelMapInfo.get(clientId);
		} else {
			logger.info("Unable to find the channel for client id : " + clientId);
			return null;
		}
	}

	public static synchronized void removeClientChannelInfoFromMap(String clientId) throws Exception {
		if (clientChannelMapInfo.containsKey(clientId) && clientChannelMapInfo.get(clientId) != null) {
			clientChannelMapInfo.remove(clientId);
		} else {
			logger.error("Unable to fetch channel for client with id " + clientId);
		}
	}

	public static Channel fetchChannelToStealReadWork() {
		Channel channel = null;
		//logger.info("work Steal Queue is empty: " + workStealQueue.isEmpty());

		if(!workStealQueue.isEmpty()) {
			// TO DO - need to see if the node is leader. if yes enqueue it.
			Integer nodeId = workStealQueue.remove();
			/*if(nodeId.intValue() == 5) {
				workStealQueue.add(nodeId);
				return null;
			}*/
			channel = node2ChannelMap.get(nodeId);
			workStealQueue.add(nodeId);
		}
		return channel;
	}

	// To continuously check addition and removal of nodes to the current node
	private class NodeMonitor implements Runnable {
		private boolean forever = true;

		@Override
		public void run() {
			try {
				while (forever) {
					addToNode2ChannelMap(getInboundEdges());
					addToNode2ChannelMap(getOutboundEdges());				}
			} catch (Exception e) {
				logger.error("An error has occured ", e);
			}
		}

		private void addToNode2ChannelMap(EdgeList edges) {
			try {

				if (edges != null) {
					ConcurrentHashMap<Integer, EdgeInfo> edgeListMapper = edges.getEdgeListMap();
					if (edgeListMapper != null && !edgeListMapper.isEmpty()) {
						Set<Integer> nodeIdSet = edgeListMapper.keySet();
						if (nodeIdSet != null)
							for (Integer nodeId : nodeIdSet) {
								
								if (nodeId != null && !node2ChannelMap.containsKey(nodeId)
										&& edgeListMapper.containsKey(nodeId)
										&& edgeListMapper.get(nodeId).getChannel() != null) {
									if(edgeListMapper.get(nodeId).getChannel() != null ) {
										logger.info("Added node " + nodeId + " " + edgeListMapper.get(nodeId).getHost()
												+ " to channel map. ");
										node2ChannelMap.put(nodeId, edgeListMapper.get(nodeId).getChannel());
										if(!workStealQueue.contains(nodeId)) {
											logger.info("Adding node to the stealing queue: " + nodeId);
											workStealQueue.add(nodeId);
										}
									}
								}
								
							    addNodeLoadToQueue(nodeId);
							}
					}
				}
			} catch (Exception exception) {
				logger.error("An Error has occured ", exception);
			}
		}
		
		
		private synchronized void addNodeLoadToQueue(Integer nodeId){
			if(nodeId != null && !loadNodeSet.contains(nodeId.intValue())){
				System.out.println("Adding loadNodeset: " + nodeId);
				loadNodeSet.add(nodeId.intValue());
				NodeLoad node = new NodeLoad(nodeId.intValue(), 0);
				LoadQueueManager.getInstance().insertNodeLoadInfo(node);
			} 
		}

	}
	public void connectToAll() {
		for(EdgeInfo ei:this.outboundEdges.map.values()){
			if(ei.getChannel() == null) 
				ei.setChannel(connectToChannel(ei.getHost(), ei.getPort()));
		}
		
	}
}


