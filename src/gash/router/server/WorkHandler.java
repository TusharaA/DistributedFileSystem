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
package gash.router.server;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import gash.router.server.workChainHandler.ElectionMessageChainHandler;
import gash.router.server.workChainHandler.FailureHandler;
import gash.router.server.workChainHandler.HeartBeatHandler;
import gash.router.server.workChainHandler.IWorkChainHandler;
import gash.router.server.workChainHandler.NewNodeChainHandlerV2;
import gash.router.server.workChainHandler.PingHandler;
import gash.router.server.workChainHandler.TaskHandler;
import gash.router.server.workChainHandler.WorkStealHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.election.Election.RaftElectionMessage.ElectionMessageType;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger(WorkHandler.class);
	protected ServerState state;
	protected boolean debug = false;

	IWorkChainHandler hearBeatChainHandler;
	IWorkChainHandler pingMessageChainHandler;
	IWorkChainHandler failureMessageChainHandler;
	IWorkChainHandler taskMessageChainHandler;
	IWorkChainHandler workStealMessageChainHandler;
	IWorkChainHandler electionMessageChainHandler;
	IWorkChainHandler newNodeChainHandler;
	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		} else {
			return;
		}
		this.hearBeatChainHandler = new HeartBeatHandler();
		this.pingMessageChainHandler = new PingHandler();
		this.failureMessageChainHandler = new FailureHandler();
		this.taskMessageChainHandler = new TaskHandler();
		this.workStealMessageChainHandler = new WorkStealHandler();
		this.electionMessageChainHandler = new ElectionMessageChainHandler();
		this.newNodeChainHandler = new NewNodeChainHandlerV2();

		this.hearBeatChainHandler.setNextChain(electionMessageChainHandler,state);
		this.electionMessageChainHandler.setNextChain(newNodeChainHandler,state);
		this.newNodeChainHandler.setNextChain(pingMessageChainHandler,state);		
		this.pingMessageChainHandler.setNextChain(failureMessageChainHandler,state);
		this.failureMessageChainHandler.setNextChain(workStealMessageChainHandler, state);
		this.workStealMessageChainHandler.setNextChain(taskMessageChainHandler, state);
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug)
			PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {
			hearBeatChainHandler.handle(msg, channel);
			 
		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			// changing e.getMessage to some string
			//eb.setMessage(e.getMessage());
			eb.setMessage("fixing the null pointer");
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();

	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}