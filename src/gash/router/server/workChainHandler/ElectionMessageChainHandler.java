package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.election.Election.RaftElectionMessage.ElectionMessageType;
import pipe.work.Work.WorkMessage;

public class ElectionMessageChainHandler implements IWorkChainHandler {
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(ElectionMessageChainHandler.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		this.state = state;
		this.nextChainHandler = nextChain;		
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		if(workMessage.hasRaftMessage() && workMessage.getRaftMessage().getType() == ElectionMessageType.VOTE_REQUEST) {
			state.getElectionCtx().getCurrentState().VoteRequestReceived(workMessage);
		} else if(workMessage.hasRaftMessage() && workMessage.getRaftMessage().getType() == ElectionMessageType.VOTE_RESPONSE) {
			state.getElectionCtx().getCurrentState().voteRecieved(workMessage);
		} else if(workMessage.hasRaftMessage() && workMessage.getRaftMessage().getType() == ElectionMessageType.LEADER_HEARTBEAT) {
			state.getElectionCtx().getCurrentState().getHearbeatFromLeader(workMessage);
		}else if (workMessage.hasLeader() && workMessage.getLeader().getAction() == LeaderQuery.WHOISTHELEADER  && state.getElectionCtx().getTerm()>0 ) {
			WorkMessage buildNewNodeLeaderStatusResponseMessage = RaftMessageBuilder
					.buildTheLeaderIsMessage(state.getElectionCtx().getLeaderId(),state.getElectionCtx().getTerm());				
			ChannelFuture cf = channel.write(buildNewNodeLeaderStatusResponseMessage);
			try{
			channel.flush();
			cf.awaitUninterruptibly();
			if (cf.isDone() && !cf.isSuccess()) {
				logger.info("Failed to write the message to the channel ");
			}		
			}catch(Exception e){
				System.out.println("Exception at Who is the Leader"+e.getMessage());
			}
			state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);

		}else if (workMessage.hasLeader() && workMessage.getLeader().getAction() == LeaderQuery.THELEADERIS /*&& msg.getLeader().getState()==LeaderState.LEADERALIVE*/) {
			System.out.println("Current leader is ::"+workMessage.getLeader().getLeaderId());
			state.getElectionCtx().setLeaderId(workMessage.getLeader().getLeaderId());
			state.getElectionCtx().setTerm(workMessage.getLeader().getTerm());
		} else if(workMessage.hasRaftMessage() && workMessage.getRaftMessage().getType() == ElectionMessageType.LEADER_HB_ACK) {
			state.getElectionCtx().getCurrentState().sendHearbeatAck(workMessage);
		} else {
			this.nextChainHandler.handle(workMessage, channel);
		}		
	}

}
