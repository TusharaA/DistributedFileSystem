package gash.router.server.election;

import gash.router.server.edges.EdgeInfo;
import gash.router.util.RaftMessageBuilder;
import pipe.work.Work.WorkMessage;

public class LeaderState implements IRaftNodeState {
	
	private RaftElectionContext electionCtx;
	
	@Override
	public void doAction() {
		try{
			WorkMessage leaderHeartBeatMsg = RaftMessageBuilder.buildLeaderResponseMessage(electionCtx.getConf().getNodeId(), electionCtx.getTerm());
			electionCtx.setLeaderId(electionCtx.getConf().getNodeId());
			electionCtx.broadcast(leaderHeartBeatMsg);	
			//System.out.println("Sending heartbeat");
			Thread.sleep(electionCtx.getConf().getHeartbeatDt()/4);
		}catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void voteRecieved(WorkMessage msg) {
		return;		
	}

	/* Method to respond to Vote Requested by other Nodes **/
	@Override
	public void VoteRequestReceived(WorkMessage msg) {
		if(msg.getRaftMessage().getTerm() > electionCtx.getTerm()){
			electionCtx.setTerm(electionCtx.getTerm()+1);
			electionCtx.setCurrentState(electionCtx.follower);
			electionCtx.getCurrentState().VoteRequestReceived(msg);
		}	
		if(msg.getRaftMessage().getTerm() < electionCtx.getTerm())
			sendVote(msg, false);			
	}
	
	/* Method to cast Vote during election process */
	@Override
	public void sendVote(WorkMessage msg, boolean voteGranted) {
		EdgeInfo ei = electionCtx.getEmon().getOutBoundEdgesList().getEdgeListMap().get(msg.getHeader().getNodeId());
		WorkMessage voteResponseMsg = RaftMessageBuilder.buildVoteResponseMessage(msg.getHeader().getNodeId(), voteGranted, electionCtx.getTerm());
		if(ei.getChannel()!=null)
			ei.getChannel().writeAndFlush(voteResponseMsg); 		
	}
	
	@Override
	public void setElectionContext(RaftElectionContext ctx) {
		// TODO Auto-generated method stub
		this.electionCtx = ctx;
	}

	@Override
	public RaftElectionContext getElectionContext() {
		// TODO Auto-generated method stub
		return electionCtx;
	}

	@Override
	public void sendHearbeatAck(WorkMessage msg) {
	}

	@Override
	public void getHearbeatFromLeader(WorkMessage msg) {
		// TODO Auto-generated method stub
		
	}
}
