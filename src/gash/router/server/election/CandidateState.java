package gash.router.server.election;

import gash.router.server.edges.EdgeInfo;
import gash.router.util.RaftMessageBuilder;
import pipe.work.Work.WorkMessage;

public class CandidateState implements IRaftNodeState {
	private RaftElectionContext electionCtx;
	private int voteCount = 0;
	private boolean voted = false;
	private int totalActiveNodes = 1;

	@Override
	public synchronized void doAction() {
		electionCtx.computeTime();
		if(electionCtx.getTimeOut()<=0) {
			System.out.println("Node " + electionCtx.state.getConf().getNodeId() + " timed out");
			sendRequestVoteNotice();	
			electionCtx.generateTimeOut();
			return;
		}
	}

	/* Method to request votes during election process */
	public synchronized void sendRequestVoteNotice() {
		System.out.println("I am a candidate  ::::" + electionCtx.state.getConf().getNodeId());
		electionCtx.setTerm(electionCtx.getTerm()+1);
		totalActiveNodes = 1;
		voteCount = 0;
		voted = true; //self Voting
		voteCount++;
		electionCtx.generateTimeOut();
		electionCtx.broadcast(RaftMessageBuilder.buildVoteRequestMessage(electionCtx.getTerm()));
		return;
	}
	
	/* Method to count the votes received and decide on leadership during election process */
	@Override
	public synchronized void voteRecieved(WorkMessage msg) {
		System.out.println("Vote Received from : "+msg.getHeader().getNodeId() + "is : "+msg.getRaftMessage().getElectionMessage().getVoteGranted());
		if(msg.getRaftMessage().getElectionMessage().getVoteGranted()){			
			voteCount++;
			totalActiveNodes = 1;
			for(EdgeInfo ei : electionCtx.getEmon().getOutBoundEdgesList().getEdgeListMap().values()){
				if(ei.isActive() && ei.getChannel()!=null)
				{
					totalActiveNodes++;
				}
			}							
			if(voteCount > (totalActiveNodes/2) ){
				//electionCtx.timer.updateTimer();
				electionCtx.generateTimeOut();
				System.out.println("Yay! I Won the election! I am the Leader. My Node Id is " + electionCtx.getConf().getNodeId());
				electionCtx.setLeaderId(electionCtx.getConf().getNodeId());
				electionCtx.setCurrentState(electionCtx.leader);				
				voted =false; //reset once the leader is elected
				voteCount = 0; //reset once the leader is elected
			}
		}
		return;
	}
	
	/* Method to respond to Vote Requested by other Nodes **/
	@Override
	public synchronized void VoteRequestReceived(WorkMessage msg) {
		System.out.println("Entered in candidate's vote request received");
		sendVote(msg, false);
	}
	
	/* Method to cast Vote during election process */
	@Override
	public synchronized void sendVote(WorkMessage msg, boolean voteGranted) {
		EdgeInfo ei = electionCtx.getEmon().getOutBoundEdgesList().getEdgeListMap().get(msg.getHeader().getNodeId());
		if(ei.getChannel()!=null)
			ei.getChannel().writeAndFlush(RaftMessageBuilder.buildVoteResponseMessage(msg.getHeader().getNodeId(), voteGranted, electionCtx.getTerm())); 
	}
	
	/* setters and getters */
	@Override
	public void setElectionContext(RaftElectionContext ctx) {
		this.electionCtx =ctx;	
	}

	@Override
	public RaftElectionContext getElectionContext() {
		// TODO Auto-generated method stub
		return electionCtx;
	}

	@Override
	public void sendHearbeatAck(WorkMessage msg) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void getHearbeatFromLeader(WorkMessage msg) {
		// TODO Auto-generated method stub
		
	}

}
