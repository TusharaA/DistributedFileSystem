package gash.router.server.election;

import java.nio.channels.Channel;
import java.util.Random;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.WorkHandler;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import gash.router.util.RaftMessageBuilder;
import pipe.work.Work.WorkMessage;

public class RaftElectionContext implements Runnable {
	ServerState state;
	IRaftNodeState follower;
	IRaftNodeState candidate;
	IRaftNodeState leader;
	IRaftNodeState currentState;
	String timerName ;


	private int term = 0;
	private RoutingConf conf;
	private EdgeMonitor emon;
	private int leaderId;
	private boolean amReady = true;

	private int heartbeatdt = 3000;
	private long timeOut = 3000;
	private Random rand;
	private long timerBegin = 0;


	public RaftElectionContext(ServerState state){
		this.state =state;
	}
	public void init() {

		follower = new FollowerState();
		follower.setElectionContext(this);

		candidate = new CandidateState();
		candidate.setElectionContext(this);

		leader = new LeaderState();
		leader.setElectionContext(this);

		rand = new Random();
		
		heartbeatdt = conf.getHeartbeatDt();
		generateTimeOut();
		timeOut += 5000;

		this.currentState = follower;

	}

	protected void broadcast(WorkMessage msg){		
		for(EdgeInfo ei : emon.getOutBoundEdgesList().getEdgeListMap().values()){	
			if(ei.isActive() && ei.getChannel()!=null){
				System.out.println("Sending HB::"+ei.getRef());
				ei.getChannel().writeAndFlush(msg);
			}
		}
		/*
		for(EdgeInfo ei : emon.getInBoundEdgesList().getEdgeListMap().values()){	
			if(ei.isActive() && ei.getChannel()!=null){
				ei.getChannel().writeAndFlush(msg);
			}
		}*/
	}


	@Override
	public void run() {		
		while(true){
			int activeChannel = state.getEmon().getActiveChannels();
			if(activeChannel>=2){
				timerBegin = System.currentTimeMillis();
				currentState.doAction(); 
			} 
		}

	}

	//Timer 
	public synchronized void generateTimeOut() {
		int temp =  rand.nextInt(heartbeatdt)+heartbeatdt;
		timeOut = (long)temp;		
	}

	//Timer
	public synchronized void computeTime() {
		timeOut = timeOut - (System.currentTimeMillis() - timerBegin);
	}


	/* Getters and setter */
	public int getTerm() {
		return term;
	}
	public void setTerm(int term) {
		this.term = term;
	}

	public RoutingConf getConf() {
		return conf;
	}
	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public EdgeMonitor getEmon() {
		return emon;
	}
	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}
	
	public IRaftNodeState getCurrentState() {
		return currentState;
	}
	public void setCurrentState(IRaftNodeState currentState) {
		this.currentState = currentState;
	}
	
	public int getLeaderId() {
		return leaderId;
	}
	public void setLeaderId(int nodeId) {
		this.leaderId = nodeId;		
	}
	public long getTimeOut() {
		return timeOut;
	}
	public void setTimeOut(long timeOut) {
		this.timeOut = timeOut;
	}
	public boolean isAmReady() {
		return amReady;
	}
	public void setAmReady(boolean amReady) {
		this.amReady = amReady;
	}

}
