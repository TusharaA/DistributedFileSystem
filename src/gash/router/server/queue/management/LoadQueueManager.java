package gash.router.server.queue.management;

import gash.router.server.edges.EdgeMonitor;
import gash.router.util.Constants;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadQueueManager {
	protected static Logger logger = LoggerFactory.getLogger(LoadQueueManager.class);
	protected static AtomicReference<LoadQueueManager> instance = new AtomicReference<LoadQueueManager>();
	protected static PriorityQueue<NodeLoad> loadQueue;
	
	public static LoadQueueManager initLoadQueueManager() {
		instance.compareAndSet(null, new LoadQueueManager());
		return instance.get();
	}

	public static LoadQueueManager getInstance() {
		if (instance == null)
			instance.compareAndSet(null, new LoadQueueManager());
		return instance.get();
	}
	
	public LoadQueueManager() {
		logger.info(" Started the Load Queue Manager ");

        loadQueue = new PriorityQueue<NodeLoad>(new Comparator<NodeLoad>(){
            public int compare(NodeLoad a, NodeLoad b){
                return a.load - b.load;
            }
        });

	}
	
	public synchronized void insertNodeLoadInfo(NodeLoad node){
		if(!loadQueue.contains(node)) {
			System.out.println("Adding node in load: " + node.nodeId);
			loadQueue.offer(node);
		}
		
	}
	
	public NodeLoad getMininumNodeLoadInfo(int chunkCount){
		NodeLoad node = loadQueue.poll();
		if(node != null){
			int totalCount = node.getLoad() + chunkCount;
			if(totalCount > Constants.MaxLoadCount){
				setAllNodeCountsZero();
			} else {
				node.setLoad(totalCount);
				insertNodeLoadInfo(node);
			}
		}
		//System.out.println("Retrieving Node id:" + node.getNodeId());
		return node;
	}
	
	public void setAllNodeCountsZero(){
		for(int i =0; i < loadQueue.size(); i++){
			NodeLoad node = loadQueue.poll(); 
			if(node != null){
				node.setLoad(0);
			}
			loadQueue.offer(node);
		}
	}
	
	public boolean isLoadQueueNull(){
		return (loadQueue == null);
	}
	
	public PriorityQueue<NodeLoad> getLoadQueue(){
		return loadQueue;
	}
	
	//To remove
	public void printInfo(){
		System.out.println("Node size " + loadQueue.size());
	/*	for(int i = 0; i < loadQueue.size(); i++) {
			NodeLoad node = loadQueue.poll(); 
			System.out.println("Nodeid : " + node.nodeId);
		} */
	}
	
}


