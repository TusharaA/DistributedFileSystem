package gash.router.server.queue.management;

import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class QueueManager {
	protected static Logger logger = LoggerFactory.getLogger(QueueManager.class);
	protected static AtomicReference<QueueManager> instance = new AtomicReference<QueueManager>();

	/**
	 * Command queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> inboundCommandQueue;
	protected LinkedBlockingDeque<InternalChannelNode> outboundCommandQueue;

	/**
	 * Read Work Queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> outboundWorkReadQueue;
	protected LinkedBlockingDeque<InternalChannelNode> inboundWorkReadQueue;	

	/**
	 * Write Work Queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> outboundWorkWriteQueue;
	protected LinkedBlockingDeque<InternalChannelNode> inboundWorkWriteQueue;
	
	protected InboundCommandQueueThread inboundCommmanderThread;
	protected OutboundWorkWriteQueueThread outboundWorkWriterThread;
	protected InboundWorkWriteQueueThread inboundWorkWriterThread;
	protected OutboundCommandQueueThread outboundCommanderThread;
	protected OutboundWorkReadThread outboundReadThread;
	protected InboundReadQueueThread inboundReadThread;
	
	
	public static QueueManager initManager() {
		instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public static QueueManager getInstance() {
		if (instance == null)
			instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}
	
	public QueueManager() {
		logger.info(" Started the Manager ");

		inboundCommandQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundCommmanderThread = new InboundCommandQueueThread(this);
		inboundCommmanderThread.start();
		
		outboundWorkWriteQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundWorkWriterThread = new OutboundWorkWriteQueueThread(this);
		outboundWorkWriterThread.start();
		
		outboundCommandQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundCommanderThread = new OutboundCommandQueueThread(this);
		outboundCommanderThread.start();
		
		outboundWorkReadQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundReadThread = new OutboundWorkReadThread(this);
		outboundReadThread.start();
		
		inboundWorkReadQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundReadThread = new InboundReadQueueThread(this);
		inboundReadThread.start();
		
		inboundWorkWriteQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundWorkWriterThread = new InboundWorkWriteQueueThread(this);
		inboundWorkWriterThread.start();
	}
	
	public void enqueueInboundCommmand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}
	
	public InternalChannelNode dequeueInboundCommmand() throws InterruptedException {
			return inboundCommandQueue.take();
	}
	
	
	public void enqueueOutboundWorkWrite(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundWorkWriteQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("write work message is not enqueued for processing", e);
		}
	}
	
	public InternalChannelNode dequeueOutboundWorkWrite() throws InterruptedException {
			return outboundWorkWriteQueue.take();
	}
	
	public void returnOutboundWorkWrite(InternalChannelNode channelNode) throws InterruptedException {
		outboundWorkWriteQueue.putFirst(channelNode);
	}

	public void returnOutboundWorkRead(InternalChannelNode channelNode) throws InterruptedException {
		outboundWorkReadQueue.putFirst(channelNode);
	}
	
	public void enqueueOutboundCommmand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}
	
	public InternalChannelNode dequeueOutboundCommmand() throws InterruptedException {
			return outboundCommandQueue.take();
	}

	public void enqueueAtFrontOutboundCommand(InternalChannelNode channelNode) throws InterruptedException {
		outboundCommandQueue.putFirst(channelNode);
	}
	
	public void enqueueOutboundRead(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundWorkReadQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}
	
	public InternalChannelNode dequeueOutboundRead() throws InterruptedException {
		if(outboundWorkReadQueue.size() == 0)
			return null;
		return outboundWorkReadQueue.take();
	}
	
	public void enqueueInboundRead(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundWorkReadQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}
	
	public InternalChannelNode dequeueInboundRead() throws InterruptedException {
		if(inboundWorkReadQueue.size() == 0)
			return null;
		return inboundWorkReadQueue.take();
	}
	
	public void enqueueOutboundCommand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}
	
	public InternalChannelNode dequeueOutboundCommand() throws InterruptedException {
			return outboundCommandQueue.take();
	}
	
	public void enqueueInboundWorkWrite(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundWorkWriteQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in inbound work write queue for processing", e);
		}
	}
	
	public InternalChannelNode dequeueInboundWorkWrite() throws InterruptedException {
			return inboundWorkWriteQueue.take();
	}

	/**
	 * method for new node to see if the inbound write queue is empty
	 */
	public boolean isInboundWorkWriteQEmpty() {
		return this.inboundWorkWriteQueue.isEmpty();
	}
}
