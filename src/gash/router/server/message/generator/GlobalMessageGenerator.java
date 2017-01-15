package gash.router.server.message.generator;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;

public class GlobalMessageGenerator {
	private static RoutingConf routingConf;
	protected static Logger logger = LoggerFactory.getLogger(GlobalMessageGenerator.class);
	protected static AtomicReference<GlobalMessageGenerator> instance = new AtomicReference<GlobalMessageGenerator>();

	public static GlobalMessageGenerator initGenerator() {
		instance.compareAndSet(null, new GlobalMessageGenerator());
		return instance.get();	
	}

	public static GlobalMessageGenerator getInstance() {
		GlobalMessageGenerator globalMessageGenerator = instance.get();
		if(globalMessageGenerator == null) {
			instance.compareAndSet(null, new GlobalMessageGenerator());
			globalMessageGenerator = instance.get();
			logger.error(" Error while getting instance of GlobalMessageGenerator");
		}
		return globalMessageGenerator;
	}

	public static void setRoutingConf(RoutingConf routingConf) {
		GlobalMessageGenerator.routingConf = routingConf;
	}
}