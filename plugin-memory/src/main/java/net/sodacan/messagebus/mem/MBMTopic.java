/*
 * Copyright 2023 John M Churin
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sodacan.messagebus.mem;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.messagebus.MBTopic;

/**
 * A Memory Based Topic
 * @author John Churin
 *
 */
public class MBMTopic implements MBTopic, Comparator<MBRecord> {
	private final static Logger logger = LoggerFactory.getLogger(MBMTopic.class);

	private Map<String,BlockingQueue<MBRecord>> queues;
	private PriorityBlockingQueue<MBRecord> combinedQueue = new PriorityBlockingQueue<>(200, this);
	
	private Future<?> followFuture;
	private Map<String,Long> topics;
	
	@SuppressWarnings("unused")
	private Map<String,String> configProperties;
	
	private static ExecutorService executorService = Executors.newCachedThreadPool();

	/**
	 * We get passed in a pointer to the topic's queue so that we can serve records from it.
	 * This is the actual queue, not a snapshot
	 * @param topicName
	 * @param nextOffset
	 * @param queue
	 */
	public MBMTopic(Map<String,String> configProperties, Map<String,Long> topics, Map<String,BlockingQueue<MBRecord>> queues) {
		this.topics = topics;
		this.configProperties = configProperties;
		this.queues = queues;
	}
	
	/**
	 * Load up the priority so that messages are sorted by timestamp.
	 * This method doesn't wait, so it should be called from a sleep loop.
	 * @throws InterruptedException
	 */
	public boolean loadCombinedQueue() throws InterruptedException {
		boolean result = false;
		for (Entry<String, BlockingQueue<MBRecord>> e : queues.entrySet()) {
			BlockingQueue<MBRecord> q = e.getValue();
			Long startingOffset = topics.get(e.getKey());
			if (startingOffset==null) {
				startingOffset=0L;
			}
			while (q.peek()!=null) {
				MBRecord r = q.take();
				if (r.getOffset()>= startingOffset) {
					result = true;
					combinedQueue.put(r);
				}
			}
		}
		return result;
	}
	
	/**
	 * Return a reduced snapshot of the queue.
	 */
	@Override
	public Map<String, MBRecord> snapshot() {
		Map<String, MBRecord> map = new HashMap<>();
		try {
			// Reduce the queues to the most recent of each key
			while (loadCombinedQueue()) {
				MBRecord record = combinedQueue.take();
				// If the value is null, delete that key from the map
				if (record.getValue()==null) {
					map.remove(record.getKey());
				} else {
					map.put(record.getKey(), record);
				}
			}
		} catch (InterruptedException e) {
		}
		return map;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void stop() {
    	followFuture.cancel(true);
	}

	@Override
	public Future<?> follow(Consumer<MBRecord> cs) {
		followFuture = executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						// Load up priority queue
						if (loadCombinedQueue()) {
							// Process one record
							MBRecord record = combinedQueue.take();
							cs.accept(record);
						} else {
							// Nothing in the queue so sleep for a while
							Thread.sleep(1000);
						}
					}
				} catch (InterruptedException e) {
					logger.debug("Exiting MBMTopic - follow for " + topics);
				}
			}
			
		});
		return followFuture;
	}
	
	@Override
	public int compare(MBRecord mbr1, MBRecord mbr2) {
		return Long.compare( mbr1.getTimestamp(),mbr2.getTimestamp());
	}
}
