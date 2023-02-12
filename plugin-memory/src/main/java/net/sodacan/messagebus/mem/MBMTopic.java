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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.messagebus.MBTopic;

/**
 * A Memory Based Topic
 * @author John Churin
 *
 */
public class MBMTopic implements MBTopic {
	private String topicName;
	long timestamp;
	long nextOffset;
	
	private BlockingQueue<MBMRecord> queue;
	private BlockingQueue<MBRecord> followQueue;
	private AtomicBoolean closed;
	private Future<?> followFuture;
	
	private static ExecutorService executorService = Executors.newCachedThreadPool();
	
	/**
	 * We get passed in a pointer to the topic's queue so that we can serve records from it.
	 * This is the actual queue, not a snapshot
	 * @param topicName
	 * @param nextOffset
	 * @param queue
	 */
	public MBMTopic(String topicName, long nextOffset, BlockingQueue<MBMRecord> queue) {
		this.topicName = topicName;
		this.nextOffset = nextOffset;
		this.queue = queue;
	}

	@Override
	public String getTopicName() {
		return topicName;
	}

	/**
	 * Return a reduced snapshot of the queue.
	 */
	@Override
	public Map<String, MBRecord> snapshot() {
		Map<String, MBRecord> map = new HashMap<>();
		// If the value is null, delete that key from the map
		for (MBMRecord record : queue) {
			if (record.getValue()==null) {
				map.remove(record.getKey());
			} else {
				map.put(record.getKey(), record);
			}
		}
		return map;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public BlockingQueue<MBRecord> follow() {
		followQueue = new LinkedBlockingQueue<MBRecord>();
		followFuture = executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (!closed.get()) {
						followQueue.offer(queue.take());
					}
				} catch (InterruptedException e) {
					followQueue.offer(new MBMRecord());
				}
				System.out.println("Leaving follow");
			}
		});
		return followQueue;
	}

	@Override
	public void stop() {
    	closed.set(true);
    	followFuture.cancel(true);
//    	consumer.wakeup();
	}
}
