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
package net.sodacan.messagebus.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.messagebus.MBTopic;
/**
 * A wrapper around KafkaConsumer.
 * This is not thread safe. It is expected that a topic will be opened whenever it is needed.
 * We close the topic after completion of one of the snapshot/follow methods.
 * @author John Churin
 *
 */
public class MBKTopic implements MBTopic, Comparator<MBRecord>, ConsumerRebalanceListener {
	private final static Logger logger = LoggerFactory.getLogger(MBKTopic.class);
	private Duration poll_timeout_ms = Duration.ofMillis(200);
	
	private Map<String,Long> topics;
	private List<TopicPartition> partitions;
	private Map<String,Long> endOffsets;
	
//	private String topicName;
	private Map<String,String> configProperties;

	private PriorityBlockingQueue<MBRecord> combinedQueue = new PriorityBlockingQueue<>(200, this);

	// Only used for a follow, not a snapshot
	private AtomicBoolean closed;
	private Consumer<String,String> consumer;
	
	private static ExecutorService executorService = Executors.newCachedThreadPool();


	/**
	 * Construct a topic for consuming records
	 * @param brokers
	 * @param topicName
	 * @param nextOffset
	 */
	protected MBKTopic(Map<String,String> configProperties, Map<String,Long> topics) {
		this.configProperties = configProperties;
		this.topics = topics;
		String pollTimeout = configProperties.get("poll.timeout.ms");
		if (pollTimeout!=null) {
			poll_timeout_ms = Duration.ofMillis(Integer.parseInt(pollTimeout));
		}
	}
	
	protected Consumer<String, String> openConsumer() {
		Consumer<String, String> consumer = null;
		Properties properties = new Properties();
		String brokers = configProperties.get("brokers");
		if (brokers==null || brokers.isEmpty()) {
			throw new RuntimeException("brokers connection not specified in MessageBus config");
		}
		properties.setProperty("bootstrap.servers", brokers);
//		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "false");
		properties.setProperty("max.partition.fetch.bytes", "100000");
		properties.setProperty("fetch.max.wait.ms", "80");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(properties);
		partitions = new LinkedList<TopicPartition>();
		for (Entry<String,Long> e : topics.entrySet()) {
			TopicPartition partition = new TopicPartition(e.getKey(), 0);
			partitions.add(partition);
		}
		consumer.assign(partitions);
		// Get positioned for each topic
		for (TopicPartition tp : partitions) {
			Long offset = topics.get(tp.topic());
			if (offset==null) {
				offset = 0L;
			}
			consumer.seek(tp,offset+1);
		}
		// Get the ending offset for each topic
		Map<TopicPartition,Long> oe = consumer.endOffsets(partitions);
		endOffsets = new HashMap<>();
		for (Entry<TopicPartition, Long> e : oe.entrySet()) {
			// If the offset is zero, don't even bother putting it in the map
			if (e.getValue()!=0) {
				endOffsets.put(e.getKey().topic(), e.getValue());
			}
		}
		return consumer;
	}
	
	/**
	 * If we reach the end of a topic, remove it from the endOffsets map
	 * @param record
	 */
	protected void checkEndOffsets(MBRecord record) {
		if (endOffsets.isEmpty()) {
			return;
		}
		Long endOffset = endOffsets.get(record.getTopic());
		if (endOffset==null) {
			return;
		}
		endOffset--;
		if (record.getOffset()>=endOffset) {
			endOffsets.remove(record.getTopic());
		}
	}
	
	/**
	 * Load up the priority queue so that messages are sorted by timestamp.
	 * This method doesn't wait, so it should be called from a sleep loop.
	 * @Returns true if any results were added to the priority Queue
	 * @throws InterruptedException
	 */
	protected boolean loadCombinedQueue() throws InterruptedException {
		while (!endOffsets.isEmpty()) {
			ConsumerRecords<String, String> records = consumer.poll(poll_timeout_ms);
			for (ConsumerRecord<String,String> record : records ) {
				MBRecord mbr = new MBKRecord(record);
				combinedQueue.put(mbr);
				checkEndOffsets( mbr );
			}
		}
		return !combinedQueue.isEmpty();
	}

	/**
	 * <p>Load up a reduced list of records from this topic. Only the most recent of any
	 * key is in the map.</p>
	 * <p>A snapshot usually begins at offset zero and includes everything up to
	 * the end offset known when the topic was opened. A snapshot also accounts for deleted keys
	 * (tombstones in Kafka).</p>
	 */
	public Map<String, MBRecord> snapshot() {
		Map<String, MBRecord> mbrs = new HashMap<>();
		consumer = openConsumer();
		try {
			while (loadCombinedQueue()) {
				MBRecord record = combinedQueue.take();
				// If no value, then remove this key from the map. Otherwise, put the updated
				// Record into the map.
				if (record.getValue()==null) {
					mbrs.remove(record.getKey());
				} else {
					mbrs.put(record.getKey(), record);
				}
				checkEndOffsets( record );
			}
		
		} catch (Throwable e) {
			logger.error("Problem consuming from topic(s): " + topics);
			logger.error(e.getMessage());
			Throwable t = e.getCause();
			while (t!=null) {
				logger.error("  " + t.getMessage());
				t = t.getCause();
			}
			return null;
		} finally {
			consumer.close();
		}
		return mbrs;

	}
	
	/**
	 * For snapshot requests, we close before returning the snapshot.
	 * For follows, we need to chase down any streams and kill them.
	 */
	@Override
	public void close() throws IOException {
		
	}
	/**
	 * When we lose a topic
	 */
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//		TopicPartition tp = new TopicPartition(TICK_TOPIC, 0);
//		if (partitions.contains(tp)) {
//	    	logger.debug("TickSource: Going inactive" + partitions);
//			nextTime = null;	// This keeps the main loop quiet while standing by
//		}
	}

	/**
	 * When we get control, we start by reading the last record in topic.
	 * This seeds the nextTick value.
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//		TopicPartition tp = new TopicPartition(TICK_TOPIC, 0);
//		Map<TopicPartition,Long> endOffsets;
//		if (partitions.contains(tp)) {
//			nextTime = null;	// This initializes the main loop
//	    	endOffsets = consumer.endOffsets(partitions);
//	    	Long endOffset = endOffsets.get(tp);
//	    	if (endOffset > 0L ) {
//	    		endOffset--;
//	    		consumer.seek(tp, endOffset);
//	    	}
//	    	logger.debug("TickSource: Going active at offset: " + endOffset);
//		}
	}

	

	/**
	 * Load up the priority queue so that messages are sorted by timestamp.
	 * @Returns true if any results were added to the priority Queue
	 * @throws InterruptedException
	 */
	protected int loadFollowQueue() throws InterruptedException {
		int MAX_COMBINED_COUNT = 100;
		int count = 0;
		// Limit how many times we grab records
		for (int c = 0; c < MAX_COMBINED_COUNT; c++ ) {
			ConsumerRecords<String, String> records = consumer.poll(poll_timeout_ms);
			if (records.isEmpty()) {
				break;
			}
			for (ConsumerRecord<String,String> record : records ) {
				MBRecord mbr = new MBKRecord(record);
				combinedQueue.put(mbr);
				count++;
			}
		}
		return count;
	}

	/**
	 * <p>Follow a topic starting from the supplied offset until forever, or when the stream is closed.</p>
	 * <p>To feed a stream, we create a new thread to wait for records to arrive. For Kafka, it 
	 * is critical that consumption of the stream remain lively. The caller should get back to reading
	 * records from the stream as quickly as possible. Otherwise, no keep-alive signal is sent to the broker.
	 * </p>
	 */
	@Override
	public Future<?> follow(java.util.function.Consumer<MBRecord> cs) {
		closed = new AtomicBoolean(false);
		executorService.execute(new Runnable() {
			@Override 
		    public void run() {
				try {
					consumer = openConsumer();
					while (!Thread.currentThread().isInterrupted()) {
						int count = loadFollowQueue();
						for( int c = 0; c <  count;c++) {
							cs.accept( combinedQueue.take());
						}
					}
		        } catch (WakeupException e) {
		        	logger.debug("Exiting MBKTopic");
		        } catch (Throwable e) {
		            	 logger.error("Error from Kafka topic, existing this follow operation", e);
		        } 
				finally {
                	consumer.close();
				}
		    }
		});
		// This thread just waits for a cancel request and sends a wakeup to Kafka. Interrupting Kafka is a no-no.
		Future<?> future = executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						Thread.sleep(60*1000);
					}
		        } catch (Exception e) {
		        	consumer.wakeup();
				}
			}
			
		});
		return future;
	}
	
	@Override
	public void stop() {
    	closed.set(true);
    	consumer.wakeup();
	}

	@Override
	public int compare(MBRecord mbr1, MBRecord mbr2) {
		return Long.compare( mbr1.getTimestamp(),mbr2.getTimestamp());
	}

}
