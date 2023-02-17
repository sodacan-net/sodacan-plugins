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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
public class MBKTopic implements MBTopic {
	private final static Logger logger = LoggerFactory.getLogger(MBKTopic.class);
	public static final int QUEUE_SIZE = 20;

	// The nextOffset is where we desire to start consuming
	private long nextOffset;
	// endOffset is a bit misleading since these queues are "forever". It means the end offset as of the
	// topic being opened. This is needed for a snapshot function which returns records up to this point
	// even if, while fetching, additional records have been added.
	private long endOffset;
	
	private Duration poll_timeout_ms = Duration.ofMillis(200);
	
	private String topicName;
	private Map<String,String> configProperties;

	// Only used for a follow, not a snapshot
	private BlockingQueue<MBRecord> queue;
	private AtomicBoolean closed;
	private Consumer<String,String> consumer;
	
	private static ExecutorService executorService = Executors.newCachedThreadPool();


	/**
	 * Construct a topic for consuming records
	 * @param brokers
	 * @param topicName
	 * @param nextOffset
	 */
	protected MBKTopic(Map<String,String> configProperties, String topicName, long nextOffset) {
		this.nextOffset = nextOffset;
		this.topicName = topicName;		
		this.configProperties = configProperties;
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
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(properties);
		TopicPartition partition = new TopicPartition(topicName, 0);
		List<TopicPartition> partitions = Arrays.asList(partition);
		consumer.assign(partitions);
		// Get positioned
		consumer.seek(partition,nextOffset);
		// Get the ending offset, if any
		Map<TopicPartition,Long> endOffsets = consumer.endOffsets(partitions);
		endOffset = endOffsets.get(partition);
		return consumer;
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
		Consumer<String,String> consumer = openConsumer();
		try {
			while (endOffset>0) {
				ConsumerRecords<String, String> records = consumer.poll(poll_timeout_ms);
				for (ConsumerRecord<String,String> record : records ) {
					// If no value, then remove this key from the map. Otherwise, put the updated
					// Record into the map.
					if (record.value()==null) {
						mbrs.remove(record.key());
					} else {
						MBRecord mbr = new MBKRecord(record);
						mbrs.put(record.key(), mbr);
					}
					if (record.offset()==endOffset-1) {
						return mbrs;
					}
				}
			}
			return mbrs;
		} catch (Throwable e) {
			logger.error("Problem consuming from topic: " + topicName);
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
	}
	
	@Override
	public String getTopicName() {
		return topicName;
	}

	/**
	 * For snapshot requests, we close before returning the snapshot.
	 * For follows, we need to chase down any streams and kill them.
	 */
	@Override
	public void close() throws IOException {
		
	}

	/**
	 * <p>Follow a topic starting from the supplied offset until forever, or when the stream is closed.</p>
	 * <p>To feed a stream, we create a new thread to wait for records to arrive. For Kafka, it 
	 * is critical that consumption of the stream remain lively. The caller should get back to reading
	 * records from the stream as quickly as possible. Otherwise, no keep-alive signal is sent to the broker.
	 * </p>
	 */
	@Override
	public BlockingQueue<MBRecord> follow() {
		closed = new AtomicBoolean(false);
		queue = new LinkedBlockingQueue<MBRecord>(QUEUE_SIZE);
		executorService.execute(new Runnable() {
			@Override 
		    public void run() {
				try {
					consumer = openConsumer();
					while (!closed.get()) {
						ConsumerRecords<String, String> records = consumer.poll(poll_timeout_ms);
						for (ConsumerRecord<String,String> record : records ) {
							queue.put(new MBKRecord(record));
						}
					}
		        } catch (Exception e) {
		             // Ignore exception if closing
		             if (!closed.get()) {
		            	 throw new RuntimeException("Error from Kafka topic, existing this follow operation", e);
		             }
		        } finally {
                	consumer.close();
				}
		    }
		});
//		followStream = queue.stream();
		return queue;
	}
	
	@Override
	public void stop() {
		queue.offer(new MBKRecord());
    	closed.set(true);
    	consumer.wakeup();
	}

}
