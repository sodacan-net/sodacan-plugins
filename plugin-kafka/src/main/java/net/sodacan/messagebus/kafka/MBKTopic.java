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

import java.beans.PropertyChangeListener;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.messagebus.MBTopic;
/**
 * A wrapper around KafkaConsumer.
 * This is not thread safe. It is expected that a topic will be opened whenever it is needed.
 * We close the topic after completion of one of the snapshot/follow methods.
 * @author John Churin
 *
 */
public class MBKTopic implements MBTopic, Closeable {

	// The nextOffset is where we desire to start consuming
	private long nextOffset;
	// endOffset is a bit misleading since these queues are "forever". It means the end offset as of the
	// topic being opened. This is needed for a snapshot function which returns records up to this point
	// even if, while fetching, additional records have been added.
	private long endOffset;
	
	private String topicName;
	private String brokers;

	/**
	 * Construct a topic for consuming records
	 * @param brokers
	 * @param topicName
	 * @param nextOffset
	 */
	protected MBKTopic(String brokers, String topicName, long nextOffset) {
		this.nextOffset = nextOffset;
		this.topicName = topicName;
		this.brokers = brokers;
	}
	protected Consumer<String, String> openConsumer() {
		Consumer<String, String> consumer = null;
		Properties properties = new Properties();
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
	 * key is in the list.</p>
	 * <p>This a snapshot usually begins at offset zero and includes everything up 
	 * the end offset known when the topic was opened. This also accounts for deleted keys
	 * (tombstones in Kafka).</p>
	 */
	public Map<String, MBRecord> snapshot() {
		 Map<String, MBRecord> mbrs = new HashMap<>();
		Consumer<String,String> consumer = openConsumer();
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
				for (ConsumerRecord<String,String> record : records ) {
					// If no value, then remove this key from the map. Otherwise, put the updated
					// Record into the map.
					if (record.value()==null) {
						mbrs.remove(record.key());
					} else {
						MBRecord mbr = new MBKRecord(record);
						mbrs.put(record.key(), mbr);
					}
					if (record.offset()==endOffset) {
						return mbrs;
					}
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("Problem consuming from topic: " + topicName, e);
		} finally {
			consumer.close();
		}
	}
	
	public void follow( PropertyChangeListener listener) {
		try {
		} catch (Throwable e) {
			if (!(e instanceof InterruptedException || e instanceof InterruptException)) {
				throw new RuntimeException("Problem consuming from topic: " + topicName, e);
			}
			// In
		}
		
	}
//	public void consume() {
//	    while (true) {
//	         ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
//	         if (!follow && records.count()==0) {
//	        	 sendAll();
//	        	 break;
//	         }
//	         for (ConsumerRecord<K, V> record : records) {
//	        	 TopicPartition tp = new TopicPartition(record.topic(),record.partition());
//	        	 logger.debug("Record from topic: " + topicName + " Offset: " + record.offset() + " key: " + record.key());
////	        	 Instant timestamp = Instant.ofEpochMilli(record.timestamp());
////	        	 System.out.println(timestamp);
//	        	 // Are we done with preload phase
//	        	 if (record.offset() >=(endOffsets.get(tp)-1)) {
//		        	 processRecord( record.key(), record.value());
//	        		 sendAll();
//	        		 if (follow==false) return;
//	        	 }
//	        	 processRecord( record.key(), record.value());
//	         }
//	    }
//
//	}
	@Override
	public String getTopicName() {
		return topicName;
	}

	@Override
	public MBRecord poll(Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void close() throws IOException {
		
	}

}
