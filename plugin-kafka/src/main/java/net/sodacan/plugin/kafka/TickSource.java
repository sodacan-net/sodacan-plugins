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
package net.sodacan.plugin.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import net.sodacan.mode.spi.TickSourceProvider;

/**
 * We provide a source of ticks in Kafka. Only one of these runs per mode. Kafka handles leader-election
 * so any number of these can be run. All but one will just idle on the poll loop.
 * This tickSource is not self-starting. That is, each tick read from the
 * topic determines when the next tick is generated (next = last + incrememt.ms; from config.yaml).
 * But until a tick is read, no new tick can be generated. The CLI will provide this starting
 * time (default is now but it can be another time as well).
 * 
 */
@AutoService(TickSourceProvider.class)
public class TickSource extends KafkaProvider implements TickSourceProvider, ConsumerRebalanceListener {
	
	private final static Logger logger = LoggerFactory.getLogger(TickSource.class);

	
	public static final String TICK_TOPIC = "real-clock";
	public static final String TICK_KEY = "tick";
	
	private Map<String,String> configProperties;
	private static ExecutorService executorService = Executors.newCachedThreadPool();
	protected Consumer<String, String> consumer;
	private KafkaProducer<String,String> producer = null;
	protected String groupId;
	protected Duration poll_timeout_ms = null;
	protected int increment_ms;
	protected Instant nextTime;
	
	public TickSource( ) {
	}
	
	@Override
	public String toString() {
		return "KafkaTickSourcePlugin";
	}

	@Override
	public void initConfig(Map<String, String> configProperties) {
		this.configProperties = configProperties;
		groupId = getMode()+ "-tick-source";
	}

	protected void setupProducer() {
		String brokers = configProperties.get("brokers");
		String lingerMsString = configProperties.get("linger.ms");
		long lingerMs;
		if (lingerMsString==null || lingerMsString.isEmpty()) {
			lingerMs = 0l;
		} else {
			lingerMs = Long.parseLong(configProperties.get("linger.ms"));
		}
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("linger.ms", lingerMs);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	/**
	 * Send a tick to the topic
	 */
	@Override
	public void sendTick(Instant tick) {
		if (producer==null) {
			setupProducer();
		}
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TICK_TOPIC, TICK_KEY, tick.toString());
		producer.send(record);
	}

	@Override
	public void close() {
		
	}

	protected void openConsumer() {
		String pollTimeout = configProperties.get("poll.timeout.ms");
		if (pollTimeout!=null) {
			poll_timeout_ms = Duration.ofMillis(Integer.parseInt(pollTimeout));
		} else {
			poll_timeout_ms = Duration.ofSeconds(25);
		}
		String incrementMs = configProperties.get("increment.ms");
		if (incrementMs!=null) {
			increment_ms = Integer.parseInt(incrementMs);
		} else {
			increment_ms = 60000;
		}
		Properties properties = new Properties();
		String brokers = configProperties.get("brokers");
		if (brokers==null || brokers.isEmpty()) {
			throw new RuntimeException("brokers connection not specified in TickSource config");
		}
		properties.setProperty("bootstrap.servers", brokers);
		properties.setProperty("group.id", groupId);
		properties.setProperty("enable.auto.commit", "false");
		properties.setProperty("max.partition.fetch.bytes", "100000");
		properties.setProperty("fetch.max.wait.ms", "80");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(TICK_TOPIC), this);
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		TopicPartition tp = new TopicPartition(TICK_TOPIC, 0);
		if (partitions.contains(tp)) {
	    	logger.debug("TickSource: Going inactive" + partitions);
			nextTime = null;	// This keeps the main loop quiet while standing by
		}
	}

	/**
	 * When we get control, we start by reading the last record in topic.
	 * This seeds the nextTick value.
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		TopicPartition tp = new TopicPartition(TICK_TOPIC, 0);
		Map<TopicPartition,Long> endOffsets;
		if (partitions.contains(tp)) {
			nextTime = null;	// This initializes the main loop
	    	endOffsets = consumer.endOffsets(partitions);
	    	Long endOffset = endOffsets.get(tp);
	    	if (endOffset > 0L ) {
	    		endOffset--;
	    		consumer.seek(tp, endOffset);
	    	}
	    	logger.debug("TickSource: Going active at offset: " + endOffset);
		}
	}

	
	@Override
	public Future<?> start() {
		System.out.println("In TickSource::start, config: " + configProperties);
		executorService.execute(new Runnable() {
			@Override 
		    public void run() {
				try {
					openConsumer();
					Instant lastTime = null;
					// Kafka doesn't like being interrupted, we use wakeup, so this .isInterrupted is here 
					// just in case.
					while (!Thread.currentThread().isInterrupted()) {
						ConsumerRecords<String, String> records = consumer.poll(poll_timeout_ms);
						for (ConsumerRecord<String,String> record : records ) {
							// The value is just a string representation of instant, parse back to instant
							lastTime = Instant.parse(record.value());
//							// First time through after assignment
//							if (nextTime==null) {
//								nextTime = lastTime;
//							} else {
//								// Compute the next tick
//								nextTime = lastTime.plusMillis(increment_ms);
//							}
							// Compute the next tick
							nextTime = lastTime.plusMillis(increment_ms);
							System.out.println("Tick consumed: " + record.value());
						}
						if (nextTime!=null) {
							// If now is at or beyond nextTime (next tick), send a tick
							Instant now = Instant.now();
							if (now.isAfter(nextTime)) {
								// Send a new tick
								sendTick(nextTime);
								System.out.println("New Tick sent: " + nextTime);
							}
						}
					}
		        } catch (WakeupException e) {
		        	logger.debug("Exiting TickSource");
		        } catch (Throwable e) {
		            	 logger.error("Error from Kafka topic, existing this follow operation", e);
		        } 
				finally {
                	consumer.close();
				}
		    }
		});
		
		// This thread just waits for a Future::cancel() to interrupt it and then issues a
		// KafkaConsumer::wakeup() to the consumer thread and exits.
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

}
