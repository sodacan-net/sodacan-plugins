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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sodacan.SodacanException;
import net.sodacan.messagebus.MB;
import net.sodacan.messagebus.MBTopic;

/**
 * An implementation of MessageBus in Kafka
 * 
 * @author John Churin
 *
 */
public class MBK implements MB {
	private final static Logger logger = LoggerFactory.getLogger(MBK.class);
	private static final int PARTITIONS = 1;
	private static final short REPLICAS = 1;
	private static final int WAIT_SECONDS = 5;
	private static MBK instance;
	private AdminClient adminClient;
	private KafkaProducer<String,String> producer = null;

	private Map<String,String> configProperties;
	public static MB createInstance(Map<String,String> configProperties) {
		if (instance != null) {
			return instance;
//			throw new RuntimeException("MBK already initialized");
		}
		instance = new MBK(configProperties);
		return instance;
	}

	public MB getInstance() {
		if (instance == null) {
			throw new RuntimeException("MBK not initialized, call MBK.createInstance(), one time to do so");
		}
		return instance;
	}

	private MBK(Map<String,String> configProperties) {
		this.configProperties = configProperties;
		Properties props = new Properties();
		String brokers = configProperties.get("brokers");
		if (brokers==null || brokers.isEmpty()) {
			throw new RuntimeException("brokers connection not specified in " + configProperties.get("pluginType"));
		}
		props.put("bootstrap.servers", brokers);
		logger.debug("Connect Admin Client to broker(s) " + brokers);
		adminClient = AdminClient.create(props);
	}

	public Set<String> listTopics() {
		try {
			// List the topics available
			Set<String> topicNames = new HashSet<String>();
			ListTopicsResult ltr = adminClient.listTopics();
			for (String name : ltr.names().get()) {
				topicNames.add(name);
			}
			return topicNames;
		} catch (Exception e) {
			throw new RuntimeException("Unable to list topics", e);
		}
	}

	public String describeTopic(String topicName) {
		try {
			// Describe each of those topics
			DescribeTopicsResult dtr = adminClient.describeTopics(Arrays.asList(topicName));
			KafkaFuture<Map<String, TopicDescription>> rslt = dtr.allTopicNames();
			StringBuffer sb = new StringBuffer();
				for (Entry<String, TopicDescription> entry : rslt.get(5, TimeUnit.SECONDS).entrySet()) {
					sb.append(entry.getKey());
					sb.append('=');
					sb.append(entry.getValue());
					sb.append('\n');
				}
	
			// Show configuration settings for each topic
			List<ConfigResource> cr = new LinkedList<ConfigResource>();
			cr.add(new ConfigResource(ConfigResource.Type.TOPIC, topicName));
			DescribeConfigsResult dfigr = adminClient.describeConfigs(cr);
			for (Entry<ConfigResource, Config> entry : dfigr.all().get().entrySet()) {
				for (ConfigEntry ce : entry.getValue().entries()) {
					if (!ce.isDefault()) {
						sb.append("   ");
						sb.append(ce.name());
						sb.append(": ");
						sb.append(ce.value());
						sb.append('\n');
					}
				}
				for (ConfigEntry ce : entry.getValue().entries()) {
					if (ce.isDefault()) {
						sb.append(" D ");
						sb.append(ce.name());
						sb.append(": ");
						sb.append(ce.value());
						sb.append('\n');
					}
				}
			}
			return sb.toString();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException("Error describing topic " + topicName, e);
		}
	}

	/**
	 * <p>Create one or more topics. This method will request that the topics be
	 * created and will wait for the completion up to WAIT_SECONDS at with point it
	 * will throw an exception if unsuccessful. The number of partitions (1) and
	 * number of replicas (3) are FIXED for the moment. In the case of events, the
	 * number of partitions should always be 1 (per suffix) since our rule engine
	 * must be able to reason over all states and events (for a given suffix).</p>
	 * <p>A compacted topic means that only the most recent version of each key is needed.
	 * An uncompacted topic will retain records forever.</p>
	 * 
	 * @param topics list of topic names to be created
	 * @param compacted If true, a compacted topic is created (old records deleted)
	 */
	public boolean createTopics(List<String> topics, boolean compacted) {
		Map<String,String> configs = new HashMap<>();
		if (compacted) {
			configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
		} else {
			configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
			configs.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
			configs.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
		}
		configs.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime");
		List<NewTopic> newTopics = new ArrayList<NewTopic>();
		for (String topic : topics) {
			newTopics.add(new NewTopic(topic, PARTITIONS, REPLICAS).configs(configs));
		}
		CreateTopicsResult ctr = adminClient.createTopics(newTopics);
		try {
			KafkaFuture<Void> f = ctr.all();
			f.get(WAIT_SECONDS, TimeUnit.SECONDS);
			if (f.isDone()) {
				return !f.isCompletedExceptionally();
			}
			throw new RuntimeException("Create topic(s) timed out");
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.debug("Create topic(s) " + topics + " failed");
			return false;
		}
	}

	/**
	 * Create state, event, and control topics with the specified suffix. This
	 * method will not return until the topics have been created.
	 * 
	 * @param suffix
	 */
	public boolean createTopic(String topic, boolean compacted) {
		List<String> topics = new ArrayList<String>();
		topics.add(topic);
		return createTopics(topics, compacted);
	}

	public boolean deleteTopics(List<String> topics) {
		DeleteTopicsResult dtr = adminClient.deleteTopics(topics);
		try {
			KafkaFuture<Void> f = dtr.all();
			f.get(WAIT_SECONDS, TimeUnit.SECONDS);
			if (f.isDone()) {
				return true;
			}
			throw new RuntimeException("Delete topic(s) timed out");
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException("Delete topic(s) " + topics + " failed", e );
		}
	}

	public void deleteTopic(String topic) {
		List<String> topics = new ArrayList<String>();
		topics.add(topic);
		deleteTopics(topics);
	}
	
	/**
	 * Construct a topic ready for a snapshot or follow operation. The topic must exist.
	 * A topic is a one-time use object. 
	 */
	@Override
	public MBTopic openTopic(String topicName, long nextOffset) {
		// Validate the topic
		Set<String> topic = listTopics();
		if (!topic.contains(topicName)) {
			throw new SodacanException("Unknown topic name: " + topicName);
		}
		return new MBKTopic( configProperties, topicName, nextOffset );
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

	@Override
	public void produce(String topicName, String key, String value) {
		if (producer==null) {
			setupProducer();
		}
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		producer.send(record);
	}
}
