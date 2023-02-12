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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sodacan.messagebus.MB;
import net.sodacan.messagebus.MBTopic;

public class MBM implements MB {
	private final static Logger logger = LoggerFactory.getLogger(MBM.class);
	
	private Map<String, BlockingQueue<MBMRecord>> topics = new ConcurrentHashMap<>();

	@SuppressWarnings("unused")
	private Map<String,String> configProperties;


	public static MBM createInstance(Map<String,String> configProperties) {
		MBM instance;
		instance = new MBM(configProperties);
		return instance;
	}

	private MBM(Map<String,String> configProperties) {
		this.configProperties = configProperties;
		logger.debug("Memory Based Message Bus Created");
	}

	@Override
	public Set<String> listTopics() {
		return Set.copyOf(topics.keySet());
	}

	@Override
	public String describeTopic(String topicName) {
		return "Topic " + topics.get(topicName);
	}

	@Override
	public boolean createTopic(String topicName, boolean compacted) {
		if (topics.containsKey(topicName)) {
			logger.debug("Create topic " + topicName + " failed");
			return false;
		}
		topics.put(topicName,  new LinkedBlockingQueue<MBMRecord>());
		return true;
	}

	@Override
	public void deleteTopic(String topicName) {
		if (topics.containsKey(topicName)) {
			logger.debug("Topic " + topicName + " delete failed");
			return;
		}
		topics.remove(topicName);
	}

	@Override
	public MBTopic openTopic(String topicName, long offset) {
		if (topics.containsKey(topicName)) {
			logger.debug("Topic " + topicName + " does not exist");
			return null;
		}
		return new MBMTopic(topicName,offset,topics.get(topicName));
	}

	@Override
	public void produce(String topicName, String key, String value) {
		BlockingQueue<MBMRecord> queue = topics.get(topicName);
		if (queue==null) {
			throw new RuntimeException("Unknown Topic Name: " + topicName);
		}
		long timestamp = 0;	//**********************************************
		MBMRecord record = new MBMRecord( topicName, timestamp, queue.size(), key, value);
		queue.offer(record);
	}

}
