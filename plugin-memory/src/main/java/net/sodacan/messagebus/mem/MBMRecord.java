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

import net.sodacan.messagebus.MBRecord;

/**
 * <p>A simple topic record, immutable.</p>
 * @author John Churin
 *
 */
public class MBMRecord implements MBRecord {
	private String topic;
	private long timestamp;
	private long offset;
	private String key;
	private String value;

	
	public MBMRecord(String topic, long timestamp, long offset, String key, String value) {
		super();
		this.topic = topic;
		this.timestamp = timestamp;
		this.offset = offset;
		this.key = key;
		this.value = value;
	}

	public String getTopic() {
		return topic;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public long getOffset() {
		return offset;
	}
	public String getKey() {
		return key;
	}
	public String getValue() {
		return value;
	}
	
	
}
