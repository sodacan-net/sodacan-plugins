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

import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.sodacan.messagebus.MBRecord;

public class MBKRecord implements MBRecord {
	ConsumerRecord<String, String> realRecord;
	
	public MBKRecord( ConsumerRecord<String, String> realRecord ) {
		this.realRecord = realRecord;
	}
	public MBKRecord() {
		this.realRecord = null;
	}
	
	@Override
	public String getTopic() {
		return realRecord.topic();
	}

	@Override
	public long getTimestamp() {
		return realRecord.timestamp();
	}

	@Override
	public long getOffset() {
		return realRecord.offset();
	}

	@Override
	public String getKey() {
		return realRecord.key();
	}

	@Override
	public String getValue() {
		return realRecord.value();
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getValue());
		sb.append("  {");
		sb.append(getOffset());
		sb.append(',');
		sb.append(Instant.ofEpochMilli(getTimestamp()).toString());
		sb.append('}');
		return sb.toString();
	}
	
	@Override
	public boolean isEOF() {
		return (realRecord==null);
	}

}
