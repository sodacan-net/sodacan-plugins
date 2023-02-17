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
package net.sodacan.plugin.clock;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.BlockingQueue;

import com.google.auto.service.AutoService;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.mode.spi.ClockProvider;
import net.sodacan.mode.spi.Plugin;

@AutoService(ClockProvider.class)
public class StaticClock extends Plugin implements ClockProvider {
	
	public static final String PLUGIN_TYPE = "static";
	private Instant time;
	
	@Override
	public boolean isMatch(String pluginTypes) {
		if (PLUGIN_TYPE.equals(pluginTypes)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * When asked, provide the time on our (static) clock, always in Z time
	 */
	public Instant get() {
		return time;
	}

	/**
	 * A Manual clock allows the time to be set
	 */
	public void setClock(int year, int month, int day, int hour, int minute, int second) {
		long epochSeconds = LocalDateTime.of(year,month,day, hour, minute, second).toEpochSecond(ZoneOffset.of("Z"));
		time = Instant.ofEpochSecond(epochSeconds);
	}

	@Override
	public long getTimestamp() {
		return time.getEpochSecond();
	}
	
	/**
	  * Advance the clock by the specified duration such as 
	  * <code> advanceClock(Duration.ofSeconds(1); </code>
	 */
	@Override
	public void advanceClock( Duration duration) {
		time = time.plus(duration);
	}


	@Override
	public void close() {
		
	}

	@Override
	public BlockingQueue<MBRecord> follow() {
		return null;
	}

}
