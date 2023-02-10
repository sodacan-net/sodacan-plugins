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

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Supplier;
/**
 * <p>This clock remains in UTC time</p>
 * @author John Churin
 *
 */
public class ManualClock extends Clock {
	private Instant time;

	public ManualClock( int year, int month, int day, int hour, int minute, int second) {
		super();
		long epochSeconds = LocalDateTime.of(year,month,day, hour, minute, second).toEpochSecond(ZoneOffset.of("Z"));
		time = Instant.ofEpochSecond(epochSeconds);
	}
	/**
	 * An unitialized manual clock is not usable until given an initial time.
	 */
	public ManualClock() {
		time = null;
	}

	public synchronized void plusOneMinute() {
		time = time.plusSeconds(60);
	}

	public synchronized void setTime(int year, int month, int day, int hour, int minute, int second) {
		long epochSeconds = LocalDateTime.of(year,month,day, hour, minute, second).toEpochSecond(ZoneOffset.of("Z"));
		time = Instant.ofEpochSecond(epochSeconds);
	}
	
	@Override
	public ZoneId getZone() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Instant instant() {
		return time;
	}

	@Override
	public Clock withZone(ZoneId zone) {
		// TODO Auto-generated method stub
		return null;
	}


}
