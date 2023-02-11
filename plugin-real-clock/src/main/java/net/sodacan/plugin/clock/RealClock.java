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

import java.time.Instant;
import java.util.function.Supplier;

import com.google.auto.service.AutoService;

import net.sodacan.mode.spi.ClockProvider;
import net.sodacan.mode.spi.Plugin;

@AutoService(ClockProvider.class)
public class RealClock extends Plugin implements ClockProvider, Supplier<Instant> {
	public static final String PLUGIN_TYPE = "real";
	
	@Override
	public boolean isMatch(String pluginTypes) {
		if (PLUGIN_TYPE.equals(pluginTypes)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * We act as supplier of instant's from the clock
	 */
	public Supplier<Instant> getSupplier() {
		return this;
	}

	/**
	 * When asked, provide the time on the real clock
	 */
	public Instant get() {
		return Instant.now();
	}

	/**
	 * A Manual clock allows the time to be set BUT WE DON'T.
	 * Ignore this request
	 */
	public void setClock(int year, int month, int day, int hour, int minute, int second) {
	}
	
	@Override
	public long getTimestamp() {
		return Instant.now().toEpochMilli();
	}
	@Override
	public void close() {
		
	}

}
