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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import net.sodacan.messagebus.MBRecord;
import net.sodacan.mode.spi.ClockProvider;
import net.sodacan.mode.spi.MBTick;
import net.sodacan.mode.spi.Plugin;

@AutoService(ClockProvider.class)
public class RealClock extends Plugin implements ClockProvider, Runnable {
	private final static Logger logger = LoggerFactory.getLogger(RealClock.class);

	public static final int TICK_SIZE = 60*1000; // One minute
	public static final int SLEEP_SIZE = TICK_SIZE/5; // Sleep between ticks
	public static final int QUEUE_SIZE = 20;
	public static final String PLUGIN_TYPE = "real";
	private static ExecutorService executorService = Executors.newCachedThreadPool();
	private BlockingQueue<MBRecord> queue;
	private Instant nextTick = null;	// null means clock is not running
	private Future<?> future;

	@Override
	public boolean isMatch(String pluginTypes) {
		if (PLUGIN_TYPE.equals(pluginTypes)) {
			return true;
		} else {
			return false;
		}
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
	/**
	 * Cannot advance a real clock
	 * Ignore this request
	 */
	@Override
	public void advanceClock( Duration duration) {
		
	}
	
	/**
	 * Following a clock, real-time in this case, means sending tick records to a queue on a regular basis, 
	 * once every n seconds, without missing any ticks. We use the MessageBus record (MBRecord) for this purpose.
	 */
	@Override
	public BlockingQueue<MBRecord> follow() {
		queue = new LinkedBlockingQueue<MBRecord>(QUEUE_SIZE);
		
		// Setup our main loop
		future = executorService.submit(this);
		// Our caller will get evenly spaced ticks delivered through this queue.
		return queue;
	}	
	
	@Override
	public void close() {
		stop();
	}
	
	public void stop() {
		if (future!=null) {
			future.cancel(true);
			future = null;
		}
	}

	@Override
	public void run() {
		try {
			// Start up the clock
			nextTick = Instant.now();
			// Keep going till we're killed
			while (!Thread.currentThread().isInterrupted()) {
				// if the next tick is not greater than now, time for another tick
				if (nextTick.isBefore(Instant.now())) {
					// Send out a tick record
					MBTick mbt =new MBTick(nextTick.toEpochMilli());
					queue.put(mbt);
					// Advance the clock
					nextTick = nextTick.plusMillis(TICK_SIZE);
				} else {
					Thread.sleep(SLEEP_SIZE);
				}
			}
		} catch (InterruptedException e) {
			// Just leave quietly
			logger.debug("Leaving real clock");
		} catch (Throwable e) {
			logger.error("Problem in real clock");
			logger.error(e.getMessage());
			Throwable t = e.getCause();
			while (t!=null) {
				logger.error("  " + t.getMessage());
				t = t.getCause();
			}

		}
	}

}
