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
package net.sodacan.plugin.memory;

import com.google.auto.service.AutoService;

import net.sodacan.mode.spi.LoggerProvider;

/**
 * This simple logger plugin responds to mode names that begin with "test-"
 * @author John Churin
 *
 */
@AutoService(LoggerProvider.class)
final public class Logger extends MemoryProvider implements LoggerProvider {
    private int count = 0;


	@Override
	public void log(String msg) {
//		System.out.println("Seq: " + count++ + ", Mode: " + getMode() + ", Msg: " + msg);
//		this.firePropertyChange​("msg", null, msg);
		this.firePropertyChange​("msg", msg, msg);
	}


	@Override
	public String toString() {
		return "MemoryLoggerPlugin";
	}

}
