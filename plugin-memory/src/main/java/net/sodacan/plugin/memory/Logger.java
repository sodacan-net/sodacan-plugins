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

import java.beans.PropertyChangeListener;

import com.google.auto.service.AutoService;

import net.sodacan.mode.spi.LoggerProvider;
import net.sodacan.mode.spi.Plugin;

/**
 * This simple logger plugin just sends what it gets to the console
 * @author John Churin
 *
 */
@AutoService(LoggerProvider.class)
final public class Logger extends Plugin implements LoggerProvider {
	public static final String PLUGIN_TYPE = "console";
    private int count = 0;

    public boolean isMatch(String pluginType) {
		if (PLUGIN_TYPE.equals(pluginType)) {
			return true;
		} else {
			return false;
		}
	}


	@Override
	public void log(String msg) {
		System.out.println("Seq: " + count++ + ", Mode: " + getMode() + ", Msg: " + msg);
	}



}
