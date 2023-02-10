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

import net.sodacan.config.Config;
import net.sodacan.messagebus.MB;
import net.sodacan.messagebus.mem.MBM;
import net.sodacan.mode.spi.MessageBusProvider;

@AutoService(MessageBusProvider.class)
public class MemoryMessageBusProvider extends MemoryProvider implements MessageBusProvider {

	MB mb = null;
	@Override
	public MB getMB(Config config) {
		if (mb==null) {
			mb = MBM.createInstance();
		}
		return mb;
	}

}
