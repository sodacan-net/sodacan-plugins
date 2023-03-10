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
package test.net.sodacan.plugin.memory;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import net.sodacan.config.Config;
import net.sodacan.config.ConfigMode;
import net.sodacan.mode.Mode;
import net.sodacan.mode.spi.PluginEvent;

public class TestPlugins {
	private Map<String,PluginEvent> changeEvents = new HashMap<>();

	public PluginEvent findPluginEventByModeAndProperty(String mode, String propertyName) {
		return changeEvents.get(mode+propertyName);
	}
	
	@Test
	public void testMemoryLogger() {
		ConfigMode configMode = ConfigMode.newConfiguModeBuilder()
				.name("basemode")
				.loggerType("console")
				.build();
		Config config = new Config();
		config.addConfigMode(configMode);
		Mode.configure(config);
		// Done with preliminaries
		// A side effect of construction is that the mode is remembered in a singleton map
		Mode mode = Mode.findMode("basemode");

		// Tell the logger service to do something
		for (int x = 0; x < 5; x++) {
			mode.log("Hello: "+ x);
		}
		
//		PluginEvent pe = findPluginEventByModeAndProperty("Mode1","msg");
//		assert("Hello: 4".equals(pe.getNewValue()));
		
		// Before leaving the thread, remove the Mode variable
//		Mode.clearModeInThread();
	}

	
//	@Test
//	public void testSaveState() {
//		// Create some variables
//		ModuleVariables mvs = new ModuleVariables();
//		VariableDef vd1 = VariableDef.newVariableDefBuilder().name("x").initialValue(new Value(123)).build();
//		ModuleVariable v1 = (ModuleVariable)mvs.addVariable(vd1);
//		v1.setChangedInCycle(true);
//		VariableDef vd2 = VariableDef.newVariableDefBuilder().name("y").alias("z").initialValue(new Value(456)).build();
//		ModuleVariable v2 = (ModuleVariable)mvs.addVariable(vd2);
//		v2.setChangedInCycle(true);
//		ConfigMode configMode = ConfigMode.newConfiguModeBuilder()
//				.name("basemode")
//				.tickSourceType("memory")
//				.build();
//		Config config = new Config();
//		config.addConfigMode(configMode);
//		Mode.configure(config);
//		// Done with preliminaries
//		// A side effect of construction is that the mode is remembered in a singleton map
//		Mode mode = Mode.findMode("basemode");
//		SodacanModule sm = new SodacanModule();
//		assertNotNull(sm);
//		sm.setName("DummyModule");
//		mode.saveState(sm, mvs);
//		
//		ModuleVariables mvr = mode.restoreAll(sm);
//		Value vr = mvr.find("x").getValue();
//		assert(vr.getNumber().intValue()==123);
//	}
//
}
