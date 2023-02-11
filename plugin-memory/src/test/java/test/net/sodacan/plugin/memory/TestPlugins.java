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

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import net.sodacan.config.ConfigMode;
import net.sodacan.mode.BaseMode;
import net.sodacan.mode.Mode;
import net.sodacan.mode.spi.PluginEvent;
import net.sodacan.module.statement.SodacanModule;
import net.sodacan.module.value.Value;
import net.sodacan.module.variable.ModuleVariable;
import net.sodacan.module.variable.VariableDef;
import net.sodacan.module.variables.ModuleVariables;

public class TestPlugins {
	private Map<String,PluginEvent> changeEvents = new HashMap<>();

	public PluginEvent findPluginEventByModeAndProperty(String mode, String propertyName) {
		return changeEvents.get(mode+propertyName);
	}
	
	@Test
	public void testMemoryLogger() {
		// Do this only one time per basemode. This example is small. Usually, messageBus, 
		// clock, and stateStore also configured.
		ConfigMode configMode = ConfigMode.newConfiguModeBuilder()
				.name("basemode")
				.loggerType("memory")
				.build();
		// The following is normally done by Mode.config
		BaseMode baseMode = new BaseMode(configMode);
		// Done with preliminaries
		// Now create an actual mode
		Mode mode = new Mode("mode1", "basemode");
		// A side effect of construction is that the mode is remembered in a singleton map
		
		// Setting the current mode would normally be called when a thread is recently 
		// started or restarted. For example, 
		// in a filter before processing a REST api call.
		Mode.setModeInThread("Mode1");

		// Tell the logger service to do something
		for (int x = 0; x < 5; x++) {
			mode.log("Hello: "+ x);
		}
		
		PluginEvent pe = findPluginEventByModeAndProperty("Mode1","msg");
		assert("Hello: 4".equals(pe.getNewValue()));
		
		// Before leaving the thread, remove the Mode variable
		Mode.clearModeInThread();
	}

	
	@Test
	public void testSaveState() {
		// Create some variables
		ModuleVariables mvs = new ModuleVariables();
		VariableDef vd1 = VariableDef.newVariableDefBuilder().name("x").initialValue(new Value(123)).build();
		ModuleVariable v1 = (ModuleVariable)mvs.addVariable(vd1);
		v1.setChangedInCycle(true);
		VariableDef vd2 = VariableDef.newVariableDefBuilder().name("y").alias("z").initialValue(new Value(456)).build();
		ModuleVariable v2 = (ModuleVariable)mvs.addVariable(vd2);
		v2.setChangedInCycle(true);
		ConfigMode configMode = ConfigMode.newConfiguModeBuilder()
				.name("basemode")
				.stateStoreType("memory")
				.build();
		// The following is normally done by Mode.config
		BaseMode baseMode = new BaseMode(configMode);
		// Done with preliminaries
		// Now create an actual mode
		Mode mode = new Mode("mode1", "basemode");
		// A side effect of construction is that the mode is remembered in a singleton map

		SodacanModule sm = new SodacanModule();
		assertNotNull(sm);
		sm.setName("DummyModule");
		mode.saveState(sm, mvs);
		
		ModuleVariables mvr = mode.restoreAll(sm);
		Value vr = mvr.find("x").getValue();
		assert(vr.getNumber().intValue()==123);
	}

}
