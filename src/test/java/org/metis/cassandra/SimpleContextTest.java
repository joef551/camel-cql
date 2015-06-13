/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.metis.cassandra;

import org.apache.camel.Endpoint;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.*;
import org.springframework.beans.BeansException;

/**
 * Reads in a test Spring context file and runs some tests based on the file.
 * 
 */

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleContextTest {

	private static CqlComponent cc = null;

	@BeforeClass
	public static void initialize() throws Exception {
		try {
			cc = CqlComponent.cqlComponent("cassandra.xml");
		} catch (BeansException be) {
			System.out.println("ERROR: unable to load spring context, got "
					+ "this exception: \n" + be.getLocalizedMessage());
			be.printStackTrace();
		}
	}

	/**
	 * Simple test that loads text Spring XML file
	 */
	@Test
	public void TestA() {

		if (cc == null) {
			fail("ERROR: unable to load context");
		}
		
		try {
			cc.start();
			Thread.sleep(500);
			assertTrue(cc.isStarted());
			assertTrue(cc.getComponentProfile() != null);
			assertTrue(cc.getClients() != null);
			assertTrue(cc.getClients().isEmpty() == false);
			Endpoint endpoint = cc.createEndpoint("cql://user", "user", null);
			assertTrue(endpoint != null);
			assertTrue(endpoint instanceof CqlEndpoint);
			CqlEndpoint ce = (CqlEndpoint) endpoint;
			assertTrue(ce.getClient() != null);
		} catch (Exception exc) {
			fail("caught this exception: " + exc.getLocalizedMessage());
		}

	}

}
