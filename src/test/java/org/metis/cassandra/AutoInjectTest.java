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
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;

import static org.junit.Assert.*;


/**
 * Reads in a test Spring context file and runs some tests based on the file.
 * 
 */

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AutoInjectTest {

	private static CqlComponent cc = null;

	@BeforeClass
	public static void initialize() throws Exception {
	}

	/**
	 * Simple test that loads text Spring XML file
	 */
	@Test
	public void TestA() {
		try {
			cc = CqlComponent.cqlComponent("auto-inject.xml");
			cc.start();
			Thread.sleep(500);
			assertTrue(cc.isStarted());
			assertTrue(cc.getComponentProfile() != null);
			assertTrue(cc.getClients() != null);
			assertTrue(cc.getClients().isEmpty() == false);
			assertTrue(cc.getClients().size() == 1);
			assertTrue(cc.getClients().get("user") != null);
			assertTrue(cc.getClients().get("user").getCqlStmnts4Select().size() == 3);
			assertTrue(cc.getClients().get("user").getCqlStmnts4Insert().size() == 1);
			assertTrue(cc.getClients().get("user").getCluster().getClusterName().equals("myCluster"));
			assertTrue(cc.getClients().get("user").getClusterBean().getBeanName().equals("cluster1"));
			assertTrue(cc.getClients().get("user").getClusterBean().getListOfPoolingOptions().size() == 2);
			assertTrue(cc.getClients().get("user").getClusterBean().getProtocolOptions().getPort() == 9042);
			assertTrue(cc.getClients().get("user").getClusterBean().getProtocolOptions().getProtocolVersion().toInt() == 2);
			assertTrue(cc.getClients().get("user").getClusterBean().getProtocolOptions().getMaxSchemaAgreementWaitSeconds() == 2);
			assertTrue(cc.getClients().get("user").getClusterBean().getListOfPoolingOptions().get(0).getHostDistance() == LOCAL);
			assertTrue(cc.getClients().get("user").getClusterBean().getListOfPoolingOptions().get(0).getCoreConnectionsPerHost() == 5);
			assertTrue(cc.getClients().get("user").getClusterBean().getListOfPoolingOptions().get(1).getHostDistance() == REMOTE);
			assertTrue(cc.getClients().get("user").getClusterBean().getListOfPoolingOptions().get(1).getCoreConnectionsPerHost() == 2);
			Endpoint endpoint = cc.createEndpoint("cql://user", "user", null);
			assertTrue(endpoint != null);
			assertTrue(endpoint instanceof CqlEndpoint);
			CqlEndpoint ce = (CqlEndpoint) endpoint;
			assertTrue(ce.getClient() != null);
		} catch (Exception exc) {
			fail("caught this exception: " + exc.getLocalizedMessage());
		}

	}

	@Test
	public void TestB() {
		try {
			cc = CqlComponent.cqlComponent("auto-inject2.xml");
			assertTrue(cc != null);
			cc.start();
			fail("ERROR: did not get exception");
		} catch (Exception be) {			
		}
	}

}
