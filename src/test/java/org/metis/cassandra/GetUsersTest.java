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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/**
 * This test will select users whose name is "tcodd". Cassandra should return 2
 * entries from the videodb that match the query. Note that a Cassandra request
 * method is not being supplied, which means that the route will end up using
 * the Client bean's default method, which in this case must be SELECT.
 * 
 * The test will by default look for and load "cassandra.xml"
 */
// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GetUsersTest extends BaseTest {

	// test with JSON as an input
	@Test
	public void testASendMessage() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"username\":\"tcodd\"}";
		// tell the mock end point that we expect to get back a List of Maps
		// having 2 Maps, each with only one entry whose value is a String.
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// feed the route, which starts the test
		template.requestBody("direct:start", JSON);
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
	}

	// test with single map
	@Test
	public void testBSendMessage() throws Exception {
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// send a Map
		Map<String, String> map = new HashMap<String, String>();
		map.put("username", "tcodd");
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		template.requestBody("direct:start", map);
		assertMockEndpointsSatisfied();

	}

	// test with list of maps, but only with a list size of 1
	@Test
	public void testCSendMessage() throws Exception {
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// send a Map
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> map = new HashMap<String, String>();
		map.put("username", "tcodd");
		list.add(map);
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		template.requestBody("direct:start", list);
		assertMockEndpointsSatisfied();

	}

	// test with stream
	@Test
	public void testDSendMessage() throws Exception {
		String JSON = "{\"username\":\"tcodd\"}";
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// send a Stream
		InputStream is = new ByteArrayInputStream(JSON.getBytes());
		template.requestBody("direct:start", is);
		assertMockEndpointsSatisfied();

		// stop the context
		context.stop();
	}

	@Override
	// this is the route used by this test case.
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				// the message is read in from the direct:start endpoint,
				// sent to Cassandra component, then the reply is sent
				// on to the mock endpoint. The mock endpoint will then validate
				// it via the TestResult predicate.
				from("direct:start").to("cql:user").to("mock:result");
			}
		};
	}

	/**
	 * This predicate ensures that the payload returned is as expected.
	 */
	protected class TestResult implements Predicate {

		public boolean matches(Exchange exchange) {
			Object payLoad = exchange.getIn().getBody();
			if (payLoad == null || !(payLoad instanceof List)) {
				return false;
			}

			List<Object> list = (List) payLoad;
			if (list.size() != 2) {
				return false;
			}
			payLoad = list.get(0);
			if (!(payLoad instanceof Map)) {
				return false;
			}
			Map map = (Map) payLoad;
			if (map.size() != 1) {
				return false;
			}
			Object value = map.get("username");
			if (!(value instanceof String)) {
				return false;
			}

			if (!"tcodd".equals(value)) {
				return false;
			}
			return true;
		}
	}
}
