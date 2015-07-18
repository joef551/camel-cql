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

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PagingTest extends BaseTest {

	// test with JSON as an input
	@Test
	public void testASendMessage() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"tag\":\"cassandra\"}";
		// tell the mock end point that we expect to get back a List of Maps
		// having 101 Maps.
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new ATestResult());
		// feed the route, which starts the test
		template.requestBody("direct:start", JSON);
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
	}

	// test with single map - should return the fetchSize specified in the
	// cassandra.xml file
	@Test
	public void testBSendMessage() throws Exception {
		Map<String, String> map = null;
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new BTestResult());
		template.requestBody("direct:start", map);
		assertMockEndpointsSatisfied();
	}

	// @Override
	// this is the route used by this test case.
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				// the message is read in from the direct:start endpoint,
				// sent to Cassandra component, then the reply is sent
				// on to the mock endpoint. The mock endpoint will then validate
				// it via the TestResult predicate.
				from("direct:start").to("cql:tag").to("mock:result");
			}
		};
	}

	/**
	 * This predicate ensures that the payload returned is as expected. It
	 * expects one Map with a key:value pair of count:101
	 */
	protected class ATestResult implements Predicate {

		public boolean matches(Exchange exchange) {

			Object payLoad = exchange.getIn().getBody();
			if (payLoad == null || !(payLoad instanceof List)) {
				return false;
			}
			List<Object> list = (List) payLoad;
			if (list.size() != 1) {
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

			Object value = map.get("count");
			if (!(value instanceof Long)) {
				return false;
			}
			if (((Long) value) != 3) {
				return false;
			}
			return true;
		}
	}

	protected class BTestResult implements Predicate {

		public boolean matches(Exchange exchange) {

			Object payLoad = exchange.getIn().getBody();
			if (payLoad == null || !(payLoad instanceof List)) {
				return false;
			}

			List<Object> list = (List) payLoad;
			if (list.size() != 5) {
				return false;
			}

			payLoad = list.get(0);
			if (!(payLoad instanceof Map)) {
				return false;
			}

			Map map = (Map) payLoad;
			if (map.size() != 3) {
				return false;
			}

			Object value = map.get("tag");
			if (!(value instanceof String)) {
				return false;
			}

			return true;
		}
	}
}
