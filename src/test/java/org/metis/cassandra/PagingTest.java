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
import java.util.UUID;

import static org.junit.Assert.fail;
import static org.metis.utils.Constants.CASSANDRA_PAGING_STATE;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Predicate;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PagingTest extends BaseTest {

	private static String globalPagingState;
	private static Map globalMap;

	// test with JSON as an input
	@Test
	public void testASendMessage() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"tag\":\"cassandra\"}";
		// tell the mock end point that we expect to get back one Map whose only
		// key:value pair is count:3.
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new ATestResult());
		// feed the route, which starts the test
		template.requestBody("direct:start", JSON);
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
	}

	// should return a list of maps where the number of maps matches the
	// fetchSize specified in the cassandra.xml file
	@Test
	public void testBSendMessage() throws Exception {
		Map<String, String> map = null;
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new BTestResult());
		Object result = template.requestBody("direct:start", map);
		assertMockEndpointsSatisfied();
	}

	@Test
	public void testCSendMessage() throws Exception {
		if (globalPagingState == null) {
			fail("paging state was not set");
		}
		Map<String, String> map = null;
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new BTestResult());
		Object result = template.requestBodyAndHeader("direct:start", map,
				CASSANDRA_PAGING_STATE, globalPagingState);
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
	private class ATestResult implements Predicate {

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

	private class BTestResult implements Predicate {

		public boolean matches(Exchange exchange) {

			Message message = exchange.getIn();

			if (message == null) {
				return false;
			}

			// get the current paging state, which must always be returned for
			// this test
			Object pagingState = message.getHeader(CASSANDRA_PAGING_STATE);

			if (pagingState == null || !(pagingState instanceof String)) {
				return false;
			} else if (((String) pagingState).isEmpty()) {
				return false;
			}

			// tuck the paging state away in the global paging state
			globalPagingState = (String) pagingState;

			Object payLoad = message.getBody();
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

			// the tag index table has three columns
			Map map = (Map) payLoad;
			if (map.size() != 3) {
				return false;
			}

			Object value = map.get("videoid");
			if (!(value instanceof UUID)) {
				return false;
			}

			// if this is not the first paging state, then do a minor comparison
			// with the previous contents
			if (globalMap != null) {
				Object uuid = globalMap.get("videoid");
				if (((UUID) uuid).compareTo((UUID) value) == 0) {
					return false;
				}
			} else {
				globalMap = map;
			}
			return true;
		}
	}
}
