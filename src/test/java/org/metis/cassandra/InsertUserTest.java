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

import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import static org.metis.utils.Constants.*;

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InsertUserTest extends BaseTest {

	/**
	 * First insert the user jfernandez. Note how we're passing in a collection
	 * of emails
	 * 
	 * @throws Exception
	 */
	@Test
	public void testA() throws Exception {
		String JSON = "{\"username\":\"jfernandez\", \"created_date\":\"2014-06-6 13:50:00\", "
				+ "\"email\":[\"jfernandez@cox.net\",\"joef551@yahoo.com\"],"
				+ "\"firstname\":\"joe\",\"lastname\":\"fernandez\",\"password\":\"abc123\"}";
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"insert");
	}

	/**
	 * Now ensure user jfernandez was inserted
	 * 
	 * @throws Exception
	 */
	@Test
	public void testB() throws Exception {
		// this is what we send to the CqlEndpoint. Note how we're
		// specifying the request method via the payload.
		String JSON = "{\"user\":\"jfernandez\"}";
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// feed the route, which starts the test
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"select");
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
	}

	/**
	 * Delete user jfernandez
	 * 
	 * @throws Exception
	 */
	@Test
	public void testC() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"user\":\"jfernandez\"}";
		// feed the route, which starts the test
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"delete");
	}

	/**
	 * Ensure user jfernandez was deleted
	 * 
	 * @throws Exception
	 */
	@Test
	public void testD() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"user\":\"jfernandez\"}";
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new TestResult2());
		// feed the route, which starts the test
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"select");
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
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
	 * This predicate ensures that the payload returned for TestB is as
	 * expected.
	 */
	private class TestResult implements Predicate {

		public boolean matches(Exchange exchange) {

			
			Object payLoad = exchange.getIn().getBody();
			if (payLoad == null) {
				return false;
			} else if (!(payLoad instanceof List)) {
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
			if (map.size() != 2) {
				return false;
			}
			Object value = map.get("username");
			if (!(value instanceof String)) {
				return false;
			}
			
			if (!"jfernandez".equals(value)) {
				return false;
			}
			
			// retrieve the list of emails
			value = map.get("email");
			if (value == null) {
				return false;
			}
			
			if (!(value instanceof List)) {
				return false;
			}
			
			list = (List) value;
			if (list.size() != 2) {
				return false;
			}
			
			if (!"jfernandez@cox.net".equals(list.get(0).toString())) {
				return false;
			} else if (!"joef551@yahoo.com".equals(list.get(1).toString())) {
				return false;
			}

			return true;
		}
	}

	/**
	 * This predicate ensures that the payload returned for TestD is as
	 * expected.
	 */
	private class TestResult2 implements Predicate {
		public boolean matches(Exchange exchange) {
			Object payLoad = exchange.getIn().getBody();
			if (payLoad != null) {
				if (payLoad instanceof List) {
					if (((List) payLoad).isEmpty()) {
						return true;
					}
				}
				return false;
			}
			return true;
		}
	}

}
