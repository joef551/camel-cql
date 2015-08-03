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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import static org.metis.utils.Constants.*;

//Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TupleTest extends BaseTest {

	/**
	 * First insert the customer. Note how we're passing in a collection of
	 * emails and a tuple for location
	 * 
	 * @throws Exception
	 */
	@Test
	public void testA() throws Exception {		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("username", "jfernandez");
		map.put("created_date", "2014-06-6 13:50:00");
		map.put("firstname", "joe");
		map.put("lastname", "fernandez");
		map.put("password", "abc123");
		List<String> emails = new ArrayList<String>();
		emails.add("jfernandez@cox.net");
		emails.add("joef551@yahoo.com");
		map.put("email", emails);
		List<Object> tuples = new ArrayList<Object>();
		tuples.add(new Float(123.456));
		tuples.add(new Float(456.123));
		map.put("location", tuples);
		template.requestBodyAndHeader("direct:start", map, CASSANDRA_METHOD,
				"insert");
	}

	/**
	 * Now ensure user customer was inserted
	 * 
	 * @throws Exception
	 */
	@Test
	public void testB() throws Exception {
		// this is what we send to the CqlEndpoint. Note how we're
		// specifying the request method via the payload.
		String JSON = "{\"username\":\"jfernandez\"}";
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
	 * Delete the customer
	 * 
	 * @throws Exception
	 */
	@Test
	public void testC() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"username\":\"jfernandez\"}";
		// feed the route, which starts the test
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"delete");
	}

	/**
	 * Ensure customer was deleted
	 * 
	 * @throws Exception
	 */
	@Test
	public void testD() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"username\":\"jfernandez\"}";
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new TestResult2());
		// feed the route, which starts the test
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"select");
		// ask the mock endpoint if it received the expected body and
		// value.
		assertMockEndpointsSatisfied();
	}

	/**
	 * This predicate ensures that the payload returned for TestB is as
	 * expected.
	 */
	private class TestResult implements Predicate {

		public boolean matches(Exchange exchange) {

			System.out.println("**** DEBUG 1 ****");
			Object payLoad = exchange.getIn().getBody();
			if (payLoad == null) {
				return false;
			} else if (!(payLoad instanceof List)) {
				return false;
			}

			List<Object> list = (List) payLoad;
			System.out.println("**** DEBUG 2 **** = " + list.size());
			if (list.size() != 1) {
				return false;
			}
			System.out.println("**** DEBUG 2.1 ****");
			payLoad = list.get(0);
			if (!(payLoad instanceof Map)) {
				System.out.println("**** DEBUG 2.2 ****");
				return false;
			}
			System.out.println("**** DEBUG 3 ****");
			Map map = (Map) payLoad;
			if (map.size() != 7) {
				return false;
			}
			System.out.println("**** DEBUG 4 ****");
			Object value = map.get("username");
			if (!(value instanceof String)) {
				return false;
			}
			if (!"jfernandez".equals(value)) {
				return false;
			}
			System.out.println("**** DEBUG 5 ****");
			// retrieve the list of emails
			value = map.get("email");
			if (value == null) {
				return false;
			}
			System.out.println("**** DEBUG 6 ****");
			if (!(value instanceof List)) {
				return false;
			}
			System.out.println("**** DEBUG 7 ****");
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

	@Override
	// this is the route used by this test case.
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				// the message is read in from the direct:start endpoint,
				// sent to Cassandra component, then the reply is sent
				// on to the mock endpoint. The mock endpoint will then validate
				// it via the TestResult predicate.
				from("direct:start").to("cql:customer").to("mock:result");
			}
		};
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
