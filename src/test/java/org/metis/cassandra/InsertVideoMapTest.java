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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.datastax.driver.core.utils.UUIDs;

import static org.metis.utils.Constants.*;

// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InsertVideoMapTest extends BaseTest {

	private static String videoid = UUIDs.random().toString();
	private static Date now = Calendar.getInstance().getTime();
	private static Timestamp currentTimestamp = new java.sql.Timestamp(
			now.getTime());

	// insert a video record into the videos table
	@Test
	public void testA() throws Exception {
		Map<String, String> location = new HashMap<String, String>();
		location.put("US", "/us/vid/99/" + videoid);
		location.put("UK", "/uk/vid/99/" + videoid);
		Set<String> tags = new HashSet<String>();
		tags.add("is");
		tags.add("there");
		tags.add("anyone");
		tags.add("home?");
		Map<Object, Object> map = new HashMap<Object, Object>();
		map.put("videoid", videoid);
		map.put("description", "best of pink floyd");
		map.put("location", location);
		map.put("tags", tags);

		String s1[] = currentTimestamp.toString().split("\\.");
		map.put("upload_date", s1[0]);
		map.put("username", "jfernandez");
		map.put("videoname", "comfortably numb");
		template.requestBodyAndHeader("direct:start", map, CASSANDRA_METHOD,
				"insert");
	}

	/**
	 * Confirm insertion
	 * 
	 * @throws Exception
	 */
	@Test
	public void testB() throws Exception {
		String JSON = "{\"videoid\":\"" + videoid + "\"}";
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"select");
		assertMockEndpointsSatisfied();
	}

	/**
	 * Delete video
	 * 
	 * @throws Exception
	 */
	@Test
	public void testC() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"videoid\":\"" + videoid + "\"}";
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"delete");
	}

	/**
	 * Ensure video was deleted
	 * 
	 * @throws Exception
	 */
	@Test
	public void testD() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"videoid\":\"" + videoid + "\"}";
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
				from("direct:start").to("cql:video").to("mock:result");
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
			if (map.size() != 7) {
				return false;
			}
			Object value = map.get("username");
			if (value == null || !(value instanceof String)) {
				return false;
			}
			if (!"jfernandez".equals(value)) {
				return false;
			}

			value = map.get("description");
			if (value == null || !(value instanceof String)) {
				return false;
			}
			if (!"best of pink floyd".equals(value)) {
				return false;
			}

			value = map.get("tags");
			if (value == null || !(value instanceof Set)) {
				return false;
			}
			if (((Set) value).size() != 4) {
				return false;
			}

			value = map.get("location");
			if (value == null || !(value instanceof Map)) {
				return false;
			}
			if (((Map) value).size() != 2) {
				return false;
			}

			value = ((Map) value).get("UK");
			if (value == null || !(value instanceof String)) {
				return false;
			}

			if (!((String) value).equals("/uk/vid/99/" + videoid)) {
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
