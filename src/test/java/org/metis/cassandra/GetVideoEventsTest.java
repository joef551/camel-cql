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
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;


// Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GetVideoEventsTest extends BaseTest {

	@Test
	public void testASendMessage() throws Exception {
		Map map = null;
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		// feed the route, which starts the test; note that there is no payload
		template.requestBody("direct:start", map);		
		assertMockEndpointsSatisfied();
	}

	@Override
	// this is the route used by this test case.
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				// the handler mapper should map videofoobar to the videoevent
				// client
				from("direct:start").to("cql:video").to("mock:result");
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
			if (list.size() != 4) {
				return false;
			}
			payLoad = list.get(0);
			if (!(payLoad instanceof Map)) {
				return false;
			}
			Map map = (Map) payLoad;
			if (map.size() != 5) {
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
