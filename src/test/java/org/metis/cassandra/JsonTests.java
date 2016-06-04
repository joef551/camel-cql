package org.metis.cassandra;

import java.text.DateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.UUID;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.core.LocalDate;

import static org.metis.utils.Constants.*;

public class JsonTests extends BaseTest {

	/*
	 * These are String representations of the target CQL types.
	 */
	private static String test_uuid = UUIDs.random().toString();
	private static String test_uuid2 = UUIDs.random().toString();
	private static String test_varchar = "test_varchar";
	private static String test_boolean = Boolean.valueOf(true).toString();
	private static LocalDate lDate = LocalDate.fromMillisSinceEpoch(System
			.currentTimeMillis());
	private static String test_date = lDate.getYear() + "-" + lDate.getMonth()
			+ "-" + lDate.getDay();
	private static String test_decimal = BigDecimal.valueOf(4271958).toString();
	private static String test_double = Double.valueOf(427.1958).toString();
	private static String test_float = Float.valueOf(1958.427f).toString();
	private static String test_inet;
	private static String test_int = Integer.toString(27);
	private static String test_smallint = Short.toString((short) 27);
	private static String test_text = "test_text";
	private static String test_time = Long.valueOf(System.currentTimeMillis())
			.toString();

	private static String test_timestamp = DateFormat.getDateInstance().format(
			new Date());
	private static String test_timeuuid = UUIDs.timeBased().toString();
	private static String test_tinyint = Byte.valueOf(Byte.MAX_VALUE)
			.toString();
	private static String test_varint = BigInteger.valueOf(
			System.currentTimeMillis()).toString();
	private static Map<String, String> map = new HashMap<String, String>();

	@BeforeClass
	public static void initialize() throws Exception {
		test_inet = InetAddress.getLocalHost().getHostAddress();
		map.put("test_uuid", test_uuid);
		map.put("test_varchar", test_varchar);
		map.put("test_boolean", test_boolean);
		map.put("test_date", test_date);
		map.put("test_decimal", test_decimal);
		map.put("test_double", test_double);
		map.put("test_float", test_float);
		map.put("test_inet", test_inet);
		map.put("test_int", test_int);
		map.put("test_smallint", test_smallint);
		map.put("test_text", test_text);
		map.put("test_time", test_time);
		map.put("test_timestamp", test_timestamp);
		map.put("test_timeuuid", test_timeuuid);
		map.put("test_tinyint", test_tinyint);
		map.put("test_varint", test_varint);
	}

	/**
	 * Insert test fields
	 * 
	 * @throws Exception
	 */
	@Test
	public void testA() throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		String JSON = "{\"test_uuid\":\"" + test_uuid + "\", \"test_int\":"
				+ test_int + "}";
		map.put("json", JSON);
		template.requestBodyAndHeader("direct:start", map, CASSANDRA_METHOD,
				"insert");
		map.clear();
		JSON = "{\"test_uuid\":\"" + test_uuid2 + "\", \"test_int\":"
				+ test_int + "}";
		map.put("json", JSON);
		template.requestBodyAndHeader("direct:start", map, CASSANDRA_METHOD,
				"insert");
	}

	/**
	 * Use a regular SELECT to confirm
	 * 
	 * @throws Exception
	 */
	@Test
	public void testB() throws Exception {
		String JSON = "{\"test_uuid\":\"" + test_uuid + "\"}";
		getMockEndpoint("mock:result")
				.expectedMessagesMatches(new TestResult());
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"select");
		assertMockEndpointsSatisfied();
	}

	/**
	 * Use a JSON SELECT to confirm
	 * 
	 * @throws Exception
	 */
	@Test
	public void testC() throws Exception {
		// String JSON = "{\"json_test_uuid\":\"" + test_uuid + "\"}";
		getMockEndpoint("mock:result").expectedMessagesMatches(
				new JsonTestResult());
		template.requestBodyAndHeader("direct:start", null, CASSANDRA_METHOD,
				"select");
		assertMockEndpointsSatisfied();
	}

	/**
	 * Delete
	 * 
	 * @throws Exception
	 */
	@Test
	public void testD() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"test_uuid\":\"" + test_uuid + "\"}";
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"delete");
		JSON = "{\"test_uuid\":\"" + test_uuid2 + "\"}";
		template.requestBodyAndHeader("direct:start", JSON, CASSANDRA_METHOD,
				"delete");
	}

	/**
	 * Confirm deletion
	 * 
	 * @throws Exception
	 */
	@Test
	public void testE() throws Exception {
		// this is what we send to the CqlEndpoint
		String JSON = "{\"test_uuid\":\"" + test_uuid + "\"}";
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
				from("direct:start").to("cql:dtypes").to("mock:result");
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
			Map rmap = (Map) payLoad;
			if (rmap.size() != map.size()) {
				return false;
			}

			Object value = rmap.get("test_uuid");
			if (!(value instanceof UUID)) {
				return false;
			}

			value = rmap.get("test_int");
			if (!(value instanceof Integer)) {
				return false;
			}

			return true;
		}
	}

	private class JsonTestResult implements Predicate {
		public boolean matches(Exchange exchange) {
			@SuppressWarnings("unchecked")
			List<Map<String, String>> mList = (List<Map<String, String>>) exchange
					.getIn().getBody();
			if (mList == null || mList.size() != 2) {
				return false;
			}

			for (Map<String, String> map : mList) {

				if (map.size() != 1) {
					return false;
				}
				Object value = map.get("[json]");
				if (value == null) {
					return false;
				}
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
