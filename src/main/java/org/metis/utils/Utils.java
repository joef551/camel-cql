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
package org.metis.utils;

import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import static com.fasterxml.jackson.core.JsonToken.*;
import static org.metis.utils.Constants.*;

public class Utils {

	public static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	private static ObjectMapper jsonObjectMapper = new ObjectMapper();
	private static JsonFactory jsonFactory = new JsonFactory();

	/**
	 * Parse the given JSON object (stream). Returns a List of Maps, where each
	 * Map pertains to a JSON object.
	 * 
	 * A JSON object is an unordered set of key:value pairs. An object begins
	 * with { (left brace) and ends with } (right brace). Each key is followed
	 * by : (colon) and the key:value pairs are separated by , (comma). For
	 * example: <code>
	 * <p>
	 * {  
	 *    "first":   "Joe",
	 *    "last":    "Fernandez", 
	 *    "email":   "joe.fernandez@ttmsolutions.com",
	 *    "age":	  22,
	 *    "gender":   "male",
	 *    "verified": false 
	 * }
	 * </p>
	 * </code>
	 * 
	 * Metis only accepts JSON objects or JSON arrays comprised only of objects
	 * or nested arrays. For updating, each JSON object is viewed as a table row
	 * or entity.
	 * 
	 * @param jsonStream
	 * @return
	 * @throws Exception
	 */
	public static List<Map<String, String>> parseJson(InputStream jsonStream)
			throws Exception {

		return parseJson(getStringFromInputStream(jsonStream));

	}

	public static List<Map<String, String>> parseJson(String json)
			throws Exception {

		if (jsonFactory == null) {
			LOG.error("parseJson: ERROR, jsonFactory is null");
			throw new Exception("parseJson: ERROR, jsonFactory is null");
		}

		LOG.trace("parseJson: jsonString = " + json);
		JsonParser jp = jsonFactory.createParser(json);

		List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		try {
			parseJson(jp, rows);
		} finally {
			jp.close();
		}
		return rows;
	}

	/**
	 * Recursively steps through JSON object and arrays of objects. We support
	 * only a single object or an array of objects, where each object represents
	 * an entity (e.g., a student, a customer, an account, etc.).
	 * 
	 * All objects must have the same identical set of keys.
	 * 
	 * @param jp
	 * @param params
	 * @throws Exception
	 */
	private static void parseJson(JsonParser jp, List<Map<String, String>> rows)
			throws Exception {

		// get the next json token
		JsonToken current = jp.nextToken();

		// base case: return if we've reached end of json stream
		if (current == null) {
			return;
		}

		// all rows must have the identical set of keys!
		Map<String, String> firstRow = null;
		if (!rows.isEmpty()) {
			firstRow = rows.get(0);
		}

		// we only accept objects or arrays of objects
		switch (current) {
		case START_OBJECT:
			HashMap<String, String> row = new HashMap<String, String>();
			while (jp.nextToken() != END_OBJECT) {
				// parser should be on 'key' token
				String key = jp.getCurrentName().toLowerCase();
				// ensure all rows have the identical set of keys!
				if (firstRow != null && firstRow.get(key) == null) {
					String eStr = "parseJson: given list of json objects do "
							+ "not have identical set of keys";
					LOG.error(eStr);
					throw new Exception(eStr);
				}
				// now advance to 'value' token
				jp.nextToken();
				String value = jp.getText();
				row.put(key, value);
			}
			// if row is not null, add it to the rows list
			if (!row.isEmpty()) {
				// ensure all rows have the identical set of keys!
				if (firstRow != null && firstRow.size() != row.size()) {
					String eStr = "parseJson: given list of json objects do "
							+ "not have identical set of keys; number of "
							+ "keys vary";
					LOG.error(eStr);
					throw new Exception(eStr);
				}
				rows.add(row);
			}
			break;
		case START_ARRAY:
		case END_ARRAY:
			break;
		default:
			LOG.error("parseJson: ERROR, json token is neither object nor array");
			throw new Exception(
					"parseJson: ERROR, json token is neither object nor array");
		}

		// go on to the next start-of-array, end-of-array, start-of-object, or
		// end of stream
		parseJson(jp, rows);
	}

	/**
	 * Given a list of Maps<String, Object>, returns the corresponding JSON
	 * objects(s) as a JSON String
	 * 
	 * @param list
	 * @return
	 * @throws JsonProcessingException
	 */
	public static String generateJson(List<Map<String, Object>> list)
			throws JsonProcessingException, Exception {

		if (jsonObjectMapper == null) {
			LOG.error("generateJson: ERROR, jsonObjectMapper is null");
			throw new Exception("generateJson: ERROR, jsonObjectMapper is null");
		}
		return jsonObjectMapper.writeValueAsString(list);
	}

	/**
	 * Given a JSON object and Class, returns an instance of the Class that is
	 * represented by the JSON object
	 * 
	 * @param json
	 * @param c
	 * @return
	 * @throws JsonProcessingException
	 * @throws Exception
	 */
	public static Object parseJsonObject(String json, Class c)
			throws JsonProcessingException, Exception {
		if (jsonObjectMapper == null) {
			LOG.error("generateJson: ERROR, jsonObjectMapper is null");
			throw new Exception("generateJson: ERROR, jsonObjectMapper is null");
		}
		return jsonObjectMapper.readValue(json, c);
	}

	/**
	 * Returns a String representation of the contents of the input stream
	 * 
	 * @param is
	 * @return
	 */
	public static String getStringFromInputStream(InputStream is) {
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return sb.toString();
	}

	/**
	 * Used for dumping the stack trace
	 * 
	 * @param elements
	 */
	public static void dumpStackTrace(StackTraceElement[] elements) {
		if (elements == null) {
			return;
		}
		int i = 0;
		for (; i < 10; i++) {
			LOG.error("at " + elements[i].toString());
		}
		if (elements.length > i) {
			LOG.error("... " + (elements.length - i) + " more");
		}
	}

	/**
	 * Given a query string, places the name value pairs in a HashMap
	 * 
	 * @param query
	 * @return
	 */
	public static Map<String, String> getQueryMap(String query) {
		LOG.trace("getQueryMap: entered with this query string = " + query);
		if (query == null || query.isEmpty()) {
			return null;
		}
		Map<String, String> map = new HashMap<String, String>();
		String[] params = query.split(AMPERSAND_STR);
		for (String param : params) {
			String nv[] = param.split("=");
			if (nv.length == 2) {
				map.put(nv[0].trim(), nv[1].trim());
			}
		}
		LOG.trace("getQueryMap: returning this map = " + map.toString());
		return map;
	}

	public static Map<String, String> parseUrlEncoded(InputStream encodedStream)
			throws UnsupportedEncodingException {
		return parseUrlEncoded(getStringFromInputStream(encodedStream));
	}

	public static Map<String, String> parseUrlEncoded(String queryString)
			throws UnsupportedEncodingException {

		if (queryString == null || queryString.length() == 0) {
			return null;
		}

		Map<String, String> map = new HashMap<String, String>();
		for (String pair : queryString.split(AMPERSAND_STR)) {
			int eq = pair.indexOf(EQUALS_STR);
			if (eq < 0) {
				// key with no value
				map.put(URLDecoder.decode(pair, UTF8_STR), "");
			} else {
				// key=value
				String key = URLDecoder.decode(pair.substring(0, eq), UTF8_STR);
				String value = URLDecoder.decode(pair.substring(eq + 1),
						UTF8_STR);
				map.put(key.toLowerCase(), value);
			}
		}
		return map;
	}

	public static String byteArrayToHexString(byte[] b) {
		StringBuilder sb = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			int v = b[i] & 0xff;
			if (v < 16) {
				sb.append('0');
			}
			sb.append(Integer.toHexString(v));
		}
		return sb.toString().toUpperCase();
	}

	public static String getHashOf(String s) throws Exception {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		s += "TTM";
		md.update(s.getBytes());
		return byteArrayToHexString(md.digest());
	}

	/**
	 * 
	 * @param inObj
	 * @return
	 */
	public static Object objToString(Object inObj) {
		if (inObj == null) {
			return null;
		}
		if (inObj instanceof Map) {
			return objToString((Map) inObj);
		} else if (inObj instanceof Set) {
			return objToString((Set) inObj);
		} else if (inObj instanceof List) {
			return objToString((List) inObj);
		}
		return null;
	}

	public static Map<String, Object> objToString(Map<Object, Object> inMap) {
		if (inMap == null) {
			return null;
		}
		Map<String, Object> outMap = new HashMap<String, Object>();
		for (Object key : inMap.keySet()) {
			Object obj = inMap.get(key);
			if (obj instanceof Map) {
				outMap.put(key.toString(), objToString((Map) obj));
			} else if (obj instanceof Set) {
				outMap.put(key.toString(), objToString((Set) obj));
			} else if (obj instanceof List) {
				outMap.put(key.toString(), objToString((List) obj));
			} else {
				outMap.put(key.toString(), inMap.get(key).toString());
			}
		}
		return outMap;
	}

	public static Set<Object> objToString(Set<Object> inSet) {
		if (inSet == null) {
			return null;
		}
		Set<Object> outSet = new HashSet<Object>();
		for (Object obj : inSet) {
			if (obj instanceof Map) {
				outSet.add(objToString((Map) obj));
			} else if (obj instanceof Set) {
				outSet.add(objToString((Set) obj));
			} else if (obj instanceof List) {
				outSet.add(objToString((List) obj));
			} else {
				outSet.add(obj.toString());
			}
		}
		return outSet;
	}

	public static List<Object> objToString(List<Object> inList) {
		if (inList == null) {
			return null;
		}
		List<Object> outList = new ArrayList<Object>();
		for (Object obj : inList) {
			if (obj instanceof Map) {
				outList.add(objToString((Map) obj));
			} else if (obj instanceof Set) {
				outList.add(objToString((Set) obj));
			} else if (obj instanceof List) {
				outList.add(objToString((List) obj));
			} else {
				outList.add(obj.toString());
			}
		}
		return outList;
	}

	/**
	 * Creates a Cassandra TupleType based on the given list of Objects. The
	 * TupleType can then be used to create a TupleValue.
	 * 
	 * @param values
	 * @return
	 */
	public static TupleType getTupleType(List<Object> values)
			throws IllegalArgumentException {
		List<DataType> dataTypes = new ArrayList<DataType>();
		for (Object value : values) {
			if (value instanceof ByteBuffer) {
				dataTypes.add(DataType.blob());
			} else if (value instanceof BigDecimal) {
				dataTypes.add(DataType.decimal());
			} else if (value instanceof BigInteger) {
				dataTypes.add(DataType.varint());
			} else if (value instanceof Boolean) {
				dataTypes.add(DataType.cboolean());
			} else if (value instanceof InetAddress) {
				dataTypes.add(DataType.inet());
			} else if (value instanceof Integer) {
				dataTypes.add(DataType.cint());
			} else if (value instanceof Long) {
				dataTypes.add(DataType.counter());
			} else if (value instanceof Float) {
				dataTypes.add(DataType.cfloat());
			} else if (value instanceof Double) {
				dataTypes.add(DataType.cdouble());
			} else if (value instanceof Date) {
				dataTypes.add(DataType.timestamp());
			} else if (value instanceof String) {
				dataTypes.add(DataType.text());
			} else {
				throw new IllegalArgumentException("unknown data type of "
						+ value.getClass().getCanonicalName());
			}
		}
		return TupleType.of(dataTypes.toArray(new DataType[dataTypes.size()]));
	}

	public static void main(String[] args) throws Exception {
		String jsonStr = "[{\"name\":\"Joe Fernandez\", \"gender\":\"MALE\",  "
				+ "\"email\": \"joe.fernandez@ttmsolutions.com\", \"verified\":false, "
				+ "\"age\":22 }, {\"name\":\"Jim Fernandez\", \"gender\":\"MALE\",  "
				+ "\"email\": \"jim.fernandez@ttmsolutions.com\", \"verified\":false, "
				+ "\"age\":52 }]";

		List list = null;
		Map map = null;

		Object obj = parseJsonObject(jsonStr, Object.class);
		if (obj instanceof List) {
			list = (List) obj;
			System.out.println("list size = " + list.size());
			for (Object o : list) {
				System.out.println("obj type = " + o.getClass().getName());
			}
			obj = objToString(obj);
			if (!(obj instanceof List)) {
				System.out.println("ERROR: objToString did not return a List");
			}
			for (Object obj2 : (List) obj) {
				if (!(obj2 instanceof Map)) {
					System.out.println("ERROR: did not get expected map");
				}
				for (Object obj3 : ((Map) obj2).values()) {
					if (!(obj3 instanceof String)) {
						System.out
								.println("ERROR: did not get expected String");
						System.exit(1);
					}
					System.out.println("final obj = " + (String) obj3);
				}
			}
		} else {
			map = (Map) obj;
			System.out.println("map size = " + map.size());
			for (Object o : map.values()) {
				System.out.println("obj type = " + o.getClass().getName());
			}
		}

		System.out.println();

		String jsonStr2 = "{\"name\":\"Joe Fernandez\", \"gender\":\"MALE\",  "
				+ "\"email\": \"joe.fernandez@ttmsolutions.com\", \"verified\":false, "
				+ "\"age\":22, \"books\":[\"book1\",\"book2\"]}";

		obj = parseJsonObject(jsonStr2, Object.class);
		if (obj instanceof List) {
			list = (List) obj;
			System.out.println("list size = " + list.size());
			for (Object o : list) {
				System.out.println("obj type = " + o.getClass().getName());
			}
		} else {
			map = (Map) obj;
			System.out.println("map size = " + map.size());
			for (Object o : map.values()) {
				System.out.println("obj type = " + o.getClass().getName());
			}
		}
		System.out.println();

		String jsonStr3 = ""
				+ "{"
				+ "\"name\":\"Joe Fernandez\", \"gender\":\"MALE\",  "
				+ "\"email\": \"joe.fernandez@ttmsolutions.com\", \"verified\":false, "
				+ "\"age\":22, \"books\": {\"title\":\"book1\",\"isbn\":\"123-56\"} "
				+ "}";

		obj = parseJsonObject(jsonStr3, Object.class);
		if (obj instanceof List) {
			list = (List) obj;
			System.out.println("list size = " + list.size());
			for (Object o : list) {
				System.out.println("obj type = " + o.getClass().getName());
			}
		} else {
			map = (Map) obj;
			System.out.println("map size = " + map.size());
			for (Object o : map.values()) {
				System.out.println("obj type = " + o.getClass().getName());
			}
		}
		System.out.println();

		String foo = "FUBAR";
		System.out.println(foo.toString());

	}

}
