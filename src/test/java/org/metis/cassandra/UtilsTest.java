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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Test;
import org.metis.utils.Utils;

//Test methods will be executed in ascending order by name
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UtilsTest {

	@Test
	public void TestA() {
		Map<Object, Object> inMap = new HashMap<Object, Object>();
		inMap.put("key1", "value1");
		inMap.put("key2", Boolean.FALSE);
		inMap.put("key3", Integer.valueOf(1));
		Map<String, Object> outMap = Utils.objToString(inMap);
		assertTrue(outMap != null);
		assertTrue(outMap.size() > 0);
		for (Object value : outMap.values()) {
			assertTrue(value instanceof String);
		}
		assertTrue(((String) outMap.get("key1")).equals("value1"));
		assertTrue(((String) outMap.get("key2")).equals("false"));
		assertTrue(((String) outMap.get("key3")).equals("1"));
	}

	@Test
	public void TestB() {
		Map<Object, Object> inMap = new HashMap<Object, Object>();
		inMap.put(Integer.valueOf(1), "value1");
		inMap.put(Double.valueOf(2.0), Boolean.FALSE);
		inMap.put(Float.valueOf(3), Integer.valueOf(1));
		Map<String, Object> outMap = Utils.objToString(inMap);
		assertTrue(outMap != null);
		assertTrue(outMap.size() > 0);
		for (Object value : outMap.values()) {
			assertTrue(value instanceof String);
		}
		for (String key : outMap.keySet()) {
			assertTrue(key instanceof String);
		}
		assertTrue(((String) outMap.get("1")).equals("value1"));
		assertTrue(((String) outMap.get("2.0")).equals("false"));
		assertTrue(((String) outMap.get("3.0")).equals("1"));
	}

	@Test
	public void TestC() {
		Map<Object, Object> inMap1 = new HashMap<Object, Object>();
		inMap1.put(Integer.valueOf(1), "value1");
		inMap1.put(Double.valueOf(2.0), Boolean.FALSE);
		inMap1.put(Float.valueOf(3), Integer.valueOf(1));

		Map<Object, Object> inMap2 = new HashMap<Object, Object>();
		inMap2.put(Integer.valueOf(2), "value2");
		inMap2.put(Double.valueOf(3.0), Boolean.TRUE);
		inMap2.put(Float.valueOf(4), Integer.valueOf(4));

		List<Object> list = new ArrayList<Object>();
		list.add(inMap1);
		list.add(inMap2);

		List<Object> outList = Utils.objToString((List) list);
		assertTrue(outList != null);
		assertTrue(outList.size() == 2);
		assertTrue(outList.get(0) instanceof Map);
		assertTrue(outList.get(1) instanceof Map);
		Map map1 = (Map) outList.get(0);
		Map map2 = (Map) outList.get(1);
		for (Object key : map1.keySet()) {
			assertTrue(key instanceof String);
		}
		for (Object key : map2.keySet()) {
			assertTrue(key instanceof String);
		}
		for (Object value : map1.values()) {
			assertTrue(value instanceof String);
		}
		for (Object value : map2.values()) {
			assertTrue(value instanceof String);
		}
		assertTrue(((String) map1.get("1")).equals("value1"));
		assertTrue(((String) map1.get("2.0")).equals("false"));
		assertTrue(((String) map1.get("3.0")).equals("1"));
		assertTrue(((String) map2.get("2")).equals("value2"));
		assertTrue(((String) map2.get("3.0")).equals("true"));
		assertTrue(((String) map2.get("4.0")).equals("4"));
	}

	@Test
	public void TestD() {
		Map<Object, Object> inMap1 = new HashMap<Object, Object>();
		inMap1.put(Integer.valueOf(1), "value1");
		inMap1.put(Double.valueOf(2.0), Boolean.FALSE);
		inMap1.put(Float.valueOf(3), Integer.valueOf(1));

		Map<Object, Object> inMap2 = new HashMap<Object, Object>();
		inMap2.put(Integer.valueOf(2), "value2");
		inMap2.put(Double.valueOf(3.0), Boolean.TRUE);
		inMap2.put(Float.valueOf(4), Integer.valueOf(4));

		inMap1.put("inMap2", inMap2);
		Map<String, Object> outMap = Utils.objToString(inMap1);
		assertTrue(outMap != null);
		assertTrue(outMap.size() == 4);

	}
}
