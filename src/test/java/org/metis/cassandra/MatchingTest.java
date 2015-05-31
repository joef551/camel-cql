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
import org.junit.BeforeClass;
import org.junit.Test;
import org.metis.cassandra.Client;
import org.metis.cassandra.CqlStmnt;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.*;

/**
 * This test is executed against a local Cassandra node that has been given the
 * 'videodb' kepspace.
 * 
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MatchingTest {
	private static Client client;
	private static ArrayList<String> list = new ArrayList<String>();
	// private static Cluster cluster;
	private static ClusterBean clusterBean;
	static private ApplicationContext context = null;
	private static List<CqlStmnt> cqlList = null;
	private static Map<String, String> map = new HashMap<String, String>();

	/**
	 * Runs a series of test to validate CQL matching	
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void initialize() throws Exception {
		// Grab the cluster bean defined in the test cassandra.xml file.
		try {
			context = new ClassPathXmlApplicationContext("cassandra.xml");
		} catch (BeansException be) {
			System.out
					.println("ERROR: unable to load spring context, got this exception: \n"
							+ be.getLocalizedMessage());
			be.printStackTrace();
		}
		clusterBean = context.getBean(ClusterBean.class);
	}

	@Test
	public void TestA() {
		list.clear();
		map.clear();
		list.add("select * from users");
		list.add("select username, created_date from users where username = `ascii:username`");
		list.add("select videoid, username from video_event where videoid=`uuid:videoid` and username=`ascii:username`");
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
			fail("ERROR: got this Exception: " + e.getLocalizedMessage());
		}
		cqlList = client.getCqlStmnts4Select();
		assertTrue(cqlList != null);
		assertTrue(cqlList.get(0).getNumKeyTokens() == 0);
		assertTrue(cqlList.get(1).getNumKeyTokens() == 1);
		assertTrue(cqlList.get(2).getNumKeyTokens() == 2);
		assertFalse(cqlList.get(0).isPrepared());
		assertTrue(cqlList.get(1).isPrepared());
		assertTrue(cqlList.get(2).isPrepared());
		CqlStmnt stmnt = CqlStmnt.getMatch(cqlList, map.keySet());
		assertTrue(stmnt != null);
		assertTrue("select * from users".equals(stmnt.getOriginalStr()));
		map.put("username", "joef551");
		stmnt = CqlStmnt.getMatch(cqlList, map.keySet());
		assertTrue(stmnt != null);
		assertTrue("select username , created_date from users where username = ?"
				.equals(stmnt.getPreparedStr()));		
		map.put("videoid", "3984793");
		stmnt = CqlStmnt.getMatch(cqlList, map.keySet());
		assertTrue(stmnt != null);		
		assertTrue("select videoid , username from video_event where videoid= ? and username= ?"
				.equals(stmnt.getPreparedStr()));
		map.put("foobar", "3984793");
		stmnt = CqlStmnt.getMatch(cqlList, map.keySet());
		assertTrue(stmnt == null);
		map.clear();
		map.put("foo", "joef551");
		stmnt = CqlStmnt.getMatch(cqlList, map.keySet());
		assertTrue(stmnt == null);
		
		
	}
}
