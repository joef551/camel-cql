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
import java.util.List;

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
 * Runs a lot of tests against the creation and validation of the CQL
 * statements. This test is executed against a local Cassandra node that has
 * been given the 'videodb' kepspace.
 * 
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CqlStmntTest {
	private static Client client;
	private static ArrayList<String> list = new ArrayList<String>();
	// private static Cluster cluster;
	private static ClusterBean clusterBean;
	static private ApplicationContext context = null;

	/**
	 * To run these tests, Cassandra needs to be running and the videodb
	 * keyspace must be installed. See ${project-home}/src/main/cql for videodb
	 * CQL scripts
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

		List<CqlStmnt> cqlList = null;

		// this cql should throw an exception because of the empty field
		// definition
		String cql = "select first, lastju	jmih from users where first=``";
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid "
					+ "cql statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				e.printStackTrace();
				fail("ERROR: did not get IllegalArgumentException "
						+ "got this instead: " + e.getClass().getName());
			}
		}

		// setCqls4Select will throw an exception because statement doesn't
		// start with select
		cql = "selectx * from track_by_artist";
		list.add(cql);
		client = new Client();
		try {
			client.setCqls4Select(list);
			fail("ERROR: did not get Exception for this invalid "
					+ "cql statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				e.printStackTrace();
				fail("ERROR: did not get IllegalArgumentException "
						+ "got this instead: " + e.getClass().getName());

			}
		}

		// setCqls4Select will throw an exception because statement doesn't
		// start with valid method
		cql = "insertx into artists_by_first_letter (first_letter,artist) "
				+ "values (`text:first_letter`, `text:artist`)";
		list.add(cql);
		client = new Client();
		try {
			client.setCqls4Update(list);
			fail("ERROR: did not get Exception for this invalid "
					+ "cql statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				e.printStackTrace();
				fail("ERROR: did not get IllegalArgumentException "
						+ "got this instead: " + e.getClass().getName());
			}
		}

		// this cql should throw an exception because the parameterized field's
		// definition is not fully defined
		cql = "select first from users where first=`int`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for "
						+ "this invalid cql statement: " + cql);
			}
		}

		// this cql should throw an exception because the field's
		// definition is not fully defined
		cql = "select first from users where first=`:id`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Select(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for "
						+ "this invalid cql statement: " + cql);
			}
		}

		// this cql should throw an exception because the field's
		// definition is not fully defined
		cql = "select first from users where first=`:`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid "
					+ "cql statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for "
						+ "this invalid cql statement: " + cql);
			}
		}

		// this cql should throw an exception because the field's
		// definition is not fully defined
		cql = "select first from users where first=`::`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid "
					+ "cql statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException "
						+ "for this invalid cql statement: " + cql);
			}
		}

		// this cql should throw an exception because the parameter doesn't have
		// a closing backtick
		cql = "select first from users where first = `int:first";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
		}

		// this cql should throw an exception because the parameter doesn't have
		// a starting backtick
		cql = "select first from users where first = int:first`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			// Cassandra driver will throw the exception
		}

		// this cql should throw an exception because 'integer' is not a valid
		// type
		cql = "select first from users where first=`integer:first`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for "
						+ "this invalid cql statement: " + cql);
			}
		}

		// this cql should throw an exception because 'nvarchar' is not a valid
		// type
		cql = "select first from users where first=`nvarchar:first`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for "
						+ "this invalid cql statement: " + cql);
			}
		}

		// this should result in an exception because there are two cql
		// statements with the same 'signature' being assigned to the
		// select method
		list.clear();
		list.add("select first from users where first=`int:first`");
		list.add("select last from users where first=`int:first`");
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should result in an exception because there are two cql
		// statements with the same 'signature' being assigned to the
		// select
		list.clear();
		list.add("select first from users where first=`int:first`");
		list.add("select last from users where first=`int:FIRSt`");
		client = new Client();
		client.setCqls4Select(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should throw an exception because the two cql statements
		// have the same signature.
		list.clear();
		list.add("insert into fubar (name,phone) values (`ascii:name`,`text:phone`)");
		list.add("insert into foobar (name,phone) values (`text:name`,`text:phone`)");
		client = new Client();
		client.setCqls4Insert(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should throw an exception because "ascii" is not a collection
		// type
		cql = "select first from users where first=`ascii:text:first`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Select(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for this "
						+ "invalid cql statement: " + cql);
			}
		}

		// same as above, but for insert
		cql = "insert into foo values(`text:ascii:first`)";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setCqls4Insert(list);
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for this "
						+ "invalid cql statement: " + cql);
			}
		}

		// same as above, but for delete
		cql = "delete from foo where first like `int:long:last`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		client.setCqls4Delete(list);
		try {
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for this "
						+ "invalid cql statement: " + cql);
			}
		}

		// this CQL statement is valid
		cql = "select username, created_date from users where username = `ascii:username`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Select(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		cqlList = client.getCqlStmnts4Select();
		assertEquals(
				true,
				cqlList.get(0)
						.getPreparedStr()
						.equals("select username , created_date from users where username = ?"));

		assertEquals(
				true,
				cqlList.get(0)
						.getOriginalStr()
						.equals("select username, created_date from users where username = `ascii:username`"));

		// this should not throw an exception
		cql = "select videoid, username from video_event where videoid=`uuid:videoid` and username=`ascii:username`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Select(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		cqlList = client.getCqlStmnts4Select();
		// System.out.println("["+cqlList.get(0).getPreparedStr() + "]");
		assertEquals(
				true,
				cqlList.get(0)
						.getPreparedStr()
						.equals("select videoid , username from video_event where videoid= ? and username= ?"));

		// should not throw an exception
		cql = "delete from video_event where username = `text:username`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Delete(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception on single quote test: "
					+ e.getMessage());
		}
		cqlList = client.getCqlStmnts4Delete();
		assertEquals(
				true,
				cqlList.get(0).getPreparedStr()
						.equals("delete from video_event where username = ?"));

		// should not throw an exception
		cql = "insert into video_event (videoid,username) "
				+ "values (blobAsUuid(timeuuidAsBlob(now())), `text:username`)";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls4Insert(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception on single quote test: "
					+ e.getMessage());
		}

		// should throw an exception b/c we're trying to add an insert into list
		// of deletes
		cql = "insert into video_event (videoid,username) "
				+ "values (blobAsUuid(timeuuidAsBlob(now())), `text:username`)";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		try {
			client.setCqls4Delete(list);
			fail("ERROR: did not get Exception when assigning insert to delete,  "
					+ "statement: " + cql);

		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for this "
						+ "invalid cql statement: " + cql);
			}
		}

		// should throw an exception - similar to above
		cql = "delete from video_event where username = `text:username`";
		list.clear();
		list.add(cql);
		client = new Client();
		client.setClusterBean(clusterBean);
		try {
			client.setCqls4Insert(list);
			fail("ERROR: did not get Exception when assigning delete to insert,  "
					+ "statement: " + cql);

		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException for this "
						+ "invalid cql statement: " + cql);
			}
		}

		// this should not throw an exception
		list.clear();
		list.add("select firstname from users where username=`ascii:first`");
		list.add("delete from video_event where username = `text:username`");
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertTrue(client.getCqls4Delete().size() == 1);
		assertTrue(client.getCqls4Select().size() == 1);
		assertTrue(client.getCqls4Update().isEmpty());
		assertTrue(client.getCqls4Insert().isEmpty());

		// this should not throw an exception
		list.clear();
		list.add("select firstname from users where username=`ascii:first`");
		list.add("select lastname from users where username=`ascii:last`");
		list.add("delete from video_event where username = `text:username`");
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setCqls(list);
		client.setKeyspace("videodb");
		try {
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertTrue(client.getCqls4Delete().size() == 1);
		assertTrue(client.getCqls4Select().size() == 2);
		assertTrue(client.getCqls4Update().isEmpty());
		assertTrue(client.getCqls4Insert().isEmpty());

		// this should throw an exception
		list.clear();
		list.add("select firstname from users where username=`ascii:first`");
		list.add("FUBAR from video_event where username = `text:username`");
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			client.setCqls(list);
			fail("ERROR: did not get Exception when assigning bogus delete");
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException");
			}
		}

	}

}
