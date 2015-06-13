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
 * statements.
 * 
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CqlStmntTest {
	private static CqlStmnt cqlStmnt;
	private Client client;
	private static ArrayList<CqlStmnt> list = new ArrayList<CqlStmnt>();
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
			throw be;
		}
		clusterBean = context.getBean(ClusterBean.class);

		if (clusterBean == null) {
			throw new Exception("ERROR: no cluster bean found in registry");
		}
	}

	@Test
	public void TestA() {

		List<CqlStmnt> cqlList = null;

		// should not throw an exception
		String cql = "delete from video_event    where username = `text:username`";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("delete from video_event where username = `text:username`"));

		
		// should not throw an exception
		cql = "select username , created_date from users where username = `ascii:username`";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));
		
		
		// should not throw an exception
		cql = "select username , created_date from users where username = `  ascii  :  username `";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));
		
		
		// should not throw an exception
		cql = "select username , created_date from users where username = `  ascii  "
				+ ":  "
				+ "username `";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));
		
		// should not throw an exception
		cql = "    select      username,created_date     from users where username = `  ascii  "
				+ ":  "
				+ "username `";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));
		
		// should not throw an exception
		cql = "    select  \n    username , created_date     from users where username = `  ascii  "
				+ ":  "
				+ "username `";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}		
		assertEquals(
				true,
				cqlStmnt.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));
		

		// this cql should throw an exception because of the empty field
		// definition
		cql = "select first, lastju	jmih from users where first=``";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);

		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
		}

		// this cql should throw an exception because the parameter doesn't have
		// a starting backtick
		cql = "select first from users where first = int:first`";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
			fail("ERROR: did not get Exception for this invalid cql "
					+ "statement: " + cql);
		} catch (Exception e) {
			// Cassandra driver will throw the exception
		}

		// this cql should throw an exception because 'integer' is not a valid
		// type
		cql = "select first from users where first=`integer:first`";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		list.add(new CqlStmnt("select first from users where first=`int:first`"));
		list.add(new CqlStmnt("select last from users where first=`int:first`"));
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");

			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should result in an exception because there are two cql
		// statements with the same 'signature' being assigned to the
		// select
		list.clear();
		list.add(new CqlStmnt("select first from users where first=`int:first`"));
		list.add(new CqlStmnt("select last from users where first=`int:FIRSt`"));
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");

			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should throw an exception because the two cql statements
		// have the same signature.
		list.clear();
		list.add(new CqlStmnt(
				"insert into fubar (name,phone) values (`ascii:name`,`text:phone`)"));
		list.add(new CqlStmnt(
				"insert into foobar (name,phone) values (`text:name`,`text:phone`)"));
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
			client.afterPropertiesSet();
			fail("ERROR: did not get Exception for duplicate cql test");
		} catch (Exception ignore) {
		}

		// this should throw an exception because "ascii" is not a collection
		// type
		cql = "select first from users where first=`ascii:text:first`";
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
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
		cqlStmnt = new CqlStmnt(cql);
		list.clear();
		list.add(cqlStmnt);
		try {
			cqlStmnt.afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
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
						.getStatement()
						.equals("select username , created_date from users where username = `ascii:username`"));

		// this should not throw an exception
		cql = "select videoid, username from video_event where videoid=`uuid:videoid` and username=`ascii:username`";
		cqlStmnt = new CqlStmnt(cql);
		list.clear();
		list.add(cqlStmnt);
		try {
			cqlStmnt.afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
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
		cqlStmnt = new CqlStmnt(cql);
		list.clear();
		list.add(cqlStmnt);
		try {
			cqlStmnt.afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
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
		cql = "delete from video_event where username = "
				+ "`   text    :    username`";
		cqlStmnt = new CqlStmnt(cql);
		list.clear();
		list.add(cqlStmnt);
		try {
			cqlStmnt.afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
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
		cql = "delete from video_event where username = " + "`   text    :    "
				+ "username`";
		cqlStmnt = new CqlStmnt(cql);
		list.clear();
		list.add(cqlStmnt);
		try {
			cqlStmnt.afterPropertiesSet();
			client = new Client();
			client.setCqls(list);
			client.setClusterBean(clusterBean);
			client.setKeyspace("videodb");
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
		cqlStmnt = new CqlStmnt(cql);
		try {
			cqlStmnt.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception on single quote test: "
					+ e.getMessage());
		}

		// this should not throw an exception
		list.clear();
		list.add(new CqlStmnt(
				"select firstname from users where username=`ascii:first`"));
		list.add(new CqlStmnt(
				"delete from video_event where username = `text:username`"));
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			client = new Client();
			client.setClusterBean(clusterBean);
			client.setCqls(list);
			client.setKeyspace("videodb");
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertTrue(client.getCqlStmnts4Delete().size() == 1);
		assertTrue(client.getCqlStmnts4Select().size() == 1);
		assertTrue(client.getCqlStmnts4Update().isEmpty());
		assertTrue(client.getCqlStmnts4Insert().isEmpty());

		// this should not throw an exception
		list.clear();
		list.add(new CqlStmnt(
				"select firstname from users where username=`ascii:first`"));
		list.add(new CqlStmnt(
				"select lastname from users where username=`ascii:last`"));
		list.add(new CqlStmnt(
				"delete from video_event where username = `text:username`"));
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			list.get(2).afterPropertiesSet();
			client = new Client();
			client.setClusterBean(clusterBean);
			client.setCqls(list);
			client.setKeyspace("videodb");
			client.afterPropertiesSet();
		} catch (Exception e) {
			fail("ERROR: got this exception: " + e.getMessage());
		}
		assertTrue(client.getCqlStmnts4Delete().size() == 1);
		assertTrue(client.getCqlStmnts4Select().size() == 2);
		assertTrue(client.getCqlStmnts4Update().isEmpty());
		assertTrue(client.getCqlStmnts4Insert().isEmpty());

		// this should throw an exception
		list.clear();
		list.add(new CqlStmnt(
				"select firstname from users where username=`ascii:first`"));
		list.add(new CqlStmnt(
				"FUBAR from video_event where username = `text:username`"));
		client = new Client();
		client.setClusterBean(clusterBean);
		client.setKeyspace("videodb");
		try {
			list.get(0).afterPropertiesSet();
			list.get(1).afterPropertiesSet();
			fail("ERROR: did not get Exception when assigning bogus delete");
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				fail("ERROR: did not get IllegalArgumentException");
			}
		}

	}

}
