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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.metis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.DisposableBean;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static org.metis.utils.Constants.*;
import static org.metis.utils.Utils.dumpStackTrace;
import static org.metis.cassandra.CqlStmnt.getCQLStmnt;

public class Client implements InitializingBean, DisposableBean, BeanNameAware,
		Processor {

	private static final Logger LOG = LoggerFactory.getLogger(Client.class);
	private boolean isRunning;
	private ClusterBean clusterBean;
	private Cluster cluster;
	private String keyspace;
	private Session session;
	private String beanName;

	// the injected CQL statements
	private List<String> cqls4Select = new ArrayList<String>();
	private List<String> cqls4Update = new ArrayList<String>();
	private List<String> cqls4Insert = new ArrayList<String>();
	private List<String> cqls4Delete = new ArrayList<String>();

	// the CQL statement objects derived from injected values
	private List<CqlStmnt> cqlStmnts4Select;
	private List<CqlStmnt> cqlStmnts4Update;
	private List<CqlStmnt> cqlStmnts4Insert;
	private List<CqlStmnt> cqlStmnts4Delete;

	// the supported CQL statements. this component only supports DML
	// statements.
	enum Method {
		SELECT, UPDATE, INSERT, DELETE;

		public boolean isSelect() {
			return this == SELECT;
		}

		public boolean isUpdate() {
			return this == UPDATE;
		}

		public boolean isDelete() {
			return this == DELETE;
		}

		public boolean isInsert() {
			return this == INSERT;
		}
	}

	public Client() {
	}

	/**
	 * Called by Spring after all of this POJO's properties have been set.
	 * Essentially makes this POJO ready to use.
	 */
	public void afterPropertiesSet() throws Exception {

		// the client must be wired to a ClusterBean, which is used for
		// accessing a Cassandra cluster
		if (getClusterBean() == null) {
			throw new Exception("client is not wired to a ClusterBean");
		}

		// the client must pertain to a Cassandra keyspace
		if (getKeyspace() == null || getKeyspace().isEmpty()) {
			throw new Exception("client must be assigned a Cassandra keyspace");
		}

		setCluster(getClusterBean().getCluster());

		// get the session to this Client's keyspace. The session is
		// thread-safe! the connect will throw a couple of Cassandra-specific
		// exceptions
		setSession(getCluster().connect(getKeyspace()));

		LOG.info(getBeanName() + ": clusterBean name = "
				+ getClusterBean().getBeanName());
		LOG.info(getBeanName() + ": cluster name = "
				+ getSession().getCluster().getMetadata().getClusterName());
		LOG.info(getBeanName()
				+ ": all clluster hosts = "
				+ getSession().getCluster().getMetadata().getAllHosts()
						.toString());
		LOG.info(getBeanName() + ": cluster partitioner = "
				+ getSession().getCluster().getMetadata().getPartitioner());

		// do some validation
		if (getCqls4Select().isEmpty() && getCqls4Update().isEmpty()
				&& getCqls4Delete().isEmpty() && getCqls4Insert().isEmpty()) {
			throw new Exception(getBeanName()
					+ ": Client has not been assigned any CQL statements");
		}

		// create and validate the injected CQL statements
		if (!getCqls4Select().isEmpty()) {
			setCqlStmnts4Select(new ArrayList<CqlStmnt>());
			for (String cql : getCqls4Select()) {
				CqlStmnt stmt = getCQLStmnt(cql, getSession());
				if (stmt.isEqual(getCqlStmnts4Select())) {
					throw new Exception(
							"Injected CQL statements for SELECT are not distinct");
				}
				getCqlStmnts4Select().add(stmt);
			}
			if (LOG.isTraceEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Select()) {
					LOG.debug(getBeanName() + ": CQL for SELECT = "
							+ cqlstmnt.getOriginalStr());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for SELECT = "
							+ cqlstmnt.getPreparedStr());
				}
			}
		}

		if (!getCqls4Insert().isEmpty()) {
			setCqlStmnts4Insert(new ArrayList<CqlStmnt>());
			for (String cql : getCqls4Insert()) {
				CqlStmnt stmt = getCQLStmnt(cql, getSession());
				if (stmt.isEqual(getCqlStmnts4Insert())) {
					throw new Exception(
							"Injected CQL statements for INSERT are not distinct");
				}
				getCqlStmnts4Insert().add(stmt);
			}
			if (LOG.isTraceEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Insert()) {
					LOG.debug(getBeanName() + ": CQL for INSERT = "
							+ cqlstmnt.getOriginalStr());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for INSERT = "
							+ cqlstmnt.getPreparedStr());
				}
			}
		}

		if (!getCqls4Update().isEmpty()) {
			setCqlStmnts4Update(new ArrayList<CqlStmnt>());
			for (String cql : getCqls4Update()) {
				CqlStmnt stmt = getCQLStmnt(cql, getSession());
				if (stmt.isEqual(getCqlStmnts4Update())) {
					throw new Exception(
							"Injected CQL statements for UPDATE are not distinct");
				}
				getCqlStmnts4Update().add(stmt);
			}
			if (LOG.isDebugEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Update()) {
					LOG.debug(getBeanName() + ": CQL for UPDATE = "
							+ cqlstmnt.getOriginalStr());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for UPDATE = "
							+ cqlstmnt.getPreparedStr());
				}
			}
		}

		if (!getCqls4Delete().isEmpty()) {
			setCqlStmnts4Delete(new ArrayList<CqlStmnt>());
			for (String cql : getCqls4Delete()) {
				CqlStmnt stmt = getCQLStmnt(cql, getSession());
				if (stmt.isEqual(getCqlStmnts4Delete())) {
					throw new Exception(
							"Injected CQL statements for DELETE are not distinct");
				}
				getCqlStmnts4Delete().add(stmt);
			}
			if (LOG.isDebugEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Delete()) {
					LOG.debug(getBeanName() + ": CQL for DELETE = "
							+ cqlstmnt.getOriginalStr());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for DELETE = "
							+ cqlstmnt.getPreparedStr());
				}
			}
		}
		setRunning(true);
	}

	/**
	 * This is the method that is called by the CassandraProducer. A route
	 * (channel) that is using this producer may be using a thread pool;
	 * therefore, this method, and bean in general, must be multi-thread
	 * capable!
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void process(Exchange exchange) throws Exception {

		LOG.trace(getBeanName()
				+ ":camelProcess - **** processing new request ****");

		if (!isRunning()) {
			LOG.error(getBeanName()
					+ ":camelProcess: this client has not been initialized");
			throw new Exception(getBeanName()
					+ ":camelProcess: this client has not been initialized");
		}

		// determine the message exchange pattern being requested.
		// the exchange pattern must be both in and out capable
		if (!exchange.getPattern().isInCapable()) {
			throw new Exception(getBeanName()
					+ ":camelProcess: exchange pattern is not inCapable");
		} else if (!exchange.getPattern().isOutCapable()) {
			throw new Exception(getBeanName()
					+ ":camelProcess: exchange pattern is not outCapable");
		}

		// get the Camel in message (not payload) for this request
		Message inMsg = exchange.getIn();
		if (inMsg == null) {
			throw new Exception(getBeanName()
					+ ":camelProcess: unable to get in message from exchange");
		}

		// check to see if the exchange is transacted; if so, we'll
		// make the call to the server within the context of a
		// transaction
		// TODO: needs some work
		if (exchange.isTransacted()) {
			LOG.trace(getBeanName() + ":camelProcess: exchange is transacted");
		}

		// get the payload (if any) and transform it
		List<Map<Object, Object>> listMap = null;
		Object payLoad = inMsg.getBody();
		if (payLoad != null) {
			
			// if payload is a stream or string, then it must be in the form of
			// a JSON object, which then needs to be transformed into a Map or
			// List of Maps
			if (payLoad instanceof InputStream) {
				LOG.trace(getBeanName()
						+ ":camelProcess: received body as InputStream");
				payLoad = Utils.parseJsonObject(
						Utils.getStringFromInputStream((InputStream) payLoad),
						Object.class);
			} else if (payLoad instanceof String) {
				LOG.trace(getBeanName()
						+ ":camelProcess: received body as String");
				payLoad = Utils.parseJsonObject((String) payLoad, Object.class);
			}

			// the payload (in its final form) must be in the form of either a
			// List of Maps or a Map
			if (payLoad instanceof List) {
				if (((List) payLoad).get(0) instanceof Map) {
					LOG.trace(getBeanName()
							+ ":camelProcess: received payload as "
							+ "List with size = {}", ((List) payLoad).size());
				} else {
					throw new Exception(getBeanName()
							+ ":camelProcess: received payload as List, but "
							+ "List elements are not of type Map");
				}
				// ensure all values in key-value pairs are Strings
				payLoad = Utils.objToString((Object) payLoad);
				listMap = (List) payLoad;
			} else if (payLoad instanceof Map) {
				LOG.trace(getBeanName()
						+ ":camelProcess: received payload as Map with "
						+ "size = {}", ((Map) payLoad).size());
				// ensure all values in key-value pairs are Strings
				payLoad = Utils.objToString((Object) payLoad);
				listMap = new ArrayList<Map<Object, Object>>();
				listMap.add((Map) payLoad);
			} else {
				throw new Exception(getBeanName()
						+ ":camelProcess: received payload as neither a "
						+ "List nor Map");
			}
			// ensure that all the Maps in the given list have the same set of
			// keys
			Set set1 = listMap.get(0).keySet();
			for (int i = 1; i < listMap.size(); i++) {
				if (!listMap.get(i).keySet().equals(set1)) {
					throw new Exception(getBeanName()
							+ ":camelProcess:ERROR, all Maps in the "
							+ "provided List of Maps do not have the "
							+ "same key set!");
				}
			}
		} else {
			LOG.trace(getBeanName() + ":camelProcess: payload was not provided");
		}

		// execute the Map(s) and hoist the returned List of Maps up into the
		// Exchange's out message
		exchange.getOut().setBody(execute(listMap, inMsg));

	}

	/**
	 * Execute the given list of Maps. Each Map is matched up against one of the
	 * CQL statements assigned to this Cassandra Client.
	 * 
	 * @param listMap
	 * @param method
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> execute(List<Map<Object, Object>> listMap,
			Message inMsg) throws Exception {

		// see if request method is in the message
		String inMethod = (String) inMsg.getHeader(CASSANDRA_METHOD);
		if (inMethod == null || inMethod.isEmpty()) {
			LOG.trace(getBeanName()
					+ ":execute - method was not provided, defaulting to {}",
					SELECT_STR);
			inMethod = SELECT_STR;
		}

		Method method = null;
		try {
			method = Method.valueOf(inMethod.toUpperCase());
			LOG.trace(getBeanName() + ":execute - processing this method: "
					+ method.toString());
		} catch (IllegalArgumentException e) {
			throw new Exception(getBeanName()
					+ ":execute: This method is not allowed [" + method + "]");
		}

		// based on the given method, attempt to find a set of candidate CQL
		// statements
		List<CqlStmnt> cqlStmnts = getCqlStmnts(method);
		if (cqlStmnts == null || cqlStmnts.isEmpty()) {
			throw new Exception(getBeanName() + ":execute - could not acquire "
					+ "CQL statements for this method:" + method.toString());
		}

		// if no list of maps was passed in, then dummy one up
		List<Map<Object, Object>> myListMap = null;
		if (listMap == null) {
			LOG.trace(getBeanName() + ":execute - listMap was not provided");
			myListMap = new ArrayList<Map<Object, Object>>();
			myListMap.add(new HashMap<Object, Object>());
		} else {
			// create a copy of the given list.
			myListMap = new ArrayList<Map<Object, Object>>();
			for (Map<Object, Object> map : listMap) {
				myListMap.add(new HashMap<Object, Object>(map));
			}
		}

		LOG.trace(getBeanName() + ":execute - executing this many maps {}",
				myListMap.size());

		// Get the CQL statement that matches the given map(s)
		CqlStmnt cqlStmnt = CqlStmnt.getMatch(cqlStmnts,
				((Map) myListMap.get(0)).keySet());

		if (cqlStmnt == null) {
			throw new Exception(getBeanName()
					+ ":execute: this key set could not "
					+ "be mapped to a CQL statement: ["
					+ myListMap.get(0).keySet().toString() + "]");
		}

		// iterate through the given Maps (if any) and execute their
		// corresponding cql statement(s)
		try {
			List<ResultSet> resultSets = new ArrayList<ResultSet>();
			for (Map map : myListMap) {
				ResultSet resultSet = cqlStmnt.execute(map, inMsg);
				if (resultSet != null) {
					resultSets.add(resultSet);
				}
			} // for (Map map : listMap)

			LOG.trace(getBeanName()
					+ ":execute: successfully executed statement(s)");
			LOG.trace(getBeanName()
					+ ":execute: received this many result sets {}",
					resultSets.size());

			// if no result sets were returned, then we're done!
			if (resultSets.isEmpty()) {
				return null;
			}

			List<Map<String, Object>> listOutMaps = new ArrayList<Map<String, Object>>();
			Row row = null;
			// iterate through the returned result sets
			for (ResultSet resultSet : resultSets) {
				// grab the metadata for the result set
				ColumnDefinitions cDefs = resultSet.getColumnDefinitions();
				// transfer each row of the result set to a Map and place all
				// the maps in a List
				while ((row = resultSet.one()) != null) {
					Map<String, Object> map = new HashMap<String, Object>();
					for (Definition cDef : cDefs.asList()) {
						map.put(cDef.getName(), CqlToken.getObjectFromRow(row,
								cDef.getName(), cDef.getType().getName()));
					}
					listOutMaps.add(map);
				}
			}
			// return the List of Maps up into the Exchange's out message
			LOG.trace(getBeanName()
					+ ":camelProcess: sending back this many Maps {}",
					listOutMaps.size());

			return listOutMaps;

		} catch (Exception exc) {
			LOG.error(getBeanName() + ":ERROR, caught this "
					+ "Exception while executing CQL statement " + "message: "
					+ exc.toString());
			LOG.error(getBeanName() + ": exception stack trace follows:");
			dumpStackTrace(exc.getStackTrace());
			if (exc.getCause() != null) {
				LOG.error(getBeanName() + ": Caused by "
						+ exc.getCause().toString());
				LOG.error(getBeanName()
						+ ": causing exception stack trace follows:");
				dumpStackTrace(exc.getCause().getStackTrace());
			}
			// throw the exception back
			throw exc;
		}
	}

	private List<CqlStmnt> getCqlStmnts(Method method)
			throws IllegalArgumentException {
		if (method == null) {
			throw new IllegalArgumentException("provided method is null");
		}
		switch (method) {
		case SELECT:
			return getCqlStmnts4Select();
		case DELETE:
			return getCqlStmnts4Delete();
		case UPDATE:
			return getCqlStmnts4Update();
		case INSERT:
			return getCqlStmnts4Insert();
		default:
			throw new IllegalArgumentException(
					"this request method in not recongized:" + method);
		}
	}

	/**
	 * Used for validating CQL statement for select
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private String valCql4Select(String cql) throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Insert: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(SELECT_STR)) {
				throw new IllegalArgumentException(
						"valCql4Select: invalid CQL statement - does not start with 'select'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Select: invalid CQL statement - empty or null statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for insert
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private String valCql4Insert(String cql) throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Insert: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(INSERT_STR)) {
				throw new IllegalArgumentException(
						"valCql4Insert: invalid CQL statement - does not start with 'insert'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Insert: invalid CQL statement - empty or null statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for update
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private String valCql4Update(String cql) throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valCql4Update: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(UPDATE_STR)) {
				throw new IllegalArgumentException(
						"valCql4Update: invalid CQL statement - must start with "
								+ "'update'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Update: invalid CQL statement - empty or null "
							+ "statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for DELETE
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private String valCql4Delete(String cql) throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Delete: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(DELETE_STR)) {
				throw new IllegalArgumentException(
						"valSql4Delete: invalid CQL statement - must start "
								+ "with 'delete'");
			}
		} else {
			throw new IllegalArgumentException(
					"valSql4Delete: invalid CQL statement - empty or null "
							+ "statement");
		}
		return cql.trim();
	}

	/**
	 * Called by Spring context when it is closed. Spring context is supposed to
	 * dispose all singletons, like this one, when it is closed.
	 */
	public void destroy() {
		setRunning(false);
	}

	/**
	 * @return the isRunning
	 */
	public boolean isRunning() {
		return isRunning;
	}

	/**
	 * @param isRunning
	 *            the isRunning to set
	 */
	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	/**
	 * @return the clusterBean
	 */
	public ClusterBean getClusterBean() {
		return clusterBean;
	}

	/**
	 * @param clusterBean
	 *            the clusterBean to set
	 */
	public void setClusterBean(ClusterBean clusterBean) {
		this.clusterBean = clusterBean;
	}

	/**
	 * @return the cluster
	 */
	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * @param cluster
	 *            the cluster to set
	 */
	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	/**
	 * @return the keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace
	 *            the keyspace to set
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * @return the session
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session
	 *            the session to set
	 */
	public void setSession(Session session) {
		this.session = session;
	}

	@Override
	public void setBeanName(String name) {
		beanName = name;
	}

	public String getBeanName() {
		return beanName;
	}

	/**
	 * @return the sqls4Select
	 */
	public List<String> getCqls4Select() {
		return cqls4Select;
	}

	public void setCqls4Select(List<String> cqls)
			throws IllegalArgumentException {
		if (cqls == null || cqls.isEmpty()) {
			throw new IllegalArgumentException(
					"setCqls4Select: invalid list of CQL statements - empty or "
							+ "null statement");
		}
		for (String cql : cqls) {
			getCqls4Select().add(valCql4Select(cql));
		}
	}

	/**
	 * @return the cqls4Insert
	 */
	public List<String> getCqls4Insert() {
		return cqls4Insert;
	}

	/**
	 * @param cqls4Insert
	 *            the cqls4Insert to set
	 */
	public void setCqls4Insert(List<String> cqls) {
		if (cqls == null || cqls.isEmpty()) {
			throw new IllegalArgumentException(
					"setCqls4Insert: invalid list of CQL statements - empty or "
							+ "null statement");
		}
		for (String cql : cqls) {
			getCqls4Insert().add(valCql4Insert(cql));
		}
	}

	/**
	 * @return the sqls4Update
	 */
	public List<String> getCqls4Update() {
		return cqls4Update;
	}

	public void setCqls4Update(List<String> cqls)
			throws IllegalArgumentException {
		if (cqls == null || cqls.isEmpty()) {
			throw new IllegalArgumentException(
					"setCqls4Select: invalid list of CQL statements - empty or "
							+ "null statement");
		}
		for (String cql : cqls) {
			getCqls4Update().add(valCql4Update(cql));
		}
	}

	/**
	 * @return the sqls4Delete
	 */
	public List<String> getCqls4Delete() {
		return cqls4Delete;
	}

	public void setCqls4Delete(List<String> cqls)
			throws IllegalArgumentException {
		if (cqls == null || cqls.isEmpty()) {
			throw new IllegalArgumentException(
					"setCqls4Select: invalid list of CQL statements - empty or "
							+ "null statement");
		}
		for (String cql : cqls) {
			getCqls4Delete().add(valCql4Delete(cql));
		}
	}

	/**
	 * @param cqlStmnts4Select
	 *            the cqlStmnts4Select to set
	 */
	public void setCqlStmnts4Select(List<CqlStmnt> cqlStmnts4Select) {
		this.cqlStmnts4Select = cqlStmnts4Select;
	}

	/**
	 * @return the cqlStmnts4Select
	 */
	public List<CqlStmnt> getCqlStmnts4Select() {
		return cqlStmnts4Select;
	}

	/**
	 * @param cqlStmnts4Update
	 *            the cqlStmnts4Update to set
	 */
	public void setCqlStmnts4Update(List<CqlStmnt> cqlStmnts4Update) {
		this.cqlStmnts4Update = cqlStmnts4Update;
	}

	public List<CqlStmnt> getCqlStmnts4Update() {
		return cqlStmnts4Update;
	}

	/**
	 * @return the cqlStmnts4Delete
	 */
	public List<CqlStmnt> getCqlStmnts4Delete() {
		return cqlStmnts4Delete;
	}

	/**
	 * @param cqlStmnts4Delete
	 *            the cqlStmnts4Delete to set
	 */
	public void setCqlStmnts4Delete(List<CqlStmnt> cqlStmnts4Delete) {
		this.cqlStmnts4Delete = cqlStmnts4Delete;
	}

	/**
	 * @return the cqlStmnts4Insert
	 */
	public List<CqlStmnt> getCqlStmnts4Insert() {
		return cqlStmnts4Insert;
	}

	/**
	 * @param cqlStmnts4Insert
	 *            the cqlStmnts4Insert to set
	 */
	public void setCqlStmnts4Insert(List<CqlStmnt> cqlStmnts4Insert) {
		this.cqlStmnts4Insert = cqlStmnts4Insert;
	}

	/**
	 * @param cqls
	 *            the cqls to set
	 */
	public void setCqls(List<String> cqls) throws IllegalArgumentException {
		if (cqls == null || cqls.isEmpty()) {
			return;
		}
		for (String cql : cqls) {
			cql = cql.trim();
			String[] tokens = cql.split(DELIM);
			if (tokens != null && tokens.length > 0) {
				if (UPDATE_STR.equalsIgnoreCase(tokens[0])) {
					getCqls4Update().add(valCql4Update(cql));
				} else if (DELETE_STR.equalsIgnoreCase(tokens[0])) {
					getCqls4Delete().add(valCql4Delete(cql));
				} else if (SELECT_STR.equalsIgnoreCase(tokens[0])) {
					getCqls4Select().add(valCql4Select(cql));
				} else if (INSERT_STR.equalsIgnoreCase(tokens[0])) {
					getCqls4Insert().add(valCql4Insert(cql));
				} else {
					throw new IllegalArgumentException(
							"invalid CQL statement: " + cql);
				}

			}
		}
	}

}
