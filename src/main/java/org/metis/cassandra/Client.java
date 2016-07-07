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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.metis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static org.metis.utils.Constants.*;
import static org.metis.utils.Utils.dumpStackTrace;

public class Client implements InitializingBean, DisposableBean, BeanNameAware,
		ApplicationContextAware, Processor {

	// various properties
	private static final Logger LOG = LoggerFactory.getLogger(Client.class);
	private boolean isRunning;
	private ClusterBean clusterBean;
	private Cluster cluster;
	private String keyspace;
	private Session session;
	private String beanName;
	private boolean autoInject = true;
	private ApplicationContext applicationContext;

	// the injected CQL statements
	private List<CqlStmnt> cqlStmnts4Select = new ArrayList<CqlStmnt>();
	private List<CqlStmnt> cqlStmnts4Update = new ArrayList<CqlStmnt>();
	private List<CqlStmnt> cqlStmnts4Insert = new ArrayList<CqlStmnt>();
	private List<CqlStmnt> cqlStmnts4Delete = new ArrayList<CqlStmnt>();

	// the supported CQL statements. this component only supports DML
	// statements.
	enum Method {
		SELECT, UPDATE, INSERT, DELETE, NOOP;

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

		public boolean isNoop() {
			return this == NOOP;
		}
	}

	// the default method used for this bean
	private Method defaultMethod;

	private final ReentrantLock sessionLock = new ReentrantLock();
	private long sessionLockWaitTime = 10000L;

	public Client() {
	}

	/**
	 * Called by Spring after all of this POJO's properties have been set.
	 * Essentially makes this POJO ready to use.
	 */
	public void afterPropertiesSet() throws Exception {

		if (!isAutoInject()) {
			LOG.info(getBeanName() + ": disabled!");
			return;
		}

		// the client must be wired to a ClusterBean, which is used for
		// accessing a Cassandra cluster. If the Client is not wired to a
		// ClusterBean, then look for one in the application context
		if (getClusterBean() == null) {

			LOG.debug(getBeanName()
					+ ":Cluster bean not injected, looking for one in context");

			// look for a cluster bean
			Map<String, ClusterBean> clusterBeans = getApplicationContext()
					.getBeansOfType(ClusterBean.class);

			// if there are none, throw exception
			if (clusterBeans == null || clusterBeans.size() == 0) {
				throw new Exception(getBeanName()
						+ " is not wired to a ClusterBean and "
						+ "one could not be found in the "
						+ "application context");
			}

			// if there are too many to choose from, throw exception
			if (clusterBeans.size() > 1) {
				throw new Exception(getBeanName()
						+ " is not wired to a ClusterBean and there are more "
						+ "than one to choose from in the application context");
			}

			// set the cluster bean to the one and only found in the application
			// context
			setClusterBean(clusterBeans.values().toArray(new ClusterBean[1])[0]);

		}

		// the client must pertain to a Cassandra keyspace
		if (getKeyspace() == null || getKeyspace().isEmpty()) {
			throw new Exception("client must be assigned a Cassandra keyspace");
		}

		setCluster(getClusterBean().getCluster());

		/*
		 * A session holds connections to a Cassandra cluster, allowing it to be
		 * queried. Each session maintains multiple connections to the cluster
		 * nodes, provides policies to choose which node to use for each query
		 * (round-robin on all nodes of the cluster by default), and handles
		 * retries for failed query (when it makes sense), etc... Session
		 * instances are thread-safe and usually a single instance is enough per
		 * application. As a given session can only be "logged" into one
		 * keyspace at a time (where the "logged" keyspace is the one used by
		 * query if the query doesn't explicitely use a fully qualified table
		 * name), it can make sense to create one session per keyspace used.
		 * This is however not necessary to query multiple keyspaces since it is
		 * always possible to use a single session with fully qualified table
		 * name in queries.
		 */
		try {
			// initialize the Cassandra session and put out information about it
			getSession();
			LOG.info(getBeanName() + ": cluster name = "
					+ getSession().getCluster().getMetadata().getClusterName());
			LOG.info(getBeanName()
					+ ": all clluster hosts = "
					+ getSession().getCluster().getMetadata().getAllHosts()
							.toString());
			LOG.info(getBeanName() + ": cluster partitioner = "
					+ getSession().getCluster().getMetadata().getPartitioner());
		} catch (Exception exc) {
			LOG.warn(getBeanName()
					+ ":unable to connect to Cassandra cluster during bean "
					+ "initialization, msg = " + exc.getMessage());
			// the session may become available at some later point in time
		}

		LOG.info(getBeanName() + ": clusterBean name = "
				+ getClusterBean().getBeanName());

		// if no CQLs have been injected into this bean, then auto-inject any
		// CQLs found in context
		if (getApplicationContext() != null) {
			if (getCqlStmnts4Select().isEmpty()
					&& getCqlStmnts4Delete().isEmpty()
					&& getCqlStmnts4Update().isEmpty()
					&& getCqlStmnts4Insert().isEmpty()) {
				LOG.debug(getBeanName()
						+ ":CQLs not injected, looking for them in context");
				try {
					Map<String, CqlStmnt> cqlBeans = getApplicationContext()
							.getBeansOfType(CqlStmnt.class);
					if (cqlBeans != null && cqlBeans.size() > 0) {
						setCqls(new ArrayList<CqlStmnt>(cqlBeans.values()));
					} else {
						LOG.error(getBeanName()
								+ ":ERROR: No CQLs found in context");
					}
				} catch (BeansException e) {
					LOG.error(getBeanName()
							+ ":ERROR: could not find any CQLStmnt beans when processing auto-injected beans: "
							+ e.getMessage());
					throw e;
				} catch (Exception e) {
					LOG.error(getBeanName()
							+ ":ERROR: this exception occurred when processing auto-injected CQLStmnt beans: "
							+ e.getMessage());
					// end of the line
					throw e;
				}
			}
		}

		// log the CQLs and determine the default method (if any)
		int defaultMethodFlag = 0;
		if (!getCqlStmnts4Select().isEmpty()) {
			if (LOG.isTraceEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Select()) {
					LOG.debug(getBeanName() + ": CQL for SELECT = "
							+ cqlstmnt.getStatement());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for SELECT = "
							+ cqlstmnt.getPreparedStr());
				}
			}
			defaultMethodFlag |= 1;
		}

		if (!getCqlStmnts4Insert().isEmpty()) {
			if (LOG.isTraceEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Insert()) {
					LOG.debug(getBeanName() + ": CQL for INSERT = "
							+ cqlstmnt.getStatement());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for INSERT = "
							+ cqlstmnt.getPreparedStr());
				}
			}
			// update the default method indicator
			defaultMethodFlag |= 2;
		}

		if (!getCqlStmnts4Update().isEmpty()) {
			if (LOG.isDebugEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Update()) {
					LOG.debug(getBeanName() + ": CQL for UPDATE = "
							+ cqlstmnt.getStatement());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for UPDATE = "
							+ cqlstmnt.getPreparedStr());
				}
			}
			// update the default method indicator
			defaultMethodFlag |= 4;
		}

		if (!getCqlStmnts4Delete().isEmpty()) {
			if (LOG.isDebugEnabled()) {
				for (CqlStmnt cqlstmnt : getCqlStmnts4Delete()) {
					LOG.debug(getBeanName() + ": CQL for DELETE = "
							+ cqlstmnt.getStatement());
					LOG.debug(getBeanName()
							+ ": Parameterized CQL for DELETE = "
							+ cqlstmnt.getPreparedStr());
				}
			}
			// update the default method indicator
			defaultMethodFlag |= 8;
		}

		// has this client got any CQL statements? If not and autoInject is
		// true, then throw an exception
		if (defaultMethodFlag == 0 && isAutoInject()) {
			throw new Exception(getBeanName()
					+ ": Client has not been assigned any CQL statements");
		}

		if (getDefaultMethod() == null) {
			// a default method was not injected, so determine the default
			// method based on the injected CQLs
			switch (defaultMethodFlag) {
			case 1:
				setDefaultMethod(Method.SELECT);
				break;
			case 2:
				setDefaultMethod(Method.INSERT);
				break;
			case 4:
				setDefaultMethod(Method.UPDATE);
				break;
			case 8:
				setDefaultMethod(Method.DELETE);
				break;
			default:
				LOG.warn(getBeanName()
						+ ": Couldn't auto-determine default method");
			}
		}

		if (getDefaultMethod() != null) {
			LOG.debug(getBeanName() + ": Default method = "
					+ getDefaultMethod());
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
		// TODO: needs work
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
				// payLoad = Utils.objToString((Object) payLoad);
				listMap = (List) payLoad;
			} else if (payLoad instanceof Map) {
				LOG.trace(getBeanName()
						+ ":camelProcess: received payload as Map with "
						+ "size = {}", ((Map) payLoad).size());
				// ensure all values in key-value pairs are Strings
				// payLoad = Utils.objToString((Object) payLoad);
				listMap = new ArrayList<Map<Object, Object>>();
				listMap.add((Map) payLoad);
			} else {
				throw new Exception(getBeanName()
						+ ":camelProcess: received payload as neither a "
						+ "List nor Map");
			}
			// ensure that all the Maps in the given list have the same set of
			// keys. Potential quadratic running time ( O(n^2) ), but the number
			// of keys in a Map is not expected to be that large
			if (listMap.size() > 1) {
				for (int i = 0; i < listMap.size(); i++) {
					Set set1 = listMap.get(i).keySet();
					for (int j = i + 1; j < listMap.size(); j++) {
						if (!listMap.get(j).keySet().equals(set1)) {
							throw new Exception(getBeanName()
									+ ":camelProcess:ERROR, all Maps in the "
									+ "provided List of Maps do not have the "
									+ "same key set!");
						}

					}
				}
			}
		} else {
			LOG.trace(getBeanName() + ":camelProcess: payload was not provided");
		}

		// execute the Map(s) and hoist the returned List of Maps up into the
		// Exchange's out message
		exchange.getOut().setBody(execute(listMap, inMsg));
		// if requested to do so, save the current paging state
		if (inMsg.getHeader(CASSANDRA_PAGING_STATE) != null) {
			exchange.getOut().setHeader(CASSANDRA_PAGING_STATE,
					inMsg.getHeader(CASSANDRA_PAGING_STATE));
		}
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
		Method method = null;
		String inMethod = (String) inMsg.getHeader(CASSANDRA_METHOD);
		if (inMethod == null || inMethod.isEmpty()) {
			LOG.debug(getBeanName() + ":execute - method was not provided");
			method = getDefaultMethod();
			if (method == null) {
				throw new Exception(getBeanName()
						+ ":execute - method was not provided and there "
						+ "is no default set for this client");
			} else {
				LOG.debug(getBeanName()
						+ ":execute - using this default method {}", method);
			}
		} else {
			inMethod = inMethod.trim();
			try {
				method = Method.valueOf(inMethod.toUpperCase());
				LOG.debug(getBeanName()
						+ ":execute - using this passed in method {}", inMethod);
			} catch (IllegalArgumentException e) {
				throw new Exception(getBeanName()
						+ ":execute: This method is not allowed [" + method
						+ "]");
			}
		}

		// based on the given method, attempt to find a set of candidate CQL
		// statements
		List<CqlStmnt> cqlStmnts = getCqlStmnts(method);
		if (cqlStmnts == null || cqlStmnts.isEmpty()) {
			throw new Exception(getBeanName() + ":execute - could not acquire "
					+ "CQL statements for this method:" + method.toString());
		}

		// if no list of maps was passed in, then dummy one up
		List<Map<Object, Object>> myListMap = new ArrayList<Map<Object, Object>>();
		if (listMap == null) {
			LOG.debug(getBeanName() + ":execute - listMap was not provided");
			myListMap.add(new HashMap<Object, Object>());
		} else {
			// create a copy of the given list.
			for (Map<Object, Object> map : listMap) {
				myListMap.add(new HashMap<Object, Object>(map));
			}
		}

		LOG.debug(getBeanName() + ":execute - executing this many maps {}",
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

		// only one map is allowed for SELECT statements
		if (cqlStmnt.isSelect() && myListMap.size() > 1) {
			throw new Exception(getBeanName()
					+ ":execute: received more than one input "
					+ "Map for a SELECT statement, this is not allowed");
		}

		// determine the page size
		int fetchSize = (cqlStmnt.getFetchSize() >= 0) ? cqlStmnt
				.getFetchSize() : getSession().getCluster().getConfiguration()
				.getQueryOptions().getFetchSize();

		// iterate through the given Maps (if any) and execute their
		// corresponding cql statement(s)
		try {
			List<ResultSet> resultSets = new ArrayList<ResultSet>();
			for (Map map : myListMap) {
				ResultSet resultSet = cqlStmnt
						.execute(map, inMsg, getSession());
				if (resultSet != null) {
					resultSets.add(resultSet);
				}
			} // for (Map map : listMap)

			if (LOG.isDebugEnabled()) {
				LOG.debug(getBeanName()
						+ ":execute: successfully executed statement(s)");
				LOG.debug(getBeanName()
						+ ":execute: received this many result sets {}",
						resultSets.size());
				LOG.debug(getBeanName() + ":execute: using this fetchSize {}",
						fetchSize);
			}

			// if no result sets were returned, then we're done!
			if (resultSets.isEmpty()) {
				return null;
			}

			List<Map<String, Object>> listOutMaps = new ArrayList<Map<String, Object>>();

			// iterate through the returned result sets
			for (ResultSet resultSet : resultSets) {
				Row row = null;
				// grab the metadata for the result set
				ColumnDefinitions cDefs = resultSet.getColumnDefinitions();
				// transfer each row of the result set to a Map and place all
				// the maps in a List
				int rowCount = 0;
				while ((row = resultSet.one()) != null && rowCount < fetchSize) {
					Map<String, Object> map = new HashMap<String, Object>();
					for (Definition cDef : cDefs.asList()) {
						map.put(cDef.getName(), CqlToken.getObjectFromRow(row,
								cDef.getName(), cDef.getType().getName()));
					}
					listOutMaps.add(map);
					rowCount++;
				}
			}
			// return the List of Maps up into the Exchange's out message
			LOG.debug(getBeanName()
					+ ":camelProcess: sending back this many Maps {}",
					listOutMaps.size());

			return listOutMaps;

		} catch (Exception exc) {
			LOG.error(getBeanName() + ":ERROR, caught this "
					+ "Exception while executing CQL statement " + "message: "
					+ exc.toString());
			LOG.error(getBeanName() + ": exception stack trace follows:");
			exc.printStackTrace();
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
			LOG.warn(getBeanName() + ":getCqlStmnts: provided method is null");
			return null;
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
	 * 
	 * A session holds connections to a Cassandra cluster, allowing it to be
	 * queried. Each session maintains multiple connections to the cluster
	 * nodes, provides policies to choose which node to use for each query
	 * (round-robin on all nodes of the cluster by default), and handles retries
	 * for failed queries (when it makes sense), etc...
	 * 
	 * Session instances are thread-safe and usually a single instance is enough
	 * per application. As a given session can only be "logged" into one
	 * keyspace at a time (where the "logged" keyspace is the one used by
	 * queries that don't explicitly use a fully qualified table name), it can
	 * make sense to create one session per keyspace used. This is however not
	 * necessary when querying multiple keyspaces since it is always possible to
	 * use a single session with fully qualified table names in queries.
	 * 
	 * @return the Cassandra session
	 */
	public Session getSession() throws Exception {

		if (getCluster().isClosed()) {
			throw new Exception(this.getBeanName()
					+ ":getSession: cluster bean has been closed");
		}

		// wait to acquire the session lock (default wait time is 10 seconds).
		if (!sessionLock.tryLock(getSessionLockWaitTime(),
				TimeUnit.MILLISECONDS)) {
			throw new Exception(
					this.getBeanName()
							+ ":getSession: timed out attempting to acquire Cassandra session");
		}

		try {
			// session may have already existed
			if (session != null && !session.isClosed()) {
				return session;
			} else if (session != null) {
				throw new Exception(this.getBeanName()
						+ ":getSession: Cassandra session has been closed");
			}

			// session does not exist, so create one
			try {
				session = getCluster().connect(getKeyspace());
			} catch (NoHostAvailableException exc) {
				LOG.error(getBeanName()
						+ ":unable to connect Cassandra during bean initialization, msg = "
						+ exc.getMessage());
				throw exc;
			}
		} finally {
			sessionLock.unlock();
		}
		return session;
	}

	@Override
	public void setBeanName(String name) {
		beanName = name;
	}

	public String getBeanName() {
		return beanName;
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
	public void setCqls(List<CqlStmnt> cqls) throws Exception {
		if (cqls == null || cqls.isEmpty()) {
			return;
		}
		// make sure all the CQL statements are unique within a given set
		for (CqlStmnt cql : cqls) {
			switch (cql.getCqlStmntType()) {
			case SELECT:
				if (cql.isEqual(getCqlStmnts4Select())) {
					throw new Exception(
							"Injected CQL statements for SELECT are not distinct");
				}
				cqlStmnts4Select.add(cql);
				break;
			case DELETE:
				if (cql.isEqual(getCqlStmnts4Delete())) {
					throw new Exception(
							"Injected CQL statements for DELETE are not distinct");
				}
				cqlStmnts4Delete.add(cql);
				break;
			case INSERT:
				if (cql.isEqual(getCqlStmnts4Insert())) {
					throw new Exception(
							"Injected CQL statements for INSERT are not distinct");
				}
				cqlStmnts4Insert.add(cql);
				break;
			case UPDATE:
				if (cql.isEqual(getCqlStmnts4Update())) {
					throw new Exception(
							"Injected CQL statements for UPDATE are not distinct");
				}
				cqlStmnts4Update.add(cql);
				break;
			default:
				throw new Exception("Unknown CQL = [" + cql.getStatement()
						+ "]");
			}
		}
	}

	/**
	 * @param dfltMethod
	 *            the dfltMethod to set
	 */
	public void setDefaultMethod(Method defaultMethod) {
		this.defaultMethod = defaultMethod;
	}

	/**
	 * @return the defaultMethodStr
	 */
	public Method getDefaultMethod() {
		return defaultMethod;
	}

	/**
	 * @return the applicationContext
	 */
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	/**
	 * @param applicationContext
	 *            the applicationContext to set
	 */
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * @return the autoInject
	 */
	public boolean isAutoInject() {
		return autoInject;
	}

	/**
	 * @param autoInject
	 *            the autoInject to set
	 */
	public void setAutoInject(boolean autoInject) {
		this.autoInject = autoInject;
	}

	public boolean isNoop() {
		return getDefaultMethod().isNoop();
	}

	/**
	 * @return the sessionLockWaitTime
	 */
	public long getSessionLockWaitTime() {
		return sessionLockWaitTime;
	}

	/**
	 * @param sessionLockWaitTime
	 *            the sessionLockWaitTime to set
	 */
	public void setSessionLockWaitTime(long sessionLockWaitTime) {
		this.sessionLockWaitTime = sessionLockWaitTime;
	}

}
