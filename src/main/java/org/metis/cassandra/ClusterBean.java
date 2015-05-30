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
import java.util.Collection;
import java.net.InetSocketAddress;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.AddressTranslater;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;

/**
 * The ClusterBean is used for configuring an instance of the Cassandra Java
 * driver, which is then used for accessing a Cassandra cluster. You wire this
 * object to one or more Cassandra clients.
 * 
 * @author jfernandez
 * 
 */
public class ClusterBean implements InitializingBean, BeanNameAware,
		DisposableBean, Initializer {

	private String beanName;
	private String clusterName;
	private Cluster cluster;
	private Configuration configuration;
	private List<InetSocketAddress> contactPoints;
	private Collection<StateListener> initialListeners = new ArrayList<StateListener>();
	private MetricsOptions metricsOptions;
	private Policies policies;
	private LoadBalancingPolicy loadBalancingPolicy;
	private AddressTranslater addressTranslater;
	private ReconnectionPolicy reconnectionPolicy;
	private RetryPolicy retryPolicy;
	private PoolingOptions poolingOptions;
	private List<PoolingOption> listOfPoolingOptions;
	private ProtocolOptions protocolOptions;
	private SocketOptions socketOptions;
	private QueryOptions queryOptions;
	private ConsistencyLevel consistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL;
	private ConsistencyLevel serialConsistencyLevel = QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL;
	private Compression compressionLevel = Compression.NONE;
	private int fetchSize = QueryOptions.DEFAULT_FETCH_SIZE;
	private int port = ProtocolOptions.DEFAULT_PORT;
	private int protocolVersion = -1;
	private boolean jmxReportingEnabled;
	private String consistency;
	private String serialConsistency;
	private String compression = Compression.NONE.toString();
	private String clusterNodes;
	

	public ClusterBean() {
	}

	/**
	 * Create a Cluster from all the properties that were specified.
	 */
	public void afterPropertiesSet() throws Exception {

		if (getClusterName() == null) {
			setClusterName(getBeanName());
		}

		// 1. Set the MetricsOptions
		// See http://www.datastax.com/drivers/java/2.1/index.html
		setMetricsOptions(new MetricsOptions(isJmxReportingEnabled()));

		// 2. Set any injected Policies. Those not injected will be defaulted.
		setPolicies(new Policies(
				(getLoadBalancingPolicy() != null ? getLoadBalancingPolicy()
						: Policies.defaultLoadBalancingPolicy()),
				(getReconnectionPolicy() != null ? getReconnectionPolicy()
						: Policies.defaultReconnectionPolicy()),
				(getRetryPolicy() != null ? getRetryPolicy() : Policies
						.defaultRetryPolicy()),
				(getAddressTranslater() != null ? getAddressTranslater()
						: Policies.defaultAddressTranslater())));

		// 3. Set the protocol options. Can be injected via Spring, and if not a
		// default is used.
		if (getProtocolOptions() == null) {
			setProtocolOptions(new ProtocolOptions(getPort()));
		}
		setProtocolOptions(getProtocolOptions().setCompression(
				getCompressionLevel()));
		
		//getProtocolOptions().

		// 4. Set the pooling options
		setPoolingOptions(new PoolingOptions());
		if (getListOfPoolingOptions() != null) {
			for (PoolingOption po : getListOfPoolingOptions()) {
				if (po.getHostDistance() != HostDistance.IGNORED) {
					if (po.getMinSimultaneousRequests() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMinSimultaneousRequestsPerConnectionThreshold(
										po.getHostDistance(),
										po.getMinSimultaneousRequests()));
					}
					if (po.getCoreConnections() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setCoreConnectionsPerHost(
										po.getHostDistance(),
										po.getCoreConnections()));
					}
					if (po.getMaxConnections() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMaxConnectionsPerHost(po.getHostDistance(),
										po.getMaxConnections()));
					}
					if (po.getMaxSimultaneousRequests() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMaxSimultaneousRequestsPerConnectionThreshold(
										po.getHostDistance(),
										po.getMaxSimultaneousRequests()));
					}

				}

			}
		}

		// 5. Set the socket options
		if (getSocketOptions() == null) {
			setSocketOptions(new SocketOptions());
		}

		// 6. Set the QueryOptions
		setQueryOptions(new QueryOptions()
				.setConsistencyLevel(getConsistencyLevel())
				.setSerialConsistencyLevel(getSerialConsistencyLevel())
				.setFetchSize(getFetchSize()));

		// 7. Set the contact points, which are a comma-separated list of host
		// addrs with optional port. Note that all contact points provided by
		// this initializer must share the same port number.
		if (getClusterNodes() == null || getClusterNodes().isEmpty()) {
			throw new Exception("clusterNodes has not been set");
		}
		String[] nodes = getClusterNodes().trim().split(",");
		if (nodes == null || nodes.length == 0) {
			throw new Exception("invalid clusterNodes string: "
					+ getClusterNodes().trim());
		}
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		for (String node : nodes) {
			addrs.add(new InetSocketAddress(node.trim(), getPort()));
		}
		setContactPoints(addrs);

		// 8. Set the configuration
		setConfiguration(new Configuration(getPolicies(), getProtocolOptions(),
				getPoolingOptions(), getSocketOptions(), getMetricsOptions(),
				getQueryOptions()));

		// 9. Now create a Cluster from all that has been gathered
		setCluster(Cluster.buildFrom(this));

	}

	public void destroy() {
		if (getCluster() != null) {
			getCluster().close();
		}
	}

	public String getBeanName() {
		return beanName;
	}

	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public List<InetSocketAddress> getContactPoints() {
		return contactPoints;
	}

	public void setContactPoints(List<InetSocketAddress> contactPoints) {
		this.contactPoints = contactPoints;
	}

	public Collection<StateListener> getInitialListeners() {
		return initialListeners;
	}

	public void setInitialListeners(Collection<StateListener> initialListeners) {
		this.initialListeners = initialListeners;
	}

	public MetricsOptions getMetricsOptions() {
		return metricsOptions;
	}

	public void setMetricsOptions(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
	}

	public boolean isJmxReportingEnabled() {
		return jmxReportingEnabled;
	}

	public void setJmxReportingEnabled(boolean jmxEnabled) {
		jmxReportingEnabled = jmxEnabled;
	}

	public Policies getPolicies() {
		return policies;
	}

	public void setPolicies(Policies policies) {
		this.policies = policies;
	}

	public PoolingOptions getPoolingOptions() {
		return poolingOptions;
	}

	public void setPoolingOptions(PoolingOptions poolingOptions) {
		this.poolingOptions = poolingOptions;
	}

	/**
	 * @return the consistencyLevel
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * @param consistencyLevel
	 *            the consistencyLevel to set
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the consistency
	 */
	public String getConsistency() {
		return consistency;
	}

	/**
	 * @param consistency
	 *            the consistency to set
	 */
	public void setConsistency(String consistency)
			throws IllegalArgumentException {

		if (consistency == null || consistency.isEmpty()) {
			throw new IllegalArgumentException("illegal consistency level");
		}
		setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
		this.consistency = consistency;
	}

	/**
	 * @return the serialConsistencyLevel
	 */
	public ConsistencyLevel getSerialConsistencyLevel() {
		return serialConsistencyLevel;
	}

	/**
	 * @param serialConsistencyLevel
	 *            the serialConsistencyLevel to set
	 */
	public void setSerialConsistencyLevel(
			ConsistencyLevel serialConsistencyLevel) {
		this.serialConsistencyLevel = serialConsistencyLevel;
	}

	/**
	 * @return the serialConsistency
	 */
	public String getSerialConsistency() {
		return serialConsistency;
	}

	/**
	 * @param serialConsistency
	 *            the serialConsistency to set
	 */
	public void setSerialConsistency(String consistency)
			throws IllegalArgumentException {

		if (consistency == null || consistency.isEmpty()) {
			throw new IllegalArgumentException(
					"illegal serial consistency level");
		}
		setSerialConsistencyLevel(ConsistencyLevel.valueOf(consistency));
		this.serialConsistency = consistency;
	}

	/**
	 * @return the fetchSize
	 */
	public int getFetchSize() {
		return fetchSize;
	}

	/**
	 * @param fetchSize
	 *            the fetchSize to set
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * This is in the protocol options
	 * 
	 * @return the default port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port
	 *            the default port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the protocolVersion
	 */
	public int getProtocolVersion() {
		return protocolVersion;
	}

	/**
	 * This can be a negative number, in which case the version used will be the
	 * biggest version supported by the first node the driver connects to.
	 * Otherwise, it must be either 1, 2 or 3 to force using a particular protocol
	 * version
	 * 
	 * @param protocolVersion
	 *            the protocolVersion to set
	 */
	public void setProtocolVersion(int protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	/**
	 * @return the compression
	 */
	public String getCompression() {
		return compression;
	}

	/**
	 * @param compression
	 *            the compression to set
	 */
	public void setCompression(String compression)
			throws IllegalArgumentException {
		if (compression == null || compression.isEmpty()) {
			throw new IllegalArgumentException("invalid compression string");
		}
		setCompressionLevel(Compression.valueOf(compression.toUpperCase()));
		this.compression = compression;
	}

	/**
	 * @return the compressionLevel
	 */
	public Compression getCompressionLevel() {
		return compressionLevel;
	}

	/**
	 * @param compressionLevel
	 *            the compressionLevel to set
	 */
	public void setCompressionLevel(Compression compressionLevel) {
		this.compressionLevel = compressionLevel;
	}

	/**
	 * @return the protocolOptions
	 */
	public ProtocolOptions getProtocolOptions() {
		return protocolOptions;
	}

	/**
	 * @param protocolOptions
	 *            the protocolOptions to set
	 */
	public void setProtocolOptions(ProtocolOptions protocolOptions) {
		this.protocolOptions = protocolOptions;
	}

	/**
	 * @return the socketOptions
	 */
	public SocketOptions getSocketOptions() {
		return socketOptions;
	}

	/**
	 * @param socketOptions
	 *            the socketOptions to set
	 */
	public void setSocketOptions(SocketOptions socketOptions) {
		this.socketOptions = socketOptions;
	}

	/**
	 * @return the queryOptions
	 */
	public QueryOptions getQueryOptions() {
		return queryOptions;
	}

	/**
	 * @param queryOptions
	 *            the queryOptions to set
	 */
	public void setQueryOptions(QueryOptions queryOptions) {
		this.queryOptions = queryOptions;
	}

	/**
	 * @return the clusterNodes
	 */
	public String getClusterNodes() {
		return clusterNodes;
	}

	/**
	 * @param clusterNodes
	 *            the clusterNodes to set
	 */
	public void setClusterNodes(String clusterNodes) {
		this.clusterNodes = clusterNodes;
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
	 * @return the reconnectionPolicy
	 */
	public ReconnectionPolicy getReconnectionPolicy() {
		return reconnectionPolicy;
	}

	/**
	 * @param reconnectionPolicy
	 *            the reconnectionPolicy to set
	 */
	public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	/**
	 * @return the loadBalancingPolicy
	 */
	public LoadBalancingPolicy getLoadBalancingPolicy() {
		return loadBalancingPolicy;
	}

	/**
	 * @param loadBalancingPolicy
	 *            the loadBalancingPolicy to set
	 */
	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	/**
	 * @return the addressTranslater
	 */
	public AddressTranslater getAddressTranslater() {
		return addressTranslater;
	}

	/**
	 * @param addressTranslater
	 *            the addressTranslater to set
	 */
	public void setAddressTranslater(AddressTranslater addressTranslater) {
		this.addressTranslater = addressTranslater;
	}

	/**
	 * @return the retryPolicy
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * @param retryPolicy
	 *            the retryPolicy to set
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the listOfPoolingOptions
	 */
	public List<PoolingOption> getListOfPoolingOptions() {
		return listOfPoolingOptions;
	}

	/**
	 * @param listOfPoolingOptions
	 *            the listOfPoolingOptions to set
	 */
	public void setListOfPoolingOptions(List<PoolingOption> listOfPoolingOptions) {
		this.listOfPoolingOptions = listOfPoolingOptions;
	}

}
