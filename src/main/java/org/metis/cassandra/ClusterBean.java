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
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.AddressTranslater;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.NettyOptions;
import static com.datastax.driver.core.NettyOptions.DEFAULT_INSTANCE;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

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
	private String clusterNodes;
	private NettyOptions nettyOptions = DEFAULT_INSTANCE;
	private SpeculativeExecutionPolicy speculativeExecutionPolicy;

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
		if (this.getMetricsOptions() == null) {
			setMetricsOptions(new MetricsOptions());
		}

		// 2. Set any injected Policies. Those not injected will be defaulted.
		setPolicies(new Policies(
				(getLoadBalancingPolicy() != null ? getLoadBalancingPolicy()
						: Policies.defaultLoadBalancingPolicy()),
				(getReconnectionPolicy() != null ? getReconnectionPolicy()
						: Policies.defaultReconnectionPolicy()),
				(getRetryPolicy() != null ? getRetryPolicy() : Policies
						.defaultRetryPolicy()),
				(getAddressTranslater() != null ? getAddressTranslater()
						: Policies.defaultAddressTranslater()),
				(getSpeculativeExecutionPolicy() != null ? getSpeculativeExecutionPolicy()
						: Policies.defaultSpeculativeExecutionPolicy())));

		// 3. Set the protocol options. Can be injected via Spring, and if not a
		// default is used.
		if (getProtocolOptions() == null) {
			setProtocolOptions(new ProtocolOptions());
		}

		// 4. Set the pooling options
		if (getPoolingOptions() == null) {
			setPoolingOptions(new PoolingOptions());
		}
		if (getListOfPoolingOptions() != null) {
			for (PoolingOption po : getListOfPoolingOptions()) {
				if (po.getHostDistance() != HostDistance.IGNORED) {
					if (po.getCoreConnectionsPerHost() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setCoreConnectionsPerHost(
										po.getHostDistance(),
										po.getCoreConnectionsPerHost()));
					}
					if (po.getMaxConnectionsPerHost() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMaxConnectionsPerHost(po.getHostDistance(),
										po.getMaxConnectionsPerHost()));
					}
					if (po.getMaxSimultaneousRequestsPerHostThreshold() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMaxSimultaneousRequestsPerHostThreshold(
										po.getHostDistance(),
										po.getMaxSimultaneousRequestsPerHostThreshold()));
					}
					if (po.getMaxSimultaneousRequestsPerConnectionThreshold() >= 0) {
						setPoolingOptions(getPoolingOptions()
								.setMaxSimultaneousRequestsPerConnectionThreshold(
										po.getHostDistance(),
										po.getMaxSimultaneousRequestsPerConnectionThreshold()));
					}
				}
			}
		}

		// 5. Set the socket options
		if (getSocketOptions() == null) {
			setSocketOptions(new SocketOptions());
		}

		// 6. Set the query options
		if (getQueryOptions() == null) {
			setQueryOptions(new QueryOptions());
		}

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
			addrs.add(new InetSocketAddress(node.trim(), this
					.getProtocolOptions().getPort()));
		}
		setContactPoints(addrs);

		// 8. Set the configuration
		setConfiguration(new Configuration(getPolicies(), getProtocolOptions(),
				getPoolingOptions(), getSocketOptions(), getMetricsOptions(),
				getQueryOptions(), getNettyOptions()));

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

	/**
	 * @return the nettyOptions
	 */
	public NettyOptions getNettyOptions() {
		return nettyOptions;
	}

	/**
	 * @param nettyOptions
	 *            the nettyOptions to set
	 */
	public void setNettyOptions(NettyOptions nettyOptions) {
		this.nettyOptions = nettyOptions;
	}

	/**
	 * @return the speculativeExecutionPolicy
	 */
	public SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
		return speculativeExecutionPolicy;
	}

	/**
	 * @param speculativeExecutionPolicy
	 *            the speculativeExecutionPolicy to set
	 */
	public void setSpeculativeExecutionPolicy(
			SpeculativeExecutionPolicy speculativeExecutionPolicy) {
		this.speculativeExecutionPolicy = speculativeExecutionPolicy;
	}

}
