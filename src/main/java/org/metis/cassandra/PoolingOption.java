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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;

import com.datastax.driver.core.HostDistance;

public class PoolingOption implements InitializingBean, BeanNameAware,
		DisposableBean {

	private static final Logger LOG = LoggerFactory
			.getLogger(PoolingOption.class);
	private String beanName;

	private int coreConnectionsPerHost = -1;
	private int maxConnectionsPerHost = -1;
	private int newConnectionThreshold = -1;
	private int maxRequestsPerConnection = -1;
	private HostDistance hostDistance = HostDistance.IGNORED;

	public PoolingOption() {
		super();
	}

	public void afterPropertiesSet() throws Exception {
		if (this.getHostDistance() == HostDistance.IGNORED) {
			LOG.warn("this PoolingOption bean {} is being ignored",
					this.getBeanName());
		}
	}

	public void destroy() {
	}

	public String getBeanName() {
		return beanName;
	}

	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	public HostDistance getHostDistance() {
		return hostDistance;
	}

	/**
	 * The distance to a Cassandra node as assigned by a LoadBalancingPolicy
	 * (through its distance method). The distance assigned to a host influences
	 * how many connections the driver maintains towards this host. If for a
	 * given host the assigned HostDistance is LOCAL or REMOTE, some connections
	 * will be maintained by the driver to this host. More active connections
	 * will be kept to LOCAL host than to a REMOTE one (and thus well behaving
	 * LoadBalancingPolicy should assign a REMOTE distance only to hosts that
	 * are the less often queried). However, if a host is assigned the distance
	 * IGNORED, no connection to that host will be maintained active. In other
	 * words, IGNORED should be assigned to hosts that should not be used by
	 * this driver (because they are in a remote data center for instance).
	 * 
	 * @param hostDistance
	 */
	public void setHostDistance(HostDistance hostDistance) {
		this.hostDistance = hostDistance;
	}

	/**
	 * @return the coreConnectionsPerHost
	 */
	public int getCoreConnectionsPerHost() {
		return coreConnectionsPerHost;
	}

	/**
	 * Sets the core number of connections per host.
	 * 
	 * @param coreConnectionsPerHost
	 *            the coreConnectionsPerHost to set
	 */
	public void setCoreConnectionsPerHost(int coreConnections) {
		this.coreConnectionsPerHost = coreConnections;
	}

	/**
	 * @return the maxConnectionsPerHost
	 */
	public int getMaxConnectionsPerHost() {
		return maxConnectionsPerHost;
	}

	/**
	 * Sets the maximum number of connections per host.
	 * 
	 * @param maxConnectionsPerHost
	 *            the maxConnectionsPerHost to set
	 */
	public void setMaxConnectionsPerHost(int maxConnections) {
		this.maxConnectionsPerHost = maxConnections;
	}

	/**
	 * @return the newConnectionThreshold
	 */
	public int getNewConnectionThreshold() {
		return newConnectionThreshold;
	}

	/**
	 * @param newConnectionThreshold
	 *            the newConnectionThreshold to set
	 */
	public void setNewConnectionThreshold(int newConnectionThreshold) {
		this.newConnectionThreshold = newConnectionThreshold;
	}

	/**
	 * @return the maxRequestsPerConnection
	 */
	public int getMaxRequestsPerConnection() {
		return maxRequestsPerConnection;
	}

	/**
	 * @param maxRequestsPerConnection
	 *            the maxRequestsPerConnection to set
	 */
	public void setMaxRequestsPerConnection(int maxRequestsPerConnection) {
		this.maxRequestsPerConnection = maxRequestsPerConnection;
	}

}
