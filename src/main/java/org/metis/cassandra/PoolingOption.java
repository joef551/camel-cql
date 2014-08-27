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
	private int coreConnections = -1;
	private int maxConnections = -1;
	private int maxSimultaneousRequests = -1;
	private int minSimultaneousRequests = -1;
	private String distance;
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

	public String getDistance() {
		return distance;
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
	 * @param distance
	 * @throws IllegalArgumentException
	 */
	public void setDistance(String distance) throws IllegalArgumentException {
		if (distance == null || distance.isEmpty()) {
			throw new IllegalArgumentException("illegal distance string");
		}
		setHostDistance(HostDistance.valueOf(distance.toUpperCase()));
		this.distance = distance;
	}

	public HostDistance getHostDistance() {
		return hostDistance;
	}

	/**
	 * 
	 * @param hostDistance
	 */
	private void setHostDistance(HostDistance hostDistance) {
		this.hostDistance = hostDistance;
	}

	/**
	 * @return the coreConnections
	 */
	public int getCoreConnections() {
		return coreConnections;
	}

	/**
	 * Sets the core number of connections per host.
	 * 
	 * @param coreConnections
	 *            the coreConnections to set
	 */
	public void setCoreConnections(int coreConnections) {
		this.coreConnections = coreConnections;
	}

	/**
	 * @return the maxConnections
	 */
	public int getMaxConnections() {
		return maxConnections;
	}

	/**
	 * Sets the maximum number of connections per host.
	 * 
	 * @param maxConnections
	 *            the maxConnections to set
	 */
	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	/**
	 * @return the maxSimultaneousRequests
	 */
	public int getMaxSimultaneousRequests() {
		return maxSimultaneousRequests;
	}

	/**
	 * 
	 * Sets number of simultaneous requests on all connections to a host after
	 * which more connections are created.
	 * 
	 * @param maxSimultaneousRequests
	 *            the maxSimultaneousRequests to set
	 */
	public void setMaxSimultaneousRequests(int maxSimultaneousRequests) {
		this.maxSimultaneousRequests = maxSimultaneousRequests;
	}

	/**
	 * @return the minSimultaneousRequests
	 */
	public int getMinSimultaneousRequests() {
		return minSimultaneousRequests;
	}

	/**
	 * @param minSimultaneousRequests
	 *            the minSimultaneousRequests to set
	 */
	public void setMinSimultaneousRequests(int minSimultaneousRequests) {
		this.minSimultaneousRequests = minSimultaneousRequests;
	}

}
