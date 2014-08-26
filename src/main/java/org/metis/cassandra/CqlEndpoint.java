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

import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * Represents a cassandra endpoint.
 */
public class CqlEndpoint extends DefaultEndpoint {

	// the optional URI parameters
	private Map<String, Object> parameters = null;
	private String contextPath = null;
	private Client client = null;

	public CqlEndpoint(String uri, CqlComponent component,
			Map<String, Object> parameters) {
		super(uri, component);
		this.parameters = parameters;
	}

	public CqlEndpoint(String uri, String contextPath,
			CqlComponent component, Map<String, Object> parameters,
			Client client) {
		this(uri, component, parameters);
		this.contextPath = contextPath;
		this.client = client;
	}

	/**
	 * Create and return a new Producer bound to this CqlEndpoint.
	 */
	public Producer createProducer() throws Exception {
		return new CqlProducer(this);
	}

	/**
	 * Consumers are currently not supported.
	 * 
	 * TODO: Implement polling consumer that fires off events when database
	 * change occurs
	 */
	public Consumer createConsumer(Processor processor) throws Exception {
		throw new UnsupportedOperationException(
				"You cannot receive messages from this endpoint:"
						+ getEndpointUri());
	}

	public boolean isSingleton() {
		return true;
	}

	@Override
	public boolean isSynchronous() {
		return true;
	}

	/**
	 * Must allow unknown parameters, as we do not want corresponding setter and
	 * getter methods.
	 */
	@Override
	public boolean isLenientProperties() {
		return true;
	}

	public String getContextPath() {
		return this.contextPath;
	}

	public Client getClient() {
		return client;
	}

	public Object getParameter(String key) {
		if (parameters == null) {
			return null;
		}
		return parameters.get(key);
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}
}
