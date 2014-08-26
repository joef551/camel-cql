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

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;

public class CqlProducer extends DefaultProducer {

	private CqlEndpoint cassandraEndpoint = null;

	public CqlProducer(CqlEndpoint endpoint) {
		super(endpoint);
		cassandraEndpoint = endpoint;
	}

	// this is the method that is called to process an incoming message
	public void process(Exchange exchange) throws Exception {
		// tuck this producer's endpoint in the exchange
		exchange.setProperty(CqlComponent.CASSY_ENDPOINT_PROP,
				getCassandraEndpoint());
		// call into this endpoint's Client
		getCassandraEndpoint().getClient().process(exchange);
	}

	protected CqlEndpoint getCassandraEndpoint() {
		return cassandraEndpoint;
	}

}
