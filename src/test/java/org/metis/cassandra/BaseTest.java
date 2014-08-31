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

import org.apache.camel.test.junit4.CamelTestSupport;

/**
 * All Cassandra JUnit test classes should extend this base class
 * 
 * @author jfernandez
 * 
 */
public class BaseTest extends CamelTestSupport {

	public BaseTest() {
	}

	/**
	 * Override this method and return true to inform the Camel test-kit that it
	 * should only create CamelContext once (per class), so we will re-use the
	 * CamelContext between each test method in this class
	 */
	@Override
	public boolean isCreateCamelContextPerClass() {
		return true;
	}

}
