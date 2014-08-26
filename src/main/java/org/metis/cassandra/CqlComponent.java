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

import java.io.File;
import java.util.Map;

import static org.metis.utils.Constants.*;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Represents the component that manages {@link CqlEndpoint}.
 */
public class CqlComponent extends DefaultComponent {

	/**
	 * This component's URL scheme. TODO: maybe it should be "cql"?
	 */
	public static final String CASSY_SCHEME = "cassandra:";

	private static final Logger LOG = LoggerFactory
			.getLogger(CqlComponent.class);

	// This is the system property used for overriding the default name of the
	// Spring application context
	private static final String CASSY_CONTEXT_PROP = "metis.cassandra.spring.context";

	// This system property is used for enabling the background thread that
	// monitors the Spring application context file.
	private static final String CASSY_CONTEXT_MON = "metis.cassandra.context.monitor";

	// the default setting for starting the context monitor
	private static boolean dfltStartMonitor = false;

	// the 'start monitor' setting for this instance of the CqlComponent
	private Boolean startMonitor;

	// The default name of the Spring application context file.
	public static final String CASSY_DFLT_CONTEXT = "cassandra.xml";

	// used for tucking the endpoint into an exchange
	protected static final String CASSY_ENDPOINT_PROP = "org.metis.cassandra.endpoint";

	// The default CASSY context file name, which can be overridden
	// via a system property
	private static String dfltContextFileName = null;

	// The CASSY context file name used by this instance of the
	// component
	private String myContextFileName = null;

	private ComponentProfile componentProfile = null;

	private Map<String, Client> clients;

	static {
		// check to see if the default context name has been
		// overridden
		dfltContextFileName = System.getProperty(CASSY_CONTEXT_PROP);
		// if not, then use the default name
		if (dfltContextFileName == null) {
			dfltContextFileName = CASSY_DFLT_CONTEXT;
		}
		// see if the monitor is supposed to start
		if (System.getProperty(CASSY_CONTEXT_MON) != null) {
			dfltStartMonitor = true;
		}
	}

	/**
	 * This constructor is implicitly called by Camel core. For example:
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * CamelContext context = new DefaultCamelContext();
	 * context.addRoutes(new RouteBuilder() {
	 *   public void configure() {
	 *     from("test-jms:queue:test.queue").to("cassandra://uri");
	 *   }
	 * });
	 * </blockquote>
	 * </pre>
	 */
	public CqlComponent() {
	}

	public CqlComponent(CamelContext context) {
		super(context);
	}

	/**
	 * Static builder method. The method allows the end-user to explicitly add a
	 * CqlComponent to a camel context. For example,
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * CamelContext context = new DefaultCamelContext();
	 * context.addComponent("cql",CqlComponent.cqlComponent());
	 * </blockquote>
	 * </pre>
	 * 
	 * @return CqlComponent
	 */
	public static CqlComponent cqlComponent() {
		return new CqlComponent();
	}

	/**
	 * Static builder method. The method allows the end-user to explicitly add a
	 * CqlComponent to a camel context. For example,
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * CamelContext context = new DefaultCamelContext();
	 * context.addComponent("cql",CqlComponent.cqlComponent("cassandra.xml"));
	 * </blockquote>
	 * </pre>
	 * 
	 * @param contextFileName
	 *            the name of this component's Spring XML context file, which
	 *            must be in the classpath.
	 * @return CqlComponent
	 */
	public static CqlComponent cqlComponent(String contextFileName) {
		CqlComponent cc = new CqlComponent();
		cc.setContextFileName(contextFileName);
		return cc;
	}

	/**
	 * Set the name of the Spring XML context file for this component. The
	 * context file, which must be in the classpath, holds the Client beans and
	 * must include Cassandra ClusterBeans.
	 * 
	 * This method is called by a static builder method or whenever this
	 * component is configured via a Spring XML file.
	 * 
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * <bean id="cassandra" class="org.metis.cassandra.CqlComponent"> <property
	 * name="contextFileName" value="cassandra.xml"/> </bean>
	 * 
	 * </blockquote>
	 * </pre>
	 * 
	 * An end-user can stop this component, update this property, and then
	 * re-start it. When re-started, the component will re-initialize itself
	 * with this new context file.
	 * 
	 * @param contextFileName
	 *            the name of this component's Spring XML file, which must be in
	 *            the classpath
	 */
	public void setContextFileName(String contextFileName) {
		myContextFileName = contextFileName;
	}

	/**
	 * Return the Spring context file name being used by this instance of the
	 * CqlComponent
	 * 
	 * @return String file name
	 */
	public String getContextFileName() {
		return myContextFileName;
	}

	/**
	 * Return a file handle to the Spring context file currently being used by
	 * this instance of the CqlComponent. Note that this method returns
	 * null if the Component has never been started.
	 * 
	 * @return String file name
	 */
	public File getContextFile() {
		return (getComponentProfile() == null ? null : getComponentProfile()
				.getContextFile());
	}

	/**
	 * Used for specifying whether to monitor this component's Spring context
	 * file. The context file, which must be in the classpath, holds the RDB
	 * mapper bean, RDBs and Cassandra Java Driver.
	 * 
	 * This method is called whenever this component is configured via a Spring
	 * XML file. Within the Spring XML file, the configuration property is set
	 * to 'true' or 'false'. Actually, anything other than 'true' will result in
	 * 'false'.
	 * 
	 * This method must be called prior to starting the component.
	 * 
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * <bean id="cassandra" class="org.metis.cassandra.CqlComponent"> <property
	 * name="startMonitor" value="true"/> </bean>
	 * 
	 * </blockquote>
	 * </pre>
	 * 
	 * @param setting
	 *            true or false
	 */
	public void setStartMonitor(String setting) {
		try {
			startMonitor = Boolean.valueOf(setting);
		} catch (Exception e) {
			LOG.error("setStartMonitor: invalid string = " + setting);
		}
	}

	/**
	 * Return the startMonitor Boolean, which indicates whether the component
	 * was instructed to monitor its Spring context file.
	 * 
	 * @return Boolean
	 */
	public Boolean getStartMonitor() {
		if (startMonitor == null) {
			startMonitor = new Boolean(dfltStartMonitor);
		}
		return startMonitor;
	}

	/**
	 * Return a boolean, which indicates whether the component was instructed to
	 * monitor its Spring context file.
	 * 
	 * @return boolean
	 */
	public boolean isStartMonitor() {
		return getStartMonitor().booleanValue();
	}

	/**
	 * Return the Profile object currently being used by this instance of the
	 * CqlComponent. Note that this method returns null if the Component
	 * has never been started.
	 */
	public ComponentProfile getComponentProfile() {
		return componentProfile;
	}

	protected void setComponentProfile(ComponentProfile componentProfile) {
		this.componentProfile = componentProfile;
	}

	/**
	 * This method is called by camel core to get a CqlEndpoint.
	 * Assumption is made that this method must be multi-thread capable and must
	 * also be aware of this component's lifecycle.
	 */
	public Endpoint createEndpoint(String uri, String remaining,
			Map<String, Object> parameters) throws Exception {

		if (LOG.isTraceEnabled()) {
			LOG.trace("createEndpoint: Component entered");
			LOG.trace("createEndpoint: uri = " + uri);
			LOG.trace("createEndpoint: remaining = " + remaining);
		}

		// do nothing if this component has been shutdown or stopped
		// if you stop the component, then it must be explicitly started!
		if (isStopped()) {
			LOG.warn("createEndpoint: component has been stopped");
			throw new Exception("createEndpoint: component has been stopped");
		}

		// do nothing if this component has been suspended
		// if it has been suspended, then you must explicitly resume it!
		if (isSuspended()) {
			LOG.warn("createEndpoint: component has been suspended");
			throw new Exception("createEndpoint: component has been suspended");
		}

		// wait while component is in the process of starting/initializing
		while (isStarting()) {
			LOG.warn("createEndpoint: waiting for component to start");
			try {
				this.wait(1000);
			} catch (InterruptedException ignore) {
			}
		}

		// if not started, then attempt to start, which includes initializing
		// the component. Note that if previously stopped, then this would be a
		// re-start and re-initialization
		if (!isStarted()) {
			LOG.trace("createEndpoint: starting component");
			// invoke our super class's start() method, which calls this
			// component's doStart()
			try {
				start();
			} finally {
				// wake up any threads that may have been waiting for this
				// component to be started
				this.notifyAll();
			}
		}

		// So far so good, begin to service the route's request. Make sure we
		// have a proper URI (context-path)
		if (remaining == null || remaining.isEmpty()) {
			throw new Exception(
					"createEndpoint: null or empty URI context-path provided");
		}

		// the context path must start with a '/' in order to give to the
		// ClientMapper
		String contextPath = remaining;
		if (!contextPath.startsWith(FORWARD_SLASH_STR)) {
			contextPath = FORWARD_SLASH_STR + contextPath;
		}

		// if there is a ClientMapper, let it map the request to a Client
		// else, just try and do a straight map
		Object obj = (getComponentProfile().getClientMapper() != null) ? getComponentProfile()
				.getClientMapper().lookupHandler(contextPath) : getClients()
				.get(remaining);

		if (obj == null) {
			throw new Exception(
					"createEndpoint: Client not found for this context path: "
							+ remaining);
		}

		if (!(obj instanceof Client)) {
			throw new Exception(
					"createEndpoint: returned object from context returns bean of type "
							+ obj.getClass().getName() + " instead of "
							+ Client.class.getName());
		}

		LOG.trace("createEndpoint: Got this Client {} ",
				((Client) obj).getBeanName());

		// create and return a CqlEndpoint
		return new CqlEndpoint(uri, contextPath, this, parameters,
				(Client) obj);

	}

	// Prevent introspection of URI parameters on the created Endpoint instance
	@Override
	protected boolean useIntrospectionOnEndpoint() {
		return false;
	}

	// starts/initializes the component. Camel core should call the super's
	// start() method, which then calls this method. Note that this component
	// extends ServiceSupport where all the higher level lifecycle methods are
	// found
	@Override
	protected void doStart() throws Exception {

		LOG.trace("doStart: enter");

		if (shutdown.get()) {
			throw new Exception("component has been shut down");
		}
		// okay to proceed, begin initialization
		initComponent();
	}

	@Override
	// stops the component - goes back to being uninitialized
	// the super's stop() method will call this doStop, but only
	// if the component had not already been stopped
	protected void doStop() throws Exception {

		LOG.trace("doStop: enter");

		if (shutdown.get()) {
			LOG.warn("component has already been shut down");
			return;
		} else if (!isSuspended() && !isStarted()) {
			LOG.trace("doStop: component had never been started");
			return;
		}

		// component is either suspended or started

		if (getComponentProfile() != null) {
			// stop the context
			if (getComponentProfile().getCassandraContext() != null) {
				getComponentProfile().getCassandraContext().stop();
			}
			// stop the profile monitor
			getComponentProfile().stop();
		}
		setComponentProfile(null);
	}

	// ---------- Begin Initialization Methods -----------------------

	// begins the initialization process. the first thing is to get the Spring
	// XML file for this component.
	private void initComponent() throws Exception {

		if (LOG.isTraceEnabled()) {
			LOG.trace("initComponent: Component has not yet been initialized");
			LOG.trace("initComponent: Using this context file name: "
					+ dfltContextFileName);
		}

		ConfigurableApplicationContext cassandraContext = null;

		// load the spring application context, which should be somewhere
		// in the application's classpath
		if (myContextFileName == null) {
			myContextFileName = dfltContextFileName;
		}
		try {
			cassandraContext = new ClassPathXmlApplicationContext(
					myContextFileName);
			setClients(cassandraContext.getBeansOfType(Client.class));
		} catch (BeansException exc) {
			LOG.error("initComponent: caught this exception while "
					+ "attempting to load spring context file: "
					+ exc.toString());
			throw exc;
		}

		if (getClients() == null || getClients().isEmpty()) {
			cassandraContext.close();
			throw new Exception(
					"there are no beans of type Client in the Spring XML file");
		}

		// now that the context has been successfully loaded, create a new
		// profile and continue the init process
		setComponentProfile(new ComponentProfile(cassandraContext,
				myContextFileName, this));

		initClientMapper(cassandraContext);
		initProfileMonitor();

	}

	/**
	 * Initialize the ClientMappers used by this component. Note that this
	 * initial implementation only supports one mapper.
	 * <p>
	 */
	private void initClientMapper(
			ConfigurableApplicationContext cassandraContext) throws Exception {

		ClientMapper clientMapper = null;
		try {
			// find the one client mapper
			clientMapper = cassandraContext.getBean(ClientMapper.class);
		} catch (NoSuchBeanDefinitionException ignore) {
		}

		if (clientMapper != null) {
			getComponentProfile().setClientMapper(clientMapper);
			LOG.trace("initClientMapper: using a client mapper");
		} else {
			LOG.trace("initClientMapper: not using a client mapper");
		}
	}

	// checks to see if profile monitor thread is to be activated
	private void initProfileMonitor() {
		if (isStartMonitor()) {
			try {
				getComponentProfile().start();
			} catch (Exception e) {
				LOG.error("initProfileMonitor: unable to start profile because of this exception: "
						+ e.getMessage());
			}
		} else {
			if (LOG.isTraceEnabled()) {
				LOG.trace("initProfileMonitor: profile not started");
			}
		}
	}

	/**
	 * @return the clients
	 */
	public Map<String, Client> getClients() {
		return clients;
	}

	/**
	 * @param clients
	 *            the clients to set
	 */
	public void setClients(Map<String, Client> clients) {
		this.clients = clients;
	}

	// ---------- End Initialization Methods -----------------------

}
