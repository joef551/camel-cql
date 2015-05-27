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
import java.net.URL;
import java.net.URLDecoder;
import java.util.Map;
import org.apache.camel.support.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * This class is used as a profile container, of sorts, for an instance of a
 * CqlComponent. Every time someone modifies the component's Spring XML file
 * (i.e., cassandra.xml) the component adopts a new profile. It is meant to
 * support on-the-fly updating of the component's Spring XML file.
 * 
 * @author jfernandez
 * 
 */
public class ComponentProfile extends ServiceSupport implements Runnable {

	// This component's logger
	private static final Logger LOG = LoggerFactory
			.getLogger(ComponentProfile.class);

	// The CassandraResourceBeans that are bound to the component
	private Map<String, Client> clients;

	// The Cassandra context file name used by the component
	private String myContextFileName;

	// The Cassandra context file used by the component
	File myContextFile = null;

	// the component's application context file. this is the file that
	// contains the CassandraResourceBeans and their mappings. it also
	// includes the connection pools
	private ConfigurableApplicationContext cassandraContext;

	// List of HandlerMappings used by the component
	private ClientMapper clientMapper;

	// thread that will monitor the context file
	private Thread runner = null;

	// the CqlComponent that this profile is bound to
	private CqlComponent cqlComponent;

	protected ComponentProfile() {
	}

	protected ComponentProfile(ConfigurableApplicationContext cassandraContext,
			String myContextFileName, CqlComponent cqlComponent) {
		this.cassandraContext = cassandraContext;
		this.myContextFileName = myContextFileName;
		this.cqlComponent = cqlComponent;
	}

	protected ComponentProfile(CqlComponent cqlComponent) {
		this.cqlComponent = cqlComponent;
	}

	protected CqlComponent getCqlComponent() {
		return cqlComponent;
	}

	protected void setCqlComponent(CqlComponent cqlComponent) {
		this.cqlComponent = cqlComponent;
	}

	protected Map<String, Client> getClients() {
		return clients;
	}

	protected void setRdbs(Map<String, Client> clients) {
		this.clients = clients;
	}

	protected String getContextFileName() {
		return myContextFileName;
	}

	protected void setContextFileName(String myContextFileName) {
		this.myContextFileName = myContextFileName;
	}

	protected void setCassandraContext(
			ConfigurableApplicationContext cassandraContext) {
		this.cassandraContext = cassandraContext;
	}

	protected ConfigurableApplicationContext getCassandraContext() {
		return cassandraContext;

	}

	// returns a file handle to the corresponding CqlComponent's Spring
	// XML file
	protected File getContextFile() {

		if (myContextFile != null) {
			return myContextFile;
		}

		// See if the file can be found in the class path
		URL url = getClass().getClassLoader().getResource(getContextFileName());

		if (url == null) {
			// File couldn't be found in class path
			// so check the current directory
			myContextFile = new File(getContextFileName());
			if (!myContextFile.exists()) {
				LOG.error("getContextFile: Unable to locate context file");
				return null;
			}
		} else {
			try {
				myContextFile = new File(URLDecoder.decode(url.getPath(),
						"utf-8"));
			} catch (Exception e) {
				LOG.error("getContextFile - exception = " + e.toString());
				return null;
			}
		}
		return myContextFile;
	}

	// A couple of lifecycle methods. Note that these methods are invoked
	// by the start() and stop() methods in this class' super class (i.e.,
	// ServiceSupport). Those super class methods control access to these
	// do* methods.

	// Start the monitor thread. It must be marked as a Daemon thread so
	// as not to preclude the JVM from exiting! Also give the thread the
	// lowest possible priority.
	protected void doStart() throws Exception {
		if (cassandraContext == null) {
			return;
		}
		runner = new Thread(this, "Cassandra Component Profile: " + toString());
		runner.setDaemon(true);
		runner.setPriority(Thread.MIN_PRIORITY);
		runner.start();
	}

	// stop the monitor thread (if any)
	public void doStop() {
		if (runner != null) {
			runner.interrupt();
		}
	}

	// Monitor the context file
	public void run() {

		int clicks = 0, expireTime = 2;
		long lastModified = 0L;
		File fileHandle = null;

		if (LOG.isTraceEnabled()) {
			LOG.trace("Cassandra monitor thread: started");
		}

		// get some info on the file. the file should exist
		// when this thread is started. afterwards,
		// it is possible for someone to remove the file,
		// then restore it.
		fileHandle = getContextFile();
		if (fileHandle != null) {
			lastModified = fileHandle.lastModified();
		} else {
			LOG.error("Cassandra monitor thread: started without context file");
			return;
		}

		LOG.trace("Cassandra monitor thread: monitoring this file: "
				+ fileHandle.getAbsolutePath());

		// begin monitoring the file
		while (isStarted()) {

			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignore) {
			}

			// was thread asked to terminate while sleeping?
			if (isStopped()) {
				LOG.trace("monitor thread: stopped, bye! ");
				return;
			}

			// check the file every two clock ticks.
			// note: future work to have these be configurable
			if (++clicks < expireTime) {
				continue;
			}

			clicks = 0;

			if (LOG.isTraceEnabled()) {
				LOG.trace("Cassandra monitor thread: wakeup");
			}

			// was/had the file been removed?
			if (fileHandle == null) {
				// yes, so try and re-acquire a handle for it
				// we're assuming that since the file was removed,
				// then returned, that the file was updated!
				fileHandle = getContextFile();
				if (fileHandle != null) {
					LOG.trace("monitor thread: newly acquired "
							+ "fileHandle, stopping then restarting "
							+ "component");

					cycleCqlComponent();
					// we're done; a new profile has taken our place
					return;
				}
				// file exists, so get its latest mod time
			} else if (fileHandle.exists()) {
				if (lastModified != fileHandle.lastModified()) {
					// modification time has changed, so cycle the
					// component
					LOG.trace("monitor thread: file has been updated, "
							+ "stopping then restarting component. Bye.");
					cycleCqlComponent();
					// we're done; a new profile has taken our place
					return;
				}
				// file no longer exists
			} else {
				LOG.trace("monitor thread - file has been removed");
				fileHandle = null;
			}
		}
	}

	// stop and restart my CqlComponent
	private void cycleCqlComponent() {
		// first, mark this profile as being stopped
		runner = null;
		try {
			stop();
		} catch (Exception ignore) {
		}
		// now cycle the component
		try {
			// stop() will call the component's doStop()
			// start() will call the component's doStart()
			getCqlComponent().stop();
			getCqlComponent().start();
		} catch (Exception e) {
			LOG.error("Cassandra monitor thread: caught this exception while "
					+ "cycling component: " + e.getMessage());
		}
	}

	/**
	 * @return the clientMapper
	 */
	public ClientMapper getClientMapper() {
		return clientMapper;
	}

	/**
	 * @param clientMapper
	 *            the clientMapper to set
	 */
	public void setClientMapper(ClientMapper clientMapper) {
		this.clientMapper = clientMapper;
	}

}
