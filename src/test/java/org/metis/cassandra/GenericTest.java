package org.metis.cassandra;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;
import org.metis.cassandra.Client.Method;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This test is invoked against the camel1.xml file. It is simply used to test
 * the proper loading of an XML file that includes everything: Camel context
 * with route, CqlComponent definition, Client beans, and ClusterBean
 * 
 * @author jfernandez
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GenericTest {
	static private ApplicationContext context = null;

	@BeforeClass
	public static void initialize() throws Exception {
	}

	@Test
	public void testA() throws Exception {
		// create our context file
		try {
			context = new ClassPathXmlApplicationContext("camel1.xml");
		} catch (BeansException be) {
			be.printStackTrace();
			fail("ERROR: unable to load spring context, got this exception: \n"
					+ be.getLocalizedMessage());
		}
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;
			// N.B. We're invoking the cql component called "cql1" that is
			// defined in the camel1.xml
			Component comp = camelContext.getComponent("cql1");
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);
			CqlComponent cqlComp = (CqlComponent) comp;
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			camelContext.stop();
		} else {
			fail("ERROR: unable to load CamelContext");
		}
	}

	@Test
	public void testB() throws Exception {
		// create our context file
		try {
			context = new ClassPathXmlApplicationContext("camel2.xml");
		} catch (BeansException be) {
			be.printStackTrace();
			fail("ERROR: unable to load spring context, got this exception: \n"
					+ be.getLocalizedMessage());
		}
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;
			// N.B. We're invoking the default cql component
			Component comp = camelContext.getComponent("cql");
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);
			CqlComponent cqlComp = (CqlComponent) comp;
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			// assertTrue(compList.size() == 1);
			camelContext.stop();
		} else {
			fail("ERROR: unable to load CamelContext");
		}
	}

	@Test
	public void testC() throws Exception {
		// create our context file
		try {
			context = new ClassPathXmlApplicationContext("camel3.xml");
		} catch (BeansException be) {
			be.printStackTrace();
			fail("ERROR: unable to load spring context, got this exception: \n"
					+ be.getLocalizedMessage());
		}
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;
			// N.B. We're invoking the cql component called "cql1" that is
			// defined in the camel3.xml
			Component comp = camelContext.getComponent("cql1");
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);
			CqlComponent cqlComp = (CqlComponent) comp;
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			assertTrue(cqlComp.getClients().size() == 1);
			assertTrue(cqlComp.getClients().get("user") != null);
			assertTrue(cqlComp.getClients().get("user").getDefaultMethod() == Method.INSERT);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Select()
					.size() == 1);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Delete()
					.size() == 0);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Insert()
					.size() == 1);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Update()
					.size() == 0);
			camelContext.stop();
		} else {
			fail("ERROR: unable to load CamelContext");
		}
	}

	@Test
	public void testD() throws Exception {
		// create our context file
		try {
			context = new ClassPathXmlApplicationContext("camel4.xml");
		} catch (BeansException be) {
			be.printStackTrace();
			fail("ERROR: unable to load spring context, got this exception: \n"
					+ be.getLocalizedMessage());
		}
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;
			// N.B. We're invoking the cql component called "cql1" that is
			// defined in the camel4.xml
			Component comp = camelContext.getComponent("cql1");
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);
			CqlComponent cqlComp = (CqlComponent) comp;
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			assertTrue(cqlComp.getClients().size() == 1);
			assertTrue(cqlComp.getClients().get("user") != null);
			assertTrue(cqlComp.getClients().get("user").getDefaultMethod() == Method.INSERT);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Select()
					.size() == 1);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Delete()
					.size() == 0);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Insert()
					.size() == 2);
			assertTrue(cqlComp.getClients().get("user").getCqlStmnts4Update()
					.size() == 0);

			CqlStmnt cqlStmnt = cqlComp.getClients().get("user")
					.getCqlStmnts4Select().get(0);
			assertTrue(cqlStmnt.isIdempotent() == true);
			assertTrue(cqlStmnt.isTracing() == true);
			assertTrue(cqlStmnt.isPagingState() == true);
			assertTrue(cqlStmnt.getFetchSize() == 101);
			assertTrue(cqlStmnt.getDefaultTimestamp() == 123456789);
			camelContext.stop();

		} else {
			fail("ERROR: unable to load CamelContext");
		}
	}

}
