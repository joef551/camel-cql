package org.metis.cassandra;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

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
			Thread.sleep(2000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;	
			
			Component comp = camelContext.getComponent("cql1");			
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);			
			CqlComponent cqlComp = (CqlComponent)comp;			
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			
			System.out.println("*** stopping camel1; time =  "
					+ System.currentTimeMillis() / 1000);
			camelContext.stop();
			System.out.println("*** camel1 stopped; time =  "
					+ System.currentTimeMillis() / 1000);
		} else {
			System.out.println("done with test A");
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
			Thread.sleep(2000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;		
			
			Component comp = camelContext.getComponent("cql");			
			assertTrue(comp != null);
			assertTrue(comp instanceof CqlComponent);			
			CqlComponent cqlComp = (CqlComponent)comp;	
			assertTrue(cqlComp.isStarted());
			assertTrue(cqlComp.getComponentProfile() != null);
			assertTrue(cqlComp.getClients() != null);
			assertTrue(cqlComp.getClients().isEmpty() == false);
			//assertTrue(compList.size() == 1);		
			
			System.out.println("*** stopping camel2; time =  "
					+ System.currentTimeMillis() / 1000);
			camelContext.stop();
			System.out.println("*** camel2 stopped; time =  "
					+ System.currentTimeMillis() / 1000);
		} else {
			System.out.println("done with test B");
		}
	}

}
