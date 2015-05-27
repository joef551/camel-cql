package org.metis.cassandra;

import org.apache.camel.CamelContext;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Can be used to load an XML file that includes everything: Camel context with
 * route, CqlComponent definition, Client beans, and ClusterBean
 * 
 * @author jfernandez
 * 
 */
public class GenericTest {
	static private ApplicationContext context = null;

	@Test
	public static void main(String[] args) throws Exception {
		// create our context file
		try {
			context = new ClassPathXmlApplicationContext("camel1.xml");
		} catch (BeansException be) {
			System.out
					.println("ERROR: unable to load spring context, got this exception: \n"
							+ be.getLocalizedMessage());
			be.printStackTrace();
			return;
		}
		try {
			Thread.sleep(20000);
		} catch (Exception e) {
		}

		Object obj = context.getBean(CamelContext.class);

		if (obj != null) {
			CamelContext camelContext = (CamelContext) obj;
			System.out.println("stopping context; time =  "
					+ System.currentTimeMillis() / 1000);
			camelContext.stop();
			System.out.println("context stopped; time =  "
					+ System.currentTimeMillis() / 1000);
		} else {
			System.out.println("done");
		}

		System.exit(0);
	}

}
