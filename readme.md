Introduction
============
[Apache Camel](http://camel.apache.org)<font size="1"><sup>TM</sup></font> is a powerful and feature-rich open source integration framework whose goal is, in part, to facilitate the implementation of enterprise integration patterns. Camel supports or implements  most of the integration patterns that are described in the book by Bobby Woolf and Gregor Hohpe entitled, ["Enterprise Integration Patterns"](http://www.enterpriseintegrationpatterns.com "EIP" ). In the Camel vernacular, patterns are also referred to as routes. Camel is the integration framework for the open source [ServiceMix](http://servicemix.apache.org) enterprise service bus (ESB); however, Camel can also be used as a standalone integration framework. Camel includes a number of different [components](http://camel.apache.org/component.html), where each component can be viewed as a connector to an API, framework, protocol, and/or data store. For example, there are components  for smtp, ftp, tcp, file, sql, jdbc, jetty, etc. There must now be over 50 different components and the number just keeps growing. Camel routes are message patterns that are used, in part, for integrating these components. For example, you may have a route that reads messages from a JMS queue and persists the messages to different database tables. The different routing possibilities are endless. The "[Camel In Action](http://www.manning.com/ibsen/)" book is a must-read for anyone getting started with Camel. 

[Apache Cassandra](http://cassandra.apache.org)<font size="1"><sup>TM</sup></font> is a massively scalable open source NoSQL database management system (DBMS) [1]. Cassandra is highly fault-tolerant and based on the Columnar or ColumnFamily data model; think of it as a highly distributed hash table. Cassandra includes a SQL-like programming language called, "[Cassandra Query Language](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html)" (CQL), which is the default and primary interface into the Cassandra DBMS. Using CQL is similar to using SQL in that the concept of a table having rows and columns is almost the same in CQL and SQL. The main difference is that Cassandra does not support joins or subqueries, except for batch analysis through Hive. Instead, Cassandra emphasizes denormalization through CQL features like collections and clustering specified at the schema level [1]. CQL is the recommended way to interact with Cassandra. The simplicity of reading and using CQL is an advantage over older Cassandra APIs.The goal of this project is to provide a highly configurable and flexible Camel component for CQL. The CQL component allows one to create Camel routes that integrate with the Cassandra DBMS. The initial release of this component supports a Camel producer (e.g., `to()`), but not consumer. The producer provides the basic CRUD (create, read, update, delete) functionality and implements the InOut message exchange pattern (MEP). 
URI Format====
```
cql:clientName[?options]```   
Where "clientName" uniquely identifies a Cassandra client bean.  


Configuration
====

A Cassandra CQL component (org.metis.cassandra.CqlComponent) is configured through either one of the following:

- The XML file that includes the Camel context and routes that use the CQL component. As an example, see the file called "camel1.xml" in the project's test resources directory.  
- An XML file that is external to the XML file that includes the corresponding Camel context and route(s). This external XML file is, by default, called "cassandra.xml" and it must reside in the application's class path. To override the default "cassandra.xml" file name, use the **metis.cassandra.spring.context** system property or define a CqlComponent bean (see below) that specifies the 'contextFile' property.

```xml
<bean id="cql1" class="org.metis.cassandra.CqlComponent">
 <property name="contextFileName" value="mycfg.xml" />
</bean>
<camelContext xmlns="http://camel.apache.org/schema/spring">
  <route id="camel1-route">
 	<from uri="seda:cassy" />
	<to uri="cql1:user"/>
  </route>
</camelContext>
``` 

Via either of the two configuration approaches described above, you define and configure the bean objects described in the following subsections. As you read through the following configuration-related subsections, please keep in mind the use of [Camel property placeholders](http://camel.apache.org/using-propertyplaceholder.html), which provide for dynamic configurations.  


Client Bean (org.metis.cassandra.Client)
----

The client bean is a thread-safe component that is used by the CqlComponent for accessing a Cassandra cluster, a keyspace within the cluster, and any tables within the keyspace. You can define any number of client beans. The CQL component's URI is used to identify which client bean to use. For example, a URI of `cql:user` specifies the client bean with an id of "user", which may logically be used for accessing the "user"" table in a Cassandra keyspace. Also see [Client Mapper](#clientmapper) for using ant-style pattern matching to identify a client bean. 

Each client bean that you define must be assigned one or more CQL statements, which can be any combination of SELECT, UPDATE, DELETE, and INSERT query method. The incoming request message (i.e., Camel in-message) specifies a method, as well as input parameters (key:value pairs). The combination of specified method and input parameters is used to identify which of the client's CQL statements are to be used for the corresponding request message. The method is specified in the Camel Exchange's input message via a header called, "**metis.cassandra.method**". If that header is not present, the client will fall back on a default method. The default method can be either injected into the client or you can have the client choose a default method based on its injected CQL statements. You can inject the default method via the 'defaultMethod' property. For example:

```xml
<bean id="user" class="org.metis.cassandra.Client">
  ...
   <property name="defaultMethod" value="select" />  
  ...
</bean> 	
```

If a default method is not injected, the client will select one based on its injected CQL statements. If the injected CQL statements include any combination of methods (e.g., SELECT and DELETE), the client will fall back on SELECT. If the injected CQLs comprise just one method, then that method will be the default method. For example, if all the injected CQL statements are of type DELETE, then DELETE will be the default method for the client. However, if there are CQL statements for both SELECT and DELETE, then SELECT will be the chosen default method.  
 
If the incoming request message does not include input parameters, the CQL statement not having any parameterized fields (see below) will be chosen.  The input parameters are passed in  as either a Map, List of Maps, or JSON object. JSON objects are in the form of a String or Stream object. If it is a JSON object (i.e., in the form of a String or InputStream), the object will be transformed to either a Map or List of Maps. Please note that all incoming Camel exchanges must have the **InOut** message exchange pattern (MEP). The response message (Camel out message) is sent back as a List of Maps. 

So through a single Camel route, you can invoke any of the CQL statements that are assigned to a particular client. This, therefore, precludes the route from getting nailed to any one particular CQL statement for it is the method and input parameters that decide which CQL statement will be used. Here's a snippet of XML that defines a client bean called "user". 

```xml
<bean id="user" class="org.metis.cassandra.Client">
 <property name="cqls">
   <list>
	<value>select * from users</value>
	<value>select username from username_video_index where username =`text:username`</value>
	<value>select username, email from users where username = `text:user`</value>
	<value>insert into users (username, created_date, email, firstname, lastname, password) values
	  (`text:username`, `timestamp:created_date`, `list:text:email`, `text:firstname`, `text:lastname`, 
	   `text:password`) </value>
    <value>delete from users where username=`text:user`</value>
   </list>
 </property> 
 <property name="keyspace" value="videodb" />	
 <property name="clusterBean" ref="cluster1" />
</bean>
``` 

The following describe each of the client bean's properties. 

<u>cqls</u>

The **cqls** property is used to assign the client bean one or more CQL statements. In the example above, the client bean has been assigned five CQL statements: three selects, one insert, and one delete. The default query method for this client is SELECT because the injected list of CQL statements include a combination of different methods and the  "defaultMethod" property has not been specified. 

Note that four of the statements have fields delimited by backticks (e.g., \`list:text:email\`). These are *parameterized fields* that comprise 2 or 3 subfields, which are delimited by a ":". A CQL statement's parameterized fields are used for binding input parameters, which arrive with the request message, to the CQL statement. So any CQL statement with a parameterized field is essentially a CQL prepared statement. The first subfield (from right-to-left) of a parameterized field is required, and it specifies the name of the input parameter that corresponds to the field. In other words, it must match the key of a key:value pair that is passed in via the request message. In the previous example, 'email' is the name of the input parameter. The next subfield is the type of input parameter, and it must match one of the Cassandra [data types](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/DataType.Name.html). The last subfield, which is optional, is used to specify a collection (list, map, or set). Here are a couple of examples:

1. **\`list:text:email\`** is a parameterized field that is matched up to an input parameter whose key is 'email' and whose value is a list of ascii text strings. 
2. **\`text:username\`** is a parameterized field that is matched up to an input parameter whose key is 'username' and whose value is an ascii text string. 


Note from [1] about using collections. <i>"Use collections, such as Set, List or Map, when you want to store or denormalize a small amount of data. Values of items in collections are limited to 64K. Other limitations also apply. Collections work well for storing data such as the phone numbers of a user and labels applied to an email. If the data you need to store has unbounded growth potential, such as all the messages sent by a user or events registered by a sensor, do not use collections. Instead, use a table having a compound primary key and store data in the clustering columns".</i>


All CQL statements for a particular method must be distinct with respect to the parameterized fields. If you have two statements with the same query method and those two methods have the same number of parameterized fields with matching key names, then an exception will be thrown during the bean's initialization. For example, these two CQL statements, when assigned to a client bean, will result in an exception. 

````
select username from username_video_index where username =`text:username`
select first, last from user where username =`text:username`
````
Even though the two statements access different tables, they share the same number of identical parameterized fields; therefore, one is not distinct from the other wrt the parameterized fields. The following two statements will not result in an exception.

````
select username from username_video_index where username =`text:username`
select first, last from user where username =`text:username` and `int:age` = 59
````
Even though they share one identical parameterized field, they do not have an equal number of parameterized fields.

When a request message arrives, in the form of a Camel in-message that specifies a SELECT method, the example client's three assigned and **distinct** SELECT statements become candidates for the request. The input parameters (key:value pairs) in the request message dictate which of the three to use. For example, if the request message contains one Map with one key:value pair of `username:joe`, then the second SELECT CQL statement will be used. If the request message contains only one Map with one key:value pair of `user:joe`, then the third SELECT statement is used. If there is no request message (i.e., the Camel exchange does not include a payload), then the first CQL select statement is used because it has no parameterized fields. An exception is thrown if a match cannot be found. 

<u>**If the payload comprises a list of maps, then all of the maps in the payload must have the same set of keys! </u>** If there is a list of maps, then it represents a batch UPDATE, INSERT, or DELETE. A list of maps cannot be used for a SELECT.  

<u>keyspace</u>

The **keyspace** property is used to specify the name of the keyspace, within a Cassandra cluster, that the client bean is to use. 

<u>clusterBean</u>

The **clusterBean** property references a [ClusterBean](#clusterbean) within the Spring XML file that is used to connect to a Cassandra cluster (see ClusterBean section below).



[Client Mapper](id:clientmapper) (org.metis.cassandra.ClientMapper)
----
The optional client mapper is used for mapping URIs to Cassandra client beans. The mapper uses [ant-style pattern matching](https://ant.apache.org/manual/dirtasks.html). If a mapper does not exist, then the URIs will have to directly map to a client. For example, "cql:user" will directly map to the client called "user". If the mapper below is used, then "cql:videouser" will be mapped to the "videoevent" client. 

```xml
<bean id="handlerMapping" class="org.metis.cassandra.ClientMapper">
  <property name="mappings">
	<value>
		/user=user
		/video*=videoevent
	</value>
  </property>
</bean>
```
So given the above mappings, all of the following URIs will get mapped to the client with an ID of 'videoevent':

- cql:video
- cql:videoentry

You can only have one client mapper per Spring XML application context.



[Cluster Bean](id:clusterbean) (org.metis.cassandra.ClusterBean)
----
A cluster bean, in combination with its referenced beans, is used for configuring an instance of a [Cassandra Java driver](http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/whatsNew2.html), which is used for accessing a Cassandra cluster. Each client bean in the Spring context must be wired to a cluster bean. You can define any number of cluster beans; each used for accessing a different Cassandra cluster. 

The following is an example definition of a cluster bean. Please note that not all of the cluster bean's properties are depicted in the example. Also, please refer to the driver's [API docs](http://docs.datastax.com/en/drivers/java/2.1/) for more detailed information on the driver's policies and options.  


```xml
<bean id="cluster1" class="org.metis.cassandra.ClusterBean">
	
	<property name="clusterName" value="myCluster"/> 
	<property name="clusterNodes" value="127.0.0.1" />
	
	<!-- Some of the driver's policies. If one is not specified it will be 
	     defaulted. Please refer to the 2.1 driver's API documentation for a 
	     complete list of policies and their defaults -->
	<property name="loadBalancingPolicy" ref="roundRobin" />
	<property name="reconnectionPolicy" ref="reconnectionPolicy" />
	<property name="retryPolicy" ref="retryPolicy" />
	
	<property name="metricsOption" ref=" metricsOption" />
	
	<!-- If a protocol or socket option is not specified, the corresponding 
	     default is be used. The ProtocolOptions is typically used for 
	     setting the target port number of the Cassandra cluster -->
	<property name="protocolOptions" ref="protocolOptions" />
	<property name="socketOptions" ref="socketOptions" />
	
	<!-- You can specify PoolingOptions, but you can only specify those 
	     properties (e.g., poolTimeoutMillis, heartbeatIntervalSeconds, 
	     idleTimeoutSeconds) that are not associated with a HostDistance
	     (i.e., LOCAL, REMOTE, and IGNORE). For those properties that are 
	     associated with a HostDistance, use the listOfPoolingOptions 
	     property. -->
	<property name="poolingOptions" ref="poolingOptions" />
	<!-- Use the listOfPoolingOptions to specify those pooling options that 
	     are associated with a HostDistance. You should only be concerned 
	     with LOCAL and REMOTE HostDistances. -->
	<property name="listOfPoolingOptions">
	   <list>
		<ref bean="localOption"/>
		<ref bean="remoteOption"/>
	   </list>
	</property>		
	
	<property name="queryOptions" ref="queryOptions"/> 
</bean>

<!-- Example referenced beans -->

<bean id="roundRobin" class="com.datastax.driver.core.policies.RoundRobinPolicy" />
<bean id="reconnectionPolicy" class="com.datastax.driver.core.policies.ExponentialReconnectionPolicy" />
<bean id="retryPolicy" class="com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy" />

<bean id="metricsOption" class="com.datastax.driver.core.MetricsOption" >
  <constructor-arg name="jmxEnabled" value="true"/> 
</bean>


<bean id="protocolOptions" class="com.datastax.driver.core.ProtocolOptions">
 	<constructor-arg name="port" value="9042"/> 
</bean>

<bean id="poolingOptions" class="com.datastax.driver.core.PoolingOptions"/>
<bean id="localOption" class="org.metis.cassandra.PoolingOption">
		<property name="distance" value="local" />
		<property name="coreConnectionsPerHost" value="5" />
		<property name="maxConnectionsPerHost" value="10" />
		<property name="maxSimultaneousRequestsPerConnectionThreshold" value="2" />
		<property name="maxSimultaneousRequestsPerHostThreshold" value="5" />
</bean>
<bean id="remoteOption" class="org.metis.cassandra.PoolingOption">
		<property name="distance" value="remote" />
		<property name="coreConnectionsPerHost" value="5" />
		<property name="maxConnectionsPerHost" value="10" />
		<property name="maxSimultaneousRequestsPerConnectionThreshold" value="2" />
		<property name="maxSimultaneousRequestsPerHostThreshold" value="5" />
</bean>

<bean id="socketOptions" class="com.datastax.driver.core.SocketOptions"/>

<!-- An example of how to inject an Enum of ONE for the consistency level -->
<bean id="queryOptions" class="com.datastax.driver.core.QueryOptions">
  <property name="consistencyLevel">
    <util:constant static-field="com.datastax.driver.core.ConsistencyLevel.ONE"/>
  </property>
</bean>

</bean>


```

<u>clusterName</u>

The **clusterName** property is used to override the default cluster name, which is taken from the bean id. This is used primarily for JMX. 

<u>clusterNode</u>

The **clusterNodes** property is used for specifying a comma-separated list of ip addresses for nodes in a Cassandra cluster. Note that all specified nodes must share the same port number. In the Cassandra vernacular, a node is also referred to as a cluster "contact point". The driver uses a cluster contact point to connect to the cluster and discover its topology. Only one node is required (the driver will automatically retrieve the addresses of the other nodes in the cluster); however, it is usually a good idea to provide more than one node, because if that single node is unavailable, the driver cannot connect to the Cassandra cluster. 

<u>Policies</u>

The different policies supported by the driver. The example "cluster1" bean above depicts how these policies can be injected into the cluser bean.

- LoadBalancingPolicy
- ReconnectionPolicy
- RetryPolicy
- AddressTranslater
- SpeculativeExecutionPolicy

Please refer to the 2.1 Java driver's API documentantion for a complete list of its policies and their descriptions. 

<u>protocolOptions</u>

The **protocolOptions** property is used for specifying the [ProtocolOptions](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ProtocolOptions.html) to use for the discovered nodes. If you don't specify one, a default ProtocolOption is used. ProtocolOption is typically used for overriding the default port number of  9042. 

<u>poolingOptions</u>

The [options](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html) associated with connection pooling. 

<u>listOfPoolingOptions</u>

The **listOfPoolingOptions** property specifies a list of org.org.metis.cassandra.PoolingOption beans that are njected into the [PoolingOptions](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/PoolingOptions.html). You want to have a PoolingOption bean for LOCAL and REMOTE (HostDistance) nodes. For IGNORED nodes, the default for all those settings is 0 and cannot be changed. The cluster bean will inject this list of PoolingOption beans into the PoolingOptions. 

<u>socketOptions</u>

The **socketOptions** property is used for specifying low-level [SocketOptions](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/SocketOptions.html) (e.g., keepalive, solinger, etc.) for the connections to the cluster nodes. 

<u>queryOptions</u>

The **queryOptions** property is used for specifying options for the [QueryOptions](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/QueryOptions.html) for the queries.  




So What or Who Is Metis?
====
*Metis was the Titaness of the forth day and the planet Mercury. She presided over all wisdom and knowledge. She was seduced by Zeus and became pregnant with Athena. Zeus became concerned over prophecies that Metis’ children would be very powerful and that her second child (a son) would overthrow Zeus. To avoid this from ever hapenning, Zeus turned Metis into a fly and swallowed her. It is said that Metis is the source for Zeus’ wisdom and that she advises Zeus from his belly.[3]*

References
====
[1] [*DataStax, Apache Casandra 2.0 Documentation*](http://www.datastax.com/documentation/articles/cassandra/cassandrathenandnow.html)  
[2] [*DataStax, Apache Casandra Java Driver 2.0 API Reference*](http://www.datastax.com/drivers/java/2.0/)  
[3] [*Greek Mythology*](http://www.greekmythology.com/Titans/Metis/metis.html)

