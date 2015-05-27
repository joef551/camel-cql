Introduction
============
[Apache Camel](http://camel.apache.org)<font size="1"><sup>TM</sup></font> is a powerful and feature-rich open source integration framework whose goal is, in part, to facilitate the implementation of enterprise integration patterns. Camel supports or implements  most of the integration patterns that are described in the book by Bobby Woolf and Gregor Hohpe entitled, ["Enterprise Integration Patterns"](http://www.enterpriseintegrationpatterns.com "EIP" ). In the Camel vernacular, patterns are also referred to as routes. Camel is the integration framework for the open source [ServiceMix](http://servicemix.apache.org) enterprise service bus (ESB); however, Camel can also be used as a standalone integration framework. Camel includes a number of different [components](http://camel.apache.org/component.html), where each component can be viewed as a connector to an API, framework, protocol, and/or data store. For example, there are components  for smtp, ftp, tcp, file, sql, jdbc, jetty, etc. There must now be over 50 different components and the number just keeps growing. Camel routes are message patterns that are used, in part, for integrating these components. For example, you may have a route that reads messages from a JMS queue and persists the messages to different database tables. The different routing possibilities are endless. The "[Camel In Action](http://www.manning.com/ibsen/)" book is a must-read for anyone getting started with Camel. 

[Apache Cassandra](http://cassandra.apache.org)<font size="1"><sup>TM</sup></font> is a massively scalable open source NoSQL database management system (DBMS) [1]. Cassandra is highly fault-tolerant and based on the Columnar or ColumnFamily data model; think of it as a highly distributed hash table. Cassandra includes a SQL-like programming language called, "[Cassandra Query Language](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html)" (CQL), which is the default and primary interface into the Cassandra DBMS. Using CQL is similar to using SQL in that the concept of a table having rows and columns is almost the same in CQL and SQL. The main difference is that Cassandra does not support joins or subqueries, except for batch analysis through Hive. Instead, Cassandra emphasizes denormalization through CQL features like collections and clustering specified at the schema level [1]. CQL is the recommended way to interact with Cassandra. The simplicity of reading and using CQL is an advantage over older Cassandra APIs.The goal or purpose behind this project is to provide a highly configurable and flexible Camel component for CQL. The CQL component will thus allow one to create Camel routes that integrate with the Cassandra DBMS. The initial release of this component supports a Camel producer (e.g., `to()`), but not consumer. The producer provides the basic CRUD (create, read, update, delete) functionality and implements either In or InOut message exchange pattern (MEP). 
URI Format====
```
cql:clientName[?options]```   
Where "clientName" uniquely identifies a Cassandra client bean.  


Configuration
====

A Cassandra CQL component (org.metis.cassandra.CqlComponent) is configured through either an external Spring XML file (Spring application context), which by default is called, "cassandra.xml" and must reside in the class path, or the Spring XML file that includes the Camel context and routes. As an example of the latter, see the file called "camel1.xml" in the project's test resources directory.  In either of the two approaches, you define and configure the bean objects described in the following subsections. 

To override the default "cassandra.xml" file name, use the **metis.cassandra.spring.context** system property or define a CqlComponent bean as follows.

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


Client Bean (org.metis.cassandra.Client)
----

The client bean is a thread-safe component that is used by the CqlComponent for accessing a Cassandra cluster, a keyspace within the cluster, and any tables within the keyspace. You can define any number of client beans. The CQL component's URI is used to identify which client bean to use. For example, a URI of `cql:user` specifies the client bean with an id of "user", which may logically be used for accessing the "user"" table in a Cassandra keyspace. Also see [Client Mapper](#clientmapper) for using ant-style pattern matching to identify a client bean. 

Each client bean that you define is assigned one or more CQL statements, which can be any combination of SELECT, UPDATE, DELETE, and INSERT. The incoming request message (i.e., Camel in message) specifies a method, as well as input parameters (key:value pairs). The combination of specified method and input parameters is used to identify which of the client's CQL statements are to be used for the corresponding request message. The method is specified in the Camel Exchange's input message via a header called, "**metis.cassandra.method**". If that header is not present, the client will fall back on a default method. The default method can be either injected into the client or you can have the client choose a default method based on its injected CQL statements. You can inject the default method via the 'defaultMethod' property. For exmaple:

```xml
<bean id="user" class="org.metis.cassandra.Client">
  ...
   <property name="defaultMethod" value="select" />  
  ...
</bean> 	
```
If a default method is not injected, the client will select one based on the CQL statements that have been specified. If the injected CQL statements include any combination of methods (e.g., SELECT and DELETE), the client will fall back on SELECT. If the injected CQLs comprise just one method, then that method will be the default method. For example, if all the injected CQL statements are of type DELETE, then DELETE will be the default method for the client. However, if there are CQL statements for both SELECT and DELETE, then SELECT will be the chosen default method.  
 
If the incoming request message (i.e., message body in the incoming Camel exchange) does not include input parameters, then the CQL statement not having any parameterized fields (see below) will be chosen.  The input parameters are passed in  as either a Map, List of Maps, or JSON object. If it is a JSON object, the object will be transformed to either a Map or List of Maps. Please note that all incoming Camel exchanges must be have the **InOut** message exchange pattern (MEP). The response message (Camel out message) is sent back as a List of Maps. 

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


<u>cqls</u>

The **cqls** property is used to assign the client bean one or more CQL statements. In the example above, the client bean has been assigned five CQL statements: three selects, one insert, and one delete. The default query method for this client is SELECT because the injected list of CQL statements are a combination of different methods. 

Note that four of the statements have parts delimited by backticks (e.g., \`list:text:email\`). These are *parameterized fields* that comprise 2 or 3 subfields, which are delimited by a ":". Parameterized fields are used for binding input parameters to prepared CQL statements. So any CQL statement with a parameterized field is treated as a CQL prepared statement. The first subfield (from right-to-left) of a parameterized field is required, and it specifies the name of the input parameter that corresponds to the field. In other words, it must match the key of a key:value pair that is passed in via the Camel in-message's body or payload. In the previous example, 'email' is the name of the input parameter. The next subfield is the type of input parameter, and it must match one of the Cassandra [data types](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html). The last subfield, which is optional, is used to specify a collection (list, map, or set). Here are a couple of examples:

1. **\`list:text:email\`** is a parameterized field that is matched up to an input parameter whose key is 'email' and whose value is a list of ascii text strings. 
2. **\`text:username\`** is a parameterized field that is matched up to an input parameter whose key is 'username' and whose value is an ascii text string. 


Note from [1] about using collections. <i>"Use collections, such as Set, List or Map, when you want to store or denormalize a small amount of data. Values of items in collections are limited to 64K. Other limitations also apply. Collections work well for storing data such as the phone numbers of a user and labels applied to an email. If the data you need to store has unbounded growth potential, such as all the messages sent by a user or events registered by a sensor, do not use collections. Instead, use a table having a compound primary key and store data in the clustering columns"".</i>


All CQL statements for a particular method must be distinct with respect to the parameterized fields. If two statements assigned to a method have the same number of parameterized fields with matching key names, then an exception will be thrown during the bean's initializtion.  For example, these two CQL statements, when assigned to a client bean, will result in an exception. 

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

When a request arrives, in the form of a Camel in-message that specifies a SELECT method, the example client's three assigned and **distinct** SELECT statements become candidates for the request. The input parameters (key:value pairs) in the in-message's payload or body are used to decide which of the three to use. For example, if the payload contains one Map with one key:value pair of `username:joe`, then the second SELECT CQL statement will be used. If the payload contains only one Map with one key:value pair of `user:joe`, then the third SELECT statement is used. If there is no payload (i.e., no key:value pairs), then the first CQL select statement is used because it has no parameterized fields. An exception is thrown if a match cannot be found. 

<u>**If the payload comprises a list of maps, then all of the maps in the payload must have the same set of keys! </u>** If there is a list of maps, then it represents a batch UPDATE, INSERT, or DELETE. A list of maps cannot be used for a SELECT.  

<u>keyspace</u>

The **keyspace** property is used to specify the name of the keyspace, within a Cassandra cluster, that the client bean is to use. 

<u>clusterBean</u>

The **clusterBean** property references a [ClusterBean](#clusterbean) within the Spring XML file that is used to connect to a Cassandra cluster (see ClusterBean section below).



[Client Mapper](id:clientmapper) (org.metis.cassandra.ClientMapper)
----
The optional client mapper is used for mapping URIs to Cassandra clients that are defined in the Spring XML application context; the mapper uses [ant-style pattern matching](https://ant.apache.org/manual/dirtasks.html). If a mapper does not exist, then the URIs will have to directly map to a client. For example, "cql:user" will directly map to the client called "user". If the mapper below is used, then "cql:videouser" will be mapped to the "videoevent" client. 

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
A cluster bean, in combination with supporting beans, is used for configuring an instance of a Cassandra Java driver, which is used to access a Cassandra cluster. Each client bean in the Spring context must be wired to a cluster bean. You can define any number of cluster beans; each used for accessing a different Cassandra cluster. 

```xml
<bean id="cluster1" class="org.metis.cassandra.ClusterBean">
	<property name="clusterName" value="myCluster"/> 
	<property name="clusterNodes" value="127.0.0.1" />
	<property name="loadBalancingPolicy" ref="roundRobin" />
	<property name="reconnectionPolicy" ref="reconnectionPolicy" />
	<property name="protocolOptions" ref="protocolOptions" />
	<property name="socketOptions" ref="socketOptions" />
	<property name="listOfPoolingOptions">
	   <list>
		<ref bean="localOption"/>
		<ref bean="remoteOption>"/>
	   </list>
	</property>		
	<property name="fetchSize" value="5000"/> 
	<property name="consistencyLevel" value="ONE"/>
	<property name="serialConsistencyLevel" value="SERIAL"/> 		
</bean>

<bean id="roundRobin" class="com.datastax.driver.core.policies.RoundRobinPolicy" />
<bean id="reconnectionPolicy" class="com.datastax.driver.core.policies.ExponentialReconnectionPolicy" />
<bean id="protocolOptions" class="com.datastax.driver.core.ProtocolOptions">
 	<constructor-arg name="port" value="9042"/> 
</bean>
<bean id="localOption" class="org.metis.cassandra.PoolingOption">
		<property name="distance" value="local" />
		<property name="coreConnections" value="5" />
		<property name="maxConnections" value="10" />
		<property name="maxSimultaneousRequests" value="5" />
		<property name="minSimultaneousRequests" value="3" />
</bean>
<bean id="remoteOption" class="org.metis.cassandra.PoolingOption">
		<property name="distance" value="remote" />
		<property name="coreConnections" value="2" />
		<property name="maxConnections" value="5" />
		<property name="maxSimultaneousRequests" value="5" />
		<property name="minSimultaneousRequests" value="3" />
</bean>
<bean id="socketOptions" class="com.datastax.driver.core.SocketOptions"/>
```

<u>clusterName</u>

The **clusterName** property is used to override the default cluster name, which is taken from the bean id. This is used primarily for JMX. 

<u>clusterNode</u>

The **clusterNodes** property is used for specifying a comma-separated list of ip addresses for nodes in a Cassandra cluster. Note that all specified nodes must share the same port number. In the Cassandra vernacular, a node is also referred to as a cluster "contact point". The driver uses a cluster contact point to connect to the cluster and discover its topology. Only one node is required (the driver will automatically retrieve the addresses of the other nodes in the cluster); however, it is usually a good idea to provide more than one node, because if that single node is unavailable, the driver cannot connect to the Cassandra cluster. 

<u>loadBalancingPolicy</u>

The **loadBalancingPolicy** property is used to specify the load balancing policy to use on the discovered nodes. The default is DCAwareRoundRobinPolicy. In the above example, the default has been overriden with the RoundRobinPolicy. These are all the possible load balancing policies as of this writing: DCAwareRoundRobinPolicy, LatencyAwarePolicy, RoundRobinPolicy, TokenAwarePolicy, and  WhiteListPolicy. For more on the load balancing policy, click [here](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/LoadBalancingPolicy.html).

<u>reconnectionPolicy</u>

The **reconnectionPolicy** property is used to specify the [ReconnectionPolicy](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/ReconnectionPolicy.html) to use on the discovered nodes. The default is [ExponentialReconnectionPolicy](http://www.datastax.com/drivers/java/2.0/index.html?com/datastax/driver/core/policies/LoadBalancingPolicy.html), which is usually adequate. These are all the possible reconnect policies as of this writing: ConstantReconnectionPolicy and ExponentialReconnectionPolicy. For more on the reconnect policy, click [here](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/LoadBalancingPolicy.html).

<u>protocolOptions</u>

The **protocolOptions** property is used for specifying the [ProtocolOptions](http://www.datastax.com/drivers/java/2.0/index.html?com/datastax/driver/core/policies/ReconnectionPolicy.html) to use for the discovered nodes. If you don't specify one, a default ProtocolOption is used. ProtocolOption is typically used for overriding the default port number of  9042. 

<u>listOfPoolingOptions</u>

The **listOfPoolingOptions** property specifies a list of [PoolingOptions](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/PoolingOptions.html) for the connections to the cluster nodes.	There should only be two PoolingOption beans in the list; one for REMOTE and the other for LOCAL nodes. The driver uses connections in an asynchronous manner; meaning that multiple requests can be simultaneously submitted on, or multiplexed through, the same connection. Thus the driver only needs to maintain a relatively small number of connections to each Cassandra node. The pooling option is used to specfiy options that allow the driver to control how many connections are kept to the Cassandra nodes. For each node, the driver keeps a core pool of connections open at all times (coreConnections). If the use of those core connections reaches a configurable threshold (maxSimultaneousRequests), more connections are created up to the configurable maximum number of connections (maxConnections)). When the pool exceeds the maximum number of connections, connections in excess are reclaimed if the use of opened connections drops below the configured threshold (minSimultaneousRequests). You want to have a PoolingOption bean for LOCAL and REMOTE (HostDistance) nodes. For IGNORED nodes, the default for all those settings is 0 and cannot be changed. 

<u>socketOptions</u>

The **socketOptions** property is used for specifying low-level [SocketOptions](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/SocketOptions.html) (e.g., keepalive, solinger, etc.) for the connections to the cluster nodes. 

<u>fetchSize</u>

Use the **fetchSize** property to set the default (5000) fetch size to use for SELECT queries. During runtime, this default value can be overridden by assigning the Camel in-message the "**metis.cql.fetch.size**" header. 

<u>consistencyLevel</u>

Use the **consistencyLevel** property to set the default (ONE) consistency level to use for queries. To learn more about consistency levels click [here](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html). During runtime, this default value can be overridden by assigning the Camel in-message the "**metis.cql.consistency.level**" header. 

<u>serialConsistencyLevel</u>

Use the **serialConsistencyLevel** property to set the default (SERIAL) serial consistency level to use for queries. During runtime, this default value can be overridden by assigning the Camel in-message the "**metis.cql.serial.consistency.level**" header. 


So What or Who Is Metis?
====
*Metis was the Titaness of the forth day and the planet Mercury. She presided over all wisdom and knowledge. She was seduced by Zeus and became pregnant with Athena. Zeus became concerned over prophecies that Metis’ children would be very powerful and that her second child (a son) would overthrow Zeus. To avoid this from ever hapenning, Zeus turned Metis into a fly and swallowed her. It is said that Metis is the source for Zeus’ wisdom and that she advises Zeus from his belly.[3]*

References
====
[1] [*DataStax, Apache Casandra 2.0 Documentation*](http://www.datastax.com/documentation/articles/cassandra/cassandrathenandnow.html)  
[2] [*DataStax, Apache Casandra Java Driver 2.0 API Reference*](http://www.datastax.com/drivers/java/2.0/)  
[3] [*Greek Mythology*](http://www.greekmythology.com/Titans/Metis/metis.html)

