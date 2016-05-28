[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

<h1 id="Introduction">Introduction</h1>

[Apache Camel](http://camel.apache.org)<font size="1"><sup>TM</sup></font> is a powerful and feature-rich open source integration framework whose goal is, in part, to facilitate the implementation of enterprise integration patterns. Camel supports or implements  most of the integration patterns that are described in the book by Bobby Woolf and Gregor Hohpe entitled, ["Enterprise Integration Patterns"](http://www.enterpriseintegrationpatterns.com "EIP" ). In the Camel vernacular, patterns are also referred to as routes. Camel is the integration framework for the open source [ServiceMix](http://servicemix.apache.org) enterprise service bus (ESB); however, Camel can also be used as a standalone integration framework. Camel includes a number of different [components](http://camel.apache.org/component.html), where each component can be viewed as a connector to an API, framework, protocol, and/or data store. For example, there are components  for smtp, ftp, tcp, file, sql, jdbc, jetty, etc. There must now be over 50 different components and the number just keeps growing. Camel routes are message patterns that are used, in part, for integrating these components. For example, you may have a route that reads messages from a JMS queue and persists the messages to different database tables. The different routing possibilities are endless. The "[Camel In Action](http://www.manning.com/ibsen/)" book is a must-read for anyone getting started with Camel. 

[Apache Cassandra](http://cassandra.apache.org)<font size="1"><sup>TM</sup></font> is a massively scalable open source NoSQL database management system (DBMS) [1]. Cassandra is highly fault-tolerant and based on the Columnar or ColumnFamily data model; think of it as a highly distributed hash table. Cassandra includes a SQL-like programming language called, "[Cassandra Query Language](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html)" (CQL), which is the default and primary interface into the Cassandra DBMS. Using CQL is similar to using SQL in that the concept of a table having rows and columns is almost the same in CQL and SQL. The main difference is that Cassandra does not support joins or subqueries, except for batch analysis through Hive. Instead, Cassandra emphasizes denormalization through CQL features like collections and clustering specified at the schema level [1]. CQL is the recommended way to interact with Cassandra. The simplicity of reading and using CQL is an advantage over older Cassandra APIs.The goal of this project is to provide a highly configurable and flexible Camel component for CQL. The CQL component allows one to create Camel routes that integrate with the Cassandra DBMS. The initial release of this component supports a Camel producer (e.g., `to()`), but not consumer. The producer provides the basic CRUD (create, read, update, delete) functionality and implements the InOut message exchange pattern (MEP). 
Unlike other Camel Cassandra components (e.g., http://camel.apache.org/cassandra.html), this component completely decouples the Camel route from the CQL query statement and the corresponding [Cassandra Cluster](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Cluster.html) component; the latter of which is treated as a separate Cassandra connection pool. So one may define many Cassandra routes that can dynamically access any set of CQL queries and the queries themselves, in turn, access a Cassandra cluster. It is a set of key:value pairs, which are conveyed through the Camel Exchange message body, that binds the Echange to a particular CQL query. So a route is used as a conduit for dynamically binding Exchange messages to CQL queries. Exchange message bodies are also optional for those cases where a set of key:value pairs is not required to invoke the CQL query.  There is no need to specify a CQL query within the Exchange message nor as a component URI option.    

<h1 id="URI Format">URI Format</h1>
```
cql:clientName[?options]```   
Where "clientName" uniquely identifies a Cassandra [Client](#client) bean.  


<h1 id="Configuration">Configuration</h1>


<h2 id="CQLComponent">CQL Component</h2>


A Cassandra CQL component is configured through either one of the following:

1. The XML file that includes or defines the Camel contexts, routes, and Cassandra CQL component. As an example, see the file called "camel1.xml" in the project's test resources directory.  
2. An XML file that is external to the XML file that defines the corresponding Camel context and route(s). This external XML file is, by default, called "cassandra.xml" and it must reside in the application's class path. To override the default "cassandra.xml" file name, use the **metis.cassandra.spring.context** system property or define a CQL component bean (see example below) that specifies the 'contextFile' property. 

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

The above snippet defines a CQL component called "cql1" that is referenced by a Camel route called "camel1-route". The CQL component will search for an external configuration file called, "mycfg.xml".  

Note that if you take configuration approach #1 above, the corresponding CQL component will still look for an external "cassandra.xml" file and if one is found, the CQL component will auto-inject any [Client](#client) beans found in that external file, as well as those [Client](#client) beans found in the hosting Camel XML file (a.k.a., Camel registry). The client bean is described in the following section. 

Via either of the two configuration approaches described above, you define and configure the beans described in the following subsections. As you read through the following subsections, please keep in mind the use of [Camel property placeholders](http://camel.apache.org/using-propertyplaceholder.html), which provide for dynamic configurations.  

For example configuration files, see the set of XML files in the project's .../test/resources directory. 



<h2 id="client">Client</h2>


The [Client](#client) bean is a thread-safe component that is used by the CQL component for accessing a Cassandra cluster, a keyspace within the cluster, and any tables within the keyspace. You can define any number of client beans, and the CQL component will automatically inject itself with all the client beans that it locates within its application context. So there is no need to explicitly wire the CQL component to the client beans. 

The CQL endpoint's URI is used to identify the target client bean. For example, a URI endpoint of `cql:user` specifies the client bean with an id of "user", which may logically be used for accessing the "user" table in a Cassandra keyspace. Also see [Client Mapper](#clientmapper) for using ant-style pattern matching to identify a client bean. Using a Camel [property place holder](http://camel.apache.org/properties.html), such as `cql:{{client}}` , you can make the endpoint that much more dynamically configurable. 

Each client bean that you define must be assigned one or more [CQL statements](#cqlstatement), which can be any combination of SELECT, UPDATE, DELETE, and INSERT CQL query methods. A CQL endpoint's incoming request message (i.e., Camel in message) specifies one of the four query methods, as well as zero or more input parameters (key:value pairs). The combination of specified query method and input parameters is used to identify which one of the client's CQL statements the input parameters will get bound to. The query method is specified in the Camel Exchange's input message via a header called, "**metis.cassandra.method**". If that header is not present, the client will fall back on a default query method. The default query method can be either injected into the client or you can have the client choose a default method based on its injected CQL statements. You can inject the default method via the 'defaultMethod' property. For example:

```xml
<bean id="user" class="org.metis.cassandra.Client">
  ...
  <property name="defaultMethod">		
		<util:constant static-field="org.metis.cassandra.Client.Method.INSERT" />
  </property>
  ...
</bean> 	
```

If a default method is not injected, the client selects one based on its injected CQL statements. If the injected CQL statements comprise just one type of method, then that method will be the default method. For example, if all the injected CQL statements are of type DELETE, then DELETE will be the default method for the client. However, if there exists a combination of CQL statements (e.g., SELECT and DELETE), then a default method cannot be determined.   
 
If the incoming request message does not include input parameters, the client will choose the CQL statement not having any parameterized fields (see below); therefore, you should never have more than one non-parameterized CQL statement per CQL statement type.  The input parameters are passed in as either a Map, List of Maps, or JSON object. JSON objects are in the form of a String or InputStream object, which are transformed to either a Map or List of Maps. Please note that all incoming Camel exchanges must have the **InOut** message exchange pattern (MEP). The response message (Camel out message), which results from a SELECT, is sent back as a List of Maps, where each Map represents a row in the result set returned by Cassandra.  

So through a single CQL endpoint, you can invoke any of the CQL statements that are assigned to its corresponding client. This, therefore, precludes the endpoint from getting nailed to any one particular CQL statement for it is the method and input parameters that decide which CQL statement will be used. Here's a snippet of XML that defines a client bean called "user". 

```xml
<!-- A Client bean called user and its injected CQL statements -->
<bean id="user" class="org.metis.cassandra.Client">
 <property name="cqls">
	<list>
		<ref bean="select1" />
		<ref bean="select2" />
		<ref bean="select3" />
		<ref bean="insert1" />
	</list>
 </property>
 <property name="keyspace" value="videodb" />
 <property name="clusterBean" ref="cluster1" />
</bean>
``` 

The following describe each of the client bean's properties. 

<u>cqls</u>

The **cqls** property is used for injecting the client bean with a set of [CQL statements](#cqlstatement). In the example above, the client bean has been injected with a set of four CQL statements: three SELECTS and one INSERT. These are the referenced CQL statements:

```xml
<!-- A list of CQL statements used for supporting the user Client -->
<bean id="select1" class="org.metis.cassandra.CqlStmnt">
	<property name="statement" value="select * from users" />
	<property name="fetchSize" value="100" />
</bean>
<bean id="select2" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="select username from username_video_index where username =
				`text:username`" />
</bean>
<bean id="select3" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="select username, email from users where username =
				`text:user`" />
</bean>
<bean id="insert1" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="insert into users (username, created_date, email, firstname,
			lastname, password) values (`text:username`,
			`timestamp:created_date`, `list:text:email`,
			`text:firstname`,`text:lastname`,`text:password`)" />
</bean>
```

If a ***cqls*** property is not specified, the client bean will self-inject all CQL statements that it finds in its application context (bean factory). So the above client bean definition can be simplified as follows: 

```xml
<bean id="user" class="org.metis.cassandra.Client"> 
 <property name="keyspace" value="videodb" />
 <property name="clusterBean" ref="cluster1" />
</bean>
``` 

<u>keyspace</u>

The **keyspace** property is used to specify the name of the keyspace, within a Cassandra cluster, that the client bean is to reference. Note that this is overridden when a CQL statement provides a fully qualified keyspace name (e.g., keyspace_name.table_name) that is different than the one assigned to the client bean.  

<u>clusterBean</u>

The **clusterBean** property references a [ClusterBean](#clusterbean) that is used to connect to a Cassandra cluster. If a **clusterBean** property is not specified and there is only one ClusterBean defined in the application context, the client bean will self-inject itself with that one ClusterBean. Thus, the above client bean definition can be further simplified as follows:

```xml
<bean id="user" class="org.metis.cassandra.Client"> 
 <property name="keyspace" value="videodb" />
</bean>
``` 

<u>defaultMethod</u>

As previously described, the optional  **defaultMethod** property is used for specifying the default method to be used by the client bean. It accepts a Method enum, as defined in `org.metis.cassandra.Client.Method`. There are 4 possible types: SELECT, INSERT, DELETE, and UPDATE.  Here's an example of using the 'util' namespace for assgning a Method of INSERT.

```xml
<bean id="user" class="org.metis.cassandra.Client">
	<property name="keyspace" value="videodb" />
	<property name="clusterBean" ref="cluster1" />
	<property name="defaultMethod">		
		<util:constant static-field="org.metis.cassandra.Client.Method.INSERT" />
	</property>
</bean>
```


<h2 id="cqlstatement">CQL Statement</h2>

Before proceeding with this next bean, lets recap what has been covered. A CQL component self-injects all [Client](#client) beans that it locates and these Client beans are injected with a set of CQL statements. Recall that you can either explicitly inject a set of CQL statements into the client or you can let the client self-inject all the CQL statements that it locates in its respective application context.   
 
This section lists and describes all of the properties for the CQL statement bean. Most of these properties correlate to those found in the [Cassandra Statement](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Statement.html) class.

<u>statement</u>

The **statement** property is used to specify the actual CQL statement. Here's the same set of example statements listed in the previous section:

```xml
<!-- A list of CQL statements used for supporting the user Client -->
<bean id="select1" class="org.metis.cassandra.CqlStmnt">
	<property name="statement" value="select * from users" />
	<property name="fetchSize" value="100" />
</bean>
<bean id="select2" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="select username from username_video_index where username =
				`text:username`" />
</bean>
<bean id="select3" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="select username, email from users where username =
				`text:user`" />
</bean>
<bean id="insert1" class="org.metis.cassandra.CqlStmnt">
	<property name="statement"
		value="insert into users (username, created_date, email, firstname,
			lastname, password) values (`text:username`,
			`timestamp:created_date`, `list:text:email`,
			`text:firstname`,`text:lastname`,`text:password`)" />
</bean>
```

Note that three of the statements have fields delimited by backticks (e.g., \`list:text:email\`). These are *parameterized fields* that comprise 2 or 3 subfields, which are delimited by a ":" character. So any CQL statement with one or more parameterized fields is treated as a CQL prepared statement. A CQL prepared statement's parameterized fields are used for binding input parameters, which arrive via the CQL endpoint's incoming request message, to the prepared statement. 

The first subfield (from right-to-left) of a parameterized field is required, and it specifies the name of the input parameter that corresponds to the field. In other words, it must match the key name of a `key:value` pair that is passed in via an incoming request message. In the "select2" example statement above, "username" is the key name of the input parameter. The next subfield, which is also required, is the type of input parameter, and it must match one of the Cassandra [data types](http://www.datastax.com/drivers/java/2.1/com/datastax/driver/core/DataType.Name.html). The last subfield, which is optional, is used to specify a collection (list, map, or set). Here are a some examples:

1. **\`list:text:email\`** is a parameterized field that is matched up to an input parameter whose key name is 'email' and whose value is a list of ascii text strings. 
2. **\`text:username\`** is a parameterized field that is matched up to an input parameter whose key name is 'username' and whose value is an ascii text string. 
3. **\`tuple:userinfo\`** is a parameterized field that is matched up to an input parameter whose key name is 'userinfo' and whose value is a tuple. A Cassandra [tuple](http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/reference/tupleTypes.html) is passed in as a List of arbitrary objects. That list is used to create a Cassandra TupleType, from which a corresponding TupleValue is created and bound to the target prepared statement. ***N.B., Since tuples are passed in as a List of arbitrary object types and this component does not rely on a JSON schema, tuples cannot be passed in as part of a JSON object. Tuples must, instead, be passed in as a List that is an element of a Map whose key matches that of the parameterized field. A Tuple is returned as a List of objects. ***  

Note from [1] about using collections. <i>"Use collections, such as Set, List or Map, when you want to store or denormalize a small amount of data. Values of items in collections are limited to 64K. Other limitations also apply. Collections work well for storing data such as the phone numbers of a user and labels applied to an email. If the data you need to store has unbounded growth potential, such as all the messages sent by a user or events registered by a sensor, do not use collections. Instead, use a table having a compound primary key and store data in the clustering columns".</i>

All CQL statements for a particular statement type (query method) must be distinct with respect to the parameterized fields. If you have two statements with the same query method and those two methods have the same number of parameterized fields with matching key names, then an exception will be thrown during the Client bean's initialization. For example, these two CQL statements, when assigned to a Client bean, will result in an exception. 

````
select username from username_video_index where username =`text:username`
select first, last from user where username =`text:username`
````
Even though the two statements access different tables, they share the same number of identical parameterized fields; therefore, one is not distinct from the other with respect to the parameterized fields. The following two statements will not result in an exception.

````
select username from username_video_index where username =`text:username`
select first, last from user where username =`text:username` and `int:age` = 59
````
Even though they share one identical parameterized field, they do not have an equal number of parameterized fields.

When a request message arrives at a CQL endpoint, in the form of a Camel in-message, that specifies a SELECT method, the three **distinct** SELECT statements (from the above set) become candidates for the request. The input parameters (key:value pairs) in the request message dictate which of the three to use. For example, if the request message contains one Map with one key:value pair of `username:joe`, then the Map is bound to the second SELECT CQL statement. If the request message contains only one Map with one key:value pair of `user:joe`, then the Map is bound to the third SELECT statement. If there is no request message (i.e., the Camel exchange does not include an in body), the first CQL select statement is used because it has no parameterized fields. An exception is thrown if a match cannot be found. 

**If the in-message comprises a list of maps, then all of the maps in the payload must have the same set of key names!** If there is a list of maps, then it represents a batch UPDATE, INSERT, or DELETE. A list of maps cannot be used for a SELECT.  

<u>fetchSize</u>

The **fetchSize**, which is used exclusively by the SELECT statement, controls how many rows will be returned as part of a result set (the goal being to avoid loading too much results in memory for queries yielding large results). In other words, the fetchSize defines the page size that is returned by Cassandra. Please note that while a value as low as 1 can be used, it is **highly** discouraged to use such a low value in practice, as it will yield very poor performance. If in doubt, using the [default](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/QueryOptions.html#DEFAULT_FETCH_SIZE) is probably a good idea. You can override the **global** default size via the [QueryOptions](#queryoptions). The statement's fetchSize property takes precedence over the global default value. 

<u>pagingState</u>

The **pagingState** boolean property sets whether to use the [paging state](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/PagingState.html) when fetching result sets. If set to true, this will cause the next execution of a particular SELECT statement to fetch results from the next page of a result set, rather than restarting from the beginning. The paging state must be used across multiple invocations of the same SELECT CQL statement.  When pagingState is set to true, the CQL endpoint returns the current paging state as a String object via the out message's "**metis.cql.paging.state**" header. A subsequent invocation of the SELECT method then specifies the current paging state via the in message's "**metis.cql.paging.state**" header. The CQLStmnt is stateless, thus it is upto the calling application/route to maintain this state information across multiple invocations of the CQLStmnt. 

For more information on paging and the fetchSize, please click [here](http://datastax.github.io/java-driver/features/paging/). 

<u>consistencyLevel</u>

The **[consistencyLevel](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ConsistencyLevel.html)** property sets the consistency level for the corresponding query. 

<u>serialConsistencyLevel</u>

The **[serialConsistencyLevel](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ConsistencyLevel.html)** property sets the serial consistency level for the corresponding query. The serial consistency level is only used by conditional updates (so INSERT, UPDATE and DELETE with an IF condition). For those, the serial consistency level defines the consistency level of the serial phase (or "paxos" phase) while the normal consistency level defines the consistency for the "learn" phase, i.e. what type of reads will be guaranteed to see the update right away. The serial consistency level is ignored for any query that is not a conditional update (serial reads should use the regular consistency level for instance). 

<u>defaultTimestamp</u>

The **defaultTimestamp** property sets the default timestamp for the corresponding query (in microseconds since the epoch).
This feature is only available when version V3 or higher of the native protocol is in use. With earlier versions, this property  has no effect.

<u>idempotent</u>

The **idempotent** property sets whether this statement is idempotent. Idempotence plays a role in [speculative executions](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/SpeculativeExecutionPolicy.html). If a statement is not idempotent, the driver will not schedule speculative executions for it.

<u>retryPolicy</u>

The **[retryPolicy](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/RetryPolicy.html)** sets the retry policy to use for this query. The default retry policy, if this method is not called, is the one returned by Policies.getRetryPolicy() in the cluster configuration. This method is thus only useful in case you want to punctually override the default policy for this request.



<u>tracing</u>

When set, the **tracing** boolean property enables tracing for this statement. 


<h2 id="clientmapper">Client Mapper</h2>


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


<h2 id="clusterbean">Cluster Bean</h2>


A cluster bean, in combination with its referenced beans, is used for configuring an instance of a [Cassandra Java driver](http://docs.datastax.com/en/developer/java-driver/3.0/java-driver/whatsNew2.html), which is used for accessing a Cassandra cluster. Each client bean in the Spring context must be wired to a cluster bean. You can define any number of cluster beans; each used for accessing a different Cassandra cluster. 

The cluster bean is used to build an instance of a Cassandra [Cluster](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Cluster.html) object. The cluster bean implements the [Cluster.Initializer](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Cluster.Initializer.html) interface and uses itself to build a Cluster.

The following is an example definition of a cluster bean. Please note that not all of the cluster bean's properties are depicted in the example. Also, please refer to the driver's [API docs](http://docs.datastax.com/en/drivers/java/3.0/index.html) for more detailed information on the driver's policies and options.  


```xml
<bean id="cluster1" class="org.metis.cassandra.ClusterBean">
	
	<property name="clusterName" value="myCluster"/> 
	<property name="clusterNodes" value="127.0.0.1" />
	
	<!-- Some of the driver's policies. If one is not specified it will be 
	     defaulted. Please refer to the 3.x driver's API documentation for a 
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
        <!-- An example of how to inject an Enum of REMOTE for the HostDistance -->
		<property name="hostDistance">
		  <util:constant static-field="com.datastax.driver.core.HostDistance.LOCAL"/>
		</property>
		<property name="coreConnectionsPerHost" value="5" />
		<property name="maxConnectionsPerHost" value="10" />
		<property name="maxSimultaneousRequestsPerConnectionThreshold" value="2" />
		<property name="maxSimultaneousRequestsPerHostThreshold" value="5" />
</bean>
<bean id="remoteOption" class="org.metis.cassandra.PoolingOption">
        <property name="hostDistance">
		  <util:constant static-field="com.datastax.driver.core.HostDistance.REMOTE"/>
		</property>
		<property name="coreConnectionsPerHost" value="5" />
		<property name="maxConnectionsPerHost" value="10" />
		<property name="maxSimultaneousRequestsPerConnectionThreshold" value="2" />
		<property name="maxSimultaneousRequestsPerHostThreshold" value="5" />
</bean>

<bean id="socketOptions" class="com.datastax.driver.core.SocketOptions"/>


<bean id="queryOptions" class="com.datastax.driver.core.QueryOptions">
  <property name="consistencyLevel">
    <util:constant static-field="com.datastax.driver.core.ConsistencyLevel.ONE"/>
  </property>
</bean>

</bean>


```

<u>clusterName</u>

The **clusterName** property is used to override the default cluster name, which is taken from the bean id. This is used primarily for JMX. 

<u>clusterNodes</u>

The **clusterNodes** property is used for specifying a comma-separated list of ip addresses for nodes in a Cassandra cluster. Note that all specified nodes must share the same port number. In the Cassandra vernacular, a node is also referred to as a cluster [contact point](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/Cluster.Builder.html#addContactPoint-java.lang.String-). The driver uses a cluster contact point to connect to the cluster and discover its topology. Only one node is required (the driver will automatically retrieve the addresses of the other nodes in the cluster); however, it is usually a good idea to provide more than one node, because if that single node is unavailable, the driver cannot connect to the Cassandra cluster. 

<u>Policies</u>

The different policies supported by the driver. 

- LoadBalancingPolicy
- ReconnectionPolicy
- RetryPolicy
- AddressTranslater
- SpeculativeExecutionPolicy
- TimestampGenerator

The example "cluster1" bean above depicts how these policies can be injected into the cluser bean.

Please refer to the 3.x Java driver's API documentantion for a complete list of its [policies](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/policies/Policies.html) and their descriptions. 

<u>protocolOptions</u>

The **protocolOptions** property is used for specifying the [ProtocolOptions](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/ProtocolOptions.html) to use for the discovered nodes. If you don't specify one, a default ProtocolOptions is used. ProtocolOptions is typically used for overriding the default port number of  9042. 

<u>poolingOptions</u>

The [options](http://docs.datastax.com/en/drivers/java/3.0/com/datastax/driver/core/PoolingOptions.html) associated with connection pooling. 

<u>listOfPoolingOptions</u>

The **listOfPoolingOptions** property specifies a list of org.org.metis.cassandra.PoolingOption beans that are injected into the [PoolingOptions](http://www.datastax.com/drivers/java/3.0/com/datastax/driver/core/PoolingOptions.html). You want to have a PoolingOption bean for LOCAL and REMOTE (HostDistance) nodes. For IGNORED nodes, the default for all those settings is 0 and cannot be changed. The cluster bean will inject this list of PoolingOption beans into the PoolingOptions. 

<u>socketOptions</u>

The **socketOptions** property is used for specifying low-level [SocketOptions](http://www.datastax.com/drivers/java/3.0/com/datastax/driver/core/SocketOptions.html) (e.g., keepalive, solinger, etc.) for the connections to the cluster nodes. 


<h2 id="queryoptions"> </h2>
<u>queryOptions</u>

The **queryOptions** property is used for specifying options for the queries. For more details see [QueryOptions](http://www.datastax.com/drivers/java/3.0/com/datastax/driver/core/QueryOptions.html).   


<h1 id="WhoMetis">So Who or What Was Metis?</h1>


*Metis was the Titaness of the forth day and the planet Mercury. She presided over all wisdom and knowledge. She was seduced by Zeus and became pregnant with Athena. Zeus became concerned over prophecies that Metis’ children would be very powerful and that her second child (a son) would overthrow Zeus. To avoid this from ever hapenning, Zeus turned Metis into a fly and swallowed her. It is said that Metis is the source for Zeus’ wisdom and that she advises Zeus from his belly.[3]*


<h1 id="References">References</h1>

[1] [*DataStax, Apache Casandra 2.0 Documentation*](http://www.datastax.com/documentation/articles/cassandra/cassandrathenandnow.html)  
[2] [*DataStax, Apache Casandra Java Driver 2.0 API Reference*](http://www.datastax.com/drivers/java/2.0/)  
[3] [*Greek Mythology*](http://www.greekmythology.com/Titans/Metis/metis.html)

