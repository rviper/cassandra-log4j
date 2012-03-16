package at.it_fabrik.cassandra.log4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Log4J appender saving messages to a cassandra cluster
 * 
 * @author starzer
 * 
 */
public class Log4jAppender extends AppenderSkeleton {

    private final static String CLUSTER_NAME = "log-cluster";

    /**
     * cassandra configuration
     */
    private String hosts;
    private String instanceId;
    private String keyspace;
    private String columnFamily;
    private int replicationFactor = 1;

    /**
     * active cluster connection
     */
    private Cluster cluster;
    private ColumnFamilyTemplate<String, String> client;

    /**
     * logger states
     */
    private boolean startedUp = false;
    private boolean shutdown = false;

    private long lastPublishTime = -1;

    public Log4jAppender() {
	super();
	Runnable r = new Runnable() {

	    public void run() {
		while (!shutdown) {
		    try {
			Thread.sleep(1000);
			if (client == null) {
			    connect();
			}
			if (lastPublishTime > 50) {
			    // TODO: some metrics to deactivate logging to this
			    // appender if its getting too time consuming, or
			    // queue
			}
		    } catch (Exception e) {
			LogLog.error("Log4jAppender, " + e.getMessage());
		    } finally {
			startedUp = true;
		    }
		}
	    }
	};
	Thread alive = new Thread(r, "Cassandra-Log4J-Alive");
	alive.setDaemon(true);
	alive.start();
    }

    @Override
    protected void append(LoggingEvent event) {
	long startTime = System.currentTimeMillis();
	String message = event.getMessage() + "";
	if (startedUp && client != null) {
	    UUID key = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	    ColumnFamilyUpdater<String, String> updater = client.createUpdater(key.toString());
	    updater.setLong("time", event.getTimeStamp());
	    updater.setString("host", getHost());
	    updater.setString("message", message);
	    updater.setString("level", event.getLevel() + "");
	    updater.setString("name", event.getLoggerName());
	    updater.setString("thread", event.getThreadName());
	    try {
		client.update(updater);
	    } catch (HectorException e) {
		client = null;
		LogLog.error("append failed, " + e.getMessage());
	    }
	} else {
	    if (startedUp) {
		LogLog.warn("Log4jAppender, " + "cluster not available, skipping logging, " + message);
	    }
	}
	long endTime = System.currentTimeMillis();
	lastPublishTime = endTime - startTime;
    }

    public void close() {
	shutdown = true;
	cluster.getConnectionManager().shutdown();
    }

    private void connect() throws HectorException {
	LogLog.debug("creating cassandra cluster connection");
	CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(hosts);
	cassandraHostConfigurator.setMaxActive(20);
	cassandraHostConfigurator.setCassandraThriftSocketTimeout(500);
	cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(500);
	Cluster c = HFactory.getOrCreateCluster(CLUSTER_NAME, cassandraHostConfigurator);
	KeyspaceDefinition keyspaceDef = c.describeKeyspace(keyspace);
	if (keyspaceDef == null) {
	    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, columnFamily, ComparatorType.BYTESTYPE);
	    KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
		    Arrays.asList(cfDef));
	    c.addKeyspace(newKeyspace, false);
	}
	Keyspace ksp = HFactory.createKeyspace(keyspace, c);
	cluster = c;
	client = new ThriftColumnFamilyTemplate<String, String>(ksp, columnFamily, StringSerializer.get(), StringSerializer.get());
    }

    private String getHost() {
	if (instanceId != null) {
	    return instanceId;
	}
	try {
	    String host = InetAddress.getLocalHost().getHostAddress();
	    return host;
	} catch (UnknownHostException e) {
	}
	return "";
    }

    public boolean requiresLayout() {
	return false;
    }

    public void setColumnFamily(String columnFamily) {
	this.columnFamily = columnFamily;
    }

    public void setHosts(String hosts) {
	this.hosts = hosts;
    }

    public void setInstanceId(String instanceId) {
	this.instanceId = instanceId;
    }

    public void setKeyspace(String keyspace) {
	this.keyspace = keyspace;
    }

    public void setReplicationFactor(int replicationFactor) {
	this.replicationFactor = replicationFactor;
    }

}
