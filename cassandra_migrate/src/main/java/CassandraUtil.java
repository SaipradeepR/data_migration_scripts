
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraUtil.class);

    private Cluster cluster;
    private Session session;
    String host ; int port;
    public CassandraUtil(String host, int port) {
        this.host = host;
        this.port = port;
        this.cluster = Cluster.builder()
                .addContactPoints(host)
                .withPort(port)
                .withoutJMXReporting()
                .build();
        this.session = cluster.connect();
    }

    public Row findOne(String query) {
        try {
            ResultSet rs = session.execute(query);
            return rs.one();
        } catch (DriverException ex) {
            logger.error("findOne - Error while executing query {} :: ", query, ex);
            reconnect();
            return findOne(query);
        }
    }

    public List<Row> find(String query) {
        try {
            ResultSet rs = session.execute(query);
            return rs.all();
        } catch (DriverException ex) {
            reconnect();
            return find(query);
        }
    }

    public boolean upsert(String query) {
        ResultSet rs = session.execute(query);
        return rs.wasApplied();
    }

    public UserType getUDTType(String keyspace, String typeName) {
        return session.getCluster().getMetadata().getKeyspace(keyspace).getUserType(typeName);
    }

    public void reconnect() {
        session.close();
        Cluster newCluster = Cluster.builder().addContactPoint(host)
                .withPort(port).build();
        this.session = newCluster.connect();
    }

    public void close() {
        session.close();
    }

    public boolean update(Statement query) {
        ResultSet rs = session.execute(query);
        return rs.wasApplied();
    }

    public List<Row> executePreparedStatement(String query, Object... params) {
        ResultSet rs = session.execute(session.prepare(query).bind(params));
        return rs.all();
    }
}

