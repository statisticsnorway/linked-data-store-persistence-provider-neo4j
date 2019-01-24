package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.TransactionFactory;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class Neo4jTransactionFactory implements TransactionFactory {

    private final Driver driver;
    private final boolean logCypher;

    public Neo4jTransactionFactory(Driver driver, boolean logCypher) {
        this.driver = driver;
        this.logCypher = logCypher;
    }

    @Override
    public Neo4jTransaction createTransaction(boolean readOnly) throws PersistenceException {
        Session session = driver.session(readOnly ? AccessMode.READ : AccessMode.WRITE);
        return new Neo4jTransaction(session, logCypher);
    }

    @Override
    public void close() {
        driver.close();
    }
}
