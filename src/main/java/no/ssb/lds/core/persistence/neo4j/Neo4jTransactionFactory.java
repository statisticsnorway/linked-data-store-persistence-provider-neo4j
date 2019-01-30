package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Neo4jTransactionFactory implements TransactionFactory {

    private final Driver driver;
    private final boolean logCypher;

    public Neo4jTransactionFactory(Driver driver, boolean logCypher) {
        this.driver = driver;
        this.logCypher = logCypher;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        return null;
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

    <T> T writeTransaction(Function<org.neo4j.driver.v1.Transaction, T> work) {
        boolean committed = false;
        try (Session session = driver.session(AccessMode.WRITE)) {
            T result = session.writeTransaction(tx -> work.apply(tx));
            committed = true;
            return result;
        } catch (PersistenceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PersistenceException(e);
        } finally {
            if (logCypher) {
                if (committed) {
                    System.out.println("WRITE COMMITED");
                } else {
                    System.out.println("WRITE ROLLED-BACK");
                }
            }
        }
    }

    <T> T readTransaction(Function<org.neo4j.driver.v1.Transaction, T> work) {
        boolean committed = false;
        try (Session session = driver.session(AccessMode.READ)) {
            T result = session.readTransaction(tx -> work.apply(tx));
            committed = true;
            return result;
        } catch (PersistenceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PersistenceException(e);
        } finally {
            if (logCypher) {
                if (committed) {
                    System.out.println("READ COMMITED");
                } else {
                    System.out.println("READ ROLLED-BACK");
                }
            }
        }
    }

    <T> T readAutoCommit(Function<Session, T> work) {
        boolean committed = false;
        try (Session session = driver.session(AccessMode.READ)) {
            T result = work.apply(session);
            committed = true;
            return result;
        } catch (PersistenceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PersistenceException(e);
        } finally {
            if (logCypher) {
                if (committed) {
                    System.out.println("READ COMMITED");
                } else {
                    System.out.println("READ ROLLED-BACK");
                }
            }
        }
    }
}
