package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

class Neo4JCORDPersistence implements Persistence {

    private final Neo4jPersistenceProvider provider;
    private final boolean logCypher;

    Neo4JCORDPersistence(Neo4jPersistenceProvider provider, boolean logCypher) {
        this.provider = provider;
        this.logCypher = logCypher;
    }

    <T> T writeTransaction(Function<Transaction, T> work) {
        boolean committed = false;
        try (Session session = provider.driver().session(AccessMode.WRITE)) {
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

    <T> T readTransaction(Function<Transaction, T> work) {
        boolean committed = false;
        try (Session session = provider.driver().session(AccessMode.READ)) {
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
        try (Session session = provider.driver().session(AccessMode.READ)) {
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

    @Override
    public boolean createOrOverwrite(String namespace, String entity, String id, JSONObject jsonObject, Set<OutgoingLink> links) throws PersistenceException {
        return writeTransaction(tx -> {
            Neo4jPersistenceWriter writer = provider.newWriter(tx);
            boolean entityCreated = writer.createOrOverwriteEntity(links, namespace, entity, id, jsonObject);
            return entityCreated;
        });
    }

    @Override
    public JSONObject read(String namespace, String entity, String id) throws PersistenceException {
        // return readAutoCommit(session -> provider.newReader(session, null).getEntity(namespace, entity, id));
        return readTransaction(tx -> provider.newReader(null, tx).getEntity(namespace, entity, id));
    }

    @Override
    public boolean delete(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return writeTransaction(tx -> provider.newWriter(tx).deleteEntity(namespace, entity, id, policy));
    }

    @Override
    public JSONArray findAll(String namespace, String entity) throws PersistenceException {
        // return readAutoCommit(session -> provider.newReader(session, null).getEntities(namespace, entity));
        return readTransaction(tx -> provider.newReader(null, tx).getEntities(namespace, entity));
    }

    @Override
    public void close() throws PersistenceException {
        if (provider.driver() instanceof Closeable) {
            try {
                ((Closeable) provider.driver()).close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
