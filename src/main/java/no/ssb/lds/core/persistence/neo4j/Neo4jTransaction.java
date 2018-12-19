package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionStatistics;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class Neo4jTransaction implements Transaction {

    private final boolean logCypher;
    private final org.neo4j.driver.v1.Transaction neo4jTransaction;
    private final TransactionStatistics statistics = new TransactionStatistics();
    private final CompletableFuture<TransactionStatistics> result;

    Neo4jTransaction(Session session, boolean logCypher) {
        this.logCypher = logCypher;
        this.neo4jTransaction = session.beginTransaction();
        this.result = new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        if (!result.isDone()) {
            neo4jTransaction.commitAsync().thenAccept(v -> result.complete(statistics)).exceptionally(t -> {
                result.completeExceptionally(t);
                return null;
            });
        }
        return result;
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        if (!result.isDone()) {
            neo4jTransaction.rollbackAsync().thenAccept(v -> result.complete(statistics)).exceptionally(t -> {
                result.completeExceptionally(t);
                return null;
            });
        }
        return result;
    }

    StatementResult executeCypher(String query, Object... keysAndValues) {
        LinkedHashMap<String, Object> params = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            String key = (String) keysAndValues[i];
            Object value = keysAndValues[i + 1];
            params.put(key, value);
        }
        return executeCypher(query, params);
    }

    StatementResult executeCypher(String query, Map<String, Object> params) {
        if (logCypher) {
            System.out.format("\n:: CYPHER ::\n%s\n", query);
            System.out.format(":: DATA ::\n%s\n", params);
        }
        StatementResult statementResult = neo4jTransaction.run(query, params);
        return statementResult;
    }
}
