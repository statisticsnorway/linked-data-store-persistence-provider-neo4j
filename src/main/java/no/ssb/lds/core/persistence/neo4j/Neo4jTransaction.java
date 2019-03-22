package no.ssb.lds.core.persistence.neo4j;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionStatistics;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.time.temporal.Temporal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;

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

    org.neo4j.driver.v1.Transaction getNeo4jTransaction() {
        return this.neo4jTransaction;
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

    Flowable<Record> executeCypherAsync(String query, Map<String, Object> parameters) {
        if (logCypher) {
            String interactiveQuery = query;
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                String regex = "\\$" + entry.getKey();
                String replacement = serializeForInteractive(entry.getValue());
                interactiveQuery = interactiveQuery.replaceAll(regex, Matcher.quoteReplacement(replacement));
            }
            System.out.format("\n:: CYPHER (ASYNC) ::\n%s\n", interactiveQuery);
        }
        return Flowable.create(emitter -> {
            CompletionStage<StatementResultCursor> cursor = neo4jTransaction.runAsync(query, parameters);
            cursor.whenComplete((resultCursor, runThrowable) -> {
                if (runThrowable != null) {
                    emitter.onError(runThrowable);
                } else {
                    // Note, forEachAsync unwrap the series of nextAsync() using whenCompleteAsync. This is why the
                    // calling thread comes from ForkJoinPool.
                    CompletionStage<ResultSummary> cs = resultCursor.forEachAsync(emitter::onNext);
                    cs.whenComplete((resultSummary, throwable) -> {
                        if (throwable != null) {
                            emitter.onError(throwable);
                        } else {
                            emitter.onComplete();
                        }
                    });
                }

            });

        }, BackpressureStrategy.BUFFER);
    }

    private String serializeForInteractive(Object parameters) {
        StringBuilder sb = new StringBuilder();
        doSerializeRecursively(sb, parameters);
        return sb.toString();
    }

    private void doSerializeRecursively(StringBuilder sb, Object input) {
        if (input == null) {
            sb.append("NULL");
            return;
        }
        if (input instanceof Map) {
            sb.append("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) input).entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(entry.getKey()).append(": ");
                doSerializeRecursively(sb, entry.getValue());
            }
            sb.append("}");
            return;
        }
        if (input instanceof List) {
            sb.append("[");
            boolean first = true;
            for (Object o : (List<Object>) input) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                doSerializeRecursively(sb, o);
            }
            sb.append("]");
            return;
        }
        if (input instanceof CharSequence) {
            sb.append("'").append(input).append("'");
            return;
        }
        if (input instanceof Number) {
            sb.append(input);
            return;
        }
        if (input instanceof Temporal) {
            sb.append("datetime('").append(input).append("')");
        }
    }

    StatementResult executeCypher(String query, Map<String, Object> params) {
        if (logCypher) {
            String interactiveQuery = query;
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                interactiveQuery = interactiveQuery.replaceAll("\\$" + entry.getKey(), Matcher.quoteReplacement(serializeForInteractive(entry.getValue())));
            }
            System.out.format("\n:: CYPHER ::\n%s\n", interactiveQuery);
        }
        StatementResult statementResult = neo4jTransaction.run(query, params);
        return statementResult;
    }
}
