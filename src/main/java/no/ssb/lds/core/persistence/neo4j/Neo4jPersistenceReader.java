package no.ssb.lds.core.persistence.neo4j;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class Neo4jPersistenceReader {

    private final Session session;
    private final Transaction transaction;
    private final String defaultNamespace;
    private final boolean logCypher;

    Neo4jPersistenceReader(Session session, Transaction transaction, String defaultNamespace, boolean logCypher) {
        this.session = session;
        this.transaction = transaction;
        this.defaultNamespace = defaultNamespace;
        this.logCypher = logCypher;
    }

    String nodeLabel(String namespace, String entity, String classifier) {
        return (defaultNamespace.equals(namespace) ? "" : namespace + "__") + entity + ("M".equals(classifier) ? "" : "__" + classifier);
    }

    StatementResult executeCypher(String query, Object... keysAndValues) {
        Value params = Values.parameters(keysAndValues);
        if (logCypher) {
            String resolvedQuery = query;
            for (Map.Entry<String, Object> entry : params.asMap().entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    resolvedQuery = resolvedQuery.replaceAll("[$]" + entry.getKey(), "'" + value + "'");
                }
            }
            System.out.format("%s\n", resolvedQuery);
        }
        if (transaction != null) {
            return transaction.run(query, params);
        }
        return session.run(query, params);
    }

    JSONObject getEntity(String namespace, String entity, String id) {
        String query = String.format("MATCH (m:%s {id: $id}) RETURN m;", nodeLabel(namespace, entity, "M"));
        StatementResult result = executeCypher(query, "id", id);
        if (!result.hasNext()) {
            return null;
        }
        Value value = result.next().get(0);
        if (value.isNull()) {
            return null;
        }
        Node node = value.asNode();
        if (!node.containsKey("data")) {
            return null; // void node
        }
        String data = new String(node.get("data").asByteArray(), StandardCharsets.UTF_8);
        return new JSONObject(data);
    }

    JSONArray getEntities(String namespace, String entity) {
        String query = String.format("MATCH (m:%s) RETURN m LIMIT 250;", nodeLabel(namespace, entity, "M"));
        StatementResult result = executeCypher(query);
        JSONArray jsonArray = new JSONArray();
        while (result.hasNext()) {
            Value value = result.next().get(0);
            if (value.isNull()) {
                jsonArray.put(JSONObject.NULL);
            } else {
                Node node = value.asNode();
                if (node.containsKey("data")) {
                    String data = new String(node.get("data").asByteArray(), StandardCharsets.UTF_8);
                    jsonArray.put(new JSONObject(data));
                } else {
                    // void node
                }
            }
        }
        return jsonArray;
    }
}
