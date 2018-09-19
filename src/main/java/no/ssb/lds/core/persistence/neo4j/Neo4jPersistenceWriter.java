package no.ssb.lds.core.persistence.neo4j;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import org.json.JSONObject;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Neo4jPersistenceWriter {

    private final Transaction transaction;
    private final String defaultNamespace;
    private final boolean logCypher;

    Neo4jPersistenceWriter(Transaction transaction, String defaultNamespace, boolean logCypher) {
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
        return transaction.run(query, params);
    }

    boolean createOrOverwriteEntity(Set<OutgoingLink> links, String namespace, String entity, String id, JSONObject jsonObject) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE (m:").append(nodeLabel(namespace, entity, "M")).append(" {id: $id})");
        sb.append(" ON CREATE SET m.name = $name, m.uri = $uri, m.data = $data");
        sb.append(" ON MATCH SET m.data = $data\n");
        int i = 0;
        List<Object> params = new ArrayList<>(8 + 5 * links.size());
        params.add("id");
        params.add(id);
        params.add("name");
        params.add(String.format("%s/%s", entity, id));
        params.add("data");
        String jsonStr = jsonObject.toString();
        params.add(jsonStr.getBytes(StandardCharsets.UTF_8));
        params.add("uri");
        params.add(String.format("/%s/%s/%s", namespace, entity, id));
        Deque<String> nodeIdentifiers = new LinkedList<>();
        for (OutgoingLink link : links) {
            sb.append("MERGE (t").append(i).append(":").append(nodeLabel(namespace, link.edgeEntity, "M")).append(" {id: $tid").append(i).append("})");
            sb.append(" MERGE (m)-[r").append(i).append(":").append(link.relationshipName).append("]->(t").append(i).append(")");
            sb.append(" ON CREATE SET r").append(i).append(".uri = $ruri").append(i).append("\n");
            nodeIdentifiers.add("t" + i);
            params.add("tid" + i);
            params.add(link.edgeId);
            params.add("ruri" + i);
            params.add(link.relationshipURI);
            i++;
        }
        nodeIdentifiers.addFirst("m");
        String linkAllNodeIdentifiersStr = String.join(", ", nodeIdentifiers);
        nodeIdentifiers.removeFirst();
        String linkTargetNodeIdentifiersStr = String.join(", ", nodeIdentifiers);
        sb.append("WITH ").append(linkAllNodeIdentifiersStr);
        sb.append(" MATCH (m)-[r]->(x) ");
        if (linkTargetNodeIdentifiersStr.length() > 0) {
            sb.append("WHERE NOT x IN [").append(linkTargetNodeIdentifiersStr).append("] ");
        }
        sb.append("DELETE r;");
        StatementResult statementResult = executeCypher(sb.toString(), params.toArray());
        boolean atLeastOneNodeCreated = statementResult.consume().counters().nodesCreated() > 0;
        return atLeastOneNodeCreated;
    }

    boolean deleteEntity(String namespace, String entity, String id, PersistenceDeletePolicy deletePolicy) {

        if (PersistenceDeletePolicy.DELETE_INCOMING_LINKS.equals(deletePolicy)) {
            return deleteNodeAndAllRelationships(namespace, entity, id);

        } else if (PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS.equals(deletePolicy)) {
            String query = String.format("MATCH (m:%s {id: $id})<-[r]-() RETURN count(r);", nodeLabel(namespace, entity, "M"));
            StatementResult incomingRelationshipCountResult = executeCypher(query, "id", id);
            if (incomingRelationshipCountResult.single().get(0).asNumber().longValue() > 0) {
                throw new PersistenceException("Unable to delete managed-resource, there is at least one incoming link");
            }
            return deleteNodeAndAllRelationships(namespace, entity, id);

        } else if (PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES.equals(deletePolicy)) {
            throw new UnsupportedOperationException("Cascading delete not supported.");

        } else {
            throw new UnsupportedOperationException("Unsupported delete-policy: " + deletePolicy.name());
        }
    }

    boolean deleteNodeAndAllRelationships(String namespace, String entity, String id) {
        String query = String.format("MATCH (m:%s {id: $id}) DETACH DELETE m;", nodeLabel(namespace, entity, "M"));
        StatementResult result = executeCypher(query, "id", id);
        int nodesDeleted = result.consume().counters().nodesDeleted();
        return nodesDeleted > 0;
    }
}
