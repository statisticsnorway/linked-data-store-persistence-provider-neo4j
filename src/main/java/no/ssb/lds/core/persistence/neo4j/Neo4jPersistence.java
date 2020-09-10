package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.specification.Specification;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static no.ssb.lds.core.persistence.neo4j.Neo4jCreationalPatternFactory.hashOf;

public class Neo4jPersistence implements RxJsonPersistence {

    private final Neo4jTransactionFactory transactionFactory;
    private final Neo4jCreationalPatternFactory creationalPatternFactory;

    public Neo4jPersistence(Neo4jTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
        this.creationalPatternFactory = new Neo4jCreationalPatternFactory();
    }

    private static JsonDocument createJsonDocument(Map<String, FlattenedDocumentLeafNode> leafNodesByPath,
                                                   DocumentKey documentKey, boolean deleted) {
        if (deleted) {
            return new JsonDocument(documentKey, (JsonNode) null);
        }
        FlattenedDocument flattenedDocument = new FlattenedDocument(documentKey, leafNodesByPath, deleted);
        JsonNode jsonObject = new FlattenedDocumentToJson(flattenedDocument).toJsonNode();
        return new JsonDocument(documentKey, jsonObject);
    }

    /**
     * Compute path from Record.
     */
    static String getPathFromRecord(Record record) {
        Node rootNode = record.get("m").asNode();
        if (rootNode.containsKey("deleted") && rootNode.get("deleted").asBoolean(false)) {
            // Empty path if deleted.
            return "$";
        } else {
            List<Relationship> embeds = record.get("l").asList(Value::asRelationship);
            return computePath(embeds);
        }
    }

    /**
     * Convert the {@link Record} to {@link FlattenedDocumentLeafNode}.
     * <p>
     * Documents marked as deleted will be returned as empty.
     * TODO: I think this is different in other implementations.
     */
    static FlattenedDocumentLeafNode getLeafFromRecord(Record record, DocumentKey documentKey) {

        Node rootNode = record.get("m").asNode();
        if (rootNode.containsKey("deleted") && rootNode.get("deleted").asBoolean(false)) {
            return new FlattenedDocumentLeafNode(documentKey, null, FragmentType.DELETED, null, Integer.MAX_VALUE);
        }

        String pathWithIndices = getPathFromRecord(record);

        Value value = record.get("e");
        if (value.isNull()) {
            return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.NULL, null, Integer.MAX_VALUE);
        }

        Node valueNode = value.asNode();
        if (valueNode.containsKey("id")) {
            // ref-target node
            String linkTargetEntity = valueNode.labels().iterator().next();
            String id = valueNode.get("id").asString();
            String link = "/" + linkTargetEntity + "/" + id;
            return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.STRING, link, Integer.MAX_VALUE);
        } else {
            Value typeValue = valueNode.get("type");
            String type = typeValue.asString();
            Value valueValue = valueNode.get("value");
            if ("string".equals(type)) {
                String stringValue = valueValue.asString();
                return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.STRING, stringValue, Integer.MAX_VALUE);
            } else if ("number".equals(type)) {
                String stringValue = String.valueOf(valueValue.asNumber());
                return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.NUMERIC, stringValue, Integer.MAX_VALUE);
            } else if ("boolean".equals(type)) {
                String stringValue = String.valueOf(valueValue.asBoolean());
                return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.BOOLEAN, stringValue, Integer.MAX_VALUE);
            } else if ("map".equals(type)) {
                return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.EMPTY_OBJECT, null, Integer.MAX_VALUE);
            } else if ("array".equals(type)) {
                return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.EMPTY_ARRAY, null, Integer.MAX_VALUE);
            } else {
                throw new IllegalStateException("type not supported: " + typeValue.asString());
            }
        }
    }

    /**
     * Create a {@link DocumentKey} from a record.
     */
    static DocumentKey getKeyFromRecord(Record record, String namespace, String entityName) {
        String docId = record.get("r").get("id").asString();
        ZonedDateTime version = record.get("v").get("from").asZonedDateTime();
        return new DocumentKey(namespace, entityName, docId, version);
    }

    /**
     * Convert {@link Record}s stream {@link JsonDocument} stream.
     */
    static Flowable<JsonDocument> toDocuments(Flowable<Record> records, String nameSpace, String entityName) {
        return records.groupBy(record -> {
            // Group by id and version.
            return getKeyFromRecord(record, nameSpace, entityName);
        }).concatMapEager(recordById -> {
            // For each group create a
            DocumentKey key = recordById.getKey();
            return recordById.toMap(record -> {
                // Compute path.
                return getPathFromRecord(record);
            }, record -> getLeafFromRecord(record, key)).map(values -> {
                // Un-flatten.
                boolean deleted = values.size() == 1 && values.values().iterator().next().type() == FragmentType.DELETED;
                return createJsonDocument(new TreeMap<>(values), key, deleted);
            }).toFlowable();
        });
    }

    static String computePath(List<Relationship> embeds) {
        StringBuilder completePath = new StringBuilder("$");
        for (Relationship embedRel : embeds) {
            String pathElement = embedRel.get("path").asString();
            if ("[]".equals(pathElement)) {
                int index = embedRel.get("index").asNumber().intValue();
                completePath.append("[").append(index).append("]");
            } else {
                completePath.append(".").append(pathElement);
            }
        }
        return completePath.toString();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return transactionFactory.createTransaction(readOnly);
    }

    @Override
    public Completable createOrOverwrite(Transaction transaction, Flowable<JsonDocument> documentFlowable, Specification specification) {
        Neo4jTransaction tx = (Neo4jTransaction) transaction;

        Map<String, List<JsonDocument>> documentsByEntity = new LinkedHashMap<>();
        List<JsonDocument> documents = documentFlowable.toList().blockingGet();
        for (JsonDocument document : documents) {
            documentsByEntity.computeIfAbsent(document.key().entity(), e -> new ArrayList<>()).add(document);
        }
        List<Completable> completables = new ArrayList<>();
        for (Map.Entry<String, List<JsonDocument>> entry : documentsByEntity.entrySet()) {
            String entity = entry.getKey();
            List<JsonDocument> documentForEntityList = entry.getValue();
            Neo4jQueryAndParams qp = creationalPatternFactory.creationalQueryAndParams(specification, entity, documentForEntityList);
            completables.add(tx.executeCypherAsync(qp.query, qp.params).ignoreElements());
        }
        return Completable.merge(completables);
    }

    @Override
    public Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, JsonNavigationPath path, String value, Range<String> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("snapshot", snapshot);
        params.put("limit", Integer.MAX_VALUE);
        params.put("path", path.serialize());
        // TODO refactor API.
        Object objectValue = value;
        if (objectValue instanceof String) {
            String str = (String) objectValue;
            // TODO: Make configurable.
            if (str.length() > 40) {
                params.put("value", hashOf(str));
            } else {
                params.put("value", str);
            }
        } else if (objectValue instanceof Boolean) {
            params.put("value", objectValue);
        } else if (objectValue instanceof Integer) {
            params.put("value", objectValue);
        } else if (objectValue instanceof Long) {
            params.put("value", objectValue);
        } else if (objectValue instanceof Float) {
            params.put("value", objectValue);
        } else if (objectValue instanceof Double) {
            params.put("value", objectValue);
        } else {
            throw new UnsupportedOperationException("Value type not supported: " + value.getClass().getName());
        }
        cypher.append("MATCH (e :").append(entityName).append("_E {path: $path, hashOrValue: $value})<-[:EMBED*]-(m)<-[v:VERSION]-(r) ");
        cypher.append("WHERE v.from <= $snapshot AND $snapshot < v.to WITH r, v, m ORDER BY r.id LIMIT $limit\n");
        cypher.append("OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e");
        return toDocuments(neoTx.executeCypherAsync(cypher.toString(), params), namespace, entityName);
    }

    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;

        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rid", id);
        params.put("snapshot", snapshot);
        cypher.append("MATCH (r :").append(entityName).append(" {id: $rid})-[v:VERSION]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to ");
        cypher.append("WITH r, v, m OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName).firstElement();
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {

        Map<String, Object> params = new LinkedHashMap<>();

        List<String> conditions = new ArrayList<>();
        conditions.add("TRUE");

        if (range.hasAfter()) {
            conditions.add("r.id > $idAfter");
            params.put("idAfter", range.getAfter());
        }
        if (range.hasBefore()) {
            conditions.add("r.id < $idBefore");
            params.put("idBefore", range.getBefore());
        }

        // Swap the order so that we only use limit.
        String orderBy = " ORDER BY r.id " + (range.isBackward() ? "DESC " : "");
        String limit = "";
        if (range.isLimited()) {
            limit = " LIMIT $limit ";
            params.put("limit", range.getLimit());
        }

        params.put("snapshot", snapshot);

        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();

        cypher.append("MATCH (r :").append(entityName).append(") WHERE ").append(String.join(" AND ", conditions)).append(" WITH r ").append(orderBy).append("\n");
        cypher.append("MATCH (r)-[v:VERSION]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to").append(" ");
        cypher.append("WITH r, v, m ").append(limit);
        cypher.append("OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName);
    }

    @Override
    public Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id, Range<ZonedDateTime> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rid", id);

        // Construct where clauses based on the range.
        // First find the first one between ]after:before[
        // Then filter the results such as ]after:from:before[


        List<String> conditions = new ArrayList<>();
        if (range.hasAfter()) {
            params.put("snapshotFrom", range.getAfter());
            conditions.add("$snapshotFrom < v.from");
        }

        if (range.hasBefore()) {
            params.put("snapshotTo", range.getBefore());
            conditions.add("v.from < $snapshotTo");
        }

        String where = conditions.isEmpty() ? " " : " WHERE " + String.join(" AND ", conditions);

        // Swap the order so that we only use limit.
        String orderBy = range.isBackward() ? " ORDER BY r.id, v.from DESC" : " ORDER BY r.id, v.from";
        String limit = "";
        if (range.isLimited()) {
            limit = " LIMIT $limit";
            // Obs, since we are getting the r node, we need to bump limit by one.
            params.put("limit", range.getLimit());
        }

        cypher.append("MATCH (r :").append(entityName).append(" {id: $rid})-[v:VERSION]->(m) \n");
        cypher.append("WITH r, v, m ").append(orderBy).append(where).append("\n");
        cypher.append("WITH r, v, m ").append(limit).append(" OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName);
    }

    @Override
    public Flowable<JsonDocument> readTargetDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String entityName, String id, JsonNavigationPath jsonNavigationPath,
                                                      String targetEntityName, Range<String> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        try {
            Map<String, Object> parameters = new LinkedHashMap<>();

            parameters.put("snapshot", snapshot);
            parameters.put("path", jsonNavigationPath.popBack().serialize());
            parameters.put("lastPathElement", jsonNavigationPath.back());
            parameters.put("id", id);

            String orderBy = "ORDER BY r.id" + (range.isBackward() ? " DESC\n" : "\n");

            String limit = "";
            if (range.isLimited()) {
                limit = "LIMIT $limit\n";
                parameters.put("limit", range.getLimit());
            }

            String afterCondition = "";
            if (range.hasAfter()) {
                afterCondition = "AND $after < r.id\n";
                parameters.put("after", range.getAfter());
            }
            String beforeCondition = "";
            if (range.hasBefore()) {
                beforeCondition = "AND r.id < $before\n";
                parameters.put("before", range.getBefore());
            }
            String query = (
                    "MATCH   (elem:%{entityName} {id: $id})-[version:VERSION]->\n" +
                            "(root:%{entityName}_E)-[:EMBED*]->\n" +
                            "(edge:%{entityName}_E {path:$path})-[:REF {path: $lastPathElement}]->\n" +
                            "(r:" + targetEntityName + ")\n" +
                            "WHERE  version.from <= $snapshot AND $snapshot < version.to \n" +
                            afterCondition +
                            beforeCondition +
                            "WITH r\n" +
                            orderBy +
                            limit +
                            "MATCH (r)-[v:VERSION]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to " +
                            "WITH r, v, m OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e"
            ).replace("%{entityName}", entityName);

            return toDocuments(neoTx.executeCypherAsync(query, parameters), ns, targetEntityName);
        } catch (Exception ex) {
            return Flowable.error(ex);
        }
    }

    @Override
    public Flowable<JsonDocument> readSourceDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String targetEntityName, String targetId,
                                                      JsonNavigationPath relationPath, String sourceEntityName,
                                                      Range<String> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        try {
            Map<String, Object> parameters = new LinkedHashMap<>();

            parameters.put("snapshot", snapshot);
            parameters.put("path", relationPath.popBack().serialize());
            parameters.put("lastPathElement", relationPath.back());
            parameters.put("id", targetId);

            String orderBy = "ORDER BY r.id" + (range.isBackward() ? " DESC\n" : "\n");

            String limit = "";
            if (range.isLimited()) {
                limit = "LIMIT $limit\n";
                parameters.put("limit", range.getLimit());
            }

            String afterCondition = "";
            if (range.hasAfter()) {
                afterCondition = "AND $after < source.id\n";
                parameters.put("after", range.getAfter());
            }
            String beforeCondition = "";
            if (range.hasBefore()) {
                beforeCondition = "AND source.id < $before\n";
                parameters.put("before", range.getBefore());
            }
            String query = (
                    "MATCH (edge:%{sourceEntityName}_E {path:$path})-[REF {path: $lastPathElement}]->(target:%{targetEntityName} {id: $id}),\n" +
                            "      (root:%{sourceEntityName}_E)-[*0..]->(edge:%{sourceEntityName}_E),\n" +
                            "      (source:%{sourceEntityName})-[version:VERSION]->(root)\n" +
                            "WHERE  version.from <= $snapshot AND $snapshot < version.to \n" +
                            afterCondition +
                            beforeCondition +
                            "WITH source as r\n" +
                            orderBy +
                            limit +
                            "MATCH (r)-[v:VERSION]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to \n" +
                            "WITH r, v, m OPTIONAL MATCH p=(m)-[:EMBED|REF*]->(e) RETURN r, v, m, relationships(p) AS l, e\n"
            ).replace("%{sourceEntityName}", sourceEntityName).replace("%{targetEntityName}", targetEntityName);

            return toDocuments(neoTx.executeCypherAsync(query, parameters), ns, sourceEntityName);
        } catch (Exception ex) {
            return Flowable.error(ex);
        }
    }

    @Override
    public Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entityName).append(" {id: $rid})-[:VERSION {from:$version}]->(m) OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE m, e ");
        cypher.append("WITH r MATCH (r) WHERE NOT (r)--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id, "version", version)).ignoreElements();
    }

    @Override
    public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entity).append(" {id: $rid}) OPTIONAL MATCH (r)-[:VERSION]->(m) OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE m, e ");
        cypher.append("WITH r MATCH (r) WHERE NOT (r)<--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id)).ignoreElements();
    }

    @Override
    public Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entity).append(") OPTIONAL MATCH (r)-[:VERSION]->(m) OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE m, e ");
        cypher.append("WITH 1 AS a MATCH (r:").append(entity).append(") WHERE NOT (r)<--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Collections.emptyMap()).ignoreElements();
    }

    @Override
    public Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        Neo4jTransaction tx = (Neo4jTransaction) transaction;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MERGE (r:").append(entityName).append(" {id: $rid}) WITH r\n");
        cypher.append("OPTIONAL MATCH (r:").append(entityName).append(")-[v:VERSION]->() WHERE v.from <= $version AND $version < v.to WITH r, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r:").append(entityName).append(")-[v:VERSION]->() WHERE v.from > $version WITH r, prevVersion, v AS nextVersion ORDER BY v.from LIMIT 1\n");
        cypher.append("MERGE (r)-[v:VERSION {from: $version, to: coalesce(prevVersion.to, nextVersion.from, datetime('9999-01-01T00:00:00.0Z[Etc/UTC]'))}]->(m:").append(entityName).append("_E {type:'map', path:'$'})\n");
        cypher.append("SET prevVersion.to = $version, m.deleted = true\n");
        cypher.append("WITH m OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE e");

        return tx.executeCypherAsync(cypher.toString(), Map.of("rid", id, "version", version)).ignoreElements();
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return readDocuments(tx, snapshot, ns, entityName, Range.lastBefore(1, id)).isEmpty().map(wasEmpty -> !wasEmpty);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return readDocuments(tx, snapshot, ns, entityName, Range.firstAfter(1, id)).isEmpty().map(wasEmpty -> !wasEmpty);
    }

    @Override
    public void close() throws PersistenceException {
        transactionFactory.close();
    }
}
