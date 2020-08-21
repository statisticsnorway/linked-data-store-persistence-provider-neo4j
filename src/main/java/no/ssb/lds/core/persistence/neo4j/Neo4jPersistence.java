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
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        FlattenedDocument flattenedDocument = new FlattenedDocument(documentKey, leafNodesByPath, deleted);
        JsonNode jsonObject = new FlattenedDocumentToJson(flattenedDocument).toJsonNode();
        return new JsonDocument(documentKey, jsonObject);
    }

    /**
     * Compute path from Record.
     */
    static String getPathFromRecord(Record record) {
        Node rootNode = record.get("m").asNode();
        if (rootNode.containsKey("_deleted") && rootNode.get("_deleted").asBoolean(false)) {
            // Empty path if deleted.
            return "$";
        } else {
            List<Path> paths = record.get("p").asList(Value::asPath);
            return computePath(paths);
        }
    }

    /**
     * Convert the {@link Record} to {@link FlattenedDocumentLeafNode}.
     * <p>
     * Documents marked as deleted will be returned as empty.
     * TODO: I think this is different in other implementations.
     */
    static Collection<FlattenedDocumentLeafNode> getLeafsFromRecord(Record record, DocumentKey documentKey) {

        if (record.get("m").isNull()) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, null, FragmentType.NULL, null, Integer.MAX_VALUE));
        }

        Node rootNode = record.get("m").asNode();
        if (rootNode.containsKey("_deleted") && rootNode.get("_deleted").asBoolean(false)) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, null, FragmentType.DELETED, null, Integer.MAX_VALUE));
        }

        if (record.get("p").isNull()) {
            throw new UnsupportedOperationException("TODO: support ddocuments with only root-node"); // TODO
        }

        Path path = record.get("p").asPath();
        Node valueNode = path.end();

        String pathWithIndices = getPathFromRecord(record);

        if (path.end().hasLabel("RESOURCE")) {
            // ref-target path
            Iterator<String> labelIterator = valueNode.labels().iterator();
            String linkTargetEntity = labelIterator.next();
            if (linkTargetEntity.equals("RESOURCE")) {
                linkTargetEntity = labelIterator.next();
            }
            linkTargetEntity = linkTargetEntity.substring(0, linkTargetEntity.length() - "_R".length());
            String id = valueNode.get("id").asString();
            String link = "/" + linkTargetEntity + "/" + id;
            return List.of(new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.STRING, link, Integer.MAX_VALUE));
        } else {
            List<FlattenedDocumentLeafNode> result = new ArrayList<>();
            for (Map.Entry<String, Object> valueByFieldName : valueNode.asMap().entrySet()) {
                String finalPathWithIndices = pathWithIndices + "." + valueByFieldName.getKey();
                Value typeValue = valueNode.get("type");
                String type = typeValue.asString();
                Value valueValue = valueNode.get("value");
                if ("string".equals(type)) {
                    String stringValue = valueValue.asString();
                    result.add(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.STRING, stringValue, Integer.MAX_VALUE));
                } else if ("number".equals(type)) {
                    String stringValue = String.valueOf(valueValue.asNumber());
                    result.add(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.NUMERIC, stringValue, Integer.MAX_VALUE));
                } else if ("boolean".equals(type)) {
                    String stringValue = String.valueOf(valueValue.asBoolean());
                    result.add(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.BOOLEAN, stringValue, Integer.MAX_VALUE));
                } else if ("map".equals(type)) {
                    result.add(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.EMPTY_OBJECT, null, Integer.MAX_VALUE));
                } else if ("array".equals(type)) {
                    result.add(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.EMPTY_ARRAY, null, Integer.MAX_VALUE));
                } else {
                    throw new IllegalStateException("type not supported: " + typeValue.asString());
                }
            }
            return result;
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
        return records.groupBy(record ->
                getKeyFromRecord(record, nameSpace, entityName))
                .concatMapEager(recordById ->
                        recordById.flatMap(record -> Flowable.fromIterable(getLeafsFromRecord(record, recordById.getKey())))
                                .toList()
                                .map(listOfLeafNodes -> listOfLeafNodes.stream().collect(Collectors.toMap(FlattenedDocumentLeafNode::path, Function.identity())))
                                .map(map -> createJsonDocument(map, recordById.getKey(), false))
                                .toFlowable()
                );
    }

    static String computePath(List<Path> paths) {
        StringBuilder completePath = new StringBuilder("$");
        if (paths == null || paths.isEmpty()) {
            return completePath.toString();
        }
        Path path = paths.get(0);
        for (Relationship relationship : path.relationships()) {
            completePath.append(".").append(relationship.type());
            if (relationship.containsKey("index")) {
                int index = relationship.get("index").asNumber().intValue();
                completePath.append("[").append(index).append("]");
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
        // TODO Use navigation path to construct query navigation, hopefully we can avoid using graph-ql schema
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
        cypher.append("OPTIONAL MATCH p=(m)-[*]->(e) WHERE e <> r RETURN r, v, m, p");
        return toDocuments(neoTx.executeCypherAsync(cypher.toString(), params), namespace, entityName);
    }

    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;

        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rid", id);
        params.put("snapshot", snapshot);
        cypher.append("MATCH (r :").append(entityName).append("_R {id: $rid})<-[v:VERSION_OF]-(m) WHERE v.from <= $snapshot AND coalesce($snapshot < v.to, true) ");
        cypher.append("WITH r, v, m OPTIONAL MATCH p=(m)-[*]->(e) WHERE e <> r RETURN r, v, m, p");

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

        cypher.append("MATCH (r :").append(entityName).append("_R) WHERE ").append(String.join(" AND ", conditions)).append(" WITH r ").append(orderBy).append("\n");
        cypher.append("MATCH (r)<-[v:VERSION_OF]-(m) WHERE v.from <= $snapshot AND coalesce($snapshot < v.to, true)").append(" ");
        cypher.append("WITH r, v, m ").append(limit);
        cypher.append("OPTIONAL MATCH p=(m)-[*]->(e) WHERE e <> r RETURN r, v, m, p");

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

        cypher.append("MATCH (r :").append(entityName).append("_R {id: $rid})<-[v:VERSION_OF]-(m) \n");
        cypher.append("WITH r, v, m ").append(orderBy).append(where).append("\n");
        cypher.append("WITH r, v, m ").append(limit).append(" OPTIONAL MATCH p=(m)-[*]->(e) WHERE e <> r RETURN r, v, m, p");

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
            // TODO Fix this
            String query = (
                    "MATCH   (elem:%{entityName}_R {id: $id})<-[v:VERSION_OF]-(root)\n" +
                            " OPTIONAL MATCH (root)-[*]->\n" +
                            "(edge:%{entityName}_E {path:$path})-[:REF {path: $lastPathElement}]->\n" +
                            "(r:" + targetEntityName + "_R)\n" +
                            "WHERE  v.from <= $snapshot AND coalesce($snapshot < v.to, true) \n" +
                            afterCondition +
                            beforeCondition +
                            "WITH r\n" +
                            orderBy +
                            limit +
                            "MATCH (r)<-[v:VERSION_OF]-(m) WHERE v.from <= $snapshot AND coalesce($snapshot < v.to, true) " +
                            "WITH r, v, m OPTIONAL MATCH p=(m)-[*]->(e) WHERE e <> r RETURN r, v, m, relationships(p) AS l, e"
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
            // TODO fix this
            String query = (
                    "MATCH (edge:%{sourceEntityName}_E {path:$path})-[REF {path: $lastPathElement}]->(target:%{targetEntityName} {id: $id}),\n" +
                            "      (root:%{sourceEntityName}_E)-[*0..]->(edge:%{sourceEntityName}_E),\n" +
                            "      (source:%{sourceEntityName})-[version:VERSION_OF]->(root)\n" +
                            "WHERE  version.from <= $snapshot AND $snapshot < version.to \n" +
                            afterCondition +
                            beforeCondition +
                            "WITH source as r\n" +
                            orderBy +
                            limit +
                            "MATCH (r)-[v:VERSION_OF]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to \n" +
                            "WITH r, v, m OPTIONAL MATCH p=(m)-[*]->(e:EMBEDDED) RETURN r, v, m, relationships(p) AS l, e\n"
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
        cypher.append("MATCH (r:").append(entityName).append("_R {id: $rid})<-[:VERSION_OF {from:$version}]->(m) OPTIONAL MATCH (m)-[*]->(e:EMBEDDED) DETACH DELETE m, e ");
        cypher.append("WITH r MATCH (r) WHERE NOT (r)--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id, "version", version)).ignoreElements();
    }

    @Override
    public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entity).append("_R {id: $rid}) OPTIONAL MATCH (r)<-[:VERSION_OF]-(m) OPTIONAL MATCH (m)-[*]->(e:EMBEDDED) DETACH DELETE m, e ");
        cypher.append("WITH r MATCH (r) WHERE NOT (r)--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id)).ignoreElements();
    }

    @Override
    public Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entity).append("_R) OPTIONAL MATCH (r)<-[:VERSION_OF]-(m) OPTIONAL MATCH (m)-[*]->(e:EMBEDDED) DETACH DELETE m, e ");
        cypher.append("WITH 1 AS a MATCH (r:").append(entity).append("_R) WHERE NOT (r)--() DELETE r");
        return neoTx.executeCypherAsync(cypher.toString(), Collections.emptyMap()).ignoreElements();
    }

    @Override
    public Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        Neo4jTransaction tx = (Neo4jTransaction) transaction;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MERGE (r:").append(entityName).append("_R:RESOURCE {id: $rid}) WITH r\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF {from: $version}]-(m)-[*]->(e:EMBEDDED) DETACH DELETE m, e WITH r\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from <= $version AND COALESCE($version < v.to, true) WITH r, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from > $version WITH r, prevVersion, min(v.from) AS nextVersionFrom\n");
        cypher.append("CREATE (r)<-[v:VERSION_OF {from: $version, to: coalesce(prevVersion.to, nextVersionFrom)}]-(m:")
                .append(entityName) // TODO do we need to label delete marker with all interfaces of the entity type?
                .append(":INSTANCE").append(")\n");
        cypher.append("SET prevVersion.to = $version, m._deleted = true\n");
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
