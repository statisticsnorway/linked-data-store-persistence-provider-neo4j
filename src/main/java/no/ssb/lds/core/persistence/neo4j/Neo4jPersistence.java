package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.batch.Batch;
import no.ssb.lds.api.persistence.batch.ExpressionVisitor;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.flattened.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.specification.Specification;
import org.neo4j.driver.Record;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static no.ssb.lds.core.persistence.neo4j.Constants.DELETED_FIELD;
import static no.ssb.lds.core.persistence.neo4j.Constants.EMPTY_ARRAY_FIELD_PREFIX;
import static no.ssb.lds.core.persistence.neo4j.Constants.MSGPACK_FIELD;
import static no.ssb.lds.core.persistence.neo4j.Neo4jCreationalPatternFactory.mapper;

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
        if (rootNode.containsKey(DELETED_FIELD) && rootNode.get(DELETED_FIELD).asBoolean(false)) {
            // Empty path if deleted.
            return "$";
        } else {
            Path path = record.get("p").asPath();
            return computePath(List.of(path));
        }
    }

    /**
     * Convert the {@link Record} to {@link FlattenedDocumentLeafNode}.
     * <p>
     * Documents marked as deleted will be returned as empty.
     */
    static Collection<FlattenedDocumentLeafNode> getLeafsFromRootNodeOfRecord(Record record, DocumentKey documentKey) {
        if (record.get("m").isNull()) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, "$", FragmentType.NULL, null, Integer.MAX_VALUE));
        }

        Node rootNode = record.get("m").asNode();
        if (rootNode.containsKey(DELETED_FIELD) && rootNode.get(DELETED_FIELD).asBoolean(false)) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, "$", FragmentType.DELETED, null, Integer.MAX_VALUE));
        }

        String pathWithIndices = "$";

        return extractLeafsFromNode(documentKey, rootNode, pathWithIndices);
    }

    /**
     * Convert the {@link Record} to {@link FlattenedDocumentLeafNode}.
     * <p>
     * Documents marked as deleted will be returned as empty.
     */
    static Collection<FlattenedDocumentLeafNode> getLeafsFromRecord(Record record, DocumentKey documentKey) {

        if (record.get("p").isNull()) {
            return Collections.emptyList();
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
            return extractLeafsFromNode(documentKey, valueNode, pathWithIndices);
        }
    }

    private static List<FlattenedDocumentLeafNode> extractLeafsFromNode(DocumentKey documentKey, Node rootNode, String pathWithIndices) {
        List<FlattenedDocumentLeafNode> result = new ArrayList<>();
        for (Map.Entry<String, Object> valueByFieldName : rootNode.asMap().entrySet()) {
            List<FlattenedDocumentLeafNode> leafs = extractLeafNodesFromField(documentKey, pathWithIndices, valueByFieldName);
            result.addAll(leafs);
        }
        return result;
    }

    private static List<FlattenedDocumentLeafNode> extractLeafNodesFromField(DocumentKey documentKey, String pathWithIndices, Map.Entry<String, Object> valueByFieldName) {
        String fieldName = valueByFieldName.getKey();
        if (fieldName.startsWith(EMPTY_ARRAY_FIELD_PREFIX)) {
            String emptyArrayFieldName = fieldName.substring(EMPTY_ARRAY_FIELD_PREFIX.length());
            return List.of(new FlattenedDocumentLeafNode(documentKey, pathWithIndices + "." + emptyArrayFieldName, FragmentType.EMPTY_ARRAY, "[]", Integer.MAX_VALUE));
        }
        if (fieldName.startsWith(MSGPACK_FIELD)) {
            return Collections.emptyList();
        }
        Object fieldValue = valueByFieldName.getValue();
        String finalPathWithIndices = pathWithIndices + "." + fieldName;
        if (fieldValue instanceof String) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.STRING, (String) fieldValue, Integer.MAX_VALUE));
        } else if (fieldValue instanceof Number) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.NUMERIC, String.valueOf(fieldValue), Integer.MAX_VALUE));
        } else if (fieldValue instanceof Boolean) {
            return List.of(new FlattenedDocumentLeafNode(documentKey, finalPathWithIndices, FragmentType.BOOLEAN, String.valueOf(fieldValue), Integer.MAX_VALUE));
        } else if (fieldValue instanceof List) {
            List arrayFieldValue = (List) fieldValue;
            if (arrayFieldValue.isEmpty()) {
                return Collections.emptyList();
            }
            int i = 0;
            List<FlattenedDocumentLeafNode> result = new ArrayList<>(arrayFieldValue.size());
            for (Object arrayElement : arrayFieldValue) {
                String arrayElementPath = finalPathWithIndices + "[" + i + "]";
                if (arrayElement instanceof String) {
                    result.add(new FlattenedDocumentLeafNode(documentKey, arrayElementPath, FragmentType.STRING, (String) arrayElement, Integer.MAX_VALUE));
                } else if (arrayElement instanceof Number) {
                    result.add(new FlattenedDocumentLeafNode(documentKey, arrayElementPath, FragmentType.NUMERIC, String.valueOf(arrayElement), Integer.MAX_VALUE));
                } else if (arrayElement instanceof Boolean) {
                    result.add(new FlattenedDocumentLeafNode(documentKey, arrayElementPath, FragmentType.BOOLEAN, String.valueOf(arrayElement), Integer.MAX_VALUE));
                } else {
                    throw new IllegalStateException("type not supported: " + fieldValue.getClass().getName());
                }
                i++;
            }
            return result;
        }
        throw new IllegalStateException("type not supported: " + fieldValue.getClass().getName());
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
        Map<DocumentKey, Collection<FlattenedDocumentLeafNode>> leafsFromRootByDocumentKey = new LinkedHashMap<>();
        return records.groupBy(record ->
                getKeyFromRecord(record, nameSpace, entityName))
                .concatMapEager(recordById ->
                        recordById.flatMap(record -> {
                            List<FlattenedDocumentLeafNode> leafs = new ArrayList<>();
                            leafsFromRootByDocumentKey.computeIfAbsent(recordById.getKey(), k -> {
                                Collection<FlattenedDocumentLeafNode> leafsFromRootNodeOfRecord = getLeafsFromRootNodeOfRecord(record, recordById.getKey());
                                leafs.addAll(leafsFromRootNodeOfRecord); // merge with first record only
                                return leafsFromRootNodeOfRecord;
                            });
                            leafs.addAll(getLeafsFromRecord(record, recordById.getKey()));
                            return Flowable.fromIterable(leafs);
                        })
                                .toList()
                                .map(listOfLeafNodes -> listOfLeafNodes.stream().collect(Collectors.toMap(FlattenedDocumentLeafNode::path, Function.identity())))
                                .map(map -> createJsonDocument(map, recordById.getKey(), Optional.ofNullable(map.get("$"))
                                        .map(FlattenedDocumentLeafNode::type)
                                        .map(type -> type == FragmentType.DELETED)
                                        .orElse(Boolean.FALSE)))
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

    public Flowable<String> resolveMatchInBatchGroup(Transaction tx, Batch.DeleteGroup group, String namespace, Specification specification) {
        final StringBuilder where = new StringBuilder();
        final AtomicInteger pnum = new AtomicInteger();
        final Map<String, Object> params = new LinkedHashMap<>();
        group.evaluate(new ExpressionVisitor() {

            @Override
            public void enterMatch(ObjectNode matchNode) {
            }

            @Override
            public void leaveMatch(ObjectNode matchNode) {
            }

            @Override
            public void enterAnd(ObjectNode andNode, boolean first, boolean last) {
                if (!first) {
                    where.append(" AND ");
                }
                where.append("(");
            }

            @Override
            public void leaveAnd(ObjectNode andNode, boolean first, boolean last) {
                where.append(")");
            }

            @Override
            public void enterOr(ObjectNode orNode, boolean first, boolean last) {
                if (!first) {
                    where.append(" OR ");
                }
                where.append("(");
            }

            @Override
            public void leaveOr(ObjectNode orNode, boolean first, boolean last) {
                where.append(")");
            }

            @Override
            public void enterNot(ObjectNode notNode) {
                where.append("NOT (");
            }

            @Override
            public void leaveNot(ObjectNode notNode) {
                where.append(")");
            }

            @Override
            public void enterIdIn(TextNode idInNode, boolean first, boolean last) {
                if (first) {
                    pnum.incrementAndGet();
                }
                String paramIdentifier = "p" + pnum.get();
                if (first && last) {
                    where.append("r.id = $").append(paramIdentifier);
                    params.put(paramIdentifier, idInNode.textValue());
                } else {
                    if (first) {
                        where.append("any(x IN $").append(paramIdentifier).append(" WHERE r.id = x)");
                        params.put(paramIdentifier, new ArrayList<>());
                    }
                    List<String> list = (List<String>) params.get(paramIdentifier);
                    list.add(idInNode.textValue());
                }
            }

            @Override
            public void leaveIdIn(TextNode idInNode, boolean first, boolean last) {
            }

            @Override
            public void enterIdNotIn(TextNode idNotInNode, boolean first, boolean last) {
                if (first) {
                    pnum.incrementAndGet();
                }
                String paramIdentifier = "p" + pnum.get();
                if (first && last) {
                    where.append("r.id <> $").append(paramIdentifier);
                    params.put(paramIdentifier, idNotInNode.textValue());
                } else {
                    if (first) {
                        where.append("none(x IN $").append(paramIdentifier).append(" WHERE r.id = x)");
                        params.put(paramIdentifier, new ArrayList<>());
                    }
                    List<String> list = (List<String>) params.get(paramIdentifier);
                    list.add(idNotInNode.textValue());
                }
            }

            @Override
            public void leaveIdNotIn(TextNode idNotInNode, boolean first, boolean last) {
            }

            @Override
            public void enterIdStartsWith(TextNode startsWithNode) {
                String paramIdentifier = "p" + pnum.incrementAndGet();
                where.append("r.id STARTS WITH $").append(paramIdentifier);
                params.put(paramIdentifier, startsWithNode.textValue());
            }

            @Override
            public void leaveIdStartsWith(TextNode startsWithNode) {
            }
        });
        if (params.isEmpty()) {
            return Flowable.empty();
        }
        StringBuilder cypher = new StringBuilder();
        String whereCriteria = where.toString();
        cypher.append("MATCH (r:").append(group.type()).append("_R:RESOURCE) WHERE ").append(whereCriteria).append("\n");
        cypher.append("MATCH (r").append(")<-[v:VERSION_OF]-(i) WHERE v.from <= $version AND COALESCE($version < v.to, true) AND COALESCE(NOT i.").append(DELETED_FIELD).append(", true)\n");
        cypher.append("RETURN r.id AS id");
        params.put("version", group.getTimestamp());
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        return neoTx.executeCypherAsync(cypher.toString(), params).map(record -> record.get("id").asString());
    }

    @Override
    public Completable deleteBatchGroup(Transaction transaction, Batch.DeleteGroup group, String namespace, Specification specification) {
        if (group.entries().isEmpty()) {
            return Completable.complete();
        }
        Neo4jTransaction tx = (Neo4jTransaction) transaction;
        StringBuilder cypher = new StringBuilder();
        cypher.append("UNWIND $entries AS entry\n");
        cypher.append("MERGE (r:").append(group.type()).append("_R:RESOURCE {id: entry.id}) WITH r, entry.version AS version\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF {from: version}]-(m) OPTIONAL MATCH (m)-[*]->(e:EMBEDDED) DETACH DELETE m, e WITH r, version\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from <= version AND COALESCE(version < v.to, true) WITH r, v AS prevVersion, version\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from > version WITH r, prevVersion, min(v.from) AS nextVersionFrom, version\n");
        cypher.append("CREATE (r)<-[v:VERSION_OF {from: version, to: coalesce(prevVersion.to, nextVersionFrom)}]-(m:")
                .append(group.type()) // TODO do we need to label delete marker with all interfaces of the entity type?
                .append(":INSTANCE").append(")\n");
        cypher.append("SET prevVersion.to = version, m.").append(DELETED_FIELD).append(" = true\n");
        List<Map<String, Object>> entries = new ArrayList<>();
        for (Batch.Entry entry : group.entries()) {
            entries.add(Map.of("id", entry.id(), "version", entry.timestamp()));
        }
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("entries", entries);
        return tx.executeCypherAsync(cypher.toString(), params).ignoreElements();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;

        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rid", id);
        params.put("snapshot", snapshot);
        cypher.append("MATCH (r :").append(entityName).append("_R {id: $rid})<-[v:VERSION_OF]-(m) WHERE v.from <= $snapshot AND coalesce($snapshot < v.to, true) ");
        cypher.append("RETURN r, v, m");

        Maybe<Record> records = neoTx.executeCypherAsync(cypher.toString(), params).firstElement();
        return records.map(record -> {
            DocumentKey key = getKeyFromRecord(record, ns, entityName);
            if (!record.get("m").get(DELETED_FIELD).isNull() && record.get("m").get(DELETED_FIELD).asBoolean()) {
                return new JsonDocument(key, (JsonNode) null);
            }
            JsonNode document = mapper.readTree(record.get("m").get(MSGPACK_FIELD).asByteArray());
            return new JsonDocument(key, document);
        });
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {

        Map<String, Object> params = new LinkedHashMap<>();

        List<String> conditions = new ArrayList<>();

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

        cypher.append("MATCH (r :").append(entityName).append("_R)");
        if (conditions.size() > 0) {
            cypher.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        cypher.append(" WITH r ").append(orderBy).append("\n");
        cypher.append("MATCH (r)<-[v:VERSION_OF]-(m) WHERE v.from <= $snapshot AND coalesce($snapshot < v.to, true)").append(" ");
        cypher.append("RETURN r, v, m ").append(limit);

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return records.map(record -> {
            DocumentKey key = getKeyFromRecord(record, ns, entityName);
            if (!record.get("m").get(DELETED_FIELD).isNull() && record.get("m").get(DELETED_FIELD).asBoolean()) {
                return new JsonDocument(key, (JsonNode) null);
            }
            JsonNode document = mapper.readTree(record.get("m").get(MSGPACK_FIELD).asByteArray());
            return new JsonDocument(key, document);
        });
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
        cypher.append(where).append("\n");
        cypher.append("RETURN r, v, m").append(orderBy).append(limit);

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return records.map(record -> {
            DocumentKey key = getKeyFromRecord(record, ns, entityName);
            if (!record.get("m").get(DELETED_FIELD).isNull() && record.get("m").get(DELETED_FIELD).asBoolean()) {
                return new JsonDocument(key, (JsonNode) null);
            }
            JsonNode document = mapper.readTree(record.get("m").get(MSGPACK_FIELD).asByteArray());
            return new JsonDocument(key, document);
        });
    }

    @Override
    public Flowable<JsonDocument> readTargetDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String entityName, String id, JsonNavigationPath jsonNavigationPath,
                                                      String targetEntityName, Range<String> range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Flowable<JsonDocument> readSourceDocuments(Transaction tx, ZonedDateTime snapshot, String ns,
                                                      String targetEntityName, String targetId,
                                                      JsonNavigationPath relationPath, String sourceEntityName,
                                                      Range<String> range) {
        throw new UnsupportedOperationException();
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
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF {from: $version}]-(m) OPTIONAL MATCH (m)-[*]->(e:EMBEDDED) DETACH DELETE m, e WITH r\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from <= $version AND COALESCE($version < v.to, true) WITH r, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from > $version WITH r, prevVersion, min(v.from) AS nextVersionFrom\n");
        cypher.append("CREATE (r)<-[v:VERSION_OF {from: $version, to: coalesce(prevVersion.to, nextVersionFrom)}]-(m:")
                .append(entityName) // TODO do we need to label delete marker with all interfaces of the entity type?
                .append(":INSTANCE").append(")\n");
        cypher.append("SET prevVersion.to = $version, m.").append(DELETED_FIELD).append(" = true\n");
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

    @Override
    public <T> T getInstance(Class<T> clazz) {
        if (clazz.isAssignableFrom(org.neo4j.driver.Driver.class)) {
            return (T) transactionFactory.driver;
        }
        return null;
    }
}
