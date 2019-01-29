package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
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
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.json.JSONObject;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Neo4jPersistence implements RxJsonPersistence {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Neo4jTransactionFactory transactionFactory;

    public Neo4jPersistence(Neo4jTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    static String traverseSpecificationAndGenerateCypherCreateStatement(SpecificationElement specificationElement, Map<String, Object> params, DocumentKey key, JsonNode node) {
        StringBuilder cypher = new StringBuilder();
        cypher.append("MERGE (r:").append(key.entity()).append(" {id: $rid}) WITH r\n");
        cypher.append("OPTIONAL MATCH (r:").append(key.entity()).append(")-[v:VERSION {from: $version}]->(m)-[:EMBED*]->(e) DETACH DELETE m, e WITH r\n");
        cypher.append("OPTIONAL MATCH (r:").append(key.entity()).append(")-[v:VERSION]->() WHERE v.from <= $version AND $version < v.to WITH r, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r:").append(key.entity()).append(")-[v:VERSION]->() WHERE v.from > $version WITH r, prevVersion, v AS nextVersion ORDER BY v.from LIMIT 1\n");
        cypher.append("FOREACH(d IN $data |\n");
        cypher.append("  MERGE (r)-[v:VERSION {from: $version, to: coalesce(prevVersion.to, nextVersion.from, datetime('9999-01-01T00:00:00.0Z[Etc/UTC]'))}]->(m:").append(key.entity()).append("_E {type:'map', path:'$'})\n");
        cypher.append("  SET prevVersion.to = $version\n");
        params.put("rid", key.id());
        params.put("version", key.timestamp());
        int i = 0;
        for (Map.Entry<String, SpecificationElement> entry : specificationElement.getProperties().entrySet()) {
            traverseSpecification(cypher, key.entity(), entry.getValue(), 1, "    ", "m", "m" + i, "d[" + i + "]");
            i++;
        }
        cypher.append(")");
        List<Object> data = new ArrayList<>();
        params.put("data", data);
        convertJsonDocumentToMultiDimensionalCypherData(data, key, node, specificationElement);
        return cypher.toString();
    }

    private static void traverseSpecification(StringBuilder cypher, String entity, SpecificationElement element, int depth, String indentation, String parentNodeIdentifier, String nodeIdentifier, String dataListIdentifier) {
        String path = pathOf(element);
        cypher.append(indentation).append("FOREACH(d").append(depth).append(" IN ").append(dataListIdentifier).append(" |\n");
        indentation += "  ";
        dataListIdentifier = "d" + depth;
        String relationPath = element.getName();
        boolean parentIsArray = isArrayElementNode(nodeIdentifier);
        if (parentIsArray) {
            relationPath = "[]";
        }
        cypher.append(indentation).append("CREATE (").append(parentNodeIdentifier).append(")-[:EMBED {path: '").append(relationPath).append("'");
        if (parentIsArray) {
            cypher.append(", index: ").append(dataListIdentifier).append("[0]");
        }
        cypher.append("}]->");
        if (element.getJsonTypes().contains("object")) {
            cypher.append("(").append(nodeIdentifier).append(":").append(entity).append("_E {type: 'map', path: '").append(path).append("'})\n");
            int i = 0;
            for (Map.Entry<String, SpecificationElement> entry : element.getProperties().entrySet()) {
                String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "") + "[" + i + "]";
                String childNodeIdentifier = nodeIdentifier + "p" + i;
                traverseSpecification(cypher, entity, entry.getValue(), depth + 1, indentation, nodeIdentifier, childNodeIdentifier, childDataListIdentifier);
                i++;
            }
        } else if (element.getJsonTypes().contains("array")) {
            cypher.append("(").append(nodeIdentifier).append(":").append(entity).append("_E {type:'array', path: '").append(path).append("'})\n");
            String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "");
            String childNodeIdentifier = nodeIdentifier + "i";
            traverseSpecification(cypher, entity, element.getItems(), depth + 1, indentation, nodeIdentifier, childNodeIdentifier, childDataListIdentifier);
        } else {
            // value node

            // start node and properties
            cypher.append("(").append(nodeIdentifier).append(":").append(entity).append("_E {");

            // type property
            String jsonType = element.getJsonTypes().contains("string") ? "string" : element.getJsonTypes().contains("numeric") ? "numeric" : element.getJsonTypes().contains("boolean") ? "boolean" : element.getJsonTypes().stream().collect(Collectors.joining(","));
            cypher.append("type: '").append(jsonType).append("'");

            // path property
            cypher.append(", path: '").append(path).append("'");

            // value property
            cypher.append(", value: ");
            cypher.append(dataListIdentifier);
            if (parentIsArray) {
                cypher.append("[1]");
            }
            cypher.append("[0]"); // value

            // hashOrValue property
            cypher.append(", hashOrValue: ");
            cypher.append(dataListIdentifier);
            if (parentIsArray) {
                cypher.append("[1]");
            }
            cypher.append("[1]"); // hashOrValue

            // end properties
            cypher.append("})");
        }
        cypher.append(")\n"); // end foreach
    }

    private static boolean isArrayElementNode(String nodeIdentifier) {
        return nodeIdentifier.endsWith("i");
    }

    static String pathOf(SpecificationElement element) {
        Deque<String> parts = new LinkedList<>();
        SpecificationElement e = element;
        while (!SpecificationElementType.MANAGED.equals(e.getSpecificationElementType())) {
            if (e.getParent().getJsonTypes().contains("array")) {
                parts.addFirst("[]");
            } else {
                parts.addFirst(e.getName());
            }
            e = e.getParent();
        }
        parts.addFirst("$");
        String path = parts.stream().collect(Collectors.joining(".")).replaceAll("\\.\\[]", "[]");
        return path;
    }

    static void convertJsonDocumentToMultiDimensionalCypherData(List<Object> data, DocumentKey key, JsonNode node, SpecificationElement element) {
        if (node.isNull()) {
            data.add(Collections.emptyList());
        } else if (node.isContainerNode()) {
            ArrayList<Object> containerValue = new ArrayList<>();
            data.add(containerValue);
            if (node.isObject()) {
                for (Map.Entry<String, SpecificationElement> entry : element.getProperties().entrySet()) {
                    String fieldName = entry.getKey();
                    SpecificationElement childElement = entry.getValue();
                    if (node.has(fieldName)) {
                        JsonNode childNode = node.get(fieldName);
                        ArrayList<Object> childData = new ArrayList<>();
                        containerValue.add(childData);
                        convertJsonDocumentToMultiDimensionalCypherData(childData, key, childNode, childElement);
                    } else {
                        containerValue.add(Collections.emptyList());
                    }
                }
            } else if (node.isArray()) {
                SpecificationElement childElement = element.getItems();
                for (int i = 0; i < node.size(); i++) {
                    JsonNode childNode = node.get(i);
                    List<Object> childData = new ArrayList<>();
                    containerValue.add(childData);
                    childData.add(i);
                    convertJsonDocumentToMultiDimensionalCypherData(childData, key, childNode, childElement);
                }
            }
        } else {
            // non-container node
            if (node.isTextual()) {
                String str = node.textValue();
                if (str.length() > 40) {
                    data.add(List.of(str, hashOf(str)));
                } else {
                    data.add(List.of(str, str));
                }
            } else if (node.isIntegralNumber()) {
                data.add(List.of(node.longValue(), node.longValue()));
            } else if (node.isFloatingPointNumber()) {
                data.add(List.of(node.doubleValue(), node.doubleValue()));
            } else if (node.isBoolean()) {
                data.add(List.of(node.booleanValue(), node.booleanValue()));
            } else {
                throw new IllegalStateException("Got unsupported jackson-specific JsonNodeType " + node.getNodeType().name() + ", value: " + node.toString());
            }
        }
    }

    public static String hashOf(String input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < digest.length; ++i) {
            sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100), 1, 3);
        }
        return sb.toString();
    }

    @Deprecated
    private static List<JsonDocument> assembleDocumentFromVersionAndEmbeddedNodes(String namespace, String entity, StatementResult statementResult) {
        List<JsonDocument> result = new ArrayList<>();
        Map<String, FlattenedDocumentLeafNode> leafNodesByPath = new TreeMap<>();
        DocumentKey documentKey = null;
        long versionRelationshipId = Long.MIN_VALUE;
        boolean deleted = false;
        String id = null;
        String pathWithIndices = null;
        while (statementResult.hasNext()) {
            Record record = statementResult.next();
            for (Pair<String, Value> field : record.fields()) {
                String key = field.key();
                if ("r".equals(key)) {
                    String rid = field.value().asNode().get("id").asString();
                    if (id != null && !id.equals(rid)) {
                        // resource changed
                        JsonDocument document = createJsonDocument(leafNodesByPath, documentKey, deleted);
                        result.add(document);
                        leafNodesByPath.clear();
                        deleted = false;
                        pathWithIndices = null;
                        documentKey = null;
                    }
                    id = rid;
                } else if ("v".equals(key)) {
                    Relationship versionRel = field.value().asRelationship();
                    if (versionRelationshipId != versionRel.id()) {
                        versionRelationshipId = versionRel.id();
                        ZonedDateTime version = versionRel.get("from").asZonedDateTime();
                        if (documentKey == null) {
                            // first version
                            documentKey = new DocumentKey(namespace, entity, id, version);
                        } else {
                            // new version
                            JsonDocument document = createJsonDocument(leafNodesByPath, documentKey, deleted);
                            result.add(document);
                            leafNodesByPath.clear();
                            deleted = false;
                            pathWithIndices = null;
                            documentKey = new DocumentKey(namespace, entity, id, version);
                        }
                    }
                } else if ("l".equals(key)) {
                    if (!field.value().isNull()) {
                        List<Relationship> embeds = field.value().asList(v -> v.asRelationship());
                        pathWithIndices = computePath(embeds);
                    }
                } else if ("m".equals(key)) {
                    Value value = field.value();
                    if ("m".equals(key)) {
                        if (value.asNode().containsKey("deleted")) {
                            Value deleteMarkerValue = value.get("deleted");
                            deleted = deleteMarkerValue.asBoolean(false);
                        }
                    }
                } else if ("e".equals(key)) {
                    Value value = field.value();
                    if (value.isNull()) {
                        continue;
                    }
                    if (pathWithIndices == null) {
                        throw new IllegalStateException("Unable to determine path with indices");
                    }
                    Value typeValue = value.asNode().get("type");
                    String type = typeValue.asString();
                    Value valueValue = value.asNode().get("value");
                    if ("string".equals(type)) {
                        String stringValue = valueValue.asString();
                        leafNodesByPath.put(pathWithIndices, new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.STRING, stringValue, Integer.MAX_VALUE));
                    } else if ("numeric".equals(type)) {
                        String stringValue = String.valueOf(valueValue.asNumber());
                        leafNodesByPath.put(pathWithIndices, new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.NUMERIC, stringValue, Integer.MAX_VALUE));
                    } else if ("boolean".equals(type)) {
                        String stringValue = String.valueOf(valueValue.asBoolean());
                        leafNodesByPath.put(pathWithIndices, new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.BOOLEAN, stringValue, Integer.MAX_VALUE));
                    } else if ("map".equals(type)) {
                        // TODO identify empty-map and create leaf-node
                    } else if ("array".equals(type)) {
                        // TODO identify empty-array and create leaf-node
                    } else {
                        throw new IllegalStateException("type not supported: " + typeValue.asString());
                    }
                }
            }
        }
        if (documentKey == null) {
            return result;
        }
        JsonDocument document = createJsonDocument(leafNodesByPath, documentKey, deleted);
        result.add(document);
        return result;
    }

    private static JsonDocument createJsonDocument(Map<String, FlattenedDocumentLeafNode> leafNodesByPath,
                                                   DocumentKey documentKey, boolean deleted) {
        FlattenedDocument flattenedDocument = new FlattenedDocument(documentKey, leafNodesByPath, deleted);
        JSONObject jsonObject = new FlattenedDocumentToJson(flattenedDocument).toJSONObject();
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

        Value typeValue = value.asNode().get("type");
        String type = typeValue.asString();
        Value valueValue = value.asNode().get("value");
        if ("string".equals(type)) {
            String stringValue = valueValue.asString();
            return new FlattenedDocumentLeafNode(documentKey, pathWithIndices, FragmentType.STRING, stringValue, Integer.MAX_VALUE);
        } else if ("numeric".equals(type)) {
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

    static DocumentKey getKeyFromRecord(Record record, String nameSpace, String entityName) {
        String docId = record.get("r").get("id").asString();
        ZonedDateTime version = record.get("v").get("from").asZonedDateTime();
        return new DocumentKey(nameSpace, entityName, docId, version);
    }

    static Flowable<JsonDocument> toDocuments(Flowable<Record> records, String nameSpace, String entityName) {
        return records.groupBy(record -> {
            // Group by
            return getKeyFromRecord(record, nameSpace, entityName);
        }).concatMapSingle(recordById -> {
            // For each group create a
            DocumentKey key = recordById.getKey();
            return recordById.toMap(record -> {
                // Compute path.
                return getPathFromRecord(record);
            }, record -> getLeafFromRecord(record, key)).map(values -> {
                // Un-flatten.
                return createJsonDocument(new TreeMap<>(values), key, false);
            });
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
    public Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String path, String value, Range<String> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("snapshot", snapshot);
        params.put("limit", Integer.MAX_VALUE);
        params.put("path", path);
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
        cypher.append("OPTIONAL MATCH (m)-[l:EMBED*]->(e) RETURN r, v, m, l, e");
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
        cypher.append("WITH r, v, m OPTIONAL MATCH (m)-[l:EMBED*]->(e) RETURN r, v, m, l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName).firstElement();
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {

        // TODO: Refactor.
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

        String orderBy = " ORDER BY r.id " + (range.isBackward() ? "DESC " : "");
        String limit = "";
        if (range.hasFirst() || range.hasLast()) {
            limit = " LIMIT $limit ";
            params.put("limit", range.getFirst() != null ? range.getFirst() : range.getLast());
        }

        params.put("snapshot", snapshot);

        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();

        cypher.append("MATCH (r :").append(entityName).append(") WHERE ").append(String.join(" AND ", conditions)).append(" WITH r ").append(orderBy).append(limit).append("\n");
        cypher.append("MATCH (r)-[v:VERSION]->(m) WHERE v.from <= $snapshot AND $snapshot < v.to").append(" ");
        cypher.append("WITH r, v, m ");
        cypher.append("OPTIONAL MATCH (m)-[l:EMBED*]->(e) RETURN r, v, m, l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName);
    }

    @Override
    public Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id, Range<ZonedDateTime> range) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rid", id);

        String fromWhere = "";
        if (range.hasAfter()) {
            params.put("snapshotFrom", range.getAfter());
            fromWhere = "WHERE v.from < $snapshotFrom AND $snapshotFrom < v.to";
            if (range.hasBefore()) {
                fromWhere += " AND v.to < $snapshotTo";
            }
        }
        String toWhere = "WHERE firstVersion <= v.from";
        if (range.hasBefore()) {
            params.put("snapshotTo", range.getBefore());
            toWhere = "WHERE firstVersion <= v.from AND v.to < $snapshotTo";
        }

        String limit = "";
        if (range.hasFirst() || range.hasFirst()) {
            limit = "LIMIT $limit";
        }

        String orderBy = range.isBackward() ? "ORDER BY v.from DESC" : "ORDER BY v.from";

        cypher.append("MATCH (r :").append(entityName).append(" {id: $rid})-[v:VERSION]->(m) " + fromWhere + "\n");
        cypher.append("WITH r, v.from AS firstVersion\n");
        cypher.append("MATCH (r)-[v:VERSION]->(m) " + toWhere + "\n");
        cypher.append("WITH r, v, m " + orderBy + " " + limit + " OPTIONAL MATCH (m)-[l:EMBED*]->(e) RETURN r, v, m, l, e");

        Flowable<Record> records = neoTx.executeCypherAsync(cypher.toString(), params);
        return toDocuments(records, ns, entityName);
    }

    @Override
    public Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id, String relationName, Range<String> range) {
        return Flowable.error(new UnsupportedOperationException());
    }

    @Override
    public Completable createOrOverwrite(Transaction transaction, JsonDocument document, Specification specification) {
        Neo4jTransaction tx = (Neo4jTransaction) transaction;
        try {
            JsonNode root = mapper.readTree(document.document().toString());
            LinkedHashMap<String, Object> params = new LinkedHashMap<>();
            SpecificationElement entitySpecificationElement = specification.getRootElement().getProperties().get(document.key().entity());
            String cypher = traverseSpecificationAndGenerateCypherCreateStatement(entitySpecificationElement, params, document.key(), root);
            return tx.executeCypherAsync(cypher, params).ignoreElements();
        } catch (IOException e) {
            return Completable.error(new PersistenceException(e));
        }
    }

    @Override
    public Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entityName).append(" {id: $rid})-[:VERSION {from:$version}]->(m) OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE m, e");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id, "version", version)).ignoreElements();
    }

    @Override
    public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
        Neo4jTransaction neoTx = (Neo4jTransaction) tx;
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH (r:").append(entity).append(" {id: $rid}) OPTIONAL MATCH (r)-[:VERSION]->(m) OPTIONAL MATCH (m)-[:EMBED*]->(e) DETACH DELETE r, m, e");
        return neoTx.executeCypherAsync(cypher.toString(), Map.of("rid", id)).ignoreElements();
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
