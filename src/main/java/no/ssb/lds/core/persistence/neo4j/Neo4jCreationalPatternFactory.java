package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class Neo4jCreationalPatternFactory {

    private Map<SpecificationElement, String> creationalPatternByEntity = new ConcurrentHashMap<>();

    Neo4jQueryAndParams creationalQueryAndParams(Specification specification, DocumentKey key, JsonNode documentRootNode) {
        SpecificationElement entitySpecificationElement = specification.getRootElement().getProperties().get(key.entity());
        String cypher = creationalCypherForEntity(entitySpecificationElement);
        List<Object> data = new ArrayList<>();
        convertJsonDocumentToMultiDimensionalCypherData(data, documentRootNode, entitySpecificationElement);
        Map<String, Object> params = Map.of(
                "rid", key.id(),
                "version", key.timestamp(),
                "data", data
        );
        return new Neo4jQueryAndParams(cypher, params);
    }

    String creationalCypherForEntity(SpecificationElement specificationElement) {
        if (!SpecificationElementType.MANAGED.equals(specificationElement.getSpecificationElementType())) {
            throw new IllegalArgumentException("specificationElement must be of type MANAGED");
        }
        return creationalPatternByEntity.computeIfAbsent(specificationElement, e -> buildCreationalCypherForEntity(e, e.getName()));
    }

    static final Pattern linkPattern = Pattern.compile("/?([^/]*)/([^/]*)");

    static String buildCreationalCypherForEntity(SpecificationElement specificationElement, String entity) {
        StringBuilder cypher = new StringBuilder();
        cypher.append("MERGE (r:").append(entity).append(" {id: $rid}) WITH r\n");
        cypher.append("OPTIONAL MATCH (r:").append(entity).append(")-[v:VERSION {from: $version}]->(m)-[:EMBED*]->(e) DETACH DELETE m, e WITH r\n");
        cypher.append("OPTIONAL MATCH (r:").append(entity).append(")-[v:VERSION]->() WHERE v.from <= $version AND $version < v.to WITH r, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r:").append(entity).append(")-[v:VERSION]->() WHERE v.from > $version WITH r, prevVersion, v AS nextVersion ORDER BY v.from LIMIT 1\n");
        cypher.append("FOREACH(d IN $data |\n");
        cypher.append("  MERGE (r)-[v:VERSION {from: $version, to: coalesce(prevVersion.to, nextVersion.from, datetime('9999-01-01T00:00:00.0Z[Etc/UTC]'))}]->(m:").append(entity).append("_E {type:'map', path:'$'})\n");
        cypher.append("  SET prevVersion.to = $version");
        int i = 0;
        for (Map.Entry<String, SpecificationElement> entry : specificationElement.getProperties().entrySet()) {
            traverseSpecification(cypher, entity, entry.getValue(), 1, "    ", "m", "m" + i, "d[" + i + "]");
            i++;
        }
        cypher.append(")");
        return cypher.toString();
    }

    static boolean isArrayElementNode(String nodeIdentifier) {
        return nodeIdentifier.endsWith("i");
    }

    static void traverseSpecification(StringBuilder cypher, String entity, SpecificationElement element, int depth, String indentation, String parentNodeIdentifier, String nodeIdentifier, String dataListIdentifier) {
        String path = JsonNavigationPath.from(element).serialize();
        cypher.append("\n").append(indentation).append("FOREACH(d").append(depth).append(" IN ").append(dataListIdentifier).append(" |");
        indentation += "  ";
        dataListIdentifier = "d" + depth;
        String relationPath = element.getName();
        boolean parentIsArray = isArrayElementNode(nodeIdentifier);
        if (parentIsArray) {
            relationPath = "[]";
        }
        if (element.getJsonTypes().contains("object")) {
            cypher.append("\n").append(indentation).append("CREATE (").append(parentNodeIdentifier).append(")-[:").append("EMBED").append("{path: '").append(relationPath).append("'");
            if (parentIsArray) {
                cypher.append(", index: ").append(dataListIdentifier).append("[0]");
            }
            cypher.append("}]->");
            cypher.append("(").append(nodeIdentifier).append(":").append(entity).append("_E {type: 'map', path: '").append(path).append("'})");
            int i = 0;
            for (Map.Entry<String, SpecificationElement> entry : element.getProperties().entrySet()) {
                String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "") + "[" + i + "]";
                String childNodeIdentifier = nodeIdentifier + "p" + i;
                traverseSpecification(cypher, entity, entry.getValue(), depth + 1, indentation, nodeIdentifier, childNodeIdentifier, childDataListIdentifier);
                i++;
            }
        } else if (element.getJsonTypes().contains("array")) {
            cypher.append("\n").append(indentation).append("CREATE (").append(parentNodeIdentifier).append(")-[:").append("EMBED").append("{path: '").append(relationPath).append("'");
            if (parentIsArray) {
                cypher.append(", index: ").append(dataListIdentifier).append("[0]");
            }
            cypher.append("}]->");
            cypher.append("(").append(nodeIdentifier).append(":").append(entity).append("_E {type:'array', path: '").append(path).append("'})");
            String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "");
            String childNodeIdentifier = nodeIdentifier + "i";
            traverseSpecification(cypher, entity, element.getItems(), depth + 1, indentation, nodeIdentifier, childNodeIdentifier, childDataListIdentifier);
        } else {

            // value node

            boolean isReference = SpecificationElementType.REF.equals(parentIsArray
                    ? element.getParent().getSpecificationElementType()
                    : element.getSpecificationElementType());

            if (isReference) {

                List<String> sortedRefTypes = new ArrayList<>(new TreeSet<>(parentIsArray ? element.getParent().getRefTypes() : element.getRefTypes()));

                for (int i = 0; i < sortedRefTypes.size(); i++) {
                    String refType = sortedRefTypes.get(i);
                    String refDataIdentifier = "d" + (depth + 1);
                    if (i > 0 && i < sortedRefTypes.size() - 1) {
                        cypher.append("\n");
                    }
                    cypher.append("\n").append(indentation);
                    cypher.append("FOREACH(").append(refDataIdentifier).append(" IN ").append(dataListIdentifier);
                    cypher.append(parentIsArray ? "[1]" : "").append("[").append(i).append("]").append(" |");

                    cypher.append("\n").append(indentation).append("  ");
                    cypher.append("MERGE (").append(nodeIdentifier).append(":").append(refType).append(" {id: ").append(refDataIdentifier).append("})");

                    cypher.append("\n").append(indentation).append("  ");
                    cypher.append("CREATE (").append(parentNodeIdentifier).append(")-[:").append("REF {path: '").append(relationPath).append("'");
                    if (parentIsArray) {
                        cypher.append(", index: ").append(dataListIdentifier).append("[0]");
                    }
                    cypher.append("}]->(").append(nodeIdentifier).append(")");

                    cypher.append(")"); // end foreach
                }

            } else {

                cypher.append("\n").append(indentation).append("CREATE (").append(parentNodeIdentifier).append(")-[:").append("EMBED").append("{path: '").append(relationPath).append("'");
                if (parentIsArray) {
                    cypher.append(", index: ").append(dataListIdentifier).append("[0]");
                }
                cypher.append("}]->");

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
        }
        cypher.append(")"); // end foreach
    }

    static void convertJsonDocumentToMultiDimensionalCypherData(List<Object> data, JsonNode node, SpecificationElement element) {
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
                        convertJsonDocumentToMultiDimensionalCypherData(childData, childNode, childElement);
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
                    convertJsonDocumentToMultiDimensionalCypherData(childData, childNode, childElement);
                }
            }
        } else {
            // non-container node

            boolean parentIsRef = SpecificationElementType.REF.equals(element.getParent().getSpecificationElementType());
            boolean nodeIsRef = SpecificationElementType.REF.equals(element.getSpecificationElementType());

            if (parentIsRef || nodeIsRef) {
                // ref node
                TreeSet<String> sortedRefTypes = new TreeSet<>();
                List<Object> effectiveData;
                effectiveData = new ArrayList<>();
                data.add(effectiveData);
                if (parentIsRef) {
                    sortedRefTypes.addAll(element.getParent().getRefTypes());
                } else {
                    //effectiveData = data;
                    sortedRefTypes.addAll(element.getRefTypes());
                }
                Matcher m = linkPattern.matcher(node.textValue());
                if (!m.matches()) {
                    throw new IllegalArgumentException("Bad link format: \"" + node.textValue() + "\"");
                }
                String entity = m.group(1);
                String id = m.group(2);
                boolean typeIsLegal = false;
                for (String refType : sortedRefTypes) {
                    List<Object> linkData = new ArrayList<>();
                    effectiveData.add(linkData);
                    if (refType.equals(entity)) {
                        linkData.add(id);
                        typeIsLegal = true;
                    }
                }
                if (!typeIsLegal) {
                    throw new IllegalArgumentException("Bad link type: \"" + node.textValue() + "\"");
                }

            } else {
                // value node

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
    }

    static String hashOf(String input) {
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
}
