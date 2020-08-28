package no.ssb.lds.core.persistence.neo4j;

import com.fasterxml.jackson.databind.JsonNode;
import graphql.language.FieldDefinition;
import graphql.language.Node;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class Neo4jCreationalPatternFactory {

    static final String EMPTY_ARRAY_FIELD_PREFIX = "_lds_a_";

    private Map<SpecificationElement, String> creationalPatternByEntity = new ConcurrentHashMap<>();

    Neo4jQueryAndParams creationalQueryAndParams(Specification specification, String entity, List<JsonDocument> documentForEntityList) {
        SpecificationElement entitySpecificationElement = specification.getRootElement().getProperties().get(entity);
        String cypher = creationalCypherForEntity(specification.typeDefinitionRegistry(), entitySpecificationElement);
        List<Object> batch = new ArrayList<>();
        for (JsonDocument document : documentForEntityList) {
            DocumentKey key = document.key();
            JsonNode documentRootNode = document.jackson();
            List<Object> data = new ArrayList<>();
            convertJsonDocumentToMultiDimensionalCypherData(data, documentRootNode, entitySpecificationElement);
            List<Object> record = new ArrayList<>();
            record.add(key.id());
            record.add(key.timestamp());
            record.add(data);
            batch.add(record);
        }
        Map<String, Object> params = Map.of(
                "batch", batch
        );
        return new Neo4jQueryAndParams(cypher, params);
    }

    String creationalCypherForEntity(TypeDefinitionRegistry typeDefinitionRegistry, SpecificationElement specificationElement) {
        if (!SpecificationElementType.MANAGED.equals(specificationElement.getSpecificationElementType())) {
            throw new IllegalArgumentException("specificationElement must be of type MANAGED");
        }
        return creationalPatternByEntity.computeIfAbsent(specificationElement, e -> buildCreationalCypherForEntity(typeDefinitionRegistry, e));
    }

    static final Pattern linkPattern = Pattern.compile("/?([^/]*)/([^/]*)");

    static String buildCreationalCypherForEntity(TypeDefinitionRegistry typeDefinitionRegistry, SpecificationElement specificationElement) {
        String entity = specificationElement.getName();
        List<String> allTypesOfEntity = getAllTypesOfType(typeDefinitionRegistry, entity);
        StringBuilder cypher = new StringBuilder();
        cypher.append("UNWIND $batch AS record\n");
        cypher.append("MERGE (r:").append(entity).append("_R:RESOURCE {id: record[0]}) WITH r, record[1] AS version, record[2] AS data\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF {from: version}]-(m)-[*]->(e:EMBEDDED) DETACH DELETE m, e WITH r, version, data\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from <= version AND COALESCE(version < v.to, true) WITH r, version, data, v AS prevVersion\n");
        cypher.append("OPTIONAL MATCH (r").append(")<-[v:VERSION_OF]-() WHERE v.from > version WITH r, version, data, prevVersion, min(v.from) AS nextVersionFrom\n");
        cypher.append("FOREACH(d IN data |\n");
        cypher.append("  CREATE (r)<-[v:VERSION_OF {from: version, to: coalesce(prevVersion.to, nextVersionFrom)}]-(m:").append(String.join(":", allTypesOfEntity)).append(":INSTANCE").append(")\n");
        cypher.append("  SET prevVersion.to = version");
        int i = 0;
        for (Map.Entry<String, SpecificationElement> entry : specificationElement.getProperties().entrySet()) {
            traverseSpecification(cypher, typeDefinitionRegistry, entity, entry.getValue(), 1, "    ", "m", false, "m", "d[" + i + "]");
            i++;
        }
        cypher.append(")");
        return cypher.toString();
    }

    private static List<String> getAllTypesOfType(TypeDefinitionRegistry typeDefinitionRegistry, String type) {
        TypeDefinition typeDefinition = typeDefinitionRegistry.getType(type).get();
        List<String> implementedInterfaceNames = new ArrayList<>();
        if (typeDefinition instanceof ObjectTypeDefinition) {
            implementedInterfaceNames = ((ObjectTypeDefinition) typeDefinition).getImplements().stream()
                    .map(a -> ((TypeName) a).getName())
                    .collect(Collectors.toList());
        }
        implementedInterfaceNames.add(0, type);
        return implementedInterfaceNames;
    }

    static void traverseSpecification(StringBuilder cypher, TypeDefinitionRegistry typeDefinitionRegistry, String entity, SpecificationElement element, int depth, String indentation, String parentNodeIdentifier, boolean parentIsArray, String nodeIdentifier, String dataListIdentifier) {
        JsonNavigationPath jsonNavigationPath = JsonNavigationPath.from(element);
        cypher.append("\n").append(indentation).append("FOREACH(d").append(depth).append(" IN ").append(dataListIdentifier).append(" |");
        indentation += "  ";
        dataListIdentifier = "d" + depth;
        String relationPath = element.getName();
        if (parentIsArray) {
            relationPath = element.getParent().getName();
        }

        FieldDefinition fieldDefinition = null;
        TypeDefinition typeDefinition;
        String[] navPaths = jsonNavigationPath.getPath();
        Deque<String> deque = new LinkedList<>(List.of(navPaths));
        if ("$".equals(deque.getFirst())) {
            typeDefinition = typeDefinitionRegistry.getType(entity).get();
            deque.removeFirst();
        } else {
            throw new IllegalStateException("Navigation path is missing the first '$' element");
        }
        while (!deque.isEmpty()) {
            String elem = deque.getFirst();
            if ("[]".equals(elem)) {
            } else {
                List<Node> children = typeDefinition.getChildren();
                boolean found = false;
                for (Node child : children) {
                    if (child instanceof FieldDefinition) {
                        FieldDefinition def = (FieldDefinition) child;
                        String fieldName = def.getName();
                        if (elem.equals(fieldName)) {
                            found = true;
                            fieldDefinition = def;
                            typeDefinition = typeDefinitionRegistry.getType(def.getType()).get();
                            break;
                        }
                    }
                }
                if (!found) {
                    throw new IllegalStateException("Element not found: " + elem + " in type: " + typeDefinition.toString());
                }
            }
            deque.removeFirst();
        }
        String embeddedType = typeDefinition.getName();

        if (element.getJsonTypes().contains("object")) {
            nodeIdentifier = parentNodeIdentifier + "o";
            String relationType;
            if (parentIsArray) {
                relationType = element.getParent().getName();
            } else {
                relationType = element.getName();
            }
            cypher.append("\n").append(indentation).append("CREATE (").append(parentNodeIdentifier).append(")-[:").append(relationType);
            if (parentIsArray) {
                cypher.append("{index: ").append(dataListIdentifier).append("[0]}");
            }
            cypher.append("]->");
            List<String> allTypesOfType = getAllTypesOfType(typeDefinitionRegistry, embeddedType);
            cypher.append("(").append(nodeIdentifier).append(":").append(String.join(":", allTypesOfType)).append(":EMBEDDED").append(")");
            int i = 0;
            for (Map.Entry<String, SpecificationElement> entry : element.getProperties().entrySet()) {
                String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "") + "[" + i + "]";
                traverseSpecification(cypher, typeDefinitionRegistry, entity, entry.getValue(), depth + 1, indentation, nodeIdentifier, false, nodeIdentifier, childDataListIdentifier);
                i++;
            }
        } else if (element.getJsonTypes().contains("array")) {
            if (parentIsArray) {
                throw new RuntimeException("Nested array is unsupported");
            }
            // there is always exactly one child of any array specification element, otherwise there is a schema error
            SpecificationElement theItems = element.getItems();
            if (element.getSpecificationElementType() != SpecificationElementType.REF
                    && theItems.getProperties().isEmpty()) {
                // array of simple types, no need for array indicator
            } else {
                cypher.append("\n").append(indentation).append("SET ").append(nodeIdentifier).append(".").append(EMPTY_ARRAY_FIELD_PREFIX).append(element.getName()).append(" = true");
            }
            String childDataListIdentifier = dataListIdentifier + (parentIsArray ? "[1]" : "");
            traverseSpecification(cypher, typeDefinitionRegistry, entity, element.getItems(), depth + 1, indentation, nodeIdentifier, true, nodeIdentifier, childDataListIdentifier);
        } else {

            // value node

            boolean isReference = SpecificationElementType.REF.equals(parentIsArray
                    ? element.getParent().getSpecificationElementType()
                    : element.getSpecificationElementType());

            if (isReference) {

                nodeIdentifier += "r";
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
                    cypher.append("MERGE (").append(nodeIdentifier).append(":").append(refType).append("_R:RESOURCE").append(" {id: ").append(refDataIdentifier).append("})");

                    cypher.append("\n").append(indentation).append("  ");
                    String relationType;
                    if (parentIsArray) {
                        relationType = element.getParent().getName();
                    } else {
                        relationType = element.getName();
                    }
                    cypher.append("CREATE (").append(parentNodeIdentifier).append(")-[:").append(relationType);
                    if (parentIsArray) {
                        cypher.append("{index: ").append(dataListIdentifier).append("[0]}");
                    }
                    cypher.append("]->(").append(nodeIdentifier).append(")");

                    cypher.append(")"); // end foreach
                }

            } else {

                // scalar

                cypher.append("\n").append(indentation).append("SET ").append(nodeIdentifier).append(".").append(relationPath).append(" = ");
                cypher.append(dataListIdentifier);
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
                if (element.getSpecificationElementType() != SpecificationElementType.REF
                        && childElement.getProperties().isEmpty()) {
                    // array of simple types, no need for array index
                    List<Object> childData = new ArrayList<>();
                    containerValue.add(childData);
                    for (int i = 0; i < node.size(); i++) {
                        JsonNode childNode = node.get(i);
                        convertJsonDocumentToMultiDimensionalCypherData(childData, childNode, childElement);
                    }
                } else {
                    for (int i = 0; i < node.size(); i++) {
                        JsonNode childNode = node.get(i);
                        List<Object> childData = new ArrayList<>();
                        containerValue.add(childData);
                        childData.add(i);
                        convertJsonDocumentToMultiDimensionalCypherData(childData, childNode, childElement);
                    }
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
                    data.add(node.textValue());
                } else if (node.isIntegralNumber()) {
                    data.add(node.longValue());
                } else if (node.isFloatingPointNumber()) {
                    data.add(node.doubleValue());
                } else if (node.isBoolean()) {
                    data.add(node.booleanValue());
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
