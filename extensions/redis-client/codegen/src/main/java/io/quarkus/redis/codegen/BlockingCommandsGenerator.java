package io.quarkus.redis.codegen;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;
import com.github.javaparser.javadoc.JavadocBlockTag;

/**
 * Standalone code generator that derives the Lettuce blocking command implementation from each
 * reactive Redis command interface. The blocking command interfaces themselves ({@code ValueCommands},
 * {@code KeyCommands}, …) are hand-curated Quarkus API and are <b>not</b> generated.
 */
public final class BlockingCommandsGenerator {

    private static final String DATASOURCE_PKG = "io.quarkus.redis.datasource";
    private static final String LETTUCE_PKG = "io.quarkus.redis.runtime.client.lettuce";
    private static final String REACTIVE_ROOT = "ReactiveRedisCommands";
    private static final String UNI_IMPORT = "io.smallrye.mutiny.Uni";

    private static final String[] IMPORT_GROUPS = {"java.", "javax.", "jakarta.", "org.", "com."};

    // Relative to the codegen module dir; generate.sh runs from there so these resolve correctly.
    private static final Path RUNTIME_SRC = Path.of("../runtime/src/main/java").toAbsolutePath().normalize();
    private static final Path OUT = Path.of("target/generated").toAbsolutePath().normalize();
    private static final List<String> COMMAND_GROUPS = List.of("value", "keys", "hash");

    private static final Set<String> MANUAL_METHODS = Set.of();
    private static final List<String> SKIPPED_METHODS = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        System.out.println("[codegen] Starting BlockingCommandsGenerator");
        System.out.println("[codegen] runtime sources: " + RUNTIME_SRC);
        System.out.println("[codegen] output:          " + OUT);
        for (String group : COMMAND_GROUPS) {
            System.out.println("[codegen] Generating command group " + group);
            generateGroup(group.trim());
        }

        if (!SKIPPED_METHODS.isEmpty()) {
            System.out.println("[codegen] Needs hand-written implementation (emitted as UnsupportedOperationException stubs):");
            SKIPPED_METHODS.forEach(s -> System.out.println("[codegen]   - " + s));
        }
    }

    private static void generateGroup(String group) throws IOException {
        Path dir = RUNTIME_SRC.resolve(DATASOURCE_PKG.replace('.', '/')).resolve(group);
        File reactiveFile = findReactiveInterface(dir.toFile());

        if (reactiveFile == null) {
            System.out.println("[codegen] SKIP '" + group + "': no Reactive*Commands interface under " + dir);
            return;
        }

        CompilationUnit cu = StaticJavaParser.parse(reactiveFile);
        var reactiveInterface = cu.getTypes().stream()
                .filter(t -> t instanceof ClassOrInterfaceDeclaration cid && cid.isInterface())
                .map(t -> (ClassOrInterfaceDeclaration) t)
                .findFirst().orElseThrow();

        var reactiveType = reactiveInterface.getNameAsString();
        var blockingType = reactiveType.substring("Reactive".length());
        var implType = "LettuceBlocking" + blockingType + "Impl";
        var typeParams = typeParams(reactiveInterface);

        System.out.println("[codegen] " + group + ": " + reactiveType + " -> " + implType + " implements " + blockingType);

        String implSrc = generateImpl(cu, reactiveInterface, group, reactiveType, blockingType, implType, typeParams);
        writeJava(LETTUCE_PKG + "." + group, implType, implSrc);
    }

    private static File findReactiveInterface(File dir) {
        File[] files = dir.listFiles((f, name) -> name.startsWith("Reactive")
                && name.endsWith("Commands.java")
                && !name.startsWith("ReactiveTransactional"));

        if (files == null || files.length != 1) {
            return null;
        }

        return files[0];
    }

    // --- Blocking implementation generation ---

    private static String generateImpl(CompilationUnit cu, ClassOrInterfaceDeclaration reactiveIface, String group,
                                       String reactiveType, String blockingType, String implType, String typeParams) {
        Set<String> typeVars = new LinkedHashSet<>();
        reactiveIface.getTypeParameters().forEach(tp -> typeVars.add(tp.getNameAsString()));

        Set<String> imports = computeImports(cu, reactiveIface, group, reactiveType, blockingType, typeVars);
        var reactiveImpl = "LettuceReactive" + blockingType + "Impl";

        StringBuilder sb = new StringBuilder();
        sb.append(header());
        sb.append("package ").append(LETTUCE_PKG).append('.').append(group).append(";\n\n");
        sb.append(renderImports(imports));
        sb.append("\n");
        sb.append("/**\n");
        sb.append(" * Blocking wrapper around {@link ").append(reactiveImpl).append("}.\n");
        sb.append(" * <p>\n");
        sb.append(" * Each method awaits the reactive result for the configured {@code timeout}. Must not be\n");
        sb.append(" * invoked from an event loop thread.\n");
        sb.append(" *\n");
        for (String tv : typeVars) {
            sb.append(" * @param <").append(tv).append("> ").append(typeVarDescription(tv)).append('\n');
        }
        sb.append(" */\n");
        sb.append("""
                public class %IMPL%%TP% implements %BLOCKING%%TP% {

                    private final RedisDataSource dataSource;
                    private final %REACTIVE%%TP% reactive;
                    private final Duration timeout;

                    public %IMPL%(RedisDataSource dataSource, %REACTIVE%%TP% reactive, Duration timeout) {
                        this.dataSource = dataSource;
                        this.reactive = reactive;
                        this.timeout = timeout;
                    }

                    @Override
                    public RedisDataSource getDataSource() {
                        return dataSource;
                    }
                """
                .replace("%IMPL%", implType)
                .replace("%TP%", typeParams)
                .replace("%BLOCKING%", blockingType)
                .replace("%REACTIVE%", reactiveType));

        for (MethodDeclaration m : reactiveIface.getMethods()) {
            if (isConcreteMethod(m)) {
                continue;
            }
            emitImplMethod(sb, m, blockingType);
        }

        sb.append("}\n");
        return sb.toString();
    }

    private static void emitImplMethod(StringBuilder sb, MethodDeclaration m, String blockingType) {
        var name = m.getNameAsString();
        BlockingReturn br = blockingReturn(m.getType());
        var key = blockingType + "#" + name;

        String manualReason = null;
        if (br.needsManualImpl) {
            manualReason = "is not a pure await-wrapper (returns " + br.typeString + ")";
        } else if (MANUAL_METHODS.contains(key)) {
            manualReason = "is flagged in MANUAL_METHODS (await-wrapper semantics do not apply)";
        }

        if (manualReason != null) {
            SKIPPED_METHODS.add(key + " " + manualReason);
            sb.append('\n');
            sb.append("    // TODO(codegen): '").append(name).append("' ").append(manualReason)
                    .append(". Implement by hand.\n");
            sb.append("    @Override\n");
            sb.append("    public ").append(br.isVoid ? "void" : br.typeString).append(' ').append(name)
                    .append('(').append(renderParams(m)).append(") {\n");
            sb.append("        throw new UnsupportedOperationException(\"").append(name)
                    .append(" must be implemented by hand\");\n");
            sb.append("    }\n");
            return;
        }

        boolean varargs = m.getParameters().stream().anyMatch(Parameter::isVarArgs);
        boolean deprecated = isDeprecated(m);

        sb.append('\n');
        if (varargs) {
            sb.append("    @SafeVarargs\n");
        }
        sb.append("    @Override\n");
        if (deprecated) {
            sb.append("    @SuppressWarnings(\"deprecation\")\n");
        }
        sb.append("    public ").append(varargs ? "final " : "")
                .append(br.isVoid ? "void" : br.typeString).append(' ').append(name).append('(')
                .append(renderParams(m)).append(") {\n");
        sb.append("        ").append(br.isVoid ? "" : "return ")
                .append("reactive.").append(name).append('(').append(renderArgs(m))
                .append(").await().atMost(timeout);\n");
        sb.append("    }\n");
    }

    // --- Blocking return setup ---

    private record BlockingReturn(String typeString, boolean isVoid, boolean needsManualImpl) {
    }

    private static BlockingReturn blockingReturn(Type reactiveType) {
        if (!isUni(reactiveType)) {
            return new BlockingReturn(blockingTypeOf(reactiveType), reactiveType.isVoidType(), true);
        }

        Type inner = reactiveType.asClassOrInterfaceType().getTypeArguments().map(a -> a.get(0)).orElseThrow();
        return switch (inner.asString()) {
            case "Void" -> new BlockingReturn("void", true, false);
            case "Long" -> new BlockingReturn("long", false, false);
            case "Integer" -> new BlockingReturn("int", false, false);
            case "Boolean" -> new BlockingReturn("boolean", false, false);
            case "Double" -> new BlockingReturn("double", false, false);
            default -> new BlockingReturn(inner.asString(), false, false);
        };
    }

    private static boolean isUni(Type type) {
        return type.isClassOrInterfaceType()
                && type.asClassOrInterfaceType().getNameAsString().equals("Uni");
    }

    private static String blockingTypeOf(Type reactiveType) {
        if (!reactiveType.isClassOrInterfaceType()) {
            return reactiveType.asString();
        }

        var cit = reactiveType.asClassOrInterfaceType();
        var name = cit.getNameAsString();
        if (!name.startsWith("Reactive")) {
            return cit.asString();
        }

        var swappedName = name.substring("Reactive".length());
        var args = "";
        if (cit.getTypeArguments().isPresent()) {
            List<String> argStrings = new ArrayList<>();
            cit.getTypeArguments().get().forEach(t -> argStrings.add(t.asString()));
            args = "<" + String.join(", ", argStrings) + ">";
        }
        return swappedName + args;
    }

    // --- Method & signature helpers ---

    private static boolean isConcreteMethod(MethodDeclaration m) {
        return m.getBody().isPresent() || m.isStatic();
    }

    private static boolean isDeprecated(MethodDeclaration m) {
        if (m.getAnnotationByName("Deprecated").isPresent()) {
            return true;
        }
        return m.getJavadoc()
                .map(j -> j.getBlockTags().stream().anyMatch(t -> t.getType() == JavadocBlockTag.Type.DEPRECATED))
                .orElse(false);
    }

    private static String renderParams(MethodDeclaration m) {
        List<String> parts = new ArrayList<>();
        for (Parameter p : m.getParameters()) {
            parts.add(p.getTypeAsString() + (p.isVarArgs() ? "..." : "") + " " + p.getNameAsString());
        }
        return String.join(", ", parts);
    }

    private static String renderArgs(MethodDeclaration m) {
        List<String> parts = new ArrayList<>();
        for (Parameter p : m.getParameters()) {
            parts.add(p.getNameAsString());
        }
        return String.join(", ", parts);
    }

    private static String typeParams(ClassOrInterfaceDeclaration interfaceDec) {
        if (interfaceDec.getTypeParameters().isEmpty()) {
            return "";
        }
        List<String> names = new ArrayList<>();
        interfaceDec.getTypeParameters().forEach(tp -> names.add(tp.getNameAsString()));
        return "<" + String.join(", ", names) + ">";
    }

    private static String typeVarDescription(String tv) {
        return switch (tv) {
            case "K" -> "the key type";
            case "V" -> "the value type";
            case "F" -> "the field type";
            case "M" -> "the member type";
            default -> "the " + tv.toLowerCase() + " type";
        };
    }

    // --- Import handling ---

    private static Set<String> computeImports(CompilationUnit cu, ClassOrInterfaceDeclaration reactiveIface, String group,
                                              String reactiveType, String blockingType, Set<String> typeVars) {
        Set<String> imports = new LinkedHashSet<>();
        for (ImportDeclaration i : cu.getImports()) {
            String fqn = i.getNameAsString();
            if (fqn.equals(UNI_IMPORT) || fqn.equals(DATASOURCE_PKG + "." + REACTIVE_ROOT)) {
                continue;
            }
            imports.add(fqn);
        }
        imports.add("java.time.Duration");
        imports.add(DATASOURCE_PKG + ".RedisDataSource");
        imports.add(DATASOURCE_PKG + "." + group + "." + reactiveType);
        imports.add(DATASOURCE_PKG + "." + group + "." + blockingType);

        Path groupDir = RUNTIME_SRC.resolve(DATASOURCE_PKG.replace('.', '/')).resolve(group);
        Set<String> names = new TreeSet<>();
        for (MethodDeclaration m : reactiveIface.getMethods()) {
            if (isConcreteMethod(m)) {
                continue;
            }
            for (Parameter p : m.getParameters()) {
                p.getType().findAll(ClassOrInterfaceType.class).forEach(t -> names.add(t.getNameAsString()));
            }
            BlockingReturn br = blockingReturn(m.getType());
            if (!br.isVoid) {
                StaticJavaParser.parseType(br.typeString).findAll(ClassOrInterfaceType.class)
                        .forEach(t -> names.add(t.getNameAsString()));
            }
        }
        for (String simple : names) {
            if (typeVars.contains(simple)) {
                continue;
            }
            if (Files.exists(groupDir.resolve(simple + ".java"))) {
                imports.add(DATASOURCE_PKG + "." + group + "." + simple);
            }
        }
        return imports;
    }

    private static String renderImports(Set<String> imports) {
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i <= IMPORT_GROUPS.length; i++) {
            groups.add(new ArrayList<>());
        }
        for (String imp : imports) {
            int g = IMPORT_GROUPS.length;
            for (int i = 0; i < IMPORT_GROUPS.length; i++) {
                if (imp.startsWith(IMPORT_GROUPS[i])) {
                    g = i;
                    break;
                }
            }
            groups.get(g).add(imp);
        }
        StringBuilder sb = new StringBuilder();
        boolean firstGroup = true;
        for (List<String> g : groups) {
            if (g.isEmpty()) {
                continue;
            }
            g.sort(Comparator.naturalOrder());
            if (!firstGroup) {
                sb.append('\n');
            }
            firstGroup = false;
            for (String imp : g) {
                sb.append("import ").append(imp).append(";\n");
            }
        }
        return sb.toString();
    }

    // --- Output ---

    private static String header() {
        return """
                // Generated by io.quarkus.redis.codegen.BlockingCommandsGenerator — DO NOT EDIT.
                // Regenerate by running io.quarkus.redis.codegen.BlockingCommandsGenerator.
                """;
    }

    private static void writeJava(String pkg, String simpleName, String source) throws IOException {
        Path dir = OUT.resolve(pkg.replace('.', '/'));
        Files.createDirectories(dir);
        Path file = dir.resolve(simpleName + ".java");
        Files.writeString(file, source, StandardCharsets.UTF_8);
        System.out.println("[codegen]   wrote " + OUT.relativize(file));
    }
}
