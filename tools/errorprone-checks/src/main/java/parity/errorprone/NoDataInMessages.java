package parity.errorprone;

import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.SeverityLevel;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.BinaryTreeMatcher;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.bugpatterns.BugChecker.NewClassTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.util.ASTHelpers;
import com.sun.source.tree.BinaryTree;
import com.sun.source.tree.ConditionalExpressionTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.Tree;
import com.sun.tools.javac.code.Symbol.MethodSymbol;
import com.sun.tools.javac.code.Type;
import java.util.List;
import java.util.Set;
import javax.lang.model.element.ElementKind;

/**
 * A log message or an exception message carries no data (TC ADR 0001 R1-5, PLAN R2-11): no phone
 * number, digit, SDP or key may reach a log line or a stack trace through a message. A message is
 * a compile-time constant, or concatenates or formats only numbers, booleans, characters, enums
 * and {@code Path}s; values go to SLF4J's {@code addKeyValue}, which TC's masking layer handles.
 * A protobuf message is never concatenated, logged as an argument or used as a key value.
 */
@BugPattern(
        summary = "A log or exception message must not carry data: use a constant and put values in addKeyValue",
        severity = SeverityLevel.ERROR)
public final class NoDataInMessages extends BugChecker
        implements MethodInvocationTreeMatcher, NewClassTreeMatcher, BinaryTreeMatcher {

    private static final long serialVersionUID = 1L;

    private static final String LOGGER = "org.slf4j.Logger";
    private static final String EVENT_BUILDER = "org.slf4j.spi.LoggingEventBuilder";
    private static final String THROWABLE = "java.lang.Throwable";
    private static final String PROTO = "com.google.protobuf.MessageOrBuilder";
    private static final Set<String> LEVELS = Set.of("trace", "debug", "info", "warn", "error");
    private static final Set<String> SAFE_TYPES = Set.of("java.nio.file.Path", "java.io.File");

    @Override
    public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
        MethodSymbol m = ASTHelpers.getSymbol(tree);
        if (m == null) {
            return Description.NO_MATCH;
        }
        String name = m.getSimpleName().toString();
        Type owner = m.owner.type;
        List<? extends ExpressionTree> args = tree.getArguments();
        if (isSubtype(owner, LOGGER, state) && LEVELS.contains(name)) {
            return checkLoggerCall(tree, m, args, state);
        }
        if (isSubtype(owner, EVENT_BUILDER, state)) {
            switch (name) {
                case "log", "setMessage" -> {
                    return checkLoggerCall(tree, m, args, state);
                }
                case "addArgument" -> {
                    return args.isEmpty() || safe(args.get(0), state) ? Description.NO_MATCH : describe(tree);
                }
                case "addKeyValue" -> {
                    return args.size() == 2 && isProto(args.get(1), state) ? describe(tree) : Description.NO_MATCH;
                }
                default -> {
                    return Description.NO_MATCH;
                }
            }
        }
        // super(message) or this(message) in an exception's constructor.
        if (m.isConstructor() && isSubtype(owner, THROWABLE, state) && isSuperOrThis(tree)) {
            return checkMessageParameters(tree, m, args, state);
        }
        return Description.NO_MATCH;
    }

    @Override
    public Description matchNewClass(NewClassTree tree, VisitorState state) {
        MethodSymbol m = ASTHelpers.getSymbol(tree);
        if (m == null || !isSubtype(m.owner.type, THROWABLE, state)) {
            return Description.NO_MATCH;
        }
        return checkMessageParameters(tree, m, tree.getArguments(), state);
    }

    @Override
    public Description matchBinary(BinaryTree tree, VisitorState state) {
        if (tree.getKind() != Tree.Kind.PLUS || !isString(ASTHelpers.getType(tree), state)) {
            return Description.NO_MATCH;
        }
        return isProto(tree.getLeftOperand(), state) || isProto(tree.getRightOperand(), state)
                ? describe(tree) : Description.NO_MATCH;
    }

    /** The message (the first String parameter) and every other argument of a log call. */
    private Description checkLoggerCall(Tree tree, MethodSymbol m, List<? extends ExpressionTree> args,
            VisitorState state) {
        boolean messageSeen = false;
        for (int i = 0; i < args.size(); i++) {
            ExpressionTree a = args.get(i);
            Type param = paramType(m, i);
            if (!messageSeen && param != null && isString(param, state)) {
                messageSeen = true;
                if (!safe(a, state)) {
                    return describe(tree);
                }
                continue;
            }
            if (isSubtype(ASTHelpers.getType(a), THROWABLE, state) || isSubtype(ASTHelpers.getType(a), "org.slf4j.Marker", state)) {
                continue;
            }
            if (messageSeen && !safe(a, state)) {
                return describe(tree);
            }
        }
        return Description.NO_MATCH;
    }

    /** Every String parameter of an exception's constructor is a message. */
    private Description checkMessageParameters(Tree tree, MethodSymbol m, List<? extends ExpressionTree> args,
            VisitorState state) {
        for (int i = 0; i < args.size(); i++) {
            Type param = paramType(m, i);
            if (param != null && isString(param, state) && !safe(args.get(i), state)) {
                return describe(tree);
            }
        }
        return Description.NO_MATCH;
    }

    /** A constant, or a concatenation, choice or format of values that can't be personal data. */
    private boolean safe(ExpressionTree tree, VisitorState state) {
        ExpressionTree t = ASTHelpers.stripParentheses(tree);
        if (t.getKind() == Tree.Kind.NULL_LITERAL || ASTHelpers.constValue(t) != null) {
            return true;
        }
        if (t instanceof BinaryTree b && b.getKind() == Tree.Kind.PLUS && isString(ASTHelpers.getType(b), state)) {
            return safe(b.getLeftOperand(), state) && safe(b.getRightOperand(), state);
        }
        if (t instanceof ConditionalExpressionTree c) {
            return safe(c.getTrueExpression(), state) && safe(c.getFalseExpression(), state);
        }
        if (t instanceof MethodInvocationTree call && onEnum(call, state)) {
            return true;
        }
        if (t instanceof IdentifierTree id && compileTimeConstant(ASTHelpers.getSymbol(id), state)) {
            return true;
        }
        if (t instanceof MethodInvocationTree call && isFormat(call)) {
            for (ExpressionTree a : call.getArguments()) {
                if (!safe(a, state)) {
                    return false;
                }
            }
            return true;
        }
        return safeType(ASTHelpers.getType(t), state);
    }

    /** An enum's own vocabulary ({@code code.trailer()}, {@code part.name()}): fixed at compile time. */
    private static boolean onEnum(MethodInvocationTree call, VisitorState state) {
        ExpressionTree receiver = ASTHelpers.getReceiver(call);
        Type t = receiver == null ? null : ASTHelpers.getType(receiver);
        return t != null && t.tsym != null && t.tsym.getKind() == ElementKind.ENUM;
    }

    /** A parameter Error Prone's own CompileTimeConstant check holds every caller to. */
    private static boolean compileTimeConstant(com.sun.tools.javac.code.Symbol sym, VisitorState state) {
        return sym != null && sym.getKind() == ElementKind.PARAMETER
                && ASTHelpers.hasAnnotation(sym, "com.google.errorprone.annotations.CompileTimeConstant", state);
    }

    private static boolean isFormat(MethodInvocationTree call) {
        MethodSymbol m = ASTHelpers.getSymbol(call);
        return m != null && m.owner.getQualifiedName().contentEquals("java.lang.String")
                && (m.getSimpleName().contentEquals("format") || m.getSimpleName().contentEquals("valueOf"));
    }

    private static boolean safeType(Type t, VisitorState state) {
        if (t == null) {
            return false;
        }
        if (t.isPrimitive()) {
            return true;
        }
        Type unboxed = state.getTypes().unboxedType(t);
        if (unboxed != null && unboxed.isPrimitive()) {
            return true;
        }
        if (t.tsym != null && t.tsym.getKind() == ElementKind.ENUM) {
            return true;
        }
        for (String s : SAFE_TYPES) {
            if (isSubtype(t, s, state)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isProto(ExpressionTree t, VisitorState state) {
        return isSubtype(ASTHelpers.getType(t), PROTO, state);
    }

    private static boolean isString(Type t, VisitorState state) {
        return t != null && state.getTypes().isSameType(t, state.getSymtab().stringType);
    }

    private static boolean isSubtype(Type t, String name, VisitorState state) {
        Type other = state.getTypeFromString(name);
        return t != null && other != null && ASTHelpers.isSubtype(t, other, state);
    }

    private static Type paramType(MethodSymbol m, int i) {
        List<com.sun.tools.javac.code.Symbol.VarSymbol> params = m.getParameters();
        if (params.isEmpty()) {
            return null;
        }
        if (i < params.size() - 1 || !m.isVarArgs()) {
            return i < params.size() ? params.get(i).type : null;
        }
        Type last = params.get(params.size() - 1).type;
        return last instanceof Type.ArrayType arr ? arr.elemtype : last;
    }

    private static boolean isSuperOrThis(MethodInvocationTree tree) {
        return tree.getMethodSelect() instanceof IdentifierTree id
                && (id.getName().contentEquals("super") || id.getName().contentEquals("this"));
    }

    private Description describe(Tree tree) {
        return describeMatch(tree);
    }
}
