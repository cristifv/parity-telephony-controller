package parity.telephonycontroller;

import static com.tngtech.archunit.core.domain.JavaCall.Predicates.target;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.google.protobuf.MessageOrBuilder;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** The boundaries TC ADR 0001 §4.7 and step 5 rely on. */
@AnalyzeClasses(packages = "parity.telephonycontroller", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    /** Only the api package touches the generated types, so nothing else can log a message. */
    @ArchTest
    static final ArchRule GENERATED_TYPES_ONLY_IN_API = noClasses().that()
            .resideOutsideOfPackages("parity.telephonycontroller.api..", "parity.telephonycontroller.obs..")
            .should().dependOnClassesThat().resideInAPackage("parity.contracts.telephony.v1..");

    /** No code calls toString() on a protobuf message (R1-5). */
    @ArchTest
    static final ArchRule NO_PROTO_TO_STRING = noClasses().should().callMethodWhere(
            target(owner(assignableTo(MessageOrBuilder.class))).and(target(name("toString"))));

    /** The fake (its control endpoint, its hooks) is reached only through the wiring. */
    @ArchTest
    static final ArchRule FAKE_ONLY_FROM_WIRING = noClasses().that()
            .resideOutsideOfPackages("parity.telephonycontroller.fake..", "parity.telephonycontroller.config..")
            .should().dependOnClassesThat().resideInAPackage("parity.telephonycontroller.fake..");

    /** The engine and its parts stay plain Java: no Spring, so the fake and the server share them. */
    @ArchTest
    static final ArchRule ENGINE_WITHOUT_SPRING = noClasses().that().resideInAnyPackage(
            "parity.telephonycontroller.session..", "parity.telephonycontroller.node..",
            "parity.telephonycontroller.owner..", "parity.telephonycontroller.journal..",
            "parity.telephonycontroller.time..", "parity.telephonycontroller.model..")
            .should().dependOnClassesThat().resideInAPackage("org.springframework..");
}
