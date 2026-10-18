#!/usr/bin/env python3
"""Pure source checks for bounded GTest control compilation; no child processes."""

import hashlib
import json
from pathlib import Path

from managed_huge_runtime_test import (
    CppText,
    baseline_gtest_names,
    control_case_names,
    select_control_source,
    verify_control_source,
)


def refuses(function, *arguments):
    try:
        function(*arguments)
    except (ValueError, KeyError, TypeError):
        return
    raise AssertionError("unsupported or altered source was accepted")


def test_exact_projection_and_utf8_offsets():
    source = '''// λ outside every test, TEST(Fake, Comment) { }
namespace scope {
int support = 4;
TEST(Suite, Keep) {
    auto nested = [] { return std::array<int,2>{{1,2}}; };
    const char *text = u8R"tag(λ TEST(Fake, Raw) { })tag";
    const char character = '}';
    EXPECT_EQ(nested()[0], 1);
}
TEST_F(Fixture, Drop) { const char *text = "λ"; EXPECT_TRUE(true); }
} // retained namespace
'''
    known = {"Suite.Keep", "Fixture.Drop"}
    projection, proof = select_control_source(source, "Suite.Keep", known)
    original, projected = source.encode(), projection.encode()
    assert len(original) == len(projected)
    assert original.count(b"\n") == projected.count(b"\n")
    assert {case["name"] for case in CppText(projection).gtest_cases()} == {"Suite.Keep"}
    restored = bytearray(projected)
    cursor = 0
    for case in proof["cases"]:
        start, end = case["byte_start"], case["byte_end"]
        assert source[case["character_start"]:case["character_end"]].encode() == original[start:end]
        assert start != case["character_start"]  # Earlier Unicode is real UTF-8.
        assert hashlib.sha256(original[start:end]).hexdigest() == case["sha256"]
        assert projected[cursor:start] == original[cursor:start]
        if case["selected"]:
            assert projected[start:end] == original[start:end]
        else:
            restored[start:end] = original[start:end]
        cursor = end
    assert projected[cursor:] == original[cursor:]
    assert restored == original and proof["exact_gaps_bodies_inverse"]
    both, _ = select_control_source(source, "Fixture.Drop:Suite.Keep", known)
    assert both == source


def test_literals_comments_and_balanced_bodies():
    hidden = [
        '// TEST(Fake, Line) { }\n',
        '/* TEST(Fake, Block) { } */\n',
        'const char *s = "TEST(Fake, String) { \\\" }";\n',
        'const char c = \'{\';\n',
        'const char *s = R"(TEST(Fake, Raw) { })";\n',
        'const char *s = LR"tag(TEST(Fake, Wide) { })tag";\n',
        'const char *s = u8R"x(TEST(Fake, Utf8) { λ })x";\n',
        'const char *s = "escaped\\\nline TEST(Fake, Continued) { }";\n',
        'const char *s = "escaped\\\r\nline TEST(Fake, CrLf) { }";\n',
        "auto count = 1'234ULL; auto bits = 0xAB'CD;\n",
    ]
    for prefix in hidden:
        source = prefix + "TEST(A, One) { auto f=[] { return 1; }; EXPECT_EQ(f(),1); }\r\nTEST_F(B, Two) { EXPECT_TRUE(true); }\r\n"
        projection, proof = select_control_source(source, "A.One", {"A.One", "B.Two"})
        assert projection.encode()[:len(prefix.encode())] == prefix.encode()
        assert len(proof["cases"]) == 2
        assert projection.count("\r\n") == source.count("\r\n")


def test_unsupported_lexing_and_registrations_refuse():
    for source in [
        '"unterminated', "'unterminated", "/* unterminated", 'R"x(unterminated',
        'R"abcdefghijklmnopq(body)abcdefghijklmnopq"', 'R"bad delimiter(body)bad delimiter"',
        '// continued\\\nTEST(A, One) {}', '/* continued\\\n */ TEST(A, One) {}',
        'int continued\\\nname; TEST(A, One) {}', '"newline\n" TEST(A, One) {}',
        'TEST(A, One) {', 'TEST(A, One) {} TEST_F(A, One) {}',
        'TEST(A + B, One) {}', 'TEST(A, One);', 'TEST(A, One) { TEST(B, Two) {} }',
        'TEST_P(A, One) {}', 'TYPED_TEST(A, One) {}', 'TYPED_TEST_P(A, One) {}',
        'GTEST_TEST(A, One) {}', 'INSTANTIATE_TEST_SUITE_P(A, B, C);',
        '#define ALIAS TEST\nALIAS(A, One) {}', '#define TEST(A,B) ignored\nTEST(A, One) {}',
        '#if ENABLED\nTEST(A, One) {}\n#endif', 'TEST(A, One) {\n#if ENABLED\n#endif\n}',
        'TEST(DISABLED_A, One) {}', 'TEST(A, DISABLED_One) {}',
    ]:
        refuses(lambda value: CppText(value).gtest_cases(), source)


def test_filter_and_baseline_identity_refusals():
    known = {"A.One", "B.Two"}
    for value in ("", ":", "A.One:", "A.One::B.Two", "A.One:A.One", "A.*", "A.?", "A.One-B.Two", "A.One/B", "0A.One", "A.DISABLED_One", "Missing.Case"):
        refuses(control_case_names, value, known)
    refuses(select_control_source, "TEST(B, Two) {}", "A.One", known)
    report = {"tests": 2, "testsuites": [{"name": "A", "testsuite": [{"name": "One"}]}, {"name": "B", "testsuite": [{"name": "Two"}]}]}
    assert baseline_gtest_names(report) == known
    refuses(baseline_gtest_names, {**report, "tests": 3})
    refuses(baseline_gtest_names, {"tests": 2, "testsuites": [report["testsuites"][0]] * 2})


def test_changed_selected_body_gap_and_omission_refuse():
    source = "int support = 4;\nTEST(A, One) { EXPECT_EQ(7,7); }\nTEST(B, Two) { EXPECT_TRUE(true); }\n"
    known = {"A.One", "B.Two"}
    projected, proof = select_control_source(source, "A.One", known)
    # Genuine source changes must fail the same verifier used by the driver.
    refuses(verify_control_source, source, projected.replace("support = 4", "support = 5"), "A.One", known)
    refuses(verify_control_source, source, projected.replace("EXPECT_EQ(7,7)", "EXPECT_EQ(7,8)"), "A.One", known)
    case = next(case for case in proof["cases"] if case["selected"])
    raw = bytearray(projected.encode())
    raw[case["byte_start"]:case["byte_end"]] = b" " * (case["byte_end"] - case["byte_start"])
    refuses(verify_control_source, source, raw.decode(), "A.One", known)
    refuses(verify_control_source, source, source, "A.One", known)


def test_current_maintained_case_inventories():
    root = next(path for path in Path(__file__).resolve().parents if (path / ".github/instructions").is_dir())
    unit = root / "tests/host/unit"
    base = "\n".join((unit / name).read_text() for name in (
        "managed_huge_test.cpp", "managed_huge_resolver_cases.inc", "managed_huge_range_cases.inc", "managed_huge_fork_cases.inc"))
    coordinator = base + "\n" + (unit / "fork_vm_coordinator_cases.inc").read_text() + "\n" + (unit / "fork_public_wiring_cases.inc").read_text()
    anonymous = coordinator + "\n" + (unit / "managed_anonymous_remove_cases.inc").read_text()
    assert [len(CppText(text).gtest_cases()) for text in (base, coordinator, anonymous)] == [70, 110, 128]


def main():
    checks = [test_exact_projection_and_utf8_offsets, test_literals_comments_and_balanced_bodies,
              test_unsupported_lexing_and_registrations_refuse, test_filter_and_baseline_identity_refusals,
              test_changed_selected_body_gap_and_omission_refuse, test_current_maintained_case_inventories]
    for check in checks:
        check()
    print(json.dumps(dict(result="PASS_PURE_GTEST_CONTROL_SELECTION", checks=[check.__name__ for check in checks], child_processes=False)))


if __name__ == "__main__":
    main()
