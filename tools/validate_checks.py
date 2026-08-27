"""
validate_checks.py — standalone schema validator for the DQX rule catalog.

Mirrors what `DQEngine.validate_checks()` does at runtime inside the real
notebook (see notebooks/dqx_crm_gold.py), but as a dependency-free script
that runs in CI on every push/PR — so a malformed rule fails fast in review,
not at 2am when the scheduled job runs against it.

Usage: python tools/validate_checks.py notebooks/dqx_crm_gold_checks.yml
Exit code 0 = all rules valid, 1 = one or more rules failed validation.
"""
import sys

import yaml

REQUIRED_RULE_KEYS = {"name", "table_name", "criticality", "check"}
VALID_CRITICALITY = {"error", "warn"}
REQUIRED_CHECK_KEYS = {"function"}


def validate(path: str) -> list[str]:
    with open(path) as f:
        checks = yaml.safe_load(f)

    errors = []
    seen_names = set()

    for i, rule in enumerate(checks):
        label = rule.get("name", f"<rule at index {i}, no name>")

        missing = REQUIRED_RULE_KEYS - rule.keys()
        if missing:
            errors.append(f"[{label}] missing required key(s): {sorted(missing)}")
            continue

        if rule["name"] in seen_names:
            errors.append(f"[{label}] duplicate rule name")
        seen_names.add(rule["name"])

        if rule["criticality"] not in VALID_CRITICALITY:
            errors.append(
                f"[{label}] invalid criticality '{rule['criticality']}' "
                "(must be 'error' or 'warn')"
            )

        check = rule["check"]
        if not isinstance(check, dict):
            errors.append(f"[{label}] 'check' must be a mapping")
            continue

        missing_check = REQUIRED_CHECK_KEYS - check.keys()
        if missing_check:
            errors.append(f"[{label}] check missing key(s): {sorted(missing_check)}")

        if "arguments" not in check and "for_each_column" not in check:
            errors.append(
                f"[{label}] check has neither 'arguments' nor 'for_each_column'"
            )

    return errors, len(checks)


def main():
    if len(sys.argv) != 2:
        print("usage: python tools/validate_checks.py <path-to-checks.yml>")
        sys.exit(2)

    path = sys.argv[1]
    errors, total = validate(path)

    print(f"Validated {total} rules from {path}")
    if errors:
        print(f"\n{len(errors)} rule(s) failed validation:")
        for e in errors:
            print(f"  [FAIL] {e}")
        sys.exit(1)

    print("All rules valid.")
    sys.exit(0)


if __name__ == "__main__":
    main()
