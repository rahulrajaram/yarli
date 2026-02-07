# CARD-R5-01 Verification Evidence

```text
Command: test -s docs/RELEASE_BASELINE.md
Exit Code: 0
Key Output:
- (no output)
- (no output)
- (no output)
```

```text
Command: rg -n "git rev-parse HEAD|R4V_HARDENING_VERIFIED|ACCEPTANCE_RUBRIC|evidence/r4v|git tag -a" docs/RELEASE_BASELINE.md
Exit Code: 0
Key Output:
- 5:- Baseline marker: `R4V_HARDENING_VERIFIED`
- 13:- Acceptance rubric: `docs/ACCEPTANCE_RUBRIC.md`
- 27:git tag -a "${RC_TAG}" "${BASELINE_COMMIT}" -m "YARLI release candidate ${RC_TAG}"
```
