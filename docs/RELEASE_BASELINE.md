# YARLI Release Baseline (R5 Candidate)

## Baseline Provenance

- Baseline marker: `R4V_HARDENING_VERIFIED`
- Capture date (UTC): `2026-02-07T20:58:40Z`
- Baseline commit (`git rev-parse HEAD` at capture time): `153c3a5b3ebb693c53b600a6a3bdf75a3e25ecfe`

This baseline freezes the previously hardened Loop-4 verification state and defines the release-candidate tag procedure for auditors.

## Verifier Inputs

- Acceptance rubric: `docs/ACCEPTANCE_RUBRIC.md`
- Hardened verification evidence: `evidence/r4v/README.md`

## Release-Candidate Tag Plan

Intended tag format:

- `yarli-v<semver>-rc.<yyyymmdd>`

Exact non-interactive commands:

```bash
BASELINE_COMMIT="$(git rev-parse HEAD)"
RC_TAG="yarli-v0.1.0-rc.20260207"
git tag -a "${RC_TAG}" "${BASELINE_COMMIT}" -m "YARLI release candidate ${RC_TAG}"
git show "${RC_TAG}" --no-patch --decorate
```

If a baseline tag already exists, delete and recreate only with explicit reviewer approval:

```bash
git tag -d "${RC_TAG}"
git tag -a "${RC_TAG}" "${BASELINE_COMMIT}" -m "YARLI release candidate ${RC_TAG}"
```
