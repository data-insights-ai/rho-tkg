# Instant and unit historical alternative

**DATA FOR REVIEW, PENDING SOURCE REVIEW / INTEGRATION.** The unchanged six-file
[`instant-units.patch`](instant-units.patch) is a reviewed historical alternative,
not the active owner implementation. Its new-file patch now collides with owner
drafts; do not apply it. Current drafts are not evidence of a committed defect.

The alternative adds `types.Instant` and five provisional adapters using existing
TP bytes, exact scalar arithmetic, explicit target axes and distinct fractional/
range refusals. It infers no clock, epoch, reference mapping, temporal
interpretation or interval-domain conversion. The fixture-only ID table is explicit.

[Validation](instant-units-validation.json) separates missing-API compile-red,
four synthetic mutants, scoped Go 1.26.9 checks and parent focused race.
[Extraction evidence](golden-extraction-proof.json) compares all seven complete
records with the unchanged 42-case corpus, retaining each content hash. Original
16/52 and provenance are preserved. Actual owner-source conformance review,
canonical integration and broader phase/release acceptance remain pending.
