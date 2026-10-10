# Store Python Release

Use the repository's official GitHub Actions release workflow.

For the 0.2.x -> 0.3.0 transition, the intended workflow is:

```text
.github/workflows/python-wheels.yml
workflow: Python Release
input: bump=minor
```

The workflow owns:

- version calculation
- synchronized Python/Cargo version updates
- release commit
- Rust quality/tests
- Python binding tests
- platform wheels
- sdist
- PyPI trusted publishing
- Git tag
- GitHub release

Do not manually bump versions before a workflow whose job calculates the next semantic version.
Do not manually create the release tag to bypass the pipeline.
