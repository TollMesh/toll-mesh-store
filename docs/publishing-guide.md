---
layout: default
title: Publishing Guide
nav_order: 13
---

# Publishing Guide

How releases reach each of the 7 package managers.

---

## Overview

Each SDK publishes independently, triggered by a tag matching that language's own pattern and scoped to changes under that SDK's directory:

| Language | Package Manager | Trigger tag | Workflow |
|----------|-----------------|-------------|----------|
| Python | PyPI | `v*` | `.github/workflows/publish-python.yml` |
| Node.js/TypeScript | npm | `node-v*` | `.github/workflows/publish-nodejs.yml` |
| Rust | crates.io | `rust-v*` | `.github/workflows/publish-rust.yml` |
| Ruby | RubyGems | `ruby-v*` | `.github/workflows/publish-ruby.yml` |
| C#/.NET | NuGet | `csharp-v*` | `.github/workflows/publish-csharp.yml` |
| PHP | Packagist | `php-v*` | `.github/workflows/publish-php.yml` |
| Java | Maven Central | `java-v*` | `.github/workflows/publish-java.yml` |

Publishing to PyPI uses [OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/) rather than a long-lived API token stored as a secret — GitHub Actions authenticates to PyPI directly via a short-lived OIDC token scoped to the specific workflow file and environment. The other six registries are authenticated with per-registry API tokens/credentials stored as GitHub Actions secrets, standard for each ecosystem (npm token, `CARGO_REGISTRY_TOKEN`, RubyGems API key, NuGet API key, Packagist auto-update webhook or token, Sonatype/Maven Central credentials + GPG signing key).

## Releasing a new version

1. Bump the version in that SDK's manifest (`setup.py`/`pyproject.toml`, `package.json`, `Cargo.toml`, `*.gemspec`, `.csproj`, `composer.json`, `pom.xml`).
2. Commit and push.
3. Tag with that language's trigger pattern from the table above, e.g. `git tag node-v1.1.0 && git push origin node-v1.1.0`. Python is the exception — it publishes on a plain `v*` tag rather than a language-prefixed one.
4. The matching workflow builds, runs that SDK's test suite, and publishes on a green build.

Six of the seven languages publish on their own language-prefixed tag namespace specifically so that shipping a fix to one SDK doesn't force a version bump — or a publish — of the other five. Python is the exception, publishing on the unprefixed `v*` tag.

## Maintainer setup details

Registry-by-registry account setup, trusted-publisher/token configuration, and the full workflow YAML for each of the 7 registries are kept in [`internal-docs/`](https://github.com/TollMesh/toll-mesh-store/tree/master/internal-docs) in the repository rather than duplicated here, since that level of detail (credential setup, per-registry account steps) is for maintainers cutting a release, not for consumers of the packages.
