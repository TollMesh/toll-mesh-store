# TollMeshCache: Multi-Language SDK Publishing Guide

Complete step-by-step instructions for publishing TollMeshCache SDKs to package managers in all 7 supported languages.

---

## 📦 PYTHON SDK

### 1. Package Structure
```
tollmeshcache-py/
├── setup.py
├── setup.cfg
├── pyproject.toml
├── README.md
├── LICENSE
├── tollmeshcache/
│   ├── __init__.py
│   ├── queue/
│   │   ├── __init__.py
│   │   ├── job.py
│   │   ├── job_manager.py
│   │   └── errors.py
│   ├── sortedset/
│   │   ├── __init__.py
│   │   ├── skiplist.py
│   │   ├── sorted_set.py
│   │   └── errors.py
│   ├── stream/
│   │   ├── __init__.py
│   │   ├── entry.py
│   │   ├── consumer_group.py
│   │   └── errors.py
│   └── common/
│       ├── __init__.py
│       ├── clocks.py
│       └── types.py
├── tests/
│   ├── test_queue.py
│   ├── test_sortedset.py
│   ├── test_stream.py
│   └── conftest.py
└── examples/
    ├── queue_example.py
    ├── sortedset_example.py
    └── stream_example.py
```

### 2. Setup Configuration
```bash
# Create pyproject.toml
cat > pyproject.toml << 'EOF'
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "tollmeshcache"
version = "1.0.0"
description = "Distributed in-memory data structures with CRDT support"
readme = "README.md"
requires-python = ">=3.8"
license = {text = "Apache-2.0"}
authors = [{name = "TollMesh Contributors"}]
keywords = ["distributed", "crdt", "queue", "cache", "eventually-consistent"]
classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Topic :: Software Development :: Libraries",
]
EOF
```

### 3. Build & Test
```bash
# Install build dependencies
pip install --upgrade pip setuptools wheel twine

# Build source distribution and wheel
python -m build

# Run tests
pip install pytest pytest-asyncio pytest-cov
pytest tests/ --cov=tollmeshcache --cov-report=html

# Check build artifacts
twine check dist/*
```

### 4. Publish to PyPI
```bash
# Create ~/.pypirc for credentials
cat > ~/.pypirc << 'EOF'
[distutils]
index-servers = pypi testpypi

[pypi]
repository = https://upload.pypi.org/legacy/
username = __token__
password = pypi-AgEIc...  # Use token, not password

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-AgEIc...  # Test token
EOF

# Test publish to TestPyPI first
twine upload --repository testpypi dist/*

# Install from TestPyPI to verify
pip install --index-url https://test.pypi.org/simple/ tollmeshcache

# Publish to PyPI
twine upload dist/*
```

### 5. Installation for Users
```bash
# Install from PyPI
pip install tollmeshcache

# Use in code
from tollmeshcache.queue import JobManager
from tollmeshcache.sortedset import SortedSet
from tollmeshcache.stream import Stream, ConsumerGroup

# Async operations
import asyncio

async def main():
    jm = JobManager("node-1")
    job = await jm.enqueue_async("queue-1", b"task")
```

---

## 📦 NODE.JS SDK

### 1. Package Structure
```
tollmeshcache-js/
├── package.json
├── tsconfig.json
├── webpack.config.js
├── README.md
├── LICENSE
├── src/
│   ├── index.ts
│   ├── queue/
│   │   ├── job.ts
│   │   ├── job-manager.ts
│   │   └── types.ts
│   ├── sortedset/
│   │   ├── skiplist.ts
│   │   ├── sorted-set.ts
│   │   └── types.ts
│   ├── stream/
│   │   ├── entry.ts
│   │   ├── consumer-group.ts
│   │   └── types.ts
│   └── common/
│       ├── clocks.ts
│       └── errors.ts
├── dist/
│   ├── index.js
│   ├── index.d.ts
│   └── *.js
├── __tests__/
│   ├── queue.test.ts
│   ├── sortedset.test.ts
│   └── stream.test.ts
└── examples/
    ├── queue-example.js
    ├── sortedset-example.js
    └── stream-example.js
```

### 2. Package Configuration
```json
{
  "name": "tollmeshcache",
  "version": "1.0.0",
  "description": "Distributed in-memory data structures with CRDT support",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "tsc",
    "test": "jest",
    "test:coverage": "jest --coverage",
    "lint": "eslint src/",
    "prepublishOnly": "npm run build && npm run test",
    "bundle": "webpack"
  },
  "keywords": ["distributed", "crdt", "queue", "cache"],
  "license": "Apache-2.0",
  "author": "TollMesh Contributors",
  "devDependencies": {
    "@types/jest": "^29.0.0",
    "@types/node": "^18.0.0",
    "jest": "^29.0.0",
    "ts-jest": "^29.0.0",
    "typescript": "^5.0.0",
    "webpack": "^5.0.0"
  },
  "engines": {
    "node": ">=14.0.0"
  }
}
```

### 3. Build & Test
```bash
# Install dependencies
npm install

# Compile TypeScript
npm run build

# Run tests
npm run test:coverage

# Lint code
npm run lint

# Create browser bundle
npm run bundle
```

### 4. Publish to npm
```bash
# Create .npmrc for authentication
cat > ~/.npmrc << 'EOF'
registry=https://registry.npmjs.org/
//registry.npmjs.org/:_authToken=npm_AgEIc...
EOF

# Verify package
npm pack

# Dry run
npm publish --dry-run

# Publish to npm
npm publish --access public

# Publish scoped package (optional)
npm publish --scope=@tollmesh --access public
```

### 5. Installation for Users
```bash
# Install from npm
npm install tollmeshcache

# Or with yarn
yarn add tollmeshcache

# Use in code
const { JobManager, SortedSet, Stream } = require('tollmeshcache');

// Async/await
const jm = new JobManager('node-1');
const job = await jm.enqueueAsync('queue-1', Buffer.from('task'));

// TypeScript
import { JobManager, SortedSet, Stream } from 'tollmeshcache';
```

---

## 📦 JAVA SDK

### 1. Package Structure
```
tollmeshcache-java/
├── pom.xml
├── README.md
├── LICENSE
├── src/
│   ├── main/java/com/tollmesh/cache/
│   │   ├── package-info.java
│   │   ├── TollMeshCache.java
│   │   ├── queue/
│   │   │   ├── Job.java
│   │   │   ├── JobManager.java
│   │   │   └── JobStatus.java
│   │   ├── sortedset/
│   │   │   ├── SkipList.java
│   │   │   ├── SortedSet.java
│   │   │   └── SortedSetMember.java
│   │   ├── stream/
│   │   │   ├── StreamEntry.java
│   │   │   ├── Stream.java
│   │   │   ├── ConsumerGroup.java
│   │   │   └── ConsumerGroupMember.java
│   │   └── common/
│   │       ├── Clock.java
│   │       └── Errors.java
│   └── test/java/com/tollmesh/cache/
│       ├── QueueTest.java
│       ├── SortedSetTest.java
│       └── StreamTest.java
└── target/
    ├── tollmeshcache-1.0.0.jar
    └── tollmeshcache-1.0.0-javadoc.jar
```

### 2. Maven Configuration
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.tollmesh</groupId>
  <artifactId>tollmeshcache</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>
  
  <name>TollMeshCache</name>
  <description>Distributed in-memory data structures with CRDT support</description>
  <url>https://github.com/toll-mesh/store</url>
  
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>https://www.apache.org/licenses/LICENSE-2.0</url>
    </license>
  </licenses>
  
  <scm>
    <url>https://github.com/toll-mesh/store</url>
    <connection>scm:git:https://github.com/toll-mesh/store.git</connection>
  </scm>
  
  <developers>
    <developer>
      <name>TollMesh Contributors</name>
      <url>https://github.com/toll-mesh</url>
    </developer>
  </developers>
  
  <properties>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
  </properties>
  
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.13.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
  
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.8.1</version>
        <configuration>
          <source>11</source>
          <target>11</target>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-javadoc-plugin</artifactId>
        <version>3.3.0</version>
      </plugin>
    </plugins>
  </build>
  
  <distributionManagement>
    <snapshotRepository>
      <id>ossrh</id>
      <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
    </snapshotRepository>
    <repository>
      <id>ossrh</id>
      <url>https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/</url>
    </repository>
  </distributionManagement>
</project>
```

### 3. Build & Test
```bash
# Build project
mvn clean package

# Run tests
mvn test

# Generate Javadoc
mvn javadoc:javadoc

# Generate coverage report
mvn cobertura:cobertura
```

### 4. Publish to Maven Central
```bash
# Create GPG key for signing
gpg --gen-key

# Add to ~/.m2/settings.xml
cat >> ~/.m2/settings.xml << 'EOF'
<servers>
  <server>
    <id>ossrh</id>
    <username>sonatype_username</username>
    <password>sonatype_password</password>
  </server>
</servers>
EOF

# Build with signing
mvn clean deploy -P release

# Or directly to Central via OSSRH
mvn clean deploy
```

### 5. Installation for Users
```bash
# Maven dependency
<dependency>
  <groupId>com.tollmesh</groupId>
  <artifactId>tollmeshcache</artifactId>
  <version>1.0.0</version>
</dependency>

# Gradle
implementation 'com.tollmesh:tollmeshcache:1.0.0'

# Usage in code
import com.tollmesh.cache.queue.JobManager;
import com.tollmesh.cache.sortedset.SortedSet;
import com.tollmesh.cache.stream.Stream;

JobManager jm = new JobManager("node-1");
Job job = jm.enqueue("queue-1", "task".getBytes());
```

---

## 📦 RUST SDK

### 1. Package Structure
```
tollmeshcache-rs/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── LICENSE
├── src/
│   ├── lib.rs
│   ├── queue/
│   │   ├── mod.rs
│   │   ├── job.rs
│   │   └── manager.rs
│   ├── sortedset/
│   │   ├── mod.rs
│   │   ├── skiplist.rs
│   │   └── sorted_set.rs
│   ├── stream/
│   │   ├── mod.rs
│   │   ├── entry.rs
│   │   └── consumer_group.rs
│   └── common/
│       ├── mod.rs
│       ├── clocks.rs
│       └── errors.rs
├── tests/
│   ├── queue_tests.rs
│   ├── sortedset_tests.rs
│   └── stream_tests.rs
└── benches/
    ├── queue_bench.rs
    ├── sortedset_bench.rs
    └── stream_bench.rs
```

### 2. Cargo Configuration
```toml
[package]
name = "tollmeshcache"
version = "1.0.0"
edition = "2021"
authors = ["TollMesh Contributors"]
license = "Apache-2.0"
description = "Distributed in-memory data structures with CRDT support"
repository = "https://github.com/toll-mesh/store"
keywords = ["distributed", "crdt", "queue", "cache"]

[lib]
name = "tollmeshcache"
path = "src/lib.rs"

[dependencies]
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
parking_lot = "0.12"
rand = "0.8"

[dev-dependencies]
criterion = "0.5"
tokio-test = "0.4"

[[bench]]
name = "queue_bench"
harness = false

[profile.release]
opt-level = 3
lto = true
```

### 3. Build & Test
```bash
# Build library
cargo build --release

# Run tests
cargo test

# Run with coverage
cargo tarpaulin --out Html

# Run benchmarks
cargo bench

# Check documentation
cargo doc --open

# Format code
cargo fmt

# Lint
cargo clippy
```

### 4. Publish to crates.io
```bash
# Create account and token at crates.io

# Login
cargo login your_token_here

# Check package before publishing
cargo package

# Publish
cargo publish

# Publish specific version
cargo publish --version 1.0.0
```

### 5. Installation for Users
```toml
# Cargo.toml
[dependencies]
tollmeshcache = "1.0"
tokio = { version = "1.0", features = ["full"] }

# Usage in code
use tollmeshcache::queue::JobManager;
use tollmeshcache::sortedset::SortedSet;
use tollmeshcache::stream::Stream;

#[tokio::main]
async fn main() {
    let mut jm = JobManager::new("node-1");
    let job = jm.enqueue("queue-1", b"task").await.unwrap();
}
```

---

## 📦 RUBY SDK

### 1. Package Structure
```
tollmeshcache-ruby/
├── Gemfile
├── gemspec.rb
├── Rakefile
├── README.md
├── LICENSE
├── lib/
│   ├── tollmeshcache.rb
│   ├── tollmeshcache/
│   │   ├── version.rb
│   │   ├── queue/
│   │   │   ├── job.rb
│   │   │   └── manager.rb
│   │   ├── sortedset/
│   │   │   ├── skiplist.rb
│   │   │   └── sorted_set.rb
│   │   ├── stream/
│   │   │   ├── entry.rb
│   │   │   └── consumer_group.rb
│   │   └── common/
│   │       └── clocks.rb
├── spec/
│   ├── spec_helper.rb
│   ├── queue_spec.rb
│   ├── sortedset_spec.rb
│   └── stream_spec.rb
└── examples/
    ├── queue_example.rb
    ├── sortedset_example.rb
    └── stream_example.rb
```

### 2. Gemspec Configuration
```ruby
# tollmeshcache.gemspec
Gem::Specification.new do |spec|
  spec.name          = "tollmeshcache"
  spec.version       = TollMeshCache::VERSION
  spec.authors       = ["TollMesh Contributors"]
  spec.email         = ["contributors@tollmesh.io"]
  
  spec.summary       = "Distributed in-memory data structures with CRDT support"
  spec.description   = "Production-grade CRDT-based cache for distributed systems"
  spec.homepage      = "https://github.com/toll-mesh/store"
  spec.license       = "Apache-2.0"
  
  spec.required_ruby_version = ">= 2.7"
  
  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  
  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.10"
  spec.add_development_dependency "rspec-async", "~> 1.0"
  spec.add_development_dependency "rubocop", "~> 1.0"
end
```

### 3. Build & Test
```bash
# Install dependencies
bundle install

# Run tests
bundle exec rspec

# Generate coverage
COVERAGE=true bundle exec rspec

# Lint code
bundle exec rubocop

# Build gem
gem build tollmeshcache.gemspec
```

### 4. Publish to RubyGems
```bash
# Create account at rubygems.org

# Store credentials
curl -u your_username https://rubygems.org/api/v1/api_key.json > ~/.gem/credentials

# Publish
gem push tollmeshcache-1.0.0.gem

# Verify
gem search tollmeshcache
```

### 5. Installation for Users
```ruby
# Gemfile
gem 'tollmeshcache', '~> 1.0'

# Usage in code
require 'tollmeshcache'

jm = TollMeshCache::Queue::Manager.new('node-1')
job = jm.enqueue('queue-1', 'task')

# Async/await with Fiber
Async do |task|
  job = await jm.enqueue_async('queue-1', 'task')
end
```

---

## 📦 C# SDK

### 1. Package Structure
```
TollMeshCache/
├── TollMeshCache.sln
├── TollMeshCache/
│   ├── TollMeshCache.csproj
│   ├── Queue/
│   │   ├── Job.cs
│   │   └── JobManager.cs
│   ├── SortedSet/
│   │   ├── SkipList.cs
│   │   └── SortedSet.cs
│   ├── Stream/
│   │   ├── StreamEntry.cs
│   │   ├── Stream.cs
│   │   └── ConsumerGroup.cs
│   ├── Common/
│   │   └── Clocks.cs
│   └── TollMeshCache.cs
├── TollMeshCache.Tests/
│   ├── TollMeshCache.Tests.csproj
│   ├── QueueTests.cs
│   ├── SortedSetTests.cs
│   └── StreamTests.cs
└── README.md
```

### 2. Project Configuration
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net6.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    
    <PackageId>TollMeshCache</PackageId>
    <Version>1.0.0</Version>
    <Title>TollMeshCache</Title>
    <Description>Distributed in-memory data structures with CRDT support</Description>
    <Authors>TollMesh Contributors</Authors>
    <PackageProjectUrl>https://github.com/toll-mesh/store</PackageProjectUrl>
    <PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>
    <RepositoryUrl>https://github.com/toll-mesh/store</RepositoryUrl>
    <RepositoryType>git</RepositoryType>
    <PackageTags>distributed;crdt;queue;cache</PackageTags>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="System.Threading.Channels" Version="7.0.0" />
  </ItemGroup>
</Project>
```

### 3. Build & Test
```bash
# Build solution
dotnet build --configuration Release

# Run tests
dotnet test

# Generate coverage
dotnet test /p:CollectCoverage=true

# Pack NuGet package
dotnet pack --configuration Release

# Publish locally
dotnet nuget add source ./nupkg --name local
```

### 4. Publish to NuGet
```bash
# Create account at nuget.org

# Store API key
dotnet nuget update source nuget.org \
  --username your_username \
  --password your_api_key \
  --store-password-in-clear-text

# Publish
dotnet nuget push bin/Release/TollMeshCache.1.0.0.nupkg \
  --source https://api.nuget.org/v3/index.json \
  --api-key your_api_key

# Verify
nuget search TollMeshCache
```

### 5. Installation for Users
```xml
<!-- .csproj -->
<ItemGroup>
  <PackageReference Include="TollMeshCache" Version="1.0.0" />
</ItemGroup>

// Usage in code
using TollMeshCache.Queue;
using TollMeshCache.SortedSet;
using TollMeshCache.Stream;

var jm = new JobManager("node-1");
var job = await jm.EnqueueAsync("queue-1", "task");
```

---

## 📦 PHP SDK

### 1. Package Structure
```
tollmeshcache-php/
├── composer.json
├── README.md
├── LICENSE
├── src/
│   ├── TollMeshCache.php
│   ├── Queue/
│   │   ├── Job.php
│   │   └── JobManager.php
│   ├── SortedSet/
│   │   ├── SkipList.php
│   │   └── SortedSet.php
│   ├── Stream/
│   │   ├── StreamEntry.php
│   │   ├── Stream.php
│   │   └── ConsumerGroup.php
│   └── Common/
│       └── Clocks.php
├── tests/
│   ├── QueueTest.php
│   ├── SortedSetTest.php
│   └── StreamTest.php
└── examples/
    ├── queue_example.php
    ├── sortedset_example.php
    └── stream_example.php
```

### 2. Composer Configuration
```json
{
  "name": "tollmesh/cache",
  "description": "Distributed in-memory data structures with CRDT support",
  "type": "library",
  "license": "Apache-2.0",
  "authors": [
    {
      "name": "TollMesh Contributors",
      "email": "contributors@tollmesh.io"
    }
  ],
  "homepage": "https://github.com/toll-mesh/store",
  "require": {
    "php": ">=8.0"
  },
  "require-dev": {
    "phpunit/phpunit": "^10.0",
    "phpstan/phpstan": "^1.0",
    "squizlabs/php_codesniffer": "^3.7"
  },
  "autoload": {
    "psr-4": {
      "TollMesh\\Cache\\": "src/"
    }
  },
  "autoload-dev": {
    "psr-4": {
      "TollMesh\\Cache\\Tests\\": "tests/"
    }
  },
  "scripts": {
    "test": "phpunit",
    "static-analysis": "phpstan analyse src/",
    "lint": "phpcs --standard=PSR12 src/"
  }
}
```

### 3. Build & Test
```bash
# Install dependencies
composer install

# Run tests
composer test

# Static analysis
composer static-analysis

# Lint code
composer lint

# Update dependencies
composer update
```

### 4. Publish to Packagist
```bash
# Register at packagist.org

# Connect GitHub repository to Packagist
# (Automatic publishing on tag/release)

# Or manually submit
# https://packagist.org/packages/submit

# Tag release
git tag -a v1.0.0 -m "Release 1.0.0"
git push --tags

# Verify
composer search tollmesh/cache
```

### 5. Installation for Users
```bash
# Install via Composer
composer require tollmesh/cache:^1.0

# Usage in code
<?php
require 'vendor/autoload.php';

use TollMesh\Cache\Queue\JobManager;
use TollMesh\Cache\SortedSet\SortedSet;
use TollMesh\Cache\Stream\Stream;

$jm = new JobManager('node-1');
$job = $jm->enqueue('queue-1', 'task');
```

---

## 🎯 UNIFIED PUBLISHING CHECKLIST

### Pre-Release
- [ ] All tests passing in respective language
- [ ] Documentation complete (README, examples, API docs)
- [ ] Code formatted and linted
- [ ] Version number updated (1.0.0)
- [ ] Changelog created
- [ ] License file included
- [ ] Authors/contributors documented

### Package Setup
- [ ] Account created on package manager
- [ ] API token/credentials generated
- [ ] Authentication configured locally
- [ ] Package metadata complete (description, keywords, homepage)
- [ ] Repository linked

### Quality Checks
- [ ] Code coverage > 80%
- [ ] No security vulnerabilities (scan dependencies)
- [ ] Performance benchmarks established
- [ ] Example code runs without errors
- [ ] Documentation builds without warnings

### Publishing
- [ ] Dry run successful
- [ ] Package published to correct registry
- [ ] Version appears in package manager search
- [ ] Installation from package manager works
- [ ] Imported package runs without errors

### Post-Release
- [ ] GitHub release created
- [ ] Release notes published
- [ ] Documentation website updated
- [ ] Announcement posted (blog, social media)
- [ ] Monitor for issues/questions

---

## 📊 PACKAGE MANAGER COMPARISON

| Language | Manager | URL | Verification |
|----------|---------|-----|--------------|
| Python | PyPI | pypi.org | `pip search tollmeshcache` |
| Node.js | npm | npmjs.com | `npm info tollmeshcache` |
| Java | Maven | central.maven.org | Search Maven Central |
| Rust | crates.io | crates.io | `cargo search tollmeshcache` |
| Ruby | RubyGems | rubygems.org | `gem search tollmeshcache` |
| C# | NuGet | nuget.org | `nuget search TollMeshCache` |
| PHP | Packagist | packagist.org | `composer search tollmesh/cache` |

---

## 🔐 SECURITY BEST PRACTICES

1. **Sign Releases:** GPG signatures for source artifacts
2. **API Tokens:** Use minimal-privilege tokens per registry
3. **Dependencies:** Pin transitive dependencies
4. **Scanning:** Regular vulnerability scans (Snyk, WhiteSource)
5. **Supply Chain:** Only publish from CI/CD pipelines
6. **Notifications:** Subscribe to security advisories

---

## 📈 VERSION MANAGEMENT

**Semantic Versioning:** MAJOR.MINOR.PATCH

- **MAJOR (1.0.0):** Initial release with 3 core features
- **MINOR (1.1.0):** Add new data structure (e.g., Bloom Filters)
- **PATCH (1.0.1):** Bug fixes and performance improvements

---

## 🚀 CONTINUOUS PUBLISHING

Set up CI/CD to auto-publish on git tag:

```yaml
# Example GitHub Actions workflow
on:
  push:
    tags:
      - 'v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Publish to PyPI
        env:
          PYPI_TOKEN: ${{ secrets.PYPI_TOKEN }}
        run: |
          pip install twine
          twine upload dist/*
```

---

**Next:** After publishing to all 7 package managers, create unified documentation site and marketing materials.
