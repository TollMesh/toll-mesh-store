# Publishing Guide for TollMeshStore

This guide explains how to publish TollMeshStore client libraries to all major package managers.

## 📦 Package Manager Publishing Checklist

### ✅ Go (Already Published)
- **Status**: Ready via `go get`
- **Command**: `go get github.com/toll-mesh/store`
- **No action needed** - Go packages are automatically available via GitHub

### 🐍 Python (PyPI)

#### Prerequisites
```bash
pip install twine build
```

#### Setup
1. Create `setup.py` in project root:
```python
from setuptools import setup, find_packages

setup(
    name="tollmesh-client",
    version="1.0.0",
    description="TollMeshStore Python Client - Redis alternative with intelligent features",
    author="Prakhar Tripathi & Mayaplus",
    author_email="prakhar@tollmesh.dev",
    url="https://github.com/Prakhar998/toll-mesh-store",
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.56.0",
        "grpcio-tools>=1.56.0",
        "protobuf>=3.23.0",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
```

#### Publish
```bash
# Build
python -m build

# Upload to PyPI
twine upload dist/*

# Install
pip install tollmesh-client
```

### 📦 JavaScript/Node.js (npm)

#### Setup
1. Create `package.json`:
```json
{
  "name": "@toll-mesh/client",
  "version": "1.0.0",
  "description": "TollMeshStore JavaScript Client - Redis alternative with intelligent features",
  "main": "index.js",
  "types": "index.d.ts",
  "author": "Prakhar Tripathi & Mayaplus",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "https://github.com/Prakhar998/toll-mesh-store.git"
  },
  "dependencies": {
    "@grpc/grpc-js": "^1.9.0",
    "@grpc/proto-loader": "^0.7.0"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "typescript": "^5.0.0"
  },
  "keywords": [
    "cache",
    "distributed",
    "grpc",
    "redis",
    "alternative"
  ]
}
```

#### Publish
```bash
# Login to npm
npm login

# Publish
npm publish --access public

# Install
npm install @toll-mesh/client
```

### ☕ Java (Maven Central)

#### Setup
1. Create `pom.xml`:
```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.toll-mesh</groupId>
  <artifactId>tollmesh-client</artifactId>
  <version>1.0.0</version>
  <name>TollMeshStore Java Client</name>
  <description>TollMeshStore Java Client - Redis alternative with intelligent features</description>
  <url>https://github.com/Prakhar998/toll-mesh-store</url>
  
  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://opensource.org/licenses/MIT</url>
    </license>
  </licenses>
  
  <developers>
    <developer>
      <name>Prakhar Tripathi</name>
      <email>prakhar@tollmesh.dev</email>
    </developer>
  </developers>
  
  <scm>
    <connection>scm:git:https://github.com/Prakhar998/toll-mesh-store.git</connection>
    <url>https://github.com/Prakhar998/toll-mesh-store</url>
  </scm>
  
  <dependencies>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-netty-shaded</artifactId>
      <version>1.56.0</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-protobuf</artifactId>
      <version>1.56.0</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-stub</artifactId>
      <version>1.56.0</version>
    </dependency>
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>3.23.0</version>
    </dependency>
  </dependencies>
</project>
```

#### Publish
```bash
# Setup GPG key for signing
gpg --gen-key

# Configure Maven settings.xml with credentials

# Deploy to Maven Central
mvn clean deploy -P release

# Install
# Maven: Add to pom.xml
# Gradle: Add to build.gradle
```

### 🦀 Rust (crates.io)

#### Setup
1. Create `Cargo.toml`:
```toml
[package]
name = "tollmesh"
version = "1.0.0"
edition = "2021"
authors = ["Prakhar Tripathi & Mayaplus"]
license = "MIT"
description = "TollMeshStore Rust Client - Redis alternative with intelligent features"
repository = "https://github.com/Prakhar998/toll-mesh-store"
homepage = "https://github.com/Prakhar998/toll-mesh-store"
documentation = "https://docs.rs/tollmesh"
keywords = ["cache", "distributed", "grpc", "redis"]
categories = ["network-programming", "database"]

[dependencies]
tonic = "0.10"
prost = "0.12"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"

[build-dependencies]
tonic-build = "0.10"
```

#### Publish
```bash
# Create crates.io account
# https://crates.io/me

# Login
cargo login

# Publish
cargo publish

# Install
cargo add tollmesh
```

### 🔷 C# / .NET (NuGet)

#### Setup
1. Create `.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net6.0</TargetFramework>
    <PackageId>TollMesh.Client</PackageId>
    <Version>1.0.0</Version>
    <Authors>Prakhar Tripathi & Mayaplus</Authors>
    <Description>TollMeshStore .NET Client - Redis alternative with intelligent features</Description>
    <PackageProjectUrl>https://github.com/Prakhar998/toll-mesh-store</PackageProjectUrl>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
    <RepositoryUrl>https://github.com/Prakhar998/toll-mesh-store</RepositoryUrl>
    <RepositoryType>git</RepositoryType>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Grpc.Net.Client" Version="2.56.0" />
    <PackageReference Include="Google.Protobuf" Version="3.23.0" />
  </ItemGroup>
</Project>
```

#### Publish
```bash
# Create NuGet account
# https://www.nuget.org/users/account/LogOn

# Get API key from account settings

# Build
dotnet pack

# Publish
dotnet nuget push bin/Release/TollMesh.Client.1.0.0.nupkg --api-key YOUR_API_KEY --source https://api.nuget.org/v3/index.json

# Install
dotnet add package TollMesh.Client
```

### 💎 Ruby (RubyGems)

#### Setup
1. Create `tollmesh.gemspec`:
```ruby
Gem::Specification.new do |spec|
  spec.name          = "tollmesh"
  spec.version       = "1.0.0"
  spec.authors       = ["Prakhar Tripathi & Mayaplus"]
  spec.email         = ["prakhar@tollmesh.dev"]
  spec.summary       = "TollMeshStore Ruby Client"
  spec.description   = "TollMeshStore Ruby Client - Redis alternative with intelligent features"
  spec.homepage      = "https://github.com/Prakhar998/toll-mesh-store"
  spec.license       = "MIT"
  
  spec.files         = Dir.glob("lib/**/*")
  spec.require_paths = ["lib"]
  
  spec.add_dependency "grpc", "~> 1.56"
  spec.add_dependency "grpc-tools", "~> 1.56"
  
  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
end
```

#### Publish
```bash
# Create RubyGems account
# https://rubygems.org/sign_up

# Build
gem build tollmesh.gemspec

# Publish
gem push tollmesh-1.0.0.gem

# Install
gem install tollmesh
```

---

## 🚀 Publishing Workflow

### Step 1: Prepare Client Libraries
```bash
# Generate gRPC code for each language
protoc --go_out=. --go-grpc_out=. api/tollmesh.proto
protoc --python_out=. --grpc_python_out=. api/tollmesh.proto
protoc --js_out=import_style=commonjs,binary:. --grpc-web_out=import_style=commonjs,mode=grpcwebtext:. api/tollmesh.proto
# ... etc for other languages
```

### Step 2: Create Package Metadata
- Create `setup.py` for Python
- Create `package.json` for JavaScript
- Create `pom.xml` for Java
- Create `Cargo.toml` for Rust
- Create `.csproj` for C#
- Create `.gemspec` for Ruby

### Step 3: Publish to Package Managers
```bash
# Python
python -m build && twine upload dist/*

# JavaScript
npm publish --access public

# Java
mvn clean deploy -P release

# Rust
cargo publish

# C#
dotnet nuget push bin/Release/*.nupkg --api-key YOUR_KEY

# Ruby
gem push tollmesh-*.gem
```

### Step 4: Verify Installation
```bash
# Python
pip install tollmesh-client

# JavaScript
npm install @toll-mesh/client

# Java
# Add to pom.xml or build.gradle

# Rust
cargo add tollmesh

# C#
dotnet add package TollMesh.Client

# Ruby
gem install tollmesh
```

---

## 📋 Checklist for Each Language

- [ ] Create package metadata file
- [ ] Add dependencies
- [ ] Generate gRPC code
- [ ] Create account on package manager
- [ ] Get API key
- [ ] Build package
- [ ] Publish to package manager
- [ ] Verify installation works
- [ ] Update INSTALLATION.md with package manager link
- [ ] Create release notes

---

## 🔗 Package Manager Links

- **PyPI**: https://pypi.org/
- **npm**: https://www.npmjs.com/
- **Maven Central**: https://central.sonatype.com/
- **crates.io**: https://crates.io/
- **NuGet**: https://www.nuget.org/
- **RubyGems**: https://rubygems.org/

---

## 📝 Version Management

Use semantic versioning:
- **1.0.0** - Initial release
- **1.0.1** - Bug fixes
- **1.1.0** - New features
- **2.0.0** - Breaking changes

---

## 🎯 Next Steps

1. Generate gRPC code for each language
2. Create package metadata files
3. Create accounts on package managers
4. Publish to each package manager
5. Update INSTALLATION.md with links
6. Create release announcement

---

**Status**: Ready for publishing to all package managers