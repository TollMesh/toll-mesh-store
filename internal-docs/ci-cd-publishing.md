# TollMeshCache: GitHub Actions + PyPI OIDC Publishing Setup

Complete guide for automating SDK publishing to all 7 package managers using GitHub Actions with trusted publishers (OIDC).

---

## 🔐 PyPI Setup with GitHub Actions (OIDC)

### Step 1: Create PyPI Trusted Publisher

1. Go to: https://pypi.org/manage/account/publishing/
2. Click **"Add a new pending publisher"**
3. Fill in:
   - **PyPI Project Name:** `tollmeshcache`
   - **Owner:** Your GitHub username/org (e.g., `toll-mesh`)
   - **Repository name:** `store`
   - **Workflow name:** `publish-python.yml`
   - **Environment name:** `pypi` (optional but recommended)

4. Click **"Add pending publisher"**

✅ Now PyPI trusts your GitHub Actions workflow to publish!

---

### Step 2: Create GitHub Actions Workflow

Create `.github/workflows/publish-python.yml`:

```yaml
name: Publish Python SDK to PyPI

on:
  push:
    tags:
      - 'python-v*'  # e.g., python-v1.0.0
    paths:
      - 'sdks/python/**'

permissions:
  contents: read
  id-token: write  # Required for OIDC

jobs:
  publish:
    runs-on: ubuntu-latest
    environment: pypi
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install build tools
        run: |
          pip install --upgrade pip
          pip install build twine
      
      - name: Build distribution
        run: |
          cd sdks/python/
          python -m build
      
      - name: Run tests before publish
        run: |
          cd sdks/python/
          pip install pytest pytest-cov
          pytest tests/ --cov=tollmeshcache
      
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          packages-dir: sdks/python/dist/
```

### Step 3: Create GitHub Environment (Optional but Recommended)

1. Go to: Repository Settings → Environments
2. Click **"New environment"**
3. Name it: `pypi`
4. Click **"Create environment"**
5. (Optional) Add deployment protection rules

---

## 🔧 Node.js Publishing (npm)

### Step 1: Setup npm Token

1. Go to: https://www.npmjs.com/settings/your-username/tokens
2. Create "Automation" token
3. In GitHub: Settings → Secrets → New repository secret
4. Name: `NPM_TOKEN`
5. Value: Your npm automation token

### Step 2: Create Workflow

Create `.github/workflows/publish-node.yml`:

```yaml
name: Publish Node.js SDK to npm

on:
  push:
    tags:
      - 'node-v*'  # e.g., node-v1.0.0
    paths:
      - 'sdks/node/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '18'
          registry-url: 'https://registry.npmjs.org'
      
      - name: Install dependencies
        run: |
          cd sdks/node/
          npm ci
      
      - name: Run tests
        run: |
          cd sdks/node/
          npm run test:coverage
      
      - name: Build distribution
        run: |
          cd sdks/node/
          npm run build
      
      - name: Publish to npm
        run: |
          cd sdks/node/
          npm publish --access public
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

---

## 🦀 Rust Publishing (crates.io)

### Step 1: Setup Crates.io Token

1. Go to: https://crates.io/me
2. Create API token
3. In GitHub: Settings → Secrets → New repository secret
4. Name: `CARGO_REGISTRY_TOKEN`
5. Value: Your crates.io API token

### Step 2: Create Workflow

Create `.github/workflows/publish-rust.yml`:

```yaml
name: Publish Rust SDK to crates.io

on:
  push:
    tags:
      - 'rust-v*'  # e.g., rust-v1.0.0
    paths:
      - 'sdks/rust/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust
        uses: dtolnay/rust-toolchain@stable
      
      - name: Run tests
        run: |
          cd sdks/rust/
          cargo test --release
      
      - name: Run benchmarks
        run: |
          cd sdks/rust/
          cargo bench --no-run
      
      - name: Publish to crates.io
        run: |
          cd sdks/rust/
          cargo publish --token ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

---

## 💎 Ruby Publishing (RubyGems)

### Step 1: Setup RubyGems API Key

1. Go to: https://rubygems.org/settings/edit
2. Create API key (or use existing)
3. In GitHub: Settings → Secrets
4. Name: `RUBYGEMS_API_KEY`
5. Value: Your RubyGems API key

### Step 2: Create Workflow

Create `.github/workflows/publish-ruby.yml`:

```yaml
name: Publish Ruby SDK to RubyGems

on:
  push:
    tags:
      - 'ruby-v*'  # e.g., ruby-v1.0.0
    paths:
      - 'sdks/ruby/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Ruby
        uses: ruby/setup-ruby@v1
        with:
          ruby-version: '3.2'
          bundler-cache: true
          working-directory: sdks/ruby
      
      - name: Run tests
        run: |
          cd sdks/ruby/
          bundle exec rspec
      
      - name: Build gem
        run: |
          cd sdks/ruby/
          gem build tollmeshcache.gemspec
      
      - name: Publish to RubyGems
        run: |
          cd sdks/ruby/
          mkdir -p ~/.gem
          cat > ~/.gem/credentials << EOF
          ---
          :rubygems_api_key: ${{ secrets.RUBYGEMS_API_KEY }}
          EOF
          chmod 600 ~/.gem/credentials
          gem push tollmeshcache-*.gem
```

---

## #️⃣ C# Publishing (NuGet)

### Step 1: Setup NuGet API Key

1. Go to: https://www.nuget.org/account/apikeys
2. Create new API key
3. In GitHub: Settings → Secrets
4. Name: `NUGET_API_KEY`
5. Value: Your NuGet API key

### Step 2: Create Workflow

Create `.github/workflows/publish-csharp.yml`:

```yaml
name: Publish C# SDK to NuGet

on:
  push:
    tags:
      - 'csharp-v*'  # e.g., csharp-v1.0.0
    paths:
      - 'sdks/csharp/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0'
      
      - name: Restore dependencies
        run: |
          cd sdks/csharp/
          dotnet restore
      
      - name: Run tests
        run: |
          cd sdks/csharp/
          dotnet test --configuration Release
      
      - name: Build package
        run: |
          cd sdks/csharp/
          dotnet pack --configuration Release
      
      - name: Publish to NuGet
        run: |
          cd sdks/csharp/
          dotnet nuget push bin/Release/*.nupkg \
            --source https://api.nuget.org/v3/index.json \
            --api-key ${{ secrets.NUGET_API_KEY }}
```

---

## ☕ Java Publishing (Maven Central)

### Step 1: Setup GPG Keys and Sonatype Credentials

```bash
# Generate GPG key (if not already done)
gpg --gen-key

# Export public key
gpg --armor --export YOUR_KEY_ID > public.gpg

# Export secret key
gpg --armor --export-secret-keys YOUR_KEY_ID > secret.gpg
```

2. In GitHub: Settings → Secrets
   - Name: `MAVEN_GPG_PASSPHRASE`
   - Name: `MAVEN_GPG_PRIVATE_KEY` (content of secret.gpg)
   - Name: `SONATYPE_USERNAME`
   - Name: `SONATYPE_PASSWORD`

### Step 2: Create Workflow

Create `.github/workflows/publish-java.yml`:

```yaml
name: Publish Java SDK to Maven Central

on:
  push:
    tags:
      - 'java-v*'  # e.g., java-v1.0.0
    paths:
      - 'sdks/java/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Java
        uses: actions/setup-java@v4
        with:
          java-version: '11'
          distribution: 'temurin'
          cache: maven
      
      - name: Import GPG key
        run: |
          mkdir -p ~/.gnupg
          echo "${{ secrets.MAVEN_GPG_PRIVATE_KEY }}" | base64 -d | gpg --import --batch
      
      - name: Run tests
        run: |
          cd sdks/java/
          mvn test
      
      - name: Deploy to Maven Central
        run: |
          cd sdks/java/
          mvn clean deploy -P release \
            -Dgpg.passphrase="${{ secrets.MAVEN_GPG_PASSPHRASE }}" \
            -DsignatureAlgorithm=SHA256withRSA
        env:
          SONATYPE_USERNAME: ${{ secrets.SONATYPE_USERNAME }}
          SONATYPE_PASSWORD: ${{ secrets.SONATYPE_PASSWORD }}
```

---

## 🐘 PHP Publishing (Packagist - Auto from GitHub)

### Setup (One-time)

1. Go to: https://packagist.org/packages/submit
2. Enter repository URL: `https://github.com/toll-mesh/store`
3. Click "Check"
4. Packagist auto-indexes on git tags!

### Step: Create Workflow (Optional - for notifications)

Create `.github/workflows/publish-php.yml`:

```yaml
name: Publish PHP SDK to Packagist

on:
  push:
    tags:
      - 'php-v*'  # e.g., php-v1.0.0
    paths:
      - 'sdks/php/**'

jobs:
  publish:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup PHP
        uses: shivammathur/setup-php@v2
        with:
          php-version: '8.2'
          tools: composer:latest
      
      - name: Install dependencies
        run: |
          cd sdks/php/
          composer install --no-interaction
      
      - name: Run tests
        run: |
          cd sdks/php/
          composer test
      
      - name: Notify Packagist (optional)
        run: |
          curl -XPOST https://packagist.org/api/update-package?username=YOUR_USERNAME&apiToken=${{ secrets.PACKAGIST_TOKEN }} \
            -d '{"repository":{"url":"https://github.com/toll-mesh/store"}}'
```

---

## 🎯 Unified Publishing Workflow

Create `.github/workflows/publish-all.yml` to publish all SDKs on release:

```yaml
name: Publish All SDKs

on:
  release:
    types: [published]

jobs:
  extract-versions:
    runs-on: ubuntu-latest
    outputs:
      python_version: ${{ steps.versions.outputs.python }}
      node_version: ${{ steps.versions.outputs.node }}
      java_version: ${{ steps.versions.outputs.java }}
      rust_version: ${{ steps.versions.outputs.rust }}
      ruby_version: ${{ steps.versions.outputs.ruby }}
      csharp_version: ${{ steps.versions.outputs.csharp }}
      php_version: ${{ steps.versions.outputs.php }}
    
    steps:
      - name: Extract versions from tag
        id: versions
        run: |
          TAG=${{ github.event.release.tag_name }}
          # Format: v1.0.0-python,v1.0.0-node,etc.
          # Or individual tags: python-v1.0.0, node-v1.0.0, etc.
          echo "python=${TAG}" >> $GITHUB_OUTPUT
          echo "node=${TAG}" >> $GITHUB_OUTPUT

  # Include all the individual publish jobs from above
  publish-python:
    needs: extract-versions
    uses: ./.github/workflows/publish-python.yml
    secrets: inherit
  
  publish-node:
    needs: extract-versions
    uses: ./.github/workflows/publish-node.yml
    secrets: inherit
  
  publish-rust:
    needs: extract-versions
    uses: ./.github/workflows/publish-rust.yml
    secrets: inherit
  
  publish-ruby:
    needs: extract-versions
    uses: ./.github/workflows/publish-ruby.yml
    secrets: inherit
  
  publish-csharp:
    needs: extract-versions
    uses: ./.github/workflows/publish-csharp.yml
    secrets: inherit
  
  publish-java:
    needs: extract-versions
    uses: ./.github/workflows/publish-java.yml
    secrets: inherit
  
  publish-php:
    needs: extract-versions
    uses: ./.github/workflows/publish-php.yml
    secrets: inherit
  
  create-announcement:
    needs: [publish-python, publish-node, publish-rust, publish-ruby, publish-csharp, publish-java, publish-php]
    runs-on: ubuntu-latest
    
    steps:
      - name: Create GitHub Release
        run: |
          echo "✅ All SDKs published successfully!"
          echo "🎉 Version: ${{ github.event.release.tag_name }}"
```

---

## 📋 Publishing Checklist

### Before Release
- [ ] All tests passing (run locally)
- [ ] Update version numbers in all SDKs
- [ ] Update CHANGELOG.md
- [ ] Update README with new features
- [ ] Review documentation

### Create Release
```bash
# Create git tag with version
git tag -a v1.0.0 -m "Release 1.0.0: Job Queues, Sorted Sets, Streams"

# Push tag to trigger workflows
git push --tags
```

### GitHub Release
1. Go to: Releases → Create new release
2. Select tag: `v1.0.0`
3. Title: "TollMeshCache v1.0.0"
4. Description: Feature summary and links
5. Click "Publish release"

### Verification
```bash
# Python
pip install tollmeshcache==1.0.0

# Node.js
npm install tollmeshcache@1.0.0

# Rust
cargo add tollmeshcache@1.0.0

# Ruby
gem install tollmeshcache -v 1.0.0

# C#
nuget install TollMeshCache -Version 1.0.0

# PHP
composer require tollmesh/cache:^1.0

# Java
# Search Maven Central
```

---

## 🚨 Troubleshooting

### PyPI OIDC Token Issues
```bash
# Check pending publishers
curl -H "Authorization: token YOUR_GITHUB_TOKEN" \
  https://pypi.org/api/v1/oidc/pending-publishers

# Verify workflow has correct permissions
# In workflow: permissions: { id-token: write }
```

### npm Authorization Failed
- Verify token is "Automation" type
- Check token hasn't expired
- Ensure registry-url is set correctly

### Maven Central Staging
- Check Nexus UI at oss.sonatype.org
- Drop and retry if stuck
- GPG key must be uploaded to keyserver

### Rust Publishing Conflicts
- If version exists, increment version number
- Yanked versions can be published as new

### Ruby GemSpec Issues
```bash
# Validate locally first
cd sdks/ruby/
gem check -a

# Build and test
gem build tollmeshcache.gemspec
gem install tollmeshcache-*.gem --local
```

---

## 🔄 Workflow Status

Add status badge to README:

```markdown
![Python](https://github.com/toll-mesh/store/workflows/Publish%20Python%20SDK%20to%20PyPI/badge.svg)
![Node.js](https://github.com/toll-mesh/store/workflows/Publish%20Node.js%20SDK%20to%20npm/badge.svg)
![Rust](https://github.com/toll-mesh/store/workflows/Publish%20Rust%20SDK%20to%20crates.io/badge.svg)
```

---

## 📊 Monitoring Publishes

Set up notifications:

1. **GitHub:** Settings → Notifications → Workflow runs
2. **Email:** GitHub sends on success/failure
3. **Slack:** Use `slackapi/slack-github-action`
4. **Discord:** Use `sarisia/actions-status-discord`

---

## 🎓 Best Practices

✅ **Do:**
- Test on staging package manager first
- Use matrix strategy for parallel publishes
- Pin action versions for reproducibility
- Sign commits and tags with GPG
- Run full test suite before publishing
- Monitor for publishing errors immediately
- Keep secrets rotation schedule

❌ **Don't:**
- Publish on Fridays (hard to debug over weekend)
- Publish multiple versions rapidly
- Force-delete published versions
- Store API keys in code or logs
- Skip testing before publish
- Publish without changelog

---

**Next:** Push these workflows to GitHub, create your first release tag, and watch your SDKs publish automatically! 🚀
