# TollMesh Organization Setup Guide

This guide explains how to create a new GitHub organization and transfer the TollMeshStore repository to it.

## 📋 Step-by-Step Setup

### Step 1: Create GitHub Organization

1. Go to https://github.com/organizations/new
2. Fill in the organization details:
   - **Organization name**: `toll-mesh`
   - **Billing email**: Your email
   - **Organization display name**: `TollMesh - Distributed Cache & Agent Coordination`
   - **Organization website**: `https://toll-mesh.dev` (optional)
   - **Organization description**: `TollMeshStore: Redis alternative with distributed CRDTs, intelligent search, knowledge graphs, and agentic capabilities`

3. Click "Create organization"

### Step 2: Configure Organization Settings

1. Go to https://github.com/organizations/toll-mesh/settings
2. Configure:
   - **Profile**: Add organization logo and description
   - **Member privileges**: Set appropriate permissions
   - **Security**: Enable 2FA requirement
   - **Billing**: Set up billing if needed

### Step 3: Transfer Repository

#### Option A: Using GitHub Web Interface

1. Go to https://github.com/Prakhar998/toll-mesh-store/settings
2. Scroll to "Danger Zone"
3. Click "Transfer"
4. Enter new owner: `toll-mesh`
5. Confirm transfer

#### Option B: Using GitHub CLI

```bash
# Transfer repository to organization
gh repo transfer Prakhar998/toll-mesh-store --new-owner toll-mesh

# Verify transfer
gh repo view toll-mesh/toll-mesh-store
```

### Step 4: Update Repository Settings

After transfer, configure the repository:

1. Go to https://github.com/toll-mesh/toll-mesh-store/settings
2. Configure:
   - **Repository name**: `toll-mesh-store`
   - **Description**: `Redis alternative with distributed CRDTs, intelligent search, knowledge graphs, and agentic capabilities`
   - **Website**: `https://toll-mesh.dev`
   - **Topics**: `cache`, `distributed`, `grpc`, `redis`, `alternative`, `crdt`, `agents`
   - **Visibility**: Public
   - **Default branch**: `master`

### Step 5: Add Organization Members

1. Go to https://github.com/organizations/toll-mesh/people
2. Click "Invite member"
3. Add team members with appropriate roles:
   - **Owner**: Full access
   - **Maintainer**: Can manage code and settings
   - **Member**: Can contribute

### Step 6: Create Teams

1. Go to https://github.com/organizations/toll-mesh/teams
2. Create teams:
   - **Core Team**: Main developers
   - **Maintainers**: Repository maintainers
   - **Contributors**: Community contributors

### Step 7: Update Documentation

Update all references from `Prakhar998/toll-mesh-store` to `toll-mesh/toll-mesh-store`:

```bash
# Update README.md
sed -i 's|Prakhar998/toll-mesh-store|toll-mesh/toll-mesh-store|g' README.md

# Update INSTALLATION.md
sed -i 's|Prakhar998/toll-mesh-store|toll-mesh/toll-mesh-store|g' INSTALLATION.md

# Update all other files
find . -type f -name "*.md" -exec sed -i 's|Prakhar998/toll-mesh-store|toll-mesh/toll-mesh-store|g' {} \;
```

### Step 8: Update Git Remote

```bash
# Update local git remote
git remote set-url origin https://github.com/toll-mesh/toll-mesh-store.git

# Verify
git remote -v
```

### Step 9: Push Updates

```bash
# Push all changes
git push origin master

# Verify
git log --oneline -5
```

## 🔗 New Repository URLs

After transfer:
- **Repository**: https://github.com/toll-mesh/toll-mesh-store
- **Issues**: https://github.com/toll-mesh/toll-mesh-store/issues
- **Discussions**: https://github.com/toll-mesh/toll-mesh-store/discussions
- **Releases**: https://github.com/toll-mesh/toll-mesh-store/releases
- **Wiki**: https://github.com/toll-mesh/toll-mesh-store/wiki

## 📝 Update Package Managers

After organization transfer, update package manager metadata:

### Python (PyPI)
```python
url="https://github.com/toll-mesh/toll-mesh-store",
```

### JavaScript (npm)
```json
"repository": {
  "type": "git",
  "url": "https://github.com/toll-mesh/toll-mesh-store.git"
}
```

### Java (Maven)
```xml
<url>https://github.com/toll-mesh/toll-mesh-store</url>
<scm>
  <connection>scm:git:https://github.com/toll-mesh/toll-mesh-store.git</connection>
  <url>https://github.com/toll-mesh/toll-mesh-store</url>
</scm>
```

### Rust (crates.io)
```toml
repository = "https://github.com/toll-mesh/toll-mesh-store"
homepage = "https://github.com/toll-mesh/toll-mesh-store"
```

### C# / .NET (NuGet)
```xml
<RepositoryUrl>https://github.com/toll-mesh/toll-mesh-store</RepositoryUrl>
```

### Ruby (RubyGems)
```ruby
spec.homepage = "https://github.com/toll-mesh/toll-mesh-store"
```

## ✅ Verification Checklist

- [ ] Organization created at https://github.com/toll-mesh
- [ ] Repository transferred to toll-mesh organization
- [ ] Repository URL updated to https://github.com/toll-mesh/toll-mesh-store
- [ ] Local git remote updated
- [ ] All documentation updated with new URLs
- [ ] Package manager metadata updated
- [ ] Organization members added
- [ ] Teams created
- [ ] Repository settings configured
- [ ] First commit pushed to new organization

## 🎯 Next Steps

1. Create the organization at https://github.com/organizations/new
2. Transfer the repository using GitHub web interface or CLI
3. Update all documentation and package manager metadata
4. Add organization members and create teams
5. Publish to package managers with updated URLs

## 📞 Support

For help with GitHub organization setup:
- GitHub Docs: https://docs.github.com/en/organizations
- GitHub Support: https://support.github.com

---

**Status**: Ready for organization setup and repository transfer