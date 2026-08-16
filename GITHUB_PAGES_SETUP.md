# GitHub Pages Setup Guide

To enable the interactive website for TollMeshStore, follow these steps:

## Enable GitHub Pages

1. Go to: https://github.com/TollMesh/toll-mesh-store/settings/pages

2. Under "Build and deployment":
   - **Source**: Select "Deploy from a branch"
   - **Branch**: Select "master" (or main)
   - **Folder**: Select "/docs"
   - Click "Save"

3. Wait 1-2 minutes for GitHub to build and deploy

4. Your site will be available at: https://tollmesh.github.io/toll-mesh-store/

## Verify Setup

After enabling GitHub Pages:
- Check the "Deployments" tab in the repository
- Look for a successful deployment
- Visit https://tollmesh.github.io/toll-mesh-store/ to see the website

## Website Features

The interactive website includes:
- ✅ Glass morphism design
- ✅ Smooth animations
- ✅ Feature showcase
- ✅ Redis comparison table
- ✅ Multi-language support display
- ✅ Project statistics
- ✅ Responsive design
- ✅ Smooth scroll navigation

## Troubleshooting

If you see a 404 error:
1. Verify the `/docs` folder exists in the repository
2. Verify `docs/index.html` exists
3. Check that GitHub Pages is enabled in settings
4. Wait a few minutes for deployment to complete
5. Clear your browser cache and try again

## Custom Domain (Optional)

To use a custom domain:
1. Go to Settings > Pages
2. Under "Custom domain", enter your domain
3. Add DNS records as instructed by GitHub
4. Verify the domain

---

**Status**: Ready for GitHub Pages deployment