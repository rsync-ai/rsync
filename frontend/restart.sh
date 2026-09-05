#!/bin/bash

# Frontend Restart Script - No Cache
# This script clears all caches and restarts the development server

echo "🧹 Cleaning Next.js cache..."
rm -rf .next

echo "🧹 Cleaning node_modules cache..."
rm -rf node_modules/.cache

echo "✅ Cache cleared!"
echo ""
echo "🚀 Starting development server..."
echo "   URL: http://localhost:3000"
echo ""

npm run dev

