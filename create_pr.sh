#!/bin/bash
# Script to commit, push, and create PR for S3-to-GCS migration changes

set -e  # Exit on error

REPO="eva-tech/vlm-eden-dataset-etl"
BRANCH="feature/cleanup-intelligence-etl-and-refactor-s3"
BASE_BRANCH="main"
REMOTE="new-origin"

echo "🚀 Starting PR creation process..."

# Step 1: Check git status
echo "📋 Checking git status..."
git status

# Step 2: Stage all changes
echo "📦 Staging all changes..."
git add -A

# Step 3: Commit with descriptive message
echo "💾 Committing changes..."
git commit -m "Clean up intelligence-etl code and refactor S3 operations to S3-to-GCS migration

- Remove intelligence-etl specific code (migrations, queries, sync modules)
- Replace s3_service.py with gcs_service.py for S3-to-GCS copying
- Update environment variables: S3_ORIGIN_BUCKET_NAME and GCS_BUCKET_NAME
- Remove boto3 dependency, use AWS CLI and gsutil for cross-cloud copying
- Update README.md to reflect S3 origin to GCS destination architecture
- Add cron_tasks.py module for Celery Beat scheduled tasks
- Update requirements.txt: remove boto3, organize packages by category
- Update .env.dist with S3 and GCS configuration
- Remove CLEANUP_PLAN.md (temporary file)

Addresses PR comments from @xleninx:
- Remove CLEANUP_PLAN.md
- Move to cloud storage (S3 to GCS migration)
- Keep cron tasks functionality"

# Step 4: Push to remote
echo "📤 Pushing branch to remote..."
git push -u $REMOTE $BRANCH

# Step 5: Create PR using GitHub CLI
echo "🔨 Creating pull request..."

PR_BODY="## Summary
This PR refactors the ETL system to migrate from S3-only operations to S3-to-GCS cross-cloud copying, and removes all intelligence-etl specific code.

## Changes

### Code Cleanup
- ✅ Removed intelligence-etl specific code:
  - Deleted CLEANUP_PLAN.md (temporary file)
  - Removed unused migrations, queries, and sync modules
- ✅ Replaced s3_service.py with gcs_service.py:
  - New service copies files from S3 origin bucket to GCS destination bucket
  - Uses gsutil for efficient cross-cloud copying
  - Uses AWS CLI for S3 operations

### Configuration Updates
- ✅ Updated environment variables:
  - S3_ORIGIN_BUCKET_NAME (origin bucket)
  - GCS_BUCKET_NAME (destination bucket)
  - AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME)
  - GCS credentials (GCS_CREDENTIALS_PATH)
- ✅ Updated .env.dist with S3 and GCS configuration sections

### Dependencies
- ✅ Removed boto3 dependency (no longer needed)
- ✅ Organized requirements.txt by category with comments

### Documentation
- ✅ Updated README.md:
  - Reflects S3 origin to GCS destination architecture
  - Added prerequisites for AWS CLI and gsutil
  - Updated troubleshooting for cross-cloud copying
  - Added security considerations for both cloud providers

### Celery Configuration
- ✅ Added cron_tasks.py module for Celery Beat scheduled tasks
- ✅ Updated celery_app.py to include cron_tasks in CELERY_IMPORTS

## Testing Checklist
- [ ] Verify AWS CLI is installed and configured
- [ ] Verify gsutil is installed and configured
- [ ] Test cross-cloud copying: \`gsutil cp s3://bucket/file gs://bucket/file\`
- [ ] Verify environment variables are set correctly
- [ ] Test Celery tasks execution

## Related Issues
Addresses PR comments from @xleninx:
- Remove CLEANUP_PLAN.md
- Move to cloud storage (S3 to GCS migration)
- Keep cron tasks functionality"

gh pr create \
  --repo $REPO \
  --base $BASE_BRANCH \
  --head $BRANCH \
  --title "Clean up intelligence-etl code and refactor S3 operations to S3-to-GCS migration" \
  --body "$PR_BODY"

echo "✅ PR created successfully!"
echo "🔗 View PR at: https://github.com/$REPO/pull/..."

