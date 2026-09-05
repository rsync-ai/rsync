#!/bin/bash
# Initialize MinIO buckets for RSYNC AI data pipeline
# This script runs on startup to ensure all required buckets exist

set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

# Required buckets for the system
BUCKETS=(
    "pipeline-data"           # Raw pipeline data
    "intermediate-storage"    # Temporary transform data
    "agent-artifacts"         # Agent-generated files
    "audit-logs"             # Compliance audit trails
    "backups"                # Pipeline backups
)

echo "🗄️  Initializing MinIO Buckets..."
echo "================================"
echo ""

# Install mc (MinIO Client) if not available
if ! command -v mc &> /dev/null; then
    echo "📦 Installing MinIO Client..."
    curl -sL https://dl.min.io/client/mc/release/darwin-amd64/mc \
        -o /tmp/mc
    chmod +x /tmp/mc
    MC_CMD="/tmp/mc"
else
    MC_CMD="mc"
fi

# Configure MinIO alias
echo "🔗 Configuring MinIO connection..."
$MC_CMD alias set rsync-minio http://$MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY --api S3v4

# Create buckets
echo ""
echo "📂 Creating buckets..."
for bucket in "${BUCKETS[@]}"; do
    if $MC_CMD ls rsync-minio/$bucket &>/dev/null; then
        echo "  ✓ $bucket (already exists)"
    else
        $MC_CMD mb rsync-minio/$bucket
        echo "  ✓ $bucket (created)"
    fi
done

# Set bucket policies (public read for agent-artifacts, private for others)
echo ""
echo "🔐 Configuring bucket policies..."

# Public read policy for agent-artifacts (for sharing generated tools)
cat > /tmp/public-read-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::agent-artifacts/*"]
        }
    ]
}
EOF

$MC_CMD policy set-json /tmp/public-read-policy.json rsync-minio/agent-artifacts
echo "  ✓ agent-artifacts (public read)"

# Private policy for sensitive buckets
for bucket in "pipeline-data" "audit-logs" "backups"; do
    $MC_CMD policy set private rsync-minio/$bucket
    echo "  ✓ $bucket (private)"
done

# Download policy for intermediate storage (allows authenticated downloads)
$MC_CMD policy set download rsync-minio/intermediate-storage
echo "  ✓ intermediate-storage (download)"

# Set lifecycle policies
echo ""
echo "♻️  Configuring lifecycle policies..."

# Intermediate storage: Delete after 7 days
cat > /tmp/lifecycle-intermediate.json <<EOF
{
    "Rules": [
        {
            "ID": "ExpireIntermediateData",
            "Status": "Enabled",
            "Expiration": {
                "Days": 7
            }
        }
    ]
}
EOF

$MC_CMD ilm import rsync-minio/intermediate-storage < /tmp/lifecycle-intermediate.json
echo "  ✓ intermediate-storage (7-day expiration)"

# Audit logs: Transition to glacier after 90 days
cat > /tmp/lifecycle-audit.json <<EOF
{
    "Rules": [
        {
            "ID": "TransitionAuditLogs",
            "Status": "Enabled",
            "Transitions": [
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}
EOF

$MC_CMD ilm import rsync-minio/audit-logs < /tmp/lifecycle-audit.json
echo "  ✓ audit-logs (90-day glacier transition)"

# Enable versioning for critical buckets
echo ""
echo "📝 Enabling versioning..."
for bucket in "pipeline-data" "backups"; do
    $MC_CMD version enable rsync-minio/$bucket
    echo "  ✓ $bucket (versioning enabled)"
done

# Create directory structure in buckets
echo ""
echo "📁 Creating directory structure..."

# pipeline-data structure
$MC_CMD cp /dev/null rsync-minio/pipeline-data/raw/.keep
$MC_CMD cp /dev/null rsync-minio/pipeline-data/processed/.keep
$MC_CMD cp /dev/null rsync-minio/pipeline-data/failed/.keep
echo "  ✓ pipeline-data structure created"

# agent-artifacts structure
$MC_CMD cp /dev/null rsync-minio/agent-artifacts/tools/.keep
$MC_CMD cp /dev/null rsync-minio/agent-artifacts/reports/.keep
$MC_CMD cp /dev/null rsync-minio/agent-artifacts/templates/.keep
echo "  ✓ agent-artifacts structure created"

# Display bucket statistics
echo ""
echo "📊 Bucket Statistics:"
echo "================================"
for bucket in "${BUCKETS[@]}"; do
    size=$($MC_CMD du rsync-minio/$bucket 2>/dev/null | awk '{print $1}' || echo "0")
    echo "  $bucket: $size"
done

echo ""
echo "✅ MinIO initialization complete!"
echo ""
echo "📍 Endpoint: http://$MINIO_ENDPOINT"
echo "🔑 Access Key: $MINIO_ACCESS_KEY"
echo "📦 Buckets: ${#BUCKETS[@]} created"
echo ""
echo "To access MinIO console:"
echo "  http://$MINIO_ENDPOINT/minio/login"

