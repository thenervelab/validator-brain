#!/bin/bash

# Create migrations ConfigMap from files
kubectl create configmap migrations-config \
  --from-file=../db/migrations/ \
  --dry-run=client \
  -o yaml > migrations-configmap.yaml

echo "Migrations ConfigMap created in migrations-configmap.yaml" 