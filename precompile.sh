#!/bin/bash

# Deleting the master_matadata directory
if [ -d "master_metadata" ]; then
  rm -rf master_metadata
  echo "Deleted master_metadata"
else
  echo "master_metadata/ not found"
fi

if [ -d "client_files" ]; then
  rm -rf client_files
  echo "Deleted client_files"
else
  echo "client_files/ not found"
fi

# Deleting directories that start with chunk_storage
for dir in chunk_storage*/; do
  if [ -d "$dir" ]; then
    rm -rf "$dir"
    echo "Deleted $dir"
  fi
done
