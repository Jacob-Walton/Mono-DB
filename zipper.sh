#!/bin/bash

EXCLUDES=(node_modules target .vscode .idea .git)

# Build the find exclude pattern
EXCL_ARGS=()
for excl in "${EXCLUDES[@]}"; do
  EXCL_ARGS+=(-path "*/$excl" -prune -o)
done

# Run fzf to pick files
SELECTED=$(find . "${EXCL_ARGS[@]}" -type f -print | fzf -m --preview 'bat --style=numbers --color=always {}' --prompt "Select files to ZIP: " --height=40%)

if [[ -z "$SELECTED" ]]; then
  echo "No files selected."
  exit 1
fi

read -rp "Enter output zip file name (e.g. project.zip): " ZIPNAME
ZIPNAME=${ZIPNAME:-archive.zip}

echo "$SELECTED" | zip "$ZIPNAME" -@

