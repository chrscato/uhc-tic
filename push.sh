#!/bin/bash

# Git workflow automation script
# This script adds all changes, prompts for a commit message, and pushes to origin master

echo "Adding all changes to git..."
git add .

# Check if there are any changes to commit
if git diff --cached --quiet; then
    echo "No changes to commit."
    exit 0
fi

# Show status of what will be committed
echo -e "\nChanges to be committed:"
git status --short

# Prompt for commit message
echo -e "\nEnter your commit message:"
read -p "> " commit_message

# Check if commit message is not empty
if [ -z "$commit_message" ]; then
    echo "Commit message cannot be empty. Aborting."
    exit 1
fi

# Commit with the provided message
echo -e "\nCommitting changes..."
git commit -m "$commit_message"

# Check if commit was successful
if [ $? -ne 0 ]; then
    echo "Commit failed. Aborting push."
    exit 1
fi

# Push to origin master
echo -e "\nPushing to origin master..."
git push origin master

# Check if push was successful
if [ $? -eq 0 ]; then
    echo -e "\nSuccessfully pushed to origin master!"
else
    echo -e "\nPush failed. Please check your git configuration and network connection."
    exit 1
fi