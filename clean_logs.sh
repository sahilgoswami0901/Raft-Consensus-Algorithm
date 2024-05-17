#!/bin/bash

# Function to recursively delete the logs folder
delete_logs() {
    local dir="$1"
    if [ -d "$dir" ]; then
        for file in "$dir"/*; do
            if [ -d "$file" ]; then
                delete_logs "$file"
            else
                rm "$file"
            fi
        done
        rm -r "$dir"
    fi
}

# Call the function to delete the logs folder
delete_logs "logs"