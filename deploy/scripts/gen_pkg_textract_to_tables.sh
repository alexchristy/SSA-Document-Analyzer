#!/bin/bash

# Function to display the banner warning
display_warning() {
    # Clear the terminal
    clear

    # Display the banner
    echo "================================================================================"
    echo "WARNING: Textract-to-Tables Container Deployment Required"
    echo "================================================================================"
    echo
    echo "Textract-to-Tables requires you to build a container image using Docker."
    echo "This is necessary for the proper functioning of the application."
    echo
    echo "For detailed instructions on how to deploy the Textract-to-Tables container,"
    echo "please visit the following link:"
    echo "https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Deploying-Textract%E2%80%90to%E2%80%90Tables-Container"
    echo
    echo "================================================================================"
}

# Call the function to display the warning
display_warning
