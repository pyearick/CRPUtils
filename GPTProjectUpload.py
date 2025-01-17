import os
import tkinter as tk
from tkinter import filedialog

def select_directory():
    """
    Opens a tkinter dialog to select a directory.
    Returns the selected directory path or None if cancelled.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    
    directory_path = filedialog.askdirectory(
        title="Select Project Directory",
        initialdir="."  # Start in current directory
    )
    
    return directory_path if directory_path else None

def create_claude_project_text(directory_path):
    """
    Creates a Claude-formatted document containing all Python files in the specified directory.
    """
    # Start the documents wrapper
    output = "<documents>"
    
    # Find all .py files in directory
    python_files = []
    for file in os.listdir(directory_path):
        if file.endswith(".py"):
            python_files.append(file)
    
    # Sort files alphabetically for consistency
    python_files.sort()
    
    # Process each file
    for index, filename in enumerate(python_files, 1):
        file_path = os.path.join(directory_path, filename)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Create the document entry
            output += f"""<document index="{index}">
<source>{filename}</source>
<document_content>{content}</document_content>
</document>"""
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    # Close the documents wrapper
    output += "</documents>"
    
    # Write to output file
    output_path = os.path.join(directory_path, "claude_project.txt")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(f"\nProcessed {len(python_files)} Python files:")
    for file in python_files:
        print(f"- {file}")
    print(f"\nOutput saved to: {output_path}")

if __name__ == "__main__":
    print("Please select your project directory...")
    directory = select_directory()
    
    if directory:
        print(f"Selected directory: {directory}")
        create_claude_project_text(directory)
    else:
        print("No directory selected. Exiting.")