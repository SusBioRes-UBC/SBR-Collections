import os
import shutil
import argparse

def copy_pdfs_from_zotero_extract(zotero_extract_parent_folder=None, destiny_path=None):
    """
    Copy all PDF files from child folders in the Zotero extract parent folder to a destination folder.
    
    Args:
        zotero_extract_parent_folder (str): Path to the parent folder containing Zotero extract child folders
        destiny_path (str): Path to the destination folder where PDFs will be copied
    """
    # Ensure the destination directory exists
    if not os.path.exists(destiny_path):
        os.makedirs(destiny_path)
        
    # Get all child folders in the parent folder
    child_folders = [f for f in os.listdir(zotero_extract_parent_folder) 
                    if os.path.isdir(os.path.join(zotero_extract_parent_folder, f))]
    
    # Counter for copied files and skipped files
    copied_files_count = 0
    skipped_files_count = 0
    
    # Loop through each child folder
    for folder in child_folders:
        folder_path = os.path.join(zotero_extract_parent_folder, folder)
        
        # Find all PDF files in the current folder
        pdf_files = [f for f in os.listdir(folder_path) 
                    if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(folder_path, f))]
        
        # Copy each PDF file to the destination
        for pdf_file in pdf_files:
            source_path = os.path.join(folder_path, pdf_file)
            dest_path = os.path.join(destiny_path, pdf_file)
            
            # Check if the file already exists
            if os.path.exists(dest_path):
                # Check if it's the same file (by comparing size and modification time)
                src_stat = os.stat(source_path)
                dst_stat = os.stat(dest_path)
                
                if src_stat.st_size == dst_stat.st_size and src_stat.st_mtime == dst_stat.st_mtime:
                    print(f"Skipping {pdf_file} - identical file already exists in destination")
                    skipped_files_count += 1
                    continue
                
                # If not identical, handle duplicate filenames by adding a suffix
                base_name, extension = os.path.splitext(pdf_file)
                counter = 1
                while os.path.exists(dest_path):
                    new_filename = f"{base_name}_{counter}{extension}"
                    dest_path = os.path.join(destiny_path, new_filename)
                    counter += 1
            
            # Copy the file
            shutil.copy2(source_path, dest_path)
            copied_files_count += 1
    
    print(f"Successfully copied {copied_files_count} PDF files to {destiny_path}")
    if skipped_files_count > 0:
        print(f"Skipped {skipped_files_count} files that already existed in the destination")
    return copied_files_count
def main():
    # Create argument parser for command line usage
    parser = argparse.ArgumentParser(description='Copy PDF files from Zotero extract folders to a destination folder')
    parser.add_argument('--source', help='Path to the Zotero extract parent folder', required=True)
    parser.add_argument('--destination', help='Path to the destination folder where PDFs will be copied', required=True)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run the function with provided arguments
    copy_pdfs_from_zotero_extract(args.source, args.destination)

if __name__ == "__main__":
    main()
