# PL_eBayImageDuplicateRemover.py

import os
import hashlib
import logging
from collections import defaultdict
from PIL import Image
import imagehash
import shutil
from datetime import datetime
import pandas as pd

# Setup logging
log_file = "C:/Logs/PL_eBayImageDuplicateRemover.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Configuration
EBAY_BASE_DIR = "C:/ProductImages/eBay"
BACKUP_DIR = "C:/ProductImages/eBay_Duplicates_Backup"
REPORT_DIR = "C:/Logs"


def calculate_file_hash(file_path):
    """Calculate MD5 hash of file content for exact duplicate detection"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Error calculating hash for {file_path}: {e}")
        return None


def calculate_image_hash(file_path, hash_size=8):
    """Calculate perceptual hash for similar image detection"""
    try:
        with Image.open(file_path) as img:
            # Convert to RGB if necessary
            if img.mode != 'RGB':
                img = img.convert('RGB')

            # Calculate different types of perceptual hashes
            ahash = imagehash.average_hash(img, hash_size=hash_size)
            dhash = imagehash.dhash(img, hash_size=hash_size)
            phash = imagehash.phash(img, hash_size=hash_size)

            return {
                'ahash': str(ahash),
                'dhash': str(dhash),
                'phash': str(phash)
            }
    except Exception as e:
        logging.error(f"Error calculating image hash for {file_path}: {e}")
        return None


def get_image_info(file_path):
    """Get basic image information"""
    try:
        with Image.open(file_path) as img:
            return {
                'size': img.size,
                'mode': img.mode,
                'format': img.format,
                'file_size': os.path.getsize(file_path)
            }
    except Exception as e:
        logging.error(f"Error getting image info for {file_path}: {e}")
        return None


def find_all_images():
    """Find all eBay images organized by product"""
    print("Scanning for eBay images...")
    all_images = []

    if not os.path.exists(EBAY_BASE_DIR):
        print(f"eBay directory not found: {EBAY_BASE_DIR}")
        return []

    # Walk through all product directories
    for product_dir in os.listdir(EBAY_BASE_DIR):
        product_path = os.path.join(EBAY_BASE_DIR, product_dir)

        if not os.path.isdir(product_path):
            continue

        print(f"Scanning product: {product_dir}")

        # Find all image files in this product directory
        for file_name in os.listdir(product_path):
            if file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                file_path = os.path.join(product_path, file_name)

                # Extract metadata from filename if possible
                # Expected format: PRODUCT_ITEMID_SEQUENCE_DATE.jpg
                parts = file_name.split('_')

                image_info = {
                    'file_path': file_path,
                    'file_name': file_name,
                    'product': product_dir,
                    'full_path': file_path,
                    'item_id': parts[1] if len(parts) >= 2 else 'unknown',
                    'sequence': parts[2] if len(parts) >= 3 else 'unknown',
                    'date_part': parts[3].split('.')[0] if len(parts) >= 4 else 'unknown',
                    'file_size': os.path.getsize(file_path),
                    'modified_time': os.path.getmtime(file_path)
                }

                all_images.append(image_info)

    print(f"Found {len(all_images)} total images")
    return all_images


def find_exact_duplicates(images):
    """Find images with identical file content"""
    print("\nFinding exact duplicates...")

    hash_groups = defaultdict(list)
    processed = 0

    for img_info in images:
        file_hash = calculate_file_hash(img_info['file_path'])
        if file_hash:
            hash_groups[file_hash].append(img_info)

        processed += 1
        if processed % 100 == 0:
            print(f"Processed {processed}/{len(images)} images for exact duplicates")

    # Find groups with more than one image (duplicates)
    duplicates = {hash_val: imgs for hash_val, imgs in hash_groups.items() if len(imgs) > 1}

    total_duplicates = sum(len(imgs) - 1 for imgs in duplicates.values())  # -1 because we keep one
    print(f"Found {len(duplicates)} groups of exact duplicates affecting {total_duplicates} files")

    return duplicates


def find_similar_images(images, similarity_threshold=5):
    """Find visually similar images using perceptual hashing"""
    print(f"\nFinding similar images (threshold: {similarity_threshold})...")

    image_hashes = []
    processed = 0

    # Calculate hashes for all images
    for img_info in images:
        hashes = calculate_image_hash(img_info['file_path'])
        if hashes:
            img_info['hashes'] = hashes
            image_hashes.append(img_info)

        processed += 1
        if processed % 100 == 0:
            print(f"Processed {processed}/{len(images)} images for similarity")

    # Find similar images
    similar_groups = []
    processed_indices = set()

    for i, img1 in enumerate(image_hashes):
        if i in processed_indices:
            continue

        similar_group = [img1]

        for j, img2 in enumerate(image_hashes[i + 1:], i + 1):
            if j in processed_indices:
                continue

            # Calculate hash differences
            ahash_diff = imagehash.hex_to_hash(img1['hashes']['ahash']) - imagehash.hex_to_hash(img2['hashes']['ahash'])
            dhash_diff = imagehash.hex_to_hash(img1['hashes']['dhash']) - imagehash.hex_to_hash(img2['hashes']['dhash'])
            phash_diff = imagehash.hex_to_hash(img1['hashes']['phash']) - imagehash.hex_to_hash(img2['hashes']['phash'])

            # If any hash difference is below threshold, consider similar
            if min(ahash_diff, dhash_diff, phash_diff) <= similarity_threshold:
                similar_group.append(img2)
                processed_indices.add(j)

        if len(similar_group) > 1:
            similar_groups.append(similar_group)
            processed_indices.update(range(i, i + len(similar_group)))

    total_similar = sum(len(group) - 1 for group in similar_groups)  # -1 because we keep one
    print(f"Found {len(similar_groups)} groups of similar images affecting {total_similar} files")

    return similar_groups


def choose_best_image(image_group):
    """Choose the best image to keep from a group of duplicates/similar images"""

    # Sort by multiple criteria:
    # 1. Largest file size (usually better quality)
    # 2. Most recent modification time
    # 3. Shortest filename (simpler naming)

    def sort_key(img):
        return (
            -img['file_size'],  # Negative for descending (largest first)
            -img['modified_time'],  # Negative for descending (newest first)
            len(img['file_name'])  # Ascending (shortest first)
        )

    sorted_images = sorted(image_group, key=sort_key)
    return sorted_images[0]  # Return the best one


def backup_files(files_to_delete):
    """Backup files before deletion"""
    if not files_to_delete:
        return True

    print(f"\nBacking up {len(files_to_delete)} files...")

    # Create backup directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_backup_dir = os.path.join(BACKUP_DIR, f"session_{timestamp}")
    os.makedirs(session_backup_dir, exist_ok=True)

    success_count = 0

    for file_path in files_to_delete:
        try:
            # Maintain directory structure in backup
            rel_path = os.path.relpath(file_path, EBAY_BASE_DIR)
            backup_path = os.path.join(session_backup_dir, rel_path)

            # Create backup directory structure
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)

            # Copy file to backup
            shutil.copy2(file_path, backup_path)
            success_count += 1

        except Exception as e:
            logging.error(f"Error backing up {file_path}: {e}")
            print(f"Warning: Failed to backup {file_path}")

    print(f"Successfully backed up {success_count}/{len(files_to_delete)} files to {session_backup_dir}")
    return success_count == len(files_to_delete)


def delete_duplicates(exact_duplicates, similar_groups, dry_run=True):
    """Delete duplicate files, keeping the best one from each group"""

    files_to_delete = []
    kept_files = []

    # Process exact duplicates
    print(f"\nProcessing {len(exact_duplicates)} groups of exact duplicates...")
    for hash_val, image_group in exact_duplicates.items():
        best_image = choose_best_image(image_group)
        kept_files.append(best_image)

        for img in image_group:
            if img['file_path'] != best_image['file_path']:
                files_to_delete.append(img['file_path'])
                print(f"  Will delete: {img['file_name']} (keeping {best_image['file_name']})")

    # Process similar images
    print(f"\nProcessing {len(similar_groups)} groups of similar images...")
    for i, image_group in enumerate(similar_groups):
        best_image = choose_best_image(image_group)
        kept_files.append(best_image)

        print(f"Similar group {i + 1}:")
        for img in image_group:
            if img['file_path'] != best_image['file_path']:
                files_to_delete.append(img['file_path'])
                print(f"  Will delete: {img['file_name']} (keeping {best_image['file_name']})")
            else:
                print(f"  Will keep: {img['file_name']} (best quality)")

    total_to_delete = len(files_to_delete)
    total_space_saved = sum(os.path.getsize(f) for f in files_to_delete) / (1024 * 1024)  # MB

    print(f"\nSummary:")
    print(f"Total files to delete: {total_to_delete}")
    print(f"Estimated space savings: {total_space_saved:.1f} MB")

    if dry_run:
        print("\n*** DRY RUN MODE - No files will be deleted ***")
        return files_to_delete, kept_files

    # Confirm deletion
    response = input(f"\nDelete {total_to_delete} duplicate files? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Deletion cancelled.")
        return [], []

    # Backup files before deletion
    if not backup_files(files_to_delete):
        print("Backup failed. Aborting deletion for safety.")
        return [], []

    # Delete the files
    print(f"\nDeleting {total_to_delete} duplicate files...")
    deleted_count = 0

    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            deleted_count += 1
            if deleted_count % 50 == 0:
                print(f"Deleted {deleted_count}/{total_to_delete} files...")
        except Exception as e:
            logging.error(f"Error deleting {file_path}: {e}")
            print(f"Error deleting {file_path}: {e}")

    print(f"Successfully deleted {deleted_count}/{total_to_delete} files")
    print(f"Freed up approximately {total_space_saved:.1f} MB of space")

    return files_to_delete, kept_files


def generate_report(all_images, exact_duplicates, similar_groups, files_deleted, files_kept):
    """Generate a detailed report of the duplicate removal process"""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"ebay_duplicate_removal_report_{timestamp}.html")

    # Calculate statistics
    total_images = len(all_images)
    total_exact_dupes = sum(len(imgs) - 1 for imgs in exact_duplicates.values())
    total_similar = sum(len(group) - 1 for group in similar_groups)
    total_deleted = len(files_deleted)
    space_saved = sum(os.path.getsize(f) for f in files_deleted if os.path.exists(f)) / (1024 * 1024)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>eBay Image Duplicate Removal Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background: #f0f8ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
            .stat-box {{ background: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
            .stat-number {{ font-size: 2em; font-weight: bold; color: #2c5aa0; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .deleted {{ background-color: #ffebee; }}
            .kept {{ background-color: #e8f5e8; }}
        </style>
    </head>
    <body>
        <h1>eBay Image Duplicate Removal Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <div class="summary">
            <h2>Summary Statistics</h2>
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-number">{total_images}</div>
                    <div>Total Images Scanned</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_exact_dupes}</div>
                    <div>Exact Duplicates Found</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_similar}</div>
                    <div>Similar Images Found</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_deleted}</div>
                    <div>Files Deleted</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{space_saved:.1f} MB</div>
                    <div>Space Saved</div>
                </div>
            </div>
        </div>

        <h2>Duplicate Groups</h2>
    """

    # Add exact duplicate groups
    if exact_duplicates:
        html_content += "<h3>Exact Duplicates</h3>"
        for i, (hash_val, images) in enumerate(exact_duplicates.items()):
            html_content += f"<h4>Group {i + 1} ({len(images)} files)</h4><table>"
            html_content += "<tr><th>File</th><th>Size</th><th>Status</th></tr>"

            best = choose_best_image(images)
            for img in images:
                status = "KEPT" if img['file_path'] == best['file_path'] else "DELETED"
                row_class = "kept" if status == "KEPT" else "deleted"
                html_content += f"""
                <tr class="{row_class}">
                    <td>{img['file_name']}</td>
                    <td>{img['file_size']:,} bytes</td>
                    <td>{status}</td>
                </tr>
                """
            html_content += "</table>"

    # Add similar image groups
    if similar_groups:
        html_content += "<h3>Similar Images</h3>"
        for i, images in enumerate(similar_groups):
            html_content += f"<h4>Group {i + 1} ({len(images)} files)</h4><table>"
            html_content += "<tr><th>File</th><th>Size</th><th>Status</th></tr>"

            best = choose_best_image(images)
            for img in images:
                status = "KEPT" if img['file_path'] == best['file_path'] else "DELETED"
                row_class = "kept" if status == "KEPT" else "deleted"
                html_content += f"""
                <tr class="{row_class}">
                    <td>{img['file_name']}</td>
                    <td>{img['file_size']:,} bytes</td>
                    <td>{status}</td>
                </tr>
                """
            html_content += "</table>"

    html_content += """
    </body>
    </html>
    """

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Detailed report saved to: {report_path}")
    return report_path


def main():
    """Main function to orchestrate duplicate removal"""
    print("eBay Image Duplicate Remover")
    print("=" * 40)

    # Check if imagehash is available
    try:
        import imagehash
    except ImportError:
        print("Error: imagehash library not found. Please install it:")
        print("pip install imagehash")
        return

    logging.info("Starting eBay image duplicate removal process")

    # Find all images
    all_images = find_all_images()
    if not all_images:
        print("No images found to process.")
        return

    # Find exact duplicates
    exact_duplicates = find_exact_duplicates(all_images)

    # Find similar images
    print("\nDo you want to find visually similar images? (y/n): ", end="")
    find_similar = input().strip().lower() in ['y', 'yes']

    similar_groups = []
    if find_similar:
        similarity_threshold = 5  # Default threshold
        try:
            threshold_input = input(f"Enter similarity threshold (0-10, default {similarity_threshold}): ").strip()
            if threshold_input:
                similarity_threshold = int(threshold_input)
        except ValueError:
            print(f"Invalid threshold, using default: {similarity_threshold}")

        similar_groups = find_similar_images(all_images, similarity_threshold)

    # Ask for dry run or actual deletion
    print("\nDo you want to perform a dry run first? (recommended) (y/n): ", end="")
    dry_run = input().strip().lower() in ['y', 'yes', '']

    # Delete duplicates
    files_deleted, files_kept = delete_duplicates(exact_duplicates, similar_groups, dry_run)

    # Generate report
    report_path = generate_report(all_images, exact_duplicates, similar_groups, files_deleted, files_kept)

    print(f"\nProcess completed!")
    print(f"Log file: {log_file}")
    print(f"Report: {report_path}")

    if dry_run:
        print("\n*** This was a dry run - no files were actually deleted ***")
        print("Run the script again and choose 'no' for dry run to perform actual deletion.")


if __name__ == "__main__":
    main()