"""
Check if all PDFs have corresponding info HTML files
"""
import os
from pathlib import Path

downloads_dir = "downloads"

# Opinion type folders
folders = [
    "Supreme_Court_Opinions",
    "Court_of_Appeals_Published",
    "Court_of_Appeals_Published_in_Part",
    "Court_of_Appeals_Unpublished"
]

def check_folder(folder_path):
    """Check if all PDFs have corresponding HTML files"""
    pdfs_found = []
    htmls_found = []
    missing = []
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.pdf'):
                pdf_path = os.path.join(root, file)
                pdfs_found.append(pdf_path)
                
                # Extract case number from PDF filename
                # Format: "case_number_Title.pdf" -> need just case_number
                case_number = file.split('_')[0]
                html_name = f"{case_number}_info.html"
                html_path = os.path.join(root, html_name)
                
                if os.path.exists(html_path):
                    htmls_found.append(html_path)
                else:
                    missing.append({
                        'pdf': file,
                        'case_number': case_number,
                        'expected_html': html_name,
                        'location': root
                    })
    
    return pdfs_found, htmls_found, missing


print("=" * 80)
print("Checking for missing info HTML files")
print("=" * 80)

total_pdfs = 0
total_htmls = 0
total_missing = 0

for folder in folders:
    folder_path = os.path.join(downloads_dir, folder)
    
    if not os.path.exists(folder_path):
        print(f"\n{folder}: Folder doesn't exist")
        continue
    
    pdfs, htmls, missing = check_folder(folder_path)
    
    total_pdfs += len(pdfs)
    total_htmls += len(htmls)
    total_missing += len(missing)
    
    print(f"\n{folder}:")
    print(f"  PDFs found: {len(pdfs)}")
    print(f"  HTMLs found: {len(htmls)}")
    print(f"  Missing: {len(missing)}")
    
    if missing and len(missing) <= 20:
        print(f"  Missing files:")
        for item in missing[:10]:
            print(f"    - {item['case_number']} ({item['pdf'][:50]}...)")
        if len(missing) > 10:
            print(f"    ... and {len(missing) - 10} more")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total PDFs: {total_pdfs}")
print(f"Total HTMLs: {total_htmls}")
print(f"Missing HTMLs: {total_missing}")
print(f"Coverage: {(total_htmls/total_pdfs*100) if total_pdfs > 0 else 0:.1f}%")
