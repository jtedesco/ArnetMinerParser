from collections import defaultdict
import os
import re
import subprocess
import zipfile
import sys

__author__ = 'jontedesco'

# The path to the input data (check several locations, for convenience)
data_path = '/Users/jontedesco/Desktop/parsing-data'
if not os.path.exists(data_path):
    data_path = '/mnt/fcroot/full-arnetminer/data'
if not os.path.exists(data_path):
    data_path = 'data'

# Sub-strings to find in the document
strings_to_find = {
    'kdd': [
        'kdd',
        'k.d.d.',
        'knowledge discovery in databases'
    ]
}

# Matched documents for each keyword group
matches = defaultdict(list)

# Find the raw content of docs
raw_text_regex = re.compile('<dp:raw-text>.*</dp:raw-text>')

if __name__ == '__main__':

    documents_found = 0
    zip_files_processed = 0
    zip_files_to_process = 1052
    total_papers = 10454961

    for filename in os.listdir(data_path):
        full_file_path = os.path.join(data_path, filename)

        # Open zip file, or skip if invalid
        try:
            zipped_file = zipfile.ZipFile(full_file_path, "r")
        except zipfile.BadZipfile, e:
            print "Skipping '%s', error opening zip file: '%s'" % (full_file_path, e.message)
            continue
        except AssertionError, e:
            print "Skipping '%s', assertion error: '%s'" % (full_file_path, e.message)
            continue
        except IOError, e:
            print "Skipping '%s', I/O error: '%s'" % (full_file_path, e.message)
            continue

        # Iterate through each file in the document
        for xml_file_name in zipped_file.namelist():
            documents_found += 1

            # Skip non-xml files
            if not xml_file_name.endswith('xml'):
                print "Skipping non-xml file in archive: '%s'" % xml_file_name
                continue

            # Get paper contents
            xml_content = zipped_file.read(xml_file_name)
            if not len(xml_content.strip()):
                continue

            # Remove main content of paper, if possible
            xml_content = re.sub(raw_text_regex, '', xml_content)

            # Search for each substring
            substring_found = None
            for string_group in strings_to_find:
                for string_to_find in strings_to_find[string_group]:
                    if string_to_find in xml_content:
                        substring_found = string_to_find
                        matches[string_group].append((filename, xml_file_name))
                        break

            # Output progress
            if documents_found % 10 == 0:
                percent_complete = 100 * float(documents_found) / total_papers
                sys.stdout.write("\r (~%2.2f%%) Processed %d / %d ZIP Files, %d / ~%d papers ... " % (
                    percent_complete, zip_files_processed, zip_files_to_process, documents_found, total_papers)
                )

        zip_files_processed += 1

    # Extract target XML files
    inspect_dir = 'to_inspect'
    for string_group in matches:
        os.mkdir(os.path.join(inspect_dir, string_group))

        # Extract to inspection dir
        for filename, xml_file_name in matches[string_group]:
            subprocess.Popen('unzip -d %s %s %s' % (
                os.path.join(inspect_dir, string_group),
                os.path.join('data', filename),
                xml_file_name
            ), shell=True)
