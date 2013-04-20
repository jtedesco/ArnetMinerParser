from Stemmer import Stemmer
import cProfile
import json
import os
import re
import socket
import string
import zipfile
import sys
import hashlib
import operator
import traceback
from collections import defaultdict
from xml.etree import cElementTree
from _socket import AF_INET, SOCK_DGRAM

# The path to the input data
data_path = 'data'
if not os.path.exists(data_path):
    data_path = '/mnt/fcroot/full-arnetminer/data'
intermediate_results_folder = 'intermediate_output'

# Counts of papers with particular issues
documents_found = 0
documents_processed = 0
books_skipped = 0
documents_errors = 0
documents_missing_authors = 0
documents_missing_venue = 0
documents_missing_title = 0
documents_fb_non_chapter = 0
documents_missing_authors_fb_non_chapter = 0
empty_xml_files = 0
documents_skipped_from_title = 0
docs_missing_printable_data = 0
document_types = defaultdict(int)

# Counts of papers with particular reference issues
references_in_unexpected_format = 0
references_in_plaintext = 0
references_without_titles = 0
references_without_authors = 0
references_without_date = 0
references_with_invalid_date = 0
references_attempted = 0
references_succeeded = 0
docs_missing_references = 0
docs_with_few_references = 0

# Count hash collisions
hashes_seen = set()
hash_collision_count = 0
hash_collisions = set()
check_hash_collisions = False

# Shortcut for filtering a strint to printable characters
printable = lambda s: filter(lambda x: x in string.printable, s)
ascii_printable = lambda s: filter(lambda x: x in string.letters, s)

# Total counts
total_zip_files = 1052
total_papers = 10454961

zip_files_processed = 0

# Pre-compile year regex
year_regex = re.compile(r'\D(\d{4})\D')

# Flag for output mode
output_to_standard_out = True

# Data for localhost progress UDP server
server_address = ('localhost', 6005)
client_socket = socket.socket(AF_INET, SOCK_DGRAM)

# Inefficient tallies
title_counts = defaultdict(int)
venue_counts = defaultdict(int)

# Get the stop words set & stemmer for text analysis
stop_words = None
with open('stopWords.json') as stop_words_file:
    stop_words = set(json.load(stop_words_file))
stemmer = Stemmer('english')


class DBLPParseError(Exception):
    pass


def is_useless_doc(title):
    """
      Try to determine whther or not to skip this document (based on the title)
    """

    # Short titles are almost always meaningless documents
    if len(title) <= 10:
        return True

    title_lower = title.lower()

    # If the title starts (FALSE) with or matches exactly (TRUE) for the string given, skip
    starts_with_strings = [
        ('about this', False),
        ('acknowledgment', False),
        ('announcements', True),
        ('announcement', True),
        ('associate editors', True),
        ('authors\' reply', True),
        ('author\'s reply', True),
        ('author index', False),
        ('board of editors', True),
        ('book review', True),
        ('call for papers', False),
        ('case history', True),
        ('contents', False),
        ('corrigendum', True),
        ('correspondence', True),
        ('cumulative subject index', True),
        ('editorial board', False),
        ('introduction', True),
        ('inside front cover', False),
        ('letter from the editor', True),
        ('letter to the editor', True),
        ('list of contents', False),
        ('note from the publisher', True),
        ('special issue contents', True),
        ('subject index', True),
        ('title index', False),
        ('to the editor', True),
        ('withdrawn:', False),
        ('volume', False),
    ]
    starts_with_anything_to_skip = \
        max([title_lower == word if strict else title_lower.startswith(word) for word, strict in starts_with_strings])
    return starts_with_anything_to_skip


def author_surname_from_element(author_element):
    surname_el = author_element.find('{http://www.elsevier.com/xml/common/schema}surname')
    if (surname_el is None) or (surname_el.text is None) or (not len(surname_el.text.split())):
        return None
    return str(printable(surname_el.text.split()[-1]))


def author_given_name_from_element(author_element):
    given_name_el = author_element.find('{http://www.elsevier.com/xml/common/schema}given-name')
    if (given_name_el is None) or (given_name_el.text is None) or (not len(given_name_el.text.split())):
        return None
    return str(printable(given_name_el.text.split()[-1]))


def full_authors_string_from_elements(author_elements):
    """
      Get the comma-separated list of authors from a list of author XML elements
    """

    authors_strings = []
    for el in author_elements:
        given_name = author_given_name_from_element(el)
        surname = author_surname_from_element(el)
        if given_name is not None and surname is not None:
            authors_strings.append(given_name + ' ' + surname)
    return ','.join(authors_strings)


def clean_string_from_element(element):
    """
      Clean the title text (remove extra white space) from title element
    """

    raw_text = element.text
    clean_text = re.sub(r'\s+', ' ', raw_text.strip()) if raw_text is not None else None
    return clean_text


def ascii_terms_from_element(element):
    """
      Get the text of the terms from an element, after stemming and removing stop words
    """

    words = element.text.split()
    terms = [ascii_printable(stemmer.stemWord(word.lower())) for word in words if word.lower() not in stop_words]
    terms = [str(term) for term in terms if term is not None]
    return ''.join(terms)


def ascii_text_from_element(element):
    """
      Get the text, only ascii plain text, from the given XML element
    """

    raw_text = element.text
    clean_text = str(ascii_printable(raw_text.strip())) if raw_text is not None else None
    return clean_text


def hash_document_data(title, authors_string):
    """
      Calculate MD5 hash based on uniquely identifying doc data
    """
    data_string = '%sZZZ%s' % (title.strip().lower(), authors_string.strip().lower())
    data_hash = int(hashlib.sha1(data_string.encode('utf-8')).hexdigest(), 16)
    return data_hash


def parse_references(doc_root, aggregation_type):
    """
      Find the hashes of documents referenced by the document, given the document's root element and doc type
    """

    global references_attempted, references_without_titles, references_without_authors, references_without_date, \
        references_in_unexpected_format, references_with_invalid_date, references_succeeded, docs_missing_references,\
        docs_with_few_references, references_in_plaintext

    reference_elements = []

    # Explicitly handle references for a book differently than for other formats
    if aggregation_type.lower() == 'book':
        bibliography = doc_root.find('*/*/{http://www.elsevier.com/xml/common/schema}further-reading-sec')
    else:
        bibliography = doc_root.find('*/*/{http://www.elsevier.com/xml/common/schema}bibliography')

    # Try to find references directly
    if bibliography is not None:
        reference_elements += bibliography.findall(
            '*/*/{http://www.elsevier.com/xml/common/struct-bib/schema}reference'
        )
        reference_elements += bibliography.findall('*/{http://www.elsevier.com/xml/common/struct-bib/schema}reference')
    else:

        # Fall back to alternative references format (skips plaintext refs!)
        bib_ref = doc_root.find('*/*/*/{http://www.elsevier.com/xml/common/schema}bib-reference/')
        if bib_ref is not None:
            reference_elements += bib_ref.findall('{http://www.elsevier.com/xml/common/struct-bib/schema}reference')
        alt_bib_ref = doc_root.find('*/*/*/*/{http://www.elsevier.com/xml/common/schema}bib-reference')
        if alt_bib_ref is not None:
            reference_elements += alt_bib_ref.findall('{http://www.elsevier.com/xml/common/struct-bib/schema}reference')

    reference_ids = []
    contains_any_ref = False
    for reference_element in reference_elements:

        contains_any_ref = True
        references_attempted += 1

        # Find both sections of reference
        contribution = reference_element.find('{http://www.elsevier.com/xml/common/struct-bib/schema}contribution')
        host = reference_element.find('{http://www.elsevier.com/xml/common/struct-bib/schema}host')

        # Skip references that are formatted differently than expected (e.g. book references)
        if contribution is None or host is None:

            # Check if this is a text only reference
            text_ref_element = reference_element.findall('*/{http://www.elsevier.com/xml/common/schema}textref')
            if text_ref_element is not None:

                # Ignore text references (un-parsed / unstructured refs)
                references_in_plaintext += 1
                continue

            else:

                references_in_unexpected_format += 1
                continue

        # Get title
        title = contribution.find('*/{http://www.elsevier.com/xml/common/struct-bib/schema}maintitle')
        if title is None:
            title = host.find('*/*/*/*/{http://www.elsevier.com/xml/common/struct-bib/schema}maintitle')
        if title is None:
            title = host.find('*/*/*/{http://www.elsevier.com/xml/common/struct-bib/schema}maintitle')
        if title is None:
            title = host.find('*/*/{http://www.elsevier.com/xml/common/struct-bib/schema}maintitle')
        if title is not None:
            title = ascii_terms_from_element(title)

        # Handle missing or invalid titles
        if title is None:
            references_without_titles += 1
            continue

        # Get authors strings
        ref_author_elements = contribution.findall('*/{http://www.elsevier.com/xml/common/struct-bib/schema}author')
        if len(ref_author_elements):

            # Try to reference last name of first author
            first_author_surname = author_surname_from_element(ref_author_elements[0])
            reference_ids.append(hash_document_data(title, first_author_surname))

        else:
            references_without_authors += 1
            continue

        # Assume only one of this batch of references will succeed
        references_succeeded += 1

    # Record cases with few or no references
    if not contains_any_ref:
        docs_missing_references += 1
    elif len(reference_ids) < 3:
        docs_with_few_references += 1

    # Check for invalid reference ids
    if not (len(reference_ids) == 0 or min(reference_ids)):
        raise AssertionError("Failed to parse references, got 0-id references!")

    return reference_ids


def gen_documents_from_file(zipped_file):
    """
      Generator that yields the next paper contents from a zipped file containing many XML files
    """

    # Import global counts
    global books_skipped, documents_found, documents_processed, documents_missing_authors, documents_missing_title, \
        documents_fb_non_chapter, documents_missing_authors_fb_non_chapter, \
        documents_skipped_from_title, references_attempted, references_without_titles, references_without_authors, \
        references_without_date, should_output_path, hash_collision_count, hash_collisions, documents_missing_venue, \
        documents_errors

    for name in zipped_file.namelist():

        documents_found += 1

        # Skip non-xml files
        if not name.endswith('xml'):
            log(num_to_skip, "Skipping non-xml file in archive: '%s'" % name)
            continue

        try:

            # Parse paper XML
            xml_content = zipped_file.read(name)
            doc_root = cElementTree.fromstring(xml_content)

            # Remove main content of paper, if possible
            for el in doc_root.getchildren():
                raw_text = el.find('{http://www.elsevier.com/xml/common/doc-properties/schema}raw-text')
                if raw_text is not None:
                    el.remove(raw_text)

            # Skip & log empty XML files
            if not len(xml_content.strip()):
                global empty_xml_files
                empty_xml_files += 1
                continue

            # Authors
            author_elements = doc_root.findall('*/*/{http://www.elsevier.com/xml/common/schema}author')  # Book authors

            # Look in alternate location for authors, if not found above
            if not len(author_elements):
                journal_converted_article_authors = \
                    doc_root.find('*/*/{http://www.elsevier.com/xml/common/schema}author-group')
                if journal_converted_article_authors is not None:
                    author_elements += journal_converted_article_authors.findall(
                        '{http://www.elsevier.com/xml/common/schema}author'
                    )

            # Paper title
            title_element = doc_root.find('*/*/{http://purl.org/dc/elements/1.1/}title')
            title = clean_string_from_element(title_element)
            hashable_title = None if (title is None) else ascii_terms_from_element(title_element)

            # Skip this document if title is missing or empty
            if title is None or not len(title.strip()):
                documents_missing_title += 1
                continue

            # Skip this document if we can / should based on the title
            if is_useless_doc(title):
                documents_skipped_from_title += 1
                continue

            # Add the authors, if found
            if len(author_elements):
                first_author_surname = author_surname_from_element(author_elements[0])
                authors_string = full_authors_string_from_elements(author_elements)
            else:
                documents_missing_authors += 1
                continue

            # If this document is a 'non-chapter', means we can skip it
            fb_non_chapter = doc_root.find('{http://www.elsevier.com/xml/bk/schema}fb-non-chapter')
            if fb_non_chapter is not None:
                documents_fb_non_chapter += 1
                continue

            # Increment the count of this document type
            aggregation_type_element = doc_root.find(
                '*/*/{http://prismstandard.org/namespaces/basic/2.0/}aggregationType'
            )
            document_type = aggregation_type_element.text

            # Publication venue
            venue_element = doc_root.find('*/*/{http://www.elsevier.com/xml/cja/schema}jid')  # Journal id?
            if venue_element is None:
                venue_element = doc_root.find('*/*/{http://prismstandard.org/namespaces/basic/2.0/}publicationName')
            else:
                # Mark document as converted article
                document_type = aggregation_type_element.text + ' (cja)'
            venue = clean_string_from_element(venue_element)
            if venue is None:
                documents_missing_venue += 1
                continue

            document_types[document_type] += 1

            # Skip this paper if it's the same title as its conference
            if venue.lower() == title.lower():
                documents_skipped_from_title += 1
                continue

            # Publication year
            year_element = doc_root.find('*/*/{http://prismstandard.org/namespaces/basic/2.0/}coverDisplayDate')
            if year_element is not None:
                year = int(year_element.text)
            else:  # Fall back to get year from the copyright, if it's not explicitly listed
                year_element = doc_root.find('*/*/{http://www.elsevier.com/xml/common/schema}copyright')
                if year_element is None:
                    raise AssertionError("Failed to find date in document!")
                year = int(year_element.attrib['year'])
            if not (1800 <= year <= 2013):
                raise AssertionError("Found year before 1800 or after 2013!")

            # Calculate the hash (or 'index') for this document
            index = hash_document_data(hashable_title, first_author_surname)

            # Ensure that this paper's index is unique & record it
            if check_hash_collisions:
                if index in hashes_seen:
                    hash_collision_count += 1
                    hash_collisions.add(index)
                else:
                    hashes_seen.add(index)

            # Parse the references for this document
            reference_ids = parse_references(doc_root, aggregation_type_element.text)

        except:  # Handle any unforeseen errors by logging them & skipping this document

            # Handle all exceptions by tallying unforeseen errors
            log(num_to_skip, "[Unexpected Error] '%s'" % traceback.format_exc())
            documents_errors += 1
            continue

        documents_processed += 1
        yield title, authors_string, year, venue, index, reference_ids, name


def build_reference_stats_message():
    """
      Output statistics about document references
    """

    count_and_percent = lambda a, b: (a, b, float(a) / b * 100 if b > 0 else 0)

    # Output document reference errors
    output_message = "\nDocument Reference Errors Found:\n" 
    m = references_attempted
    output_message += "\tReferences In Unexpected Format: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(references_in_unexpected_format, m)
    m -= references_in_unexpected_format
    output_message += "\tReferences in Plain Text: %d / %d (%2.2f%%)\n" % count_and_percent(references_in_plaintext, m)
    m -= references_in_plaintext
    output_message += "\tReferences Without Titles: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(references_without_titles, m)
    m -= references_without_titles
    output_message += "\tReferences Without Authors: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(references_without_authors, m)
    m -= references_without_authors
    output_message += "\tReferences Without Date: %d / %d (%2.2f%%)\n" % count_and_percent(references_without_date, m)
    m -= references_without_date
    output_message += "\tReferences With Invalid Date: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(references_with_invalid_date, m)
    m -= references_with_invalid_date
    output_message += "\tReferences Succeeded: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(references_succeeded, references_attempted)

    # Don't raise a fatal error if counts are not as expected
    if m != references_succeeded:
        print "Document reference tally failed!!! m: %d, actually succeeded: %d" % (m, references_succeeded)

    # Try to estimate the recall of references
    output_message += "\nDocuments Without References: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(docs_missing_references, documents_processed)
    output_message += "\nDocuments With Few References: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(docs_with_few_references, documents_processed)

    return output_message


def build_document_stats_message():
    """
      Output statistics about documents
    """

    count_and_percent = lambda a, b: (a, b, float(a) / b * 100 if b > 0 else 0)

    # Fatal paper parsing errors (most likely bad data)
    n = documents_found
    output_message = "\nDocument Errors (Fatal):\n"
    output_message += "\tEmpty Document Files: %d / %d (%2.2f%%)\n" % count_and_percent(empty_xml_files, n)
    n -= empty_xml_files
    output_message += "\tBooks Skipped: %d / %d (%2.2f%%)\n" % count_and_percent(books_skipped, n)
    n -= books_skipped
    output_message += "\tDocuments Missing Title: %d / %d (%2.2f%%)\n" % count_and_percent(documents_missing_title, n)
    n -= documents_missing_title
    output_message += "\tDocuments Skipped Based on Title: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(documents_skipped_from_title, n)
    n -= documents_skipped_from_title
    output_message += "\tDocuments Skipped From Non-Chapter Tag: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(documents_fb_non_chapter, n)
    n -= documents_fb_non_chapter
    output_message += "\tDocuments Missing Authors: %d / %d (%2.2f%%)\n" % \
                      count_and_percent(documents_missing_authors, n)
    n -= documents_missing_authors
    output_message += "\tDocuments Missing Venue: %d / %d (%2.2f%%)\n" % count_and_percent(documents_missing_venue, n)
    n -= documents_missing_venue
    output_message += '\tDocuments Skipped (Unknown Error): %d / %d (%2.2f%%)\n' % \
                      count_and_percent(documents_errors, n)
    n -= documents_errors

    if n != documents_processed:
        print "Document tally failed!!! n: %d, actually succeeded: %d" % (n, documents_processed)

    output_message += "\nDocuments Missing Printable Data: %d / %d/ (%2.2f%%)\n" % \
                      count_and_percent(docs_missing_printable_data, n)

    # Non-fatal document parsing errors (potentially bad data)
    output_message += "\nDocument Errors (Ignored):\n"
    output_message += "\tDocument Hash Collisions: %d / %d (%2.2f%%)\n" % count_and_percent(hash_collision_count, n)

    # Breakdown of type of documents encountered
    output_message += "\nDocument Types:\n"
    for key, count in document_types.iteritems():
        output_message += "\t%s: %d / %d (%2.2f%%)\n" % \
                          tuple([key.title()] + list(count_and_percent(document_types[key], documents_processed)))

    return output_message


def build_title_and_venue_tallies_message():
    """
      Output tallies for the most common titles and venues
    """

    k = 10
    output_message = "\nMost Frequent Titles:\n"
    sorted_titles = sorted(title_counts.iteritems(), key=operator.itemgetter(1))
    sorted_titles = list(reversed(sorted_titles))
    for i in xrange(0, k):
        output_message += "\t%s: %d\n" % (sorted_titles[i][0], sorted_titles[i][1])

    output_message += "\nMost Frequent Venues:\n"
    sorted_venues = sorted(venue_counts.iteritems(), key=operator.itemgetter(1))
    sorted_venues = list(reversed(sorted_venues))
    for i in xrange(0, min(k, len(sorted_venues))):
        output_message += "\t%s: %d\n" % (sorted_venues[i][0], sorted_venues[i][1])

    return output_message


def output_progress(
        num_to_skip,
        estimated_total_documents,
        zip_files_to_process,
        zip_files_processed,
        documents_processed,
        flush=False):
    """
      Output the current parser progress
    """

    percent_complete = (documents_processed / float(estimated_total_documents) * 100.0)

    if output_to_standard_out:

        # Output incremental results to stdout
        sys.stdout.write("\r (~%2.2f%%) Processed %d / %d ZIP Files, %d / ~%d papers ... " % (
            percent_complete, zip_files_processed, zip_files_to_process, documents_processed, estimated_total_documents)
        )
        if flush:
            sys.stdout.flush()

    else:

        # Send raw values to progress server
        progress_data = '%d %d %d %d' % (num_to_skip, zip_files_processed, zip_files_to_process, documents_processed)
        client_socket.sendto(progress_data, server_address)


def log(start_num, message):
    """
      Log to the log file for this slave
    """

    log_path = os.path.join(intermediate_results_folder, '%d-log.txt' % start_num)
    file_mode = 'a' if os.path.exists(log_path) else 'w'
    with open(log_path, file_mode) as log_file:
        log_file.write(message + '\n')


def main(output_path, num_to_skip, num_to_process, tally_venues_and_titles=False):

    global zip_files_processed, docs_missing_printable_data

    # Estimate the max documents & files to process
    estimated_total_documents = int(float(num_to_process) / total_zip_files * total_papers)

    # Open output file
    output_file = open(output_path, 'w')

    file_num = 0
    for filename in os.listdir(data_path):
        full_file_path = os.path.join(data_path, filename)

        # Skip this file if we should
        file_num += 1
        if file_num <= num_to_skip + 1:
            continue

        # Stop once we've parsed as many as we were told to
        if zip_files_processed >= num_to_process:
            break

        # Open zip file, or skip if invalid
        try:
            zipped_file = zipfile.ZipFile(full_file_path, "r")
        except zipfile.BadZipfile, e:
            log(num_to_skip, "Skipping '%s', error opening zip file: '%s'" % (full_file_path, e.message))
            continue
        except AssertionError, e:
            log(num_to_skip, "Skipping '%s', assertion error: '%s'" % (full_file_path, e.message))
            continue
        except IOError, e:
            log(num_to_skip, "Skipping '%s', I/O error: '%s'" % (full_file_path, e.message))
            continue

        for title, authors, year, venue, index, reference_ids, orig_filename in gen_documents_from_file(zipped_file):

            # Inefficient tallies
            if tally_venues_and_titles:
                title_counts[title] += 1
                venue_counts[venue] += 1

            # Output paper data to output file (just remove non-ascii characters)
            title, printable_authors, printable_venue = \
                printable(title.strip()), printable(authors.strip()), printable(venue.strip())
            if len(printable_authors) and len(printable_venue):
                output_file.write('#*%s\n' % title)
                output_file.write('#@%s\n' % printable_authors)
                output_file.write('#year%d\n' % year)
                output_file.write('#conf%s\n' % printable_venue)
                output_file.write('#index%d\n' % index)
                output_file.write('#path%s:%s\n' % (filename, orig_filename))
                output_file.write(''.join(['#%%%d\n' % ref_id for ref_id in reference_ids]) + '\n')
            else:
                docs_missing_printable_data += 1

            # Write the current progress to stdout (intermittently)
            if documents_processed % 100 == 0:
                output_progress(
                    num_to_skip, estimated_total_documents, num_to_process, zip_files_processed, documents_processed
                )

        # Output parsing progress
        zip_files_processed += 1

        # Cleanup
        zipped_file.close()

        # Update the progress
        output_progress(
            num_to_skip, estimated_total_documents, num_to_process, zip_files_processed, documents_processed, flush=True
        )

    # Output document & reference statistics
    output_message = build_document_stats_message()
    output_message += build_reference_stats_message()
    if tally_venues_and_titles:
        output_message += build_title_and_venue_tallies_message()
    if output_to_standard_out:
        print output_message
    else:
        with open(os.path.join(intermediate_results_folder, '%d-stats.txt') % num_to_skip, 'w') as output_file:
            output_file.write(output_message)


def output_usage(num_to_skip):
    """
      Output the usage of the program, and exit
    """

    log(num_to_skip,
        "USAGE: \033[1m first_pass_slave_parser.py <start> <num> <progress on stdout> [<debug>]\033[0m\n" +
        "\t\033[1m<start>\033[0m: the number of zip file to parse first (numbered from 1)\n" +
        "\t\033[1m<num>\033[0m: the number of files following to parse\n" +
        "\t\033[1m<progress on stdout>\033[0m: whether to show progress on standard out ('y') or in file ('n')\n" +
        "\t\033[1m<debug>\033[0m: whether or not ('y' / 'n') to profile or tally titles and venues during parsing")
    sys.exit()


if __name__ == "__main__":

    max_num_args = 5
    min_num_args = 4

    # Verify correct number of arguments
    if len(sys.argv) < min_num_args or len(sys.argv) > max_num_args:
        output_usage(0)

    # Verify the numerical arguments
    invalid_first_args = False
    num_to_skip = 0
    num_to_process = total_zip_files
    try:
        num_to_skip = int(sys.argv[1]) - 1
        num_to_process = int(sys.argv[2])
    except ValueError:
        output_usage(num_to_skip)

    # Based the output path off of the starting file
    output_path = os.path.join(intermediate_results_folder, '%d-intermediate_output.txt' % num_to_skip)

    # Parse whether or not progress should be output to standard out (rather than static file)
    if sys.argv[3][0] not in {'y', 'n', 'Y', 'N'}:
        output_usage(num_to_skip)
    output_to_standard_out = sys.argv[3][0] in {'y', 'Y'}

    # Verify the last optional argument
    invalid_fourth_arg = len(sys.argv) > min_num_args and sys.argv[min_num_args][0] not in {'y', 'n', 'Y', 'N'}
    if invalid_fourth_arg:
        output_usage(num_to_skip)
    should_profile = len(sys.argv) > min_num_args and sys.argv[min_num_args][0] in {'y', 'Y'}

    if should_profile:
        cProfile.run("main('%s', %d, %d, tally_venues_and_titles=%r)" % (
            output_path, num_to_skip, num_to_process, should_profile
        ))
    else:
        main(output_path, num_to_skip, num_to_process, tally_venues_and_titles=should_profile)
