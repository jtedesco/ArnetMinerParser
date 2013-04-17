import cProfile
from collections import defaultdict
import os
import traceback

__author__ = 'jontedesco'

import re
import sys

__author__ = 'jontedesco'

# Global stats
invalid_papers = 0
references_attempted = 0
references_succeeded = 0
dangling_references = 0
collision_references = 0

# Regular expressions for stripping non-visible characters
control_chars = ''.join(map(unichr, list(range(0, 32)) + list(range(127, 160))))
control_chars_regex = re.compile('[%s]' % re.escape(control_chars))
non_ascii_regex = re.compile('\W')

# The dictionary of citation counts for each paper
citation_counts = defaultdict(int)


def __remove_control_chars(string):
    string = string.strip('\xef\xbb\xbf')
    return control_chars_regex.sub('', string)


def __papers_from_file(input_file, index_collisions, paper_indices):
    """
      Generator function over papers (gets data from the next entry)
    """

    global invalid_papers, dangling_references, collision_references, references_attempted, references_succeeded

    # Tokens for parsing
    title_token = '#*'
    author_token = '#@'
    year_token = '#year'
    conf_token = '#conf'
    index_token = '#index'
    path_token = '#path'
    citation_token = '#%'

    # Predicates for error checking
    # Next entry data
    title = None
    authors = None
    year = None
    conference = None
    index = None
    path = None
    references = []

    this_line_empty = False

    for line in input_file:
        last_line_was_empty = this_line_empty
        this_line_empty = len(line.strip()) == 0

        line = line.strip()

        try:
            # Parse entry, asserting that entries appear in title -> authors -> conference order
            if line.startswith(title_token):
                title = line[len(title_token):].strip('.')
            elif line.startswith(author_token):
                authors = [author.strip() for author in line[len(author_token):].split(',')]
            elif line.startswith(year_token):
                year = int(line[len(year_token):].strip())
            elif line.startswith(conf_token):
                conference = line[len(conf_token):]
            elif line.startswith(index_token):
                index = int(line[len(index_token):])
            elif line.startswith(path_token):
                path = line[len(path_token):]
            elif line.startswith(citation_token):
                citation_index = int(line[len(citation_token):].strip())
                references_attempted += 1

                # Add reference if (1) not a collision and (2) links to a paper found
                if citation_index in index_collisions:
                    collision_references += 1
                    continue
                if citation_index not in paper_indices:
                    dangling_references += 1
                    continue

                references_succeeded += 1
                references.append(citation_index)
                citation_counts[citation_index] += 1

            # We've reached the end of the entry
            elif len(line) == 0:

                if last_line_was_empty:
                    continue

                # Only output if:
                #   (1) data is all not None
                #   (2) title, authors, and conference are valid (non-empty)
                #   (3) index index was found (do not enforce citation count)
                if all((title, authors, conference)) and index is not None:
                    if index not in index_collisions:
                        yield title, authors, year, conference, citation_counts[index], index, references, path
                else:
                    if all([item is None for item in [conference, index, title, authors]]):
                        continue
                    invalid_papers += 1

                # Reset for the next paper
                title = None
                authors = None
                year = None
                conference = None
                index = None
                path = None
                references = []
        except:

            # Handle all exceptions by tallying unforeseen errors
            print "[Unexpected Error Parsing Docs] '%s'" % traceback.format_exc()
            continue


def parse_intermediate_arnetminer_dataset():
    """
        Parse the intermediate full arnet miner dataset (in pseudo-arnetminer format), and output the full arnetminer
        plaintext format.
    """

    # Find intermediate output data
    laptop_input_path = 'intermediate_output-real-040613321'
    generic_input_path = 'intermediate_output'
    input_folder_path = laptop_input_path if os.path.exists(laptop_input_path) else generic_input_path

    # Counts for statistics
    TOTAL_PAPERS = 10454961
    VALID_PAPERS = 0.9 * TOTAL_PAPERS  # Estimate 90% validity

    # Holds all paper indices found
    paper_indices = []
    papers_processed = 0


    for input_file_name in os.listdir(input_folder_path):

        try:

            # Only look at intermediate results files
            if not input_file_name.endswith('intermediate_output.txt'):
                continue
            input_file = open(os.path.join(input_folder_path, input_file_name))

            # Build the citation counts for all papers
            for line in input_file:

                # Just look at index lines
                if not line.startswith('#index'):
                    continue

                paper_index = int(line[len('#index'):].strip())
                paper_indices.append(paper_index)

                # Record progress
                papers_processed += 1
                if papers_processed % 100 == 0:
                    sys.stdout.write("\r (%2.2f%%) Recorded %d / %d paper indices..." % (
                        (float(papers_processed) / VALID_PAPERS) * 100, papers_processed, VALID_PAPERS
                    ))

        except:

            # Handle all exceptions by tallying unforeseen errors
            print "[Unexpected Error Parsing Indices] '%s'" % traceback.format_exc()
            continue

    # Sort the indices (likely slow, with about 10 million papers)
    paper_indices.sort()

    # Stores information about duplicate paper indices
    index_collisions = set()
    index_collisions_count = 0

    # Look for duplicates in the indices (index collisions)
    last_index = 0
    for paper_index in paper_indices:

        # Tally this collision
        if paper_index == last_index:
            index_collisions.add(paper_index)
            index_collisions_count += 1

        last_index = paper_index

    # Output index collisions
    print "\n\nSkipped %d / %d (%2.2f%%) papers due to index collisions" % (
        index_collisions_count, papers_processed, (float(index_collisions_count) / papers_processed * 100)
    )

    # Reference counts for these papers
    total_reference_count = 0
    papers_with_references = 0
    total_citation_count = 0
    papers_with_citations = 0

    # Take paper indices as a set (to use to check dangling references)
    paper_indices = set(paper_indices)

    papers_processed = 0
    output_file = open(os.path.join('final_output', 'final_output.txt'), 'w')
    for input_file_name in os.listdir(input_folder_path):

        # Only look at intermediate results files
        if not input_file_name.endswith('intermediate_output.txt'):
            continue
        input_file = open(os.path.join(input_folder_path, input_file_name))

        # Add each paper to graph (adding missing associated terms, authors, and conferences)
        for title, authors, year, conference, citation_count, index, references, path in \
                __papers_from_file(input_file, index_collisions, paper_indices):

            # Output this paper to the final output file
            output_file.write('#*%s\n' % title)
            output_file.write('#@%s\n' % ','.join(authors))
            output_file.write('#year%d\n' % year)
            output_file.write('#conf%s\n' % conference)
            output_file.write('#citation%d\n' % citation_count)
            output_file.write('#index%d\n' % index)
            output_file.write('#path%s\n' % path)
            output_file.write(''.join(['#%%%d\n' % ref_id for ref_id in references]) + '\n')

            # Output progress
            papers_processed += 1
            if papers_processed % 10 == 0:
                sys.stdout.write("\r (%2.2f%%) Processed %d / %d papers..." % (
                    float(papers_processed)/ VALID_PAPERS * 100, papers_processed, VALID_PAPERS)
                )

            # Tally number of references and citations associated with this paper
            if len(references):
                papers_with_references += 1
            total_reference_count += len(references)
            if citation_count:
                papers_with_citations += 1
            total_citation_count += citation_count

    count_and_percent = lambda a, b: (a, b, float(a) / b * 100)

    # Output paper reference stats
    total_found = papers_processed + invalid_papers
    print "\n\nTotal Processed Papers: %d / %d (%2.2f%%)" % count_and_percent(papers_processed, total_found)
    print "  Invalid Papers Skipped: %d / %d (%2.2f%%)" % count_and_percent(invalid_papers, total_found)
    print "  References to Collisions: %d / %d (%2.2f%%)" % \
          count_and_percent(collision_references, references_attempted)
    print "  Dangling References: %d / %d (%2.2f%%)" % count_and_percent(dangling_references, references_attempted)
    print "  Papers with References: %d / %d (%2.2f%%)" % count_and_percent(papers_with_references, papers_processed)
    print "  Average Number of References (Overall): %2.2f" % (float(total_reference_count) / papers_processed)
    print "  Average Number of References (For Papers with References): %2.2f" % (
        float(total_reference_count) / papers_with_references
    )
    print "  Papers with Citations: %d / %d (%2.2f%%)" % count_and_percent(papers_with_citations, papers_processed)
    print "  Average Number of Citations (Overall): %2.2f" % (float(total_citation_count) / papers_processed)
    print "  Average Number of Citations (For Papers with Citations): %2.2f" % (
        float(total_citation_count) / papers_with_citations
    )

if __name__ == '__main__':
    should_profile = len(sys.argv) > 1
    if should_profile:
        cProfile.run('parse_intermediate_arnetminer_dataset()')
    else:
        parse_intermediate_arnetminer_dataset()
