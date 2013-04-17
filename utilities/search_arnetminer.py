import sys
from collections import defaultdict
from Stemmer import Stemmer

confs = [
    'POWER',
    'AAAI',
    'CIKM',
    'CVPR',
    'ECIR',
    'ECML',
    'EDBT',
    'ICDE',
    'ICDM',
    'ICML',
    'IJCAI',
    'KDD',
    'PAKDD',
    'PKDD',
    'PODS',
    'SDM',
    'SIGIR',
    'SIGMOD',
    'VLDB',
    'WSDM',
    'WWW'
]

keywords = [
    'proceeding',
    'proceedings',

    'AAAI',
    'artificial',
    'intelligence',

    'CIKM',
    'information',
    'knowledge',

    'CVPR',
    'computer',

    'ECIR',
    'retrieval',

    'ECML',
    'machine',
    'learning',

    'EDBT',
    'database',

    'ICDE',
    'data',

    'ICDM',
    'data',
    'mining',

    'ICML',
    'machine',
    'learning',

    'IJCAI',
    'artificial',
    'intelligence',

    'KDD',
    'knowledge',
    'discovery',
    'database',

    'PAKDD',
    'knowledge',
    'discovery',
    'data',
    'mining',

    'PKDD',
    'knowledge',
    'discovery',

    'PODS',
    'database',

    'SDM',
    'SIAM',
    'data',
    'mining',

    'SIGIR',
    'information',
    'retreival',

    'SIGMOD',
    'data',

    'VLDB',
    'very',
    'large',
    'database',

    'WSDM',
    'web',
    'search',
    'data',
    'mining',

    'WWW',
    'web'
]
stemmer = Stemmer('english')
new_keywords = set()
for keyword in keywords:
    new_keywords.add(stemmer.stemWord(keyword.lower()))
keywords = new_keywords

conferences = defaultdict(int)


def matches_keywords(title):
    title_keywords = set([stemmer.stemWord(w) for w in title.lower().split()])
    return len(title_keywords.intersection(keywords))


def matches_confs(conf):
    return any([conf.strip() in real_conf for real_conf in confs])

citations_found = 0
with open('arnetminer_full.txt') as f:
    docs_found = 0
    total_docs = 8409380
    for line in f:
        if line.strip().startswith('#conf'):
            docs_found += 1
            conf = line[len('#conf'):].strip().lower()
            if matches_confs(conf):
                conferences[conf] += 1
        elif line.strip().startswith('#%'):
            citations_found += 1
        if docs_found % 20 == 0:
            sys.stdout.write('\rProcessed %d (%2.2f%%)' % (docs_found, 100 * float(docs_found) / total_docs))

print "\n"
for key in sorted(conferences.keys()):
    print "\t%s: %d" % (key, conferences[key])

print "Citations Found: %d" % citations_found
print "Docs Found: %d" % docs_found
