ArnetMinerParser
================

Efficient Python parser for the full Arnetminer dataset.

Dependencies:

  - Python 2.7 or greater

Contains Python code for a distributed parser for the raw Arnetminer XML data. This
parser happens in two phases:

  1. A distributed parser (using 4 slave processes) that sanitizes the raw data
    and outputs it into an intermediate format
  2. A single-threaded parser that does several final passes to error check and
     combine intermediate results

These directories also include utilities to help debug and process the original data.

Dataset
=======

The full Arnetminer data is assumed to be included in the working directory of the parser, under:

    data/

This directory is assumbed to contain 1049 individual zip files, each containing roughly 1000 documents
in XML format. This is the original data from Arnetminer.org, as of March 13, 2013.

The final output of the format follows a simple format that parallels that of the publicly available datasets.
Each line begins with an identifier for the data found on that line, as is described [here](http://arnetminer.org/arnetpage-detail?id=279):

    #* --- paperTitle
    #@ --- Authors
    #year ---- Year
    #conf --- publication venue
    #index ---- index id of this paper
    #% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)

For example:

    #*Spatial Data Structures.
    #@Hanan Samet
    #year1995
    #confModern Database Systems
    #index25
    #%165
    #%464
    #%331
    #%963