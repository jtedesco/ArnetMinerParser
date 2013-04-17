hese directories contain the full XML data of Arnetminer.

    data/

        Contains 1049 individual zip files, each containing roughly 1000 documents
        in XML format. This is the original data from Arnetminer.org, as of March 13, 2013.

    parser/

        Contains Python code for a distributed parser for the raw Arnetminer XML data. This
        parser happens in two phases:

            (1) a distributed parser (using 3 slave processes) that sanitizes the raw data
                and outputs it into an intermediate format
            (2) a single-threaded parser that does several final passes to error check and
                combine intermediate results

    arnetminer_full.txt.bz2

        Contains the final, sanitized result of parsing the raw data. This is in the the
        same format as the Arnetminer V5 ctiation dataset, except that only one index
        is found for each document:

            http://arnetminer.org/arnetpage-detail?id=279

        Within this archive, you will find one plain text file. Each line begins with an
        identifier for the data found on that line, as is described at the above website:

            #* --- paperTitle
            #@ --- Authors
            #year --- Year
            #conf --- publication venue
            #citation --- number of citations for this paper
            #index --- index id of this paper
            #path --- the path to the XML paper in the original data
            #% --- the id of references of this paper (there are multiple lines, with each indicating a reference)

        For example:

            #*Spatial Data Structures
            #@Hanan Samet
            #year1995
            #confModern Database Systems
            #citation4
            #index25
            #path10-001.ZIP:0123/123.xml
            #%165
            #%464
            #%331
            #%963

This data was provided under a special agreement with Tsinghua University, on the condition that it does not leave our research group.

Please, DO NOT DISTRIBUTE.

Thank you,
Jon Tedesco (tedesco1@illinois.edu)
