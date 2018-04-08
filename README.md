# Advanced Information Retrieval Summer Term 2018

## Installation

You need a standard python3 distribution and pip3. Install additional
dependencies by executing

    ./install.sh

or using the file `requirements.txt` directly.


## Scripts

In all subsequent commands we assume your current working directory is the repository root.

### air18.index

When having successfully installed the dependencies, we can start with indexing.
To get an overview over the command line options execute

    python3 -m air18.index -h

You can either parse the original TREC XML-like files or our own prepared JSON files.

The index will be created in a directory `~/.air18/index`. This directory is cleared on every startup of the indexing script.

### air18.search

After successfully having run the indexing, you can start a search via

    python3 -m air18.search {tf-idf | bm25 | bm25va}

Per default, all topics are evaluated and up to 1000 documents are returned in TREC format.
Again, for an overview of the command line options, pass the `-h` option.


### evaluate.sh

After successfully having run the indexing, you can start automatic evaluation via

    ./evaluate.sh
