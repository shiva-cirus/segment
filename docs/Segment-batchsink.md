# Segment batch sink

Description
-----------

Writes a record to Segment.

Use Case
--------

This source is used whenever you need to write to a FileSet in text format.

Properties
----------

**fileSetName:** The name of the FileSet to write to.

**fieldSeparator:** The separator to join input record fields on. Defaults to ','.

**outputDir:** The output directory to write to. Macro enabled.

Example
-------

This example writes to a FileSet named 'users', using the '|' character to separate record fields:

    {
        "name": "TextFileSet",
        "type": "batchsink",
        "properties": {
            "fileSetName": "users",
            "fieldSeparator": "|",
            "outputDir": "${outputDir}"
        }
    }

Before running the pipeline, the 'outputDir' runtime argument must be specified.
