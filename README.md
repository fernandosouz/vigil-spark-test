# Odd Occurrence

This program processes a set of input files, located in a directory, and writes the result to a new file in TSV format in the specified output directory.

## Requirements

- Spark 2.x
- Scala 2.x
- Hadoop 3.x

## Usage

To run the program, use the following command:

spark-submit --class Main --master local[*] [path-to-jar-file] [input-directory] [output-directory]

where:
- `[path-to-jar-file]` is the path to the compiled jar file of the program
- `[input-directory]` is the path to the directory that contains the input files
- `[output-directory]` is the path to the directory where the output file will be written

## Input files

The input files are located in the specified input directory, and they have the following characteristics:

- They contain headers, the column names are random strings, and they are not consistent across files
- Both columns are integer values
- Some files are CSV, some are TSV

## Output file

The program writes the result to a new file in TSV format in the specified output directory, with the following characteristics:

- The first column contains each key exactly once
- The second column contains the integer occurring an odd number of times for the key

## Implementation details

The program implements the following steps:

1. Read the input files from the specified input directory
2. Transform the data into the desired format, by grouping the data by key and value and aggregating the count of values
and filtering odd occurrence values
3. Write the result to a new file in TSV format in the specified output directory

The program uses Spark for data processing, and Hadoop for file system operations.