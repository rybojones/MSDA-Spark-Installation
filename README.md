# Word-Pair Count

_word_pair_count.py_ is a python executable that reads in a text file and aggregates word-pair counts for words that appear in the same sentence.

## Installation

You need to have python version 3.8.5 installed, as well as pyspark version 3.0.0.

```bash
pip install python=3.8.5 pyspark=3.0.0
```
or, if using miniconda.

```bash
conda install python=3.8.5 pyspark=3.0.0
```
You can install pyspark using the detailed instructions provided in _Spark Installation Tutorial.docx_. This file is included in the .zip distribution.

## Usage

Navigate to the folder with the _word_pair_count.py_ script and type the following into the bash terminal (ensure that _input.txt_ is in the same location as _word_pair_count.py_.):

```bash
python word_pair_count.py
```

The script will run and print the top 25 aggregated most frequent word-pairs from each line. An output text file will be produced in the same location as _word_pair_count.py_, named _output.txt_. This file will contain the entire list of tuples - word-pair and count.

## Contributing
The following links contained helpful code that was adapted for the purposes of this program:
- https://stackoverflow.com/questions/22520932/python-remove-all-non-alphabet-chars-from-string/29350747
- https://stackoverflow.com/questions/1546226/is-there-a-simple-way-to-remove-multiple-spaces-in-a-string
- https://stackoverflow.com/questions/58465089/counting-all-possible-word-pairs-using-pyspark

## License
None at this time.
