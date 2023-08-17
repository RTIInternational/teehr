"""
Library of code to ingest NGEN outputs to the TEEHR parquet data model format.

This code would basically just be a *.csv parser the write to parquet files.
Given how variable the ngen output can be at this point, it is not worth
writing robust converter code.  A few helper functions below.

See also examples/loading/ngen_to_parquet.ipynb
"""
# import json
# import re

# def get_forcing_file_pattern(realization_filepath):
#     with open(realization_filepath) as f:
#         j = json.load(f)

#     file_pattern = j["global"]["forcing"].get("file_pattern", None)
#     path = j["global"]["forcing"].get("path", None)


# def glob_re(pattern, strings):
#     return filter(re.compile(pattern).match, strings)
pass
