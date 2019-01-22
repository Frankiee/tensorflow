from gutenberg.acquire import load_etext
import os
import re

from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers

# ########################################################
# ### Specify Google Cloud Platform project and bucket ###
# ########################################################
PROJECT = 'qwiklabs-gcp-dfc6cc9248653b44'
BUCKET = 'qwiklabs-gcp-dfc6cc9248653b44'
REGION = 'us-central1'

# this is what this notebook is demonstrating
PROBLEM = 'poetry_line_problem'

# for bash
os.environ['PROJECT'] = PROJECT
os.environ['BUCKET'] = BUCKET
os.environ['REGION'] = REGION
os.environ['PROBLEM'] = PROBLEM

# #####################
# ### Download data ###
# #####################
# We will get some poetry anthologies from Project Gutenberg.

books = [
    # bookid, skip N lines
    (26715, 1000, 'Victorian songs'),
    (30235, 580, 'Baldwin collection'),
    (35402, 710, 'Swinburne collection'),
    (574, 15, 'Blake'),
    (1304, 172, 'Bulchevys collection'),
    (19221, 223, 'Palgrave-Pearse collection'),
    (15553, 522, 'Knowles collection')
]

with open('poetry_raw.txt', 'w') as ofp:
    lineno = 0
    for (id_nr, toskip, title) in books:
        startline = lineno
        text = strip_headers(load_etext(id_nr)).strip()
        lines = text.split('\n')[toskip:]
        # any line that is all upper case is a title or author name
        # also don't want any lines with years (numbers)
        for line in lines:
            if (len(line) > 0 and line.upper() != line and
                    not re.match('.*[0-9]+.*', line) and len(line) < 50):
                cleaned = re.sub('[^a-z\'\-]+', ' ', line.strip().lower())
                ofp.write(cleaned)
                ofp.write('\n')
                lineno = lineno + 1
            else:
                ofp.write('\n')
        print('Wrote lines {} to {} from {}'.format(startline, lineno, title))


# ###############################
# ### Create training dataset ###
# ###############################
# We are going to train a machine learning model to write poetry given a
# starting point. We'll give it one line, and it is going to tell us the
# next line. So, naturally, we will train it on real poetry. Our feature
# will be a line of a poem and the label will be next line of that poem.
#
# Our training dataset will consist of two files. The first file will
# consist of the input lines of poetry and the other file will consist of
# the corresponding output lines, one output line per input line.
with open('poetry_raw.txt', 'r') as rawfp, open(
        'poetry_input.txt', 'w') as infp, open(
        'poetry_output.txt', 'w') as outfp:
    prev_line = ''
    for curr_line in rawfp:
        curr_line = curr_line.strip()
        # poems break at empty lines, so this ensures we train only
        # on lines of the same poem
        if len(prev_line) > 0 and len(curr_line) > 0:
            infp.write(prev_line + '\n')
            outfp.write(curr_line + '\n')
        prev_line = curr_line
