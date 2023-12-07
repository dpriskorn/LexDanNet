# LexDanNet
**Note: until improved to match on lexical category also (see https://github.com/dpriskorn/LexDanNet/issues/2) it should be used with extreme caution**

Script to extract and add DanNet 2.2 word ID to Danish Wikidata lexemes

## Features
* Fetching DanNet
* Extracting data we need
* Downloading lexemes
* Matching using the lemmas

TODO:
* Match using both lemma and lexical category

## Use
`python main.py`

## What I learned
* chatGPT needs a lot of handholding when it comes to WikibaseIntegrator. It produces garbage.
* Small projects like this still takes hours to do which is rather surprising, we need better/simpler infrastructure
* WikibaseIntegrator has no helper method to check if it has logged in or not https://github.com/LeMyst/WikibaseIntegrator/issues/638