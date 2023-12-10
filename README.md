# LexDanNet
Enity linking CLI-application to help link DanNet 2.2 word ID with Danish Wikidata lexemes.

![image](https://github.com/dpriskorn/LexDanNet/assets/68460690/0863e927-e00f-4825-b963-59222278b003)

## Features
* Fetching DanNet
* Extracting data we need
* Downloading lexemes missing DanNet 2.2. ID
* Matches using both lemma and lexical category

### TODO
* extraction of DDO id's
* linking senses also
* add "missing in" statements when lemma is not found in DanNet

## Use
`python main.py`

## What I learned
* chatGPT needs a lot of handholding when it comes to WikibaseIntegrator. It produces garbage.
* Small projects like this still takes hours to do which is rather surprising, we need better/simpler infrastructure
* WikibaseIntegrator has no helper method to check if it has logged in or not https://github.com/LeMyst/WikibaseIntegrator/issues/638
