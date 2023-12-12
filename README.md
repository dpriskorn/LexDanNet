# LexDanNet
Enity linking CLI-application to help link DanNet 2.2 word ID with Danish Wikidata lexemes.

![image](https://github.com/dpriskorn/LexDanNet/assets/68460690/0863e927-e00f-4825-b963-59222278b003)

## Features
* Fetching DanNet
* Extracting data we need
* Downloading lexemes missing DanNet 2.2. ID
* Matches using both lemma and lexical category
* add "missing in" statements when lemma is not found in DanNet

### TODO
* extraction of DDO id's
* linking senses also
* extract sentiment and add it either to sense or lexemes in Wikidata (discussion needed)

## Use
`python main.py`

## What I learned
* chatGPT needs a lot of handholding when it comes to WikibaseIntegrator. It produces garbage.
* WikibaseIntegrator has no helper method to check if it has logged in or not https://github.com/LeMyst/WikibaseIntegrator/issues/638
* DanNet is not one thing. They have many different versions. More programming is needed to support the newer ttl-based versions. 

## Value estimation of this project and the finished linking
Entity linking and improving the link between disparate resources describing the language is very valuable to a society.
These links can be used to help enrich the data in Wikidata and in DanNet in this case. 
E.g. this application has unearthed that a lot of lexemes are currently missing in DanNet and currently perhaps not documented anywhere else than in Wikidata.
A scientist capable of linking datasets and producing valuable output costs about 0,7-1 mio SEK a year to hire.
Recently Swedish scientists with ML but no RDF or linked data competence got 5 mio. to work on the (messy) Riksdagen dataset.
Based on these observations I, Dennis Priskorn, estimate that the value of this project is around 500.000 SEK once completed.
The code alone is estimated to be worth at least 50.000 SEK or about a monthly wage of a senior Python developer in Sweden.
