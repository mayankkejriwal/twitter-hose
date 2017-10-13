from json import loads
from hatebase import HatebaseAPI
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as VS

# hatebase = HatebaseAPI({"key": key})
# filters = {'about_nationality': '1', 'language': 'eng'}
# output = "json"
# query_type = "sightings"
# response = hatebase.performRequest(filters, output, query_type)
#
# # convert to Python object
# response = loads(response)
# print response
sentences = ["VADER is full of crap.",      # positive sentence example
            "VADER is not smart, handsome, nor funny.",   # negation sentence example
            "VADER is smart, handsome, and funny!",       # punctuation emphasis handled correctly (sentiment intensity adjusted)
            "VADER is very smart, handsome, and funny."]
analyzer = VS()
for sentence in sentences:
    vs = analyzer.polarity_scores(sentence)
    # print("{:-<65} {}".format(sentence, str(vs)))
    print sentence, vs