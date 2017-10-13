import codecs, json, gzip
import tarfile

def analyze_tar():
    path = '/Volumes/isicvlnas01/projects/safe/data/twitter/tweet-data/raw-decahose/2016/09/'
    tar = tarfile.open(path+"twitter-decahose-ALL-2016-09-01.tar")
    member = tar.getmembers()[1]
    # gz_handler = tar.extractfile(member)


def read_in_hashtags(gz_file=None):
    count = 1
    hashtags = list()
    if gz_file is None:
        path = '/Users/mayankkejriwal/datasets/twitter-hose/twitter-decahose-ALL-2016-12-31/'
        gz_file = path+'twitter-decahose-2016-12-31-01.json.gz'
    with gzip.open(gz_file, 'r', 'utf-8') as f:
        for line in f:
            try:
                obj = json.loads(line)
                try:

                    hashtag_objects = obj['entities']['hashtags']
                    for o in hashtag_objects:
                        hashtags.append(o['text'])
                    # print hashtags
                    count += 1
                    if count % 1000 == 0:
                        hashtags = list(set(hashtags))
                        print len(hashtags)
                except:
                    print 'exception!'
                    count += 1
                    continue
            except:
                print 'somethings wrong reading in json'
                print line
                continue


# analyze_tar()
# read_in_hashtags()