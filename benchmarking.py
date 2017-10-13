import codecs, json, gzip, glob
from jsonpath_rw import jsonpath, parse


def get_info(gz_file):
    info = dict()
    info['timestamp_count'] = 0
    info['timestamp_difference_sum'] = long(0)
    info['timestamp_difference_sqrd_sum'] = long(0)
    info['non_timestamp_count'] = 0
    info['num_tweets'] = 0
    first_line = True
    with gzip.open(gz_file, 'rb') as f:
        try:
            for line in f:
                try:
                    obj = json.loads(line[0:-1])
                    info['num_tweets'] += 1
                    if first_line is True:
                        timestamp_old = long(obj['timestamp_ms']) # has to be there
                        first_line = False
                        continue
                    # user_id = str(obj['user']['id'])
                    if 'timestamp_ms' in obj:
                        info['timestamp_difference_sum'] += (long(obj['timestamp_ms'])-timestamp_old)
                        diff_sqrd = (long(obj['timestamp_ms'])-timestamp_old)*(long(obj['timestamp_ms'])-timestamp_old)
                        timestamp_old = long(obj['timestamp_ms'])
                        info['timestamp_difference_sqrd_sum'] += diff_sqrd
                        info['timestamp_count'] += 1
                    else:
                        info['non_timestamp_count'] += 1

                    # print count
                    # count += 1
                    # if count > 10:
                    #     break
                except:
                    print 'problem in line: ', line
                    continue
        except Exception as e:
            print e
            print 'file ', gz_file, ' is corrupted...breaking, regard info with caution.'
    return info


def construct_bi_network_timestamp_analysis(gz_file, hashtag_network, reply_network):
    """
    We will construct two networks in the form of tab delimited adjacency lists
    the first network will contain a user id as each element of the first line, and every other element will be
    the id of another user that has been mentioned in the post
    the second network will contain a user id in the first element, and hashtags as remaining elements
    This way, we have the ability to join the two networks

    """
    jpath_users = parse('entities.user_mentions[*].id')
    jpath_hashtags = parse('entities.hashtags[*].text')
    out_hashtag = codecs.open(hashtag_network, 'w', 'utf-8')
    out_reply = codecs.open(reply_network, 'w', 'utf-8')
    count = 0
    info = dict()
    info['timestamp_count'] = 0
    info['timestamp_sum'] = long(0)
    info['timestamp_sqrd_sum'] = long(0)
    info['non_timestamp_count'] = 0
    info['num_tweets'] = 0
    with gzip.open(gz_file, 'rb') as f:
        try:
          for line in f:
            try:
                obj = json.loads(line[0:-1])
                info['num_tweets'] += 1 # be careful, this timestamp info. is not what you might be looking for (instead, see get_info)
                user_id = str(obj['user']['id'])
                if 'timestamp_ms' in obj:
                    info['timestamp_sum'] += long(obj['timestamp_ms'])
                    info['timestamp_sqrd_sum'] += (long(obj['timestamp_ms'])*long(obj['timestamp_ms']))
                    info['timestamp_count'] += 1
                else:
                    info['non_timestamp_count'] += 1
                reply_users = list(set([str(match.value) for match in jpath_users.find(obj)]))
                if reply_users:
                    out_reply.write('\t'.join([user_id]+reply_users))
                    out_reply.write('\n')
                hashtags = list(set([match.value for match in jpath_hashtags.find(obj)]))
                if hashtags:
                    out_hashtag.write('\t'.join([user_id]+hashtags))
                    out_hashtag.write('\n')
                print count
                count += 1
                # if count > 10:
                #     break
            except:
                print 'problem in line: ',line
                continue
        except Exception as e:
            print e
            print 'file ',gz_file,' is corrupted...breaking, regard info with caution.'
    out_reply.close()
    out_hashtag.close()
    return info


def construct_bi_networks_with_info(gz_folder, info_file):
    listOfFiles = glob.glob(gz_folder + '*.json.gz')
    file_info = dict()
    for fi in listOfFiles:
        print 'processing input file: ',fi
        hashtag_file = fi.replace('json.gz', 'hashtag.tsv')
        reply_file = fi.replace('json.gz', 'reply.tsv')
        print 'output hashtag file: ', hashtag_file
        print 'output reply file: ', reply_file

        info = construct_bi_network_timestamp_analysis(fi, hashtag_file, reply_file)
        file_info[fi]=info
    out = codecs.open(info_file, 'w', 'utf-8')
    json.dump(file_info, out)
    out.close()


def print_info_files(gz_folder):
    listOfFiles = glob.glob(gz_folder + '*.json.gz')
    out = codecs.open(gz_folder+'info.jl', 'w', 'utf-8')
    for fi in listOfFiles:
        print 'processing input file: ',fi
        info = get_info(fi)
        json.dump(info, out)
        out.write('\n')
    out.close()


# path = '/Users/mayankkejriwal/datasets/twitter-hose/twitter-decahose-ALL-2016-12-31/'
# print_info_files(path)
# construct_bi_networks_with_info(path, path+'timestamp_ms_data.json')
# construct_bi_network_timestamp_analysis(path+'twitter-decahose-2016-12-31-15.json.gz', path+'twitter-decahose-2016-12-31-15-hashtag-new.tsv',
#                      path + 'twitter-decahose-2016-12-31-15-reply-new.tsv')
# l = path+'twitter-decahose-2016-12-31-00.json.gz'
# k = l.replace('json.gz', 'hashtag.tsv')
# print k