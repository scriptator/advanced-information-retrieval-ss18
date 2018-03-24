
segment_keys = ['a', 'f', 'k', 'p', 'u', 'z', 'o']


def segment_key(doc_token):
    t = doc_token[1][0].lower()
    for segment_highest_key in segment_keys:
        if t <= segment_highest_key:
            return segment_highest_key

    return segment_keys[-1]
