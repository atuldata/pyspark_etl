import itertools
import datetime

def generate_split(bdate, edate, interval):
    """splits [bdate,edate) at the given intervals"""
    start = bdate
    while start < edate:
        yield start
        start += interval
    yield edate


def split_points(begin, end, interval):
    """split, as list of points"""
    return [x for x in generate_split(begin, end, interval)]


def split_pairs(begin, end, interval):
    """split, as list of pairs"""
    return [x for x in pairwise(generate_split(begin,end,interval))]


# from itertool recipes:
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None) # advance b by one
    return itertools.izip(a, b) # exhausted at shortest element


def test_split():
    start = datetime.datetime(2015,10,1)
    end = datetime.datetime(2015,10,11)
    return split_pairs(start, end, datetime.timedelta(days=3))
