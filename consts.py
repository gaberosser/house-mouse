__author__ = 'gabriel'


def postcode_area(letters, max_number, exclude=None):
    """
    Generate all the postcode regions for a given area
    :param letters:
    :param max_number:
    :return:
    """
    if exclude is None:
        return ["%s%d" % (letters, i) for i in range(max_number + 1)]
    else:
        if not hasattr(exclude, '__iter__'):
            exclude = [exclude]
        return ["%s%d" % (letters, i) for i in range(max_number + 1) if i not in exclude]


BR = postcode_area('BR', 8, exclude=0)
CR = postcode_area('CR', 9, exclude=1)
SE = postcode_area('SE', 28, exclude=0)
SW = postcode_area('SW', 20, exclude=0)
E = postcode_area('E', 20, exclude=[0, 19])
EC = postcode_area('EC', 4, exclude=0)
N = postcode_area('N', 22, exclude=0)
NW = postcode_area('NW', 11, exclude=0)
W = postcode_area('W', 14, exclude=0)

INNER_LONDON = (
    SE,
    SW,
    E,
    EC,
    N,
    NW,
    W
)