import re


def get_schema_name(organization_slug):
    return re.sub('[^A-Za-z0-9]+', '', organization_slug)

def first_true(iterable, default=None, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)

def combine_and_sort_dictionary_values(data, tmp_data, vals):
    """
    Helps combine strings from two dictionaries checking through the keys in vals.
    data: Dictionary with values
    tmp_data: Dictionary to store temp data.
    vals: List with keys for both data and base dictionaries
    return: String with a new value
    """
    a = [data[x] for x in vals]
    b = [tmp_data[x] for x in vals]

    return (",".join(sorted(x for x in
        [first_true(a), first_true(b)] if x)))
