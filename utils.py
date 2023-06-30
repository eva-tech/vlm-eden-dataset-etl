"""This file contains utility functions that are used in the application."""
import re


def get_schema_name(organization_slug):
    """Get the schema name from the organization slug.

    :param organization_slug:
    :return: strung
    """
    return re.sub("[^A-Za-z0-9]+", "", organization_slug)


def first_true(iterable, default=None, pred=None):
    """Return the first true value in the iterable. If no true value is found, returns *default* If *pred* is not None."""
    return next(filter(pred, iterable), default)


def combine_and_sort_dictionary_values(data, tmp_data, vals):
    """Combine and sort dictionary values. If the value is not present in the dictionary, it will be ignored.

    :param data:
    :param tmp_data:
    :param vals:
    :return: string
    """
    a = [data[x] for x in vals]
    b = [tmp_data[x] for x in vals]

    return ",".join(sorted(x for x in [first_true(a), first_true(b)] if x))
