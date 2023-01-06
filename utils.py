import re


def get_schema_name(organization_slug):
    return re.sub('[^A-Za-z0-9]+', '', organization_slug)
