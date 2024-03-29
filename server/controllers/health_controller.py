from flask import make_response


def health_check():  # noqa: E501
    """Health check

     # noqa: E501


    :rtype: str
    """
    return make_response('Healthy!', 200)
