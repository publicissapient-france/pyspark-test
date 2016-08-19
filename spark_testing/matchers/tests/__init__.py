
def assertion_error_message(expected, but):
    """
    Helper to check for hamcrest exception message

    :param expected: The expected part of the message
    :param but: The but part of the message
    :return: hamcrest formatted exception message
    """
    return '\nExpected: %s\n     but: %s\n' % (expected, but)
