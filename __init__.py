import ast


# TODO: this functionality is copied from mongo_base_block,
# consider unifying
def evaluate_expression(expression, signal, force_dict=True):
    """ Evaluates an expression against a signal.

    This method will allow the expression to evaluate to a dictionary or
    a string representing a dictionary. In either case, a dictionary will
    be returned. If both of those fail, the value of force_dict determines
    whether or not the expression can be returned.

    Params:
        expression (expression): The ExpressionProperty reference
        signal (Signal): The signal to use to evaluate the expression
        force_dict (bool): Whether or not the expression has to evaluate
            to a dictionary

    Returns:
        result: The result of the expression evaluated with the signal

    Raises:
        TypeError: If force_dict is True and the expression is not a dict
    """
    exp_result = expression(signal)
    if exp_result:
        if not isinstance(exp_result, dict):
            try:
                # Let's at least try to make it a dict first
                exp_result = ast.literal_eval(exp_result)
            except Exception as e:
                # Didn't work, this may or may not be a problem, we'll find out
                # in the next block of code
                pass

        if not isinstance(exp_result, dict):
            # Ok, this is still not a dict, what should we do?
            if force_dict:
                raise TypeError("Expression needs to eval to a dict: "
                                "{}".format(expression))

    return exp_result
