"""Work out, from the source itself, which form fields a function reads.

patchTrip has to know every key update_trip_values_from_form_data and processDates
look up: those are the fields it must carry over from the stored trip, and anything
else in a payload is a typo worth reporting. Deriving that from the functions' own
AST keeps the two in step — a field added to the update pipeline becomes patchable
straight away, with no list here to remember to update.
"""

import ast
import inspect
import textwrap


def form_keys_read_by(*functions) -> set[str]:
    """Every form key the given functions read.

    Each function is assumed to reach into its form dict by literal key:
    ``form["k"]``, ``form.get("k")``, ``"k" in form``.
    """
    keys = set()
    for func in functions:
        keys |= _parse(func)[2]
    return keys


def literals_compared_with(func, key: str) -> set[str]:
    """The string literals the value of ``key`` is compared against — i.e. the
    values the function actually recognises, as opposed to whatever its final
    ``else`` treats as a catch-all."""
    tree, form_arg, _ = _parse(func)
    values = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or _key_of(node.left, form_arg) != key:
            continue
        for op, comparator in zip(node.ops, node.comparators):
            if isinstance(op, (ast.Eq, ast.NotEq)):
                values |= _as_str(comparator)
    return values


def _parse(func):
    """The function's AST, the name of the parameter carrying its form dict, and
    the keys read from it.

    The form dict is found rather than assumed to be first: it is the parameter
    that gets looked up by string key, which tells ``formData`` apart from the
    ``trip_id`` beside it whatever order the signature happens to put them in.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(func))).body[0]
    params = [a.arg for a in tree.args.args + tree.args.kwonlyargs]

    reads: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        for param in params:
            keys = _keys_read_by(node, param)
            if keys:
                reads.setdefault(param, set()).update(keys)
    if not reads:
        raise ValueError(f"{func.__name__} reads no form field by literal key")

    form_arg = max(reads, key=lambda param: len(reads[param]))
    return tree, form_arg, reads[form_arg]


def _as_str(node) -> set[str]:
    """The node as a one-element set when it is a string literal, else empty."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    return set()


def _is_form(node, form_arg) -> bool:
    """The form dict itself, or a view of it (``form.keys()``)."""
    if isinstance(node, ast.Name):
        return node.id == form_arg
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return _is_form(node.func.value, form_arg) and node.func.attr in (
            "keys",
            "items",
            "values",
        )
    return False


def _key_of(node, form_arg):
    """The key a ``form["k"]`` / ``form.get("k")`` expression looks up, or None."""
    if isinstance(node, ast.Subscript) and _is_form(node.value, form_arg):
        keys = _as_str(node.slice)
    elif (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and _is_form(node.func.value, form_arg)
        and node.args
    ):
        keys = _as_str(node.args[0])
    else:
        return None
    return next(iter(keys), None)


def _keys_read_by(node, form_arg) -> set[str]:
    key = _key_of(node, form_arg)
    if key is not None:
        return {key}
    # `"k" in form` / `"k" not in form.keys()` — a presence test is a read too.
    if isinstance(node, ast.Compare):
        for op, comparator in zip(node.ops, node.comparators):
            if isinstance(op, (ast.In, ast.NotIn)) and _is_form(comparator, form_arg):
                return _as_str(node.left)
    return set()
