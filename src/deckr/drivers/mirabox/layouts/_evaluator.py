from __future__ import annotations

import ast
import re
from simpleeval import SimpleEval, FeatureNotAvailable

# -----------------------------
# Errors
# -----------------------------


class PolicyError(ValueError):
    pass


# -----------------------------
# Safety limits
# -----------------------------

MAX_EXPR_LEN = 500
MAX_AST_NODES = 200
MAX_REGEX_LEN = 120


# -----------------------------
# Regex helpers (whitelisted)
# -----------------------------


def _check_pattern(p: str) -> None:
    if not isinstance(p, str):
        raise PolicyError("Regex pattern must be a string.")
    if len(p) > MAX_REGEX_LEN:
        raise PolicyError("Regex pattern too long.")


def match(pattern: str, value: object) -> bool:
    _check_pattern(pattern)
    return bool(re.fullmatch(pattern, str(value)))


def search(pattern: str, value: object) -> bool:
    _check_pattern(pattern)
    return bool(re.search(pattern, str(value)))


# -----------------------------
# Expression guards
# -----------------------------


def _reject_single_equals(expr: str) -> None:
    # Reject accidental assignment-like syntax
    if re.search(r"(?<![=!<>])=(?![=])", expr):
        raise PolicyError("Use '==' for equality; assignment '=' is not allowed.")


def _enforce_expr_limits(expr: str) -> None:
    if len(expr) > MAX_EXPR_LEN:
        raise PolicyError(f"Expression too long (max {MAX_EXPR_LEN} chars).")

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise PolicyError(f"Invalid syntax: {e.msg}") from e

    nodes = sum(1 for _ in ast.walk(tree))
    if nodes > MAX_AST_NODES:
        raise PolicyError("Expression too complex.")


# -----------------------------
# Evaluator factory
# -----------------------------


def make_policy_evaluator(context: dict[str, object]) -> SimpleEval:
    s = SimpleEval(names=context)

    # Only allow explicitly whitelisted helpers
    s.functions = {
        "match": match,  # full regex match
        "search": search,  # regex search / contains
    }

    # Optional but often sensible:
    # forbid attribute access entirely
    s.attributes = {}

    return s


# -----------------------------
# Public API
# -----------------------------


def eval_policy(expr: str, context: dict[str, object]) -> bool:
    _reject_single_equals(expr)
    _enforce_expr_limits(expr)

    evaluator = make_policy_evaluator(context)

    try:
        result = evaluator.eval(expr)
    except FeatureNotAvailable as e:
        raise PolicyError(f"Disallowed feature: {e}") from e
    except Exception as e:
        raise PolicyError(f"Policy evaluation failed: {e}") from e

    if not isinstance(result, bool):
        raise PolicyError("Policy must evaluate to true or false.")

    return result
