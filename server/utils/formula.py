import ast
import logging
import operator
from functools import reduce
from typing import Any
from typing import Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
MAX_FORMULA_LENGTH = 255


def byte_offset_to_char_offset(source: str, byte_offset: int) -> int:
    pre_source = ""
    while True:
        try:
            # Cut out the bytes before the offset and try to decode it.
            pre_source = source.encode()[:byte_offset].decode()
            break
        except UnicodeDecodeError:
            # Decoding failed, move back 1 byte.
            byte_offset -= 1
            continue
    # Decoding succeeded, count the characters.
    return len(pre_source)


class FormulaError(Exception):
    pass


class FormulaSyntaxError(FormulaError):
    def __init__(self, msg: str, lineno: int, offset: int):
        self.msg = msg
        self.lineno = lineno
        self.offset = offset

    @classmethod
    def from_ast_node(
        cls,
        source: str,
        node: ast.AST,
        msg: str,
    ) -> "FormulaSyntaxError":
        lineno = node.lineno
        col_offset = node.col_offset
        offset = byte_offset_to_char_offset(source, col_offset)
        return cls(msg=msg, lineno=lineno, offset=offset + 1)

    @classmethod
    def from_syntax_error(cls, error: SyntaxError, msg: str) -> "FormulaSyntaxError":
        return cls(msg=f"{msg}: {error.msg}", lineno=error.lineno, offset=error.offset)

    def __str__(self):
        return f"{self.lineno}:{self.offset}: {self.msg}"


class FormulaRuntimeError(FormulaError):
    pass


def eval_node(source: str, node: ast.AST, params: Dict[str, Any]) -> float:
    evaluators = {
        ast.Expression: eval_expression,
        ast.Constant: eval_constant,
        ast.Name: eval_name,
        ast.BinOp: eval_binop,
        ast.UnaryOp: eval_unaryop,
        ast.BoolOp: eval_boolop,
    }

    for ast_type, evaluator in evaluators.items():
        if isinstance(node, ast_type):
            return evaluator(source, node, params)

    raise FormulaSyntaxError.from_ast_node(source, node, "This syntax is not supported")


def eval_expression(source: str, node: ast.Expression, params: Dict[str, Any]) -> float:
    return eval_node(source, node.body, params)


def eval_constant(source: str, node: ast.Constant, _params: Dict[str, Any]) -> float:
    if isinstance(node.value, int) or isinstance(node.value, float):
        return float(node.value)
    else:
        raise FormulaSyntaxError.from_ast_node(
            source,
            node,
            "Literals of this type are not supported",
        )


def eval_name(source: str, node: ast.Name, params: Dict[str, Any]) -> float or bool:
    try:
        if isinstance(params[node.id], bool):
            return bool(params[node.id])
        elif isinstance(params[node.id], str) and params[node.id] in ["Yes", "No"]:
            return True if params[node.id] == "Yes" else False

        return float(params[node.id])
    except KeyError:
        raise FormulaSyntaxError.from_ast_node(
            source,
            node,
            f"Undefined variable: {node.id}",
        )


def eval_binop(source: str, node: ast.BinOp, params: Dict[str, Any]) -> float:
    operations = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
    }

    left_value = eval_node(source, node.left, params)
    right_value = eval_node(source, node.right, params)

    try:
        apply = operations[type(node.op)]
    except KeyError:
        raise FormulaSyntaxError.from_ast_node(
            source,
            node,
            "Operations of this type are not supported",
        )

    return apply(left_value, right_value)


def eval_unaryop(
    source: str,
    node: ast.UnaryOp,
    params: Dict[str, Any],
) -> float:
    operations = {
        ast.USub: operator.neg,
        ast.Not: operator.not_,
    }

    operand_value = eval_node(source, node.operand, params)

    try:
        apply = operations[type(node.op)]
    except KeyError:
        raise FormulaSyntaxError.from_ast_node(
            source,
            node,
            "Operations of this type are not supported",
        )

    return apply(operand_value)


def eval_boolop(source: str, node: ast.BoolOp, params: Dict[str, Any]) -> float:
    operations = {
        ast.And: operator.and_,
        ast.Or: operator.or_,
    }
    values = [eval_node(source, val, params) for val in node.values]

    try:
        apply = operations[type(node.op)]
    except KeyError:
        raise FormulaSyntaxError.from_ast_node(
            source,
            node,
            "Operations of this type are not supported",
        )

    return reduce(apply, values)


def validate_formula(
    formula: str,
) -> (bool, str):
    logger.info(f"Validating formula: {formula}")

    if not formula or len(formula) > MAX_FORMULA_LENGTH:
        return False, "Empty formula" if not formula else "formula string is too long"
    try:
        ast.parse(formula, "<string>", mode="eval")
    except SyntaxError as e:
        err_msg = f"{e.lineno}:{e.offset}: Could not parse: {e.msg}"
        return False, err_msg

    return True, ""


def evaluate_formula(formula: str, params: Dict[str, Any]):
    logger.info(f"Evaluating formula: {formula} --- {params}")

    try:
        node = ast.parse(formula, "<string>", mode="eval")
    except SyntaxError as e:
        raise FormulaSyntaxError.from_syntax_error(e, "Could not parse")

    try:
        return eval_node(formula, node, params)
    except FormulaSyntaxError:
        raise
    except Exception as e:
        raise FormulaRuntimeError(f"Evaluation failed: {e}")
