import collections
import inspect
from inspect import Parameter
from typing import Any, Callable, Dict, Optional, OrderedDict, Set, Tuple, Type

import attrs


@attrs.define(frozen=True)
class FunctionSignature:
    """
    Helper class to inspect a function' parameter and return types.

    In Python 3.8+ whether an argument is positional only or keyword only can be specified using the / and * syntax,
    respectively. As an example:

    def f(pos1, pos2, /, pos_or_kwd, *, kwd1, kwd2):
      -----------    ----------     ----------
      |              |              |
      |              Positional or  |
      |              keyword        Keyword only
      Positional only

    1. Everything before / is positional only;
    2. Everything after * is keyword only.
    3. Note that order matters – / must come before *.
    4. If you don't explicitly specify positional or keyword only through the syntax,
        all arguments are of the positional or keyword kind.
    """

    args: OrderedDict[str, inspect.Parameter] = attrs.field()
    kwargs: Dict[str, inspect.Parameter] = attrs.field()

    has_var_arg: bool = attrs.field()
    has_var_kwarg: bool = attrs.field()

    return_type: Optional[Type] = attrs.field()

    @classmethod
    def from_function(cls, function: Callable) -> "FunctionSignature":
        signature = inspect.signature(function)

        if signature.return_annotation not in (inspect.Signature.empty, None):
            return_type = signature.return_annotation
        else:
            return_type = None

        parameters = list(signature.parameters.values())

        args = collections.OrderedDict(
            (p.name, p) for p in parameters if p.kind in [Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY]
        )
        kwargs = {p.name: p for p in parameters if p.kind in [Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY]}

        has_var_arg = any(p.kind == Parameter.VAR_POSITIONAL for p in parameters)
        has_var_kwarg = any(p.kind == Parameter.VAR_KEYWORD for p in parameters)

        return cls(
            args=args, kwargs=kwargs, has_var_arg=has_var_arg, has_var_kwarg=has_var_kwarg, return_type=return_type
        )

    def assign(self, args, kwargs) -> "NamedArguments":
        """
        Categorizes and names the ``args`` and ``kwargs`` arguments based on the function signature.

        Raise an exception if the arguments do not match the function's signature.

        :returns the assigned positional, keyword and variable parameters.
        """

        # Assigns positional arguments.

        named_args = collections.OrderedDict(
            (arg_type.name, arg_value) for arg_type, arg_value in zip(self.args.values(), args)
        )

        if len(args) > len(self.args):
            if self.has_var_arg:
                var_args = tuple(args[len(named_args) :])
            else:
                raise ValueError(f"expected {len(self.args)} arguments, got {len(args)}.")
        else:
            unassigned_args = [
                a
                for a in list(self.args.values())[len(args) :]
                if a.kind == Parameter.POSITIONAL_ONLY and a.default == Parameter.empty
            ]
            if len(unassigned_args) > 0:
                unassigned_kwarg_names = ", ".join(a.name for a in unassigned_args)
                raise ValueError(f"unassigned positional parameter(s): {unassigned_kwarg_names}.")

            var_args = tuple()

        # Assign keyword arguments.

        double_assigned_args = [a for a in kwargs.keys() if a in named_args]
        if len(double_assigned_args) > 0:
            double_assigned_arg_names = ", ".join(a for a in double_assigned_args)
            raise ValueError(f"parameter(s) assigned twice: {double_assigned_arg_names}.")

        if not self.has_var_kwarg:
            invalid_kwargs = [a for a in kwargs.keys() if a not in self.kwargs]
            if len(invalid_kwargs) > 0:
                invalid_kwarg_names = ", ".join(a for a in invalid_kwargs)
                raise ValueError(f"invalid keyword parameter(s): {invalid_kwarg_names}.")

        unassigned_kwargs = [
            a
            for a in self.kwargs.values()
            if a.default == Parameter.empty and a.name not in named_args and a.name not in kwargs
        ]
        if len(unassigned_kwargs) > 0:
            unassigned_kwarg_names = ", ".join(a.name for a in unassigned_kwargs)
            raise ValueError(f"unassigned keyword parameter(s): {unassigned_kwarg_names}.")

        return NamedArguments(args=named_args, kwargs=kwargs, var_args=var_args)


@attrs.define(frozen=True)
class NamedArguments:
    """Contains the argument values of a function call, but associated with their respective names, based on the
    function's signature."""

    args: OrderedDict[str, Any] = attrs.field(factory=OrderedDict)
    kwargs: Dict[str, Any] = attrs.field(factory=dict)

    var_args: Tuple = attrs.field(default=tuple())

    def __getitem__(self, name: str) -> Any:
        """Gets the value of an argument by name."""

        if name in self.args:
            return self.args[name]
        elif name in self.kwargs:
            return self.kwargs[name]
        else:
            raise KeyError(f"unknown argument name: {name}.")

    def as_args_kwargs(self) -> Tuple[Tuple, Dict[str, Any]]:
        """Returns a tuple of positional and keyword parameters that can be used to call the function."""

        return self.var_args, {**self.args, **self.kwargs}

    def keys(self) -> Set[str]:
        """Returns all argument names."""

        keys = set(self.args.keys())
        keys.update(self.kwargs.keys())
        return keys

    def split(self, arg_names: Set[str]) -> Tuple["NamedArguments", "NamedArguments"]:
        """Returns the subset of the arguments that matches the argument names, and those that do not."""

        includes = NamedArguments(
            args=OrderedDict((name, value) for name, value in self.args.items() if name in arg_names),
            kwargs={name: value for name, value in self.kwargs.items() if name in arg_names},
            var_args=tuple(),
        )
        excludes = NamedArguments(
            args=OrderedDict((name, value) for name, value in self.args.items() if name not in arg_names),
            kwargs={name: value for name, value in self.kwargs.items() if name not in arg_names},
            var_args=self.var_args,
        )

        return includes, excludes

    def reassigned(self, **changes) -> "NamedArguments":
        """Returns a new ``NamedArguments`` objects with some of the values reassigned.

        .. code:: python

            named_args.reassign(arg_1="new_value", arg_2="new_value")

        """

        args = self.args.copy()
        kwargs = self.kwargs.copy()

        for arg_name, arg_value in changes.items():
            if arg_name in args:
                args[arg_name] = arg_value
            elif arg_name in kwargs:
                kwargs[arg_name] = arg_value
            else:
                raise ValueError(f"invalid argument key: `{arg_name}`.")

        return attrs.evolve(self, args=args, kwargs=kwargs)

    def merge(self, other: "NamedArguments") -> "NamedArguments":
        """Returns a new ``NamedArguments`` object with the values of both objects merged."""

        args = self.args.copy()
        args.update(other.args)

        return NamedArguments(
            args=args, kwargs={**self.kwargs, **other.kwargs}, var_args=self.var_args + other.var_args
        )
