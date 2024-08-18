import inspect
import unittest
from collections import OrderedDict

from parfun.kernel.function_signature import FunctionSignature


class TestFunctionSignature(unittest.TestCase):
    def test_assign(self):
        def function_1(arg_1, arg_2=None):
            ...

        signature = FunctionSignature.from_function(function_1)

        self.assertFalse(signature.has_var_arg)
        self.assertFalse(signature.has_var_kwarg)

        self.assertRaises(ValueError, lambda: signature.assign([], {}))
        self.assertRaises(ValueError, lambda: signature.assign([1, 2, 3], {}))
        self.assertRaises(ValueError, lambda: signature.assign([], {"arg_other": 10}))
        self.assertRaises(ValueError, lambda: signature.assign([], {"arg_1": 1, "arg_2": 2, "arg_3": 3}))

        assigned_args = signature.assign([1, 2], {})

        self.assertEqual(assigned_args.args, OrderedDict(arg_1=1, arg_2=2))
        self.assertEqual(assigned_args.kwargs, {})
        self.assertEqual(assigned_args.var_args, tuple())

        assigned_args = signature.assign([], {"arg_1": 1, "arg_2": 2})

        self.assertEqual(assigned_args.args, OrderedDict())
        self.assertEqual(assigned_args.kwargs, {"arg_1": 1, "arg_2": 2})
        self.assertEqual(assigned_args.var_args, tuple())

        def function_2(arg_1, arg_2=None, *args, **kwargs):
            ...

        signature = FunctionSignature.from_function(function_2)

        self.assertTrue(signature.has_var_arg)
        self.assertTrue(signature.has_var_kwarg)

        self.assertRaises(ValueError, lambda: signature.assign([], {}))

        assigned_args = signature.assign([1, 2, 3], {"arg_4": 4})

        self.assertEqual(assigned_args.args, OrderedDict(arg_1=1, arg_2=2))
        self.assertEqual(assigned_args.kwargs, {"arg_4": 4})
        self.assertEqual(assigned_args.var_args, (3,))

        # Positional only, positional or keyword, keyword only
        def function_3(arg_1, /, arg_2, arg_3=0, *, arg_4=1, **kwargs):
            ...

        signature = inspect.signature(function_3)
        self.assertEqual(signature.parameters["arg_1"].kind, inspect.Parameter.POSITIONAL_ONLY)
        self.assertEqual(signature.parameters["arg_2"].kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        self.assertEqual(signature.parameters["arg_3"].kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        self.assertEqual(signature.parameters["arg_4"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertEqual(signature.parameters["kwargs"].kind, inspect.Parameter.VAR_KEYWORD)

        # Positional only, positional or keyword, var positional, var keyword
        def function_4(arg_1, /, arg_2=1, *args, **kwargs):
            ...

        signature = inspect.signature(function_4)
        self.assertEqual(signature.parameters["arg_1"].kind, inspect.Parameter.POSITIONAL_ONLY)
        self.assertEqual(signature.parameters["arg_2"].kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        self.assertEqual(signature.parameters["args"].kind, inspect.Parameter.VAR_POSITIONAL)
        self.assertEqual(signature.parameters["kwargs"].kind, inspect.Parameter.VAR_KEYWORD)


if __name__ == "__main__":
    unittest.main()
