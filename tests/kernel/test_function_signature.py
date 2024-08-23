import unittest
from collections import OrderedDict

from parfun.kernel.function_signature import FunctionSignature


class TestFunctionSignature(unittest.TestCase):
    def test_assign(self):
        def function_1(arg_1, arg_2=None):
            pass

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
            pass

        signature = FunctionSignature.from_function(function_2)

        self.assertTrue(signature.has_var_arg)
        self.assertTrue(signature.has_var_kwarg)

        self.assertRaises(ValueError, lambda: signature.assign([], {}))

        assigned_args = signature.assign([1, 2, 3], {"arg_4": 4})

        self.assertEqual(assigned_args.args, OrderedDict(arg_1=1, arg_2=2))
        self.assertEqual(assigned_args.kwargs, {"arg_4": 4})
        self.assertEqual(assigned_args.var_args, (3,))


if __name__ == "__main__":
    unittest.main()
