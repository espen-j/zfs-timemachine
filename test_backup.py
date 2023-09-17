from unittest import TestCase

import backup


class Test(TestCase):
    def test_run_command(self):
        (data, err_data, return_code) = backup.run_command("echo 'hello test'")

        self.assertIn("hello test", data)
        self.assertIs(err_data, '')
        self.assertEqual(return_code, 0)

