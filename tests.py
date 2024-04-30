import unittest
import zipfile
from utils import unzip
import os
import asyncio
import anyio
import shutil
import os


class TestUtils(unittest.TestCase):
    def setUp(self) -> None:
        self.filename = "test.zip"
        return super().setUp()

    def test_unzip(self):
        zipfile.ZipFile(self.filename, mode="w")
        is_exists = os.path.exists(self.filename)
        self.assertIs(is_exists, True)
        # unzip(self.filename, "")

    def doCleanups(self) -> None:
        if os.path.exists(self.filename):
            os.remove(self.filename)
        return super().doCleanups()


if __name__ == "__main__":
    unittest.main()
