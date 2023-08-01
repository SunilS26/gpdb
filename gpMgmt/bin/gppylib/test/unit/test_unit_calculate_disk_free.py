#!/usr/bin/env python3

from mock import Mock, patch, call,  MagicMock
from gppylib.test.unit.gp_unittest import GpTestCase, run_tests, FakeCursor
from lib.calculate_disk_free import calculate_disk_free, _disk_free
from gppylib.operations.validate_disk_space import FileSystem
import io

class CalculateDiskFreeTestCase(GpTestCase):

    @patch('lib.calculate_disk_free._disk_free')
    def test_calculate_disk_free(self, mock1):
        cmd = MagicMock()
        cmd.returncode = 0
        cmd.stdout = "Filesystem     1024-blocks  Used Available Capacity Mounted on\n \
                        /tmp/disk1               124     0         124       0% /tmp/seg1\n"
        mock1.return_value = cmd


        ret = calculate_disk_free(['/data/seg1'])
        self.assertIsInstance(ret[0], FileSystem)
        self.assertEqual(ret[0].name, "/tmp/disk1")
        self.assertEqual(ret[0].directories, ['/tmp/seg1'])
        self.assertEqual(ret[0].disk_free, 124)

    @patch('sys.stderr', new_callable=io.StringIO)
    @patch('lib.calculate_disk_free._disk_free')
    def test_calculate_disk_free(self, mock1, mock_stderr):
        cmd = MagicMock()
        cmd.returncode = -1
        cmd.stderr = "Generic Error"
        expected_stderr = "Failed to calculate free disk space: Generic Error."
        mock1.return_value = cmd

        ret = calculate_disk_free(['/data/seg1'])
        self.assertEqual(ret, [])
        self.assertEqual(expected_stderr, mock_stderr.getvalue())

    @patch('lib.calculate_disk_free.findCmdInPath')
    @patch('lib.calculate_disk_free.subprocess.run')
    def test_disk_free(self, mock1, mock2):
        cmd = MagicMock()
        cmd.returncode = 0
        cmd.stdout = "Filesystem     1024-blocks  Used Available Capacity Mounted on\n \
                                /tmp/disk1               124     0         124       0% /tmp/seg1\n"
        mock1.return_value = cmd

        ret = _disk_free(['/data/seg1'])
        self.assertEqual(ret.stdout, cmd.stdout)

    @patch('lib.calculate_disk_free.findCmdInPath')
    @patch('lib.calculate_disk_free.subprocess.run')
    def test_disk_free_invalid_dir(self, mock1, mock2):
        cmd1 = MagicMock()
        cmd1.returncode = 1

        cmd2 = MagicMock()
        cmd2.returncode = 0
        cmd2.stdout = "Filesystem     1024-blocks  Used Available Capacity Mounted on\n \
                                   /tmp/disk1               124     0         124       0% /tmp/seg1\n"
        mock1.side_effect = [cmd1, cmd2]

        ret = _disk_free("/data/seg1/invalid_dir")
        self.assertEqual(ret.stdout, cmd2.stdout)
