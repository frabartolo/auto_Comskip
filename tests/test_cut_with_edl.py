import os
import sys
import unittest

import cut_with_edl

# Ensure the `src` directory is importable when tests are executed from the repo root
TESTS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(TESTS_DIR, '..'))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class TestEDLParsing(unittest.TestCase):
    def test_parse_and_build_filter(self):
        # Two commercial blocks: 10-20 and 40-50
        edl_lines = [
            "10.0 20.0 0\n",
            "40.0 50.0 0\n",
        ]

        cuts = cut_with_edl.parse_edl_lines(edl_lines)
        self.assertEqual(len(cuts), 2)

        keep = cut_with_edl.keep_segments_from_cuts(cuts)
        # Expected keep segments: (0,10), (20,40), (50,None)
        self.assertEqual(len(keep), 3)
        self.assertEqual(keep[0], (0.0, 10.0))
        self.assertEqual(keep[1], (20.0, 40.0))
        self.assertEqual(keep[2], (50.0, None))

        fc = cut_with_edl.build_filter_complex(keep)
        self.assertIn("trim=start=0.0:end=10.0", fc)
        self.assertIn("trim=start=20.0:end=40.0", fc)
        self.assertIn("trim=start=50.0", fc)
        self.assertIn("concat=n=3", fc)

    def test_empty_edl_creates_full_keep(self):
        edl_lines = []
        cuts = cut_with_edl.parse_edl_lines(edl_lines)
        self.assertEqual(cuts, [])
        keep = cut_with_edl.keep_segments_from_cuts(cuts)
        self.assertEqual(len(keep), 1)
        self.assertEqual(keep[0], (0.0, None))
        fc = cut_with_edl.build_filter_complex(keep)
        self.assertIn("concat=n=1", fc)


if __name__ == '__main__':
    unittest.main()
