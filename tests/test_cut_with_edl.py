"""Unit tests for cut_with_edl module.

Tests EDL parsing, keep-segment computation, and FFmpeg filter_complex generation.
These tests validate the core logic without requiring FFmpeg or actual video files.

Test coverage:
    - EDL parsing with commercial markers
    - Keep-segment inversion from cut points
    - FFmpeg filter string construction
    - Edge cases (empty EDL, multiple commercials)

Usage:
    python -m unittest tests.test_cut_with_edl -v
"""
import os
import sys
import unittest
import src.cut_with_edl as cut_with_edl

# Ensure the `src` directory is importable when tests are executed from the repo root
TESTS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(TESTS_DIR, '..'))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class TestEDLParsing(unittest.TestCase):
    """Test suite for EDL parsing and filter construction logic.

    Validates the complete pipeline from EDL text to FFmpeg filter_complex string.
    Tests ensure that commercial segments are correctly identified and inverted
    into keep-segments, and that the resulting filter strings are valid.
    """

    def test_parse_and_build_filter(self):
        """Test EDL parsing with multiple commercial blocks.

        Scenario: Video with two commercials at 10-20s and 40-50s.
        Expected keep segments: 0-10s, 20-40s, 50s-end.
        Verifies filter_complex contains correct trim points and concat count.
        """
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
        """Test EDL parsing with no commercial markers.

        Scenario: Empty EDL (no commercials detected).
        Expected: Single keep segment from 0 to end of file.
        Verifies that videos without commercials pass through unchanged.
        """
        edl_lines = []
        cuts = cut_with_edl.parse_edl_lines(edl_lines)
        self.assertEqual(cuts, [])
        keep = cut_with_edl.keep_segments_from_cuts(cuts)
        self.assertEqual(len(keep), 1)
        self.assertEqual(keep[0], (0.0, None))
        fc = cut_with_edl.build_filter_complex(keep)
        self.assertIn("concat=n=1", fc)


class TestNoCommercialDetection(unittest.TestCase):
    """Test suite for detecting videos with no commercials detected.

    Validates the edl_has_no_commercials function that checks if an EDL file
    has any commercial markers. Used to trigger no-cut conversion path.
    """

    def setUp(self):
        """Create temporary test EDL files."""
        self.test_dir = os.path.join(PROJECT_ROOT, "tmp_test")
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        """Clean up temporary test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_edl_empty_file(self):
        """Test detection of empty EDL file."""
        edl_path = os.path.join(self.test_dir, "empty.edl")
        with open(edl_path, "w") as f:
            f.write("")
        self.assertTrue(cut_with_edl.edl_has_no_commercials(edl_path))

    def test_edl_only_comments(self):
        """Test detection of EDL with only comments."""
        edl_path = os.path.join(self.test_dir, "comments.edl")
        with open(edl_path, "w") as f:
            f.write("# This is a comment\n# Another comment\n")
        self.assertTrue(cut_with_edl.edl_has_no_commercials(edl_path))

    def test_edl_with_blank_lines_and_comments(self):
        """Test detection of EDL with blank lines and comments only."""
        edl_path = os.path.join(self.test_dir, "blank_comments.edl")
        with open(edl_path, "w") as f:
            f.write("\n# Comment\n\n  \n# Another\n")
        self.assertTrue(cut_with_edl.edl_has_no_commercials(edl_path))

    def test_edl_with_one_commercial(self):
        """Test detection of EDL with one commercial marker."""
        edl_path = os.path.join(self.test_dir, "one_commercial.edl")
        with open(edl_path, "w") as f:
            f.write("10.0 20.0 0\n")
        self.assertFalse(cut_with_edl.edl_has_no_commercials(edl_path))

    def test_edl_with_multiple_commercials(self):
        """Test detection of EDL with multiple commercial markers."""
        edl_path = os.path.join(self.test_dir, "multi_commercial.edl")
        with open(edl_path, "w") as f:
            f.write("# Start\n10.0 20.0 0\n# Middle\n40.0 50.0 0\n# End\n")
        self.assertFalse(cut_with_edl.edl_has_no_commercials(edl_path))

    def test_edl_nonexistent_file(self):
        """Test detection on nonexistent file."""
        edl_path = os.path.join(self.test_dir, "nonexistent.edl")
        # Should return True (no commercials) when file cannot be read
        self.assertTrue(cut_with_edl.edl_has_no_commercials(edl_path))


if __name__ == '__main__':
    unittest.main()
