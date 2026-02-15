import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from bot.user_prefs import normalize_style, style_label


def test_compact_alias_maps_to_beginner():
    assert normalize_style("compact") == "beginner"


def test_short_alias_maps_to_beginner():
    assert normalize_style("b") == "beginner"


def test_style_label_uses_beginner_for_legacy_value():
    assert style_label("compact") == "초보"
