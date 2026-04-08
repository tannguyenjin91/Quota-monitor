from routes.web import merge_banner_selection_codes


def test_merge_banner_selection_codes_keeps_tree_and_flat_from_all_codes():
    tree_codes = ["S3", "S2"]
    flat_codes = []
    all_codes = ["S3", "S2", "Q9", "GENDER"]
    merged = merge_banner_selection_codes(tree_codes, flat_codes, all_codes)
    assert merged == ["S3", "S2", "Q9", "GENDER"]


def test_merge_banner_selection_codes_deduplicates_and_preserves_order():
    tree_codes = ["S3", "S2"]
    flat_codes = ["Q9", "S2"]
    all_codes = ["S3", "S2", "Q9", "Q9", "GENDER"]
    merged = merge_banner_selection_codes(tree_codes, flat_codes, all_codes)
    assert merged == ["S3", "S2", "Q9", "GENDER"]
