from typing import Iterable, List, Sequence

from thefuzz import fuzz

from utils.text_utils import normalize_header, normalize_text


def merge_header_rows(rows: Sequence[Sequence[object]]) -> List[str]:
    if not rows:
        return []
    width = max(len(row) for row in rows)
    merged = []
    for col_idx in range(width):
        parts = []
        for row in rows:
            value = row[col_idx] if col_idx < len(row) else ""
            text = normalize_text(value)
            if text:
                parts.append(text)
        merged.append(" | ".join(dict.fromkeys(parts)))
    return merged


def detect_header_band(preview_rows: Sequence[Sequence[object]], max_header_rows: int = 5):
    candidate_rows = []
    for row in preview_rows[:max_header_rows]:
        populated = sum(1 for cell in row if normalize_text(cell))
        if populated >= 2:
            candidate_rows.append(row)
        if len(candidate_rows) >= 3:
            break
    if not candidate_rows:
        candidate_rows = [preview_rows[0]] if preview_rows else []
    return merge_header_rows(candidate_rows), len(candidate_rows)


def detect_header_position(preview_rows: Sequence[Sequence[object]], candidate_groups: dict, max_scan_rows: int = 10):
    best_row_index = 0
    best_score = -1

    for row_index, row in enumerate(preview_rows[:max_scan_rows]):
        row_values = [normalize_header(cell) for cell in row if normalize_text(cell)]
        if not row_values:
            continue

        score = 0
        for candidates in candidate_groups.values():
            normalized_candidates = [normalize_header(candidate) for candidate in candidates]
            matched = any(
                candidate == value or candidate in value or value in candidate
                for candidate in normalized_candidates
                for value in row_values
            )
            if matched:
                score += 1

        if score > best_score:
            best_score = score
            best_row_index = row_index

    return best_row_index


def find_best_matching_column(headers: Iterable[str], candidates: Iterable[str], score_cutoff: int = 70):
    normalized_candidates = [normalize_header(candidate) for candidate in candidates]
    best_index = None
    best_score = -1

    for idx, header in enumerate(headers):
        normalized_header = normalize_header(header)
        for candidate in normalized_candidates:
            if normalized_header == candidate:
                return idx, 100
            if candidate and candidate in normalized_header:
                score = 95
            else:
                score = fuzz.token_sort_ratio(normalized_header, candidate)
            if score > best_score:
                best_index = idx
                best_score = score

    if best_index is None or best_score < score_cutoff:
        raise ValueError(f"Could not match required column from candidates: {list(candidates)}")
    return best_index, best_score
