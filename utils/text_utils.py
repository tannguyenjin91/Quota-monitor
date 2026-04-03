import re
import unicodedata


def normalize_text(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"\s+", " ", text)


def strip_accents(value) -> str:
    text = normalize_text(value)
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_key(value) -> str:
    text = strip_accents(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def normalize_rejection_value(value) -> str:
    return normalize_text(value).lower()


def normalize_answer_code(value) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def is_blank(value) -> bool:
    return normalize_text(value) == ""
