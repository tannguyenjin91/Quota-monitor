import json
import os
from typing import Any

from utils.text_utils import normalize_key, normalize_text, strip_accents


def suggest_quota_setup(user_prompt: str, variables: list[dict], display_mode: str, percent_mode: str) -> dict[str, Any]:
    prompt = normalize_text(user_prompt)
    if not prompt:
        return empty_suggestion(display_mode, percent_mode, "empty")

    api_suggestion = suggest_with_openai(prompt, variables, display_mode, percent_mode)
    if api_suggestion:
        return api_suggestion
    return suggest_with_heuristics(prompt, variables, display_mode, percent_mode)


def suggest_with_openai(user_prompt: str, variables: list[dict], display_mode: str, percent_mode: str) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
    except ImportError:
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    client = OpenAI(api_key=api_key)
    compact_variables = [
        {
            "variable_code": item.get("variable_code"),
            "question_label": item.get("question_label"),
            "question_type": item.get("question_type"),
            "available_labels": (item.get("available_labels") or [])[:12],
            "quota_eligible": item.get("quota_eligible"),
        }
        for item in variables
    ]
    instructions = (
        "You map a market research reporting request to a crosstab config. "
        "Return JSON only with keys: horizontal_variable, vertical_variable, selected_filters, explanation. "
        "selected_filters must be a list of {variable_code, value}. Choose variables only from the provided catalog."
    )
    try:
        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": instructions},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "request": user_prompt,
                            "variables": compact_variables,
                            "display_mode": display_mode,
                            "percent_mode": percent_mode,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        )
        content = response.choices[0].message.content or "{}"
        payload = json.loads(content)
    except Exception:
        return None
    return normalize_suggestion_payload(payload, variables, display_mode, percent_mode, engine="openai")


def suggest_with_heuristics(user_prompt: str, variables: list[dict], display_mode: str, percent_mode: str) -> dict[str, Any]:
    prompt_key = normalize_key(user_prompt)
    variable_matches = []
    for variable in variables:
        score = score_variable(prompt_key, variable)
        matched_labels = matched_category_labels(prompt_key, variable)
        if matched_labels:
            score += 12 + len(matched_labels)
        variable_matches.append(
            {
                "variable": variable,
                "score": score,
                "matched_labels": matched_labels,
                "kind": classify_variable_kind(variable),
            }
        )

    relevant = [item for item in variable_matches if item["score"] > 0 or item["matched_labels"]]
    relevant.sort(key=lambda item: (-item["score"], item["variable"].get("variable_code", "")))

    horizontal = pick_axis_variable(relevant, axis="horizontal")
    vertical = pick_axis_variable([item for item in relevant if item["variable"].get("variable_code") != horizontal], axis="vertical")

    if not horizontal and relevant:
        horizontal = relevant[0]["variable"].get("variable_code")
    if not vertical:
        fallback_pool = [item for item in relevant if item["variable"].get("variable_code") != horizontal]
        if fallback_pool:
            vertical = fallback_pool[0]["variable"].get("variable_code")

    selected_filters = []
    for item in relevant:
        variable_code = item["variable"].get("variable_code")
        if variable_code in {horizontal, vertical}:
            continue
        for label in item["matched_labels"][:1]:
            selected_filters.append({"variable_code": variable_code, "value": label})
        if len(selected_filters) >= 3:
            break

    explanation_parts = []
    if horizontal:
        explanation_parts.append(f"Column suggested: {horizontal}")
    if vertical:
        explanation_parts.append(f"Row suggested: {vertical}")
    if selected_filters:
        explanation_parts.append("Filters: " + "; ".join(f"{item['variable_code']} = {item['value']}" for item in selected_filters))

    return {
        "horizontal_variable": horizontal or "",
        "vertical_variable": vertical or "",
        "selected_filters": selected_filters,
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "engine": "heuristic",
        "explanation": " | ".join(explanation_parts) if explanation_parts else "No strong match found from the current prompt.",
    }


def normalize_suggestion_payload(payload: dict[str, Any], variables: list[dict], display_mode: str, percent_mode: str, engine: str) -> dict[str, Any]:
    valid_codes = {item.get("variable_code") for item in variables}
    selected_filters = []
    for item in payload.get("selected_filters", []) or []:
        variable_code = normalize_text(item.get("variable_code"))
        value = normalize_text(item.get("value"))
        if variable_code in valid_codes and value:
            selected_filters.append({"variable_code": variable_code, "value": value})
    return {
        "horizontal_variable": payload.get("horizontal_variable") if payload.get("horizontal_variable") in valid_codes else "",
        "vertical_variable": payload.get("vertical_variable") if payload.get("vertical_variable") in valid_codes else "",
        "selected_filters": selected_filters[:3],
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "engine": engine,
        "explanation": normalize_text(payload.get("explanation")),
    }


def empty_suggestion(display_mode: str, percent_mode: str, engine: str) -> dict[str, Any]:
    return {
        "horizontal_variable": "",
        "vertical_variable": "",
        "selected_filters": [],
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "engine": engine,
        "explanation": "",
    }


def score_variable(prompt_key: str, variable: dict) -> int:
    haystack = " ".join(
        [
            normalize_key(variable.get("variable_code", "")),
            normalize_key(variable.get("question_label", "")),
            " ".join(normalize_key(label) for label in variable.get("available_labels", [])[:8]),
        ]
    )
    score = 0
    for token in tokenize(prompt_key):
        if token and token in haystack:
            score += 2
    return score


def matched_category_labels(prompt_key: str, variable: dict) -> list[str]:
    matched = []
    for label in variable.get("available_labels", []) or []:
        normalized_label = normalize_key(label)
        if normalized_label and normalized_label in prompt_key:
            matched.append(label)
    return matched


def classify_variable_kind(variable: dict) -> str:
    base_searchable = " ".join(
        [
            normalize_key(variable.get("variable_code", "")),
            normalize_key(variable.get("question_label", "")),
        ]
    )
    category_tokens = [normalize_key(label) for label in variable.get("available_labels", [])[:8]]
    category_searchable = " ".join(category_tokens)

    if any(keyword in base_searchable for keyword in ["gioi_tinh", "gender", "sex"]):
        return "gender"
    if any(keyword in base_searchable for keyword in ["huong", "direction"]):
        return "direction"
    if any(keyword in base_searchable for keyword in ["khoang_cach", "distance", "km"]):
        return "distance"
    if any(keyword in base_searchable for keyword in ["tuoi", "age"]):
        return "age"

    if {"nam", "nu"}.issubset(set(category_tokens)) or {"male", "female"}.issubset(set(category_tokens)):
        return "gender"
    if len({"bac", "dong", "tay", "nam", "dong_bac", "tay_nam"}.intersection(set(category_tokens))) >= 2:
        return "direction"
    if any("km" in token for token in category_tokens) or {"near", "far"}.intersection(set(category_tokens)):
        return "distance"
    if len({"18_29", "30_39", "40_49", "50_59"}.intersection(set(category_tokens))) >= 2:
        return "age"
    return "generic"


def pick_axis_variable(matches: list[dict], axis: str) -> str:
    preferred_kinds = {
        "horizontal": ["gender", "age", "generic", "distance", "direction"],
        "vertical": ["direction", "age", "distance", "generic", "gender"],
    }
    for kind in preferred_kinds[axis]:
        for item in matches:
            if item["kind"] == kind:
                return item["variable"].get("variable_code", "")
    return ""


def tokenize(value: str) -> list[str]:
    normalized = strip_accents(value).lower()
    return [token for token in normalized.replace("_", " ").split() if len(token) >= 2]
