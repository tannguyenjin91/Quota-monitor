import pandas as pd


def build_quota_table(cleaned_df: pd.DataFrame, variable_catalog_lookup: dict, row_variable: str, column_variable: str, display_mode: str, percent_mode: str):
    working_df = cleaned_df[
        (cleaned_df["is_accepted_for_quota"] == True)  # noqa: E712
        & (cleaned_df[f"decoded__{row_variable}"].fillna("") != "")
        & (cleaned_df[f"decoded__{column_variable}"].fillna("") != "")
    ].copy()

    row_categories = ordered_categories(variable_catalog_lookup.get(row_variable, {}), working_df[f"decoded__{row_variable}"])
    column_categories = ordered_categories(variable_catalog_lookup.get(column_variable, {}), working_df[f"decoded__{column_variable}"])

    count_table = pd.crosstab(
        working_df[f"decoded__{row_variable}"],
        working_df[f"decoded__{column_variable}"],
        dropna=False,
    )
    count_table = count_table.reindex(index=row_categories, columns=column_categories, fill_value=0)

    grand_total = int(count_table.to_numpy().sum())
    row_totals = count_table.sum(axis=1)
    column_totals = count_table.sum(axis=0)

    count_view = build_table_view(
        row_categories=row_categories,
        column_categories=column_categories,
        count_table=count_table,
        row_totals=row_totals,
        column_totals=column_totals,
        grand_total=grand_total,
        mode="Count",
        percent_mode=percent_mode,
    )
    percent_view = build_table_view(
        row_categories=row_categories,
        column_categories=column_categories,
        count_table=count_table,
        row_totals=row_totals,
        column_totals=column_totals,
        grand_total=grand_total,
        mode="Percent",
        percent_mode=percent_mode,
    )

    if display_mode == "Count + Percent":
        table_views = [count_view, percent_view]
        primary_view = count_view
    elif display_mode == "Percent":
        table_views = [percent_view]
        primary_view = percent_view
    else:
        table_views = [count_view]
        primary_view = count_view

    return {
        "row_categories": row_categories,
        "column_categories": column_categories,
        "rows": primary_view["rows"],
        "total_row": primary_view["total_row"],
        "table_views": table_views,
        "accepted_base_size": grand_total,
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "count_table": count_table,
    }


def build_banner_table(
    cleaned_df: pd.DataFrame,
    variable_catalog_lookup: dict,
    row_variables: list[str],
    banner_variables: list[str],
    display_mode: str,
    percent_mode: str,
    banner_tree: list[dict] | None = None,
    banner_layout_mode: str = "flat",
):
    accepted_df = cleaned_df[cleaned_df["is_accepted_for_quota"] == True].copy()  # noqa: E712
    row_variables = [item for item in row_variables if item]
    banner_variables = resolve_banner_variables(banner_tree, banner_variables)
    if banner_layout_mode == "tree":
        column_groups = build_banner_column_groups(
            accepted_df=accepted_df,
            variable_catalog_lookup=variable_catalog_lookup,
            banner_variables=banner_variables,
        )
    else:
        column_groups = build_flat_banner_column_groups(
            accepted_df=accepted_df,
            variable_catalog_lookup=variable_catalog_lookup,
            banner_variables=banner_variables,
        )

    flat_columns = []
    for group in column_groups:
        flat_columns.extend(group["columns"])

    modes = ["Count", "Percent"] if display_mode == "Count + Percent" else [display_mode]
    banner_views = []
    for mode in modes:
        sections = []
        for row_variable in row_variables:
            row_entry = variable_catalog_lookup.get(row_variable, {})
            rows = build_banner_section_rows(
                accepted_df=accepted_df,
                variable_catalog_lookup=variable_catalog_lookup,
                row_variable=row_variable,
                row_entry=row_entry,
                flat_columns=flat_columns,
                display_mode=mode,
                percent_mode=percent_mode,
            )
            if not rows:
                continue
            sections.append(
                {
                    "row_variable": row_variable,
                    "question_label": row_entry.get("question_label", row_variable),
                    "rows": rows,
                }
            )
        banner_views.append({"mode": mode, "sections": sections})

    return {
        "report_type": "banner_table",
        "selected_banner_variables": banner_variables,
        "selected_banner_layout_mode": banner_layout_mode,
        "column_groups": column_groups,
        "flat_columns": flat_columns,
        "sections": banner_views[0]["sections"] if banner_views else [],
        "banner_views": banner_views,
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "accepted_base_size": int(len(accepted_df)),
        "banner_tree": banner_tree or [{"variable_code": code, "label": variable_catalog_lookup.get(code, {}).get("question_label", code)} for code in banner_variables],
}


def build_drilldown_table(
    cleaned_df: pd.DataFrame,
    variable_catalog_lookup: dict,
    row_variable: str,
    row_value: str,
    column_variable: str,
    column_value: str,
    breakdown_variable: str,
    display_mode: str,
    percent_mode: str,
):
    filtered_df = cleaned_df[
        (cleaned_df["is_accepted_for_quota"] == True)  # noqa: E712
        & (cleaned_df[f"decoded__{row_variable}"].fillna("") == row_value)
        & (cleaned_df[f"decoded__{column_variable}"].fillna("") == column_value)
        & (cleaned_df[f"decoded__{breakdown_variable}"].fillna("") != "")
    ].copy()

    category_series = filtered_df[f"decoded__{breakdown_variable}"] if f"decoded__{breakdown_variable}" in filtered_df.columns else pd.Series(dtype=str)
    categories = ordered_categories(variable_catalog_lookup.get(breakdown_variable, {}), category_series)
    count_series = category_series.value_counts().reindex(categories, fill_value=0) if not filtered_df.empty else pd.Series(index=categories, data=0)
    total_count = int(count_series.sum()) if len(count_series) else 0

    rows = []
    for category in categories:
        count_value = int(count_series.get(category, 0))
        percent_value = calculate_percent(
            count_value,
            grand_total=total_count,
            row_total=count_value,
            column_total=count_value,
            percent_mode="total_percent" if percent_mode == "total_percent" else percent_mode,
        )
        rows.append({"label": category, "cell": render_cell(count_value, percent_value, display_mode)})

    total_cell = render_cell(total_count, 100.0 if total_count else 0.0, display_mode)
    return {
        "row_variable": row_variable,
        "row_value": row_value,
        "column_variable": column_variable,
        "column_value": column_value,
        "breakdown_variable": breakdown_variable,
        "breakdown_label": variable_catalog_lookup.get(breakdown_variable, {}).get("question_label", breakdown_variable),
        "rows": rows,
        "total": total_cell,
        "display_mode": display_mode,
        "percent_mode": percent_mode,
        "base_size": total_count,
    }


def apply_additional_filters(cleaned_df: pd.DataFrame, selected_filters):
    filtered_df = cleaned_df.copy()
    for item in selected_filters or []:
        variable_code = item.get("variable_code", "")
        selected_value = item.get("value", "")
        if not variable_code or selected_value == "":
            continue
        column_name = f"decoded__{variable_code}"
        if column_name not in filtered_df.columns:
            continue
        filtered_df = filtered_df[filtered_df[column_name].fillna("") == selected_value]
    return filtered_df


def ordered_categories(variable_entry: dict, observed_series):
    configured = variable_entry.get("available_labels") or []
    observed = [value for value in observed_series.fillna("").tolist() if value]
    observed_unique = list(dict.fromkeys(observed))
    if configured:
        ordered = [label for label in configured if label in observed_unique]
        extras = [label for label in observed_unique if label not in ordered]
        return ordered + extras
    return observed_unique or sorted(set(observed))


def calculate_percent(count_value: int, grand_total: int, row_total: int, column_total: int, percent_mode: str):
    if percent_mode == "row_percent":
        denominator = row_total
    elif percent_mode == "column_percent":
        denominator = column_total
    else:
        denominator = grand_total
    return round((count_value / denominator * 100) if denominator else 0, 1)


def render_cell(count_value: int, percent_value: float, display_mode: str):
    if display_mode == "Count":
        return {"count": count_value, "percent": percent_value, "display": str(count_value)}
    if display_mode == "Percent":
        return {"count": count_value, "percent": percent_value, "display": f"{percent_value:.1f}%"}
    return {"count": count_value, "percent": percent_value, "display": f"{count_value}\n({percent_value:.1f}%)"}


def render_banner_cell(base_df: pd.DataFrame, category_df: pd.DataFrame | None, column_spec: dict, display_mode: str, percent_mode: str):
    filters = column_spec.get("filters", [])
    column_base_df = filter_by_decoded_values(base_df, filters)
    scoped_df = base_df if category_df is None else category_df
    scoped_filtered_df = filter_by_decoded_values(scoped_df, filters)
    column_base_count = int(len(column_base_df))
    count_value = int(len(scoped_filtered_df))
    row_total = int(len(base_df if category_df is None else category_df))
    percent_value = calculate_percent(
        count_value,
        grand_total=int(len(base_df)),
        row_total=row_total,
        column_total=column_base_count,
        percent_mode=percent_mode,
    )
    return render_cell(count_value, percent_value, display_mode)


def build_table_view(row_categories, column_categories, count_table, row_totals, column_totals, grand_total: int, mode: str, percent_mode: str):
    rendered_rows = []
    for row_category in row_categories:
        rendered_cells = []
        for column_category in column_categories:
            count_value = int(count_table.loc[row_category, column_category])
            percent_value = calculate_percent(
                count_value,
                grand_total=grand_total,
                row_total=int(row_totals.loc[row_category]),
                column_total=int(column_totals.loc[column_category]),
                percent_mode=percent_mode,
            )
            rendered_cells.append(render_cell(count_value, percent_value, mode))

        rendered_rows.append(
            {
                "label": row_category,
                "cells": rendered_cells,
                "total": render_cell(
                    int(row_totals.loc[row_category]),
                    calculate_percent(int(row_totals.loc[row_category]), grand_total, int(row_totals.loc[row_category]), grand_total, percent_mode),
                    mode,
                ),
            }
        )

    total_row = {
        "label": "Total",
        "cells": [
            render_cell(
                int(column_totals.loc[column_category]),
                calculate_percent(int(column_totals.loc[column_category]), grand_total, grand_total, int(column_totals.loc[column_category]), percent_mode),
                mode,
            )
            for column_category in column_categories
        ],
        "total": render_cell(grand_total, 100.0 if grand_total else 0.0, mode),
    }
    return {
        "mode": mode,
        "rows": rendered_rows,
        "total_row": total_row,
    }


def resolve_banner_variables(banner_tree: list[dict] | None, banner_variables: list[str]):
    if banner_tree:
        tree_codes = []
        for item in banner_tree:
            if isinstance(item, dict) and item.get("variable_code"):
                variable_code = str(item["variable_code"]).strip()
                if variable_code and variable_code not in tree_codes:
                    tree_codes.append(variable_code)
        if tree_codes:
            return tree_codes
    return [item for item in banner_variables if item]


def build_banner_column_groups(accepted_df: pd.DataFrame, variable_catalog_lookup: dict, banner_variables: list[str]):
    if not banner_variables:
        return [
            {
                "variable_code": "__overall__",
                "question_label": "Total",
                "columns": [{"key": "__overall_total__", "label": "Total", "filters": []}],
            }
        ]

    first_variable = banner_variables[0]
    remaining_variables = banner_variables[1:]
    first_entry = variable_catalog_lookup.get(first_variable, {})
    first_categories = ordered_categories(first_entry, accepted_df.get(f"decoded__{first_variable}", pd.Series(dtype=str)))

    column_groups = [
        {
            "variable_code": "__overall__",
            "question_label": "Total",
            "columns": [{"key": "__overall_total__", "label": "Total", "filters": []}],
        }
    ]
    for first_category in first_categories:
        prefix_filters = [{"variable_code": first_variable, "value": first_category}]
        group_columns = [{"key": f"{first_variable}::{first_category}::__total__", "label": "Total", "filters": prefix_filters}]
        if remaining_variables:
            for combo in expand_banner_combinations(accepted_df, variable_catalog_lookup, remaining_variables, prefix_filters):
                combo_label = " | ".join(item["value"] for item in combo)
                group_columns.append(
                    {
                        "key": "combo::" + "::".join(f"{item['variable_code']}={item['value']}" for item in combo),
                        "label": combo_label,
                        "filters": combo,
                    }
                )
        column_groups.append(
            {
                "variable_code": first_variable,
                "question_label": first_category,
                "columns": group_columns,
            }
        )
    return column_groups


def build_flat_banner_column_groups(accepted_df: pd.DataFrame, variable_catalog_lookup: dict, banner_variables: list[str]):
    column_groups = [
        {
            "variable_code": "__overall__",
            "question_label": "Total",
            "columns": [{"key": "__overall_total__", "label": "Total", "filters": []}],
        }
    ]
    for variable_code in banner_variables:
        variable_entry = variable_catalog_lookup.get(variable_code, {})
        categories = ordered_categories(variable_entry, accepted_df.get(f"decoded__{variable_code}", pd.Series(dtype=str)))
        group_columns = [{"key": f"{variable_code}::__total__", "label": "Total", "filters": []}]
        group_columns.extend(
            {
                "key": f"{variable_code}::{category}",
                "label": category,
                "filters": [{"variable_code": variable_code, "value": category}],
            }
            for category in categories
        )
        column_groups.append(
            {
                "variable_code": variable_code,
                "question_label": variable_entry.get("question_label", variable_code),
                "columns": group_columns,
            }
        )
    return column_groups


def expand_banner_combinations(accepted_df: pd.DataFrame, variable_catalog_lookup: dict, variables: list[str], prefix_filters: list[dict]):
    if not variables:
        return [prefix_filters]
    variable_code = variables[0]
    remaining = variables[1:]
    working_df = filter_by_decoded_values(accepted_df, prefix_filters)
    categories = ordered_categories(variable_catalog_lookup.get(variable_code, {}), working_df.get(f"decoded__{variable_code}", pd.Series(dtype=str)))
    combinations = []
    for category in categories:
        next_filters = [*prefix_filters, {"variable_code": variable_code, "value": category}]
        combinations.extend(expand_banner_combinations(accepted_df, variable_catalog_lookup, remaining, next_filters))
    return combinations


def build_banner_section_rows(
    accepted_df: pd.DataFrame,
    variable_catalog_lookup: dict,
    row_variable: str,
    row_entry: dict,
    flat_columns: list[dict],
    display_mode: str,
    percent_mode: str,
):
    rows = []
    base_df = accepted_df.copy()
    rows.append(
        {
            "is_base": True,
            "category_label": "Base",
            "cells": [render_banner_cell(base_df, None, column_spec, display_mode, percent_mode) for column_spec in flat_columns],
        }
    )

    row_series_name = f"decoded__{row_variable}"
    if row_series_name in accepted_df.columns:
        scoped_df = accepted_df[accepted_df[row_series_name].fillna("") != ""].copy()
        categories = ordered_categories(row_entry, scoped_df[row_series_name])
        for category in categories:
            category_df = scoped_df[scoped_df[row_series_name] == category]
            rows.append(
                {
                    "is_base": False,
                    "category_label": category,
                    "cells": [render_banner_cell(scoped_df, category_df, column_spec, display_mode, percent_mode) for column_spec in flat_columns],
                }
            )
        return rows

    ma_option_columns = [
        column_name
        for column_name in accepted_df.columns
        if column_name.startswith(f"decoded__{row_variable}_")
    ]
    if not ma_option_columns:
        return rows

    for column_name in ma_option_columns:
        category_label = ma_option_label(column_name, variable_catalog_lookup)
        category_df = accepted_df[accepted_df[column_name].apply(is_selected_ma_value)]
        rows.append(
            {
                "is_base": False,
                "category_label": category_label,
                "cells": [render_banner_cell(accepted_df, category_df, column_spec, display_mode, percent_mode) for column_spec in flat_columns],
            }
        )
    return rows


def ma_option_label(column_name: str, variable_catalog_lookup: dict):
    variable_code = column_name.replace("decoded__", "", 1)
    if variable_code in variable_catalog_lookup:
        return variable_catalog_lookup[variable_code].get("question_label", variable_code)
    suffix = variable_code.split("_", 1)[-1]
    return suffix


def is_selected_ma_value(value):
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return value > 0
    normalized = str(value).strip().lower()
    if normalized in {"", "0", "false", "no", "khong", "không", "none", "nan"}:
        return False
    return True


def filter_by_decoded_values(input_df: pd.DataFrame, filters: list[dict]):
    filtered_df = input_df
    for item in filters or []:
        variable_code = item.get("variable_code", "")
        value = item.get("value", "")
        column_name = f"decoded__{variable_code}"
        if not variable_code or column_name not in filtered_df.columns:
            continue
        filtered_df = filtered_df[filtered_df[column_name].fillna("") == value]
    return filtered_df
