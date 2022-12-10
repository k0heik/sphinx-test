from module import prepare_df


def test_add_unit_info_setting_columns(unit_info_df):
    unit_info_df = unit_info_df.drop(columns=["target_kpi", "target_kpi_value", "purpose", "mode"])
    assert (
        set(list(unit_info_df.columns))
        & set(["target_kpi", "target_kpi_value", "purpose", "mode"])
    ) == set()

    result_df = prepare_df.add_unit_info_setting_columns(unit_info_df)

    assert len(result_df) == len(unit_info_df)
    assert set(result_df.columns) == set(
        list(unit_info_df.columns)
        + ["target_kpi", "target_kpi_value", "purpose", "mode"]
    )
