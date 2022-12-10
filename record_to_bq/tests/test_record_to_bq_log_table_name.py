from lambda_handler import _log_table_name


def test_log_table_name():
    assert "prefix_20220109" == _log_table_name("prefix", 2022, 1, 9)
    assert "prefix_20221230" == _log_table_name("prefix", 2022, 12, 30)
    assert "prefix_2020109" == _log_table_name("prefix", 202, 1, 9)
