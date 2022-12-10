import pytest

from common_module.bigquery_util import BigQueryService

from common_module.datamanage_util import output_distributed_preprocess_manager


_SSM_GCP_KEY_PARAMETER_NAME = "/SOPHIAAI/DWH/GCP/ACCESS_KEY/DEVELOP"
_GCP_PROJECT_ID = "sophiaai-develop"
_DATASET_NAME = "bid_optimisation_ml"


@pytest.fixture
def _bq(monkeypatch):
    monkeypatch.setenv("SSM_GCP_KEY_PARAMETER_NAME", _SSM_GCP_KEY_PARAMETER_NAME)
    monkeypatch.setenv("GCP_PROJECT_ID", _GCP_PROJECT_ID)
    monkeypatch.setenv("DATASET_NAME", _DATASET_NAME)

    bq = BigQueryService(_GCP_PROJECT_ID, _DATASET_NAME)

    yield bq


def test_output_distributed_preprocess_manager(_bq, mocker):
    _bq.s3_to_gcs = mocker.MagicMock()
    _bq.append_gcs_to_bq = mocker.MagicMock()
    mocker.patch("common_module.datamanage_util.write_df_to_s3", return_value=None)

    mocker.patch("common_module.datamanage_util.time.sleep", return_value=None)
    output_distributed_preprocess_manager(
        s3_bucket=None,
        key=None,
        gcs_bucket=None,
        bq=_bq,
        bq_dataset_name=None,
        bq_table_name=None,
        max_trials=2)(None)


def test_output_distributed_preprocess_manager_failed(_bq, mocker):
    _bq.s3_to_gcs = mocker.MagicMock()
    _bq.append_gcs_to_bq = mocker.MagicMock(side_effect=Exception)
    mocker.patch("common_module.datamanage_util.write_df_to_s3", return_value=None)

    mocker.patch("common_module.datamanage_util.time.sleep", return_value=None)
    with pytest.raises(Exception):
        output_distributed_preprocess_manager(
            s3_bucket=None,
            key=None,
            gcs_bucket=None,
            bq=_bq,
            bq_dataset_name=None,
            bq_table_name=None,
            max_trials=2)(None)


test_output_distributed_preprocess_manager_retry_success_try_num = 0


def test_output_distributed_preprocess_manager_retry_success(_bq, mocker):
    def _side_efffect(*args, **kwargs):
        global test_output_distributed_preprocess_manager_retry_success_try_num
        print(test_output_distributed_preprocess_manager_retry_success_try_num)
        if test_output_distributed_preprocess_manager_retry_success_try_num == 0:
            test_output_distributed_preprocess_manager_retry_success_try_num += 1
            raise Exception()
        else:
            return

    _bq.append_gcs_to_bq = mocker.MagicMock(side_effect=_side_efffect)
    _bq.s3_to_gcs = mocker.MagicMock()
    mocker.patch("common_module.datamanage_util.write_df_to_s3", return_value=None)

    global test_output_distributed_preprocess_manager_retry_success_try_num
    test_output_distributed_preprocess_manager_retry_success_try_num = 0

    mocker.patch("common_module.datamanage_util.time.sleep", return_value=None)
    output_distributed_preprocess_manager(
        s3_bucket=None,
        key=None,
        gcs_bucket=None,
        bq=_bq,
        bq_dataset_name=None,
        bq_table_name=None,
        max_trials=2)(None)
