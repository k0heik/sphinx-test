import pytest

from common_module.system_util import get_bucket_key_from_s3_uri


class TestSystemUtilGetBucketKeyFromS3URI:
    @pytest.mark.parametrize("s3_uri, expected_bucket, expected_key", [
        ("s3://bucket_name/key",
         "bucket_name",
         "key"),
        ("s3://bucket_name/key/key2",
         "bucket_name",
         "key/key2"),
        ("s3://bucket_name/key/key2/key3",
         "bucket_name",
         "key/key2/key3"),
    ])
    def test_get_bucket_key_from_s3_uri(
        self, s3_uri, expected_bucket, expected_key
    ):
        bucket, key = get_bucket_key_from_s3_uri(s3_uri)

        assert bucket == expected_bucket
        assert key == expected_key

    @pytest.mark.parametrize("s3_uri", [
        ("s4://bucket_name/key"),
        ("s://bucket_name/key"),
        ("s3//bucket_name/key"),
        ("s3:/bucket_name/key"),
        ("s3:bucket_name/key"),
        ("bucket_name/key"),
    ])
    def test_get_bucket_key_from_s3_uri_error(self, s3_uri):
        with pytest.raises(ValueError):
            get_bucket_key_from_s3_uri(s3_uri)
