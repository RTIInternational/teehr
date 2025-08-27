import pytest
from pathlib import Path
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path

def test_concat_paths_with_string_path():
    result = to_path_or_s3path("s3://bucket/path", "to", "file")
    assert isinstance(result, S3Path)
    assert str(result) == "s3://bucket/path/to/file"

def test_concat_paths_with_local_string_path():
    result = to_path_or_s3path("/local/path", "to", "file")
    assert isinstance(result, Path)
    assert str(result) == "/local/path/to/file"

def test_concat_paths_with_path_object():
    result = to_path_or_s3path(Path("/local/path"), "to", "file")
    assert isinstance(result, Path)
    assert str(result) == "/local/path/to/file"

def test_concat_paths_with_s3path_object():
    result = to_path_or_s3path(S3Path("s3://bucket/path"), "to", "file")
    assert isinstance(result, S3Path)
    assert str(result) == "s3://bucket/path/to/file"

def test_concat_paths_with_invalid_arg():
    with pytest.raises(ValueError):
        to_path_or_s3path("/local/path", 123)

if __name__ == "__main__":
    test_concat_paths_with_string_path()
    test_concat_paths_with_local_string_path()
    test_concat_paths_with_path_object()
    test_concat_paths_with_s3path_object()
    test_concat_paths_with_invalid_arg()
    print("All tests passed")