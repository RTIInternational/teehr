import pytest
from teehr.utils.s3path import S3Path
from pathlib import Path

def test_s3path_initialization_comma():
    path = S3Path("s3://bucket", "folder", "file.txt")
    assert str(path) == "s3://bucket/folder/file.txt"

def test_s3path_initialization_div():
    path = S3Path(S3Path("s3://bucket"), "folder", "file.txt")
    assert str(path) == "s3://bucket/folder/file.txt"

def test_s3path_invalid_initialization():
    with pytest.raises(ValueError, match="Invalid S3 path"):
        S3Path("invalid_path")

def test_s3path_invalid_initialization_int():
    with pytest.raises(ValueError, match="Invalid S3 path"):
        S3Path("invalid_path", 12, "file.txt")

def test_s3path_concatenation():
    path = S3Path("s3://bucket", "folder")
    new_path = path / "file.txt"
    assert str(new_path) == "s3://bucket/folder/file.txt"

def test_s3path_invalid_concatenation():
    path = S3Path("s3://bucket", "folder")
    with pytest.raises(ValueError, match="Only strings can be concatenated to S3Path"):
        path / 123

def test_s3path_repr():
    path = S3Path("s3://bucket", "folder", "file.txt")
    assert repr(path) == "S3Path(s3://bucket/folder/file.txt)"

def test_s3path_invalid_path():
    with pytest.raises(ValueError, match="Invalid S3 path"):
        S3Path(Path("/bucket"), "folder")

if __name__ == "__main__":
    test_s3path_initialization_comma()
    test_s3path_initialization_div()
    test_s3path_invalid_initialization()
    test_s3path_invalid_initialization_int()
    test_s3path_concatenation()
    test_s3path_invalid_concatenation()
    test_s3path_repr()
    test_s3path_invalid_path()
    print("All tests passed")
