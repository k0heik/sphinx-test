import io
import joblib


def serialize(obj) -> bytes:
    """object to bytes
    Args:
            obj (Any): Any object that can be serialized
    Returns:
            bytes: binary data of obj
    """
    f = io.BytesIO()
    joblib.dump(obj, f, compress=3)
    return f.getvalue()


def deserialize(binary: bytes):
    """bytes to obj
    Args:
            binary (bytes): binary data of an object
    Returns:
            obj (Any): deserialized obj
    """
    f = io.BytesIO(binary)
    obj = joblib.load(f)
    return obj
