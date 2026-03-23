import logging
from pathlib import Path
from typing import Union, get_args, get_origin

logger = logging.getLogger(__name__)

# Python 类型 -> JSON Schema 类型映射
TYPE_MAP = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    list: "array",
    dict: "object",
}

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB


def _get_json_type(py_type) -> str:
    """Python 类型转 JSON Schema 类型"""
    if get_origin(py_type) is Union:
        args = [a for a in get_args(py_type) if a is not type(None)]
        if args:
            return _get_json_type(args[0])
    origin = get_origin(py_type)
    if origin is not None:
        return TYPE_MAP.get(origin, "string")
    return TYPE_MAP.get(py_type, "string")


def compress_image_if_needed(file_path: str) -> str:
    """如果图片超过 10MB，压缩后返回压缩文件路径；否则返回原路径。"""
    path = Path(file_path)
    if path.suffix.lower() not in _IMAGE_EXTENSIONS:
        return file_path
    if not path.is_file():
        return file_path

    file_size = path.stat().st_size
    if file_size <= _MAX_IMAGE_SIZE:
        return file_path

    try:
        from PIL import Image

        compressed_path = path.parent / f"{path.stem}_compressed.jpg"
        with Image.open(path) as img:
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            quality = 85
            while quality >= 20:
                img.save(compressed_path, "JPEG", quality=quality, optimize=True)
                if compressed_path.stat().st_size <= _MAX_IMAGE_SIZE:
                    break
                quality -= 10

            if compressed_path.stat().st_size > _MAX_IMAGE_SIZE:
                max_dim = 2048
                img.thumbnail((max_dim, max_dim), Image.LANCZOS)
                img.save(compressed_path, "JPEG", quality=60, optimize=True)

        logger.info(
            "Image compressed: %s -> %s (%d -> %d bytes)",
            path.name,
            compressed_path.name,
            file_size,
            compressed_path.stat().st_size,
        )
        return str(compressed_path)
    except Exception:
        logger.warning("Failed to compress image %s, using original", file_path)
        return file_path


IMAGE_MIME_MAP = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
}


def encode_image_bytes_to_data_uri(data: bytes, *, mime: str) -> str | None:
    import base64

    try:
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        logger.warning("Failed to encode image bytes")
        return None


def encode_image_to_data_uri(file_path: str) -> str | None:
    """将本地图片文件编码为 base64 data URI。"""
    import base64

    path = Path(file_path)
    if not path.is_file():
        logger.warning("Image file not found: %s", file_path)
        return None

    mime = IMAGE_MIME_MAP.get(path.suffix.lower(), "image/jpeg")
    try:
        data = path.read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        logger.warning("Failed to encode image %s", file_path)
        return None
