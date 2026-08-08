from __future__ import annotations

from typing import Final


MEDIA_TYPES: Final[tuple[str, ...]] = (
    "image",
    "video",
    "text",
    "audio",
    "document",
    "other",
)

_IMAGE_EXTENSIONS = frozenset(
    {"avif", "bmp", "gif", "heic", "heif", "ico", "jpeg", "jpg", "png", "svg", "tif", "tiff", "webp"}
)
_VIDEO_EXTENSIONS = frozenset(
    {"3gp", "avi", "m2ts", "m4v", "mkv", "mov", "mp4", "mpeg", "mpg", "mts", "ogv", "webm", "wmv"}
)
_AUDIO_EXTENSIONS = frozenset(
    {"aac", "aiff", "flac", "m4a", "mp3", "oga", "ogg", "opus", "wav", "wma"}
)
_TEXT_EXTENSIONS = frozenset(
    {
        "c",
        "conf",
        "cpp",
        "css",
        "csv",
        "go",
        "h",
        "hpp",
        "htm",
        "html",
        "ini",
        "java",
        "js",
        "json",
        "jsonl",
        "log",
        "markdown",
        "md",
        "mjs",
        "py",
        "rb",
        "rs",
        "sh",
        "sql",
        "swift",
        "toml",
        "ts",
        "tsv",
        "txt",
        "xml",
        "yaml",
        "yml",
    }
)
_DOCUMENT_EXTENSIONS = frozenset(
    {
        "7z",
        "doc",
        "docx",
        "epub",
        "gz",
        "key",
        "numbers",
        "odp",
        "ods",
        "odt",
        "pages",
        "pdf",
        "ppt",
        "pptx",
        "rar",
        "rtf",
        "tar",
        "xls",
        "xlsx",
        "zip",
    }
)
_TEXT_APPLICATION_MIME_TYPES = frozenset(
    {
        "application/javascript",
        "application/json",
        "application/ld+json",
        "application/sql",
        "application/toml",
        "application/x-httpd-php",
        "application/x-javascript",
        "application/x-sh",
        "application/x-yaml",
        "application/xhtml+xml",
        "application/xml",
        "application/yaml",
    }
)
_GENERIC_MIME_TYPES = frozenset({"", "application/octet-stream", "binary/octet-stream"})


def media_filename(file_path: object) -> str:
    """Return a lexical basename without consulting the local filesystem."""

    value = "" if file_path is None else str(file_path)
    return value.replace("\\", "/").rsplit("/", 1)[-1]


def media_file_extension(file_path: object) -> str:
    """Return the final lexical filename extension, case-folded and without a dot."""

    filename = media_filename(file_path)
    dot_index = filename.rfind(".")
    if dot_index <= 0 or dot_index == len(filename) - 1:
        return ""
    return filename[dot_index + 1 :].casefold()


def normalize_file_extension_filter(value: object) -> str:
    normalized = str(value).strip().casefold().removeprefix(".")
    if not normalized or "." in normalized or "/" in normalized or "\\" in normalized:
        raise ValueError("file_extension must be one extension with an optional leading dot")
    return normalized


def normalize_media_type_filter(value: object) -> str:
    normalized = str(value)
    if normalized not in MEDIA_TYPES:
        allowed = ", ".join(MEDIA_TYPES)
        raise ValueError(f"media_type must be one of: {allowed}")
    return normalized


def media_filename_contains(file_path: object, query: object) -> bool:
    return str(query).casefold() in media_filename(file_path).casefold()


def classify_media_type(media_kind: object, mime_type: object, file_path: object) -> str:
    """Classify stored media deterministically from metadata and lexical extension."""

    mime = str(mime_type or "").split(";", 1)[0].strip().casefold()
    extension = media_file_extension(file_path)
    kind = "".join(char for char in str(media_kind or "").casefold() if char.isalnum())

    if mime.startswith("image/"):
        return "image"
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "audio"
    if (
        mime.startswith("text/")
        or mime in _TEXT_APPLICATION_MIME_TYPES
        or mime.endswith("+json")
        or mime.endswith("+xml")
    ):
        return "text"

    if extension in _IMAGE_EXTENSIONS:
        return "image"
    if extension in _VIDEO_EXTENSIONS:
        return "video"
    if extension in _AUDIO_EXTENSIONS:
        return "audio"
    if extension in _TEXT_EXTENSIONS:
        return "text"
    if extension in _DOCUMENT_EXTENSIONS:
        return "document"

    if "photo" in kind or "image" in kind or "sticker" in kind:
        return "image"
    if "video" in kind or "animation" in kind:
        return "video"
    if "audio" in kind or "voice" in kind:
        return "audio"
    if "text" in kind:
        return "text"
    if "document" in kind or "file" in kind:
        return "document"

    if mime not in _GENERIC_MIME_TYPES:
        return "document"
    return "other"
