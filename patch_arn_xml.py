"""
Fill empty <Arn/> or <RoleARN></RoleARN> elements in XML *responses*.

Run (reverse‑proxy example):
    mitmdump -s patch_arn_xml.py \
             --mode reverse:http://localhost:9000 \
             --listen-port 9999 \
             --set keep_host_header=true          # keep SigV4 intact
"""

from mitmproxy import http, ctx
import xml.etree.ElementTree as ET
import gzip, zlib
from io import BytesIO

DUMMY_ARN = "arn:aws:iam::000000000000:role/DummyRole"

# ------------------------------------------------------------ helpers
def _decompress(body: bytes, enc: str) -> bytes:
    enc = enc.lower()
    if enc in ("gzip", "x-gzip"):
        return gzip.decompress(body)
    if enc == "deflate":
        try:
            return zlib.decompress(body)
        except zlib.error:
            return zlib.decompress(body, -zlib.MAX_WBITS)   # raw DEFLATE
    return body

def _compress(raw: bytes, enc: str) -> bytes:
    enc = enc.lower()
    if enc in ("gzip", "x-gzip"):
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(raw)
        return buf.getvalue()
    if enc == "deflate":
        return zlib.compress(raw)
    return raw


# ------------------------------------------------------------ main hook
def response(flow: http.HTTPFlow) -> None:
    
    ctype = flow.response.headers.get("content-type", "").lower()
    if "xml" not in ctype:                     # skip JSON / binaries
        return

    enc = flow.response.headers.get("content-encoding", "")
    raw = flow.response.raw_content or b""
    raw = _decompress(raw, enc) if enc else raw

    try:
        tree = ET.fromstring(raw.decode("utf-8"))
    except (UnicodeDecodeError, ET.ParseError):
        return                                 # not valid UTF‑8 XML → skip

    # Build a namespace map if the root is namespaced, e.g. {urn:aws...}Tag
    ns = {}
    if tree.tag.startswith("{"):
        uri, _local = tree.tag[1:].split("}", 1)
        ns = {"ns": uri}

    changed = False
    for tag in ("Arn", "RoleARN"):
        path = f".//{'ns:' if ns else ''}{tag}"
        for elem in tree.findall(path, ns):
            if not (elem.text or "").strip():
                elem.text = DUMMY_ARN
                changed = True

    if not changed:
        return                                 # nothing to patch

    new_xml = ET.tostring(tree, encoding="utf-8", xml_declaration=True)
    new_raw = _compress(new_xml, enc) if enc else new_xml

    flow.response.raw_content = new_raw
    flow.response.headers["content-length"] = str(len(new_raw))
    ctx.log.info(
        f"Patched empty ARN in XML response for {flow.request.pretty_url} "
        f"(encoding={enc or 'none'})"
    )
