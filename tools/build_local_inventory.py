#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Buduje Local Inventory feed (XML + TSV) na podstawie standardowego feedu Google (XML) z Shopera.

Wymagane zmienne środowiskowe (GitHub Secrets):
- SOURCE_FEED_URL – pełny URL do feedu Shopera (np. https://.../GoogleProductSearch/)
- STORE_CODE      – kod sklepu identyczny z tym w Merchant Center / Google Business Profile (np. MAIN)

Opcjonalne:
- DEFAULT_AVAILABILITY – domyślna dostępność (domyślnie: in_stock)
- OUT_BASENAME         – nazwa bazowa plików w dist/ (domyślnie: local_inventory)
"""
import os, sys, io, csv, re, datetime, pathlib, gzip, zlib
import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request

NS = {"g": "http://base.google.com/ns/1.0"}

SOURCE_FEED_URL = os.environ.get("SOURCE_FEED_URL")
STORE_CODE = os.environ.get("STORE_CODE")
DEFAULT_AVAILABILITY = os.environ.get("DEFAULT_AVAILABILITY", "in_stock")
OUT_BASENAME = os.environ.get("OUT_BASENAME", "local_inventory")

if not SOURCE_FEED_URL or not STORE_CODE:
    print("ERROR: missing SOURCE_FEED_URL or STORE_CODE env vars", file=sys.stderr)
    sys.exit(2)

_ALLOWED_AVAIL = {
    "in stock": "in_stock",
    "in_stock": "in_stock",
    "available": "in_stock",
    "preorder": "preorder",
    "out of stock": "out_of_stock",
    "out_of_stock": "out_of_stock",
    "on display to order": "on_display_to_order",
    "on_display_to_order": "on_display_to_order",
    "limited availability": "limited_availability",
    "limited_availability": "limited_availability",
    # polskie warianty na wszelki wypadek
    "dostępny": "in_stock",
    "niedostępny": "out_of_stock",
    "brak": "out_of_stock",
}

def normalize_availability(val: str) -> str:
    if not val:
        return DEFAULT_AVAILABILITY
    key = re.sub(r"\s+", " ", val.strip().lower())
    return _ALLOWED_AVAIL.get(key, DEFAULT_AVAILABILITY)

def _decompress_if_needed(data: bytes, encoding: str) -> bytes:
    if not encoding:
        return data
    enc = encoding.lower()
    try:
        if "gzip" in enc:
            return gzip.decompress(data)
        if "deflate" in enc:
            try:
                return zlib.decompress(data, -zlib.MAX_WBITS)
            except zlib.error:
                return zlib.decompress(data)
    except Exception:
        return data
    return data

def fetch_xml(url: str) -> ET.ElementTree:
    if not re.match(r'^https?://', url, re.I):
        url = 'https://' + url
    if not url.endswith("/"):
        url = url + "/"

    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LIA-Builder/1.1)",
            "Accept": "application/xml, text/xml, */*;q=0.1",
            "Accept-Encoding": "identity",
        },
    )

    last_err = None
    for attempt in range(3):
        try:
            with urlopen(req, timeout=90) as resp:
                status = getattr(resp, "status", 200)
                ctype = (resp.headers.get("Content-Type") or "").lower()
                cenc  = (resp.headers.get("Content-Encoding") or "").lower()
                raw   = resp.read()
            data = _decompress_if_needed(raw, cenc).strip()

            if not data:
                raise RuntimeError(f"Empty response body (HTTP {status}, Content-Type: {ctype})")
            if not data.startswith(b"<"):
                snippet = data[:200].decode("utf-8", "replace")
                raise RuntimeError(
                    f"Non-XML response (HTTP {status}, Content-Type: {ctype}). First bytes:\n{snippet}"
                )

            return ET.ElementTree(ET.fromstring(data))

        except Exception as e:
            last_err = e
            if attempt < 2:
                import time
                time.sleep(2 * (attempt + 1))
            else:
                print(f"ERROR: failed to fetch XML from {url!r}: {e}", file=sys.stderr)
                raise

def extract_items(tree: ET.ElementTree):
    root = tree.getroot()
    items = []

    # 1) klasyczny RSS: rss/channel/item
    for it in root.findall(".//item"):
        gid = it.findtext("g:id", default=None, namespaces=NS) or it.findtext("id")
        if not gid:
            continue
        availability = it.findtext("g:availability", default=None, namespaces=NS) or it.findtext("availability")
        price = it.findtext("g:price", default=None, namespaces=NS) or it.findtext("price")
        items.append({
            "id": gid.strip(),
            "availability": normalize_availability(availability) if availability else DEFAULT_AVAILABILITY,
            "price": price.strip() if price else None,
        })

    # 2) fallback bez niedozwolonego XPath-a:
    #    przejdź po wszystkich węzłach i bierz tylko te, które mają BEZPOŚREDNIE dziecko <g:id>
    if not items:
        seen = set()
        for node in root.iter():
            gid_el = node.find("g:id", NS)
            if gid_el is None or not (gid_el.text and gid_el.text.strip()):
                continue
            gid = gid_el.text.strip()
            if gid in seen:
                continue
            availability = node.findtext("g:availability", default=None, namespaces=NS)
            price = node.findtext("g:price", default=None, namespaces=NS)
            items.append({
                "id": gid,
                "availability": normalize_availability(availability) if availability else DEFAULT_AVAILABILITY,
                "price": price.strip() if price else None,
            })
            seen.add(gid)

    if not items:
        raise RuntimeError("No items with <g:id> found in source feed (unexpected source structure)")
    return items

def ensure_dist():
    outdir = pathlib.Path("dist")
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def write_xml(items, outpath: pathlib.Path):
    # Wymuś, by elementy z URI Google miały prefiks "g", a nie "ns0"
    ET.register_namespace('g', NS['g'])

    rss = ET.Element("rss", attrib={"version": "2.0"})
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "Local Inventory Feed"
    ET.SubElement(channel, "link").text = "https://example.invalid/"
    ET.SubElement(channel, "description").text = "Generated by LIA Builder"

    for it in items:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, ET.QName(NS["g"], "id")).text = it["id"]
        ET.SubElement(item, ET.QName(NS["g"], "store_code")).text = STORE_CODE
        ET.SubElement(item, ET.QName(NS["g"], "availability")).text = it["availability"]
        if it.get("price"):
            ET.SubElement(item, ET.QName(NS["g"], "price")).text = it["price"]

    tree = ET.ElementTree(rss)
    tree.write(outpath, encoding="utf-8", xml_declaration=True)


def write_tsv(items, outpath: pathlib.Path):
    with outpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["id", "store_code", "availability", "price"])
        for it in items:
            w.writerow([it["id"], STORE_CODE, it["availability"], it.get("price") or ""])

def main():
    print("INFO: fetching source XML ...", file=sys.stderr)
    tree = fetch_xml(SOURCE_FEED_URL)
    print("INFO: parsing items ...", file=sys.stderr)
    items = extract_items(tree)
    outdir = ensure_dist()

    xml_out = outdir / f"{OUT_BASENAME}.xml"
    tsv_out = outdir / f"{OUT_BASENAME}.tsv"

    write_xml(items, xml_out)
    write_tsv(items, tsv_out)

    print(f"Wrote: {xml_out} and {tsv_out} ({len(items)} rows)")

if __name__ == "__main__":
    main()
