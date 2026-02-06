"""
Export ALL NVDB objekttyper (med geometri) i Finnmark til én GeoPackage (.gpkg),
med ett lag per objekttype.

✅ Ingen hardkodet OBJTYPE_ID
✅ Henter objekttyper automatisk
✅ Tar bare med objekttyper som har geometri
✅ Skriver til ny fil per kjøring (unngår WinError 32 / fil-lås i QGIS)
"""

from nvdbapiv4 import nvdbFagdata, nvdbObjekttyper
import geopandas as gpd
from shapely.geometry import shape
from shapely import wkt
from pathlib import Path
from datetime import datetime
import json
import re


# --- Innstillinger ---
FYLKE = 56  # Finnmark
CRS = "EPSG:5973"  # NVDB default
OUT_DIR = Path(".")
BASENAME = "nvdb_finnmark_all"
INCLUDE_PROPERTIES_AS_JSON = True  # lagrer hele objektet (uten geometri) som JSON i kolonnen "properties"


def safe_layer_name(name: str, fallback: str) -> str:
    """
    GPKG layer names are usually fine, but keep it simple/portable:
    - only letters/numbers/underscore
    - max length trimmed
    """
    if not name:
        name = fallback
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = fallback
    return name[:50]  # keep reasonably short


def parse_geometry(geom):
    """Robust geometry parsing for NVDB responses."""
    if not geom:
        return None

    # Case A: GeoJSON-like dict
    if isinstance(geom, dict):
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if isinstance(gtype, str) and coords is not None:
            try:
                return shape(geom)
            except Exception:
                pass

        # Case B: WKT inside dict
        wkt_text = geom.get("wkt") or geom.get("WKT")
        if isinstance(wkt_text, str) and wkt_text.strip():
            try:
                return wkt.loads(wkt_text)
            except Exception:
                return None

        return None

    # Case C: WKT directly as string
    if isinstance(geom, str) and geom.strip():
        try:
            return wkt.loads(geom)
        except Exception:
            return None

    return None


def main():
    # Ny fil per kjøring (unngår lås-problemer)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = OUT_DIR / f"{BASENAME}_{ts}.gpkg"

    # Hent alle objekttyper
    ot = nvdbObjekttyper()

    # Filtrer til de som har geometri (ellers blir det masse "skipped")
    # NB: noen ganger heter feltet litt ulikt – vi håndterer begge.
    objtypes = []
    for o in ot:
        has_geom = o.get("harGeometri")
        if has_geom is None:
            has_geom = o.get("har_geometri")  # fallback om API endrer casing
        if has_geom:
            objtypes.append(o)

    print(f"Fant {len(objtypes)} objekttyper med geometri. Starter eksport for fylke={FYLKE}…")
    print(f"Output: {outfile}\n")

    total_features = 0
    total_skipped_no_geom = 0
    total_skipped_bad_geom = 0
    failed_types = 0

    # Vi skriver ett lag per objekttype
    for idx, o in enumerate(objtypes, start=1):
        objtype_id = o.get("id")
        objtype_name = o.get("navn") or f"objtype_{objtype_id}"
        layer = safe_layer_name(objtype_name, fallback=f"objtype_{objtype_id}")

        print(f"[{idx}/{len(objtypes)}] Objekttype {objtype_id} - {objtype_name} -> layer '{layer}'")

        try:
            nvdb = nvdbFagdata(objtype_id)
            nvdb.filter({"fylke": FYLKE})
        except Exception as e:
            failed_types += 1
            print(f"  !! Klarte ikke å sette opp spørring for {objtype_id}: {e}\n")
            continue

        rows = []
        skipped_no_geom = 0
        skipped_bad_geom = 0
        fetched = 0

        try:
            for obj in nvdb:
                fetched += 1
                geom = obj.get("geometri")
                geometry = parse_geometry(geom)

                if not geom:
                    skipped_no_geom += 1
                    continue
                if geometry is None:
                    skipped_bad_geom += 1
                    continue

                rec = {
                    "id": obj.get("id"),
                    "objekttype": objtype_id,
                    "objekttype_navn": objtype_name,
                    "geometry": geometry,
                }

                if INCLUDE_PROPERTIES_AS_JSON:
                    # lagre resten av objektet (uten geometri) som JSON tekst
                    obj_copy = dict(obj)
                    obj_copy.pop("geometri", None)
                    rec["properties"] = json.dumps(obj_copy, ensure_ascii=False)

                rows.append(rec)

        except Exception as e:
            failed_types += 1
            print(f"  !! Feil under henting av {objtype_id}: {e}\n")
            continue

        if not rows:
            print(f"  -> 0 features (fetched={fetched}, skipped_no_geom={skipped_no_geom}, skipped_bad_geom={skipped_bad_geom})\n")
            total_skipped_no_geom += skipped_no_geom
            total_skipped_bad_geom += skipped_bad_geom
            continue

        gdf = gpd.GeoDataFrame(rows, crs=CRS)

        # Skriv lag:
        # - første lag: mode="w" (lager fil)
        # - videre lag: mode="a" (append)
        mode = "w" if not outfile.exists() else "a"

        try:
            gdf.to_file(outfile, layer=layer, driver="GPKG", mode=mode)
        except TypeError:
            # Noen eldre kombinasjoner av geopandas/fiona støtter ikke mode=
            # Da gjør vi: skriv første gang, ellers append med fiona via to_file uten mode (ofte fungerer for gpkg).
            # Hvis dette feiler hos deg, gi beskjed om versjonene så tilpasser vi.
            if not outfile.exists():
                gdf.to_file(outfile, layer=layer, driver="GPKG")
            else:
                gdf.to_file(outfile, layer=layer, driver="GPKG")

        print(f"  -> skrev {len(gdf)} features (skipped_no_geom={skipped_no_geom}, skipped_bad_geom={skipped_bad_geom})\n")

        total_features += len(gdf)
        total_skipped_no_geom += skipped_no_geom
        total_skipped_bad_geom += skipped_bad_geom

    print("FERDIG ✅")
    print(f"Fil: {outfile}")
    print(f"Totalt skrevet: {total_features}")
    print(f"Totalt skipped (no geom): {total_skipped_no_geom}")
    print(f"Totalt skipped (bad geom): {total_skipped_bad_geom}")
    print(f"Objekttyper som feilet: {failed_types}")


if __name__ == "__main__":
    main()
