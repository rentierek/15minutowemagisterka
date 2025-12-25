"""
Skrypt do pobierania i czyszczenia danych o transakcjach nieruchomości
Źródło: MSIP Kraków - Rejestr Cen i Wartości Nieruchomości

Rynki: Pierwotny i Wtórny
Okres: 2023-2025

CZYSZCZENIE DANYCH:
1. Usuwamy transakcje z ceną < 5000 zł/m² (niewiarygodne)
2. Liczymy średnią tylko dla jednostek z >= 10 transakcjami w roku
"""

import requests
import json
from pathlib import Path
import csv

# Konfiguracja
BASE_URLS = {
    "pierwotny": "https://msip.um.krakow.pl/arcgis/rest/services/ZSOZ/Rc_Lokale_m_Rp_Ju/MapServer/0",
    "wtorny": "https://msip.um.krakow.pl/arcgis/rest/services/ZSOZ/Rc_Lokale_m_Rw_Ju/MapServer/0"
}

OUTPUT_DIR = Path(__file__).parent / "data"
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"

# Filtr czasowy - 2023, 2024, 2025
WHERE_CLAUSE = "data_zaw_year >= 2023"

# Progi czyszczenia
MIN_PRICE_M2 = 5000  # Minimalna wiarygodna cena za m²
MIN_TRANSACTIONS_PER_YEAR = 5  # Minimum transakcji w roku dla jednostki


def get_query_params():
    return {
        "where": WHERE_CLAUSE,
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326",
        "returnGeometry": "true",
    }


def get_record_count(base_url):
    params = {
        "where": WHERE_CLAUSE,
        "returnCountOnly": "true",
        "f": "json"
    }
    response = requests.get(f"{base_url}/query", params=params)
    response.raise_for_status()
    return response.json().get("count", 0)


def download_features(base_url, offset=0, limit=2000):
    params = get_query_params()
    params["resultOffset"] = offset
    params["resultRecordCount"] = limit
    
    response = requests.get(f"{base_url}/query", params=params)
    response.raise_for_status()
    return response.json()


def merge_geojson(features_list):
    all_features = []
    for geojson in features_list:
        if "features" in geojson:
            all_features.extend(geojson["features"])
    return {"type": "FeatureCollection", "features": all_features}


def download_market_data(market_name, base_url):
    print(f"\n{'='*60}")
    print(f"📦 Pobieranie: RYNEK {market_name.upper()}")
    print(f"{'='*60}")
    
    total_count = get_record_count(base_url)
    print(f"📊 Znaleziono {total_count} rekordów")
    
    if total_count == 0:
        return None
    
    all_features = []
    offset = 0
    batch_size = 2000
    
    while offset < total_count:
        print(f"⏳ Pobieranie {offset + 1} - {min(offset + batch_size, total_count)}...")
        try:
            geojson = download_features(base_url, offset=offset, limit=batch_size)
            all_features.append(geojson)
            fetched = len(geojson.get("features", []))
            if fetched == 0:
                break
            offset += batch_size
        except requests.exceptions.RequestException as e:
            print(f"❌ Błąd: {e}")
            break
    
    merged = merge_geojson(all_features)
    print(f"✅ Pobrano {len(merged['features'])} rekordów")
    return merged


def clean_data(geojson, market_name):
    """Czyści dane wg zdefiniowanych reguł."""
    print(f"\n🧹 Czyszczenie danych ({market_name})...")
    
    original_count = len(geojson["features"])
    removed_price = 0
    
    # Krok 1: Usuń transakcje z ceną < 5000 zł/m²
    cleaned_features = []
    for f in geojson["features"]:
        price = f["properties"].get("sr_cena_m2", 0)
        if price >= MIN_PRICE_M2:
            cleaned_features.append(f)
        else:
            removed_price += 1
    
    print(f"   ❌ Usunięto {removed_price} rekordów (cena < {MIN_PRICE_M2} zł/m²)")
    
    # Krok 2: Agreguj per jednostka/rok i sprawdź minimalną liczbę transakcji
    unit_year_stats = {}
    
    for f in cleaned_features:
        props = f["properties"]
        unit_name = props.get("nazwa_jedn", "Nieznana")
        year = props.get("data_zaw_year", 2023)
        key = (unit_name, year)
        
        if key not in unit_year_stats:
            unit_year_stats[key] = {
                "transactions": 0,
                "features": []
            }
        
        unit_year_stats[key]["transactions"] += props.get("lkl_count", 0)
        unit_year_stats[key]["features"].append(f)
    
    # Filtruj tylko te z >= MIN_TRANSACTIONS
    final_features = []
    removed_low_count = 0
    
    for key, data in unit_year_stats.items():
        if data["transactions"] >= MIN_TRANSACTIONS_PER_YEAR:
            final_features.extend(data["features"])
        else:
            removed_low_count += len(data["features"])
    
    print(f"   ❌ Usunięto {removed_low_count} rekordów (< {MIN_TRANSACTIONS_PER_YEAR} transakcji/rok)")
    print(f"   ✅ Pozostało {len(final_features)} rekordów (z {original_count})")
    
    return {"type": "FeatureCollection", "features": final_features}


def aggregate_for_map(geojson, market_name):
    """Agreguje dane do formatu dla mapy."""
    units = {}
    
    for f in geojson["features"]:
        props = f["properties"]
        unit_name = props.get("nazwa_jedn", "Nieznana")
        year = props.get("data_zaw_year", 2023)
        area_m2 = props.get("st_area(shape)", 0)
        
        key = (unit_name, year)
        
        if key not in units:
            units[key] = {
                "nazwa": unit_name,
                "rok": year,
                "rynek": market_name,
                "transakcje": 0,
                "ceny_m2": [],
                "powierzchnia_m2": area_m2,
                "geometry": f.get("geometry")
            }
        
        units[key]["transakcje"] += props.get("lkl_count", 0)
        if props.get("sr_cena_m2"):
            units[key]["ceny_m2"].append(props["sr_cena_m2"])
    
    return units


def create_map_html(pierwotny_data, wtorny_data, output_file):
    """Generuje HTML z mapą Leaflet."""
    
    all_features = []
    
    for data, market in [(pierwotny_data, "pierwotny"), (wtorny_data, "wtorny")]:
        for key, unit in data.items():
            if unit["geometry"]:
                avg_price = sum(unit["ceny_m2"]) / len(unit["ceny_m2"]) if unit["ceny_m2"] else 0
                area_ha = round(unit.get("powierzchnia_m2", 0) / 10000, 1)
                all_features.append({
                    "type": "Feature",
                    "geometry": unit["geometry"],
                    "properties": {
                        "nazwa": unit["nazwa"],
                        "rok": unit["rok"],
                        "rynek": market,
                        "transakcje": unit["transakcje"],
                        "srednia_cena_m2": round(avg_price, 0),
                        "powierzchnia_ha": area_ha
                    }
                })
    
    geojson_data = json.dumps({
        "type": "FeatureCollection",
        "features": all_features
    }, ensure_ascii=False)
    
    html = f'''<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mapa Transakcji Nieruchomości - Kraków 2023-2025</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
        
        #map {{ height: calc(100vh - 80px); width: 100%; }}
        
        .header {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: white;
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        }}
        
        .header h1 {{ font-size: 1.3rem; font-weight: 600; }}
        
        .controls {{
            display: flex;
            gap: 15px;
            align-items: center;
            flex-wrap: wrap;
        }}
        
        .control-group {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .control-group label {{
            font-size: 0.85rem;
            color: #a0a0a0;
        }}
        
        select {{
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid #3a3a5a;
            background: #2a2a4a;
            color: white;
            font-size: 0.9rem;
            cursor: pointer;
        }}
        
        select:hover {{ border-color: #5a5a8a; }}
        
        .legend {{
            position: absolute;
            bottom: 30px;
            right: 20px;
            background: rgba(26, 26, 46, 0.95);
            padding: 15px;
            border-radius: 10px;
            color: white;
            z-index: 1000;
            min-width: 200px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }}
        
        .legend h4 {{
            margin-bottom: 10px;
            font-size: 0.9rem;
            color: #a0a0a0;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 5px 0;
            font-size: 0.85rem;
        }}
        
        .legend-color {{
            width: 20px;
            height: 20px;
            border-radius: 4px;
        }}
        
        .stats {{
            position: absolute;
            top: 100px;
            left: 20px;
            background: rgba(26, 26, 46, 0.95);
            padding: 15px;
            border-radius: 10px;
            color: white;
            z-index: 1000;
            min-width: 240px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }}
        
        .stats h4 {{
            margin-bottom: 10px;
            font-size: 0.9rem;
            color: #a0a0a0;
        }}
        
        .stat-row {{
            display: flex;
            justify-content: space-between;
            margin: 8px 0;
            font-size: 0.85rem;
        }}
        
        .stat-value {{
            font-weight: 600;
            color: #4ecca3;
        }}
        
        .cleaning-info {{
            position: absolute;
            bottom: 30px;
            left: 20px;
            background: rgba(26, 26, 46, 0.95);
            padding: 12px;
            border-radius: 10px;
            color: #888;
            z-index: 1000;
            font-size: 0.75rem;
            max-width: 250px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏠 Transakcje Nieruchomości - Kraków 2023-2025</h1>
        <div class="controls">
            <div class="control-group">
                <label>Rynek:</label>
                <select id="marketFilter">
                    <option value="all">Wszystkie (suma)</option>
                    <option value="pierwotny">Pierwotny</option>
                    <option value="wtorny">Wtórny</option>
                </select>
            </div>
            <div class="control-group">
                <label>Rok:</label>
                <select id="yearFilter">
                    <option value="all">Wszystkie lata (suma)</option>
                    <option value="2023">2023</option>
                    <option value="2024">2024</option>
                    <option value="2025">2025</option>
                </select>
            </div>
            <div class="control-group">
                <label>Koloruj wg:</label>
                <select id="colorBy">
                    <option value="price">Cena/m²</option>
                    <option value="transactions">Liczba transakcji</option>
                </select>
            </div>
        </div>
    </div>
    
    <div id="map"></div>
    
    <div class="stats" id="statsPanel">
        <h4>📊 Statystyki (widoczne obszary)</h4>
        <div class="stat-row">
            <span>Jednostki:</span>
            <span class="stat-value" id="statUnits">-</span>
        </div>
        <div class="stat-row">
            <span>Transakcje:</span>
            <span class="stat-value" id="statTransactions">-</span>
        </div>
        <div class="stat-row">
            <span>Śr. cena/m²:</span>
            <span class="stat-value" id="statAvgPrice">-</span>
        </div>
        <div class="stat-row">
            <span>Min cena/m²:</span>
            <span class="stat-value" id="statMinPrice">-</span>
        </div>
        <div class="stat-row">
            <span>Max cena/m²:</span>
            <span class="stat-value" id="statMaxPrice">-</span>
        </div>
    </div>
    
    <div class="legend" id="legend">
        <h4>🎨 Legenda (cena/m²)</h4>
        <div class="legend-item"><div class="legend-color" style="background: #2ecc71;"></div><span>&lt; 12 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #f1c40f;"></div><span>12 000 - 16 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>16 000 - 20 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 20 000 zł</span></div>
    </div>
    
    <div class="cleaning-info">
        ⚠️ <strong>Czyszczenie danych:</strong><br>
        • Ceny &lt; 5 000 zł/m² usunięte<br>
        • Min. 10 transakcji/jednostkę/rok
    </div>
    
    <script>
        const data = {geojson_data};
        
        const map = L.map('map').setView([50.06, 19.94], 12);
        
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '&copy; OpenStreetMap, &copy; CARTO',
            maxZoom: 19
        }}).addTo(map);
        
        let geoJsonLayer = null;
        
        function getPriceColor(price) {{
            if (price < 12000) return '#2ecc71';
            if (price < 16000) return '#f1c40f';
            if (price < 20000) return '#e67e22';
            return '#e74c3c';
        }}
        
        function getTransactionColor(count) {{
            if (count < 100) return '#3498db';
            if (count < 500) return '#9b59b6';
            if (count < 1000) return '#e67e22';
            return '#e74c3c';
        }}
        
        function updateLegend(colorBy) {{
            const legend = document.getElementById('legend');
            if (colorBy === 'price') {{
                legend.innerHTML = `
                    <h4>🎨 Legenda (cena/m²)</h4>
                    <div class="legend-item"><div class="legend-color" style="background: #2ecc71;"></div><span>&lt; 12 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #f1c40f;"></div><span>12 000 - 16 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>16 000 - 20 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 20 000 zł</span></div>
                `;
            }} else {{
                legend.innerHTML = `
                    <h4>🎨 Legenda (transakcje)</h4>
                    <div class="legend-item"><div class="legend-color" style="background: #3498db;"></div><span>&lt; 100</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #9b59b6;"></div><span>100 - 500</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>500 - 1000</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 1000</span></div>
                `;
            }}
        }}
        
        function updateMap() {{
            const marketFilter = document.getElementById('marketFilter').value;
            const yearFilter = document.getElementById('yearFilter').value;
            const colorBy = document.getElementById('colorBy').value;
            
            // Agreguj dane per jednostka
            const aggregated = {{}};
            
            data.features.forEach(f => {{
                const props = f.properties;
                
                // Filtruj po rynku
                if (marketFilter !== 'all' && props.rynek !== marketFilter) return;
                // Filtruj po roku
                if (yearFilter !== 'all' && props.rok !== parseInt(yearFilter)) return;
                
                // Klucz - tylko nazwa (agregujemy wszystkie lata/rynki)
                const key = props.nazwa;
                
                if (!aggregated[key]) {{
                    aggregated[key] = {{
                        type: 'Feature',
                        geometry: f.geometry,
                        properties: {{
                            nazwa: props.nazwa,
                            transakcje: 0,
                            suma_cen: 0,
                            liczba_cen: 0,
                            wszystkie_ceny: [],
                            powierzchnia_ha: props.powierzchnia_ha || 0
                        }}
                    }};
                }}
                
                aggregated[key].properties.transakcje += props.transakcje;
                if (props.srednia_cena_m2 > 0) {{
                    aggregated[key].properties.suma_cen += props.srednia_cena_m2 * props.transakcje;
                    aggregated[key].properties.liczba_cen += props.transakcje;
                    aggregated[key].properties.wszystkie_ceny.push(props.srednia_cena_m2);
                }}
            }});
            
            // Oblicz średnie ważone
            const filteredFeatures = Object.values(aggregated).map(f => {{
                f.properties.srednia_cena_m2 = f.properties.liczba_cen > 0 
                    ? Math.round(f.properties.suma_cen / f.properties.liczba_cen) 
                    : 0;
                return f;
            }});
            
            if (geoJsonLayer) {{
                map.removeLayer(geoJsonLayer);
            }}
            
            geoJsonLayer = L.geoJSON({{type: 'FeatureCollection', features: filteredFeatures}}, {{
                style: function(feature) {{
                    const props = feature.properties;
                    const color = colorBy === 'price' 
                        ? getPriceColor(props.srednia_cena_m2)
                        : getTransactionColor(props.transakcje);
                    
                    return {{
                        fillColor: color,
                        weight: 1,
                        opacity: 0.8,
                        color: '#ffffff',
                        fillOpacity: 0.7
                    }};
                }},
                onEachFeature: function(feature, layer) {{
                    const props = feature.properties;
                    layer.bindPopup(`
                        <div style="font-family: sans-serif; min-width: 220px;">
                            <h3 style="margin: 0 0 10px 0; color: #1a1a2e;">${{props.nazwa}}</h3>
                            <p style="margin: 5px 0;"><strong>Powierzchnia:</strong> ${{props.powierzchnia_ha}} ha</p>
                            <p style="margin: 5px 0;"><strong>Transakcje:</strong> ${{props.transakcje.toLocaleString('pl-PL')}}</p>
                            <p style="margin: 5px 0;"><strong>Śr. cena/m²:</strong> ${{props.srednia_cena_m2.toLocaleString('pl-PL')}} zł</p>
                        </div>
                    `);
                }}
            }}).addTo(map);
            
            // Statystyki
            let totalTransactions = 0;
            let allPrices = [];
            
            filteredFeatures.forEach(f => {{
                totalTransactions += f.properties.transakcje;
                if (f.properties.srednia_cena_m2 > 0) {{
                    allPrices.push(f.properties.srednia_cena_m2);
                }}
            }});
            
            const avgPrice = allPrices.length > 0 ? Math.round(allPrices.reduce((a,b) => a+b, 0) / allPrices.length) : 0;
            const minPrice = allPrices.length > 0 ? Math.min(...allPrices) : 0;
            const maxPrice = allPrices.length > 0 ? Math.max(...allPrices) : 0;
            
            document.getElementById('statUnits').textContent = filteredFeatures.length;
            document.getElementById('statTransactions').textContent = totalTransactions.toLocaleString('pl-PL');
            document.getElementById('statAvgPrice').textContent = avgPrice.toLocaleString('pl-PL') + ' zł';
            document.getElementById('statMinPrice').textContent = minPrice.toLocaleString('pl-PL') + ' zł';
            document.getElementById('statMaxPrice').textContent = maxPrice.toLocaleString('pl-PL') + ' zł';
            
            updateLegend(colorBy);
        }}
        
        document.getElementById('marketFilter').addEventListener('change', updateMap);
        document.getElementById('yearFilter').addEventListener('change', updateMap);
        document.getElementById('colorBy').addEventListener('change', updateMap);
        
        updateMap();
    </script>
</body>
</html>'''
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ Mapa zapisana: {output_file}")


def main():
    print("🏠 POBIERANIE I CZYSZCZENIE DANYCH - KRAKÓW 2023-2025")
    print(f"📅 Okres: 2023-2025")
    print(f"🧹 Czyszczenie: cena >= {MIN_PRICE_M2} zł/m², min {MIN_TRANSACTIONS_PER_YEAR} transakcji/rok")
    
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    all_pierwotny = {}
    all_wtorny = {}
    
    for market_name, base_url in BASE_URLS.items():
        # Pobierz
        geojson = download_market_data(market_name, base_url)
        
        if geojson and geojson["features"]:
            # Zapisz surowe
            raw_file = RAW_DIR / f"transactions_{market_name}_2023_2025_raw.geojson"
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, ensure_ascii=False)
            
            # Wyczyść
            cleaned = clean_data(geojson, market_name)
            
            # Zapisz oczyszczone
            clean_file = RAW_DIR / f"transactions_{market_name}_2023_2025_clean.geojson"
            with open(clean_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned, f, ensure_ascii=False)
            
            # Agreguj dla mapy
            if market_name == "pierwotny":
                all_pierwotny = aggregate_for_map(cleaned, market_name)
            else:
                all_wtorny = aggregate_for_map(cleaned, market_name)
    
    # Generuj mapę
    print(f"\n🗺️  Generowanie mapy...")
    map_file = OUTPUT_DIR / "mapa_transakcji_2023_2025.html"
    create_map_html(all_pierwotny, all_wtorny, map_file)
    
    # Podsumowanie
    print(f"\n{'='*60}")
    print("📈 PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"   Pierwotny: {len(all_pierwotny)} rekordów (jednostka/rok)")
    print(f"   Wtórny: {len(all_wtorny)} rekordów (jednostka/rok)")
    print(f"\n🌐 Otwórz: {map_file}")


if __name__ == "__main__":
    main()
