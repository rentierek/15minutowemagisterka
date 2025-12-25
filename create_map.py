"""
Skrypt do generowania interaktywnej mapy transakcji nieruchomości
Z podziałem na: jednostki urbanistyczne, rynek (pierwotny/wtórny), rok (2024/2025)
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
OUTPUT_FILE = DATA_DIR / "mapa_transakcji.html"

def load_geojson(filepath):
    """Wczytuje plik GeoJSON."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def aggregate_by_unit_year(geojson, market_name):
    """Agreguje dane per jednostka i rok."""
    units = {}
    
    for feature in geojson["features"]:
        props = feature.get("properties", {})
        unit_name = props.get("nazwa_jedn", "Nieznana")
        year = props.get("data_zaw_year", 2024)
        
        key = (unit_name, year)
        
        if key not in units:
            units[key] = {
                "nazwa": unit_name,
                "rok": year,
                "rynek": market_name,
                "transakcje": 0,
                "ceny_m2": [],
                "geometry": feature.get("geometry")
            }
        
        units[key]["transakcje"] += props.get("lkl_count", 0)
        if props.get("sr_cena_m2"):
            units[key]["ceny_m2"].append(props["sr_cena_m2"])
    
    return units

def create_map_html(pierwotny_data, wtorny_data):
    """Generuje HTML z mapą Leaflet."""
    
    # Przygotuj dane do JavaScript
    all_features = []
    
    for data, market in [(pierwotny_data, "pierwotny"), (wtorny_data, "wtorny")]:
        for key, unit in data.items():
            if unit["geometry"]:
                avg_price = sum(unit["ceny_m2"]) / len(unit["ceny_m2"]) if unit["ceny_m2"] else 0
                all_features.append({
                    "type": "Feature",
                    "geometry": unit["geometry"],
                    "properties": {
                        "nazwa": unit["nazwa"],
                        "rok": unit["rok"],
                        "rynek": market,
                        "transakcje": unit["transakcje"],
                        "srednia_cena_m2": round(avg_price, 0)
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
    <title>Mapa Transakcji Nieruchomości - Kraków 2024-2025</title>
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
        
        .header h1 {{ font-size: 1.4rem; font-weight: 600; }}
        
        .controls {{
            display: flex;
            gap: 15px;
            align-items: center;
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
            min-width: 220px;
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
    </style>
</head>
<body>
    <div class="header">
        <h1>🏠 Transakcje Nieruchomości - Kraków 2024-2025</h1>
        <div class="controls">
            <div class="control-group">
                <label>Rynek:</label>
                <select id="marketFilter">
                    <option value="all">Wszystkie</option>
                    <option value="pierwotny">Pierwotny</option>
                    <option value="wtorny">Wtórny</option>
                </select>
            </div>
            <div class="control-group">
                <label>Rok:</label>
                <select id="yearFilter">
                    <option value="all">2024-2025</option>
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
        <h4>📊 Statystyki (widoczne)</h4>
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
    </div>
    
    <div class="legend" id="legend">
        <h4>🎨 Legenda (cena/m²)</h4>
        <div class="legend-item"><div class="legend-color" style="background: #2ecc71;"></div><span>&lt; 10 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #f1c40f;"></div><span>10 000 - 15 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>15 000 - 20 000 zł</span></div>
        <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 20 000 zł</span></div>
    </div>
    
    <script>
        const data = {geojson_data};
        
        // Inicjalizacja mapy
        const map = L.map('map').setView([50.06, 19.94], 12);
        
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '&copy; OpenStreetMap, &copy; CARTO',
            maxZoom: 19
        }}).addTo(map);
        
        let geoJsonLayer = null;
        
        function getPriceColor(price) {{
            if (price < 10000) return '#2ecc71';
            if (price < 15000) return '#f1c40f';
            if (price < 20000) return '#e67e22';
            return '#e74c3c';
        }}
        
        function getTransactionColor(count) {{
            if (count < 50) return '#3498db';
            if (count < 200) return '#9b59b6';
            if (count < 500) return '#e67e22';
            return '#e74c3c';
        }}
        
        function updateLegend(colorBy) {{
            const legend = document.getElementById('legend');
            if (colorBy === 'price') {{
                legend.innerHTML = `
                    <h4>🎨 Legenda (cena/m²)</h4>
                    <div class="legend-item"><div class="legend-color" style="background: #2ecc71;"></div><span>&lt; 10 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #f1c40f;"></div><span>10 000 - 15 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>15 000 - 20 000 zł</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 20 000 zł</span></div>
                `;
            }} else {{
                legend.innerHTML = `
                    <h4>🎨 Legenda (transakcje)</h4>
                    <div class="legend-item"><div class="legend-color" style="background: #3498db;"></div><span>&lt; 50</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #9b59b6;"></div><span>50 - 200</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e67e22;"></div><span>200 - 500</span></div>
                    <div class="legend-item"><div class="legend-color" style="background: #e74c3c;"></div><span>&gt; 500</span></div>
                `;
            }}
        }}
        
        function updateMap() {{
            const marketFilter = document.getElementById('marketFilter').value;
            const yearFilter = document.getElementById('yearFilter').value;
            const colorBy = document.getElementById('colorBy').value;
            
            // Agreguj dane per jednostka (sumuj jeśli oba lata wybrane)
            const aggregated = {{}};
            
            data.features.forEach(f => {{
                const props = f.properties;
                
                // Filtruj
                if (marketFilter !== 'all' && props.rynek !== marketFilter) return;
                if (yearFilter !== 'all' && props.rok !== parseInt(yearFilter)) return;
                
                const key = props.nazwa + '_' + props.rynek;
                
                if (!aggregated[key]) {{
                    aggregated[key] = {{
                        type: 'Feature',
                        geometry: f.geometry,
                        properties: {{
                            nazwa: props.nazwa,
                            rynek: props.rynek,
                            transakcje: 0,
                            suma_cen: 0,
                            liczba_cen: 0
                        }}
                    }};
                }}
                
                aggregated[key].properties.transakcje += props.transakcje;
                if (props.srednia_cena_m2 > 0) {{
                    aggregated[key].properties.suma_cen += props.srednia_cena_m2;
                    aggregated[key].properties.liczba_cen += 1;
                }}
            }});
            
            // Oblicz średnie
            const filteredFeatures = Object.values(aggregated).map(f => {{
                f.properties.srednia_cena_m2 = f.properties.liczba_cen > 0 
                    ? Math.round(f.properties.suma_cen / f.properties.liczba_cen) 
                    : 0;
                return f;
            }});
            
            // Usuń starą warstwę
            if (geoJsonLayer) {{
                map.removeLayer(geoJsonLayer);
            }}
            
            // Dodaj nową
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
                    const rynekPL = props.rynek === 'pierwotny' ? 'Pierwotny' : 'Wtórny';
                    layer.bindPopup(`
                        <div style="font-family: sans-serif; min-width: 200px;">
                            <h3 style="margin: 0 0 10px 0; color: #1a1a2e;">${{props.nazwa}}</h3>
                            <p style="margin: 5px 0;"><strong>Rynek:</strong> ${{rynekPL}}</p>
                            <p style="margin: 5px 0;"><strong>Transakcje:</strong> ${{props.transakcje.toLocaleString('pl-PL')}}</p>
                            <p style="margin: 5px 0;"><strong>Śr. cena/m²:</strong> ${{props.srednia_cena_m2.toLocaleString('pl-PL')}} zł</p>
                        </div>
                    `);
                }}
            }}).addTo(map);
            
            // Aktualizuj statystyki
            let totalTransactions = 0;
            let totalPrices = 0;
            let countPrices = 0;
            
            filteredFeatures.forEach(f => {{
                totalTransactions += f.properties.transakcje;
                if (f.properties.srednia_cena_m2 > 0) {{
                    totalPrices += f.properties.srednia_cena_m2;
                    countPrices += 1;
                }}
            }});
            
            document.getElementById('statUnits').textContent = filteredFeatures.length;
            document.getElementById('statTransactions').textContent = totalTransactions.toLocaleString('pl-PL');
            document.getElementById('statAvgPrice').textContent = countPrices > 0 
                ? Math.round(totalPrices / countPrices).toLocaleString('pl-PL') + ' zł' 
                : '-';
            
            updateLegend(colorBy);
        }}
        
        // Event listeners
        document.getElementById('marketFilter').addEventListener('change', updateMap);
        document.getElementById('yearFilter').addEventListener('change', updateMap);
        document.getElementById('colorBy').addEventListener('change', updateMap);
        
        // Inicjalna aktualizacja
        updateMap();
    </script>
</body>
</html>'''
    
    return html


def main():
    print("🗺️  Generowanie mapy transakcji nieruchomości...")
    
    # Wczytaj dane
    pierwotny_file = RAW_DIR / "transactions_pierwotny_2024_2025.geojson"
    wtorny_file = RAW_DIR / "transactions_wtorny_2024_2025.geojson"
    
    if not pierwotny_file.exists() or not wtorny_file.exists():
        print("❌ Brak plików z danymi! Uruchom najpierw download_data.py")
        return
    
    pierwotny = load_geojson(pierwotny_file)
    wtorny = load_geojson(wtorny_file)
    
    print(f"📦 Wczytano: {len(pierwotny['features'])} rekordów (pierwotny)")
    print(f"📦 Wczytano: {len(wtorny['features'])} rekordów (wtórny)")
    
    # Agreguj dane
    pierwotny_agg = aggregate_by_unit_year(pierwotny, "pierwotny")
    wtorny_agg = aggregate_by_unit_year(wtorny, "wtorny")
    
    # Generuj HTML
    html = create_map_html(pierwotny_agg, wtorny_agg)
    
    # Zapisz
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ Mapa zapisana: {OUTPUT_FILE}")
    print(f"\n🌐 Otwórz plik w przeglądarce, aby zobaczyć mapę!")


if __name__ == "__main__":
    main()
