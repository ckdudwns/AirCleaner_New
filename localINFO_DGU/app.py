# app.py
from flask import Flask, request, render_template, redirect, url_for, Response, jsonify
import re
import requests
import urllib.parse
from urllib.parse import quote
from pyproj import Transformer
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import csv
import io
import math
import pandas as pd
import os
import ssl
from requests.adapters import HTTPAdapter

app = Flask(__name__)

# --- SSL 오류 해결을 위한 커스텀 어댑터 및 세션 ---
class CustomHttpAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.set_ciphers('DEFAULT:@SECLEVEL=1')
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount('https://', CustomHttpAdapter())
# ----------------------------------------------------

# 🔑 API 키
KAKAO_API_KEY = "7c5ffe1b2f9e318d2bfa882a539bb429"
AIRKOREA_SERVICE_KEY = "tlBcA73yJuLT1PSGixHpbHwLcINQEVtZ0g5xfd2E5/+qZUSmPK1hSFACjbw+pauS2glnKPhOPUcniVoBRkGfpA=="

# ---------------------------
# 데이터 로드
# ---------------------------
HISTORICAL_DATA_FILE = r'C:\Users\chaye\localINFO_DGU\annual_pm_averages.csv'
historical_data = None
try:
    print(f"'{HISTORICAL_DATA_FILE}' 파일을 로드합니다.")
    try:
        historical_data = pd.read_csv(HISTORICAL_DATA_FILE, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"UTF-8 로딩 실패. 'cp949'로 다시 시도합니다.")
        historical_data = pd.read_csv(HISTORICAL_DATA_FILE, encoding='cp949')
except FileNotFoundError:
    print(f"경고: '{HISTORICAL_DATA_FILE}' 파일을 찾을 수 없습니다.")
except Exception as e:
    print(f"'{HISTORICAL_DATA_FILE}' 파일 로드 중 오류 발생: {e}")

# ---------------------------
# 유틸리티 및 헬퍼 함수
# ---------------------------
def preprocess_address(address: str) -> str:
    address = address.strip()
    address = re.sub(r'\s+', ' ', address)
    address = re.sub(r'[(),."\'`]', '', address)
    address = re.sub(r'\s+\d{1,4}(?:층|호|동)\s*$', '', address)
    return address

def is_valid_road_address(address: str) -> bool:
    pattern = r"^[가-힣]+\s[가-힣]+\s[가-힣]+\s[가-힣0-9]+(?:로|길)\s?\d{1,3}(?:-\d{1,3})?$"
    return bool(re.match(pattern, address.strip()))

def convert_to_tm(lat: float, lon: float):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y

def _to_float(x):
    try:
        if x is None: return None
        s = str(x).strip()
        if s in ["", "-", "NA", "null", "None"]: return None
        return float(s)
    except (ValueError, TypeError):
        return None

def _mean(values):
    arr = [v for v in values if isinstance(v, (int, float)) and not math.isnan(v)]
    if not arr: return None
    return sum(arr) / len(arr)

def get_grade_label(grade: str) -> str:
    grade_map = {'1': '좋음', '2': '보통', '3': '나쁨', '4': '매우나쁨'}
    return grade_map.get(str(grade), "N/A")

def _create_csv_response(filename: str, header: list, rows: list[dict]):
    si = io.StringIO()
    cw = csv.DictWriter(si, fieldnames=header)
    cw.writeheader()
    cw.writerows(rows)
    output = si.getvalue().encode("utf-8-sig")
    encoded_filename = quote(filename)
    return Response(
        output,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=\"download.csv\"; filename*=UTF-8''{encoded_filename}"}
    )

# ---------------------------
# AirKorea API 헬퍼 함수 (데이터 조합 로직 적용)
# ---------------------------
def get_nearby_stations_with_network(tmX, tmY, limit=3):
    msr_url = "https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getNearbyMsrstnList"
    msr_params = {"serviceKey": AIRKOREA_SERVICE_KEY, "returnType": "json", "tmX": tmX, "tmY": tmY, "ver": "1.0"}
    r = session.get(msr_url, params=msr_params, timeout=5)
    r.raise_for_status()
    items = r.json().get("response", {}).get("body", {}).get("items", []) or []
    detailed = []
    for it in items[:limit]:
        st_name = it.get("stationName")
        network_type = get_station_network_type(st_name)
        detailed.append({"stationName": st_name, "addr": it.get("addr"), "distance": it.get("tm"), "network_type": network_type})
    return detailed

def get_station_network_type(station_name: str):
    if not station_name: return None
    url = "https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList"
    params = {"serviceKey": AIRKOREA_SERVICE_KEY, "returnType": "json", "stationName": station_name}
    try:
        r = session.get(url, params=params, timeout=5)
        r.raise_for_status()
        items = r.json().get("response", {}).get("body", {}).get("items", []) or []
        return items[0].get("mangName") if items else None
    except Exception:
        return None

def get_realtime_pm(station_name: str):
    url = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty"
    p = {"serviceKey": AIRKOREA_SERVICE_KEY, "stationName": station_name, "dataTerm": "DAILY", "ver": "1.3", "pageNo": "1", "numOfRows": "1", "returnType": "json"}
    r = session.get(url, params=p, timeout=5)
    r.raise_for_status()
    items = r.json().get("response", {}).get("body", {}).get("items", []) or []
    if not items: return None
    it = items[0]
    return {"timestamp": it.get("dataTime"), "pm10_ug_m3": _to_float(it.get("pm10Value")), "pm2_5_ug_m3": _to_float(it.get("pm25Value")), "pm10_category": get_grade_label(it.get("pm10Grade")), "pm2_5_category": get_grade_label(it.get("pm25Grade"))}

def _get_monthly_stats_from_api(station_name: str, begin_mm: str, end_mm: str):
    """(내부 함수) AirKorea API를 통해서만 월별 통계를 가져옵니다."""
    try:
        url = "https://apis.data.go.kr/B552584/ArpltnStatsSvc/getMsrstnAcctoRMmrg"
        p = {"serviceKey": AIRKOREA_SERVICE_KEY, "returnType": "json", "inqBginMm": begin_mm, "inqEndMm": end_mm, "msrstnName": station_name, "pageNo": "1", "numOfRows": "120"}
        r = session.get(url, params=p, timeout=8)
        r.raise_for_status()
        data = r.json() or {}
        header = data.get("response", {}).get("header", {})
        if header and header.get("resultCode") != "00":
            raise RuntimeError(f"AirKorea API Error: {header.get('resultMsg')}")
        items = data.get("response", {}).get("body", {}).get("items", []) or []
        if not items: return []
        
        print(f"✅ API에서 '{station_name}' 데이터 {len(items)}건 조회 성공.")
        return [{"stationName": it.get("msrstnName") or station_name, "month": it.get("msurMm"), "pm10_avg": _to_float(it.get("pm10Value")), "pm25_avg": _to_float(it.get("pm25Value"))} for it in items]
    except Exception as e:
        print(f"⚠️ API 조회 실패 (_get_monthly_stats_from_api): {e}")
        return []

def _get_monthly_stats_from_csv(station_name: str, months_to_find: set):
    """(내부 함수) 로컬 CSV 파일에서 특정 월의 데이터를 가져옵니다."""
    if historical_data is None or historical_data.empty or not months_to_find:
        return []
    try:
        df_station = historical_data[historical_data['측정소명'] == station_name].copy()
        if df_station.empty: return []

        df_station['yyyymm'] = df_station['년'].astype(str) + df_station['월'].astype(str).str.zfill(2)
        df_filtered = df_station[df_station['yyyymm'].isin(months_to_find)]
        
        csv_results = []
        for _, row in df_filtered.iterrows():
            csv_results.append({
                "stationName": row['측정소명'], "month": row['yyyymm'],
                "pm10_avg": _to_float(row.get('PM10')), "pm25_avg": _to_float(row.get('PM2.5'))
            })
        return csv_results
    except Exception as e:
        print(f"❌ CSV 데이터 처리 중 오류 발생 (_get_monthly_stats_from_csv): {e}")
        return []

def get_monthly_stats(station_name: str, begin_mm: str, end_mm: str):
    """
    지정된 기간의 월별 미세먼지 통계를 조회합니다.
    API에서 데이터를 우선 조회한 후, 누락된 월의 데이터가 CSV 파일에 있으면
    해당 데이터를 가져와 병합하여 반환합니다.
    """
    # 1. 조회해야 할 전체 월 목록(YYYYMM)을 생성
    expected_months = set()
    try:
        current_dt = datetime.strptime(begin_mm, "%Y%m")
        end_dt = datetime.strptime(end_mm, "%Y%m")
        while current_dt <= end_dt:
            expected_months.add(current_dt.strftime("%Y%m"))
            current_dt += relativedelta(months=1)
    except ValueError:
        print(f"오류: 날짜 형식 변환 실패 begin='{begin_mm}', end='{end_mm}'")
        return []

    # 2. API를 통해 데이터 조회
    api_data = _get_monthly_stats_from_api(station_name, begin_mm, end_mm)
    
    # 3. API에서 가져온 월 목록을 확인하여 누락된 월을 계산
    retrieved_months = {item['month'] for item in api_data}
    missing_months = expected_months - retrieved_months
    
    # 4. 누락된 월이 있다면 CSV에서 조회
    csv_data = []
    if missing_months:
        print(f"🔄 '{station_name}'의 누락된 {len(missing_months)}개월 데이터를 CSV에서 찾습니다: {sorted(list(missing_months))}")
        csv_data = _get_monthly_stats_from_csv(station_name, missing_months)
    
    # 5. API 데이터와 CSV 데이터를 병합하고 정렬하여 반환
    combined_data = api_data + csv_data
    combined_data.sort(key=lambda x: x.get('month', ''), reverse=True)
    return combined_data

def aggregate_annual_from_monthly(monthly_rows):
    bucket = defaultdict(lambda: {"pm10": [], "pm25": []})
    for row in monthly_rows:
        y = (row.get("month") or "")[:4]
        if y:
            if row.get("pm10_avg") is not None: bucket[y]["pm10"].append(row["pm10_avg"])
            if row.get("pm25_avg") is not None: bucket[y]["pm25"].append(row["pm25_avg"])
    return [{"year": y, "pm10_avg": _mean(v["pm10"]), "pm25_avg": _mean(v["pm25"])} for y, v in sorted(bucket.items())]

# ---------------------------
# Flask 라우트
# ---------------------------
@app.route("/", methods=["GET"])
def index():
    q = request.args.get("q", "")
    error = request.args.get("error", "")
    return render_template("index.html", q=q, error=error)

@app.route("/search", methods=["POST"])
def search():
    q = (request.form.get("q") or "").strip()
    three_years = request.form.get("three_years", "")
    params = {"q": q}
    if three_years: params["three_years"] = "1"
    if not q: return redirect(url_for("index", error="주소/장소명을 입력하세요."))
    return redirect(url_for("air_quality_view", **params))

@app.route("/api/combined-monthly-data")
def combined_monthly_data():
    try:
        raw_query = request.args.get("q")
        months_to_fetch = int(request.args.get("months", 12))
        if not raw_query: return jsonify({"error": "검색어가 필요합니다."}), 400
        q = preprocess_address(raw_query)
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
        resp = session.get(url, headers=headers, params={"query": q}, timeout=5)
        docs = resp.json().get("documents", [])
        if not docs: return jsonify({"error": "검색 결과가 없습니다."}), 404
        lat, lon = float(docs[0].get("y")), float(docs[0].get("x"))
        tmX, tmY = convert_to_tm(lat, lon)
        stations = get_nearby_stations_with_network(tmX, tmY, limit=1)
        if not stations: return jsonify({"error": "인근 측정소를 찾을 수 없습니다."}), 404
        station_name = stations[0]['stationName']

        today = datetime.today()
        end_mm = (today - relativedelta(months=1)).strftime("%Y%m")
        begin_mm = (today - relativedelta(months=months_to_fetch)).strftime("%Y%m")
        
        combined_data = get_monthly_stats(station_name, begin_mm, end_mm)
        return jsonify(combined_data)

    except Exception as e:
        print(f"/api/combined-monthly-data에서 오류 발생: {e}")
        return jsonify({"error": "서버 내부에서 데이터를 처리하는 중 오류가 발생했습니다."}), 500

@app.route("/air-quality", methods=["GET"])
def air_quality_view():
    raw_query = (request.args.get("q") or "").strip()
    if not raw_query: return redirect(url_for("index", error="주소/장소명을 입력하세요."))
    
    lat_lon_match = re.match(r'^\s*([0-9]+\.[0-9]+)\s*,\s*([0-9]+\.[0-9]+)\s*$', raw_query)
    
    try:
        if lat_lon_match:
            lat, lon = float(lat_lon_match.group(1)), float(lat_lon_match.group(2))
            search_type, place_name, display_address = "GPS 좌표", f"위도: {lat}, 경도: {lon}", ""
        else:
            q = preprocess_address(raw_query)
            search_type = "도로명 주소" if is_valid_road_address(q) else "장소명(키워드)"
            url = "https://dapi.kakao.com/v2/local/search/address.json" if is_valid_road_address(q) else "https://dapi.kakao.com/v2/local/search/keyword.json"
            headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
            resp = session.get(url, headers=headers, params={"query": q}, timeout=5)
            resp.raise_for_status()
            docs = resp.json().get("documents", [])
            if not docs: return render_template("index.html", q=raw_query, error=f"'{raw_query}' 검색 결과가 없습니다."), 404
            first = docs[0]
            lat, lon = float(first.get("y")), float(first.get("x"))
            place_name = first.get("place_name")
            display_address = first.get("road_address_name") or first.get("address_name", "-")

        tmX, tmY = convert_to_tm(lat, lon)
        stations = get_nearby_stations_with_network(tmX, tmY, limit=3)
        if not stations: return render_template("index.html", q=raw_query, error="가까운 측정소를 찾을 수 없습니다."), 404

        realtime_data, pm10_list, pm25_list = [], [], []
        for s in stations:
            try:
                rt = get_realtime_pm(s["stationName"]) or {}
                realtime_data.append({**s, **rt})
                if rt.get("pm10_ug_m3") is not None: pm10_list.append(rt["pm10_ug_m3"])
                if rt.get("pm2_5_ug_m3") is not None: pm25_list.append(rt["pm2_5_ug_m3"])
            except Exception:
                realtime_data.append({**s, "timestamp": "오류"})
        
        average_pm10 = _mean(pm10_list)
        average_pm25 = _mean(pm25_list)
        
        station_names = [s['stationName'] for s in stations]
        past_annual_data = None
        if historical_data is not None and not historical_data.empty and station_names:
            filtered_df = historical_data[historical_data['측정소명'].isin(station_names)]
            annual_df = filtered_df[filtered_df['월'] == 13].copy()
            annual_df.rename(columns={'년': '연도', 'PM10': 'annual_pm10_avg', 'PM2.5': 'annual_pm25_avg'}, inplace=True)
            past_annual_data = annual_df.sort_values(by=['측정소명', '연도']).to_dict('records')

        three_years_selected = 'three_years' in request.args
        today = datetime.today()
        end_mm_api = (today - relativedelta(months=1)).strftime("%Y%m")
        begin_mm = (today - relativedelta(months=35 if three_years_selected else 11)).strftime("%Y%m")
        month_range = {"begin": begin_mm, "end": end_mm_api}
        
        monthly_data, annual_data = [], []
        for s in stations:
            try:
                # get_monthly_stats가 이제 자동으로 누락된 데이터를 채워주므로 이 부분은 수정할 필요가 없습니다.
                mrows = get_monthly_stats(s["stationName"], begin_mm, end_mm_api)
                for row in mrows: row["network_type"] = s.get("network_type")
                monthly_data.extend(mrows)
                if three_years_selected and mrows:
                    ann = aggregate_annual_from_monthly(mrows)
                    for a in ann:
                        a.update({"stationName": s["stationName"], "network_type": s.get("network_type")})
                    annual_data.extend(ann)
            except Exception:
                monthly_data.append({"stationName": s["stationName"], "month": "데이터 조회 오류"})

        return render_template("result.html", raw_query=raw_query, search_type=search_type, place_name=place_name,
                               address=display_address, lat=lat, lon=lon, tmX=round(tmX, 3), tmY=round(tmY, 3),
                               realtime=realtime_data, average_pm10=average_pm10, average_pm25=average_pm25,
                               monthly=monthly_data, annual=annual_data or None, month_range=month_range,
                               three_years=three_years_selected, past_annual_data=past_annual_data)
    except Exception as e:
        return render_template("index.html", q=raw_query, error=f"오류가 발생했습니다: {e}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)