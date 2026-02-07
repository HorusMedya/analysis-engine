"""
Benim Analizlerim - Celery Tasks
Analysis Engine for OddsTips
"""

import os
import json
from datetime import datetime
from celery import Celery
from celery.utils.log import get_task_logger
import pandas as pd
from sqlalchemy import create_engine, text
import redis

# ============================================
# Configuration
# ============================================
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
ANALYSIS_DB_URL = os.getenv('DATABASE_URL')  # Analiz veritabanı

# Database host/port/user/password for dynamic connections
DB_HOST = os.getenv('DB_HOST', 'c4k8s8o4088kk0c4cwkkk0oc')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'analysis_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'nhDEAQwW9zLkMC8')

# Celery App
app = Celery('analysis')
app.config_from_object({
    'broker_url': os.getenv('CELERY_BROKER_URL', REDIS_URL),
    'result_backend': os.getenv('CELERY_RESULT_BACKEND', REDIS_URL),
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json'],
    'timezone': 'Europe/Istanbul',
    'task_time_limit': 300,  # 5 dakika max
    'task_soft_time_limit': 240,  # 4 dakika uyarı
})

logger = get_task_logger(__name__)

# Redis for progress updates
redis_client = redis.from_url(REDIS_URL)

# Analysis DB engine (for results)
# Connection pooling ile stale bağlantı sorunlarını önle
analysis_engine = create_engine(
    ANALYSIS_DB_URL,
    pool_pre_ping=True,      # Her sorgudan önce bağlantıyı test et
    pool_recycle=300,         # 5 dakikada bir bağlantıları yenile
    pool_timeout=30,          # Bağlantı bekleme timeout
    pool_size=5,              # Havuz boyutu
    max_overflow=10           # Maksimum ek bağlantı
) if ANALYSIS_DB_URL else None

# Database engines cache (for dynamic connections)
_db_engines = {}

def get_database_engine(database_name: str):
    """Dinamik veritabanı bağlantısı oluştur veya cache'den al"""
    if database_name not in _db_engines:
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{database_name}"
        _db_engines[database_name] = create_engine(
            db_url,
            pool_pre_ping=True,      # Her sorgudan önce bağlantıyı test et
            pool_recycle=300,         # 5 dakikada bir bağlantıları yenile
            pool_timeout=30,          # Bağlantı bekleme timeout
            pool_size=5,              # Havuz boyutu
            max_overflow=10           # Maksimum ek bağlantı
        )
        logger.info(f"Database engine created: {database_name}")
    return _db_engines[database_name]


# ============================================
# Odds Column Mapping - KAPANIŞ ORANLARI (c_ prefix)
# ============================================
def get_odds_columns(site_id):
    """Site ID'ye göre KAPANIŞ odds sütun isimlerini döndür"""
    return {
        # Maç Sonucu (1X2) - Kapanış
        'home_win': f'c_ft1_{site_id}',       # Ev sahibi kazanır
        'draw': f'c_ftx_{site_id}',            # Beraberlik
        'away_win': f'c_ft2_{site_id}',        # Deplasman kazanır
        
        # Over/Under 2.5 - Kapanış
        'over_25': f'c_fto25_{site_id}',       # 2.5 Üst
        'under_25': f'c_ftu25_{site_id}',      # 2.5 Alt
        
        # Over/Under 1.5 - Kapanış
        'over_15': f'c_fto15_{site_id}',       # 1.5 Üst
        'under_15': f'c_ftu15_{site_id}',      # 1.5 Alt
        
        # Over/Under 0.5 - Kapanış
        'over_05': f'c_fto05_{site_id}',       # 0.5 Üst
        'under_05': f'c_ftu05_{site_id}',      # 0.5 Alt
        
        # Karşılıklı Gol - Kapanış
        'btts_yes': f'c_bty_{site_id}',        # Karşılıklı Gol Var
        'btts_no': f'c_btn_{site_id}',         # Karşılıklı Gol Yok
        
        # Çifte Şans - Kapanış
        '1x': f'c_ftdc1x_{site_id}',           # 1 veya X
        '12': f'c_ftdc12_{site_id}',           # 1 veya 2
        'x2': f'c_ftdcx2_{site_id}',           # X veya 2
    }


# ============================================
# Helper Functions
# ============================================
def update_progress(result_id: int, progress: int, message: str = None):
    """Progress callback - Veritabanına yaz (Redis yerine)"""
    try:
        # Mesajı 250 karaktere truncate et (VARCHAR 255 sınırı için güvenli)
        safe_message = (message[:250] + '...') if message and len(message) > 250 else (message or '')
        logger.info(f"Progress update: {result_id} -> {progress}% ({safe_message})")
        with analysis_engine.connect() as conn:
            conn.execute(text("""
                UPDATE users_analysis_results 
                SET progress = :progress, 
                    progress_message = :message, 
                    updated_at = NOW()
                WHERE id = :id
            """), {'progress': progress, 'message': safe_message, 'id': result_id})
            conn.commit()
        logger.info(f"Progress update successful: {result_id} -> {progress}%")
    except Exception as e:
        logger.error(f"Progress update FAILED for {result_id}: {e}", exc_info=True)


def update_result_status(result_id: int, status: str, payload: dict = None):
    """Veritabanında sonuç durumunu güncelle"""
    with analysis_engine.connect() as conn:
        if payload:
            conn.execute(text("""
                UPDATE users_analysis_results 
                SET status = :status, payload = :payload, updated_at = NOW()
                WHERE id = :id
            """), {'status': status, 'payload': json.dumps(payload), 'id': result_id})
        else:
            conn.execute(text("""
                UPDATE users_analysis_results 
                SET status = :status, updated_at = NOW()
                WHERE id = :id
            """), {'status': status, 'id': result_id})
        conn.commit()


def safe_float(val):
    """Güvenli float dönüşümü"""
    if val is None or pd.isna(val):
        return None
    try:
        return round(float(val), 2)
    except:
        return None


def get_match_history(conn, table_name, home_team, away_team, match_date, limit=20):
    """
    Bir maç için geçmiş benzer maçları sorgula.
    H2H: Aynı iki takımın önceki maçları
    """
    try:
        query = text(f"""
            SELECT * FROM {table_name}
            WHERE score IS NOT NULL
            AND dateth < :match_date
            AND (
                (home = :home AND away = :away)
                OR (home = :away AND away = :home)
            )
            ORDER BY dateth DESC
            LIMIT :limit
        """)
        
        params = {
            'home': home_team,
            'away': away_team,
            'match_date': match_date,
            'limit': limit
        }
        
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        logger.warning(f"H2H sorgu hatası: {home_team} vs {away_team} - {e}")
        return pd.DataFrame()


def get_filtered_history(conn, table_name, match_row, filters, match_date, limit=100):
    """
    Bir maç için filtre kriterlerine göre geçmiş benzer maçları bul.
    
    Her maçın kendi değerlerini alır ve bu değerlere göre geçmişte benzer maçları arar.
    Örnek: A/MS/1=1.67, A/MS/X=3.70, A/MS/2=5.00 → bu oranlara sahip geçmiş maçları bul
    """
    if not filters:
        return pd.DataFrame(), []
    
    try:
        # Dinamik WHERE koşulları oluştur
        where_conditions = ["score IS NOT NULL", "dateth < :match_date"]
        params = {'match_date': match_date, 'limit': limit}
        
        applied_filters = []
        
        for i, filter_item in enumerate(filters):
            # field veya column alanını kontrol et (Flutter 'column' gönderiyor)
            field = filter_item.get('field') or filter_item.get('column')
            operator = filter_item.get('operator', '=')
            offset_value = filter_item.get('value')  # offset için (-, +, -+ operatörlerinde)
            
            if not field:
                continue
            
            # Maçın bu alan için değerini al
            if field not in match_row.index:
                logger.warning(f"Filtre alanı bulunamadı: {field}")
                continue
                
            match_value = match_row.get(field)
            if match_value is None or pd.isna(match_value):
                continue
            
            param_name = f"val_{i}"
            
            try:
                match_val_float = float(match_value)
            except (ValueError, TypeError):
                match_val_float = None
            
            if operator == '=' or operator == '' or operator == 'equals' or operator == 'odds_equal':
                # Eşit: tam değer eşleşmesi
                if match_val_float is not None:
                    where_conditions.append(f"{field} = :{param_name}")
                    params[param_name] = match_val_float
                else:
                    where_conditions.append(f"{field} = :{param_name}")
                    params[param_name] = match_value
                applied_filters.append(f"{field} = {match_value}")
                    
            elif (operator == '!=' or operator == 'not_equals') and match_val_float is not None:
                where_conditions.append(f"{field} != :{param_name}")
                params[param_name] = match_val_float
                applied_filters.append(f"{field} != {match_value}")
                
            elif operator == '>' and match_val_float is not None:
                where_conditions.append(f"{field} > :{param_name}")
                params[param_name] = match_val_float
                applied_filters.append(f"{field} > {match_value}")
                
            elif operator == '<' and match_val_float is not None:
                where_conditions.append(f"{field} < :{param_name}")
                params[param_name] = match_val_float
                applied_filters.append(f"{field} < {match_value}")
                
            elif operator == '>=' and match_val_float is not None:
                where_conditions.append(f"{field} >= :{param_name}")
                params[param_name] = match_val_float
                applied_filters.append(f"{field} >= {match_value}")
                
            elif operator == '<=' and match_val_float is not None:
                where_conditions.append(f"{field} <= :{param_name}")
                params[param_name] = match_val_float
                applied_filters.append(f"{field} <= {match_value}")
                
            elif match_val_float is not None and operator.startswith('-') and not operator.startswith('-+'):
                # Negatif aralık: -0.05, -0.10, -0.20, -0.50 veya genel '-'
                try:
                    offset = abs(float(operator))
                except (ValueError, TypeError):
                    offset = float(offset_value) if offset_value else 0.05
                min_val = match_val_float - offset
                max_val = match_val_float
                where_conditions.append(f"{field} BETWEEN :{param_name}_min AND :{param_name}_max")
                params[f"{param_name}_min"] = min_val
                params[f"{param_name}_max"] = max_val
                applied_filters.append(f"{field} [{min_val:.2f}, {max_val:.2f}]")
                
            elif match_val_float is not None and operator.startswith('+'):
                # Pozitif aralık: +0.05, +0.10, +0.20, +0.50 veya genel '+'
                try:
                    offset = abs(float(operator))
                except (ValueError, TypeError):
                    offset = float(offset_value) if offset_value else 0.05
                min_val = match_val_float
                max_val = match_val_float + offset
                where_conditions.append(f"{field} BETWEEN :{param_name}_min AND :{param_name}_max")
                params[f"{param_name}_min"] = min_val
                params[f"{param_name}_max"] = max_val
                applied_filters.append(f"{field} [{min_val:.2f}, {max_val:.2f}]")
                
            elif match_val_float is not None and (operator == '-+' or operator.startswith('±')):
                # Tolerans: ±0.05, ±0.10, ±0.20, ±0.50 veya genel '-+' / '±'
                try:
                    offset = abs(float(operator.replace('±', '')))
                except (ValueError, TypeError):
                    offset = float(offset_value) if offset_value else 0.05
                min_val = match_val_float - offset
                max_val = match_val_float + offset
                where_conditions.append(f"{field} BETWEEN :{param_name}_min AND :{param_name}_max")
                params[f"{param_name}_min"] = min_val
                params[f"{param_name}_max"] = max_val
                applied_filters.append(f"{field} ±{offset} [{min_val:.2f}, {max_val:.2f}]")
            
            else:
                # Tanınmayan operatör - loglayıp atla
                logger.warning(f"Tanınmayan operatör: '{operator}' (field: {field})")
        
        if len(where_conditions) <= 2:
            # Hiç filtre uygulanamadı
            return pd.DataFrame(), []
        
        where_clause = " AND ".join(where_conditions)
        query = text(f"""
            SELECT * FROM {table_name}
            WHERE {where_clause}
            ORDER BY dateth DESC
            LIMIT :limit
        """)
        
        df = pd.read_sql(query, conn, params=params)
        logger.info(f"Filtre geçmişi: {len(df)} maç bulundu ({', '.join(applied_filters[:3])}...)")
        
        return df, applied_filters
        
    except Exception as e:
        logger.warning(f"Filtreli geçmiş sorgu hatası: {e}")
        return pd.DataFrame(), []


def calculate_match_stats(df_history, stat_columns):
    """
    Geçmiş maçların istatistiklerini hesapla.
    Her sütun için pozitif/negatif/yüzde döndür.
    """
    if df_history.empty:
        return {}
    
    match_stats = {}
    
    for col in stat_columns:
        if col in df_history.columns:
            try:
                col_data = df_history[col].dropna()
                total = len(col_data)
                
                if total > 0:
                    # Boolean sütunlar için == 1 karşılaştırması yap
                    # Bu, 1'den büyük değerlerin yanlış sayılmasını önler
                    positive = int((col_data == 1).sum())
                    
                    # Güvenlik: positive total'i aşamaz
                    if positive > total:
                        positive = total
                    
                    negative = total - positive
                    percentage = round(positive / total * 100, 1)
                    
                    match_stats[col] = {
                        'positive': positive,
                        'negative': negative,
                        'total': total,
                        'percentage': percentage,
                        'label': col.upper().replace('_', ' ')
                    }
            except:
                continue
    
    return match_stats


def calculate_market_predictions(df_history):
    """
    Her market için ayrı güven hesaplaması yapar.
    Esnek eşik değerleri ile tahmin döndürür.
    """
    if df_history.empty or len(df_history) < 1:
        return {}
    
    predictions = {}
    total = len(df_history)
    
    def get_signal(confidence):
        """Güven seviyesine göre sinyal döndür"""
        if confidence >= 70:
            return 'strong'
        elif confidence >= 60:
            return 'medium'
        elif confidence >= 50:
            return 'weak'
        elif confidence >= 30:
            return 'risky'
        return None
    
    def calc_stat(col_name, threshold=50):
        """Tek sütun için istatistik hesapla"""
        if col_name not in df_history.columns:
            return None
        col_data = df_history[col_name].dropna()
        if len(col_data) == 0:
            return None
        positive = int((col_data == 1).sum())
        pct = round(positive / len(col_data) * 100, 1)
        return pct
    
    # ========================================
    # MAÇSONU (1X2)
    # ========================================
    if 'haft' in df_history.columns:
        # haft: 1=Ev, 0=Beraberlik, -1=Deplasman (veya benzeri)
        haft_data = df_history['haft'].dropna()
        if len(haft_data) > 0:
            home_wins = int((haft_data == 1).sum())
            draws = int((haft_data == 0).sum())
            away_wins = int((haft_data == -1).sum()) if -1 in haft_data.values else (len(haft_data) - home_wins - draws)
            
            home_pct = round(home_wins / len(haft_data) * 100, 1)
            draw_pct = round(draws / len(haft_data) * 100, 1)
            away_pct = round(away_wins / len(haft_data) * 100, 1)
            
            max_pct = max(home_pct, draw_pct, away_pct)
            if max_pct >= 50:
                if home_pct == max_pct:
                    predictions['match_result'] = {'value': '1', 'confidence': home_pct, 'signal': get_signal(home_pct)}
                elif draw_pct == max_pct:
                    predictions['match_result'] = {'value': 'X', 'confidence': draw_pct, 'signal': get_signal(draw_pct)}
                else:
                    predictions['match_result'] = {'value': '2', 'confidence': away_pct, 'signal': get_signal(away_pct)}
    
    # ========================================
    # ÜST/ALT GOLLER
    # ========================================
    goal_markets = [
        ('ouft15', 'over_15'),
        ('ouft25', 'over_25'),
        ('ouft35', 'over_35'),
    ]
    
    for db_col, market_key in goal_markets:
        pct = calc_stat(db_col)
        if pct is not None:
            if pct >= 50:
                predictions[market_key] = {'value': 'Over', 'confidence': pct, 'signal': get_signal(pct)}
            elif (100 - pct) >= 50:
                predictions[market_key] = {'value': 'Under', 'confidence': round(100 - pct, 1), 'signal': get_signal(100 - pct)}
    
    # ========================================
    # KARŞILIKLI GOL (BTTS)
    # ========================================
    btts_pct = calc_stat('btsyn')
    if btts_pct is not None:
        if btts_pct >= 50:
            predictions['btts'] = {'value': 'Yes', 'confidence': btts_pct, 'signal': get_signal(btts_pct)}
        elif (100 - btts_pct) >= 50:
            predictions['btts'] = {'value': 'No', 'confidence': round(100 - btts_pct, 1), 'signal': get_signal(100 - btts_pct)}
    
    # ========================================
    # EV SAHİBİ GOL
    # ========================================
    home_goal_markets = [
        ('ft_home_over_05', 'home_over_05'),
        ('ft_home_over_15', 'home_over_15'),
    ]
    
    for db_col, market_key in home_goal_markets:
        pct = calc_stat(db_col)
        if pct is not None:
            if pct >= 50:
                predictions[market_key] = {'value': 'Over', 'confidence': pct, 'signal': get_signal(pct)}
            elif (100 - pct) >= 50:
                predictions[market_key] = {'value': 'Under', 'confidence': round(100 - pct, 1), 'signal': get_signal(100 - pct)}
    
    # ========================================
    # DEPLASMAN GOL
    # ========================================
    away_goal_markets = [
        ('ft_away_over_05', 'away_over_05'),
        ('ft_away_over_15', 'away_over_15'),
    ]
    
    for db_col, market_key in away_goal_markets:
        pct = calc_stat(db_col)
        if pct is not None:
            if pct >= 50:
                predictions[market_key] = {'value': 'Over', 'confidence': pct, 'signal': get_signal(pct)}
            elif (100 - pct) >= 50:
                predictions[market_key] = {'value': 'Under', 'confidence': round(100 - pct, 1), 'signal': get_signal(100 - pct)}
    
    # ========================================
    # SKOR TAHMİNİ (%30+ eşik)
    # ========================================
    if 'ftscore' in df_history.columns:
        score_data = df_history['ftscore'].dropna()
        if len(score_data) > 0:
            score_counts = score_data.value_counts()
            if len(score_counts) > 0:
                top_score = score_counts.index[0]
                top_pct = round(score_counts.iloc[0] / len(score_data) * 100, 1)
                if top_pct >= 30:
                    predictions['score'] = {'value': str(top_score), 'confidence': top_pct, 'signal': get_signal(top_pct)}
    
    return predictions


# ============================================
# Main Analysis Task
# ============================================
@app.task(bind=True, max_retries=2)
def run_analysis(self, result_id: int, filter_data: dict):
    """
    Ana analiz görevi
    """
    try:
        logger.info(f"Analiz başladı: {result_id}")
        update_result_status(result_id, 'running')
        update_progress(result_id, 0, 'Analiz başlatılıyor...')
        
        # ===================
        # 1. Veri Çekme
        # ===================
        update_progress(result_id, 10, 'Maç verileri çekiliyor...')
        
        analysis_date = filter_data['date']
        site_id = filter_data['bookmaker_id']
        database_name = filter_data.get('database_name', 'soccerdb1')  # Varsayılan soccerdb1
        filters = filter_data.get('filters', [])
        min_matches = filter_data.get('min_matches', 10)  # Minimum benzer maç sayısı
        max_matches = filter_data.get('max_matches', 100)  # Maksimum analiz maç sayısı
        
        # DEBUG: Filtre içeriğini logla
        logger.info(f"Filtreler: {len(filters)} adet - {filters}")
        logger.info(f"MIN/MAX: min_matches={min_matches}, max_matches={max_matches}")
        
        # Dinamik veritabanı bağlantısı
        db_engine = get_database_engine(database_name)
        logger.info(f"Database: {database_name}, Site ID: {site_id}")
        
        # Odds sütunlarını al
        odds_cols = get_odds_columns(site_id)
        
        # Bugünkü maçları çek
        table_name = f"odds_{site_id}"
        
        query = f"""
            SELECT * FROM {table_name}
            WHERE DATE(dateth) = :date_param
        """
        
        with db_engine.connect() as conn:
            df_today = pd.read_sql(text(query), conn, params={'date_param': analysis_date})
        
        total_matches = len(df_today)
        logger.info(f"Toplam maç: {total_matches} ({database_name}.{table_name})")

        
        if total_matches == 0:
            update_result_status(result_id, 'completed', {
                'rows': [],
                'summary': {'total_matches': 0, 'filtered_matches': 0, 'analyzed_matches': 0}
            })
            update_progress(result_id, 100, 'Tamamlandı - Maç bulunamadı')
            return {'status': 'completed', 'matches': 0}
        
        # ===================
        # 2. Her Maç İçin Filtre-Bazlı Geçmiş Analizi
        # ===================
        # NOT: Filtreler maçları ELEMİYOR!
        # Her maç için, o maçın değerlerine göre geçmişte benzer maçlar aranıyor.
        # Örnek: Newcastle vs Leeds için A/MS oranları 1.67/3.70/5.00 ise,
        # geçmişte bu oranlara sahip tüm maçlar bulunuyor.
        
        update_progress(result_id, 20, f'Her maç için geçmiş analizi yapılıyor ({total_matches} maç)...')
        
        # İstatistik sütunları (41 adet)
        stat_columns = [
            # Maç Sonu Üst/Alt (5)
            'ouft05', 'ouft15', 'ouft25', 'ouft35', 'ouft45',
            # İlk Yarı Üst/Alt (3)
            'oufh05', 'oufh15', 'oufh25',
            # 2. Yarı Üst/Alt (3)
            'sh_over_under_05', 'sh_over_under_15', 'sh_over_under_25',
            # Kazanan (2)
            'haft', 'hafh',
            # 2. Yarı Kazanan (1)
            'sh_winner',
            # Karşılıklı Gol (3)
            'btsyn', 'btfh', 'sh_btts',
            # Maç Sonu Skor Bilgileri (2)
            'ft_score_diff', 'ft_total_goals',
            # Ev Sahibi Gol - Maç Sonu (5)
            'ft_home_over_05', 'ft_home_over_15', 'ft_home_over_25', 'ft_home_over_35', 'ft_home_over_45',
            # Deplasman Gol - Maç Sonu (5)
            'ft_away_over_05', 'ft_away_over_15', 'ft_away_over_25', 'ft_away_over_35', 'ft_away_over_45',
            # Ev Sahibi Gol - İlk Yarı (3)
            'fh_home_over_05', 'fh_home_over_15', 'fh_home_over_25',
            # Deplasman Gol - İlk Yarı (3)
            'fh_away_over_05', 'fh_away_over_15', 'fh_away_over_25',
            # Ev Sahibi Gol - 2. Yarı (3)
            'sh_home_over_05', 'sh_home_over_15', 'sh_home_over_25',
            # Deplasman Gol - 2. Yarı (3)
            'sh_away_over_05', 'sh_away_over_15', 'sh_away_over_25',
        ]
        
        results = []
        analyzed_count = 0
        
        with db_engine.connect() as conn:
            for idx, match in df_today.iterrows():
                if idx % 5 == 0:
                    progress = 20 + int((idx / total_matches) * 60)
                    update_progress(result_id, progress, f'{idx+1}/{total_matches} maç analiz ediliyor...')
                
                match_id = match.get('id') or idx
                home_team = str(match.get('home', ''))
                away_team = str(match.get('away', ''))
                match_date = str(match.get('dateth', analysis_date))
                
                # Odds verilerini çek
                odds_data = {}
                for key, col_name in odds_cols.items():
                    if col_name in match.index:
                        odds_data[key] = safe_float(match[col_name])
                
                # Stats objesi (oranlar)
                stats = {
                    '1X2': {
                        'home': odds_data.get('home_win'),
                        'draw': odds_data.get('draw'),
                        'away': odds_data.get('away_win'),
                    },
                    'OU25': {
                        'over': odds_data.get('over_25'),
                        'under': odds_data.get('under_25'),
                    },
                    'OU15': {
                        'over': odds_data.get('over_15'),
                        'under': odds_data.get('under_15'),
                    },
                    'BTTS': {
                        'yes': odds_data.get('btts_yes'),
                        'no': odds_data.get('btts_no'),
                    },
                }
                
                # Boş objeleri temizle
                stats = {k: v for k, v in stats.items() if any(val is not None for val in v.values())}
                
                # ===== YENİ MANTIK: Filtre-Bazlı Geçmiş Sorgusu =====
                # Eğer filtre varsa → filtre değerlerine göre geçmiş maçları bul
                # Eğer filtre yoksa → H2H (aynı takımlar) kullan
                
                if filters:
                    # Filtre-bazlı geçmiş sorgusu
                    df_history, applied_filters = get_filtered_history(
                        conn, table_name, match, filters, match_date, limit=max_matches
                    )
                    match_count = len(df_history)
                    logger.info(f"Maç {idx+1}: {home_team} vs {away_team} → {match_count} benzer maç (filtre)")
                else:
                    # H2H sorgusu (filtre yoksa)
                    df_history = get_match_history(conn, table_name, home_team, away_team, match_date, limit=min(max_matches, 20))
                    match_count = len(df_history)
                    applied_filters = []
                    if match_count > 0:
                        logger.info(f"Maç {idx+1}: {home_team} vs {away_team} → {match_count} H2H maç")
                
                # Geçmiş maçların istatistiklerini hesapla
                match_stats = calculate_match_stats(df_history, stat_columns)
                
                # Market bazlı tahminleri hesapla (YENİ ESNEK SİSTEM)
                predictions = calculate_market_predictions(df_history)
                
                # Geçmiş maçların listesi (tüm maçlar)
                history_list = []
                for _, hist_match in df_history.iterrows():
                    history_list.append({
                        'home': str(hist_match.get('home', '')),
                        'away': str(hist_match.get('away', '')),
                        'date': str(hist_match.get('dateth', '')),
                        'score': str(hist_match.get('score', '')) if pd.notna(hist_match.get('score')) else None,
                    })
                
                result_row = {
                    'rowId': int(match_id),
                    'match_id': int(match_id),
                    'home': home_team,
                    'away': away_team,
                    'date': match_date,
                    'country': str(match.get('country', '')),
                    'league': str(match.get('league', '')),
                    'score': str(match.get('score')) if pd.notna(match.get('score')) else None,
                    'odds': odds_data,
                    'stats': stats,
                    'match_stats': match_stats,  # Geçmiş maçların istatistikleri
                    'match_count': match_count,   # Benzer geçmiş maç sayısı
                    'history': history_list,      # Geçmiş maçların listesi
                    'applied_filters': applied_filters[:5] if filters else [],  # Uygulanan filtreler
                    'predictions': predictions,   # YENİ: Market bazlı tahminler
                }
                
                # Sadece geçmiş verisi olan maçları ekle
                if match_count > 0:
                    results.append(result_row)
                    analyzed_count += 1
        
        logger.info(f"Analiz tamamlandı: {analyzed_count}/{total_matches} maç için geçmiş bulundu")

        
        # ===================
        # 4. Toplam İstatistikler (Aggregate Stats)
        # ===================
        update_progress(result_id, 90, 'Toplam istatistikler hesaplanıyor...')
        
        # İstatistik sütunları - boolean sonuçlar
        stat_columns = [
            # Maç Sonu Alt/Üst
            'ouft05', 'ouft15', 'ouft25', 'ouft35',
            # Ev Sahibi Gol
            'ft_home_over_05', 'ft_home_over_15', 'ft_home_over_25',
            # Deplasman Gol  
            'ft_away_over_05', 'ft_away_over_15', 'ft_away_over_25',
            # Karşılıklı Gol
            'btfh', 'btsyn',
            # İlk Yarı
            'fh_home_over_05', 'fh_away_over_05', 'hafh',
            # İkinci Yarı
            'sh_over_under_05', 'sh_over_under_15', 'sh_over_under_25',
            'sh_home_over_05', 'sh_away_over_05', 'sh_btts',
        ]
        
        aggregate_stats = {}
        
        for col in stat_columns:
            if col in df_today.columns:
                try:
                    # Boolean sütun - True sayısını al
                    col_data = df_today[col].dropna()
                    total = len(col_data)
                    
                    if total > 0:
                        # Boolean veya 1/0 değerler için
                        positive = int(col_data.sum())
                        negative = total - positive
                        percentage = round(positive / total * 100, 1)
                        
                        aggregate_stats[col] = {
                            'positive': positive,
                            'negative': negative,
                            'total': total,
                            'percentage': percentage,
                            'label': col.upper().replace('_', ' ')
                        }
                except Exception as e:
                    logger.warning(f"İstatistik hesaplama hatası {col}: {e}")
                    continue
        
        update_progress(result_id, 95, 'Özet hesaplanıyor...')
        
        summary = {
            'total_matches': total_matches,
            'filtered_matches': analyzed_count,  # Geçmiş bulunan maç sayısı
            'analyzed_matches': len(results),
            'analysis_date': analysis_date,
            'bookmaker_id': site_id,
            'database_name': database_name,  # Skor güncellemesi için gerekli
            'scored_matches': len([r for r in results if r.get('score') and r.get('score') != '-'])
        }
        
        payload = {
            'rows': results,
            'summary': summary,
            'aggregate_stats': aggregate_stats  # YENİ: Toplam istatistikler
        }
        
        update_result_status(result_id, 'completed', payload)
        update_progress(result_id, 100, 'Tamamlandı!')
        
        logger.info(f"Analiz tamamlandı: {result_id}, {len(results)} sonuç")
        return {'status': 'completed', 'matches': len(results)}
        
    except Exception as e:
        logger.error(f"Analiz hatası: {result_id} - {str(e)}")
        update_result_status(result_id, 'failed')
        update_progress(result_id, -1, f'Hata: {str(e)}')
        raise


@app.task(bind=True)
def update_scores(self, result_id: int):
    """Maç skorlarını güncelle"""
    try:
        logger.info(f"Skor güncellemesi: {result_id}")
        return {'status': 'completed'}
    except Exception as e:
        logger.error(f"Skor güncelleme hatası: {result_id} - {str(e)}")
        raise
