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
MAIN_DB_URL = os.getenv('MAIN_DB_URL')  # Mevcut odds veritabanı
ANALYSIS_DB_URL = os.getenv('DATABASE_URL')  # Analiz veritabanı

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

# Database engines
main_engine = create_engine(MAIN_DB_URL) if MAIN_DB_URL else None
analysis_engine = create_engine(ANALYSIS_DB_URL) if ANALYSIS_DB_URL else None


# ============================================
# Helper Functions
# ============================================
def update_progress(result_id: int, progress: int, message: str = None):
    """Progress callback - Redis'e yaz"""
    redis_client.hset(f"analysis:{result_id}", mapping={
        'progress': progress,
        'message': message or '',
        'updated_at': datetime.now().isoformat()
    })
    redis_client.expire(f"analysis:{result_id}", 3600)  # 1 saat TTL


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


# ============================================
# Main Analysis Task
# ============================================
@app.task(bind=True, max_retries=2)
def run_analysis(self, result_id: int, filter_data: dict):
    """
    Ana analiz görevi
    
    Args:
        result_id: users_analysis_results.id
        filter_data: {
            'date': '2026-01-15',
            'bookmaker_id': '1881',
            'filters': [...],
            'columns': [...]
        }
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
        bookmaker_id = filter_data['bookmaker_id']
        filters = filter_data.get('filters', [])
        columns = filter_data.get('columns', [])
        
        # Bugünkü maçları çek
        table_name = f"odds_{bookmaker_id}"  # örn: odds_1881
        
        query = f"""
            SELECT * FROM {table_name}
            WHERE match_date = :match_date
        """
        
        with main_engine.connect() as conn:
            df_today = pd.read_sql(text(query), conn, params={'match_date': analysis_date})
        
        total_matches = len(df_today)
        logger.info(f"Toplam maç: {total_matches}")
        
        if total_matches == 0:
            update_result_status(result_id, 'completed', {
                'rows': [],
                'summary': {'total_matches': 0}
            })
            update_progress(result_id, 100, 'Tamamlandı - Maç bulunamadı')
            return {'status': 'completed', 'matches': 0}
        
        # ===================
        # 2. Filtreleme (DB'de)
        # ===================
        update_progress(result_id, 20, 'Filtreler uygulanıyor...')
        
        # Filtreleri uygula (Pandas ile)
        for filter_item in filters:
            field = filter_item['field']
            operator = filter_item['operator']
            value = filter_item['value']
            
            if field not in df_today.columns:
                continue
                
            if operator == '=':
                df_today = df_today[df_today[field] == value]
            elif operator == '>':
                df_today = df_today[df_today[field] > float(value)]
            elif operator == '<':
                df_today = df_today[df_today[field] < float(value)]
            elif operator == 'between':
                min_val, max_val = value
                df_today = df_today[(df_today[field] >= float(min_val)) & (df_today[field] <= float(max_val))]
        
        filtered_count = len(df_today)
        logger.info(f"Filtreden geçen maç: {filtered_count}")
        
        # ===================
        # 3. Historical Matching
        # ===================
        update_progress(result_id, 30, f'Geçmiş veriler analiz ediliyor ({filtered_count} maç)...')
        
        results = []
        for idx, match in df_today.iterrows():
            progress = 30 + int((idx / max(filtered_count, 1)) * 60)
            update_progress(result_id, progress, f'{idx+1}/{filtered_count} maç işleniyor...')
            
            # Historical maçları bul (aynı oran aralığında)
            match_id = match.get('match_id') or match.get('id')
            home_odds = match.get('home_odds') or match.get('o_ft1')
            
            # Basitleştirilmiş historical query
            # TODO: Gerçek historical matching mantığı eklenecek
            history_data = []
            
            # Sonuç objesi oluştur
            result_row = {
                'rowId': int(match_id) if match_id else idx,
                'match_id': int(match_id) if match_id else None,
                'home': match.get('home', ''),
                'away': match.get('away', ''),
                'date': str(match.get('match_date', analysis_date)),
                'league': match.get('league', ''),
                'score': match.get('score', '-'),
                'ou25_open': float(match.get('ou25_open', 0)) if match.get('ou25_open') else None,
                'stats': {},
                'history': history_data,
                'match_count': len(history_data)
            }
            
            # Seçilen sütunları ekle
            for col in columns:
                if col in match.index:
                    val = match[col]
                    result_row['stats'][col] = {
                        'value': str(val) if pd.notna(val) else None,
                        'category': 'info'
                    }
            
            results.append(result_row)
        
        # ===================
        # 4. Özet İstatistikler
        # ===================
        update_progress(result_id, 95, 'Özet hesaplanıyor...')
        
        summary = {
            'total_matches': total_matches,
            'filtered_matches': filtered_count,
            'analyzed_matches': len(results),
            'analysis_date': analysis_date,
            'bookmaker_id': bookmaker_id
        }
        
        # ===================
        # 5. Sonucu Kaydet
        # ===================
        payload = {
            'rows': results,
            'summary': summary
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


# ============================================
# Score Update Task
# ============================================
@app.task(bind=True)
def update_scores(self, result_id: int):
    """Maç skorlarını güncelle"""
    try:
        logger.info(f"Skor güncellemesi: {result_id}")
        
        # TODO: Skor güncelleme mantığı
        # 1. Result'ı çek
        # 2. Her maç için güncel skoru al
        # 3. Payload'ı güncelle
        
        return {'status': 'completed'}
        
    except Exception as e:
        logger.error(f"Skor güncelleme hatası: {result_id} - {str(e)}")
        raise
