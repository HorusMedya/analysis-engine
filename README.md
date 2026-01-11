# Benim Analizlerim - Docker Altyapısı

## Hızlı Başlangıç

### 1. Environment Ayarları
```bash
cp .env.example .env
# .env dosyasını düzenleyin
```

### 2. Coolify ile Deploy

**Seçenek A: Docker Compose Import**
- Coolify'da "New Project" → "Docker Compose"
- `docker-compose.yml` içeriğini yapıştırın

**Seçenek B: Ayrı Servisler**
1. PostgreSQL ekle (port 5433)
2. Redis ekle (port 6380)
3. Python Worker ekle (30 replica)
4. Flower ekle (opsiyonel, monitoring)

### 3. Veritabanı Bağlantısı
Laravel `.env` dosyasına ekleyin:
```
ANALYSIS_REDIS_URL=redis://your-server:6380
ANALYSIS_DB_URL=postgresql://analysis_user:pass@your-server:5433/analysis_db
```

## Servisler

| Servis | Port | Açıklama |
|--------|------|----------|
| PostgreSQL | 5433 | Analiz sonuçları DB |
| Redis | 6380 | Queue & Cache |
| Celery Workers | - | 30 worker, her biri 4GB RAM |
| Flower | 5555 | Celery monitoring UI |

## Monitoring

Flower UI: `http://your-server:5555`
- Kullanıcı: `admin`
- Şifre: `.env` dosyasından

## Kaynak Kullanımı

| Servis | RAM | CPU |
|--------|-----|-----|
| PostgreSQL | 32 GB | 16 core |
| Redis | 4 GB | 2 core |
| Workers (30x) | 120 GB | 60 core |
| **Toplam** | **156 GB** | **78 core** |
