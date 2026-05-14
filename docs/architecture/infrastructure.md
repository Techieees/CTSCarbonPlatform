# Infrastructure Architecture

Bu dosya repoda görünen runtime ve deployment düzenini anlatır. Kod tabanında container veya tam deployment manifesti yok; Nginx ve Gunicorn ayarları ayrı dosyalarla tutuluyor.

## Runtime Stack

Uygulama Flask + Gunicorn + Nginx düzeni için hazırlanmış.

- Flask app: `frontend/app.py`
- Gunicorn config: `gunicorn.conf.py`
- Nginx include dosyaları: `deploy/`
- Veritabanı: SQLite, `frontend/instance/ghg_data.db`
- Upload ve local storage: `frontend/instance/uploads/`
- Pipeline ve batch storage: `data/` ve `storage/`

Gunicorn `127.0.0.1:8000` üzerinde dinler. Nginx'in public HTTP(S) trafiğini Gunicorn'a proxy etmesi beklenir.

## Gunicorn

`gunicorn.conf.py` şu an tek worker ile ayarlı:

- `workers = 1`
- `threads = 4`
- `timeout = 300`
- `bind = "127.0.0.1:8000"`

Dosyadaki notlar SQLite için worker artırmanın doğru olmadığını açıkça söylüyor. Bunun sebebi iki şey:

- SQLite file lock davranışı.
- In-memory background job state.

Worker sayısı artırılırsa her process kendi `jobs` sözlüğünü taşır. Bir processin başlattığı jobı diğer process göremez.

## Nginx

`deploy/nginx-carbon-platform.conf` sadece upload body limitini artırır:

- `client_max_body_size 50M`

Bu Evidence ve multipart uploadlar için gerekli. Flask tarafında evidence için uygulama limiti `25 MB`; Nginx limiti daha yüksek tutulmuş.

`deploy/nginx-profile-photos-block.conf` legacy profile photo static pathini kapatır:

- `/static/profile_photos/` doğrudan erişime kapalıdır.
- Profile photo sadece Flask route ile servis edilir: `/api/profile-photo/<user_id>`.

Bu yaklaşım login_required kontrolünü Flask tarafında tutar.

## Flask App Structure

Flask app import edildiğinde:

1. Config pathleri çözülür.
2. Instance ve upload klasörleri oluşturulur.
3. SQLAlchemy SQLite URI ile başlar.
4. LoginManager kurulur.
5. Modeller register edilir.
6. Request hookları, error handlerlar ve routes yüklenir.

`_ensure_db_tables()` gibi yardımcılar bazı route'larda tablo varlığını garanti etmek için çağrılır. SQLite migrationları yer yer uygulama kodu içinde yapılır.

## Static Asset Flow

Static dosyalar `frontend/static/` altındadır. Flask `url_for('static', filename=...)` ile servis eder.

Kullanılan asset tipleri:

- CSS: `frontend/static/css/`
- JS: `frontend/static/js/`
- Lottie JSON: `frontend/static/lottie/`
- Images: `frontend/static/images/`
- Static data JSON: `frontend/static/data/`

Nginx tarafında static için özel tam config repoda görünmüyor. Mevcut include yalnızca profile photo legacy pathini kapatıyor.

`base.html` CDN bağımlılıkları da kullanır:

- Bootstrap CSS/JS.
- Lottie web.
- ECharts.
- Mapbox GL JS ve geocoder, sadece locations sayfasında.

## Upload Storage

Upload pathleri `config` içinden gelir:

- `FRONTEND_INSTANCE_DIR = frontend/instance`
- `FRONTEND_UPLOAD_DIR = frontend/instance/uploads`
- `STORAGE_ROOT = storage`
- `PROFILE_PHOTOS_STORAGE_DIR`, config package içinde tanımlı.

Evidence storage abstraction var:

- `frontend/storage/providers/base.py`
- `frontend/storage/providers/local.py`
- `frontend/storage/__init__.py`

Bugünkü provider local disk. `LocalStorageProvider` path traversal'a karşı relative POSIX path kontrolü yapar.

Evidence dosyaları mantıksal olarak şöyle yerleşir:

- staging: `evidence/_staging/...`
- final: `evidence/YYYY/MM/<sha>.<ext>`
- thumbnails: `evidence/_thumbs/YYYY/MM/<sha>_thumb.webp`

Profile photo tarafında legacy static klasörden storage pathine tek seferlik migration helperı var.

## Pipeline Storage

Veri pipeline klasörleri `data/` ve `storage/` altında:

- `data/stage1_preprocess/input`
- `data/stage1_preprocess/output`
- `data/stage1_preprocess/klarakarbon`
- `data/stage2_mapping/input`
- `data/stage2_mapping/output`
- `data/stage2_mapping/manual_mappings`
- `data/stage2_mapping/travel`
- `data/stage2_mapping/klarakarbon`
- `storage/pipeline_runs`
- `storage/pipeline_runs_web`
- `storage/engage_waste`

Stage2 scriptleri ayrıca `engine/stage2_mapping/` altındaki bazı workbook ve cache dosyalarını okur.

## Background Job Execution

Joblar ayrı bir worker sistemiyle değil, Flask process içinde daemon thread olarak çalışır.

Job state:

- Runtime state: `jobs` dict.
- Lock: `_JOBS_LOCK`.
- Cleanup: tamamlanmış joblar belirli süre sonra memory'den silinir.
- Status endpointleri job dictinden okur.

Kalıcı job audit tabloları bazı akışlarda ayrıca vardır:

- `MappingRun`
- `EmployeeCommutingGeneratedRun`
- `SupplierSyncRun`

Bu tablolar job progress state'in tamamını değil, iş sonucunu ve audit bilgisini taşır.

## External Integrations

### CCC API

Config:

- `CCC_API_BASE_URL`
- `CCC_USERNAME`
- `CCC_PASSWORD`
- `CCC_API_PAGE_SIZE`
- `config/ccc_get_endpoints.json`
- `config/ccc_sheet_mapping.json`

Kod:

- `engine/stage1_preprocess/api_sources/ccc_client.py`
- `engine/stage1_preprocess/api_sources/ccc_purchase_orders.py`
- `engine/stage1_preprocess/api_sources/ccc_generic_ingest.py`
- `frontend/services/ccc_data_entry_import_service.py`

### Engage Waste API

Config:

- `ENGAGE_WASTE_BASE_URL`
- `ENGAGE_WASTE_SUBSCRIPTION_KEY`

Kod:

- `frontend/services/engage_waste_service.py`

Raw responses `storage/engage_waste/raw/` altında arşivlenir. Preview bundle'lar `storage/engage_waste/previews/` altında tutulur.

### OpenWeather ve Mapbox

`OPENWEATHER_API_KEY` public locations contextinde kullanılır. Mapbox token template'e `window.MAPBOX_TOKEN` olarak verilir. Mapbox runtime browser tarafında çalışır.

### Email

Password reset email için `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD`, `MAIL_DEFAULT_SENDER` ayarları kullanılır.

### Power BI

Kodda Power BI için doğrudan API client görünmüyor. Stage2 forecasting ve totals scriptlerinde "Power BI view" ile uyumlu workbook/sheet üretimi yorumlarda geçiyor. Bu nedenle mevcut entegrasyon dosya/workbook uyumluluğu düzeyinde okunmalı.

## Request Lifecycle

Tipik istek akışı:

1. Browser Nginx'e istek gönderir.
2. Nginx Gunicorn'a proxy eder.
3. Gunicorn tek worker ve dört thread ile Flask'a iletir.
4. Flask before_request hookları çalışır:
   - Performance timer.
   - Profile completion guard.
   - Activity session.
   - Readonly auditor guard.
5. Route handler çalışır.
6. SQLAlchemy SQLite DB'ye erişir.
7. Response after_request hooklarından geçer:
   - Slow route/SQL timing.
   - Static cache header.
   - Activity log.
8. Response browser'a döner.

## Operasyonel Riskler

- Tek worker, in-memory job state için bilinçli; ancak uzun CPU/pandas işleri aynı process içinde çalışır.
- SQLite write contention joblar çoğaldıkça görünür olur.
- Stage2 output directory shared olduğu için lock kaldırılırsa runlar birbirinin çıktısını okuyabilir.
- Background joblar daemon thread olduğu için process kapanırsa yarım kalabilir.
- Evidence optimization Ghostscript veya PyMuPDF varlığına göre farklı davranır.
- External API credentials runtime env veya `config/api_credentials.env` üzerinden gelir; bu dosya güvenli yönetilmeli.
- Büyük Excel previewleri request süresini artırabilir.

## Runtime Diyagramı

Request lifecycle ve background job diyagramları `docs/architecture/diagrams/` altında bulunur.
