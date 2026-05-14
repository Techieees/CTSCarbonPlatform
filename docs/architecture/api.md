# API Architecture

Bu dosya Flask rotalarını davranış gruplarına ayırır. Kodda REST tasarımı tek tip değil; bazı endpointler JSON API gibi, bazıları form POST ve redirect ile çalışır. Bu mevcut yapıyı yansıtır.

## Authentication ve Session

Ana rotalar:

- `GET/POST /login`
- `GET /logout`
- `GET/POST /register`
- `GET/POST /request-access`
- `GET/POST /forgot-password`
- `GET/POST /reset-password/<token>`

Session Flask-Login ile tutulur. `@login_required` çoğu app route'u korur. Rol kontrolleri route içinde yapılır:

- `current_user.is_admin`
- `normalize_user_role(current_user.role)`
- owner/super_admin/admin özel kontrolleri
- readonly auditor guard

Profile completion guard, profile setup tamamlanmadan app endpointlerine erişimi engeller.

## Data Entry API

Data Entry endpointleri template-driven çalışır. API önce şirket ve sheet görünürlüğünü kontrol eder, sonra template registry üzerinden header ve rule döndürür.

Endpointler:

- `GET /api/excel_schema/companies`
- `GET /api/excel_schema/sheets`
- `GET /api/excel_schema/headers`
- `GET /api/data_entry/rows`
- `GET /api/data-entry/site-tags`
- `GET /api/data-entry/reporting-periods`
- `POST /api/products-input/save`
- `GET /api/products-input/export`

Ortak kontroller:

- Şirket erişimi `_user_can_access_company()`.
- Sheet varlığı `_resolve_template_sheet_name()`.
- Hidden sheet kontrolü.
- Header listesi `_get_data_entry_template_schema()`.

Data Entry save davranışı hücreleri `DataEntry` tablosuna yazar. Bazı kaynaklar Data Entry API'sini doğrudan kullanmaz; backend helperları aynı upsert mantığını çağırır.

## Mapping API

Endpointler:

- `POST /run-mapping`
- `POST /api/mapping/run`
- `GET /api/mapping/download/<run_id>`
- `GET /api/mapping/download_merged`
- mapping preview detail endpointleri
- mapping review endpointleri
- admin unmapped row endpointleri

`POST /api/mapping/run` admin gerektirir. Request body:

- `company`
- `sheet`
- opsiyonel `rows`
- opsiyonel `entry_group`

Endpoint önce satırları kaydedebilir, sonra background mapping job başlatır. Response job id döndürür. Mapping sonucu job status API'sinden ve mapping preview/download endpointlerinden izlenir.

## Job API

Job state in-memory `jobs` dictindedir.

Önemli endpointler:

- `GET /job-status/<job_id>`
- `GET /jobs`
- `POST /cancel-job/<job_id>`

Job response genelde şunları içerir:

- `job_id`
- `type`
- `company`
- `status`
- `progress`
- `message`
- `error`
- timestamps
- opsiyonel `rows`
- opsiyonel `result`

Job iptali sadece flag yazar. İlgili job fonksiyonu `_raise_if_job_cancelled()` çağırdığında durur.

## Evidence API

Endpointler:

- `GET /api/evidence/row-summary`
- `GET /api/evidence/for-row`
- `GET /api/evidence/search`
- `POST /api/evidence/upload`
- `POST /api/evidence/link`
- `DELETE|POST /api/evidence/unlink`
- `GET /api/evidence/<id>`
- `GET /api/evidence/<id>/download`
- `GET /api/evidence/<id>/preview`
- `GET /api/evidence/<id>/audit-summary`

Upload multipart form ister:

- `company`
- `file`

Backend dosyayı staging'e yazar, boyut ve MIME kontrolü yapar, SHA256 hesaplar ve dedupe eder. Yeni dosya pending olarak DB'ye yazılır, optimization jobı başlatılır.

Link endpointi body içinde şirket, sheet, entry group listesi ve evidence id bekler. Evidence hazır değilse `409` döner.

## Translation API

Endpoint:

- `POST /run-translation`

Body:

- `company`

Akış:

1. Kullanıcı yetkisi kontrol edilir.
2. Şirket erişimi kontrol edilir.
3. Translation module yüklenir.
4. Sheet ve kolon planı hazırlanır.
5. Background job başlatılır.

Çeviri Data Entry kolonlarını günceller. Değişen kaynak satırlar için açık unmapped kayıtlar geçersizleştirilir.

## CCC API

CCC tarafında hem UI form POST hem JSON API kullanımı var.

Önemli endpointler:

- `GET/POST /data-sources/ccc-api`
- `POST /api/ccc/import-to-data-entry`
- `POST /api/suppliers/ccc/sync`
- `GET /api/suppliers/ccc/registry`

`/data-sources/ccc-api` üç işi yapabilir:

- Connection test.
- Tek endpoint sync.
- Run All APIs background job.

`/api/ccc/import-to-data-entry` purchase order satırlarını Data Entry'ye ekler. Body:

- `projects`, project label listesi.
- opsiyonel `year_filter`.

Import jobı CCC project idlerini çözer, purchase orderları çeker, status filtresi uygular, dedupe key üretir, template headerlarına çevirir ve Data Entry satırı olarak kaydeder. Insert edilen batchler için mapping jobı kuyruğa alınabilir.

## Engage Waste API

Endpointler:

- `POST /api/engage-waste/fetch`
- `POST /api/engage-waste/import`
- `GET /api/engage-waste/status`

Fetch body:

- `company_name`
- opsiyonel `limit_per_page`
- opsiyonel `max_pages`
- opsiyonel `reporting_period_fallback`
- opsiyonel `extra_query`

Fetch dış API'den raw satırları çeker, raw JSON arşivler, normalize eder, preview bundle yazar ve preview id döndürür.

Import body:

- `preview_id`
- opsiyonel `row_indices`

Import preview bundle'ı okur, selected rows Data Entry'ye yazar, dedupe yapar ve mapping jobı kuyruğa alabilir.

## Employee Commuting API

Endpointler:

- `GET/POST /api/employee-commuting/headcount`
- `GET/POST /api/employee-commuting/national-averages`
- `GET /api/employee-commuting/runs`
- `POST /api/employee-commuting/generate`

Headcount ve national average tabloları DB'de tutulur. Generate endpointi bu iki veri setinden Data Entry satırları üretir. İş sonunda mapping hedefleri varsa `employee_commuting_mapping` umbrella jobı başlatılır.

## Averages ve Scenarios API

Endpointler:

- `GET /data-sources/averages`
- `GET /data-sources/scenarios`
- `GET/POST /api/averages/save`
- `GET/POST /api/scenarios/save`

Averages save, DB'ye kaydettikten sonra virtual sheet mapping çalıştırabilir. Scenarios kaydı JSON payload olarak tutulur.

## Analytics ve Data Output API

Ekranlar çoğunlukla form POST ile çalışır:

- `/analytics/forecasting`
- `/analytics/decarbonization`
- `/analytics/mapped-window-output`
- totals/share/double-counting/audit output ekranları
- `/data-output/travel`
- `/data-output/klarakarbon`
- download endpointleri

Bu endpointler genelde workbook üretir veya en son output workbooku preview için okur. Run history JSON log dosyalarına yazılır.

## Admin API

Admin rotaları:

- Access request yönetimi.
- User role yönetimi.
- Governance register CRUD ve export.
- Background job admin ekranı.
- Emission factor browser.
- Supplier registries.
- Mapping review ve unmapped mapping yönetimi.
- Performance diagnostics.

Admin kontrolleri çoğunlukla route içinde `current_user.is_admin` veya rol helperlarıyla yapılır.

## API Tasarım Notları

Tutarlı yanıt formatı yok. Bazı endpointler `{ok: true}` döner, bazıları `{job_id, status}`, bazıları redirect/flash kullanır. Bu mevcut Flask/Jinja ağırlıklı yapı için çalışıyor, ancak API client geliştirmek isteyen biri endpoint bazında response shape kontrol etmeli.

Pahalı endpointlere dikkat:

- Mapping başlatma endpointleri.
- CCC sync ve import.
- Engage fetch.
- Analytics output generation.
- Workbook preview/download.
- Owner analytics.
- Admin performance diagnostics geniş snapshotlar.

Uzun işler için doğrudan request içinde çalıştırma yerine mevcut job patterni tercih edilmeli.
