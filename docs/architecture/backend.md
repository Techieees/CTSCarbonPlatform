# Backend Architecture

Backend tek Flask uygulamasıdır. Ana giriş noktası `frontend/app.py`; aynı dosya route, model, helper, job ve persistence kodunun büyük bölümünü içerir.

## Uygulama Başlatma

`frontend/app.py` import sırasında şunları yapar:

- `.env` ve `config` üzerinden runtime pathlerini okur.
- `Flask`, `SQLAlchemy` ve `LoginManager` nesnelerini kurar.
- `frontend/instance/ghg_data.db` SQLite dosyasını kullanır.
- Upload klasörünü `FRONTEND_UPLOAD_DIR` olarak ayarlar.
- Evidence upload boyutunu `MAX_UPLOAD_BYTES` ile sınırlar.
- Global thread pool ve in-memory job store hazırlar.

`config.py` ve `config/__init__.py` birlikte bulunuyor. Flask tarafında `from config import ...` kullanıldığı için Python import çözümlemesinde package olan `config/__init__.py` pratikte ana kaynak gibi davranır. Bu dosya `DATA_DIR`, stage klasörleri, upload klasörleri ve external API ayarlarını oluşturur.

## Blueprint Durumu

Kod tabanında Flask Blueprint kullanılmıyor. Tüm rotalar `@app.route(...)` ile doğrudan app üzerine kayıt ediliyor.

Bu yapı küçük sistemlerde basit kalır. Bu kod tabanında ise rota sayısı, model sayısı ve job helperları aynı dosyada biriktiği için dosya artık mimari sınır yerine uygulama arşivi gibi davranıyor.

## Route Grupları

### Public ve marketing sayfaları

Public sayfalar `index`, `platform`, `who-we-are`, methodology, impact, ESG, CSRD ve LCA sayfalarını kapsar. Bu sayfalar çoğunlukla template render eder, DB kullanımı düşüktür.

### Authentication

Ana rotalar:

- `/login`
- `/logout`
- `/register`
- `/request-access`
- `/forgot-password`
- `/reset-password/<token>`
- `/profile/setup`
- `/settings/profile`

Login Flask-Login üzerinden yapılır. Kullanıcı parolası hash olarak `User.password_hash` alanında tutulur. Password reset tokenları `PasswordResetToken` tablosunda hashlenmiş token, expiry ve used bilgisiyle saklanır.

Email gönderimi `MAIL_*` ayarlarıyla yapılır. Mail ayarı yoksa reset akışı debug/log davranışına düşebilir.

### Data Entry

Ana ekran `/dashboard` rotasıdır. Data Entry API'leri:

- `/api/excel_schema/companies`
- `/api/excel_schema/sheets`
- `/api/excel_schema/headers`
- `/api/data_entry/rows`
- `/api/data-entry/site-tags`
- `/api/data-entry/reporting-periods`
- `/api/products-input/save`
- `/api/products-input/export`

Data Entry hücre bazlı saklanır. Her hücre `DataEntry` satırıdır; satır kimliği için `company_name`, `sheet_name`, `entry_group`, `row_index` ve `column_name` birlikte kullanılır.

### Mapping

Mapping rotaları:

- `/run-mapping`
- `/api/mapping/run`
- `/api/mapping/download/<run_id>`
- `/api/mapping/download_merged`
- mapping preview ve review sayfaları
- admin unmapped row ekranları

Mapping job `_run_mapping_job()` içinde yürür. Akış:

1. Data Entry satırları DataFrame'e çevrilir.
2. `run_mapping()` geçici workbook yazar.
3. `main_mapping.process_all_sheets()` çalıştırılır.
4. Çıktı workbook olarak saklanır.
5. Normalized emission, summary, review snapshot, preview archive ve unmapped kayıtları yazılır.
6. Kullanıcıya notification üretilir.

Stage2 ortak output klasörünü kullandığı için web mapping tarafında `_STAGE2_MAP_LOCK` var.

### Translation

`/run-translation` admin/owner kullanıcılar için Data Entry satırları üzerinde çeviri jobı başlatır. Modül `engine.stage1_preprocess.Datas.translate_me_the_chosen_one_30Sep` içinden yüklenir.

Çeviri sadece `TARGET_COMPANIES` ve `TRANSLATION_COLUMNS_BY_SHEET` ile tanımlanan şirket/sheet/kolon çiftlerinde çalışır. Sonuç Data Entry kolonlarına geri yazılır. Kaynak değer değiştiği için ilgili açık unmapped kayıtları supersede edilir.

### Evidence

Evidence API'leri:

- `/api/evidence/upload`
- `/api/evidence/link`
- `/api/evidence/unlink`
- `/api/evidence/for-row`
- `/api/evidence/search`
- `/api/evidence/<id>`
- `/api/evidence/<id>/download`
- `/api/evidence/<id>/preview`
- `/api/evidence/<id>/audit-summary`

Upload önce staging path'e kaydedilir. Hash alınır, şirket bazında dedupe edilir, `EvidenceFile` row'u pending olarak yazılır ve optimization background jobı başlatılır. Hazır olan dosya `DataEntryEvidence` ile bir veya daha fazla Data Entry satırına bağlanabilir.

### CCC API

CCC tarafında iki ayrı kullanım var:

- API source ekranı, CCC endpointlerini test eder ve workbook/cache çıktısı üretir.
- Data Entry import akışı, purchase order satırlarını Scope 3 Category 1 Data Entry satırına çevirir.

Önemli rotalar:

- `/data-sources/ccc-api`
- `/api/ccc/import-to-data-entry`
- `/api/suppliers/ccc/sync`
- `/api/suppliers/ccc/registry`

CCC integration modülleri `engine/stage1_preprocess/api_sources/` altında. Web tarafı bu modülleri `importlib.util.spec_from_file_location` ile yükler.

### Engage Waste API

Engage Waste iki adımlı çalışır:

- `/api/engage-waste/fetch`, API'den sayfaları çeker, raw JSON'u arşivler, preview bundle yazar.
- `/api/engage-waste/import`, preview içinden seçilen satırları Data Entry'ye yazar ve mapping jobı kuyruğa alabilir.

Transform kodu `frontend/services/engage_waste_service.py` içindedir. Waste stream çevirisi `deep_translator.GoogleTranslator` ile yapılır, hata olursa kaynak metin korunur.

### Admin ve governance

Admin tarafında kullanıcı yönetimi, access requests, governance register, emission factor browser, background jobs, performance diagnostics ve mapping review ekranları var.

Governance register ayrı audit log tutar. Metin alanlarında credential benzeri içerik için basit regex kontrolü vardır.

### Dashboard ve analytics

Dashboard tarafında iki ana yol var:

- Web dashboardları DB özetlerini okur.
- Analytics output ekranları Stage2 workbook üretir veya en son üretilmiş workbook'u okur.

Ekranlar arasında forecasting, decarbonization, mapped window output, totals tables, share analysis, double counting, audit output ve emissions map bulunur.

## Service Modülleri

`frontend/services/` altındaki modüller uygulamanın bazı işlerini ayırıyor:

- `notification_service.py`, notification row üretimi ve okundu işaretleme.
- `messaging_service.py`, konuşma listesi ve mesaj gönderme.
- `search_service.py`, arama ve run log kaynakları.
- `ccc_data_entry_import_service.py`, CCC purchase order satırlarını Data Entry hücrelerine çevirme.
- `engage_waste_service.py`, Engage Waste fetch, normalize, preview ve import hazırlığı.
- `supplier_sync_service.py`, CCC supplier registry sync.
- `reporting_period_service.py`, reporting period normalize ve sıralama.
- `site_tag_service.py`, site tag registry çözümü.

Servisler yardımcı olmuş, ancak çoğu hala `app.py` içindeki model sınıfları ve session ile çağrılıyor. Yani tam bir katman ayrımı yok.

## Background Jobs

`run_in_background()` her job için kısa bir id üretir, `jobs` sözlüğüne kayıt atar ve daemon thread başlatır.

Job türleri:

- `mapping`
- `employee_commuting_mapping`
- `translation`
- `preprocess`
- `pipeline`
- `evidence_processing`
- `ccc_import`
- `ccc_supplier_sync`
- `ccc_api_run_all`
- `engage_waste_import`
- analytics output işleri

Job iptali cooperative çalışır. Job fonksiyonu `_raise_if_job_cancelled()` çağırırsa iptali görür. Subprocess veya uzun pandas işlemi içinde iptal hemen etkili olmayabilir.

## Database Interaction

Kod SQLAlchemy ORM kullanıyor, ancak modeller ve sorgular aynı dosyada. Bazı alanlarda query yoğunluğu dikkat çekiyor:

- Messaging conversation listesi her conversation için kullanıcı ve unread count sorgusu yapabilir.
- Notification dropdown ve collaboration polling kısa aralıklarla API çağırır.
- Dashboard admin analytics bütün `MappingRunSummary` satırlarını alıp Python tarafında son run seçer.
- Excel preview ve analytics output ekranları request sırasında workbook okuyabilir.
- Evidence search `most_linked` sıralaması için aggregate subquery kullanır; doğru index yoksa büyüdükçe pahalılaşır.

`Engine` event listenerları SQL sürelerini ölçer ve request response headerlarına SQL sayısını yazar. Bu, darboğaz aramak için faydalı bir yerleşik araçtır.

## Büyük Dosyalar ve Bakım Riskleri

Belirgin büyük dosyalar:

- `frontend/app.py`, yaklaşık 26 bin satır.
- `engine/stage2_mapping/main_mapping.py`, yaklaşık 1700 satır.
- `engine/stage2_mapping/Run_Everything.py`, çok sayıda stage ve post-fix çağrısı içerir.
- `frontend/templates/dashboard.html`, Data Entry ekranı ve büyük inline JS taşır.

Riskler:

- `app.py` içinde model, route, job ve pipeline helperları birbirine yakın duruyor.
- Stage2 output klasörü paylaşıldığı için mapping eşzamanlılığı lock ile korunuyor.
- In-memory job store çoklu Gunicorn worker ile uyumlu değil.
- SQLite yazma kilidi nedeniyle paralel import/mapping işleri sınırlı.
- Bazı route handlerları request içinde Excel okur veya pandas hesapları yapar.
- Bazı ilişkiler string eşleşmesine dayanır; rename veya template değişikliği sessiz kırılma yaratabilir.

## Duplicated Logic ve Fragile Dependencies

Tekrarlayan desenler:

- Company normalize ve access check helperları birçok akışta tekrar çağrılıyor.
- Data Entry row normalize, dedupe ve upsert mantığı CCC, Engage, Employee Commuting ve manuel girişte benzer şekilde kullanılıyor.
- Analytics output ekranları benzer run history, file lookup ve workbook preview kodlarını paylaşıyor.
- Background job başlatma her endpointte benzer active job kontrolü ve progress mesajı ile tekrarlanıyor.

Kırılgan noktalar:

- Sheet adları kod içinde string olarak yaygın.
- Stage2 scriptleri çalışma dizini ve dosya adlarına duyarlı.
- `config.py` ve `config/__init__.py` aynı isimli modül/package olarak birlikte var.
- `frontend/app.py` içinde lazy importlar hata anında runtime'da patlar, startup sırasında görünmeyebilir.

## Synchronous Bottleneckler

Şu işler request içinde dikkat ister:

- CCC API fetch ekranında tek endpoint sync bazı durumlarda request içinde çalışabilir.
- Analytics output üretim rotaları bazı scriptleri doğrudan çalıştırır.
- Dashboard admin analytics geniş tarih aralığında özetleri Python tarafında toparlar.
- Excel preview okuma request sırasında yapılır.
- Evidence upload dosyayı request içinde staging'e yazar ve hash hesaplar; optimization background'a bırakılır.

## İyileştirme Adayları

Bu doküman refactor önermiyor, ancak riskleri kayda geçirmek için adaylar:

- `frontend/app.py` içinden model, auth, mapping, evidence, admin ve analytics rotalarını modüllere ayırmak.
- Job state'i veritabanı veya ayrı queue backend ile kalıcı hale getirmek.
- SQLite yerine Postgres kullanıldığında Gunicorn worker sayısını artırmayı değerlendirmek.
- Stage2 output yazımını run id bazlı izole klasöre taşımak.
- Dashboard için daha fazla precomputed summary kullanmak.
- Data Entry ve evidence ilişkilerinde string anahtarların etrafına daha açık constraint veya helper katmanı koymak.
