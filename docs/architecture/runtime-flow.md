# Runtime Flow

Bu dosya uygulamanın request, job, upload, dashboard ve import akışlarını birlikte anlatır. Ayrıntılı diyagramlar `docs/architecture/diagrams/` altındadır.

## Request Lifecycle

Normal bir page request şu sırayla ilerler:

1. Browser Nginx'e gelir.
2. Nginx isteği Gunicorn'a proxy eder.
3. Gunicorn tek worker içindeki threadlerden biriyle Flask route'unu çalıştırır.
4. `before_request` hookları session, profile completion, readonly guard ve timing bilgilerini işler.
5. Route DB, filesystem veya external API ile konuşur.
6. Template veya JSON response üretilir.
7. `after_request` hookları slow route ve activity metriklerini kaydeder.
8. Browser response'u alır, JS varsa ek API çağrılarını başlatır.

Bu akış server-rendered template merkezlidir. Dashboard ve Data Entry gibi ekranlar ilk HTML sonrası API polling ve fetch çağrılarıyla davranış kazanır.

## Authentication Flow

Login formu email/password alır. Kullanıcı DB'de bulunur, password hash doğrulanır, sonra Flask-Login session cookie oluşturur.

Login sonrası ek guardlar çalışır:

- Kullanıcı aktif mi?
- Profile setup tamamlanmış mı?
- Route public mi?
- Kullanıcının rolü route için yeterli mi?
- Readonly auditor yazma endpointine gitmeye çalışıyor mu?

Admin ve owner kontrolleri merkezi bir policy katmanında değil; helperlar ve route içi kontrollerle uygulanıyor.

## Background Job Flow

Long-running işler HTTP request içinde tamamlanmaz. Route job kaydını memory'ye yazar ve thread başlatır.

Tipik job lifecycle:

1. API request job başlatır.
2. `run_in_background()` job dict oluşturur.
3. Thread `app.app_context()` içinde target fonksiyonu çalıştırır.
4. Job fonksiyonu progress ve message alanlarını günceller.
5. UI `job-status/<job_id>` endpointini poll eder.
6. İş bittiğinde status `completed`, `failed` veya `cancelled` olur.
7. Cleanup thread eski tamamlanan jobları memory'den siler.

Job state process memory'dedir. Process restart olduğunda progress kaybolur. Bazı işlerin sonuçları DB veya dosyada kalır.

## Upload Flow

Upload akışları dosya tipine göre farklılaşır. Evidence upload en belirgin örnektir.

Evidence flow:

1. Browser multipart form ile dosyayı gönderir.
2. Flask dosyayı temporary staging path'e yazar.
3. Boyut, extension ve MIME benzeri signature kontrolü yapılır.
4. SHA256 hesaplanır.
5. Aynı şirket içinde hash varsa mevcut kayıt kullanılır.
6. Yeni dosya final storage pathine taşınır.
7. `EvidenceFile` pending olarak kaydedilir.
8. Thumbnail/optimization background jobı başlar.
9. UI dosyayı row ile linkler.
10. Preview/download route'ları evidence dosyasını servis eder.

Profile photo upload ayrı route ve storage helperları kullanır. Legacy static path kapalıdır.

## Evidence Processing

Evidence processing format bazlıdır:

- PDF: Ghostscript ile optimize edilmeye çalışılır. İlk sayfa thumbnail için PyMuPDF kullanılabilir.
- Image: Pillow ile WebP optimize ve thumbnail üretimi.
- Office/archive gibi dosyalar: güvenli archive rewrite denenebilir veya raw olarak tutulur.

Processing başarısız olursa original dosya tutulabilir. Kayıtta `processing_status`, `processing_error`, size ve thumbnail path alanları güncellenir.

## Mapping Flow

Mapping request kullanıcıdan şirket ve sheet alır. Data Entry satırları workbooka dönüştürülür, Stage2 subprocess çalıştırılır ve sonuçlar DB'ye normalize edilir.

Runtime açısından önemli noktalar:

- Stage2 shared klasörleri nedeniyle lock kullanılır.
- Subprocess stdout/stderr job loguna yazılır.
- Excel dosyaları hem input hem output contract olarak kullanılır.
- Dashboard verileri için summary/normalized tablolara yazılır.

Mapping sonucu aynı anda birkaç tüketiciye gider:

- Download.
- Preview.
- Review/unmapped queue.
- Dashboard summaries.
- Carbon Accounting charts.

## Translation Flow

Translation Data Entry üzerinde çalışır. Source kolonları çevrilir ve target kolonlar güncellenir.

Önemli etki: Translation değişen satırları mapping review tarafında invalid yapabilir. Bu doğru, çünkü mapped/unmapped kararının dayandığı source text artık değişmiştir.

## CCC API Import Flow

CCC import web UI'dan veya API endpointinden başlar.

Akış:

1. Endpoint credentials ve project seçimini doğrular.
2. CCC API client purchase order veya configured endpoint verisini çeker.
3. Raw data normalized DataFrame'e çevrilir.
4. Import service Data Entry headerlarına map eder.
5. Dedupe key daha önce import edilen satırları ayırır.
6. Yeni satırlar `DataEntry` kayıtlarına yazılır.
7. Notification oluşturulur.
8. İstenirse mapping jobı başlatılır.

Supplier sync ayrı bir akıştır ama aynı CCC client ailesini kullanır.

## Dashboard Data Flow

Dashboard tek bir kaynaktan beslenmiyor:

- `DataEntry`, source data varlığını gösterir.
- `MappingRun`, son jobları ve statusleri verir.
- `MappingRunSummary`, coverage ve totals verir.
- `NormalizedEmissionRecord`, chart ve accounting hesapları için kullanılır.
- In-memory `jobs`, aktif background job panelini besler.
- Notification tablosu topbar ve action cardları besler.

Bu yüzden dashboard performansı sadece tek query ile ölçülemez. Route, DB özetleri, job state ve template payload büyüklüğü birlikte izlenmeli.

## Frontend Rendering Flow

Server-rendered template ilk HTML'i getirir. Sonra page-specific JS iki modelden biriyle çalışır:

- Template içine gömülü JSON payloadı okur.
- API endpointlerinden veri çeker.

Chart sayfaları çoğunlukla JSON script tag kullanır. Data Entry ekranı fetch API ile state'i sürekli yeniler.

## Hata ve Gözlem Noktaları

Bakılması gereken yerler:

- Flask logları: route exception, slow request, job status.
- Job detail: `jobs[job_id]` içindeki `message`, `error`, `result`.
- DB tabloları: `MappingRun`, `SupplierSyncRun`, `EvidenceFile`.
- Output dosyaları: `storage/pipeline_runs*`, `data/stage2_mapping/output`.
- Browser console: chart payload parse ve JS fetch hataları.
- `/admin/performance-diagnostics`: frontend node/script/chart sayıları ve backend timing snapshotları.

## Akış Diyagramları

Bu dosyayı destekleyen Mermaid diyagramları:

- `request-lifecycle.mmd`
- `authentication-flow.mmd`
- `background-jobs.mmd`
- `upload-flow.mmd`
- `evidence-processing.mmd`
- `mapping-pipeline.mmd`
- `translation-pipeline.mmd`
- `ccc-api-import-flow.mmd`
- `dashboard-data-flow.mmd`
- `frontend-rendering-flow.mmd`
