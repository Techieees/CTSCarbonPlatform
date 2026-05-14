# CTS Carbon Platform System Overview

Bu doküman mevcut kod tabanının nasıl çalıştığını anlatır. Amaç sistemi yeniden tasarlamak değil, bugün çalışan yapıyı bakım, denetim ve geliştirme açısından anlaşılır hale getirmektir.

## Kısa Özet

CTS Carbon Platform tek bir Flask uygulaması etrafında kurulmuş. Uygulama `frontend/app.py` içinde başlıyor; rota tanımları, SQLAlchemy modelleri, job kuyruğu, dashboard hesapları, admin ekranları ve mapping çağrıları büyük ölçüde aynı dosyada duruyor.

Karbon verisi birkaç yoldan sisteme giriyor:

- Kullanıcılar Data Entry ekranında Jinja/JavaScript tablosu üzerinden satır giriyor.
- Admin kullanıcılar CCC API ve Engage Waste API ile dış kaynaklardan veri çekiyor.
- Klarakarbon ve Travel MGMT gibi özel kaynaklar dosya yükleme ve preprocess scriptleri ile hazırlanıyor.
- Averages ve Employee Commuting gibi bazı veri setleri doğrudan veritabanına kaydedilip daha sonra Data Entry formatına çevriliyor.

Mapping tarafı Flask içinde yeniden yazılmamış. Web uygulaması Data Entry satırlarını geçici Excel dosyasına çeviriyor, `engine/stage2_mapping/main_mapping.py` içindeki mevcut mapping kodunu çalıştırıyor, sonra çıkan workbook ve DataFrame'i tekrar veritabanına bağlıyor. Bu karar iş mantığını tek yerde tutuyor, ama web isteği ile dosya tabanlı batch dünyası arasında sıkı bir bağ yaratıyor.

## Ana Parçalar

### Flask uygulaması

`frontend/app.py` uygulamanın merkezi. Şunları aynı süreçte yapıyor:

- Flask app, login manager ve SQLAlchemy kurulumu.
- SQLAlchemy model tanımları.
- Public sayfalar, authenticated sayfalar ve admin ekranları.
- Data Entry, mapping, evidence, notification, messaging ve analytics API rotaları.
- In-process background job kuyruğu.
- Stage1 ve Stage2 scriptlerini çağıran yardımcılar.
- Performans ölçümü için request ve SQL timing hookları.

Blueprint kullanılmıyor. Bu, rotaları hızlı bulmayı kolaylaştırıyor ama dosya büyüdükçe değişikliklerin yan etkisini anlamayı zorlaştırıyor.

### Veri tabanı

Uygulama varsayılan olarak SQLite kullanıyor: `frontend/instance/ghg_data.db`. Modeller `frontend/app.py` içinde tanımlı. Veriler üç ana grupta toplanıyor:

- Kullanıcı, profil, rol, aktivite ve erişim kayıtları.
- Data Entry, mapping run, normalized emission, evidence ve review kayıtları.
- Feed, notification, messaging, awards, reports ve governance ekranlarının kayıtları.

Bazı ilişkiler açık foreign key ile kurulmuş. Bazı iş ilişkileri ise string alanlar üzerinden yürüyor; örneğin `company_name`, `sheet_name`, `entry_group` üçlüsü Data Entry ve Evidence tarafında pratik bir bağ gibi kullanılıyor.

### Background jobs

Uzun süren işler `run_in_background()` ile aynı Flask sürecinde daemon thread olarak çalıştırılıyor. Job state bellekteki `jobs` sözlüğünde tutuluyor. Bazı işlerin ayrıca kalıcı run tablosu var, örneğin `MappingRun`, `EmployeeCommutingGeneratedRun` ve `SupplierSyncRun`.

Bu model basit ve anlaşılır, ancak process restart olduğunda in-memory job durumu gider. Gunicorn şu an tek worker ile çalışacak şekilde ayarlanmış; bu da SQLite kilitleri ve in-memory job state için bilinçli bir sınırlama.

### Mapping ve analytics pipeline

Pipeline iki modda çalışıyor:

- CLI tarafında `run_pipeline.py` ve `pipeline/` modülleri Stage1 ve Stage2 scriptlerini subprocess ile çağırıyor.
- Web tarafında `frontend/app.py` Data Entry satırlarını workbook'a yazarak `main_mapping.process_all_sheets()` çalıştırıyor.

Stage2 çıktı üretimi dosya tabanlı. `data/stage2_mapping/output/` ve `frontend/instance/mapping_runs/` içinde workbook kopyaları oluşuyor. Dashboard ve admin raporları bu çıktıların özetlerini veritabanına taşıyan `MappingRunSummary`, `NormalizedEmissionRecord`, `MappingReviewSnapshot` ve `MappingPreviewArchive` kayıtlarına dayanıyor.

### Frontend

Frontend Jinja template ve vanilla JavaScript karışımı. `base.html` ortak layout, sidebar, topbar, notification dropdown, Lottie ve performans ölçüm scriptlerini yüklüyor.

Data Entry ekranı büyük ölçüde `dashboard.html` içindeki inline JavaScript ile çalışıyor. Chart ekranları ECharts kullanıyor. Mapbox yalnızca locations sayfasında, ECharts world map ise emissions map sayfasında kullanılıyor.

## Veri Akışı

Temel akış:

1. Kullanıcı login olur; Flask-Login session ve `User` modeli üzerinden kimlik doğrulanır.
2. Kullanıcı Data Entry ekranında şirket ve kategori seçer.
3. Template registry ilgili şirket için görünür sheet listesini ve header setini döndürür.
4. Satırlar `DataEntry` tablosuna hücre bazlı kaydedilir.
5. Mapping job tetiklenirse satırlar DataFrame'e çevrilir.
6. Web katmanı geçici workbook oluşturur ve Stage2 mapping kodunu çağırır.
7. Mapping sonucu workbook olarak saklanır.
8. Normalized emission, summary, review snapshot ve unmapped row kayıtları veritabanına yazılır.
9. Dashboard, report, scope detail ve admin ekranları bu özet katmanlarını okur.

CCC API ve Engage Waste akışları Data Entry önüne otomasyon ekler. Dış kaynaktan veri çekilir, normalize edilir, dedupe anahtarı üretilir, hedef template headerlarına map edilir ve Data Entry satırı olarak yazılır. Sonrasında aynı mapping hattı kullanılır.

## Neden Bu Yapılar Var

Mevcut sistemde mapping kuralları uzun süredir kullanılan Excel ve pandas scriptlerinde duruyor. Flask uygulaması bu kuralları yeniden uygulamak yerine onları çağırıyor. Bu sayede web UI ile batch pipeline aynı hesaplama davranışını kullanıyor.

`NormalizedEmissionRecord` gibi özet tabloların varlığı önemli. Mapping çıktısı workbook olarak doğuyor, ancak dashboard ve rapor ekranlarının her istekte Excel okuması pahalı olurdu. Bu yüzden mapping sonrası satır düzeyi sonuçlar veritabanına taşınıyor.

Evidence sistemi de ayrı tutulmuş. Dosyanın kendisi storage altında duruyor, `EvidenceFile` dosya metadatasını, `DataEntryEvidence` ise Data Entry satırıyla ilişkiyi tutuyor. Bu yapı aynı belgeyi birden fazla satıra bağlamayı mümkün kılıyor.

## Güncel Tradeofflar

- Tek büyük Flask dosyası geliştirmeyi hızlandırmış, fakat artık modül sınırları zayıf.
- In-process job kuyruğu kurulum yükünü azaltıyor, fakat worker restart ve çoklu worker senaryosunda güvenilir değil.
- SQLite dağıtımı kolaylaştırıyor, fakat eşzamanlı yazma ve uzun mapping işleri sırasında kilit riski yaratıyor.
- Stage2 scriptleri import edilerek ve ortak output klasörüne yazılarak kullanılıyor. `_STAGE2_MAP_LOCK` bu yüzden gerekli.
- Template ve Data Entry mantığı esnek, fakat string alanlara dayalı ilişkiler bakımda dikkat istiyor.
- Frontend büyük oranda sayfa bazlı inline scriptlerle çalışıyor. Basit sayfalarda sorun değil, Data Entry gibi büyük ekranlarda test ve tekrar kullanım zorlaşıyor.

## Ölçeklenme ve Bakım Endişeleri

En görünür sınır Gunicorn ayarı: `workers = 1`, `threads = 4`. Bu ayar SQLite ve in-memory job state için mantıklı. Postgres veya kalıcı bir job kuyruğu olmadan worker sayısını artırmak job görünürlüğünü ve dosya yazımlarını belirsiz hale getirir.

Bakım açısından en riskli bölgeler:

- `frontend/app.py`, yaklaşık 26 bin satırla çok fazla sorumluluk taşıyor.
- Mapping çalıştırma web katmanına yakın duruyor ve Stage2 output klasörünü paylaşıyor.
- Dashboard ve report ekranlarında bazı hesaplar istekte yapılmaya devam ediyor.
- Bazı admin ve analytics ekranları Excel dosyalarını request sırasında okuyor.
- Messaging ve notification sorgularında sayfa başına birden fazla küçük sorgu oluşabilecek yerler var.
- CCC, Engage Waste, Employee Commuting ve mapping işleri aynı in-memory job mekanizmasına bağlı.

## Sıkı Bağlı Alanlar

- `DataEntry` ile mapping arasında header isimleri ve sheet isimleri üzerinden sıkı bir bağ var.
- `MappingRun`, `MappingRunSummary`, `MappingReviewSnapshot`, `MappingPreviewArchive`, `MappingUnmappedRow` ve `NormalizedEmissionRecord` aynı mapping run kimliği etrafında çalışıyor.
- `EvidenceFile` ve `DataEntryEvidence` fiziksel storage pathleriyle birlikte Data Entry satır anahtarlarına bağlı.
- `frontend/app.py` hem model sınıflarını hem servis çağrılarını bildiği için servislerin çoğu tersine app içindeki modellere parametre olarak bağımlı.
- Stage2 scriptleri belirli workbook adlarını, sheet adlarını ve output klasörlerini bekliyor.

## Async İşleme Nerede Önemli

Şu işlerin request thread içinde bitmesi beklenmemeli:

- Stage2 mapping.
- Klarakarbon ve Travel preprocess.
- CCC Run All APIs ve CCC purchase order import.
- Engage Waste import.
- Evidence optimization ve thumbnail üretimi.
- Employee Commuting dataset generation ve ardından gelen mapping işleri.
- Forecasting, decarbonization ve audit output üretimi.

Bugünkü async modeli thread tabanlı. Kullanıcı deneyimi için yeterli, ancak kalıcı kuyruk, retry ve process bağımsız job state gerekirse bu alan ilk aday olur.

## Diyagramlar

Mermaid diyagramları `docs/architecture/diagrams/` altında tutulur:

- `request-lifecycle.mmd`
- `authentication-flow.mmd`
- `mapping-pipeline.mmd`
- `translation-pipeline.mmd`
- `upload-flow.mmd`
- `evidence-processing.mmd`
- `background-jobs.mmd`
- `frontend-rendering-flow.mmd`
- `ccc-api-import-flow.mmd`
- `dashboard-data-flow.mmd`
