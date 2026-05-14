# Database Architecture

Veri tabanı katmanı SQLAlchemy ORM ile kurulmuş. Modellerin tamamı `frontend/app.py` içinde tanımlı. Varsayılan runtime veritabanı SQLite dosyasıdır: `frontend/instance/ghg_data.db`.

## Genel Model

Şema üç ana iş alanına ayrılıyor:

- Platform ve kullanıcı kayıtları.
- Karbon veri girişi, mapping çıktısı ve evidence kayıtları.
- Sosyal, admin, governance ve analytics destek tabloları.

Bazı tablolar açık foreign key kullanıyor. Bazı ilişkiler ise string alanlarla kuruluyor. Özellikle `company_name`, `sheet_name` ve `entry_group` iş anahtarı gibi davranıyor.

## Çekirdek Tablolar

### User

Kullanıcı kimliği, şirket, rol ve profil alanlarını tutar. Flask-Login `UserMixin` ile kullanılır.

Önemli alanlar:

- `email`, unique.
- `password_hash`.
- `company_name`.
- `role`, `owner`, `super_admin`, `admin`, `manager`, `user` gibi değerler.
- `is_admin`, legacy/admin gate için hala kullanılıyor.
- Profil alanları ve onboarding durumu.

### Company

Şirket adı ve logo yolunu tutar. `created_by_user_id` ile kullanıcıya bağlanabilir. Data Entry tarafında şirket ilişkilerinin çoğu bu tabloya değil string `company_name` alanlarına dayanır.

### DataEntry

Web Data Entry ekranının ana tablosu. Satır bazlı değil, hücre bazlıdır.

Alanlar:

- `company_name`
- `sheet_name`
- `entry_group`
- `uploaded_by_user_id`
- `row_index`
- `column_name`
- `value`
- `created_at`

Bir grid satırı, aynı `company_name`, `sheet_name`, `entry_group` ve `row_index` değerlerini paylaşan birden fazla `DataEntry` kaydıdır. Bu tasarım template kolonlarının değişmesine uyum sağlar, ancak sorgu ve mapping öncesi pivot işlemlerini gerekli kılar.

### MappingRun

Web üzerinden tetiklenen tek şirket ve tek sheet mapping çalışmasını tutar.

Alanlar:

- `id`, kısa run id.
- `user_id`.
- `company_name`.
- `sheet_name`.
- `status`.
- `input_path`.
- `output_path`.
- `source_entry_group`.

Mapping pipeline için ana audit kaydıdır.

### MappingRunSummary

Dashboard ve Carbon Accounting ekranları için hafif özet tablodur. Mapping run çıktı workbookundan türetilir.

Alanlar:

- `run_id`
- `company_name`
- `sheet_name`
- `scope`
- `tco2e_total`
- `rows_count`
- `mapped_categories_count`
- `total_categories`
- `coverage_pct`

Bu tablo dashboard okumalarını hızlandırır. Ancak `run_id` için unique constraint var, foreign key yok.

### NormalizedEmissionRecord

Mapping sonrası satır düzeyi kanonik emisyon kaydıdır. Dashboard, carbon accounting ve admin raporları için Excel output yerine DB okuma katmanı sağlar.

Önemli alanlar:

- `mapping_run_id`, `MappingRun.id` foreign key.
- `company_name`, `sheet_name`.
- `source_entry_group`, `source_row_key`.
- `mapping_status`, `is_mapped`.
- `ef_id`, `ef_name`, `ef_source`, `ef_value`.
- `emissions_tco2e`.
- `scope`.
- `reporting_period_key`, `reporting_period_display`.
- `source_workbook_path`.

Unique constraint: `mapping_run_id`, `source_row_key`.

### MappingReviewSnapshot

Mapping çıktısını UI review ve audit için saklar. Mapping hesabını sürmez, mapped DataFrame'in okunabilir kopyasını taşır.

Öne çıkan alanlar:

- `mapping_run_id`
- `source_row_identifier`
- `description`, `supplier`, source amount/unit
- mapped EF bilgileri
- `confidence_score`
- `review_status`
- reviewer ve note alanları
- `detail_payload`

### MappingPreviewArchive

Preview detay ekranları için metadata tutar. Row-level preview disk üzerindeki JSONL/bundle dosyasında saklanır.

Alanlar:

- `run_id`, unique ve `MappingRun.id` foreign key.
- `user_id`
- `company_name`, `sheet_name`
- `mapped_row_count`, `unmapped_row_count`
- `totals_summary_json`
- `mapped_excel_path`
- `preview_bundle_rel`

### MappingUnmappedRow

Mapping çalışmış ama EF bulunamamış satırları ayrı review kuyruğunda tutar.

Alanlar:

- `run_id`, `MappingRun.id` foreign key.
- `user_id`
- `company_name`, `sheet_name`
- `source_entry_group`
- `row_number`, `row_label`
- `row_payload`
- `review_status`
- `assigned_ef_id`
- `owner_notes`
- resolved metadata

Indexler company, sheet, status ve run row lookup için eklenmiş.

## Evidence Tabloları

### EvidenceFile

Yüklenen belge veya görselin metadata kaydıdır.

Alanlar:

- `company_name`
- `original_filename`, `stored_filename`
- `file_extension`, `mime_type`
- `sha256_hash`
- original ve optimized size
- `storage_path`, `thumbnail_storage_path`
- `uploaded_by`
- `processing_status`
- deleted/orphan flags
- `relation_count`

Unique constraint: `company_name`, `sha256_hash`. Bu şirket bazında dedupe sağlar.

### DataEntryEvidence

Evidence dosyası ile Data Entry satırını bağlar.

Alanlar:

- `company_name`
- `sheet_name`
- `entry_group`
- `evidence_file_id`
- `linked_by`
- `linked_at`

Unique constraint aynı evidence dosyasının aynı Data Entry satırına iki kez bağlanmasını engeller.

## External Import ve Supplier Tabloları

### CccSupplierRegistry

CCC supplier master verisini tutar.

Önemli alanlar:

- `external_supplier_id`
- `supplier_name`
- `normalized_name`
- `source_system`
- `raw_json`
- country, currency, relation ve type alanları
- `active`, `deleted_at`
- sync timestamps
- `usage_count`

Unique constraint: `source_system`, `external_supplier_id`.

### SupplierSyncRun

CCC supplier sync işinin audit kaydıdır.

Alanlar:

- `sync_source`
- `sync_mode`
- `status`
- fetched/upserted/skipped sayıları
- `error_json`
- `triggered_by_user_id`
- `started_at`, `finished_at`

### SupplierSyncCheckpoint

Incremental sync için key-value checkpoint tutar.

### CarbonSteelSupplier ve InternalSupplier

Manuel tedarikçi registry tablolarıdır. Internal supplier registry Stage2 double counting kuralları için token export eder.

## Employee Commuting Tabloları

### EmployeeCommutingHeadcount

Şirket ve reporting period bazında headcount tutar. Unique constraint `company_name`, `reporting_period_key`.

### EmployeeCommutingNationalAverage

Ülke bazında ulaşım oranları ve average one day bilgisini tutar. Unique constraint `country`.

### EmployeeCommutingGeneratedRun

Employee Commuting dataset generation joblarının audit kaydıdır.

Bu akış önce DB tablolarını okur, Data Entry satırları üretir, sonra mapping jobı tetikleyebilir.

## Reports, Feed ve Collaboration

Rapor ve içerik tabloları:

- `Report`
- `ReportCategory`
- `Newsletter`
- `Event`
- `AwardsForm`
- `AwardsQuestion`
- `AwardsSubmission`
- `AwardsAnswer`
- `Challenge`
- `ChallengeResponse`

Feed tabloları:

- `FeedPost`
- `PostReaction`
- `Comment`
- `CommentLike`
- `UserFollow`

Collaboration tabloları:

- `Notification`
- `Message`
- `UserActivityLog`
- `PageVisitEvent`

Bu tablolar platform UI deneyimini destekler. Karbon hesaplamasının çekirdeği değiller, ancak admin analytics ve notification akışları için kullanılırlar.

## Governance Tabloları

### AccessRequest

Kullanıcı erişim taleplerini tutar.

### GovernanceRegister

API, yazılım, admin access, service account ve benzeri erişim kayıtlarını tutar. Credential benzeri metinleri engellemek için uygulama katmanında regex kontrolü bulunur.

### GovernanceRegisterAuditLog

Governance register değişikliklerinin append-only log kaydıdır.

## İlişkiler

Açık foreign key olan ana bağlar:

- `User` -> birçok kayıt için `created_by`, `uploaded_by`, `user_id`.
- `MappingRun` -> `MappingUnmappedRow`, `MappingReviewSnapshot`, `MappingPreviewArchive`, `NormalizedEmissionRecord`.
- `EvidenceFile` -> `DataEntryEvidence`.
- `AwardsForm` -> `AwardsQuestion`, `AwardsSubmission`; submission -> answers.
- `FeedPost` -> `Comment`, `PostReaction`.
- `Message` -> sender ve receiver kullanıcıları.

İş mantığında inferred ilişki gibi kullanılan alanlar:

- `DataEntry.company_name` ve `Company.company_name`.
- `DataEntry.company_name/sheet_name/entry_group` ile `DataEntryEvidence` alanları.
- `MappingRun.company_name/sheet_name/source_entry_group` ile Data Entry batchleri.
- `MappingRunSummary.run_id` ile `MappingRun.id`.
- `NormalizedEmissionRecord.reporting_period_key` ile dashboard filtreleri.

## Sorgu Riskleri

Olası N+1 veya pahalı sorgu alanları:

- `messaging_service.list_conversations()` mesajları aldıktan sonra her konuşma için `User.query.get()` ve unread count yapıyor.
- Evidence listelerinde uploader e-postaları ayrı query ile toplu çekiliyor; burada iyi bir batching var.
- Dashboard admin analytics önce çok sayıda `MappingRunSummary` alıp Python tarafında latest seçiyor.
- Owner analytics, activity logları pandas DataFrame'e çevirerek hesaplıyor; veri büyüdükçe request maliyeti artar.
- Excel output preview ekranları DB yerine dosya okur; büyük workbooklarda response süresi artabilir.

## ERD

Ana ERD diyagramı `docs/architecture/diagrams/database-erd.mmd` dosyasındadır.

## Bakım Notları

SQLite ve tek worker ayarı birlikte düşünülmeli. Veritabanı Postgres'e taşınmadan ve job state kalıcı hale gelmeden worker sayısı artırılırsa aynı jobı farklı processler farklı görür.

`DataEntry` esnek ama pahalı bir modeldir. Geniş gridler için her hücre ayrı row olduğu için save, load ve mapping öncesi pivot işlemleri dikkat ister.

String ilişkileri güçlü bir migration disiplini ister. Sheet adı değişiklikleri sadece template dosyasında değil, mapping, dashboard, import ve evidence tarafında da etkili olabilir.
