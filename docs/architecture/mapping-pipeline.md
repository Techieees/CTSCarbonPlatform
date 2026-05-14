# Mapping Pipeline

Mapping pipeline mevcut engine scriptlerini web uygulamasına bağlayan katmandır. Flask tarafı mapping mantığını baştan yazmaz; Data Entry verisini workbook formatına çevirir, Stage2 scriptlerini subprocess olarak çalıştırır, sonra çıktıyı DB ve UI için tekrar okur.

## Ana Parçalar

Pipeline üç katmandan oluşur:

- Web orchestration: `frontend/app.py`
- Pipeline wrapper: `pipeline/`
- Engine scripts: `engine/stage1_preprocess/` ve `engine/stage2_mapping/`

Web orchestration kullanıcı, şirket, sheet, Data Entry ve job state bilgisini bilir. Engine ise workbook dosyalarını ve mapping kurallarını bilir.

## Stage1 Preprocess

Stage1 veri kaynaklarını temizlenmiş workbooklara çevirir.

Kod:

- `pipeline/stages/stage1_preprocess.py`
- `preprocess_jobs.py`
- `engine/stage1_preprocess/Datas/`
- `engine/stage1_preprocess/api_sources/`

Stage1 script listesi:

- `the_chosen_one_11November.py`
- `Clean_me_the_chosen_one_2Dec.py`
- `normalized10.py`
- `Currency_converter_17Dec.py`
- `translate_me_the_chosen_one_30Sep.py`

Klarakarbon ve Travel gibi bazı kaynaklar özel preprocess joblarıyla çalışır. Bunlar kullanıcı uploadunu staging/output klasörlerine yazar, scriptleri subprocess ile çalıştırır, sonra output workbooku web UI için hazırlar.

## Stage2 Mapping

Stage2 ana entrypoint:

- `engine/stage2_mapping/Run_Everything.py`

Bu dosya şu adımları sırayla çağırır:

- `main_mapping`
- source append
- double counting booklets
- FERA mapping
- company totals aggregation
- GHGP category reorganize
- final cleaning
- period filter
- forecasting
- decarbonization scenarios

Stage2 varsayılan olarak ortak klasörleri ve sabit workbook adlarını kullanır. Bu yüzden Flask tarafında `_STAGE2_MAPPING_LOCK` bulunur. Aynı anda iki Stage2 run çalışırsa çıktı dosyaları karışabilir.

## Web Mapping Flow

Tek şirket/sheet mapping akışı:

1. Kullanıcı Data Entry ekranında sheet seçer.
2. Satırlar `DataEntry` tablosuna yazılır.
3. `/api/mapping/run` çağrılır.
4. Flask `MappingRun` kaydı oluşturur.
5. Data Entry batchi temporary Excel workbooka export edilir.
6. Stage2 input klasörüne uygun dosya hazırlanır.
7. `_run_mapping_job` background thread içinde çalışır.
8. Stage2 scripti subprocess olarak çağrılır.
9. Output workbook okunur.
10. `MappingRun`, `MappingRunSummary`, `NormalizedEmissionRecord`, `MappingReviewSnapshot` ve preview archive güncellenir.
11. UI job status polling ile sonucu gösterir.

Bu akışta kritik dönüşüm Data Entry hücre modelinden workbook sheet modeline geçiştir.

## Data Entry'den Workbook'a

`DataEntry` hücre bazlı saklandığı için pipeline öncesi şu işlem gerekir:

- `company_name`, `sheet_name`, `entry_group` filtrelenir.
- `row_index` ile logical row grupları kurulur.
- `column_name` değerleri Excel kolonlarına çevrilir.
- Template schema kolon sırası belirler.
- Workbook yazılır.

Mapping scriptleri workbook beklediği için bu adaptör katmanı korunmuş.

## Engine Mapping Logic

`engine/stage2_mapping/main_mapping.py` mapping'in büyük kısmını içerir.

Öne çıkan parçalar:

- Input ve override workbook adları.
- Excluded/drop sheet listeleri.
- Manual mapping workbookları.
- External lookup builder fonksiyonları.
- Service provided ve CTS Denmark özel lookup kuralları.
- EF id/name/value/source alanlarının hesaplanması.
- Her sheet için mapped output üretimi.

Bu dosya emission factor seçimi, manual override ve sheet-specific mapping kurallarını birlikte taşır. Web tarafı bu iş kurallarını çoğaltmamalı.

## Manual Mapping ve Overrides

Stage2 manual mapping workbooklarını okur:

- `manual_mappings`
- override workbook
- extra override workbook listesi

Mapping coverage ve unmapped review bu dosyalara ve Data Entry source kolonlarına bağlıdır. Sheet veya kolon adı değişirse hem template hem mapping lookup bozulabilir.

## Translation Pipeline

Translation mapping'den ayrı ama Data Entry ile yakın çalışır.

Akış:

1. Kullanıcı `/run-translation` çağırır.
2. Backend sheet ve kolon planını çıkarır.
3. Translation helper module import edilir.
4. Job thread içinde source textler çevrilir.
5. `DataEntry` target kolonları güncellenir.
6. Değişen satırlarla ilişkili open unmapped kayıtları invalid yapılır.

Translation doğrudan Data Entry tablosunu değiştirir. Bu yüzden mapping öncesinde veya unmapped review öncesinde çalıştırılması sonuçları etkiler.

## CCC Import'tan Mapping'e

CCC purchase order importu mapping pipeline'a veri kaynağı olarak bağlanır:

1. CCC API'den purchase order satırları çekilir.
2. Status filter uygulanır.
3. Dedupe key üretilir.
4. Satırlar Data Entry headerlarına normalize edilir.
5. `DataEntry` hücreleri olarak yazılır.
6. Insert edilen batchler için mapping jobı otomatik tetiklenebilir.

Bu sayede CCC entegrasyonu Stage2 logicini bilmeden aynı Data Entry adaptörünü kullanır.

## Output Normalization

Pipeline çıktılarını sadece Excel olarak bırakmıyor. Web tarafı bazı çıktıları DB'ye de yazar:

- `MappingRunSummary`
- `NormalizedEmissionRecord`
- `MappingReviewSnapshot`
- `MappingPreviewArchive`
- `MappingUnmappedRow`

Bu tablolar dashboard, review, audit ve download ekranlarını Excel parsing maliyetinden kısmen ayırır.

## Job ve Locking

Mapping long-running iş olduğu için thread içinde çalışır. Stage2 sırasında lock alınır. Bu, throughput'u düşürür ama shared output klasörleri nedeniyle yanlış dosya okuma riskini azaltır.

Cancel desteği cooperative çalışır. Job fonksiyonu belirli noktalarda cancel flag kontrol ederse durur. Çalışan subprocess her zaman anında kesilmeyebilir.

## Synchronous Bottleneckler

Dikkat edilmesi gereken alanlar:

- Stage2 pandas işlemleri CPU ve IO ağırlıklı.
- Excel read/write maliyeti büyük sheetlerde yüksek.
- Output preview request içinde workbook okuyabilir.
- Mapping sonuçlarının DB'ye normalize edilmesi büyük runlarda write yükü yaratır.
- Stage2 global output klasörleri lock gerektirir.

## Bakım Notları

Mapping pipeline'da en önemli sınır web modeli ile engine workbook modeli arasındadır. Yeni kaynak eklerken önce Data Entry sheet schema'sı, sonra workbook export, sonra Stage2 sheet mapping kontrol edilmeli.

Stage2 scriptlerinin shared filesystem varsayımları kırılmadan worker sayısı veya paralellik artırılmamalı.

Bir mapping hatası araştırılırken şu sırayla bakmak genelde en hızlı yoldur:

1. Data Entry row payload.
2. Export edilen input workbook.
3. Stage2 stdout/stderr.
4. Output workbook mapped sheet.
5. DB normalized records.
6. UI preview/archive.

Mapping pipeline diyagramı `docs/architecture/diagrams/mapping-pipeline.mmd` dosyasındadır.
