## Carbon-platform – Monorepo Yapısı

Bu repository 3 ayrı kod tabanını modüler biçimde içerir:

- **Stage 1 (Preprocess)**: `engine/stage1_preprocess`
  - Merge, cleaning, normalization, currency, translation adımlarını kapsar.
  - Mevcut script’lerin iş mantığı değiştirilmemiştir.
- **Stage 2 (Mapping & Scenario Engine)**: `engine/stage2_mapping`
  - Giriş noktası: `engine/stage2_mapping/Run_Everything.py`
  - Mevcut orchestrator/mantık korunur.
- **Frontend**: `frontend`
  - Flask tabanlı web arayüzü.

### Master entry point (tek komut)

`run_pipeline.py` stage’leri orkestre eden tek giriş noktasıdır. Mantığı değiştirmez; sadece mevcut script’leri doğru sırayla çağırır.

Örnek kullanım (repo kökünde `Carbon-platform` klasöründe):

```bash
python .\run_pipeline.py all
```

Stage2’ye argüman forward etmek için:

```bash
python .\run_pipeline.py all -- --start 2025-01-01 --months 12
```

Sadece Stage1:

```bash
python .\run_pipeline.py stage1
```

Sadece Stage2:

```bash
python .\run_pipeline.py stage2 -- --start 2025-01-01 --months 12
```

Dry-run (komutları yazdır):

```bash
python .\run_pipeline.py all --dry-run
```

### Yeni eklenen modül

- `pipeline/`: İnce “wrapper” katmanı
  - `pipeline/stages/stage1_preprocess.py`: Stage1 script’lerini sırayla çalıştırır
  - `pipeline/stages/stage2_mapping.py`: Stage2 `Run_Everything.py`’yi çalıştırır
  - `pipeline/orchestrator.py`: Stage seçimi ve orchestration

