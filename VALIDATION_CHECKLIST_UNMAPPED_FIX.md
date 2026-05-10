# Doğrulama: Unmapped / CCC / Tarih / Performans düzeltmeleri

**Önkoşul:** Üretim benzeri ortam, bilinen bir `company` + `sheet` (eski Portekizce açık unmapped, çevrilmiş satırlar, CCC import, `Jan'-2026` ve `Jan-2026` karışık dönemler).

| # | Kontrol | Beklenen | Nasıl |
|---|--------|----------|--------|
| 1 | Çevrilmiş ama hâlâ eşleşmeyen satırlar | Admin **Mapping › Unmapped** listesinde görünür (No Match senkronu sonrası). | Çeviri + Map çalıştır; filtre `status=open`, arama ile açıklama doğrula. |
| 2 | EF ile tam eşlenen satırlar | Açık unmapped’tan kaybolur (stale cleanup). | Map sonrası ilgili satırda live `ef_id` + uyumlu `status`; unmapped sayfasını yenile. |
| 3 | CCC “No Match” | Dashboard batch / bildirimlerde **fully mapped** sayılmaz; `mapping_state` `pending` veya `unmapped` (Map sonrası duruma göre). | CCC import → Data Entry’ye bak; admin upload popup / batch API yanıtında `mapping_state`, `mapping_counts`. |
| 4 | Upload popup | `pending` / `unmapped` / `partially_mapped` / `fully_mapped` rozetleri metinle uyumlu. | Giriş yap → popup; `mapping_status` + `mapping_state` (DevTools → `api_admin_upload_notifications`). |
| 5 | Raporlama dönemi etiketi | Açılır listede **Jan-2026** (tırnaksız); eski `Jan'-2026` varsa görünümde normalize. | Data Entry veya `GET /api/data-entry/reporting-periods`; grid hücresi `normalizeReportingPeriodClient`. |
| 6 | Dönem sırası | Kronolojik (Ocak→Aralık 2026), alfabetik değil. | Aynı API / dropdown seçenek sırası. |
| 7 | Sayfa yükü | `/dashboard`, `/dashboard/analytics`, `/locations`, `/admin/mapping/unmapped` gözle daha akıcı veya `[PERF]` süreleri karşılaştırmalı iyileşme. | Sunucu logunda `[PERF] page=…` satırları (önce/sonra isteğe bağlı ölçüm). |
| 8 | Log etiketleri | Aşağıdakiler üretim logunda görünür (ilgili aksiyonda). | Sunucu stdout/stderr veya uygulama günlüğü. |

**Aranacak log önekleri**

- `[UNMAPPED_SYNC]` — supersede / insert / live kapanma
- `[UNMAPPED_REFRESH]` — kapsamlı stale reconcile özeti
- `[TRANSLATION_INVALIDATION]` — çeviri sonrası metadata temizliği
- `[MAPPING_STATE]` — mapping metadata özeti; CCC tamam mesajı
- `[PERF]` — sayfa / DB / aggregation süreleri

**Hızlı API kontrolleri**

```http
GET /api/admin/upload_notifications
GET /api/data-entry/reporting-periods
```

**Not:** Çeviri sadece değişen satırların `entry_group` + satırına supersede uygular; Map yeniden çalıştırılmadan yeni unmapped satırı yoksa listede görünmeyebilir — testte mutlaka **Map** adımını dahil edin.
