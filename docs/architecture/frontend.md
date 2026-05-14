# Frontend Architecture

Frontend Flask/Jinja template, Bootstrap, vanilla JavaScript, ECharts, Mapbox ve Lottie üzerine kurulu. Tek sayfa uygulaması değil; her ana ekran server-rendered template olarak gelir, gerekli JavaScript sayfada veya static dosyalarda çalışır.

## Template Yapısı

Ana layout `frontend/templates/base.html`.

Bu dosya şunları sağlar:

- Ortak `<head>` stilleri.
- Public marketing nav.
- Authenticated app shell, sidebar ve topbar.
- Search bar ve notification dropdown include'ları.
- Theme preload scripti.
- Lottie ve global JS yüklemeleri.
- Frontend performans metriği toplama.
- Admin upload notification modalını tetikleyen inline script.

Ortak template parçaları:

- `partials/app_sidebar.html`
- `components/search_bar.html`
- `components/notification_dropdown.html`
- `components/chat_widget.html`
- `partials/feed_post_card.html`
- `components/post_composer.html`
- `macros/ui_illustrations.html`

## Sayfa Grupları

### Public pages

`index.html`, `platform.html`, `locations.html`, methodology, impact, ESG, CSRD ve LCA sayfaları public template grubunu oluşturur. `base.html` içinde endpoint listesiyle public marketing nav seçilir.

### App shell pages

Login sonrası sayfalarda sidebar ve topbar görünür. Data Entry, feed, dashboard, reports, admin, supplier ve governance ekranları bu shell içinde render edilir.

### Data Entry

`dashboard.html` en ağır templatelerden biridir. Aynı dosya:

- Company/sheet seçimi.
- Editable grid.
- Evidence modalı.
- Mapping butonları.
- Background job paneli.
- Recent mappings listesi.
- Inline JavaScript ile Data Entry client logic.

Bu ekran template ve JavaScript olarak tek dosyada büyümüş. UI davranışını anlamak için hem Flask endpointlerini hem template içindeki inline scripti okumak gerekiyor.

### Analytics output

`analytics_output.html` farklı output türleri için ortak ekran gibi kullanılır. Forecasting, decarbonization, CCC API source, mapped window, totals, share analysis ve audit output aynı template üzerinden farklı context ile render edilir.

### Charts

Chart ekranları çoğunlukla JSON payload script tagleri ile veri alır ve static JS modülleriyle çizilir.

Örnekler:

- `dashboard_admin.html`, chart payloadlarını JSON script tagleriyle verir.
- `scope_detail.html`, `scope-dashboard-payload` verir.
- `emissions_map.html`, `emissions-map-payload` verir.
- `owner_analytics.html`, `owner-analytics-chart-data` verir.

`frontend/static/js/charts/init_charts.js` chart init hub gibi çalışır. ECharts hazır olana kadar requestAnimationFrame ile bekler, sonra ilgili DOM/payload varsa chartları kurar.

## JavaScript Modülleri

### Global scripts

`base.html` her authenticated veya public sayfada şu scriptleri yükleyebilir:

- Bootstrap bundle.
- `cts-performance.js`
- Lottie web CDN.
- `lottie-init.js`
- `busy-overlay.js`
- `saas.js`
- `global-effects.js`
- `theme.js`
- authenticated sayfalarda `mapping-notification-cards.js`, `collaboration.js`, `sidebar-layout.js`

Bu global yükler sayfalar arasında tutarlı davranış sağlar. Buna karşılık basit public sayfalarda bile bazı genel script maliyeti oluşabilir.

### Performance helpers

`cts-performance.js` küçük bir runtime helper sağlar:

- Tekil poller yönetimi.
- Script'i bir kez yükleme.
- Init timing ölçümü.
- Document hidden iken poller durdurma.

`base.html` load sonrasında DOM node, script, image, chart host ve Lottie sayısını localStorage'a yazar. `/admin/performance-diagnostics` bu browser metriğini gösterebilir.

### Lottie

`lottie-init.js` `.lottie-icon[data-animation]` elementlerini tarar. IntersectionObserver ile görünürlüğe göre initialize eder. MutationObserver ile sonradan eklenen iconları takip eder.

Dikkat noktası: `base.html` Lottie web CDN'i defer olarak yükler, `lottie-init.js` de gerekirse `CtsPerf.loadScriptOnce()` ile lazy load yapabilir. Bu iki yol birlikte çalışıyor, ama Lottie asset sayısı çok olan sayfalarda başlangıç maliyeti izlenmeli.

### Collaboration

`collaboration.js` notification, unread chat ve typing/thread update polling yapar. `CtsPerf.managePoll` kullanıldığı yerler var, ancak typing/thread poll için doğrudan `setInterval` de bulunuyor.

Bu ekranlar çok kullanıcı olduğunda backend'e düzenli kısa aralıklı istek üretir.

### Data Entry inline script

`dashboard.html` içindeki inline JS şu işleri yapar:

- Companies ve sheets yükleme.
- Header ve row fetch.
- Grid render ve paste/edit işlemleri.
- Save.
- Mapping job başlatma.
- Translation job başlatma.
- Evidence upload/link/unlink modalı.
- Job polling.

Bu kod sayfa davranışının büyük bölümünü taşır. Static module'a ayrılmadığı için test etmek ve başka sayfada kullanmak zor.

### Chart system

Chart modülleri `frontend/static/js/charts/` ve `frontend/static/js/components/charts/` altında.

Ana desen:

1. Flask route template'e JSON payload yazar.
2. Template ECharts CDN yükler.
3. `init_charts.js` veya sayfaya özel chart script çalışır.
4. `echarts_theme.js` içindeki ortak `initChart()` chart instance oluşturur.
5. Theme change eventlerinde chartlar yeniden çizilir veya resize edilir.

Bu yapı jQuery veya framework gerektirmiyor. Chartların veri sözleşmesi ise template idleri ve JSON shape'lerine bağlı.

## Map Sistemleri

İki farklı map yaklaşımı var:

- `locations.html` Mapbox GL JS kullanır. Data URL'den location GeoJSON/JSON çeker, ayrıca ArcGIS water risk layer sorgusu yapar.
- `emissions_map.html` ECharts world map kullanır. Ülke koordinatları ve flag verileri static JSON dosyalarından fetch edilir.

Mapbox token template üzerinden `window.MAPBOX_TOKEN` olarak verilir.

## Modal Sistemleri

Bootstrap modal ağırlıklı kullanılıyor.

Önemli modal alanları:

- Evidence attachments modalı.
- Admin upload notification modalı.
- Feed delete ve challenge response modalları.
- Mapping review detail panel davranışları.
- Governance register edit/create formları.

Modal state çoğunlukla sayfa içi JS içinde tutuluyor. Büyük ekranlarda modal davranışı backend endpointleriyle sıkı bağlı.

## Upload Components

Upload UI'leri:

- Evidence upload, Data Entry modalı içinden multipart POST.
- Report/newsletter/admin uploads.
- Klarakarbon preprocess upload.
- Travel preprocess upload.
- Profile photo ve cover image upload.

Evidence upload en gelişmiş akışa sahip. Dosya staging'e yazılır, dedupe edilir, background optimization başlar. UI pending ve ready durumlarını API'den izler.

## Notification Systems

Notification dropdown `components/notification_dropdown.html` ile layout'a dahil edilir. Backend `Notification` tablosundan okunmamış sayıları ve son bildirimleri verir.

Mapping jobları, CCC import, supplier sync, analytics ve API connection testleri notification üretir. Ayrıca bazı mapping notification payloadları feed kartı gibi render edilebilir.

## Ağır Template ve Rendering Riskleri

Dikkat edilmesi gereken templateler:

- `dashboard.html`, Data Entry ekranı, evidence modalı ve uzun inline JS nedeniyle ağır.
- `analytics_output.html`, workbook preview ve job polling davranışlarını aynı dosyada taşır.
- `owner_analytics.html`, çok sayıda chart host içerir.
- `dashboard_admin.html`, admin chart payloadlarını ve enterprise chart hostlarını birlikte render eder.
- `base.html`, tüm sayfalara çok sayıda global asset ve inline script ekler.

Olası bottleneckler:

- Chart payloadları büyük JSON olarak HTML içine gömülürse response ve parse süresi artar.
- Data Entry grid geniş sheetlerde DOM node sayısını hızlı artırır.
- Lottie icon sayısı yüksekse SVG init maliyeti hissedilir.
- `collaboration.js` polling çok kullanıcıda backend request sayısını artırır.
- Mapbox water risk layer dış API çağrısı kullanıcı tarafında bekleme yaratabilir.

## Tekrarlanan JavaScript Desenleri

Sık tekrar eden patternler:

- `fetch` wrapperları.
- Busy state butonları.
- Table row render ve collect logic.
- Job polling.
- Bootstrap modal açma/kapama.
- JSON script tag parse etme.

`cts-performance.js` ve chart helperları bazı tekrarları azaltıyor. Data Entry ve admin ekranlarında hala sayfa içi özel JS ağırlığı yüksek.

## Frontend Veri Akışı

Tipik server-rendered chart akışı:

1. Flask route DB veya workbook verisini hazırlar.
2. Template payloadı `<script type="application/json">` içine yazar.
3. ECharts CDN yüklenir.
4. Static chart module payloadı okur ve chartı render eder.

Tipik Data Entry akışı:

1. `/dashboard` shell ve initial state render eder.
2. JS şirket/sheet listesini API'den çeker.
3. Header ve row API'leri grid verisini getirir.
4. Kullanıcı değişiklikleri save API'sine gönderir.
5. Mapping veya translation job API ile başlatılır.
6. Job status polling UI progress günceller.

## Bakım Notları

Frontend bug ararken önce template mi static JS mi karar vermek gerekiyor. Aynı davranış iki yerde olabilir.

Yeni chart eklerken mevcut `init_charts.js` ve `echarts_theme.js` patterni tercih edilmeli. Yeni Data Entry davranışı eklerken inline script daha da büyüyeceği için küçük, sayfaya özel static modüle taşımak daha okunur olur.

Global script yükleri basit public sayfalarda da çalıştığı için asset maliyeti düzenli izlenmeli. `/admin/performance-diagnostics` bu iş için zaten veri topluyor.
