# GHG Data Collection System

Modern ve kullanıcı dostu bir web tabanlı sera gazı (GHG) veri toplama sistemi. 28 alt şirketten veri toplamak için tasarlanmıştır.

## Özellikler

- 🔐 **Güvenli Kullanıcı Kimlik Doğrulama**: E-posta ve şifre ile giriş
- 🏢 **Çoklu Şirket Desteği**: 28 farklı şirket için ayrı erişim
- 📊 **Excel Schema Integration**: Sheet names and headers are read directly from company workbooks
- ✍️ **Web Data Entry**: Users enter data directly in the browser without downloading templates
- 📈 **Gelişmiş Raporlama**: Dashboard ve admin paneli
- 🔒 **Rol Tabanlı Erişim**: Admin ve kullanıcı rolleri
- 📱 **Responsive Tasarım**: Mobil ve masaüstü uyumlu

## Desteklenen Şirketler

- Navitas (Portugal, Nordics) -Portugal
- Navitas (Portugal, Nordics) - Norway and Finland
- Caerus Architects
- SD Nordics
- CTS-VDC
- CTS Nordics AS Norway
- CTS Sweden
- CTS Denmark
- QEC
- CTS Finland OY
- Velox
- Mecwide
- Porvelox
- DC Piping
- MC Prefab
- Gapit Nordics
- Nordic EPOD
- CTS EU Portugal (CTS Nordics Eng)
- BIMMS
- Commissioning Services

## Desteklenen Kategoriler

- Scope 1 Fuel Usage (Mobile Combustion)
- Scope 1 Gas Usage (Stationary Combustion)
- Scope 1 Fugitive Gas Emissions
- Scope 2 Electricity
- Scope 2 District Heating
- Scope 3 Category 1 Purchased Goods
- Scope 3 Category 1 Purchased Services
- Scope 3 Category 1 Tier 1 Supplier Summary
- Scope 3 Category 2 Capital Goods
- Scope 3 Category 4 and 9 Transportation and Distribution
- Scope 3 Category 5 Waste
- Scope 3 Category 6 Business Travel
- Scope 3 Category 7 Employee Commuting
- Scope 3 Category 8 Electricity
- Scope 3 Category 8 District Heating
- Scope 3 Category 8 Fuel Usage (Mobile Combustion)
- Scope 3 Category 8 Fugitive Gas Emissions
- Scope 3 Category 8 Gas Usage (Stationary Combustion)
- Scope 3 Category 10 Processing of Sold Products
- Scope 3 Category 11 Use of Sold Products
- Scope 3 Category 12 End of Life Treatment of Sold Products
- Scope 3 Category 13 Downstream Leased Assets
- Scope 3 Category 15 Investments
- Water Tracker

## Kurulum

### Gereksinimler

- Python 3.8+
- pip

### 1. Projeyi İndirin

```bash
git clone <repository-url>
cd ghg-data-collection-system
```

### 2. Sanal Ortam Oluşturun

```bash
python -m venv venv
venv\Scripts\activate  # Windows
# veya
source venv/bin/activate  # Linux/Mac
```

### 3. Bağımlılıkları Yükleyin

```bash
pip install -r requirements.txt
```

### 4. Veritabanını Kurun

#### SQLite (Varsayılan)
```bash
python app.py
```

### 5. Admin Kullanıcısı Oluşturun

```python
from app import app, db, User
from werkzeug.security import generate_password_hash

with app.app_context():
    admin_user = User(
        email='admin@example.com',
        password_hash=generate_password_hash('your-password'),
        company_name='Admin',
        is_admin=True
    )
    db.session.add(admin_user)
    db.session.commit()
```

## Kullanım

### 1. Uygulamayı Başlatın

```bash
python app.py
```

Uygulama `http://localhost:5000` adresinde çalışacaktır.

### 2. Kullanıcı Kaydı

1. Ana sayfada "Get Started" butonuna tıklayın
2. E-posta, şifre ve şirket adınızı girin
3. Hesabınızı oluşturun

### 3. Data Entry

1. Data Entry sayfasında şirket workbook’unuzdaki kategorileri görün
2. İlgili kategori/sheet’i seçin
3. Veriyi doğrudan web tablosuna girin
4. Kaydedin ve gerekirse hemen mapping çalıştırın

### 4. Admin Paneli

Admin kullanıcıları için:
- Kullanıcı yönetimi
- Gönderim geçmişi
- Sistem istatistikleri
- Veri indirme

## Veritabanı Şeması

### Users
- `id`: Birincil anahtar
- `email`: E-posta adresi (benzersiz)
- `password_hash`: Şifre hash'i
- `company_name`: Şirket adı
- `is_admin`: Admin yetkisi
- `created_at`: Kayıt tarihi

### MappingRunSummary
- `run_id`: Mapping run kimliği
- `company_name`: Şirket adı
- `sheet_name`: Kategori/sheet adı
- `scope`: Scope numarası
- `tco2e_total`: Toplam ton CO2e
- `rows_count`: İşlenen satır sayısı
- `created_at`: Oluşturulma zamanı

## Konfigürasyon

### Environment Variables

`.env` dosyası oluşturun:

```env
SECRET_KEY=your-secret-key-here
MAIL_SERVER=smtp.gmail.com
MAIL_PORT=587
MAIL_USE_TLS=true
MAIL_USERNAME=your-email@gmail.com
MAIL_PASSWORD=your-app-password
```

### Excel Schema Source

Şirketlere ait giriş şeması doğrudan `engine/stage1_preprocess/Datas/input` altındaki workbook’lardan okunur.
Her workbook:
1. Şirketi temsil eder
2. Sheet adlarıyla kategorileri tanımlar
3. Header satırlarıyla web form kolonlarını tanımlar

## Güvenlik

- Şifreler bcrypt ile hash'lenir
- CSRF koruması
- Dosya yükleme güvenliği
- Rol tabanlı erişim kontrolü
- Session yönetimi

## Geliştirme

### Yeni Şirket / Kategori Ekleme

1. İlgili şirket workbook’unu `engine/stage1_preprocess/Datas/input` altına ekleyin
2. Gerekli sheet’leri ve header’ları workbook içinde tanımlayın
3. Gerekirse header adına göre frontend/backend validation kurallarını güncelleyin

### API Geliştirme

Gelecekte REST API eklemek için:
- Flask-RESTful kullanın
- JWT token authentication ekleyin
- API rate limiting ekleyin

## Deployment

### Production Sunucusu

```bash
# Gunicorn ile
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app

# Nginx ile reverse proxy
# SSL sertifikası ekleyin
# Environment variables ayarlayın
```

### Docker (Gelecekte)

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:app"]
```

## Sorun Giderme

### Yaygın Sorunlar

1. **Workbook bulunamıyor**: Stage1 input klasöründeki şirket dosyasını kontrol edin
2. **Dosya yükleme hatası**: Dosya boyutu ve formatını kontrol edin
3. **Validation hatası**: Tarih alanlarının `YYYY-MM-DD`, sayısal alanların numeric olduğundan emin olun

### Loglar

```bash
# Debug modunda çalıştırın
export FLASK_ENV=development
python app.py
```

## Katkıda Bulunma

1. Fork yapın
2. Feature branch oluşturun (`git checkout -b feature/amazing-feature`)
3. Commit yapın (`git commit -m 'Add amazing feature'`)
4. Push yapın (`git push origin feature/amazing-feature`)
5. Pull Request oluşturun

## Lisans

Bu proje MIT lisansı altında lisanslanmıştır.

## İletişim

Sorularınız için: [your-email@example.com]

## Changelog

### v1.0.0 (2025-01-18)
- İlk sürüm
- Temel kullanıcı yönetimi
- Web tabanlı veri girişi
- Admin paneli
- Responsive tasarım 