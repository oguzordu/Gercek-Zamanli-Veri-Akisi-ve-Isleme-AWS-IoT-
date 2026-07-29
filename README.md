# Real-Time IoT Data Pipeline

## Türkçe

Gerçek zamanlı bir veri toplama hattı: bir üretici (producer) veriyi AWS IoT
Core'a akıtıyor, bu da bir Lambda fonksiyonunu tetikleyerek her kaydı
ilişkisel bir veritabanına yazdırıyor.

### Mimari

```
data_producer/producer.py → AWS IoT Core → lambda_function/lambda_function.py → RDS
```

- **`producer.py`** — veriyi gerçek zamanlı üretip bir cihaz sertifikasıyla
  kimlik doğrulayarak MQTT üzerinden yayınlıyor.
- **AWS IoT Core** — akışı alıp bir IoT kuralı üzerinden Lambda'ya
  yönlendiriyor.
- **`lambda_function.py`** — gelen her kaydı Amazon RDS'ye yazan bir AWS
  Lambda fonksiyonu.

### Kullanılan Teknolojiler

Python, AWS IoT Core, AWS Lambda, Amazon RDS, MQTT, X.509 cihaz
sertifikaları.

### Not

Bu proje, artık kapatılmış bir AWS hesabı üzerinde ders projesi olarak
geliştirildi; pipeline şu an deploy edilmiş durumda değil. Repodaki cihaz
sertifikaları etkisiz durumda (hesap artık mevcut değil) ve sadece referans
amaçlı tutuluyor — `certs/*-private.pem.key` yine de git-ignore edilmiş
durumda.

---

## English

A real-time data ingestion pipeline: a producer streams data to AWS IoT Core,
which triggers a Lambda function that persists each record to a relational
database.

### Architecture

```
data_producer/producer.py → AWS IoT Core → lambda_function/lambda_function.py → RDS
```

- **`producer.py`** — generates and publishes data in real time over MQTT,
  authenticated with a device certificate.
- **AWS IoT Core** — receives the stream and routes it to Lambda via an IoT
  rule.
- **`lambda_function.py`** — an AWS Lambda function that writes each incoming
  record to Amazon RDS.

### Tech Stack

Python, AWS IoT Core, AWS Lambda, Amazon RDS, MQTT, X.509 device
certificates.

### Note

This was built as a coursework project on an AWS account that has since been
closed; the pipeline is not currently deployed. Device certificates in this
repo are inert (the account no longer exists) and are kept for reference
only — `certs/*-private.pem.key` is git-ignored going forward regardless.
