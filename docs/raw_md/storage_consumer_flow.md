<hr>
<font face="B Mitra" size=5>
<div dir=rtl>

<font size=6>
<b>زیرسیستم ذخیره‌سازی (Storage Consumer)</b>
</font>

<br>
<p align="justify">
<font size=4>
این مستند معماری، فلوی اجرا، تضمین‌های داده‌ای و تصمیمات مهندسی زیرسیستم Storage Consumer را شرح می‌دهد. این زیرسیستم سه تاپیک <code>app-stats</code>، <code>reviews</code> و <code>network-metrics</code> را از Kafka مصرف می‌کند و آن‌ها را در PostgreSQL می‌نویسد. نقش آن در پروژه یک نقش ویژه است: <b>این تنها نقطه‌ای است که داده‌ی پروژه از حالت «در حال عبور» به حالت «ماندگار و قابل کوئری» تبدیل می‌شود.</b>
</font>
</p>

<br>
<p align="justify">
<font size=4>
همین یک جمله، تفاوت سطح سخت‌گیری این زیرسیستم با بقیه را توضیح می‌دهد. اگر Crawler وسط کار Crash کند، همان اپ در دور بعدی دوباره خزش می‌شود و چیزی از دست نمی‌رود. اگر Network Analyzer شکست بخورد، همان فایل pcap دوباره تحلیل می‌شود. اما اگر Storage Consumer یک پیام را از دست بدهد، آن داده <b>برای همیشه</b> رفته است. تمام پیچیدگی‌های این مستند — Commit دستی Offset، کلیدهای Idempotency، تراکنش واحد، و صف پیام‌های مرده — پاسخ مستقیم همین یک نیازمندی هستند.
</font>
</p>

<hr>

<font size=5>
<b>۱. چهار تضمین اصلی زیرسیستم</b>
</font>

<br>
<p align="justify">
<font size=4>
پیش از هر جزئیات فنی، کل طراحی این زیرسیستم را می‌توان در چهار تضمین خلاصه کرد. هر تصمیم مهندسی در ادامه‌ی این مستند، در خدمت یکی از این چهار مورد است.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>تضمین</th>
<th>مکانیزم تحقق</th>
<th>چه چیزی را غیرممکن می‌کند</th>
</tr>
<tr>
<td><b>هیچ پیام منتشرشده‌ای از دست نمی‌رود</b></td>
<td>Offset فقط <b>پس از</b> Commit موفق PostgreSQL جابه‌جا می‌شود (<code>enable_auto_commit=False</code>)</td>
<td>پیامی که خوانده شده اما نوشته نشده، هرگز به‌عنوان «پردازش‌شده» علامت نمی‌خورد</td>
</tr>
<tr>
<td><b>تحویل مجدد، داده را تکراری نمی‌کند</b></td>
<td>هر جدول یک <code>UNIQUE</code> روی کلید طبیعی پیام دارد و هر نوشتن یک <code>INSERT ... ON CONFLICT</code> است</td>
<td>تبدیل At-least-once کافکا به Effectively-once در سطح پایگاه‌داده</td>
</tr>
<tr>
<td><b>یک پیام خراب، کل تاپیک را متوقف نمی‌کند</b></td>
<td>پیام غیرقابل‌ذخیره در جدول <code>dead_letter_events</code> ثبت می‌شود و Offset از رویش عبور می‌کند</td>
<td>قفل‌شدن ابدی یک Partition روی یک پیام معیوب (Poison Pill)</td>
</tr>
<tr>
<td><b>یک Pipeline نیمه‌جان، یعنی فرآیند مرده</b></td>
<td>شکست هر Thread، تمام Threadهای دیگر را متوقف و فرآیند را با کد خروج غیرصفر خارج می‌کند</td>
<td>کانتینری که Uptime سالم نشان می‌دهد در حالی که یکی از سه تاپیک بی‌صدا مصرف نمی‌شود</td>
</tr>
</table>

<hr>

<font size=5>
<b>۲. جایگاه زیرسیستم در معماری کل پروژه</b>
</font>

<br>
<p align="justify">
<font size=4>
این زیرسیستم انتهای زنجیره‌ی جمع‌آوری داده و ابتدای زنجیره‌ی تحلیل است. دو تولیدکننده‌ی مستقل (Crawler و Network Analyzer) روی سه تاپیک پیام می‌نویسند، و این زیرسیستم هر سه را به چهار جدول تحت مالکیت خودش تبدیل می‌کند.
</font>
</p>

</div>
</font>

```mermaid
flowchart LR
    CR["Crawler<br/>(هر ۱ ساعت)"] --> T1["Topic: app-stats<br/>۳ Partition"]
    CR --> T2["Topic: reviews<br/>۳ Partition"]
    NA["Network Analyzer<br/>(بر اساس درخواست)"] --> T3["Topic: network-metrics<br/>۳ Partition"]

    T1 --> SC
    T2 --> SC
    T3 --> SC

    subgraph SC["Storage Consumer - یک فرآیند، سه Thread"]
        direction TB
        P1["Pipeline: app-stats<br/>Group: storage-consumer.app-stats"]
        P2["Pipeline: reviews<br/>Group: storage-consumer.reviews"]
        P3["Pipeline: network-metrics<br/>Group: storage-consumer.network-metrics"]
    end

    P1 --> DB1["جدول app_stats"]
    P2 --> DB2["جدول reviews"]
    P3 --> DB3["جدول network_metrics"]
    P1 --> DLQ["جدول dead_letter_events"]
    P2 --> DLQ
    P3 --> DLQ

    APP["جدول applications<br/>(مالکیت: App API / Django)"] -.->|"فقط خواندن<br/>package_name ← application_id"| SC

    style CR fill:#2563eb,color:#fff
    style NA fill:#7c3aed,color:#fff
    style SC fill:#0f172a,color:#fff
    style DLQ fill:#dc2626,color:#fff
    style APP fill:#f59e0b,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<hr>

<font size=5>
<b>۳. تصمیم معماری کلیدی: چرا سه Thread و نه یک Consumer واحد؟</b>
</font>

<br>
<p align="justify">
<font size=4>
یک Consumer واحد می‌توانست به هر سه تاپیک Subscribe کند و کد ساده‌تری داشته باشد. این گزینه عمداً رد شد، و دلیل آن <b>هیچ ارتباطی به سرعت ندارد</b> — به «انصاف در تأخیر» مربوط است.
</font>
</p>

<br>
<p align="justify">
<font size=4>
حجم بار این سه تاپیک به‌شدت نامتوازن است. تاپیک <code>reviews</code> یک مسئله‌ی توان عملیاتی است: در هر دور خزش تا ۲۰۰٫۰۰۰ پیام در بازه‌ی ۵ تا ۱۵ دقیقه منتشر می‌شود. در مقابل، <code>network-metrics</code> یک مسئله‌ی تأخیر است: روزی چند پیام، اما درست در لحظه‌ای که کاربر یک تست شبکه اجرا کرده و منتظر دیدن نتیجه است. با یک Consumer واحد، آن چند پیام حساس به تأخیر پشت صف ۲۰۰٫۰۰۰ پیامی نقدها می‌ماندند و دقیقه‌ها معطل می‌شدند.
</font>
</p>

<br>
<p align="justify">
<font size=4>
پس Thread در این طراحی ابزار <b>جداسازی (Isolation)</b> است، نه ابزار افزایش سرعت. هر Pipeline حلقه‌ی Poll مستقل، Consumer Group مستقل، اتصال پایگاه‌داده‌ی مستقل و کش مستقل خودش را دارد. نتیجه‌ی جانبی این تصمیم بسیار مهم است: چون هیچ‌چیز بین Threadها مشترک نیست (به‌جز تنظیمات Immutable و یک رخداد توقف)، <b>در کل این زیرسیستم حتی یک Lock هم وجود ندارد</b>.
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    Q{"سه تاپیک با بار نامتوازن<br/>چگونه مصرف شوند؟"}

    Q -->|"گزینه ۱"| ONE["یک Consumer واحد<br/>Subscribe به هر سه تاپیک"]
    Q -->|"گزینه ۲ (انتخاب‌شده)"| MULTI["سه Thread، سه Consumer Group"]

    ONE --> BAD1["صف مشترک: ۲۰۰٫۰۰۰ نقد<br/>جلوی یک پیام network-metrics"]
    ONE --> BAD2["Backoff پایگاه‌داده برای یک تاپیک<br/>دو تاپیک دیگر را هم متوقف می‌کند"]
    ONE --> BAD3["Reset کردن Offset یک تاپیک<br/>بدون تأثیر بر بقیه ممکن نیست"]

    MULTI --> GOOD1["تأخیر هر تاپیک مستقل<br/>(اندازه‌گیری‌شده: زیر ۱ ثانیه هنگام ingest ۳٫۰۰۰ نقد)"]
    MULTI --> GOOD2["Share-Nothing: بدون Lock"]
    MULTI --> GOOD3["Lag و Offset هر تاپیک جدا و قابل مدیریت"]

    style MULTI fill:#16a34a,color:#fff
    style ONE fill:#dc2626,color:#fff
    style Q fill:#2563eb,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>Pipeline</th>
<th>جدول مقصد</th>
<th>اندازه‌ی بچ (<code>max_poll_records</code>)</th>
<th>ماهیت مسئله</th>
</tr>
<tr>
<td><code>app-stats</code></td>
<td><code>app_stats</code></td>
<td>۲۰۰</td>
<td>حجم متوسط و منظم؛ یک پیام به‌ازای هر اپ در هر دور</td>
</tr>
<tr>
<td><code>reviews</code></td>
<td><code>reviews</code></td>
<td>۵۰۰</td>
<td>توان عملیاتی؛ انبوه پیام در بازه‌ی کوتاه، بچ بزرگ‌تر بهینه‌تر است</td>
</tr>
<tr>
<td><code>network-metrics</code></td>
<td><code>network_metrics</code></td>
<td>۵۰</td>
<td>تأخیر؛ بهتر است هرچه دارد را بنویسد تا منتظر پر شدن بچ بماند</td>
</tr>
</table>

<hr>

<font size=5>
<b>۴. فلوی کامل پردازش یک بچ</b>
</font>

<br>
<p align="justify">
<font size=4>
قلب این زیرسیستم یک حلقه‌ی کوتاه است: <code>poll</code> → پردازش → <code>commit</code>. آنچه این حلقه را کوتاه نگه می‌دارد یک قاعده است: <b>هیچ حالتی (State) از یک Poll به Poll بعدی منتقل نمی‌شود.</b> نتیجه‌ی این قاعده آن است که Rebalance شدن Consumer Group در میانه‌ی یک بچ به‌طور خودکار بی‌خطر است — بچ در جریان، نانوشته رها می‌شود و به هر عضوی که Partition را تحویل بگیرد دوباره تحویل داده می‌شود.
</font>
</p>

</div>
</font>

```mermaid
sequenceDiagram
    participant K as Kafka
    participant W as Worker
    participant D as decoders
    participant R as ApplicationResolver
    participant B as batching
    participant DLQ as DeadLetterRepository
    participant Repo as Repository
    participant PG as PostgreSQL

    W->>K: poll(timeout=1000ms, max_records=N)
    K-->>W: بچ رکوردها

    Note over W: شروع تراکنش واحد برای کل بچ

    W->>D: decode_batch(records)
    D-->>W: (پیام‌های سالم، پیام‌های ردشده)
    Note over D: JSON نامعتبر، فیلد الزامی غایب،<br/>نوع اشتباه، مقدار خارج از بازه‌ی ستون

    W->>R: resolve(cursor, مجموعه‌ی package_nameها)
    R->>PG: SELECT ... WHERE package_name = ANY(%s)
    Note over R: یک کوئری برای کل بچ، نه یک کوئری به‌ازای هر پیام<br/>+ کش TTL اختصاصی همین Thread
    PG-->>R: نگاشت package_name ← application_id
    R-->>W: application_idها

    W->>B: bind_application_ids(...)
    B-->>W: (رکوردهای متصل‌شده، اپ‌های ثبت‌نشده)

    W->>DLQ: record(cursor, تمام پیام‌های ردشده)
    Note over DLQ,PG: در همان تراکنش بچ

    W->>B: dedupe_by_key(rows)
    Note over B: الزامی است، نه بهینه‌سازی:<br/>Postgres اجازه نمی‌دهد یک INSERT دو بار<br/>همان ردیف را در ON CONFLICT DO UPDATE لمس کند

    W->>Repo: upsert(cursor, rows)
    Repo->>PG: INSERT ... VALUES %s ON CONFLICT ... RETURNING (xmax = 0)
    PG-->>Repo: ردیف‌های بازگشتی
    Repo-->>W: WriteOutcome(inserted, updated, skipped)

    Note over W,PG: COMMIT تراکنش — داده اکنون ماندگار است

    W->>K: commit() — و فقط اکنون Offset جابه‌جا می‌شود
    W->>W: یک خط لاگ INFO با تمام شمارنده‌ها
```

<font face="B Mitra" size=5>
<div dir=rtl>

<hr>

<font size=5>
<b>۵. ترتیب حیاتی: نوشتن پیش از Commit</b>
</font>

<br>
<p align="justify">
<font size=4>
مهم‌ترین خط این زیرسیستم، ترتیب دو عملیات است. <code>enable_auto_commit</code> عمداً خاموش است، چون Commit خودکار بر پایه‌ی زمان کار می‌کند نه بر پایه‌ی موفقیت نوشتن — یعنی می‌تواند Offset پیامی را جابه‌جا کند که هنوز در پایگاه‌داده ننشسته است. با Commit دستی و پس از نوشتن، تنها حالت خرابی ممکن، <b>تحویل مجدد</b> است، و تحویل مجدد به‌لطف قیدهای <code>UNIQUE</code> بی‌خطر است.
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    S["بچ خوانده شد"] --> WR["نوشتن در PostgreSQL"]

    WR -->|"موفق"| CM["Commit کردن Offset در Kafka"]
    WR -->|"Crash پیش از نوشتن"| C1["Offset جابه‌جا نشده<br/>← بچ دوباره تحویل داده می‌شود<br/>← هیچ داده‌ای گم نشده"]

    CM -->|"موفق"| OK["چرخه‌ی بعدی"]
    CM -->|"Crash پس از نوشتن، پیش از Commit"| C2["داده نوشته شده ولی Offset قدیمی است<br/>← بچ دوباره تحویل داده می‌شود<br/>← ON CONFLICT آن را بی‌اثر می‌کند"]
    CM -->|"شکست Commit (Rebalance)"| C3["داده ماندگار است<br/>← لاگ هشدار، ادامه‌ی کار<br/>← پس از ۵ شکست پیاپی: خروج عامدانه"]

    C1 --> SAFE["نتیجه: At-least-once"]
    C2 --> SAFE
    C3 --> SAFE
    SAFE --> EO["به‌همراه Idempotency<br/>= Effectively-once"]

    style WR fill:#2563eb,color:#fff
    style CM fill:#f59e0b,color:#fff
    style SAFE fill:#16a34a,color:#fff
    style EO fill:#0f172a,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
نکته‌ی مهم درباره‌ی شکست Commit: شکست Commit به‌خودی‌خود بی‌خطر است (داده از قبل ماندگار شده و تحویل مجدد بی‌اثر است)، پس این حالت لاگ می‌شود و کار ادامه پیدا می‌کند. اما اگر Commit هرگز موفق نشود، Pipeline تا ابد همان رکوردها را بازپردازش می‌کند در حالی که Lag هیچ‌وقت کاهش نمی‌یابد. این حالت <code>Livelock</code> بسیار سخت‌تر از یک Crash تشخیص داده می‌شود، پس عمداً به Crash تبدیل شده است: پس از ۵ شکست پیاپی، فرآیند با خطا خارج می‌شود تا کانتینر Restart شود.
</font>
</p>

<hr>

<font size=5>
<b>۶. Idempotency: کلید طبیعی هر جدول</b>
</font>

<br>
<p align="justify">
<font size=4>
کافکا تحویل At-least-once می‌دهد؛ تبدیل آن به Effectively-once وظیفه‌ی این زیرسیستم است. مکانیزم، یک قید <code>UNIQUE</code> روی «کلید طبیعی» هر پیام است، به‌همراه یک <code>INSERT ... ON CONFLICT</code> متناسب با معنای همان جدول. انتخاب بین <code>DO NOTHING</code> و <code>DO UPDATE</code> در هر جدول یک تصمیم معنایی است، نه سلیقه‌ای.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>جدول</th>
<th>کلید Idempotency</th>
<th>رفتار در تعارض</th>
<th>استدلال</th>
</tr>
<tr>
<td><code>app_stats</code></td>
<td><code>(application_id, crawled_at)</code></td>
<td><code>DO NOTHING</code></td>
<td>هر پیام با همان جفت کلید، طبق تعریف یک تحویل مجدد است — Crawler برای هر اپ در هر دور یک پیام با <code>crawled_at</code> دقیق (میکروثانیه) می‌فرستد، پس چیز تازه‌تری در پیام دوم نیست. خزش بعدی <code>crawled_at</code> جدید دارد و ردیف جدید می‌سازد؛ همین است که جدول را Append-Only نگه می‌دارد.</td>
</tr>
<tr>
<td><code>reviews</code></td>
<td><code>(review_id)</code></td>
<td><code>DO UPDATE</code> با نگهبان یکنوا</td>
<td>تنها جدولی که واقعاً به‌روزرسانی می‌شود، چون <code>thumbs_up_count</code> یک نقد در طول زمان تغییر می‌کند. اما به‌روزرسانی بی‌قید و شرط خطرناک است (بخش بعد).</td>
</tr>
<tr>
<td><code>network_metrics</code></td>
<td><code>(analysis_id)</code></td>
<td><code>DO NOTHING</code></td>
<td>یک فایل pcap یک‌بار تحلیل می‌شود؛ همان <code>analysis_id</code> برای بار دوم یعنی تحویل مجدد، نه اصلاح. اجرای واقعی مجدد یک تست، UUID تازه و در نتیجه ردیف تازه تولید می‌کند.</td>
</tr>
<tr>
<td><code>dead_letter_events</code></td>
<td><code>(topic, partition, kafka_offset)</code></td>
<td><code>DO NOTHING</code></td>
<td>مختصات یک رکورد در کافکا، هویت آن است. Replay یک Partition همان پیام‌های خراب را دوباره رد می‌کند و این نباید ردیف‌ها را چند برابر کند.</td>
</tr>
</table>

<hr>

<font size=5>
<b>۷. نگهبان یکنوا روی جدول reviews</b>
</font>

<br>
<p align="justify">
<font size=4>
جدول <code>reviews</code> تنها جدولی است که به‌روزرسانی می‌شود، و همین آن را در معرض دو خرابی واقعی قرار می‌دهد که هر دو با یک شرط <code>WHERE</code> حل شده‌اند. عبارت به‌کاررفته چنین است:
</font>
</p>

<br>
<p align="justify">
<font size=4>
<code>ON CONFLICT (review_id) DO UPDATE SET ... WHERE EXCLUDED.last_synced_at &gt; reviews.last_synced_at</code>
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    IN["پیام نقد رسید<br/>(کلید: review_id)"] --> EX{"این review_id<br/>از قبل وجود دارد؟"}

    EX -->|"خیر"| INS["INSERT ردیف جدید<br/>first_seen_at = now()"]

    EX -->|"بله"| GUARD{"last_synced_at پیام<br/>تازه‌تر از ردیف موجود است؟"}

    GUARD -->|"بله — پیام تازه‌تر"| UPD["UPDATE مقادیر<br/>(thumbs_up_count و ...)"]
    GUARD -->|"خیر — پیام کهنه"| SKIP["No-op<br/>در شمارنده‌ی skipped ثبت می‌شود"]

    SKIP --> R1["سناریوی ۱ - Replay:<br/>یک پیام قدیمی، ردیف تازه را بازنویسی نمی‌کند"]
    SKIP --> R2["سناریوی ۲ - ترتیب برهم‌خورده:<br/>کافکا ترتیب را فقط داخل هر Partition تضمین می‌کند،<br/>پس Retry کند یک خزش قدیمی می‌تواند بعد از خزش جدید برسد"]

    style INS fill:#16a34a,color:#fff
    style UPD fill:#2563eb,color:#fff
    style SKIP fill:#f59e0b,color:#fff
    style GUARD fill:#7c3aed,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
دو ستون هم عمداً از فهرست <code>DO UPDATE SET</code> غایب هستند:
</font>
</p>

<p align="justify">
<font size=4>
<ul>

<li><b><code>first_seen_at</code>:</b> مقدار <code>DEFAULT now()</code> خودش را از لحظه‌ی Insert نگه می‌دارد، تا معنایش «اولین بار که این نقد را دیدیم» بماند، مهم نیست چند بار بعد از آن همگام‌سازی مجدد شود.</li>
<br>

<li><b><code>sentiment</code>:</b> این ستون به زیرسیستم آینده‌ی <code>sentiment/</code> تعلق دارد. دست زدن به آن در اینجا یعنی هر بار که Crawler یک نقد را دوباره می‌خواند، نتیجه‌ی تحلیل احساسات محاسبه‌شده پاک شود.</li>

</ul>
</font>
</p>

<hr>

<font size=5>
<b>۸. تفکیک سه‌گانه‌ی خطاها</b>
</font>

<br>
<p align="justify">
<font size=4>
اشتباه گرفتن این سه دسته با هم، دقیقاً همان جایی است که یک Consumer کافکا یا داده از دست می‌دهد یا تا ابد قفل می‌شود. به همین دلیل این تفکیک در کد صریح و آزمون‌شده است.
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    E["خطایی رخ داد"] --> T{"دسته‌بندی خطا"}

    T -->|"موقتی (Transient)"| TR["OperationalError, InterfaceError<br/>(قطع اتصال، Deadlock، Restart پایگاه‌داده)"]
    T -->|"دائمی، مخصوص یک پیام"| PM["MessageDecodeError<br/>یا package_name ثبت‌نشده"]
    T -->|"مرگبار (Fatal)"| FT["ConfigError, MigrationError<br/>و هر خطای پیش‌بینی‌نشده"]

    TR --> TRA["Retry همان بچ با Backoff نمایی<br/>۱s → ۲s → ۴s → ۸s → ۱۶s (۵ تلاش)<br/>+ ساخت مجدد اتصال پیش از هر تلاش"]
    TRA --> TRB["هرگز Dead-letter نمی‌شود:<br/>ریختن یک OperationalError به DLQ یعنی دور انداختن<br/>داده‌ی کاملاً سالم فقط چون Postgres در حال Restart بود"]

    PM --> PMA["ثبت در dead_letter_events<br/>+ عبور Offset از روی پیام"]
    PMA --> PMB["هرگز Retry نمی‌شود:<br/>Retry کردن یک JSON نامعتبر، Partition را برای همیشه قفل می‌کند"]

    FT --> FTA["خروج فرآیند با کد غیرصفر<br/>← Restart کانتینر<br/>← ادامه از آخرین Offset ثبت‌شده"]

    style TR fill:#f59e0b,color:#fff
    style PM fill:#dc2626,color:#fff
    style FT fill:#0f172a,color:#fff
    style T fill:#2563eb,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
یک نکته‌ی طراحی: خطاهای موقتی <b>عمداً هیچ کلاس اختصاصی در این زیرسیستم ندارند</b>. آن‌ها همان <code>psycopg2.OperationalError</code> و <code>kafka.errors.KafkaError</code> اصلی باقی می‌مانند. بسته‌بندی‌کردن‌شان در یک Exception داخلی، فقط تشخیص «کدام مجموعه Retry می‌شود» را سخت‌تر می‌کرد.
</font>
</p>

<hr>

<font size=5>
<b>۹. صف پیام‌های مرده در همان تراکنش</b>
</font>

<br>
<p align="justify">
<font size=4>
پیامی که هرگز قابل ذخیره نیست (JSON نامعتبر، فیلد هویتی غایب، یا <code>package_name</code> ثبت‌نشده) در جدول <code>dead_letter_events</code> ثبت می‌شود. نکته‌ی کلیدی این طراحی، <b>محل Commit</b> است: این ثبت در همان تراکنشی انجام می‌شود که داده‌های سالم همان بچ در آن نوشته می‌شوند.
</font>
</p>

<br>
<p align="justify">
<font size=4>
این همان خصوصیتی است که یک تاپیک DLQ در کافکا نمی‌توانست بدهد: با تراکنش واحد، <b>غیرممکن است</b> که Offset از روی یک پیام خراب عبور کند در حالی که رکورد آن پیام گم شده باشد. اگر تراکنش Rollback شود، هم داده و هم رکورد خطا با هم برمی‌گردند و بچ دوباره تحویل داده می‌شود.
</font>
</p>

</div>
</font>

```mermaid
flowchart LR
    subgraph TX["یک تراکنش واحد PostgreSQL"]
        direction TB
        G["ردیف‌های سالم<br/>INSERT ... ON CONFLICT"]
        BAD["ردیف‌های ردشده<br/>INSERT INTO dead_letter_events"]
    end

    IN["بچ ۵۰۰ پیامی<br/>(۴۹۷ سالم + ۳ خراب)"] --> TX
    TX -->|"COMMIT"| DONE["هر دو با هم ماندگار شدند<br/>← اکنون Offset جابه‌جا می‌شود"]
    TX -->|"ROLLBACK"| BACK["هیچ‌کدام ماندگار نشد<br/>← Offset دست‌نخورده<br/>← تحویل مجدد کل بچ"]

    style TX fill:#eff6ff,color:#0f172a
    style G fill:#16a34a,color:#fff
    style BAD fill:#dc2626,color:#fff
    style DONE fill:#2563eb,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
جدول <code>dead_letter_events</code> علاوه بر دلیل خطا، <b>خودِ Payload خام</b> را در یک ستون <code>BYTEA</code> نگه می‌دارد (نه <code>TEXT</code>) — دقیقاً به این دلیل که پیامی که به‌خاطر UTF-8 نامعتبر رد شده هم باید قابل ذخیره باشد. Payloadهای بزرگ‌تر از ۶۴ کیلوبایت بریده می‌شوند و این بریدگی در متن دلیل خطا ثبت می‌شود.
</font>
</p>

<br>
<p align="justify">
<font size=4>
<b>کنترل حجم لاگ:</b> در سناریوی واقعی (تغییر نام یک فیلد در سمت تولیدکننده، یا Reset یک Group روی Backlog با فرمت قدیمی) ممکن است تمام ۵۰۰ پیام یک بچ با هم رد شوند. یک خط لاگ به‌ازای هر پیام، ۵۰۰ هشدار در هر بچ تولید می‌کرد و همه‌چیز دیگر را در لاگ کانتینر دفن می‌کرد. پس فقط ۵ مورد اول با جزئیات لاگ می‌شوند و بقیه در یک خط خلاصه‌ی گروه‌بندی‌شده بر اساس دلیل جمع می‌شوند. جزئیات کامل هر رد شدن در جدول موجود است، که اصلاً هدف داشتن آن جدول است.
</font>
</p>

<hr>

<font size=5>
<b>۱۰. حل کلید خارجی: از package_name به application_id</b>
</font>

<br>
<p align="justify">
<font size=4>
پیام‌های هر سه تاپیک <code>package_name</code> حمل می‌کنند، هرگز <code>application_id</code>. این تصمیم عمدی است: تولیدکنندگان (یک Scraper و یک تحلیل‌گر بسته‌های شبکه) نباید هویت پایگاه‌داده را بشناسند. تبدیل این دو به هم، وظیفه‌ی این زیرسیستم است چون <b>مالک اسکیما خودش است</b>.
</font>
</p>

<br>
<p align="justify">
<font size=4>
این خواندن مستقیماً از PostgreSQL انجام می‌شود، <b>نه</b> از طریق Endpoint مربوط به App API که دو زیرسیستم دیگر استفاده می‌کنند. استدلال: این زیرسیستم از قبل Credential پایگاه‌داده را دارد، این جست‌وجو یک کوئری روی یک Unique Index است، و گذاشتن یک فراخوانی HTTP روی مسیر داغِ نوشتن، فقط یک نقطه‌ی شکست جدید اضافه می‌کرد بدون هیچ فایده‌ای.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>تصمیم</th>
<th>مقدار / رفتار</th>
<th>دلیل</th>
</tr>
<tr>
<td>یک کوئری برای کل بچ</td>
<td><code>WHERE package_name = ANY(%s)</code></td>
<td>برای ۵۰۰ نقد یک اپ، تفاوت بین یک Round-trip و ۵۰۰ Round-trip است</td>
</tr>
<tr>
<td>کش اختصاصی هر Thread</td>
<td>TTL پیش‌فرض ۳۰۰ ثانیه</td>
<td>مشترک کردن یک کش بین سه Thread، نیازمند Lock روی هر پیام بود تا در ساعتی یک کوئری صرفه‌جویی شود</td>
</tr>
<tr>
<td>عدم کش کردن نتایج ناموفق</td>
<td>Miss در کش ذخیره نمی‌شود</td>
<td>Negative Caching باعث می‌شد اپی که اپراتور همین حالا ثبت کرده، تا انقضای TTL دیده نشود</td>
</tr>
<tr>
<td>بدون فیلتر <code>is_active</code></td>
<td>اپ غیرفعال هم <code>application_id</code> می‌گیرد</td>
<td>داده‌ای که از قبل تولید شده باید ذخیره شود؛ Soft Delete یک حکم درباره‌ی «چه چیزی را بعداً خزش کن» است، نه درباره‌ی داده‌ی تاریخی</td>
</tr>
<tr>
<td>اپ ثبت‌نشده = Dead Letter</td>
<td>بدون <code>raise</code>، فقط ردشدن همان پیام</td>
<td>قفل کردن Partition تا زمانی که کسی اپ را ثبت کند، ingestion تمام اپ‌های دیگر را هم متوقف می‌کرد</td>
</tr>
</table>

<hr>

<font size=5>
<b>۱۱. مالکیت اسکیما و مهاجرت‌های SQL</b>
</font>

<br>
<p align="justify">
<font size=4>
یک پایگاه‌داده، اما مالکیت جدول‌ها تفکیک‌شده است. جنگو (App API) مالک جدول <code>applications</code> است و این زیرسیستم مالک چهار جدول خودش. این چهار جدول <b>هیچ Model جنگویی ندارند</b> و با فایل‌های شماره‌گذاری‌شده‌ی SQL ساخته می‌شوند.
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    subgraph DJ["مالکیت App API - جنگو"]
        A["apps_registry_application<br/>(جدول applications)"]
    end

    subgraph SC["مالکیت Storage Consumer - SQL خام"]
        S1["0001_app_stats.sql"]
        S2["0002_reviews.sql"]
        S3["0003_network_metrics.sql"]
        S4["0004_dead_letter_events.sql"]
        MT["storage_consumer_migrations<br/>(جدول رهگیری وضعیت)"]
    end

    A -.->|"Foreign Key"| S1
    A -.->|"Foreign Key"| S2
    A -.->|"Foreign Key"| S3

    PRE["بررسی پیش‌پرواز:<br/>to_regclass روی جدول applications"] --> DEC{"جدول وجود دارد؟"}
    DEC -->|"خیر"| ERR["MigrationError با پیام صریح:<br/>«ابتدا مهاجرت‌های app_api را اجرا کنید»<br/>به‌جای خطای مبهم Foreign Key"]
    DEC -->|"بله"| LOCK["pg_advisory_xact_lock<br/>(دو کانتینر همزمان، یک بار اجرا)"]
    LOCK --> APPLY["اعمال فایل‌های اعمال‌نشده به‌ترتیب شماره"]

    style DJ fill:#f59e0b,color:#fff
    style SC fill:#2563eb,color:#fff
    style ERR fill:#dc2626,color:#fff
    style APPLY fill:#16a34a,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
دو نکته‌ی مهم در این طراحی: اول، <code>pg_advisory_xact_lock</code> تضمین می‌کند اگر چند نسخه از کانتینر همزمان بالا بیایند، مهاجرت‌ها فقط یک بار اعمال شوند. دوم، بررسی پیش‌پرواز با <code>to_regclass</code> باعث می‌شود اپراتور پیام «مهاجرت‌های <code>app_api</code> را اجرا کنید» ببیند، نه یک خطای Foreign Key که علتش قابل حدس نیست.
</font>
</p>

<br>
<p align="justify">
<font size=4>
همچنین یک نکته‌ی فنی که جنگو آن را پنهان می‌کرد: <b>PostgreSQL به‌ازای یک کلید خارجی به‌طور خودکار Index نمی‌سازد</b> (این کار را جنگو در سطح ORM انجام می‌دهد). چون این چهار جدول Model جنگویی ندارند، Indexهای لازم روی کلیدهای خارجی صریحاً در همین فایل‌های SQL ساخته شده‌اند.
</font>
</p>

<hr>

<font size=5>
<b>۱۲. لایه‌ی پایداری: یک بچ، یک دستور INSERT</b>
</font>

<br>
<p align="justify">
<font size=4>
هر جدول یک Repository دارد که فقط در سه چیز با بقیه تفاوت دارد: نام جدول، فهرست ستون‌ها، و متن <code>ON CONFLICT</code>. تمام منطق مشترک — تبدیل کل بچ به یک دستور <code>INSERT</code> چندردیفی و شمارش نتیجه — یک‌جا نوشته شده است.
</font>
</p>

<br>
<p align="justify">
<font size=4>
Repositoryها <b>هیچ اتصالی به پایگاه‌داده نگه نمی‌دارند</b>؛ Cursor از بیرون (توسط Worker) به آن‌ها داده می‌شود. همین نکته است که اجازه می‌دهد نوشتن داده و ثبت پیام‌های مرده در یک تراکنش واحد قرار بگیرند.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>جزئیات پیاده‌سازی</th>
<th>توضیح</th>
</tr>
<tr>
<td><code>execute_values</code> با <code>page_size=1000</code></td>
<td>بزرگ‌تر از بزرگ‌ترین <code>max_poll_records</code> (۵۰۰ برای نقدها)، پس یک بچ در حالت معمول یک Round-trip است</td>
</tr>
<tr>
<td><code>RETURNING (xmax = 0) AS inserted</code></td>
<td>Postgres مقدار <code>xmax</code> را برای ردیف تازه Insert شده صفر و برای ردیفی که <code>DO UPDATE</code> جایگزینش کرده غیرصفر می‌گذارد. همین است که به یک خط لاگ اجازه می‌دهد در یک نگاه بگوید «این Backfill است یا وضعیت پایدار»</td>
</tr>
<tr>
<td>شمارنده‌ی <code>skipped</code></td>
<td>ردیف‌هایی که پایگاه‌داده نپذیرفت: تعارض کلید در <code>DO NOTHING</code>، یا به‌روزرسانی‌ای که نگهبان یکنوا رد کرد. مقدار مداوماً غیرصفر یعنی چیزی در حال Replay است</td>
</tr>
<tr>
<td><code>dedupe_by_key</code> پیش از نوشتن</td>
<td>یک الزام است، نه بهینه‌سازی: Postgres یک <code>ON CONFLICT DO UPDATE</code> که دو بار همان ردیف را در یک دستور لمس کند رد می‌کند و کل تراکنش را Abort می‌کند</td>
</tr>
</table>

<hr>

<font size=5>
<b>۱۳. اعتبارسنجی در لحظه‌ی Decode</b>
</font>

<br>
<p align="justify">
<font size=4>
Decoderها فقط ساختار پیام را بررسی نمی‌کنند؛ آن‌ها <b>محدودیت‌های ستون پایگاه‌داده</b> را هم در همان لحظه اعمال می‌کنند: بازه‌ی <code>SMALLINT</code> و <code>INTEGER</code> و <code>BIGINT</code>، طول <code>VARCHAR</code>، معتبر بودن UUID، و مقادیر <code>NaN</code> و <code>Inf</code>.
</font>
</p>

<br>
<p align="justify">
<font size=4>
دلیل این سخت‌گیری در جای درست، یک نکته‌ی مهم است: چون کل بچ در یک تراکنش نوشته می‌شود، <b>یک مقدار خارج از بازه که به SQL برسد، تراکنش را برای کل بچ Abort می‌کند</b> — یعنی ۴۹۹ پیام سالم هم قربانی یک پیام معیوب می‌شوند. با گرفتن آن در لحظه‌ی Decode، همان یک پیام به DLQ می‌رود و بقیه سالم نوشته می‌شوند.
</font>
</p>

<br>
<p align="justify">
<font size=4>
یک تصمیم مرتبط دیگر: کتابخانه‌ی <code>google-play-scraper</code> فیلد زمانی <code>at</code> را بدون Timezone (Naive) تولید می‌کند، در حالی که تمام ستون‌های زمانی این زیرسیستم <code>TIMESTAMPTZ</code> هستند. سیاست صریح این است که زمان‌های Naive در همین لایه به UTC نسبت داده شوند، تا هیچ ابهامی به پایگاه‌داده نرسد.
</font>
</p>

<hr>

<font size=5>
<b>۱۴. پیکربندی: Fail-Fast و قابل تنظیم</b>
</font>

<br>
<p align="justify">
<font size=4>
تمام تنظیمات در یک کلاس <code>Settings</code> واحد و <code>frozen</code> جمع شده‌اند که در لحظه‌ی شروع Validate می‌شود. مقدار محیطی نامعتبر، بلافاصله برنامه را با پیام واضح متوقف می‌کند، نه ساعت‌ها بعد وسط یک بچ. این کلاس Immutable و Slotted است چون <b>تنها شیئی است که بین سه Thread مشترک است</b> (به‌جز رخداد توقف) و همین Immutable بودن است که مدل Share-Nothing را بدون Lock ایمن می‌کند.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>تنظیم</th>
<th>مقدار پیش‌فرض</th>
<th>دلیل انتخاب</th>
</tr>
<tr>
<td><code>enable_auto_commit</code></td>
<td><code>False</code></td>
<td>Commit خودکار بر پایه‌ی زمان است نه موفقیت نوشتن؛ می‌توانست Offset پیامی را جابه‌جا کند که هنوز ننشسته است</td>
</tr>
<tr>
<td><code>auto_offset_reset</code></td>
<td><code>earliest</code></td>
<td>یک Group تازه باید کل Backlog موجود را بخواند، نه اینکه داده‌ی منتشرشده را نادیده بگیرد</td>
</tr>
<tr>
<td><code>poll_timeout_ms</code></td>
<td><code>1000</code></td>
<td>به‌قدر کافی کوتاه که SIGTERM حداکثر یک ثانیه معطل بماند</td>
</tr>
<tr>
<td><code>max_poll_interval_ms</code></td>
<td><code>300000</code> (۵ دقیقه)</td>
<td>سقف زمان پردازش یک بچ پیش از آنکه Broker عضو را مرده فرض کند؛ با احتساب بدترین حالت Retry پایگاه‌داده انتخاب شده</td>
</tr>
<tr>
<td><code>session_timeout_ms</code></td>
<td><code>45000</code></td>
<td>تحمل نوسان شبکه بدون Rebalance بی‌مورد</td>
</tr>
<tr>
<td><code>db_retry_max_attempts</code></td>
<td><code>5</code> با Backoff پایه‌ی ۱ ثانیه</td>
<td>پوشش یک Restart کامل PostgreSQL (۱+۲+۴+۸+۱۶ ثانیه) بدون از دست دادن بچ</td>
</tr>
<tr>
<td><code>application_cache_ttl_seconds</code></td>
<td><code>300</code></td>
<td>تعادل بین کم کردن کوئری و دیده شدن سریع اپ تازه ثبت‌شده</td>
</tr>
<tr>
<td><code>shutdown_timeout_seconds</code></td>
<td><code>30</code></td>
<td>فرصت کافی برای اتمام و Commit بچ در جریان؛ در Compose با <code>stop_grace_period: 45s</code> هم‌راستا شده</td>
</tr>
</table>

<br>
<p align="justify">
<font size=4>
اندازه‌ی بچ هر Pipeline سه سطح اولویت دارد تا اپراتور بتواند بدون تغییر کد آن را تنظیم کند: متغیر مخصوص همان Pipeline (مثل <code>STORAGE_MAX_POLL_RECORDS_REVIEWS</code>)، سپس متغیر عمومی <code>STORAGE_MAX_POLL_RECORDS</code>، و در نهایت مقدار پیش‌فرض تنظیم‌شده در کد.
</font>
</p>

<hr>

<font size=5>
<b>۱۵. Supervisor، سلامت و خاموشی تمیز</b>
</font>

<br>
<p align="justify">
<font size=4>
Supervisor یک قاعده را اعمال می‌کند: <b>فرآیند تا زمانی زنده است که تمام Pipelineهایش زنده باشند.</b> کانتینری که با دو Pipeline از سه Pipeline در حال کار باشد، Uptime سالمی نشان می‌دهد در حالی که داده بی‌صدا قطع شده است — و این از Crash کردن بدتر است، چون دیده نمی‌شود.
</font>
</p>

</div>
</font>

```mermaid
stateDiagram-v2
    [*] --> Migrating: اجرای مهاجرت‌ها
    Migrating --> Running: اسکیما به‌روز است
    Migrating --> Exit1: خطای MigrationError

    Running --> Running: حلقه‌ی poll / write / commit
    Running --> Stopping: دریافت SIGTERM یا SIGINT
    Running --> Failing: شکست یکی از سه Pipeline

    Stopping --> Joined: اتمام و Commit بچ در جریان، سپس LeaveGroup
    Failing --> Joined: توقف Pipelineهای دیگر هم درخواست می‌شود

    Joined --> Exit0: هیچ Pipelineای خطا نداشت
    Joined --> Exit1: حداقل یک Pipeline خطا داشت

    Exit0 --> [*]: کد خروج صفر
    Exit1 --> [*]: کد خروج یک، سپس Restart کانتینر
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
<b>سلامت به‌تفکیک Pipeline (بدون سرور HTTP):</b> هر Worker در هر دور حلقه، فایل مخصوص خودش را <code>touch</code> می‌کند و اسکریپت <code>healthcheck.sh</code> بررسی می‌کند که تمام فایل‌های مورد انتظار تازه باشند. نتیجه این است که کانتینری که Thread نقدهایش گیر کرده، فقط به‌خاطر «در حال اجرا بودن» سالم گزارش نمی‌شود. عمل <code>touch</code> نرخ‌محدود است تا یک حلقه‌ی پرمشغله به یک System Call در هر Poll تبدیل نشود، و هر خطای نوشتن فایل تنها یک بار هشدار می‌دهد و نادیده گرفته می‌شود — پر بودن <code>/tmp</code> یک مشکل مانیتورینگ است و نباید به قطعی ingestion تبدیل شود.
</font>
</p>

<br>
<p align="justify">
<font size=4>
<b>خاموشی تمیز:</b> Entrypoint کانتینر با <code>exec</code> اجرا می‌شود تا فرآیند پایتون PID 1 شود و SIGTERM را مستقیماً دریافت کند (نه اینکه یک Shell آن را ببلعد). سیگنال در مرز Poll دیده می‌شود، بچ در جریان کامل و Commit می‌شود، و سپس Consumer با <code>LeaveGroup</code> از Group خارج می‌شود تا Broker بلافاصله Rebalance کند به‌جای انتظار برای انقضای کامل Session — در یک Rolling Restart این تفاوت میان چند ثانیه و نزدیک یک دقیقه ingestion متوقف است.
</font>
</p>

<hr>

<font size=5>
<b>۱۶. استراتژی تست</b>
</font>

<br>
<p align="justify">
<font size=4>
مجموعه‌ی تست‌ها به دو بخش تقسیم می‌شود، و این تقسیم مستقیماً از یک تصمیم معماری می‌آید: هسته‌ی زیرسیستم (<code>config</code>، <code>events</code>، <code>decoders</code>، <code>batching</code>، <code>retry_policy</code>) عمداً <b>نه Kafka می‌شناسد و نه psycopg2</b>. همین است که اجازه می‌دهد قرارداد داده‌ی هر سه تاپیک بدون بالا آوردن هیچ سرویسی آزمون شود.
</font>
</p>

</div>
</font>

```mermaid
flowchart TD
    ALL["۲۷۸ تست"] --> U["۲۲۷ تست واحد<br/>بدون Broker، بدون پایگاه‌داده<br/>اجرا در حدود ۱.۵ ثانیه"]
    ALL --> I["۵۱ تست یکپارچگی<br/>(نشان‌دار با marker: integration)<br/>روی Kafka و PostgreSQL واقعی"]

    U --> U1["قرارداد Decode و بازه‌ی ستون‌ها"]
    U --> U2["اعتبارسنجی Settings"]
    U --> U3["ترتیب write-before-commit<br/>با Consumer و Repository جعلی"]
    U --> U4["مجموعه‌ی خطاهای قابل Retry"]

    I --> I1["Idempotency واقعی:<br/>Reset کردن Offset و Replay کامل تاپیک"]
    I --> I2["جداسازی همزمانی:<br/>تأخیر network-metrics هنگام ingest انبوه نقدها"]
    I --> I3["اجرای واقعی مهاجرت‌ها روی پایگاه‌داده‌ی موقت"]
    I --> I4["استقلال Offset هر Pipeline"]

    style U fill:#16a34a,color:#fff
    style I fill:#f59e0b,color:#fff
    style ALL fill:#2563eb,color:#fff
```

<font face="B Mitra" size=5>
<div dir=rtl>

<br>
<p align="justify">
<font size=4>
تست‌های یکپارچگی همه‌ی تاپیک‌ها و اپ‌های مورد نیاز خودشان را می‌سازند و پس از پایان پاک می‌کنند، پس اجرای آن‌ها روی یک Stack فعال بی‌خطر است. اگر Kafka یا PostgreSQL در دسترس نباشند، این تست‌ها به‌جای شکست، Skip می‌شوند تا مجموعه روی ماشینی بدون کانتینر هم سبز بماند.
</font>
</p>

<hr>

<font size=5>
<b>۱۷. عملیات: تأیید، Replay و مقیاس‌دهی</b>
</font>

<br>
<p align="justify">
<font size=4>
سه عملیات روزمره‌ی این زیرسیستم به‌صورت صریح مستند و ابزارمند شده‌اند.
</font>
</p>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>عملیات</th>
<th>روش</th>
<th>نکته‌ی مهم</th>
</tr>
<tr>
<td>تأیید سلامت استقرار</td>
<td>اسکریپت <code>scripts/verify/verify_storage_consumer.sh</code></td>
<td>کل مسیر را به‌ترتیب بررسی می‌کند: نسخه‌ی اسکیما، تعداد Partition تاپیک‌ها، پیام‌های تولیدشده، ردیف‌های نوشته‌شده، پیام‌های مرده، و Lag هر Group — پس شکست، مرحله‌ی خراب را نشان می‌دهد نه کل Pipeline را</td>
</tr>
<tr>
<td>Replay یک تاپیک</td>
<td>Reset کردن Offset همان Consumer Group به <code>earliest</code></td>
<td>چون هر تاپیک Group مستقل دارد، می‌توان <code>reviews</code> را برای بازسازی کامل جدولش Replay کرد <b>بدون</b> اینکه <code>app-stats</code> هم دوباره پردازش شود. Idempotency باعث می‌شود این کار داده را تکراری نکند</td>
</tr>
<tr>
<td>مقیاس‌دهی یک Pipeline</td>
<td>اجرای نسخه‌ی دوم کانتینر با <code>STORAGE_PIPELINES</code> مخصوص همان Pipeline</td>
<td>چون هر تاپیک ۳ Partition دارد، تا ۳ عضو فعال در هر Group پشتیبانی می‌شود. همین متغیر اجازه می‌دهد همان Image به‌صورت سه کانتینر تک‌Pipeline با Restart Policy مستقل هم اجرا شود، بدون تغییر کد</td>
</tr>
</table>

<br>
<p align="justify">
<font size=4>
<b>یک نکته‌ی مهم برای اولین استقرار:</b> تمام پیام‌هایی که همین حالا روی تاپیک‌های <code>app-stats</code> و <code>reviews</code> نشسته‌اند، از قبل از تثبیت قرارداد فعلی فیلدها هستند (ساختار خام camelCase کتابخانه). این پیام‌ها طبق طراحی به <code>dead_letter_events</code> می‌روند. این یک باگ نیست؛ در اولین استقرار باید این Backlog قدیمی را رد کرد (Reset کردن Offset به <code>latest</code>) یا تاپیک‌ها را پاک کرد.
</font>
</p>

<hr>

<font size=5>
<b>۱۸. تصمیمات کلیدی دیگر</b>
</font>

<br>
<p align="justify">
<font size=4>
<ul>

<li><b>یک Consumer Group به‌ازای هر تاپیک، نه یک Group برای هر سه:</b> Groupهای جدا همان چیزی هستند که اجازه می‌دهند اپراتور <code>reviews</code> را برای بازسازی جدولش Reset کند بدون Replay شدن <code>app-stats</code>، و همان چیزی که Lag هر تاپیک را در kafka-ui جداگانه قابل مشاهده می‌کند.</li>
<br>

<li><b>ساخت تاپیک با سه Partition از ابتدا:</b> تاپیک ساخته‌شده به‌صورت خودکار فقط یک Partition دارد، و این سقف هر Consumer Group را برای همیشه روی یک عضو فعال قفل می‌کند. تعداد Partition یک تاپیک قابل کاهش نیست، پس فهمیدن این موضوع زیر بار، یعنی بازسازی تاپیک؛ ساختن آن‌ها از ابتدا تنها لحظه‌ی ارزان برای درست انجام دادنش است.</li>
<br>

<li><b>Repositoryها بدون اتصال و بدون State:</b> Cursor از بیرون تزریق می‌شود تا نوشتن به تراکنش بچ ملحق شود، نه اینکه تراکنش خودش را باز کند. همین یک تصمیم است که تراکنش واحد داده و DLQ را ممکن کرده است.</li>
<br>

<li><b>تفکیک <code>.env</code> هاست از مقادیر Docker:</b> فایل <code>storage_consumer/.env</code> فقط برای اجرای مستقل روی هاست است (<code>localhost:9092</code> و <code>127.0.0.1</code>)، در حالی که سرویس در Compose به <code>kafka:29092</code> و <code>postgres</code> نیاز دارد. این زیرسیستم روشن‌ترین نمونه‌ی این تفکیک در کل پروژه است، چون تنها زیرسیستمی است که <b>هم</b> به Kafka و <b>هم</b> به PostgreSQL وصل می‌شود و در نتیجه هر دو دسته آدرس را دارد.</li>
<br>

<li><b>تکرار عامدانه‌ی <code>retry_policy</code>:</b> این فایل نسخه‌ی مستقلی از همتای خود در <code>crawler/</code> و <code>network_analyzer/</code> است، چون هر زیرسیستم یک واحد استقرار مستقل با Image و <code>requirements.txt</code> خودش است و سورس بقیه در Image آن حاضر نیست. آنچه <b>واقعاً متفاوت</b> است، مجموعه‌ی خطاهای قابل Retry است: دو زیرسیستم دیگر HTTP را Retry می‌کنند، این یکی پایگاه‌داده را.</li>

</ul>
</font>
</p>

<hr>

<font size=5>
<b>۱۹. ابزارها و تکنولوژی‌های استفاده‌شده</b>
</font>

<br>

<table dir="rtl" align="right" style="width:100%; text-align:right; border-collapse:collapse;" border="1">
<tr>
<th>ابزار</th>
<th>نقش</th>
</tr>
<tr>
<td>kafka-python (KafkaConsumer)</td>
<td>مصرف پیام از سه تاپیک با Commit دستی Offset و مدیریت Consumer Group</td>
</tr>
<tr>
<td>psycopg2</td>
<td>اتصال به PostgreSQL و اجرای <code>INSERT</code> چندردیفی با <code>execute_values</code></td>
</tr>
<tr>
<td>threading (stdlib)</td>
<td>یک Thread به‌ازای هر Pipeline، به‌همراه رخداد توقف مشترک برای خاموشی هماهنگ</td>
</tr>
<tr>
<td>SQL خام + جدول رهگیری مهاجرت</td>
<td>مالکیت اسکیمای چهار جدول این زیرسیستم، مستقل از مهاجرت‌های جنگو</td>
</tr>
<tr>
<td>dataclasses (frozen, slots)</td>
<td>تنظیمات و رخدادهای Immutable که بین Threadها بی‌خطر مشترک می‌شوند</td>
</tr>
<tr>
<td>python-dotenv</td>
<td>بارگذاری پیکربندی محیطی در حالت اجرای مستقل (venv)</td>
</tr>
<tr>
<td>pytest (با marker جداگانه‌ی integration)</td>
<td>تفکیک تست‌های بدون سرویس از تست‌هایی که به Kafka و PostgreSQL واقعی نیاز دارند</td>
</tr>
<tr>
<td>Docker Healthcheck + فایل‌های Heartbeat</td>
<td>سنجش سلامت به‌تفکیک هر Pipeline، بدون نیاز به بالا آوردن یک سرور HTTP</td>
</tr>
</table>

</div>
</font>
<hr>
