# eprocurement-notifier
Notifier for Taiwan Eprocurement

# Usage
```
python query_declaration.py \
-s 20180515 \
-e 20180515 \
-f 20180515 \
-u eprocurement \
-p eprocurement \
-i localhost \
-b tw_eprocurement
```

```
python notify.py \
-s 20180515 \
-n notify_config.json \
-f 20180515 \
-u eprocurement \
-p eprocurement \
-i localhost \
-b tw_eprocurement \
-j sender@example.com \
-k password \
-l smtp.example.com
```
