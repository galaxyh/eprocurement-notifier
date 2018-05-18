# eprocurement-notifier
Notify subscribers with matching procurement declarations keywords for Taiwan government e-procurement website.

# Interpreter
Python 3

# Dependency
requests, lxml, beautifulsoup4, mysql-connector-python-rf

# Usage
```
python query_declaration.py \
-s 20180515 \
-e 20180515 \
-f 20180515 \
-u eprocurement \
-p eprocurement \
-i localhost \
-b tw_procurement
```

```
python notify.py \
-s 20180515 \
-n notify_config.json \
-f 20180515 \
-u eprocurement \
-p eprocurement \
-i localhost \
-b tw_procurement \
-j sender@example.com \
-k password \
-l smtp.example.com
```
