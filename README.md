# xlogd

`xlogd` is designed for specified purposes, it replaces `logstash`

send a JSON string into redis with `RPUSH`

```json
{
  "message": "[2018/09/10 17:24:22.120] CRID[945bea8e42de2796] this is a message",
  "source": "/var/log/prod/err/api-customer.log",
  "beat": {
    "hostname": "example.web.01"
  }
}
```

`xlogd` will fetch and convert it into a elasticsearch document

```json
{
  "timestamp": "2018-09-10T17:24:22.120Z",
  "env": "prod",
  "topic": "err",
  "project": "api-customer",
  "hostname": "example.web.01",
  "crid": "945bea8e42de2796",
  "message": "CRID[945bea8e42de2796] this is a message"
}
```