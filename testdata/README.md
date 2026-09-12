# Postman / 手工上传测试样例

## 目录

| 目录 | 数量 | 期望 |
|------|------|------|
| [`valid/`](valid/) | 20 | `POST /v1/recordings` → `201`（同文件再传 → 幂等 `200`） |
| [`invalid/`](invalid/) | 10 | → `400`，`error.code = invalid_upload` |
| [`short.wav`](short.wav) | 1 | 给根目录 [`api.http`](../api.http) 用 |

合法文件含：`valid_01..05.wav`（真实短 WAV）、`06..10.mp3`、`11..15.m4a`、`16..20.aac`（扩展名合法 + 非空；接口不校验音频魔数）。

非法文件含：空 `.wav`/`.mp3`、错误扩展名（`.txt` / `.flac` / `.ogg` / `.mp4` / `.doc` / `.md` / `.zip`）、无扩展名 `no_ext`。

## Postman

1. 启动 API（如 `docker compose -f docker/docker-compose.yaml up`）
2. `POST http://127.0.0.1:8000/v1/recordings`
3. Body → form-data → key=`file`（类型 File）→ 选 `testdata/valid/*` 或 `testdata/invalid/*`

## 重新生成

在仓库根目录：

```bash
python scripts/generate_testdata.py
```

测「超过 50MB」时（会占约 50MB 磁盘，已 gitignore）：

```bash
python scripts/generate_testdata.py --oversize
```
