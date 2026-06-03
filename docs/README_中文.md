# rt-llm-proxy 中文文档

欢迎使用 rt-llm-proxy！这是完整的中文文档库，帮助你快速开始和深入理解这个项目。

## 📚 文档导航

### 🚀 新手入门

如果你是新用户，按这个顺序阅读：

1. **[中文指南](中文指南.md)** — 5 分钟了解项目是什么、支持哪些功能
2. **[中文部署指南 — 本地开发](中文部署指南.md#本地开发环境)** — 30 分钟搭建本地环境
3. **[中文常见问题 — 快速开始](中文常见问题.md#部署和安装)** — 遇到问题时查看

### 💻 部署和运维

准备上线或优化性能？

1. **[中文部署指南](中文部署指南.md)** — 完整的部署步骤
   - 本地开发环境
   - Docker Compose 部署
   - 级联（自托管）部署
   - 中国用户加速指南
   - 故障排查

2. **[中文常见问题 — 故障排查](中文常见问题.md#故障排查)** — 常见问题和解决方案

3. **[中文常见问题 — 部署和运维](中文常见问题.md#部署和运维)** — 生产环境建议

### 🏗️ 架构和设计

深入理解系统设计：

1. **[中文指南 — 架构概览](中文指南.md#架构概览)** — 核心模块和数据流
2. **[ARCHITECTURE.md](ARCHITECTURE.md)** — 详细的工程决策和优化
3. **[中文指南 — 级联管道](中文指南.md#级联管道cascade)** — 自托管 ASR/LLM/TTS

### ⚡ 性能优化

如何获得最佳性能：

1. **[bench/README.md](bench/README.md)** — Opus 基准和容量数据
2. **[中文常见问题 — 性能和容量](中文常见问题.md#性能和容量)** — 性能优化技巧
3. **[中文部署指南 — 性能基准](中文部署指南.md#性能基准)** — 实测数据

### ❓ 常见问题

### 快速问答

- **Q: 这是什么？** → [中文常见问题 — 功能相关](中文常见问题.md#功能相关)
- **Q: 怎么开始？** → [中文部署指南 — 本地运行](中文部署指南.md#3-本地运行)
- **Q: 遇到问题？** → [中文常见问题 — 故障排查](中文常见问题.md#故障排查)
- **Q: 性能如何优化？** → [中文常见问题 — 性能和容量](中文常见问题.md#性能和容量)

---

## 🎯 按使用场景查找文档

### 场景 1：我想快速体验一下

**耗时**: ~10 分钟

1. 安装 Go 1.25+ 和 libopus
   ```bash
   sudo apt-get install -y libopus-dev libopusfile-dev pkg-config
   ```

2. 获取 Gemini API 密钥（[点击获取](https://aistudio.google.com/app/apikeys)）

3. 运行代理
   ```bash
   export GEMINI_API_KEY=your_key
   go run ./cmd/proxy -addr :8080
   ```

4. 打开浏览器访问 http://localhost:8080/demo/

**相关文档**: [中文部署指南 — 本地运行](中文部署指南.md#3-本地运行)

### 场景 2：我想在 Docker 中运行

**耗时**: ~5 分钟

```bash
cp .env.example .env
# 编辑 .env，设置 GEMINI_API_KEY
docker compose up --build
```

打开 http://localhost:8080/demo/

**相关文档**: [中文部署指南 — Docker Compose 基础堆栈](中文部署指南.md#基础堆栈仅代理)

### 场景 3：我想运行自托管级联（完整 AI 套件）

**耗时**: ~30 分钟（包括模型下载）

前置条件：
- NVIDIA GPU（推荐 L20 或更高）
- 公网 IP

步骤：
```bash
export PUBLIC_IP=your.public.ip
export QWEN_MODEL_PATH=/path/to/Qwen3.5-9B  # 可选
docker compose -f docker-compose.yml \
               -f docker-compose.cascade.yml up --build
```

访问 http://<PUBLIC_IP>:8080/demo/?model=cascade

**相关文档**: 
- [中文指南 — 级联管道](中文指南.md#级联管道cascade)
- [中文部署指南 — 级联部署](中文部署指南.md#级联部署自托管-asrllmtts)

### 场景 4：我想添加速率限制和转录存储

**耗时**: ~10 分钟

```bash
docker compose -f docker-compose.yml \
               -f docker-compose.redis-kafka.yml up --build
```

现在有：
- ✅ Redis 速率限制
- ✅ Kafka 转录侧通道
- ✅ 分布式会话重连

**相关文档**: [中文部署指南 — Docker Compose 叠加层](中文部署指南.md#docker-compose-叠加层)

### 场景 5：我在调试问题或优化性能

**首先**：查看日志和指标
```bash
# 查看实时统计
curl http://localhost:6060/stats | jq

# 查看日志
docker compose logs -f rt-llm-proxy

# 分析性能
curl http://localhost:6060/debug/pprof/profile?seconds=30 | go tool pprof -
```

**然后**：查找对应的文档

| 问题 | 文档 |
|---|---|
| WebRTC 连接失败 | [故障排查 — WebRTC](中文常见问题.md#问题webrtc-连接失败) |
| 高延迟或卡顿 | [故障排查 — 延迟](中文常见问题.md#问题高延迟或卡顿) |
| Kafka 事件未发送 | [故障排查 — Kafka](中文常见问题.md#问题kafka-事件未发送) |
| 级联无法连接侧车 | [故障排查 — 级联](中文常见问题.md#问题级联自托管无法连接到侧车) |
| 内存泄漏 | [故障排查 — 内存](中文常见问题.md#问题内存泄漏或随时间-cpu-增加) |

**相关文档**: [中文常见问题 — 故障排查](中文常见问题.md#故障排查)

### 场景 6：我想在中国使用

关键点：
1. **Go 代理加速**（必须）— [中文部署指南 — 中国用户](中文部署指南.md#go-依赖加速)
2. **Docker 加速**（推荐） — [中文部署指南 — Docker 中国镜像](中文部署指南.md#docker-中国镜像)
3. **使用豆包** 而非 Gemini（本地化 API，无需 VPN）
   ```bash
   export DOUBAO_APP_ID=...
   export DOUBAO_ACCESS_TOKEN=...
   go run ./cmd/proxy
   # 访问 http://localhost:8080/demo/?model=doubao
   ```

**相关文档**: [中文部署指南 — 中国用户特殊说明](中文部署指南.md#中国用户特殊说明)

### 场景 7：我想在生产环境上线

**关键步骤**：

1. 部署到服务器，前置反向代理（Nginx）
2. 启用 Redis 速率限制
3. 启用 Kafka 侧通道（转录持久化）
4. 部署 TURN/SFU（LiveKit / Pipecat）用于 NAT 穿透和水平扩展
5. 设置监控和告警

**相关文档**: [中文常见问题 — 部署和运维](中文常见问题.md#部署和运维)

---

## 📖 完整文档列表

### 中文文档

| 文档 | 大小 | 重点 |
|---|---|---|
| **[中文指南](中文指南.md)** | 15 分钟 | 项目概述、功能、配置、架构 |
| **[中文部署指南](中文部署指南.md)** | 30 分钟 | 本地、Docker、级联部署、故障排查 |
| **[中文常见问题](中文常见问题.md)** | 20 分钟 | FAQ、性能、优化、安全 |
| **[README_中文.md](README_中文.md)** | 当前文件 | 文档导航和使用场景 |

### 英文文档（深度）

| 文档 | 重点 |
|---|---|
| **[README.md](../README.md)** | 功能概览、快速开始、Docker Compose |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | 系统设计、工程优化、模块结构 |
| **[CONTEXT.md](../CONTEXT.md)** | 域语汇、关键术语、模块接缝 |
| **[bench/README.md](bench/README.md)** | 性能基准、容量数据、优化结果 |

---

## 🔑 关键概念速查

### 提供商（Provider）

| 提供商 | 类型 | 特点 | 成本 |
|---|---|---|---|
| **Gemini** | 云托管 | 无需部署，质量好 | 按 API 调用计费 |
| **豆包** | 云托管 | 本地化，无需 VPN | 按 API 调用计费 |
| **Cascade** | 自托管 | 完全可控，支持注入 | 需要 GPU 硬件成本 |
| **Loopback** | 模拟 | 仅用于测试 | 免费 |

**选择建议**：
- 🚀 快速开始 → Gemini
- 🇨🇳 中国用户 → 豆包
- 🎛️ 完全控制 → Cascade
- 🧪 开发测试 → Loopback

### 核心概念

| 概念 | 说明 |
|---|---|
| **Bridge** | WebRTC 网桥，连接浏览器和 LLM |
| **Model** | 提供商适配器接口（Gemini / 豆包 / Cascade） |
| **Cascade** | 自托管 ASR→LLM→TTS 管道 |
| **转录** | 语音识别结果，保存到 Kafka |
| **会话** | 一次对话的完整上下文，支持重连 |
| **速率限制** | 使用 Redis 限制每 IP 的并发会话数 |
| **自适应复杂度** | 高负载时自动降低 Opus 编码质量 |

### 常用命令速查

```bash
# 基础
go run ./cmd/proxy -addr :8080

# Gemini
export GEMINI_API_KEY=... && go run ./cmd/proxy

# 豆包
export DOUBAO_APP_ID=... && go run ./cmd/proxy ?model=doubao

# 级联（自托管）
docker compose -f docker-compose.yml -f docker-compose.cascade.yml up --build

# Redis + Kafka
docker compose -f docker-compose.yml -f docker-compose.redis-kafka.yml up --build

# 自适应复杂度（性能优化）
go run ./cmd/proxy -adaptive sessions

# 管理端点
curl http://localhost:6060/stats | jq
```

---

## 🆘 遇到问题？

### 快速诊断

1. **代理在运行吗？**
   ```bash
   curl http://localhost:8080/stats
   ```
   如果没有响应，检查代理进程。

2. **API 密钥有效吗？**
   ```bash
   echo $GEMINI_API_KEY  # 应该不为空
   ```

3. **网络能联通吗？**
   ```bash
   # WebRTC UDP 端口开放？
   nc -u -l 10000  # 一个终端
   nc -u 127.0.0.1 10000  # 另一个终端
   ```

### 查找答案

1. **快速答案** → [中文常见问题](中文常见问题.md)
2. **部署问题** → [中文部署指南 — 故障排查](中文部署指南.md#故障排查)
3. **性能优化** → [中文常见问题 — 性能和容量](中文常见问题.md#性能和容量)
4. **深度理解** → [ARCHITECTURE.md](ARCHITECTURE.md)

---

## 📞 获取帮助

| 渠道 | 用途 |
|---|---|
| **GitHub Issues** | 报告 Bug、功能请求 |
| **GitHub Discussions** | 提问、讨论、分享经验 |
| **文档** | 自助学习、故障排查 |

---

## 📝 贡献指南

- 发现文档错误或缺失？提交 PR！
- 有更好的表达方式？我们欢迎改进！
- 翻译到其他语言？非常棒！

---

## 📌 重要提示

### 单主机限制

rt-llm-proxy 设计为**单主机纵向扩展**，不是 Kubernetes 横向扩展。

- ✅ **能做**：单机 600–1000 个并发会话（取决于硬件）
- ❌ **不能做**：Kubernetes `replicas: N`（WebRTC 媒体亲和性问题）

**生产建议**：在成熟媒体层（TURN + SFU，如 LiveKit / Pipecat）前面运行。

### 故障转移能力

| 级别 | 支持 | 说明 |
|---|---|---|
| L1 | ✅ | 服务器重启，客户端重连 |
| L2 | ✅ | 重连恢复会话元数据 |
| L3 | ⚠️ | 部分恢复对话进度（需要 Kafka） |
| L4 | ❌ | 无缝连接迁移（不支持） |

---

## 版本和兼容性

- **Go**: 1.25+
- **Docker**: 24.0+
- **libopus**: 任何现代版本
- **操作系统**: Linux、macOS、Windows（WSL2）

---

## 许可证

检查项目根目录的 `LICENSE` 文件。

---

## 文档最后更新

这份中文文档于 2026 年编写。由于项目仍在积极开发中，部分内容可能已过期。如有不一致，以英文文档为准。

---

**准备好了？** 👉 [从中文指南开始](中文指南.md)

