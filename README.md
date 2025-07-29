# 🌀 SkyTrace Bluesky用户历史记录爬取

本项目是用于批量抓取 [Bluesky](https://bsky.app/) 用户历史数据的工具集，包括 DID 映射、镜像构建与媒体资源下载。项目基于 **Go 语言** 实现，具备良好的并发性能和可维护性。

## 🧭 模块概览

| 模块名             | 功能说明                                       |
|--------------------|------------------------------------------------|
| `plc-mirror`       | 构建并运行 PLC 镜像服务，用于全量同步 DID 数据 |
| `did-export`       | 提取用户名对应的 DID（去中心化身份标识）       |
| `blob-downloader`  | 下载用户历史记录中的媒体数据（blob）           |

---

## ⚙️ 环境依赖

- Go 1.20 或更高版本  
- Git（用于克隆本项目）
- 网络访问 Bluesky 所需权限

安装依赖：

```bash
git clone https://github.com/your-org/bluesky-crawler.git
cd bluesky-crawler
go mod tidy

📦 模块使用说明

1️⃣ plc-mirror - PLC 镜像服务

用于本地构建 PLC 目录镜像站点，服务 DID 查询请求。

2️⃣ did-export - 导出用户 DID

从用户名句柄（如 alice.bsky.social）获取其对应的 DID。

3️⃣ blob-downloader - 下载用户媒体资源

根据用户 DID 批量下载其历史发帖中的媒体资源，如图片、视频等。

📬 联系方式

如有建议或问题，欢迎提交 Issue 或联系项目维护者(shange0403@163.com)。
