# 抓取bluesky中的blob数据
## 构建流程
1. 基于 indexer项目，在docker容器下启动数据库，并且自动抓取bluesky中的did，存入bluesky/repo(表)
2. 初始化DNET_BlueSky 项目作为go的初始目录。 在blobs文件夹下启动爬取blob数据
2.1 构建blob数据表sudo docker exec -i f6c1b75c6927 psql -U postgres -d bluesky < migrations/blob_indexing.sql 
2.2 

将blob_cid存入数据库，将





# Bluesky Blob Indexer

This tool indexes and downloads blob data (images, attachments, etc.) from Bluesky repositories and stores them both in a PostgreSQL database and on disk.

## Features

- Downloads all blob data from Bluesky user repositories
- Stores blob metadata in PostgreSQL database
- Organizes blob files on disk with sharded directory structure
- Parallel processing for efficient downloading
- Rate limiting to avoid overwhelming Bluesky servers
- Prometheus metrics for monitoring
- Picks up where it left off if interrupted

## Usage

```bash
# Run with default configuration
./blob-indexer

# Run with custom worker count
./blob-indexer -workers 5

# Run with debug logging
./blob-indexer -log-level 0
```

## Configuration

Configuration is handled through environment variables or command line flags:

| Environment Variable | Flag | Description | Default |
|----------------------|------|-------------|---------|
| INDEXER_POSTGRES_URL | - | PostgreSQL database URL | - |
| INDEXER_METRICS_PORT | - | Port for Prometheus metrics | - |
| INDEXER_CONTACT_INFO | - | Contact info for user agent | - |
| - | -workers | Number of parallel download workers | 3 |
| - | -log-level | Log level (-1=trace, 0=debug, 1=info) | 1 |
| - | -log-format | Log format (text/json) | text |
| - | -log | Path to log file (stdout if empty) | - |

## Database Schema

The tool uses two main tables:

1. `repos` - Extended with:
   - `last_blob_index_attempt` - When blobs were last indexed
   - `last_blob_error` - Error message if blob indexing failed

2. `media_blobs` - Stores metadata about downloaded blobs:
   - `id` - Primary key
   - `repo` - Foreign key to repos table
   - `cid` - Content ID (unique identifier from Bluesky)
   - `path` - Path to the blob file on disk
   - `size` - Size of the blob in bytes
   - `mime_type` - MIME type of the blob
   - `created_at` - When the record was created
   - `updated_at` - When the record was last updated

## File Storage

Blobs are stored in the `/mydata/blob` directory with a 2-level sharded structure:
- First 2 characters of the CID form the first directory level
- Characters 3-4 of the CID form the second directory level
- The full CID is used as the filename

Example: A blob with CID `bafkreihwsnozzjmpr4bkdpbvn4hmekgn57vd6pmzjlt4gepsedfsr75d5m` would be stored at:
`/mydata/blob/ba/fk/bafkreihwsnozzjmpr4bkdpbvn4hmekgn57vd6pmzjlt4gepsedfsr75d5m`

## Prerequisites

- Go 1.19 or higher
- PostgreSQL database
- Access to Bluesky network
- Sufficient disk space for blob storage

## Build

```bash
go build -o blob-indexer
```

## Related Tools

- **record-indexer** - Indexes Bluesky repository data
- **plc-mirror** - Creates a local mirror of the PLC directory

## Architecture

The blob indexer follows these steps:
1. Query database for repos that need blob indexing
2. For each repo, get the list of blobs from Bluesky API
3. Queue each blob for downloading
4. Worker pool processes the queue in parallel
5. Downloaded blobs are stored on disk and indexed in database
6. Process repeats periodically to catch new content 