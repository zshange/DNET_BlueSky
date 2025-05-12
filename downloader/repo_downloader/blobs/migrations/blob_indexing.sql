-- Add blob indexing related columns to repos table
ALTER TABLE repos 
  ADD COLUMN IF NOT EXISTS last_blob_index_attempt TIMESTAMP,
  ADD COLUMN IF NOT EXISTS last_blob_error TEXT;

-- Create media_blobs table if it doesn't exist
CREATE TABLE IF NOT EXISTS media_blobs (
  did TEXT NOT NULL  REFERENCES repos(did),
  blob_cid TEXT NOT NULL PRIMARY KEY,
  mime_type TEXT,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT blobs_cid_unique UNIQUE (blob_cid),
  size BIGINT,
  type TEXT DEFAULT 'default'
);

-- Add indexes
CREATE INDEX IF NOT EXISTS idx_media_blobs_cid ON media_blobs(blob_cid);

-- Index to help find repos that need blob indexing
CREATE INDEX IF NOT EXISTS idx_repos_blob_indexing 
  ON repos(last_indexed_rev, last_blob_index_attempt) 
  WHERE last_indexed_rev != ''; 