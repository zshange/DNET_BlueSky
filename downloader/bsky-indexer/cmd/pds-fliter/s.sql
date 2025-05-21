-- 在数据库中创建表
-- sudo docker exec -i 2f379c996555 psql -U postgres -d bluesky < s.sql 
-- 如果表存在则删除
DROP TABLE IF EXISTS filtered_repos CASCADE;

CREATE TABLE filtered_repos (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    did VARCHAR(255) NOT NULL REFERENCES repos(did),
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pds BIGINT NOT NULL REFERENCES pds(id),
    last_indexed_rev text,
    first_rev_since_reset text,
    last_firehose_rev text,
    first_cursor_since_reset bigint,
    tombstoned_at timestamp with time zone,
    last_index_attempt timestamp with time zone,
    last_error text,
    failed_attempts bigint DEFAULT 0,
    last_known_key text
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_filtered_repos_updated_at
    BEFORE UPDATE ON filtered_repos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
