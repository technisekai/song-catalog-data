create table if not exists silver.songs_catalog (
	id UInt64,
    code String,
    original_artist String,
    song_title String,
    isrc String,
    artist_name String,
    recordings_title String,
    video_id String,
    channel_id String,
    video_title String,
    source_data LowCardinality(String),
    _created_at DateTime DEFAULT now()
) engine = ReplacingMergeTree
order by (id);