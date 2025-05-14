with src__songs_catalog as (
	select
		code,
		original_artist ,
		song_title
	from bronze.gsheet_songs_catalog gsc
	where original_artist is not null and original_artist != ''
	and song_title is not null and song_title != ''
	and _created_at >= (select coalesce(max(_created_at), '0000-00-00 00:00:00') from silver.songs_catalog)
),
src__spotify_metadata as (
	select
		code,
		isrc,
		title,
		artist,
		album,
		release_date
		_created_at
	from bronze.spotify_metadata sm 
),
src__youtube_metadata as (
	select
		code,
		video_id, 
		channel_id, 
		song_title, 
		artist, 
		video_title,
		_created_at
	from bronze.youtube_metadata
),
stg__merge_source as (
	select
		xxHash64(
			concat(
				ssc.code, 
				ssc.original_artist, 
				ssc.song_title, 
				coalesce(ssd.isrc, ''), 
				coalesce(ssd.artist_name, ''),
				coalesce(ssd.recordings_title, ''),
				coalesce(ssd.video_id, ''),
				coalesce(ssd.channel_id, ''),
				coalesce(ssd.video_title, '')
			)
		) as id,
		*
	from src__songs_catalog ssc
	left join (
		select
			ssm.code as code,
			ssm.isrc as isrc,
			ssm.artist as artist_name,
			ssm.title as recordings_title,
			null as video_id,
			null as channel_id,
			null as video_title,
			'spotify' as source_data
		from src__spotify_metadata ssm
		union all
		select 
			sym.code as code,
			null as isrc,
			sym.artist as artist_name,
			sym.song_title as recordings_title,
			sym.video_id as video_id,
			sym.channel_id as channel_id,
			sym.video_title as video_title,
			'youtube' as source_data
		from
		src__youtube_metadata sym
	) as ssd
	using(code)
)
select * from stg__merge_source