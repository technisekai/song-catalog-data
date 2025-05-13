import requests
from googleapiclient.discovery import build

def get_metadata_from_spotify(
        search_track_url: str, 
        keyword: str, 
        access_token: str, 
        limit_results: int = 5
) -> list[dict]:
    """
    Extract metadata from spotify api
    Params:
        - search_track_url: endpoint to search tracks
        - keyword: song title
        - access_token: credentials to hit endpoint
        - limit_results: count tracks will be showed
    Return:
        list of metadata - 
        [{"title": ..., "artist": ..., "album": ..., "release_date": ..., "isrc": ...}, ...]
    """
    # Search tracks based on keyword
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "q": f"track:{keyword}",
        "type": "track",
        "limit": limit_results
    }
    response = requests.get(search_track_url, headers=headers, params=params)
    tracks = response.json().get("tracks", {}).get("items", [])
    # Extract tracks metadata
    results = []
    for track in tracks:
        track_info = {
            "title": track.get("name", None),
            "artist": track.get("artists", [{}])[0].get("name", None),
            "album": track.get("album", {}).get("name", None),
            "release_date": track.get("album", {}).get("release_date", None),
            "isrc": track.get("external_ids", {}).get("isrc", None)
        }
        results.append(track_info)
    return results

def get_metadata_from_youtube(
        keyword: str, 
        auth: build, 
        limit_results: int = 5
) -> list[dict]:
    """
    Get videos metadata from youtube api
    Params:
        - keyword: song title
        - auth: authentication method
        - limit_results: count tracks will be showed
    Return:
        list of metadata - 
        [{"video_id": ..., "channel_id": ..., "song_title": ..., "artist": ..., "video_title": ...}, ...]
    """
    # Search videos based on keyword
    response = auth.search().list(
        q=keyword,
        type="video",
        part="id",
        maxResults=limit_results
    ).execute()
    # Extract videos metadata
    results = []
    videos = [item["id"]["videoId"] for item in response["items"]]
    for video_id in videos:
        video_response = auth.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id
        ).execute()
        snippet = video_response['items'][0].get('snippet', None)
        if snippet:
            results.append({
                "video_id": video_id,
                "channel_id": snippet.get('channelId', None),
                "song_title": snippet.get('title', None),
                "artist": snippet.get('channelTitle', None),
                "video_title": snippet.get('title', None)
            })
    return results