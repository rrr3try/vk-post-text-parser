from vk_api.exceptions import VkApiError
from typing import List, Dict
import vk_api
import json
import os
import requests
from urllib.parse import urlparse
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from datetime import datetime

API_VERSION = "5.131"
POSTS_AT_A_TIME = 100  # VK API post count limit

with open("config.json", "r", encoding="utf-8") as config_file:
    f_data = json.load(config_file)

    ACCESS_TOKEN = f_data["access_token"]
    DOMAIN = f_data["domain"]

    RESTRICTED_WORDS = tuple(f_data["post_filter"]["restricted_words"])
    AD_ALLOWED = f_data["post_filter"]["ad_allowed"]
    REPOST_ALLOWED = f_data["post_filter"]["repost_allowed"]
    DOWNLOAD_ATTACHMENTS = f_data["download_attachments"]

    POST_NUMBER = f_data["post_number"]

    del f_data


def is_appropriate_post(post_data) -> bool:
    post_text = post_data["text"]
    return not any([
        not AD_ALLOWED and post_data["ad"],
        not REPOST_ALLOWED and post_data["repost"],
        any(restricted_word in post_text for restricted_word in RESTRICTED_WORDS)
    ])


def get_max_offset(api) -> int:
    try:
        return api.method(
            method="wall.get",
            values={"domain": DOMAIN, "count": POSTS_AT_A_TIME}
        )["count"]
    except VkApiError:
        raise VkApiError("Invalid access token. How to get your own token: "
                         "https://dev.vk.com/api/access-token/getting-started")


def parse_wall_data(api, post_offset):
    data = api.method(method="wall.get", values={
            "domain": DOMAIN,
            "offset": post_offset,
            "count": POSTS_AT_A_TIME
        })["items"]

    posts = []
    raw_posts = []
    for post in data:
        raw_posts.append(post)
        
        post_data = {
            "text": post["text"],
            "ad": post["marked_as_ads"],
            "repost": "copy_history" in post,
            "id": post["id"],
            "owner_id": post["owner_id"],
            "attachments": post.get("attachments", []),
            "date": post["date"]
        }
        
        if "copy_history" in post and post["copy_history"]:
            repost_data = post["copy_history"][0]
            post_data["repost_text"] = repost_data.get("text", "")
            post_data["repost_attachments"] = repost_data.get("attachments", [])
        
        posts.append(post_data)
    
    return posts, raw_posts


def download_attachment(attachment, attachments_dir, post_id):
    if attachment["type"] == "photo":
        photo = attachment["photo"]
        sizes = photo["sizes"]
        max_size_photo = max(sizes, key=lambda x: x["width"] * x["height"])
        url = max_size_photo["url"]
        
        filename = f"photo_{photo['id']}.jpg"
        filepath = os.path.join(attachments_dir, filename)
        
        response = requests.get(url)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
        
    elif attachment["type"] == "doc":
        doc = attachment["doc"]
        url = doc["url"]
        ext = doc.get("ext", "")
        filename = f"doc_{doc['id']}.{ext}" if ext else f"doc_{doc['id']}"
        filepath = os.path.join(attachments_dir, filename)
        
        response = requests.get(url)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
    
    elif attachment["type"] == "video":
        video = attachment["video"]
        filename = f"video_{video['id']}_info.txt"
        filepath = os.path.join(attachments_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"Video ID: {video['id']}\n")
            f.write(f"Title: {video.get('title', 'No title')}\n")
            f.write(f"Duration: {video.get('duration', 'Unknown')} seconds\n")
            f.write(f"Views: {video.get('views', 'Unknown')}\n")
            if 'description' in video:
                f.write(f"Description: {video['description']}\n")


def save_post(post_data, base_dir, raw_post_json):
    post_id = post_data["id"]
    post_date = post_data["date"]
    date_str = datetime.fromtimestamp(post_date).strftime("%Y-%m-%d")
    
    post_dir = os.path.join(base_dir, f"post_{post_id}_{date_str}")
    os.makedirs(post_dir, exist_ok=True)
    
    # Create VK post link
    owner_id = post_data["owner_id"]
    vk_link = f"https://vk.com/wall{owner_id}_{post_id}\n\n"
    
    # Combine post text with repost text if available
    full_text = vk_link + post_data["text"]
    if post_data.get("repost_text"):
        full_text += f"\n\n--- REPOST ---\n{post_data['repost_text']}"
    
    post_file = os.path.join(post_dir, f"post_{date_str}.txt")
    with open(post_file, 'w', encoding='utf-8') as f:
        f.write(full_text)
    
    # Save raw JSON response
    json_file = os.path.join(post_dir, "raw_response_item.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(raw_post_json, f, ensure_ascii=False, indent=2)
    
    # Download attachments if enabled
    if DOWNLOAD_ATTACHMENTS:
        all_attachments = post_data["attachments"][:]
        
        # Add repost attachments if available
        if post_data.get("repost_attachments"):
            all_attachments.extend(post_data["repost_attachments"])
        
        if all_attachments:
            attachments_dir = os.path.join(post_dir, "attachments")
            os.makedirs(attachments_dir, exist_ok=True)
            
            for attachment in all_attachments:
                try:
                    download_attachment(attachment, attachments_dir, post_id)
                except Exception as e:
                    print(f"Error downloading attachment: {e}")


def main():
    global POST_NUMBER

    api = vk_api.VkApi(token=ACCESS_TOKEN, api_version=API_VERSION)
    max_offset = get_max_offset(api)
    print(f"max_offset: {max_offset}")
    if not POST_NUMBER:
        POST_NUMBER = max_offset

    # Get the latest post date for filename
    first_posts, _ = parse_wall_data(api, 0)
    latest_date = first_posts[0]["date"] if first_posts else int(datetime.now().timestamp())
    date_str = datetime.fromtimestamp(latest_date).strftime("%Y-%m-%d")
    
    base_dir = f"{DOMAIN}_{date_str}"
    os.makedirs(base_dir, exist_ok=True)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("posts processed"),
        console=None
    ) as progress:
        total_posts_to_process = min(POST_NUMBER, max_offset)
        task = progress.add_task("Processing posts", total=total_posts_to_process)
        
        post_offset = 0
        processed_count = 0
        
        while (post_offset < POST_NUMBER) and (post_offset < max_offset):
            posts, raw_posts = parse_wall_data(api, post_offset)
            post_offset += POSTS_AT_A_TIME

            for i, post_data in enumerate(posts):
                if is_appropriate_post(post_data):
                    save_post(post_data, base_dir, raw_posts[i])
                processed_count += 1
                progress.update(task, advance=1)
                
                if processed_count >= total_posts_to_process:
                    break
            
            if processed_count >= total_posts_to_process:
                break


if __name__ == "__main__":
    main()