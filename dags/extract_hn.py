import requests
import json
import os
from datetime import datetime


# ============================================================
# Configuration
# ============================================================

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"

STATE_FILE = "data-lake/state.json"

BATCH_SIZE = 100


# ============================================================
# 1. Hacker News API - Get Top Story IDs
# ============================================================

response = requests.get(TOP_STORIES_URL, timeout=30)
response.raise_for_status()

story_ids = response.json()

print("========================================")
print("HACKER NEWS EXTRACTION")
print("========================================")
print("Status Code:", response.status_code)
print("Number of Story IDs:", len(story_ids))
print("First 10 Story IDs:", story_ids[:10])


# ============================================================
# 2. Read Incremental State
# ============================================================

if not os.path.exists(STATE_FILE):
    state = {
        "last_processed_index": 0
    }

    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)

else:
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)


last_processed_index = state.get("last_processed_index", 0)

print("----------------------------------------")
print("Last processed index:", last_processed_index)


# ============================================================
# 3. Select Current Batch
# ============================================================

start = last_processed_index
end = min(start + BATCH_SIZE, len(story_ids))

batch_story_ids = story_ids[start:end]

if not batch_story_ids:
    print("No new story IDs available.")
    print("Extraction finished.")
    exit(0)


# Batch ID
batch_number = start // BATCH_SIZE

print("----------------------------------------")
print("Batch number:", batch_number)
print("Batch start:", start)
print("Batch end:", end)
print("Number of stories in this batch:", len(batch_story_ids))


# ============================================================
# 4. Get Hacker News Item
# ============================================================

def get_item(item_id):

    url = (
        f"https://hacker-news.firebaseio.com/v0/"
        f"item/{item_id}.json"
    )

    response = requests.get(
        url,
        timeout=10
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# 5. Get Story Details
# ============================================================

stories = []

print("----------------------------------------")
print("Fetching stories...")

for i, story_id in enumerate(batch_story_ids, start=1):

    print(
        f"Fetching story {i}/{len(batch_story_ids)} "
        f"- ID: {story_id}"
    )

    try:

        story = get_item(story_id)

        if story and story.get("type") == "story":

            stories.append(story)

    except requests.RequestException as e:

        print(
            f"Failed to fetch story {story_id}: {e}"
        )


print("Number of stories fetched:", len(stories))


# ============================================================
# 6. Get Comments
# ============================================================

comments = []

print("----------------------------------------")
print("Fetching comments...")

for i, story in enumerate(stories, start=1):

    story_id = story.get("id")

    comment_ids = story.get("kids", [])

    print(
        f"Fetching comments for story "
        f"{i}/{len(stories)} "
        f"- Story ID: {story_id} "
        f"- Comments: {len(comment_ids)}"
    )

    for comment_id in comment_ids:

        try:

            comment = get_item(comment_id)

            if comment and comment.get("type") == "comment":

                comment["story_id"] = story_id

                comments.append(comment)

        except requests.RequestException as e:

            print(
                f"Failed to fetch comment "
                f"{comment_id}: {e}"
            )


print("Number of comments fetched:", len(comments))


# ============================================================
# 7. Create Unique Batch Folder
# ============================================================

today = datetime.now().strftime("%Y-%m-%d")

batch_id = f"batch_{batch_number:04d}"


stories_path = (
    f"data-lake/raw/stories/"
    f"dt={today}/"
    f"{batch_id}"
)

comments_path = (
    f"data-lake/raw/comments/"
    f"dt={today}/"
    f"{batch_id}"
)


os.makedirs(stories_path, exist_ok=True)
os.makedirs(comments_path, exist_ok=True)


# ============================================================
# 8. Save Raw Stories JSON
# ============================================================

stories_file = f"{stories_path}/stories.json"

with open(
    stories_file,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        stories,
        f,
        ensure_ascii=False,
        indent=2
    )


# ============================================================
# 9. Save Raw Comments JSON
# ============================================================

comments_file = f"{comments_path}/comments.json"

with open(
    comments_file,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        comments,
        f,
        ensure_ascii=False,
        indent=2
    )


print("----------------------------------------")
print("Raw stories saved to:")
print(stories_file)

print("Raw comments saved to:")
print(comments_file)


# ============================================================
# 10. Update Incremental State
# ============================================================

state["last_processed_index"] = end

state["last_batch"] = batch_id

state["last_run_date"] = today

with open(
    STATE_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        state,
        f,
        indent=4
    )


# ============================================================
# 11. Final Summary
# ============================================================

print("========================================")
print("EXTRACTION COMPLETED SUCCESSFULLY")
print("========================================")

print("Batch ID:", batch_id)

print("Stories fetched:", len(stories))

print("Comments fetched:", len(comments))

print("Processed index:", start, "→", end)

print("Next batch starts from index:", end)

print("State updated successfully.")

