"""Synthetic ecommerce events writer for the shared Docker volume."""
import json
import os
import random
import time
from datetime import datetime, timezone

EVENT_TYPES = ['page_view', 'add_to_cart', 'purchase']
PRODUCT_IDS = [101, 102, 103, 104]
USER_IDS = list(range(1, 21))

while True:
    event = {
        'event_type': random.choice(EVENT_TYPES),
        'user_id': random.choice(USER_IDS),
        'product_id': random.choice(PRODUCT_IDS) if random.random() > 0.3 else None,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }
    filename = f'/data/events/raw/event_{int(time.time())}_{random.randint(1000, 9999)}.json'
    os.makedirs('/data/events/raw', exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(event, f)
    time.sleep(2)
