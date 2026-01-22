# src/yolo_detect.py
"""
Scans downloaded images, runs object detection, and classifies content.
Output: data/enriched/yolo_detections/YYYY-MM-DD/detections.csv
"""

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from ultralytics import YOLO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yolo_enrich")

# Constants
TODAY = datetime.today().strftime("%Y-%m-%d")
RAW_IMAGES_DIR = Path("data/raw/images")
ENRICHED_DIR = Path("data/enriched/yolo_detections") / TODAY
ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = ENRICHED_DIR / "detections.csv"

# Load YOLOv8 nano model (downloads automatically on first run)
model = YOLO("yolov8n.pt")

# COCO class names (YOLOv8 uses standard COCO 80-class labels)
COCO_NAMES = model.names  # e.g., {0: 'person', 39: 'bottle', 40: 'wine glass', ...}

# Map relevant COCO classes to semantic groups
PRODUCT_CLASSES = {
    'bottle', 'cup', 'vase', 'scissors', 'toothbrush',
    'cell phone', 'remote', 'keyboard', 'mouse'
}
PERSON_CLASS = 'person'

def classify_image(detections: List[str]) -> Tuple[str, float]:
    """
    Classify image based on detected objects.
    Returns (category, max_confidence)
    """
    has_person = PERSON_CLASS in detections
    has_product = any(cls in PRODUCT_CLASSES for cls in detections)

    if has_person and has_product:
        category = "promotional"
    elif has_product and not has_person:
        category = "product_display"
    elif has_person and not has_product:
        category = "lifestyle"
    else:
        category = "other"

    # For simplicity, return max confidence as proxy (YOLO returns per-box conf)
    # In real use, you'd store all boxes—but here we simplify to one row per image
    return category, 0.0  # confidence handled at detection level

def process_image(image_path: Path) -> Optional[Dict]:
    """Run YOLO on a single image and return structured result."""
    try:
        results = model(image_path, verbose=False)
        result = results[0]

        if result.boxes is None or len(result.boxes) == 0:
            return {
                "message_id": int(image_path.stem),
                "image_path": str(image_path),
                "detected_objects": [],
                "max_confidence": 0.0,
                "image_category": "other"
            }

        boxes = result.boxes
        class_ids = boxes.cls.cpu().numpy().astype(int)
        confidences = boxes.conf.cpu().numpy()

        detected_names = [COCO_NAMES[i] for i in class_ids]
        max_conf = float(confidences.max())

        category, _ = classify_image(set(detected_names))

        return {
            "message_id": int(image_path.stem),
            "image_path": str(image_path),
            "detected_objects": detected_names,
            "max_confidence": max_conf,
            "image_category": category
        }

    except Exception as e:
        logger.warning(f"Failed to process {image_path}: {e}")
        return None

def main():
    logger.info("Starting YOLO enrichment...")
    all_results = []

    if not RAW_IMAGES_DIR.exists():
        logger.error("No raw images found.")
        return

    for channel_dir in RAW_IMAGES_DIR.iterdir():
        if not channel_dir.is_dir():
            continue
        logger.info(f"Processing channel: {channel_dir.name}")
        for img_file in channel_dir.glob("*.jpg"):
            result = process_image(img_file)
            if result:
                all_results.append(result)

    # Write to CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "message_id", "image_path", "detected_objects",
            "max_confidence", "image_category"
        ])
        writer.writeheader()
        for r in all_results:
            # Convert list to string for CSV
            r["detected_objects"] = ";".join(r["detected_objects"])
            writer.writerow(r)

    logger.info(f"✅ YOLO enrichment complete. {len(all_results)} images processed.")
    logger.info(f"Output saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()