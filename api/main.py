
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, schemas

app = FastAPI(
    title="Kara Solutions Medical Telegram API",
    description="Analytical API for Ethiopian medical Telegram channels",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Welcome to Kara Solutions Data Platform API"}

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(database.get_db)
):
    """
    Returns most frequently mentioned terms in message_text.
    Simple approach: split on whitespace and count (for demo).
    In production: use NLP/tokenization.
    """
    query = text("""
        SELECT
            UNNEST(STRING_TO_ARRAY(LOWER(message_text), ' ')) AS term,
            COUNT(*) AS frequency
        FROM public.fct_messages
        WHERE LENGTH(message_text) > 0
        GROUP BY term
        HAVING term ~ '^[a-z]{3,}$'  -- only alphabetic words ≥3 chars
        ORDER BY frequency DESC
        LIMIT :limit
    """)
    results = db.execute(query, {"limit": limit}).fetchall()
    return [{"term": r.term, "frequency": r.frequency} for r in results]

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(
    channel_name: str,
    db: Session = Depends(database.get_db)
):
    """Daily posting activity and engagement for a channel."""
    query = text("""
        SELECT
            d.full_date::TEXT AS date,
            COUNT(*) AS message_count,
            ROUND(AVG(m.view_count), 2) AS avg_views
        FROM public.fct_messages m
        JOIN public.dim_channels c ON m.channel_key = c.channel_key
        JOIN public.dim_dates d ON m.date_key = d.date_key
        WHERE c.channel_name = :channel_name
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        LIMIT 30
    """)
    results = db.execute(query, {"channel_name": channel_name}).fetchall()
    if not results:
        raise HTTPException(status_code=404, detail="Channel not found")
    return [
        {"date": r.date, "message_count": r.message_count, "avg_views": r.avg_views}
        for r in results
    ]

@app.get("/api/search/messages", response_model=List[schemas.MessageSearchResult])
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(database.get_db)
):
    """Search messages containing a keyword (case-insensitive)."""
    like_pattern = f"%{query.lower()}%"
    sql = text("""
        SELECT
            m.message_id,
            c.channel_name,
            m.message_date::TEXT,
            m.message_text,
            m.view_count AS views
        FROM public.fct_messages m
        JOIN public.dim_channels c ON m.channel_key = c.channel_key
        WHERE LOWER(m.message_text) LIKE :pattern
        ORDER BY m.view_count DESC
        LIMIT :limit
    """)
    results = db.execute(sql, {"pattern": like_pattern, "limit": limit}).fetchall()
    return [
        {
            "message_id": r.message_id,
            "channel_name": r.channel_name,
            "message_date": r.message_date,
            "message_text": r.message_text,
            "views": r.views
        }
        for r in results
    ]

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(database.get_db)):
    """Image usage statistics by channel."""
    query = text("""
        SELECT
            c.channel_name,
            COUNT(*) AS total_images,
            ROUND(AVG(CASE WHEN d.image_category = 'promotional' THEN 1 ELSE 0 END) * 100, 1) AS promotional_pct,
            ROUND(AVG(CASE WHEN d.image_category = 'product_display' THEN 1 ELSE 0 END) * 100, 1) AS product_display_pct
        FROM public.fct_image_detections d
        JOIN public.dim_channels c ON d.channel_key = c.channel_key
        GROUP BY c.channel_name
        ORDER BY total_images DESC
    """)
    results = db.execute(query).fetchall()
    return [
        {
            "channel_name": r.channel_name,
            "total_images": r.total_images,
            "promotional_pct": float(r.promotional_pct),
            "product_display_pct": float(r.product_display_pct)
        }
        for r in results
    ]