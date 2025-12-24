# routes/static_proxy.py

import io
import os
import mimetypes

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from utils.Cloude.Cloude import get_bytes  # R2 se bytes laane wala helper

router = APIRouter(prefix="/static", tags=["static"])


@router.get("/{path:path}")
def serve_static_from_r2(path: str):
    """
    Old URL:  /api/v1/static/<kuch-bhi>
    New flow:
      - <kuch-bhi> ko R2 key maan ke get_bytes() se file nikaalte hain
      - Content-Type extension se guess karte hain
      - StreamingResponse ke through browser ko bhej dete hain
    """
    key = path.lstrip("/")  # just in case

    if not key:
        raise HTTPException(status_code=404, detail="File path not specified")

    try:
        data = get_bytes(key)
    except Exception as e:
        # yahan detail me R2 ka error bhi aa sakta hai, chahe to generic rakh sakte ho
        raise HTTPException(
            status_code=404,
            detail=f"File not found in storage: {e}",
        )

    if not data:
        raise HTTPException(
            status_code=404,
            detail="Empty file or not found",
        )

    # Content-Type guess karo (pdf/image/video/doc sab ke liye)
    content_type, _ = mimetypes.guess_type(key)
    if not content_type:
        content_type = "application/octet-stream"

    filename = os.path.basename(key) or "file"

    return StreamingResponse(
        io.BytesIO(data),
        media_type=content_type,
        headers={
            # inline = browser me open karne ki koshish
            "Content-Disposition": f'inline; filename="{filename}"'
        },
    )
