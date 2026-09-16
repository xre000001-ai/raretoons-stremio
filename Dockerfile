FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY addon.py episodes_index.jsonl streams.json ./

# Hugging Face Spaces / Render set PORT themselves; default keeps local dev simple
# TMDB API key - can be overridden via env TMDB_API_KEY or URL prefix /{key}/manifest.json
ENV PORT=7860
ENV TMDB_API_KEY=1af06616dcbb28ff03088d87d63211f5
EXPOSE 7860

CMD ["python3", "-u", "addon.py"]
