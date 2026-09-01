ORION DASHBOARD V2

The dashboard is served directly by the same Flask/Railway service.

Railway Start Command:
python "bot(6)_dashboard.py"

Open the Railway public URL directly. Do not open index.html from your computer.
The dashboard automatically uses the current page origin, avoiding the previous file:// / CORS issue.

Set READ_API_SECRET in Railway. The dashboard endpoint is read-only and does not execute, cancel, modify or close trades.
Keep the original live Railway bot untouched until this test deployment is confirmed.
