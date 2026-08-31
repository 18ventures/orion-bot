ORION DASHBOARD V1
Built from bot(6).py.

bot(6)_dashboard.py is a copy of the latest bot with one strictly read-only Flask endpoint:
GET /api/client/dashboard?client_id=1
Header: X-Secret: <READ_API_SECRET>

It reads the selected client's balance, BTC position and existing daily counters.
It cannot execute, cancel, modify or close orders.

The original bot(6).py is untouched. Keep the current Railway deployment live until this copy is tested.
Set READ_API_SECRET in Railway before testing.
Never put exchange private/API keys in index.html.
