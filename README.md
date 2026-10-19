# CPOS lead feed

The CPOS outreach agent set up this repo by itself. Twice a week (Sunday and Wednesday mornings) GitHub runs
`extract.py`, which picks about 300 new independent Ontario businesses from Overture Maps (Ottawa first, a mix
of industries, never the same business twice) and saves them as a new file in `feed/history/`. The agent checks
this repo every few hours, adds each new file to its Leads tab once, and starts an extra run by itself when its
leads run low and no file is waiting.

- Nothing here needs you. To get a batch right now: Actions > Lead feed > Run workflow (you can change how many
  businesses and which cities).
- `state/sent_ids.txt` remembers every business already handed over. Delete a line to allow that one again.
- When the agent gets a new version, it may update `extract.py` and the workflow here by itself.
- Map data: Overture Maps Foundation, places licensed CDLA Permissive 2.0.
